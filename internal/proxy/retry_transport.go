package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/netip"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
)

// reverseRetryRoundTripper applies the same compiled response decision and
// immutable retry snapshot used by the forward proxy. Reverse requests are
// retried only after their body is fully captured and before ReverseProxy has
// committed any downstream response bytes.
type reverseRetryRoundTripper struct {
	router              *routing.Router
	pool                outbound.PoolAccessor
	initial             routedOutbound
	account             string
	requestTotalTimeout time.Duration
	transportFor        func(routedOutbound) http.RoundTripper
	onRoute             func(routing.RouteResult, *node.NodeEntry)
	decorateAttempt     func(*http.Request, routedOutbound) (*http.Request, *upstreamRequestAttemptTrace)
	onAttemptEgress     func(headerBytes, bodyBytes int64)

	promotable bool
}

func (t *reverseRetryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.router == nil || t.pool == nil {
		return nil, context.Canceled
	}

	requestBudget, budgetEnabled := effectiveProxyRequestBudget(t.initial.Route.RequestTotalTimeout, t.requestTotalTimeout)
	requestCtx := req.Context()
	cancelRequest := func() {}
	releaseRequest := func() {}
	if budgetEnabled {
		requestCtx, cancelRequest, releaseRequest = withProxyRequestBudgetController(req.Context(), requestBudget)
	}
	cleanupRequest := true
	defer func() {
		if cleanupRequest {
			cancelRequest()
		}
	}()

	baseReq := req
	var capture *replayBodyCapture
	var preparedBody []byte
	var bodyPrepared bool
	if budgetEnabled && baseReq.Body != nil && baseReq.Body != http.NoBody {
		var prepared bool
		var prepareErr error
		var passthroughBody io.ReadCloser
		preparedBody, prepared, passthroughBody, prepareErr = captureRequestBodyForRetry(requestCtx, req)
		if prepareErr != nil {
			releaseRequest()
			cancelRequest()
			return nil, prepareErr
		}
		bodyPrepared = prepared
		baseReq = baseReq.Clone(baseReq.Context())
		if prepared {
			baseReq.Body = http.NoBody
			baseReq.ContentLength = int64(len(preparedBody))
			baseReq.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(preparedBody)), nil
			}
		} else if passthroughBody != nil {
			baseReq.Body = passthroughBody
		} else {
			capture = newReplayBodyCapture(baseReq.Body, baseReq.ContentLength)
			baseReq.Body = capture
		}
	}
	finishResponse := func(resp *http.Response, accepted bool, cancelAttempt context.CancelFunc) (*http.Response, error) {
		t.promotable = accepted
		releaseRequest()
		if cancelAttempt == nil {
			cancelRequest()
			return resp, nil
		}
		if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
			cancelAttempt()
			cancelRequest()
			return resp, nil
		}
		cleanupRequest = false
		cancel := func() {
			cancelAttempt()
			cancelRequest()
		}
		if rwc, ok := resp.Body.(io.ReadWriteCloser); ok {
			resp.Body = &cancelOnCloseReadWriteCloser{ReadWriteCloser: rwc, cancel: cancel}
		} else {
			resp.Body = &cancelOnCloseReadCloser{ReadCloser: resp.Body, cancel: cancel}
		}
		return resp, nil
	}
	budget := t.initial.Route.RetryBudget
	if budget < 1 {
		budget = 1
	}
	attemptedEntries := map[*node.NodeEntry]struct{}{t.initial.Entry: {}}
	attemptedIPs := map[netip.Addr]struct{}{t.initial.Route.EgressIP: {}}
	current := t.initial
	advance := func() (routedOutbound, bool) {
		if requestCtx.Err() != nil {
			return routedOutbound{}, false
		}
		exclusions := routing.RouteRetryExclusions{
			Entries:   make([]*node.NodeEntry, 0, len(attemptedEntries)),
			EgressIPs: make([]netip.Addr, 0, len(attemptedIPs)),
		}
		for entry := range attemptedEntries {
			exclusions.Entries = append(exclusions.Entries, entry)
		}
		for ip := range attemptedIPs {
			exclusions.EgressIPs = append(exclusions.EgressIPs, ip)
		}
		nextRoute, err := t.router.RouteRequestNext(current.Route, exclusions)
		if err != nil {
			return routedOutbound{}, false
		}
		next, bindErr := bindRoutedOutbound(nextRoute, t.pool)
		if bindErr != nil {
			return routedOutbound{}, false
		}
		attemptedEntries[next.Entry] = struct{}{}
		attemptedIPs[next.Route.EgressIP] = struct{}{}
		return next, true
	}

	for attempt := 0; attempt < budget; attempt++ {
		attemptCtx := requestCtx
		var cancelAttempt context.CancelFunc
		releaseAttempt := func() {}
		bounded := false
		if budgetEnabled {
			attemptCtx, cancelAttempt, releaseAttempt, bounded = attemptContextForRequest(requestCtx, attempt, budget)
		}
		if bounded && attemptCtx == nil {
			t.promotable = false
			if err := req.Context().Err(); err != nil {
				return nil, err
			}
			return nil, context.DeadlineExceeded
		}
		if attemptCtx == nil {
			attemptCtx = req.Context()
		}
		if t.onRoute != nil {
			t.onRoute(current.Route, current.Entry)
		}
		outReq := baseReq
		if preparedBody != nil {
			outReq = cloneForwardRequestForRetry(baseReq, preparedBody)
		} else if attempt > 0 {
			outReq = cloneForwardRequestForRetry(baseReq, capture.Bytes())
		}
		outReq = outReq.WithContext(attemptCtx)
		var attemptTrace *upstreamRequestAttemptTrace
		if t.decorateAttempt != nil {
			outReq, attemptTrace = t.decorateAttempt(outReq, current)
		}
		pendingHeaderBytes := headerWireLen(outReq.Header)
		resp, err, bodyBytes, bodyComplete := roundTripWithBodyCompletion(
			attemptCtx, t.transportFor(current), outReq,
		)
		bodyReplayable := bodyPrepared || requestCanBeReplayed(req, capture)
		retryCurrent := func() bool {
			if cancelAttempt != nil {
				cancelAttempt()
			}
			next, ok := advance()
			if !ok {
				return false
			}
			current = next
			return true
		}
		if !bodyComplete {
			if err != nil {
				failureMatch, failureMatched, _ := applyTransportFailureRule(t.router, current.Route, attemptTrace, err)
				canRetry := budgetEnabled && failureMatched && failureMatch.RetryNext() && (attemptTrace == nil || !attemptTrace.responseStarted()) && resp == nil && bounded &&
					bodyReplayable && attempt+1 < budget && requestCtx.Err() == nil
				if canRetry && retryCurrent() {
					continue
				}
			}
			if cancelAttempt != nil {
				cancelAttempt()
			}
			t.promotable = false
			return nil, err
		}
		if attemptTrace != nil && attemptTrace.commitEgress(resp != nil && err == nil, err) && t.onAttemptEgress != nil {
			t.onAttemptEgress(pendingHeaderBytes, bodyBytes)
		}
		if bodyComplete && resp != nil && err == nil {
			releaseAttempt()
		}
		if err != nil {
			failureMatch, failureMatched, _ := applyTransportFailureRule(t.router, current.Route, attemptTrace, err)
			canRetry := budgetEnabled && failureMatched && failureMatch.RetryNext() && (attemptTrace == nil || !attemptTrace.responseStarted()) && resp == nil && bounded &&
				bodyReplayable && attempt+1 < budget && requestCtx.Err() == nil
			if canRetry && retryCurrent() {
				continue
			}
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if cancelAttempt != nil {
				cancelAttempt()
			}
			return nil, err
		}

		decision, matched := applyResponseRules(t.router, current.Route, resp)
		finalAccepted := !matched || decision.Action == platform.ResponseRuleActionPassthrough
		if !matched || !budgetEnabled || !decision.RetryNext() || !bodyReplayable || attempt+1 >= budget {
			return finishResponse(resp, finalAccepted, cancelAttempt)
		}
		if requestCtx.Err() != nil {
			_ = resp.Body.Close()
			if cancelAttempt != nil {
				cancelAttempt()
			}
			return nil, req.Context().Err()
		}

		next, ok := advance()
		if !ok {
			return finishResponse(resp, false, cancelAttempt)
		}
		_ = resp.Body.Close()
		if cancelAttempt != nil {
			cancelAttempt()
		}
		current = next
	}
	return nil, context.Canceled
}
