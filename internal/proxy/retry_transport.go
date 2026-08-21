package proxy

import (
	"context"
	"net/http"
	"net/netip"

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
	router          *routing.Router
	pool            outbound.PoolAccessor
	initial         routedOutbound
	account         string
	transportFor    func(routedOutbound) http.RoundTripper
	onRoute         func(routing.RouteResult, *node.NodeEntry)
	decorateAttempt func(*http.Request, routedOutbound) (*http.Request, *upstreamRequestAttemptTrace)
	onAttemptEgress func(headerBytes, bodyBytes int64)

	promotable bool
}

func (t *reverseRetryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.router == nil || t.pool == nil {
		return nil, context.Canceled
	}

	baseReq := req
	var capture *replayBodyCapture
	if baseReq.Body != nil && baseReq.Body != http.NoBody {
		capture = newReplayBodyCapture(baseReq.Body)
		baseReq = baseReq.Clone(baseReq.Context())
		baseReq.Body = capture
	}
	budget := t.initial.Route.RetryBudget
	if budget < 1 {
		budget = 1
	}
	attemptedEntries := map[*node.NodeEntry]struct{}{t.initial.Entry: {}}
	attemptedIPs := map[netip.Addr]struct{}{t.initial.Route.EgressIP: {}}
	current := t.initial
	advance := func() (routedOutbound, bool) {
		if req.Context().Err() != nil {
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
		attemptCtx, cancelAttempt, bounded := attemptContextForRequest(req.Context(), attempt, budget)
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
		if attempt > 0 {
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
		if !bodyComplete {
			if cancelAttempt != nil {
				cancelAttempt()
			}
			t.promotable = false
			return nil, err
		}
		if attemptTrace != nil && attemptTrace.commitEgress(resp != nil && err == nil, err) && t.onAttemptEgress != nil {
			t.onAttemptEgress(pendingHeaderBytes, bodyBytes)
		}
		if err != nil {
			failureMatch, failureMatched, _ := applyTransportFailureRule(t.router, current.Route, attemptTrace, err)
			canRetry := failureMatched && failureMatch.RetryNext() && (attemptTrace == nil || !attemptTrace.responseStarted()) && resp == nil && bounded &&
				requestCanBeReplayed(req, capture) && attempt+1 < budget && req.Context().Err() == nil
			if canRetry {
				if cancelAttempt != nil {
					cancelAttempt()
				}
				if next, ok := advance(); ok {
					current = next
					continue
				}
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
		if !matched || !decision.RetryNext() || !requestCanBeReplayed(req, capture) || attempt+1 >= budget {
			t.promotable = finalAccepted
			if cancelAttempt != nil {
				if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
					cancelAttempt()
				} else {
					resp.Body = &cancelOnCloseReadCloser{ReadCloser: resp.Body, cancel: cancelAttempt}
				}
			}
			return resp, nil
		}
		if req.Context().Err() != nil {
			_ = resp.Body.Close()
			if cancelAttempt != nil {
				cancelAttempt()
			}
			return nil, req.Context().Err()
		}

		next, ok := advance()
		if !ok {
			t.promotable = false
			return resp, nil
		}
		_ = resp.Body.Close()
		if cancelAttempt != nil {
			cancelAttempt()
		}
		current = next
	}
	return nil, context.Canceled
}
