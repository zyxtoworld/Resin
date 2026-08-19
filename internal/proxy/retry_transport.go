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

	for attempt := 0; attempt < budget; attempt++ {
		if t.onRoute != nil {
			t.onRoute(current.Route, current.Entry)
		}
		outReq := baseReq
		if attempt > 0 {
			outReq = cloneForwardRequestForRetry(baseReq, capture.Bytes())
		}
		var attemptTrace *upstreamRequestAttemptTrace
		if t.decorateAttempt != nil {
			outReq, attemptTrace = t.decorateAttempt(outReq, current)
		}
		pendingHeaderBytes := headerWireLen(outReq.Header)
		var bodyCounter *countingReadCloser
		if outReq.Body != nil && outReq.Body != http.NoBody {
			bodyCounter = newCountingReadCloser(outReq.Body)
			outReq.Body = bodyCounter
		}
		resp, err := t.transportFor(current).RoundTrip(outReq)
		if attemptTrace != nil && attemptTrace.commitEgress(resp != nil && err == nil, err) && t.onAttemptEgress != nil {
			bodyBytes := int64(0)
			if bodyCounter != nil {
				bodyBytes = bodyCounter.Total()
			}
			t.onAttemptEgress(pendingHeaderBytes, bodyBytes)
		}
		if err != nil {
			return nil, err
		}

		decision, matched := applyResponseRules(t.router, current.Route, resp)
		finalAccepted := !matched || decision.Action == platform.ResponseRuleActionPassthrough
		if !matched || !decision.RetryNext() || !requestCanBeReplayed(req, capture) || attempt+1 >= budget {
			t.promotable = finalAccepted
			return resp, nil
		}
		if req.Context().Err() != nil {
			_ = resp.Body.Close()
			return nil, req.Context().Err()
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
		nextRoute, nextErr := t.router.RouteRequestNext(current.Route, exclusions)
		if nextErr != nil {
			t.promotable = false
			return resp, nil
		}
		next, bindErr := bindRoutedOutbound(nextRoute, t.pool)
		if bindErr != nil {
			t.promotable = false
			return resp, nil
		}
		attemptedEntries[next.Entry] = struct{}{}
		attemptedIPs[next.Route.EgressIP] = struct{}{}
		_ = resp.Body.Close()
		current = next
	}
	return nil, context.Canceled
}
