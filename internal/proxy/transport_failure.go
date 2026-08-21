package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
)

// classifyTransportFailure returns a stable policy input. It deliberately
// uses only transport milestones observed by this attempt; error strings are
// not a protocol and must not drive routing decisions.
func classifyTransportFailure(err error, trace *upstreamRequestAttemptTrace) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}

	var netErr net.Error
	timedOut := errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
	if !timedOut {
		return "transport_error"
	}
	if trace == nil || !trace.gotConn.Load() {
		return "connect_timeout"
	}
	if trace.firstResponseByteSeen.Load() {
		return "idle_timeout"
	}
	if trace.wroteRequest.Load() {
		return "response_header_timeout"
	}
	return "transport_timeout"
}

// applyTransportFailureRule applies a transport failure policy. A started
// response may still quarantine the current route, but callers must gate
// retry-next on responseStarted so one downstream request never gets two
// upstream responses.
func applyTransportFailureRule(
	router *routing.Router,
	route routing.RouteResult,
	trace *upstreamRequestAttemptTrace,
	err error,
) (platform.ResponseRuleMatch, bool, string) {
	kind := classifyTransportFailure(err, trace)
	if kind == "" || kind == "canceled" || len(route.ResponseRules) == 0 {
		return platform.ResponseRuleMatch{}, false, kind
	}
	match, ok := route.ResponseRules.MatchFailure(kind, time.Now())
	if ok && match.Cooldown && router != nil {
		router.QuarantineRoute(route, match.Scope, match.Until)
	}
	return match, ok, kind
}

// attemptContextForRequest reserves the remaining caller deadline across the
// attempts still available in this immutable retry snapshot. A transport
// failure retry is not allowed without an overall deadline: otherwise one
// stuck RoundTripper could consume an unbounded amount of time.
func attemptContextForRequest(parent context.Context, attempt, total int) (context.Context, context.CancelFunc, bool) {
	if parent == nil {
		parent = context.Background()
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return parent, nil, false
	}
	remainingSlots := total - attempt
	if remainingSlots < 1 {
		return nil, nil, true
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, nil, true
	}
	perAttempt := remaining / time.Duration(remainingSlots)
	if perAttempt <= 0 {
		return nil, nil, true
	}
	ctx, cancel := context.WithTimeout(parent, perAttempt)
	return ctx, cancel, true
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel func()
	once   sync.Once
}

func (r *cancelOnCloseReadCloser) finish() {
	if r == nil || r.cancel == nil {
		return
	}
	r.once.Do(r.cancel)
}

func (r *cancelOnCloseReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil {
		r.finish()
	}
	return n, err
}

func (r *cancelOnCloseReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.finish()
	return err
}
