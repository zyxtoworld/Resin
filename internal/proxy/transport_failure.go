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

const (
	// maxProxyAttempts bounds work per request. A platform view may contain
	// thousands of candidates, but that is not a reason to create thousands of
	// tiny deadline slices or to retry until the caller's deadline is exhausted.
	maxProxyAttempts = 3

	// attemptBudgetReservationSlots reserves time for the current attempt and
	// one next-node attempt. Further attempts are still possible when earlier
	// attempts finish early, but candidate cardinality never shrinks the first
	// attempt to milliseconds.
	attemptBudgetReservationSlots = 2
)

func proxyAttemptLimit(candidateBudget int) int {
	if candidateBudget < 1 {
		return 1
	}
	if candidateBudget > maxProxyAttempts {
		return maxProxyAttempts
	}
	return candidateBudget
}

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

// withProxyRequestBudgetController creates the one request-level budget used
// by all attempts in a proxy request. A caller deadline always wins when it is
// earlier. A zero budget deliberately preserves fail-closed behavior for
// callers that have no deadline and did not opt into a proxy budget.
func withProxyRequestBudgetController(parent context.Context, total time.Duration) (context.Context, context.CancelFunc, func()) {
	if parent == nil {
		parent = context.Background()
	}
	if total <= 0 {
		return parent, func() {}, func() {}
	}
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= total {
		return parent, func() {}, func() {}
	}
	return newStoppableDeadlineContext(parent, total)
}

type stoppableDeadlineContext struct {
	parent context.Context
	done   chan struct{}

	mu       sync.Mutex
	err      error
	closed   bool
	active   bool
	deadline time.Time
	timer    *time.Timer
	stopHook func() bool
}

func newStoppableDeadlineContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, func()) {
	if parent == nil {
		parent = context.Background()
	}
	c := &stoppableDeadlineContext{
		parent:   parent,
		done:     make(chan struct{}),
		active:   true,
		deadline: time.Now().Add(timeout),
	}
	// Install both asynchronous handles while holding the same mutex used by
	// finish. A canceled parent or a zero/very short timer may invoke its
	// callback before AfterFunc/time.AfterFunc returns; the callback must not
	// observe a half-installed context and the constructor must not attach a
	// handle after finish has closed done.
	c.mu.Lock()
	c.stopHook = context.AfterFunc(parent, func() { c.finish(parent.Err(), true) })
	c.timer = time.AfterFunc(timeout, func() { c.finish(context.DeadlineExceeded, false) })
	c.mu.Unlock()
	cancel := func() {
		c.finish(context.Canceled, false)
	}
	release := func() {
		c.release()
	}
	return c, cancel, release
}

func (c *stoppableDeadlineContext) Deadline() (time.Time, bool) {
	c.mu.Lock()
	active := c.active
	deadline := c.deadline
	c.mu.Unlock()
	if active {
		return deadline, true
	}
	return c.parent.Deadline()
}

func (c *stoppableDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *stoppableDeadlineContext) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.parent.Err()
}

func (c *stoppableDeadlineContext) Value(key any) any { return c.parent.Value(key) }

func (c *stoppableDeadlineContext) finish(err error, fromParent bool) {
	if err == nil {
		err = context.Canceled
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.active = false
	c.err = err
	stopHook := c.stopHook
	c.stopHook = nil
	timer := c.timer
	c.timer = nil
	close(c.done)
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if !fromParent && stopHook != nil {
		stopHook()
	}
}

func (c *stoppableDeadlineContext) release() {
	c.mu.Lock()
	if c.closed || !c.active {
		c.mu.Unlock()
		return
	}
	c.active = false
	timer := c.timer
	c.timer = nil
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

// attemptContextForRequest reserves the remaining request deadline across the
// attempts still available in this immutable retry snapshot. The caller must
// pass the context returned by withProxyRequestBudgetController so a configured budget
// is created once, not reset for every retry.
func attemptContextForRequest(parent context.Context, attempt, total int) (context.Context, context.CancelFunc, func(), bool) {
	if parent == nil {
		parent = context.Background()
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return parent, nil, func() {}, false
	}
	remainingSlots := total - attempt
	if remainingSlots < 1 {
		return nil, nil, func() {}, true
	}
	if remainingSlots > attemptBudgetReservationSlots {
		remainingSlots = attemptBudgetReservationSlots
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, nil, func() {}, true
	}
	perAttempt := remaining / time.Duration(remainingSlots)
	if perAttempt <= 0 {
		return nil, nil, func() {}, true
	}
	ctx, cancel, release := newStoppableDeadlineContext(parent, perAttempt)
	return ctx, cancel, release, true
}

func effectiveProxyRequestBudget(platformBudget, globalCap time.Duration) (time.Duration, bool) {
	if platformBudget <= 0 {
		return 0, false
	}
	if globalCap > 0 && platformBudget > globalCap {
		return globalCap, true
	}
	return platformBudget, true
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

// cancelOnCloseReadWriteCloser preserves the bidirectional body contract used
// by HTTP 101 upgrades while keeping the same request-budget lifetime rule as
// cancelOnCloseReadCloser.
type cancelOnCloseReadWriteCloser struct {
	io.ReadWriteCloser
	cancel    func()
	once      sync.Once
	closeOnce sync.Once
	closeErr  error
}

func (r *cancelOnCloseReadWriteCloser) finish() {
	if r == nil || r.cancel == nil {
		return
	}
	r.once.Do(r.cancel)
}

func (r *cancelOnCloseReadWriteCloser) Read(p []byte) (int, error) {
	n, err := r.ReadWriteCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		r.finish()
	}
	return n, err
}

func (r *cancelOnCloseReadWriteCloser) Write(p []byte) (int, error) {
	return r.ReadWriteCloser.Write(p)
}

func (r *cancelOnCloseReadWriteCloser) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.ReadWriteCloser.Close()
		r.finish()
	})
	return r.closeErr
}
