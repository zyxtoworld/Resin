package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// captureRequestHeaders serializes headers to canonical wire format for
// request-log payload capture. It operates on a clone so redaction never
// changes the headers sent to the upstream request.
func captureRequestHeaders(header http.Header) []byte {
	if header == nil {
		return nil
	}
	clone := header.Clone()
	for name := range clone {
		if !isSensitiveCapturedHeader(name) {
			continue
		}
		clone[name] = []string{"[REDACTED]"}
	}
	var buf bytes.Buffer
	_ = clone.Write(&buf)
	return buf.Bytes()
}

func isSensitiveCapturedHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie",
		"x-api-key", "x-auth-token", "x-access-token", "x-csrf-token":
		return true
	default:
		return false
	}
}

// headerWireLen returns canonical wire-format header bytes length.
func headerWireLen(header http.Header) int64 {
	if header == nil || len(header) == 0 {
		return 0
	}
	var buf bytes.Buffer
	_ = header.Write(&buf)
	return int64(buf.Len())
}

func captureHeadersWithLimit(header http.Header, maxBytes int) ([]byte, int, bool) {
	payload := captureRequestHeaders(header)
	totalLen := len(payload)
	if totalLen == 0 {
		return nil, 0, false
	}
	if maxBytes >= 0 && totalLen > maxBytes {
		return payload[:maxBytes], totalLen, true
	}
	return payload, totalLen, false
}

type payloadCaptureReadCloser struct {
	rc       io.ReadCloser
	maxBytes int
	mu       sync.Mutex
	aborted  bool
	payload  bytes.Buffer
	totalLen int
}

func newPayloadCaptureReadCloser(rc io.ReadCloser, maxBytes int) *payloadCaptureReadCloser {
	return &payloadCaptureReadCloser{
		rc:       rc,
		maxBytes: maxBytes,
	}
}

func (c *payloadCaptureReadCloser) Read(p []byte) (int, error) {
	if c == nil || c.rc == nil {
		return 0, io.EOF
	}
	n, err := c.rc.Read(p)
	if n <= 0 {
		return n, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return n, err
	}
	c.totalLen += n
	if c.maxBytes < 0 {
		_, _ = c.payload.Write(p[:n])
	} else {
		remaining := c.maxBytes - c.payload.Len()
		if remaining > 0 {
			if n <= remaining {
				_, _ = c.payload.Write(p[:n])
			} else {
				_, _ = c.payload.Write(p[:remaining])
			}
		}
	}
	return n, err
}

func (c *payloadCaptureReadCloser) abandon() {
	if c != nil {
		c.mu.Lock()
		c.aborted = true
		c.mu.Unlock()
	}
}

func (c *payloadCaptureReadCloser) Close() error {
	if c == nil || c.rc == nil {
		return nil
	}
	return c.rc.Close()
}

func (c *payloadCaptureReadCloser) Payload() []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return nil
	}
	return append([]byte(nil), c.payload.Bytes()...)
}

func (c *payloadCaptureReadCloser) TotalLen() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return 0
	}
	return c.totalLen
}

func (c *payloadCaptureReadCloser) Truncated() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return false
	}
	return c.totalLen > c.payload.Len()
}

func (c *payloadCaptureReadCloser) Snapshot() ([]byte, int, bool, bool) {
	if c == nil {
		return nil, 0, false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return nil, 0, false, false
	}
	payload := append([]byte(nil), c.payload.Bytes()...)
	return payload, c.totalLen, c.totalLen > c.payload.Len(), true
}

// attemptRequestBody owns one upstream attempt's request-body completion.
// net/http permits a RoundTripper to finish reading and close Request.Body in
// a goroutine after RoundTrip returns. The close signal is therefore the
// linearization point for egress accounting and request-log finalization.
type attemptRequestBody struct {
	rc          io.ReadCloser
	done        chan struct{}
	closeOnce   sync.Once
	mu          sync.Mutex
	activeReads int
	closed      bool
	abandoned   bool
	completed   bool
	closeErr    error
}

type attemptBodyAborter interface {
	abandon()
}

func newAttemptRequestBody(rc io.ReadCloser) *attemptRequestBody {
	return &attemptRequestBody{
		rc:   rc,
		done: make(chan struct{}),
	}
}

func (b *attemptRequestBody) Read(p []byte) (int, error) {
	if b == nil {
		return 0, io.EOF
	}
	b.mu.Lock()
	if b.closed || b.abandoned {
		b.mu.Unlock()
		return 0, io.EOF
	}
	b.activeReads++
	b.mu.Unlock()
	n, err := b.rc.Read(p)
	b.mu.Lock()
	b.activeReads--
	b.tryCompleteLocked()
	b.mu.Unlock()
	return n, err
}

func (b *attemptRequestBody) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		err := b.rc.Close()
		b.mu.Lock()
		b.closeErr = err
		b.closed = true
		b.tryCompleteLocked()
		b.mu.Unlock()
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeErr
}

func (b *attemptRequestBody) abort() {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.abandoned {
		b.mu.Unlock()
		return
	}
	b.abandoned = true
	b.mu.Unlock()
	if abandoner, ok := b.rc.(attemptBodyAborter); ok {
		abandoner.abandon()
	}
	go func() { _ = b.Close() }()
}

func (b *attemptRequestBody) tryCompleteLocked() {
	if b.completed || !b.closed || b.abandoned || b.activeReads != 0 {
		return
	}
	b.completed = true
	close(b.done)
}

// await waits for the transport's close and all active reads. Cancellation or
// the cleanup budget abandons the wrapper before closing it; no mutable Resin
// capture is then read or published by the caller.
var errAttemptBodyQuiescence = errors.New("upstream request body did not quiesce")

const attemptBodyQuiescenceBudget = time.Second

var errNonCompliantAttemptCallLimit = errors.New("upstream non-compliant attempt call limit reached")

// maxNonCompliantAttemptCalls bounds calls routed through the defensive
// adapter path below. Production outbound requests use *http.Transport and
// stay on net/http's context-aware synchronous path; this quota is only for a
// RoundTripper implementation that can outlive its canceled caller.
const maxNonCompliantAttemptCalls = 64

type nonCompliantAttemptCallLimiter struct {
	slots chan struct{}
}

func newNonCompliantAttemptCallLimiter(capacity int) *nonCompliantAttemptCallLimiter {
	if capacity <= 0 {
		capacity = 1
	}
	return &nonCompliantAttemptCallLimiter{slots: make(chan struct{}, capacity)}
}

func (l *nonCompliantAttemptCallLimiter) tryAcquire() (func(), bool) {
	if l == nil || l.slots == nil {
		return func() {}, true
	}
	select {
	case l.slots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-l.slots })
		}, true
	default:
		return nil, false
	}
}

var defaultNonCompliantAttemptCallLimiter = newNonCompliantAttemptCallLimiter(maxNonCompliantAttemptCalls)

func (b *attemptRequestBody) await(ctx context.Context, budget time.Duration) error {
	if b == nil {
		return nil
	}
	if budget <= 0 {
		budget = attemptBodyQuiescenceBudget
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		b.abort()
		return ctx.Err()
	case <-timer.C:
		b.abort()
		return errAttemptBodyQuiescence
	}
}

// roundTripWithBodyCompletion runs one upstream attempt and waits for the
// RoundTripper-owned request body to close before exposing its byte count.
// The bool is false when cancellation prevented a completed body owner; in
// that case callers must fail closed and must not commit the partial count.
func roundTripWithBodyCompletion(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
) (*http.Response, error, int64, bool) {
	return roundTripWithBodyCompletionBudgetAndDeadline(ctx, transport, req, attemptBodyQuiescenceBudget, false)
}

func roundTripWithBodyCompletionBudget(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
	budget time.Duration,
) (*http.Response, error, int64, bool) {
	return roundTripWithBodyCompletionBudgetAndDeadline(ctx, transport, req, budget, false)
}

func roundTripWithAttemptDeadline(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
) (*http.Response, error, int64, bool) {
	return roundTripWithBodyCompletionBudgetAndDeadline(ctx, transport, req, attemptBodyQuiescenceBudget, true)
}

func roundTripWithBodyCompletionBudgetAndDeadline(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
	budget time.Duration,
	enforceCallDeadline bool,
) (*http.Response, error, int64, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	var counter *countingReadCloser
	var owner *attemptRequestBody
	if req != nil && req.Body != nil && req.Body != http.NoBody {
		counter = newCountingReadCloser(req.Body)
		owner = newAttemptRequestBody(counter)
		req.Body = owner
	}
	var resp *http.Response
	var err error
	if enforceCallDeadline {
		resp, err = roundTripWithAttemptContext(ctx, transport, req)
	} else {
		resp, err = transport.RoundTrip(req)
	}
	if errors.Is(err, errNonCompliantAttemptCallLimit) {
		if owner != nil {
			owner.abort()
		}
		return nil, err, 0, false
	}
	if owner != nil {
		// A successful RoundTrip may return response headers while an HTTP/2 or
		// full-duplex transport is still finishing the request body. The response
		// is already the accepted upstream result at this boundary: never close it
		// or turn it into a synthetic transport error just because Close is late.
		if resp != nil && err == nil && ctx.Err() == nil {
			select {
			case <-owner.done:
				return resp, err, counter.Total(), true
			default:
				completion := newAttemptBodyCompletion(owner, counter)
				completion.wrapResponseBody(resp)
				return resp, err, 0, false
			}
		}
		if awaitErr := owner.await(ctx, budget); awaitErr != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			// Preserve a transport error for policy classification even when the
			// request body owner did not quiesce. Callers must still fail closed
			// for egress accounting, but a known transport failure must not skip
			// its platform cooldown rule.
			if err != nil && !errors.Is(awaitErr, context.Canceled) && !errors.Is(awaitErr, context.DeadlineExceeded) {
				return nil, err, 0, false
			}
			return nil, awaitErr, 0, false
		}
	}
	if counter == nil {
		return resp, err, 0, true
	}
	return resp, err, counter.Total(), true
}

type roundTripResult struct {
	resp *http.Response
	err  error
}

type roundTripCall struct {
	mu        sync.Mutex
	done      chan struct{}
	finished  bool
	abandoned bool
	result    roundTripResult
}

// roundTripWithAttemptContext makes the attempt deadline a call boundary as
// well as a request context. The standard transport normally returns when the
// context is canceled, but an outbound adapter can otherwise leave RoundTrip
// blocked while the proxy has already spent the attempt budget. A late result
// is never usable by this attempt; the RoundTrip worker closes its body.
func roundTripWithAttemptContext(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
) (*http.Response, error) {
	// Every production routed transport is an *http.Transport. Let net/http
	// enforce cancellation directly; it owns the connection lifecycle and does
	// not need an abandoned-call goroutine. Unknown adapter implementations use
	// the bounded defensive path below.
	if _, ok := transport.(*http.Transport); ok {
		return transport.RoundTrip(req)
	}
	return roundTripWithAttemptContextUsingLimiter(ctx, transport, req, defaultNonCompliantAttemptCallLimiter)
}

func roundTripWithAttemptContextUsingLimiter(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
	limiter *nonCompliantAttemptCallLimiter,
) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if limiter == nil {
		limiter = defaultNonCompliantAttemptCallLimiter
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		return transport.RoundTrip(req)
	}
	releaseSlot, acquired := limiter.tryAcquire()
	if !acquired {
		if req != nil {
			if owner, ok := req.Body.(*attemptRequestBody); ok {
				owner.abort()
			}
		}
		return nil, errNonCompliantAttemptCallLimit
	}

	call := &roundTripCall{done: make(chan struct{})}
	go func() {
		defer releaseSlot()
		resp, err := transport.RoundTrip(req)
		call.mu.Lock()
		call.finished = true
		if call.abandoned {
			call.mu.Unlock()
			closeRoundTripResponse(resp)
			return
		}
		call.result = roundTripResult{resp: resp, err: err}
		close(call.done)
		call.mu.Unlock()
	}()

	select {
	case <-call.done:
		call.mu.Lock()
		result := call.result
		call.mu.Unlock()
		return result.resp, result.err
	case <-ctx.Done():
		call.mu.Lock()
		if call.finished {
			result := call.result
			call.mu.Unlock()
			closeRoundTripResponse(result.resp)
			return nil, ctx.Err()
		}
		call.abandoned = true
		call.mu.Unlock()
		if req != nil {
			if owner, ok := req.Body.(*attemptRequestBody); ok {
				owner.abort()
			} else if aborter, ok := req.Body.(attemptBodyAborter); ok {
				aborter.abandon()
			}
		}
		if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
		return nil, ctx.Err()
	}
}

func closeRoundTripResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

// attemptBodyCompletion owns request-body accounting after a successful
// RoundTrip has already returned a response. Its final callback is driven by
// the response-body lifecycle and fires at most once. There is no second
// request-body timeout after a response has been accepted: EOF/Close either
// observes a completed owner or abandons it immediately and fails closed.
type attemptBodyCompletion struct {
	owner   *attemptRequestBody
	counter *countingReadCloser

	settleOnce   sync.Once
	finalizeOnce sync.Once

	mu              sync.Mutex
	settledComplete bool
	finalized       bool
	finalBytes      int64
	finalComplete   bool
	onFinalized     func(int64, bool)
	callbackCalled  bool
}

func newAttemptBodyCompletion(
	owner *attemptRequestBody,
	counter *countingReadCloser,
) *attemptBodyCompletion {
	return &attemptBodyCompletion{
		owner:   owner,
		counter: counter,
	}
}

func (c *attemptBodyCompletion) settle(complete bool) {
	if c == nil {
		return
	}
	c.settleOnce.Do(func() {
		if !complete {
			c.owner.abort()
		}
		c.mu.Lock()
		c.settledComplete = complete
		c.mu.Unlock()
	})
}

func (c *attemptBodyCompletion) abandon() {
	if c == nil || c.owner == nil {
		return
	}
	select {
	case <-c.owner.done:
		c.settle(true)
	default:
		c.owner.abort()
		c.settle(false)
	}
}

// handoff transfers the attempt-release and final accounting ownership to the
// response lifecycle. Releasing the pre-response attempt timer immediately is
// required once a response has been accepted; the completion object still
// owns the one-shot final accounting callback.
func (c *attemptBodyCompletion) handoff(
	releaseAttempt func(),
	onFinalized func(int64, bool),
) {
	if c == nil {
		return
	}
	if releaseAttempt != nil {
		releaseAttempt()
	}
	c.mu.Lock()
	c.onFinalized = onFinalized
	finalized := c.finalized
	c.mu.Unlock()
	if finalized {
		c.invokeFinalized()
	}
}

func (c *attemptBodyCompletion) finalize() {
	if c == nil {
		return
	}
	c.finalizeOnce.Do(func() {
		// EOF/Close is the serialization point. If the transport has not closed
		// the request body yet, abandon it immediately and fail closed; never
		// block a successful response on a second quiescence timeout.
		c.abandon()
		c.mu.Lock()
		complete := c.settledComplete
		bodyBytes := int64(0)
		if complete && c.counter != nil {
			bodyBytes = c.counter.Total()
		}
		c.finalized = true
		c.finalBytes = bodyBytes
		c.finalComplete = complete
		c.mu.Unlock()
		c.invokeFinalized()
	})
}

func (c *attemptBodyCompletion) invokeFinalized() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if !c.finalized || c.callbackCalled || c.onFinalized == nil {
		c.mu.Unlock()
		return
	}
	c.callbackCalled = true
	callback := c.onFinalized
	bodyBytes := c.finalBytes
	complete := c.finalComplete
	c.mu.Unlock()
	callback(bodyBytes, complete)
}

func (c *attemptBodyCompletion) wrapResponseBody(resp *http.Response) {
	if c == nil || resp == nil {
		return
	}
	if resp.Body == nil {
		resp.Body = http.NoBody
	}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok {
		resp.Body = &attemptResponseReadWriteBody{ReadWriteCloser: rwc, completion: c}
		return
	}
	resp.Body = &attemptResponseBody{ReadCloser: resp.Body, completion: c}
}

type attemptResponseBody struct {
	io.ReadCloser
	completion *attemptBodyCompletion
	closeOnce  sync.Once
	closeErr   error
}

func (b *attemptResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && b.completion != nil {
		b.completion.finalize()
	}
	return n, err
}

func (b *attemptResponseBody) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.ReadCloser.Close()
		if b.completion != nil {
			b.completion.finalize()
		}
	})
	return b.closeErr
}

type attemptResponseReadWriteBody struct {
	io.ReadWriteCloser
	completion *attemptBodyCompletion
	closeOnce  sync.Once
	closeErr   error
}

func (b *attemptResponseReadWriteBody) Read(p []byte) (int, error) {
	n, err := b.ReadWriteCloser.Read(p)
	if err != nil && b.completion != nil {
		b.completion.finalize()
	}
	return n, err
}

func (b *attemptResponseReadWriteBody) Write(p []byte) (int, error) {
	return b.ReadWriteCloser.Write(p)
}

func (b *attemptResponseReadWriteBody) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.ReadWriteCloser.Close()
		if b.completion != nil {
			b.completion.finalize()
		}
	})
	return b.closeErr
}

type attemptRoundTripState struct {
	complete    atomic.Bool
	headerBytes atomic.Int64
	bodyBytes   atomic.Int64
}

type bodyCompletionRoundTripper struct {
	next  http.RoundTripper
	state *attemptRoundTripState
}

func (t bodyCompletionRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	headerBytes := headerWireLen(req.Header)
	resp, err, bodyBytes, complete := roundTripWithBodyCompletion(req.Context(), t.next, req)
	deferred := responseBodyCompletion(resp)
	if t.state != nil {
		setAttemptRoundTripState(t.state, complete, headerBytes, bodyBytes)
		if deferred != nil {
			deferred.handoff(nil, func(bodyBytes int64, complete bool) {
				setAttemptRoundTripState(t.state, complete, headerBytes, bodyBytes)
			})
		}
	}
	return resp, err
}

func responseBodyCompletion(resp *http.Response) *attemptBodyCompletion {
	if resp == nil || resp.Body == nil {
		return nil
	}
	switch body := resp.Body.(type) {
	case *attemptResponseBody:
		return body.completion
	case *attemptResponseReadWriteBody:
		return body.completion
	default:
		return nil
	}
}

func setAttemptRoundTripState(state *attemptRoundTripState, complete bool, headerBytes, bodyBytes int64) {
	if state == nil {
		return
	}
	state.complete.Store(complete)
	if complete {
		state.headerBytes.Store(headerBytes)
		state.bodyBytes.Store(bodyBytes)
		return
	}
	state.headerBytes.Store(0)
	state.bodyBytes.Store(0)
}

// countingReadCloser wraps a body stream and records total read bytes.
type countingReadCloser struct {
	rc    io.ReadCloser
	total atomic.Int64
}

func newCountingReadCloser(rc io.ReadCloser) *countingReadCloser {
	return &countingReadCloser{rc: rc}
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.total.Add(int64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rc.Close()
}

func (c *countingReadCloser) abandon() {
	if c == nil {
		return
	}
	if abandoner, ok := c.rc.(attemptBodyAborter); ok {
		abandoner.abandon()
	}
}

func (c *countingReadCloser) Total() int64 {
	if c == nil {
		return 0
	}
	return c.total.Load()
}

// countingReadWriteCloser wraps a bidirectional stream and records
// bytes read/written independently.
type countingReadWriteCloser struct {
	rwc        io.ReadWriteCloser
	totalRead  int64
	totalWrite int64
}

func newCountingReadWriteCloser(rwc io.ReadWriteCloser) *countingReadWriteCloser {
	return &countingReadWriteCloser{rwc: rwc}
}

func (c *countingReadWriteCloser) Read(p []byte) (int, error) {
	n, err := c.rwc.Read(p)
	if n > 0 {
		c.totalRead += int64(n)
	}
	return n, err
}

func (c *countingReadWriteCloser) Write(p []byte) (int, error) {
	n, err := c.rwc.Write(p)
	if n > 0 {
		c.totalWrite += int64(n)
	}
	return n, err
}

func (c *countingReadWriteCloser) Close() error {
	return c.rwc.Close()
}

func (c *countingReadWriteCloser) TotalRead() int64 {
	return c.totalRead
}

func (c *countingReadWriteCloser) TotalWrite() int64 {
	return c.totalWrite
}
