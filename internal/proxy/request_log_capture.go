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
	return roundTripWithBodyCompletionBudget(ctx, transport, req, attemptBodyQuiescenceBudget)
}

func roundTripWithBodyCompletionBudget(
	ctx context.Context,
	transport http.RoundTripper,
	req *http.Request,
	budget time.Duration,
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
	resp, err := transport.RoundTrip(req)
	if owner != nil {
		if awaitErr := owner.await(ctx, budget); awaitErr != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			return nil, awaitErr, 0, false
		}
	}
	if counter == nil {
		return resp, err, 0, true
	}
	return resp, err, counter.Total(), true
}

type attemptRoundTripState struct {
	complete    bool
	headerBytes int64
	bodyBytes   int64
}

type bodyCompletionRoundTripper struct {
	next  http.RoundTripper
	state *attemptRoundTripState
}

func (t bodyCompletionRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	headerBytes := headerWireLen(req.Header)
	resp, err, bodyBytes, complete := roundTripWithBodyCompletion(req.Context(), t.next, req)
	if t.state != nil {
		t.state.complete = complete
		if complete {
			t.state.headerBytes = headerBytes
			t.state.bodyBytes = bodyBytes
		} else {
			t.state.headerBytes = 0
			t.state.bodyBytes = 0
		}
	}
	return resp, err
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
