package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Resinat/Resin/internal/routing"
)

// Request-attempt diagnostics are operational evidence, not an unbounded
// event sink. Keep both the in-memory lifecycle and persisted JSON bounded.
const (
	MaxRequestAttemptDiagnostics           = 32
	MaxRequestAttemptDiagnosticStringBytes = 128
	MaxRequestAttemptDiagnosticsJSONBytes  = 128 << 10
)

// NormalizeRequestAttemptDiagnostics removes control characters, caps all
// diagnostic strings, and retains the first and last records when the caller
// observed more than the fixed detail bound. The boolean reports discarded
// middle records; callers must expose it alongside the detail slice.
func NormalizeRequestAttemptDiagnostics(input []RequestAttemptDiagnostic) (output []RequestAttemptDiagnostic, truncated bool) {
	if len(input) == 0 {
		return nil, false
	}
	selected := input
	if len(selected) > MaxRequestAttemptDiagnostics {
		truncated = true
		selected = make([]RequestAttemptDiagnostic, 0, MaxRequestAttemptDiagnostics)
		selected = append(selected, input[:MaxRequestAttemptDiagnostics-1]...)
		selected = append(selected, input[len(input)-1])
	}
	output = make([]RequestAttemptDiagnostic, len(selected))
	for i := range selected {
		output[i] = selected[i]
		output[i].NodeHash = SanitizeRequestAttemptDiagnosticText(output[i].NodeHash)
		output[i].EgressIP = SanitizeRequestAttemptDiagnosticText(output[i].EgressIP)
		output[i].Transport = SanitizeRequestAttemptDiagnosticText(output[i].Transport)
		output[i].ErrorKind = SanitizeRequestAttemptDiagnosticText(output[i].ErrorKind)
		output[i].CancelReason = SanitizeRequestAttemptDiagnosticText(output[i].CancelReason)
		output[i].ReleaseReason = SanitizeRequestAttemptDiagnosticText(output[i].ReleaseReason)
	}
	return output, truncated
}

func SanitizeRequestAttemptDiagnosticText(value string) string {
	value = strings.ToValidUTF8(value, "")
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"authorization", "bearer ", "cookie", "api-key", "http://", "https://"} {
		if strings.Contains(lower, marker) {
			return "[redacted]"
		}
	}
	var b strings.Builder
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			continue
		}
		n := utf8.RuneLen(r)
		if n < 0 || b.Len()+n > MaxRequestAttemptDiagnosticStringBytes {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

// attemptDiagnostic is mutable only through its mutex. Transport callbacks
// and response-body finalizers can run outside the RoundTrip caller, while the
// request lifecycle must still take one immutable snapshot for persistence.
type attemptDiagnostic struct {
	mu    sync.Mutex
	base  time.Time
	value RequestAttemptDiagnostic
}

func newAttemptDiagnostic(base time.Time, index int, route routing.RouteResult, transport http.RoundTripper) *attemptDiagnostic {
	transportName := "<nil>"
	if transport != nil {
		transportName = fmt.Sprintf("%T", transport)
	}
	return &attemptDiagnostic{
		base: base,
		value: RequestAttemptDiagnostic{
			Attempt:                 index + 1,
			RouteGeneration:         route.RouteGeneration,
			PlatformRevisionNs:      route.PlatformRevisionNs,
			NodeHash:                route.NodeHash.Hex(),
			EgressIP:                route.EgressIP.String(),
			Transport:               transportName,
			RetryBudget:             route.RetryBudget,
			RequestTotalTimeoutMs:   route.RequestTotalTimeout.Milliseconds(),
			RequestAttemptTimeoutMs: route.RequestAttemptTimeout.Milliseconds(),
			MaxAttempts:             route.MaxAttempts,
		},
	}
}

func (d *attemptDiagnostic) elapsedMs() int64 {
	if d == nil || d.base.IsZero() {
		return 0
	}
	n := time.Since(d.base).Milliseconds()
	if n < 0 {
		return 0
	}
	if n == 0 {
		// Zero means that a milestone was not observed in the persisted
		// contract. Once an observed callback runs, preserve that fact even
		// when it happened within the first millisecond.
		return 1
	}
	return n
}

func (d *attemptDiagnostic) markStarted() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.value.StartedMs = d.elapsedMs()
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markGotConn() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.value.GotConnMs == 0 {
		d.value.GotConnMs = d.elapsedMs()
	}
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markWroteRequest() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.value.WroteRequestMs == 0 {
		d.value.WroteRequestMs = d.elapsedMs()
	}
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markFirstResponseByte() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.value.FirstResponseByteMs == 0 {
		d.value.FirstResponseByteMs = d.elapsedMs()
	}
	d.value.ResponseStarted = true
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markBodyStart() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.value.BodyStartMs == 0 {
		d.value.BodyStartMs = d.elapsedMs()
	}
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markRoundTripEnd() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.value.RoundTripEndMs = d.elapsedMs()
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markResponseHeader() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.value.ResponseHeaderMs = d.elapsedMs()
	d.mu.Unlock()
}

func (d *attemptDiagnostic) markBodyFinish(bytes int64, complete bool) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.value.BodyFinishMs = d.elapsedMs()
	d.value.ResponseBodyBytes = bytes
	d.value.ResponseBodyComplete = complete
	d.mu.Unlock()
}

func (d *attemptDiagnostic) setResponse(resp *http.Response) {
	if d == nil || resp == nil {
		return
	}
	d.mu.Lock()
	d.value.ResponseStatus = resp.StatusCode
	d.mu.Unlock()
}

func (d *attemptDiagnostic) setRequestBody(complete bool, bytes int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.value.RequestBodyComplete = complete
	if complete {
		d.value.RequestBodyBytes = bytes
	} else {
		d.value.RequestBodyBytes = 0
	}
	if complete {
		d.value.RequestBodyFinishMs = d.elapsedMs()
	}
	d.mu.Unlock()
}

func (d *attemptDiagnostic) setErrorKind(kind string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.value.ErrorKind = kind
	d.mu.Unlock()
}

func (d *attemptDiagnostic) setCancelReason(reason string) {
	if d == nil || reason == "" {
		return
	}
	d.mu.Lock()
	d.value.CancelReason = reason
	d.mu.Unlock()
}

func (d *attemptDiagnostic) setReleaseReason(reason string) {
	if d == nil || reason == "" {
		return
	}
	d.mu.Lock()
	d.value.ReleaseReason = reason
	d.mu.Unlock()
}

func (d *attemptDiagnostic) setAttemptDeadline(ctx context.Context) {
	if d == nil || ctx == nil {
		return
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return
	}
	d.mu.Lock()
	d.value.AttemptDeadlineMs = time.Until(deadline).Milliseconds()
	d.mu.Unlock()
}

func (d *attemptDiagnostic) snapshot() RequestAttemptDiagnostic {
	if d == nil {
		return RequestAttemptDiagnostic{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.value
}

type attemptDiagnosticResponseBody struct {
	io.ReadCloser
	diagnostic *attemptDiagnostic
	mu         sync.Mutex
	total      int64
	finished   bool
	eof        bool
	closeOnce  sync.Once
	closeErr   error
}

func (b *attemptDiagnosticResponseBody) Read(p []byte) (int, error) {
	if b == nil || b.ReadCloser == nil {
		return 0, io.EOF
	}
	if b.diagnostic != nil {
		b.diagnostic.markBodyStart()
	}
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	b.total += int64(n)
	if err == io.EOF {
		b.eof = true
	}
	b.mu.Unlock()
	if err != nil {
		b.finish(err == io.EOF)
	}
	return n, err
}

func (b *attemptDiagnosticResponseBody) Close() error {
	if b == nil || b.ReadCloser == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.closeErr = b.ReadCloser.Close()
		b.mu.Lock()
		eof := b.eof
		b.mu.Unlock()
		b.finish(eof)
	})
	return b.closeErr
}

type attemptDiagnosticResponseReadWriteBody struct {
	*attemptDiagnosticResponseBody
}

func (b *attemptDiagnosticResponseReadWriteBody) Write(p []byte) (int, error) {
	if b == nil || b.ReadCloser == nil {
		return 0, io.ErrClosedPipe
	}
	rwc, ok := b.ReadCloser.(io.ReadWriteCloser)
	if !ok {
		return 0, io.ErrClosedPipe
	}
	return rwc.Write(p)
}

func (b *attemptDiagnosticResponseBody) finish(complete bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.finished {
		b.mu.Unlock()
		return
	}
	b.finished = true
	total := b.total
	b.mu.Unlock()
	if b.diagnostic != nil {
		b.diagnostic.markBodyFinish(total, complete)
	}
}

func wrapAttemptDiagnosticResponseBody(resp *http.Response, diagnostic *attemptDiagnostic) {
	if resp == nil || resp.Body == nil || resp.Body == http.NoBody || diagnostic == nil {
		return
	}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok {
		resp.Body = &attemptDiagnosticResponseReadWriteBody{
			attemptDiagnosticResponseBody: &attemptDiagnosticResponseBody{
				ReadCloser: rwc,
				diagnostic: diagnostic,
			},
		}
		return
	}
	resp.Body = &attemptDiagnosticResponseBody{ReadCloser: resp.Body, diagnostic: diagnostic}
}
