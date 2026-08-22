package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/platform"
)

type asyncBodyRoundTripper struct {
	allowBody           chan struct{}
	returned            chan struct{}
	readDone            chan []byte
	headerBytes         chan int64
	response            *http.Response
	err                 error
	traceAfterBodyClose bool
	startOnce           sync.Once
}

func (t *asyncBodyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.headerBytes != nil {
		t.headerBytes <- headerWireLen(req.Header)
	}
	trace := httptrace.ContextClientTrace(req.Context())
	if t.traceAfterBodyClose && trace != nil {
		req.Body = &traceOnCloseBody{inner: req.Body, trace: trace}
	}
	t.startOnce.Do(func() {
		go func() {
			close(t.returned)
			<-t.allowBody
			body, err := io.ReadAll(req.Body)
			if err == nil {
				err = req.Body.Close()
			}
			if err != nil {
				body = append([]byte("read-error:"), err.Error()...)
			}
			t.readDone <- body
		}()
	})
	return t.response, t.err
}

type traceOnCloseBody struct {
	inner io.ReadCloser
	trace *httptrace.ClientTrace
}

func (b *traceOnCloseBody) Read(p []byte) (int, error) {
	return b.inner.Read(p)
}

func (b *traceOnCloseBody) Close() error {
	if b.trace != nil {
		b.trace.GotConn(httptrace.GotConnInfo{})
		b.trace.WroteRequest(httptrace.WroteRequestInfo{})
	}
	return b.inner.Close()
}

func installTestDirectTransport(t *testing.T, once *sync.Once, slot interface {
	Store(*http.Transport)
}, rt http.RoundTripper,
) {
	t.Helper()
	transport := &http.Transport{}
	transport.RegisterProtocol("http", rt)
	once.Do(func() { slot.Store(transport) })
	t.Cleanup(transport.CloseIdleConnections)
}

func waitForChannel[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

type gatedResponseBody struct {
	allow  <-chan struct{}
	reader *strings.Reader
}

func (b *gatedResponseBody) Read(p []byte) (int, error) {
	<-b.allow
	return b.reader.Read(p)
}

func (b *gatedResponseBody) Close() error { return nil }

type releaseAndWaitResponseBody struct {
	release   chan<- struct{}
	completed <-chan struct{}
	reader    *strings.Reader
	once      sync.Once
}

func (b *releaseAndWaitResponseBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.release) })
	<-b.completed
	return b.reader.Read(p)
}

func (b *releaseAndWaitResponseBody) Close() error { return nil }

type closeSignalReadCloser struct {
	io.ReadCloser
	closed    chan struct{}
	closeOnce sync.Once
}

func (b *closeSignalReadCloser) Close() error {
	err := b.ReadCloser.Close()
	b.closeOnce.Do(func() { close(b.closed) })
	return err
}

type closeCountingResponseBody struct {
	reader *strings.Reader
	closes atomic.Int32
}

func (b *closeCountingResponseBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *closeCountingResponseBody) Close() error {
	b.closes.Add(1)
	return nil
}

func TestForwardProxyDirectBodyCompletionUsesSharedOwner(t *testing.T) {
	allowBody := make(chan struct{})
	allowResponse := make(chan struct{})
	rt := &asyncBodyRoundTripper{
		allowBody:   allowBody,
		returned:    make(chan struct{}),
		readDone:    make(chan []byte, 1),
		headerBytes: make(chan int64, 1),
		response: &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &gatedResponseBody{allow: allowResponse, reader: strings.NewReader("forward-ok")},
		},
	}
	emitter := newMockEventEmitter()
	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken:       "token",
		Events:           emitter,
		ProxyBypassRules: []string{"example.com"},
	})
	installTestDirectTransport(t, &fp.directOnce, &fp.directTransport, rt)

	request := httptest.NewRequest(http.MethodPost, "http://example.com/upload", strings.NewReader("forward-body"))
	request.Header.Set("Proxy-Authorization", basicAuth("plat", "token"))
	request.Header.Set("X-Test", "forward")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		fp.ServeHTTP(response, request)
		close(done)
	}()

	waitForChannel(t, rt.returned, "forward RoundTrip return")
	select {
	case <-done:
		t.Fatal("forward returned before the response body was released")
	default:
	}
	close(allowBody)
	if got := string(waitForChannel(t, rt.readDone, "forward body read")); got != "forward-body" {
		t.Fatalf("forward body: got %q, want %q", got, "forward-body")
	}
	close(allowResponse)
	waitForChannel(t, done, "forward handler")
	if response.Code != http.StatusOK || response.Body.String() != "forward-ok" {
		t.Fatalf("forward response: status=%d body=%q", response.Code, response.Body.String())
	}
	log := waitForChannel(t, emitter.logCh, "forward request log")
	wantHeaderBytes := waitForChannel(t, rt.headerBytes, "forward header accounting")
	if log.EgressBytes != wantHeaderBytes+int64(len("forward-body")) {
		t.Fatalf("forward egress bytes: got %d, want exactly %d", log.EgressBytes, wantHeaderBytes+int64(len("forward-body")))
	}
	select {
	case <-emitter.logCh:
		t.Fatal("forward emitted duplicate request log")
	default:
	}
}

func TestReverseProxyDirectBodyCompletionUsesSharedOwner(t *testing.T) {
	allowBody := make(chan struct{})
	allowResponse := make(chan struct{})
	rt := &asyncBodyRoundTripper{
		allowBody:   allowBody,
		returned:    make(chan struct{}),
		readDone:    make(chan []byte, 1),
		headerBytes: make(chan int64, 1),
		response: &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &gatedResponseBody{allow: allowResponse, reader: strings.NewReader("reverse-ok")},
		},
	}
	emitter := newMockEventEmitter()
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:       "token",
		Events:           emitter,
		ProxyBypassRules: []string{"example.com"},
	})
	installTestDirectTransport(t, &rp.directOnce, &rp.directTransport, rt)

	request := httptest.NewRequest(
		http.MethodPost,
		"/token/plat:acct/http/example.com/upload",
		strings.NewReader("reverse-body"),
	)
	request.Header.Set("X-Test", "reverse")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		rp.ServeHTTP(response, request)
		close(done)
	}()

	waitForChannel(t, rt.returned, "reverse RoundTrip return")
	select {
	case <-done:
		t.Fatal("reverse returned before the response body was released")
	default:
	}
	close(allowBody)
	if got := string(waitForChannel(t, rt.readDone, "reverse body read")); got != "reverse-body" {
		t.Fatalf("reverse body: got %q, want %q", got, "reverse-body")
	}
	close(allowResponse)
	waitForChannel(t, done, "reverse handler")
	if response.Code != http.StatusOK || response.Body.String() != "reverse-ok" {
		t.Fatalf("reverse response: status=%d body=%q", response.Code, response.Body.String())
	}
	log := waitForChannel(t, emitter.logCh, "reverse request log")
	wantHeaderBytes := waitForChannel(t, rt.headerBytes, "reverse header accounting")
	if log.EgressBytes != wantHeaderBytes+int64(len("reverse-body")) {
		t.Fatalf("reverse egress bytes: got %d, want exactly %d", log.EgressBytes, wantHeaderBytes+int64(len("reverse-body")))
	}
	select {
	case <-emitter.logCh:
		t.Fatal("reverse emitted duplicate request log")
	default:
	}
}

func TestDirectProxyBodyCompletionWaitsForAsyncError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *asyncBodyRoundTripper) (http.Handler, *http.Request, *mockEventEmitter)
	}{
		{
			name: "forward",
			setup: func(t *testing.T, rt *asyncBodyRoundTripper) (http.Handler, *http.Request, *mockEventEmitter) {
				emitter := newMockEventEmitter()
				fp := NewForwardProxy(ForwardProxyConfig{
					ProxyToken:       "token",
					Events:           emitter,
					ProxyBypassRules: []string{"example.com"},
				})
				installTestDirectTransport(t, &fp.directOnce, &fp.directTransport, rt)
				request := httptest.NewRequest(http.MethodPost, "http://example.com/upload", strings.NewReader("forward-error-body"))
				request.Header.Set("Proxy-Authorization", basicAuth("plat", "token"))
				request.Header.Set("X-Test", "forward-error")
				return fp, request, emitter
			},
		},
		{
			name: "reverse",
			setup: func(t *testing.T, rt *asyncBodyRoundTripper) (http.Handler, *http.Request, *mockEventEmitter) {
				emitter := newMockEventEmitter()
				rp := NewReverseProxy(ReverseProxyConfig{
					ProxyToken:       "token",
					Events:           emitter,
					ProxyBypassRules: []string{"example.com"},
				})
				installTestDirectTransport(t, &rp.directOnce, &rp.directTransport, rt)
				request := httptest.NewRequest(
					http.MethodPost,
					"/token/plat:acct/http/example.com/upload",
					strings.NewReader("reverse-error-body"),
				)
				request.Header.Set("X-Test", "reverse-error")
				return rp, request, emitter
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowBody := make(chan struct{})
			rt := &asyncBodyRoundTripper{
				allowBody:           allowBody,
				returned:            make(chan struct{}),
				readDone:            make(chan []byte, 1),
				headerBytes:         make(chan int64, 1),
				err:                 errors.New("async transport error"),
				traceAfterBodyClose: true,
			}
			handler, request, emitter := tc.setup(t, rt)
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				handler.ServeHTTP(response, request)
				close(done)
			}()

			waitForChannel(t, rt.returned, "error RoundTrip return")
			select {
			case <-done:
				t.Fatal("direct handler returned before the asynchronous body completed")
			default:
			}
			close(allowBody)
			body := string(waitForChannel(t, rt.readDone, "error request body read"))
			if !strings.HasSuffix(body, "error-body") {
				t.Fatalf("request body: got %q", body)
			}
			waitForChannel(t, done, "direct error handler")
			if response.Code == http.StatusOK {
				t.Fatalf("direct error unexpectedly returned 200: body=%q", response.Body.String())
			}
			log := waitForChannel(t, emitter.logCh, "direct error request log")
			bodyLen := int64(len(body))
			wantHeaderBytes := waitForChannel(t, rt.headerBytes, "direct error header accounting")
			if log.EgressBytes != bodyLen+wantHeaderBytes {
				t.Fatalf("direct error egress bytes: got %d, want exactly %d", log.EgressBytes, bodyLen+wantHeaderBytes)
			}
			select {
			case <-emitter.logCh:
				t.Fatal("direct error emitted duplicate request log")
			default:
			}
		})
	}
}

func TestReverseRetryResponseRuleReplayableBodyUsesDistinctNext(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusTooManyRequests},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "rate-limited"},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("compile response rule: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/retry")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RetryBudget = 2

	const payload = `{"stream":true}`
	var calls atomic.Int32
	attemptedIPs := make([]string, 0, 2)
	commits := make(chan int64, 2)
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
		},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempt := int(calls.Add(1))
				attemptedIPs = append(attemptedIPs, candidate.Route.EgressIP.String())
				body, readErr := io.ReadAll(req.Body)
				closeErr := req.Body.Close()
				if readErr != nil || closeErr != nil || string(body) != payload {
					t.Fatalf("attempt %d body completion: body=%q read=%v close=%v", attempt, body, readErr, closeErr)
				}
				trace := httptrace.ContextClientTrace(req.Context())
				if trace != nil {
					trace.GotConn(httptrace.GotConnInfo{})
					trace.WroteRequest(httptrace.WroteRequestInfo{})
				}
				status := http.StatusOK
				responseBody := "ok"
				if attempt == 1 {
					status = http.StatusTooManyRequests
					responseBody = "rate-limited"
				}
				return &http.Response{
					StatusCode: status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(responseBody)),
					Request:    req,
				}, nil
			})
		},
		onAttemptEgress: func(_, bodyBytes int64) { commits <- bodyBytes },
	}

	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodPost, "https://example.com/retry", strings.NewReader(payload)))
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("replayable 429 retry: resp=%#v err=%v", resp, err)
	}
	_ = resp.Body.Close()
	if got := calls.Load(); got != 2 {
		t.Fatalf("retry attempts: got %d, want 2", got)
	}
	if len(attemptedIPs) != 2 || attemptedIPs[0] == attemptedIPs[1] {
		t.Fatalf("retry did not use distinct egress IPs: %v", attemptedIPs)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if got := waitForChannel(t, commits, "replayable 429 egress commit"); got != int64(len(payload)) {
			t.Fatalf("attempt %d egress body bytes: got %d, want %d", attempt+1, got, len(payload))
		}
	}
	select {
	case got := <-commits:
		t.Fatalf("duplicate egress commit: %d", got)
	default:
	}
}

func TestReverseRetryAsyncBodyCompletionEachAttemptCommitsOnce(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusTooManyRequests},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "rate-limited"},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("compile response rule: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "async-body", "https://example.com/retry")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RetryBudget = 2

	const payload = `{"stream":true}`
	type bodyObservation struct {
		body     []byte
		readErr  error
		closeErr error
	}
	allowFirstBody := make(chan struct{})
	firstBodyDone := make(chan struct{})
	bodyObservations := make(chan bodyObservation, 2)
	var calls atomic.Int32
	attemptedIPs := make([]string, 0, 2)
	commits := make(chan int64, 2)
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
		},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempt := int(calls.Add(1))
				attemptedIPs = append(attemptedIPs, candidate.Route.EgressIP.String())
				trace := httptrace.ContextClientTrace(req.Context())
				if trace != nil {
					trace.GotConn(httptrace.GotConnInfo{})
					trace.WroteRequest(httptrace.WroteRequestInfo{})
				}
				if attempt == 1 {
					if req.GetBody != nil {
						t.Fatalf("async first attempt unexpectedly had GetBody")
					}
					go func() {
						<-allowFirstBody
						body, readErr := io.ReadAll(req.Body)
						closeErr := req.Body.Close()
						bodyObservations <- bodyObservation{body: body, readErr: readErr, closeErr: closeErr}
						close(firstBodyDone)
					}()
					return &http.Response{
						StatusCode: http.StatusTooManyRequests,
						Header:     make(http.Header),
						Body: &releaseAndWaitResponseBody{
							release:   allowFirstBody,
							completed: firstBodyDone,
							reader:    strings.NewReader("rate-limited"),
						},
						Request: req,
					}, nil
				}

				body, readErr := io.ReadAll(req.Body)
				closeErr := req.Body.Close()
				bodyObservations <- bodyObservation{body: body, readErr: readErr, closeErr: closeErr}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
		onAttemptEgress: func(_, bodyBytes int64) { commits <- bodyBytes },
	}

	request := httptest.NewRequest(http.MethodPost, "https://example.com/retry", nil)
	request.Body = io.NopCloser(strings.NewReader(payload))
	request.GetBody = nil
	request.ContentLength = int64(len(payload))
	resp, err := retry.RoundTrip(request)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("async replayable 429 retry: resp=%#v err=%v attempts=%d", resp, err, calls.Load())
	}
	if body, readErr := io.ReadAll(resp.Body); readErr != nil || string(body) != "ok" {
		t.Fatalf("async final response: body=%q err=%v", body, readErr)
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Fatalf("close async final response: %v", closeErr)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("async retry attempts: got %d, want 2", got)
	}
	if len(attemptedIPs) != 2 || attemptedIPs[0] == attemptedIPs[1] {
		t.Fatalf("async retry did not use distinct egress IPs: %v", attemptedIPs)
	}
	for attempt := 0; attempt < 2; attempt++ {
		observation := waitForChannel(t, bodyObservations, "async request body completion")
		if observation.readErr != nil || observation.closeErr != nil || string(observation.body) != payload {
			t.Fatalf("attempt %d body completion: body=%q read=%v close=%v", attempt+1, observation.body, observation.readErr, observation.closeErr)
		}
		if got := waitForChannel(t, commits, "async egress commit"); got != int64(len(payload)) {
			t.Fatalf("attempt %d egress body bytes: got %d, want %d", attempt+1, got, len(payload))
		}
	}
	select {
	case got := <-commits:
		t.Fatalf("duplicate async egress commit: %d", got)
	default:
	}
}

func TestRoundTripWithBodyCompletionWaitsBeforeReturningTransportError(t *testing.T) {
	allowBody := make(chan struct{})
	rt := &asyncBodyRoundTripper{
		allowBody: allowBody,
		returned:  make(chan struct{}),
		readDone:  make(chan []byte, 1),
		err:       errors.New("upstream response read failed"),
	}
	request := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("error-body"))
	result := make(chan struct {
		err   error
		bytes int64
	}, 1)
	go func() {
		_, err, bytes, complete := roundTripWithBodyCompletionBudget(
			request.Context(), rt, request, time.Second,
		)
		if !complete {
			result <- struct {
				err   error
				bytes int64
			}{err: err}
			return
		}
		result <- struct {
			err   error
			bytes int64
		}{err: err, bytes: bytes}
	}()

	waitForChannel(t, rt.returned, "error RoundTrip return")
	select {
	case <-result:
		t.Fatal("transport error returned before body completion")
	default:
	}
	close(allowBody)
	if got := string(waitForChannel(t, rt.readDone, "error body read")); got != "error-body" {
		t.Fatalf("error body: got %q, want %q", got, "error-body")
	}
	outcome := waitForChannel(t, result, "transport error")
	if !errors.Is(outcome.err, rt.err) || outcome.bytes != int64(len("error-body")) {
		t.Fatalf("outcome: err=%v bytes=%d", outcome.err, outcome.bytes)
	}
}

type completionTrackingBody struct {
	closed    chan struct{}
	closeOnce sync.Once
}

type neverCloseRequestRoundTripper struct {
	returned     chan struct{}
	responseBody *completionTrackingBody
	once         sync.Once
}

func (t *neverCloseRequestRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	t.once.Do(func() { close(t.returned) })
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       t.responseBody,
	}, nil
}

func (b *completionTrackingBody) Read([]byte) (int, error) { return 0, io.EOF }

func (b *completionTrackingBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestRoundTripWithBodyCompletionAcceptsResponseBeforeRequestBodyClose(t *testing.T) {
	responseBody := &completionTrackingBody{closed: make(chan struct{})}
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: responseBody}, nil
	})
	request := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("never-close"))
	resp, err, bytes, complete := roundTripWithBodyCompletionBudget(
		request.Context(), rt, request, 25*time.Millisecond,
	)
	if complete || err != nil || resp == nil || bytes != 0 {
		t.Fatalf("outcome: resp=%#v err=%v bytes=%d complete=%v", resp, err, bytes, complete)
	}
	if _, readErr := io.ReadAll(resp.Body); readErr != nil {
		t.Fatalf("read accepted response: %v", readErr)
	}
	_ = resp.Body.Close()
	waitForChannel(t, responseBody.closed, "response body close")
}

func TestRoundTripWithBodyCompletionReturnsStartedResponseBeforeRequestBodyClose(t *testing.T) {
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("started-before-request-close")),
		}, nil
	})
	request := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("request-body"))
	started := time.Now()
	resp, err, bytes, complete := roundTripWithBodyCompletionBudget(
		request.Context(), rt, request, 250*time.Millisecond,
	)
	if err != nil || resp == nil || complete || bytes != 0 {
		t.Fatalf("outcome: resp=%#v err=%v bytes=%d complete=%v", resp, err, bytes, complete)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("response was blocked on request-body close for %v", elapsed)
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		t.Fatalf("read started response: %v", readErr)
	}
	if string(body) != "started-before-request-close" {
		t.Fatalf("response body: got %q", body)
	}
	_ = resp.Body.Close()
}

func TestAttemptBodyCompletionDoesNotAbortAcceptedResponseAfterQuiescenceBudget(t *testing.T) {
	requestBody := &completionTrackingBody{closed: make(chan struct{})}
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("still-streaming")),
		}, nil
	})
	request := httptest.NewRequest(http.MethodPost, "http://example.com", requestBody)
	resp, err, _, complete := roundTripWithBodyCompletionBudget(
		request.Context(), rt, request, 25*time.Millisecond,
	)
	if err != nil || resp == nil || complete {
		t.Fatalf("outcome: resp=%#v err=%v complete=%v", resp, err, complete)
	}
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-requestBody.closed:
		t.Fatal("accepted response completion aborted the request body on the old budget")
	case <-timer.C:
	}
	if _, readErr := io.ReadAll(resp.Body); readErr != nil {
		t.Fatalf("read accepted response after budget: %v", readErr)
	}
	_ = resp.Body.Close()
	waitForChannel(t, requestBody.closed, "request body close after response close")
}

func TestAttemptBodyCompletionCloseFinalizesWithoutWaitingAndOnlyOnce(t *testing.T) {
	requestBody := &completionTrackingBody{closed: make(chan struct{})}
	responseUnderlying := &closeCountingResponseBody{reader: strings.NewReader("response")}
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       responseUnderlying,
		}, nil
	})
	request := httptest.NewRequest(http.MethodPost, "http://example.com", requestBody)
	resp, err, _, complete := roundTripWithBodyCompletionBudget(
		request.Context(), rt, request, 250*time.Millisecond,
	)
	if err != nil || resp == nil || complete {
		t.Fatalf("outcome: resp=%#v err=%v complete=%v", resp, err, complete)
	}
	completion := responseBodyCompletion(resp)
	if completion == nil {
		t.Fatal("accepted response has no body completion")
	}
	finalized := make(chan struct{}, 2)
	var callbackCount atomic.Int32
	var finalBytes atomic.Int64
	var finalComplete atomic.Bool
	completion.handoff(nil, func(bodyBytes int64, bodyComplete bool) {
		callbackCount.Add(1)
		finalBytes.Store(bodyBytes)
		finalComplete.Store(bodyComplete)
		finalized <- struct{}{}
	})
	started := time.Now()
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Fatalf("close response: %v", closeErr)
	}
	if elapsed := time.Since(started); elapsed >= 100*time.Millisecond {
		t.Fatalf("response Close waited for request-body quiescence: %v", elapsed)
	}
	waitForChannel(t, finalized, "body completion callback")
	if got := callbackCount.Load(); got != 1 {
		t.Fatalf("callback count after Close: got %d, want 1", got)
	}
	if got := responseUnderlying.closes.Load(); got != 1 {
		t.Fatalf("underlying response Close count: got %d, want 1", got)
	}
	if finalComplete.Load() || finalBytes.Load() != 0 {
		t.Fatalf("incomplete request body was committed: complete=%v bytes=%d", finalComplete.Load(), finalBytes.Load())
	}
	_ = resp.Body.Close()
	select {
	case <-finalized:
		t.Fatal("body completion callback fired more than once")
	default:
	}
	if got := responseUnderlying.closes.Load(); got != 1 {
		t.Fatalf("underlying response Close count after repeated Close: got %d, want 1", got)
	}
	waitForChannel(t, requestBody.closed, "abandoned request body close")
}

func TestAttemptBodyCompletionCancellationWaitsForResponseLifecycleToFinalize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	requestBody := &completionTrackingBody{closed: make(chan struct{})}
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("response")),
		}, nil
	})
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://example.com", requestBody)
	resp, err, _, complete := roundTripWithBodyCompletionBudget(ctx, rt, request, time.Second)
	if err != nil || resp == nil || complete {
		t.Fatalf("outcome: resp=%#v err=%v complete=%v", resp, err, complete)
	}
	completion := responseBodyCompletion(resp)
	if completion == nil {
		t.Fatal("accepted response has no body completion")
	}
	finalized := make(chan struct{}, 1)
	completion.handoff(nil, func(bodyBytes int64, bodyComplete bool) {
		if bodyBytes != 0 || bodyComplete {
			t.Errorf("canceled request body committed: complete=%v bytes=%d", bodyComplete, bodyBytes)
		}
		finalized <- struct{}{}
	})
	cancel()
	select {
	case <-finalized:
		t.Fatal("context cancellation finalized lifecycle asynchronously")
	case <-time.After(50 * time.Millisecond):
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Fatalf("close canceled response: %v", closeErr)
	}
	waitForChannel(t, finalized, "canceled body completion callback")
	waitForChannel(t, requestBody.closed, "canceled request body close")
}

func TestReverseRetryStartedResponseDoesNotRetryBeforeRequestBodyCompletion(t *testing.T) {
	env := newProxyE2EEnv(t)
	initial, err := env.router.RouteRequest("plat", "started-response", "https://example.com/started")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	// This test is a lifecycle canary, not a response-rule retry test. Disable
	// the request-level capture budget so the attempt owner wraps requestBody
	// itself and response Close is the only abandonment path under test.
	initial.RequestTotalTimeout = 0

	allowRequestBody := make(chan struct{})
	allowResponseBody := make(chan struct{})
	requestBodyClosed := make(chan struct{})
	requestBody := &closeSignalReadCloser{
		ReadCloser: io.NopCloser(strings.NewReader("request-body")),
		closed:     requestBodyClosed,
	}
	requestBodyRead := make(chan string, 1)
	commits := make(chan int64, 1)
	returned := make(chan struct{})
	var calls atomic.Int32
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
		},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				trace := httptrace.ContextClientTrace(req.Context())
				if trace != nil {
					trace.GotConn(httptrace.GotConnInfo{})
					trace.WroteRequest(httptrace.WroteRequestInfo{})
				}
				go func() {
					<-allowRequestBody
					body, readErr := io.ReadAll(req.Body)
					if readErr != nil {
						t.Errorf("request body read: %v", readErr)
					}
					requestBodyRead <- string(body)
				}()
				close(returned)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       &gatedResponseBody{allow: allowResponseBody, reader: strings.NewReader("first-response")},
				}, nil
			})
		},
		onAttemptEgress: func(_, bodyBytes int64) { commits <- bodyBytes },
	}

	result := make(chan struct {
		resp *http.Response
		err  error
	}, 1)
	go func() {
		resp, roundTripErr := retry.RoundTrip(httptest.NewRequest(
			http.MethodPost,
			"https://example.com/started",
			requestBody,
		))
		result <- struct {
			resp *http.Response
			err  error
		}{resp: resp, err: roundTripErr}
	}()

	waitForChannel(t, returned, "started response")
	outcome := waitForChannel(t, result, "started response result")
	if outcome.err != nil || outcome.resp == nil || outcome.resp.StatusCode != http.StatusOK {
		t.Fatalf("started response result: resp=%#v err=%v", outcome.resp, outcome.err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("started response retried: got %d attempts, want 1", got)
	}

	oldBudget := time.NewTimer(attemptBodyQuiescenceBudget + 50*time.Millisecond)
	defer oldBudget.Stop()
	select {
	case <-requestBodyClosed:
		t.Fatal("accepted response inherited the old request-body quiescence timeout")
	case <-oldBudget.C:
	}

	close(allowResponseBody)
	body, readErr := io.ReadAll(outcome.resp.Body)
	if readErr != nil || string(body) != "first-response" {
		t.Fatalf("started response body: body=%q err=%v", body, readErr)
	}
	if closeErr := outcome.resp.Body.Close(); closeErr != nil {
		t.Fatalf("close started response: %v", closeErr)
	}
	waitForChannel(t, requestBodyClosed, "aborted request body close")
	close(allowRequestBody)
	if got := waitForChannel(t, requestBodyRead, "aborted request body read"); got != "" {
		t.Fatalf("aborted request body was replayed: %q", got)
	}
	select {
	case got := <-commits:
		t.Fatalf("incomplete started response committed egress: %d", got)
	default:
	}
}

func TestBodyCompletionCancellationFailsClosedForAllRealCallers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *neverCloseRequestRoundTripper, context.Context) (http.Handler, *http.Request, *mockEventEmitter)
	}{
		{
			name: "forward direct",
			setup: func(t *testing.T, rt *neverCloseRequestRoundTripper, ctx context.Context) (http.Handler, *http.Request, *mockEventEmitter) {
				emitter := newMockEventEmitter()
				fp := NewForwardProxy(ForwardProxyConfig{
					ProxyToken:       "token",
					Events:           emitter,
					ProxyBypassRules: []string{"example.com"},
				})
				installTestDirectTransport(t, &fp.directOnce, &fp.directTransport, rt)
				request := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://example.com/upload", strings.NewReader("body"))
				request.Header.Set("Proxy-Authorization", basicAuth("plat", "token"))
				return fp, request, emitter
			},
		},
		{
			name: "reverse direct",
			setup: func(t *testing.T, rt *neverCloseRequestRoundTripper, ctx context.Context) (http.Handler, *http.Request, *mockEventEmitter) {
				emitter := newMockEventEmitter()
				rp := NewReverseProxy(ReverseProxyConfig{
					ProxyToken:       "token",
					Events:           emitter,
					ProxyBypassRules: []string{"example.com"},
				})
				installTestDirectTransport(t, &rp.directOnce, &rp.directTransport, rt)
				return rp, httptest.NewRequestWithContext(ctx, http.MethodPost, "/token/plat:acct/http/example.com/upload", strings.NewReader("body")), emitter
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			responseBody := &completionTrackingBody{closed: make(chan struct{})}
			rt := &neverCloseRequestRoundTripper{
				returned:     make(chan struct{}),
				responseBody: responseBody,
			}
			handler, request, emitter := tc.setup(t, rt, ctx)
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				handler.ServeHTTP(response, request)
				close(done)
			}()
			waitForChannel(t, rt.returned, "never-close RoundTrip return")
			cancel()
			waitForChannel(t, done, "canceled direct handler")
			waitForChannel(t, responseBody.closed, "canceled response body close")
			log := waitForChannel(t, emitter.logCh, "canceled direct request log")
			if log.EgressBytes != 0 {
				t.Fatalf("canceled handler committed egress bytes: %d", log.EgressBytes)
			}
			select {
			case <-emitter.logCh:
				t.Fatal("canceled handler emitted duplicate request log")
			default:
			}
		})
	}
}

func TestBodyCompletionCancellationFailsClosedForReverseRetryCaller(t *testing.T) {
	env := newProxyE2EEnv(t)
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/retry")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RetryBudget = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responseBody := &completionTrackingBody{closed: make(chan struct{})}
	rt := &neverCloseRequestRoundTripper{
		returned:     make(chan struct{}),
		responseBody: responseBody,
	}
	commits := make(chan struct{}, 1)
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := newUpstreamRequestTrace().newAttempt()
			trace := httptrace.WithClientTrace(req.Context(), attempt.clientTrace())
			return req.WithContext(trace), attempt
		},
		transportFor:    func(routedOutbound) http.RoundTripper { return rt },
		onAttemptEgress: func(int64, int64) { commits <- struct{}{} },
	}

	result := make(chan struct {
		resp *http.Response
		err  error
	}, 1)
	go func() {
		request := httptest.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/retry", strings.NewReader("body"))
		resp, roundTripErr := retry.RoundTrip(request)
		result <- struct {
			resp *http.Response
			err  error
		}{resp: resp, err: roundTripErr}
	}()
	waitForChannel(t, rt.returned, "never-close retry RoundTrip return")
	cancel()
	outcome := waitForChannel(t, result, "accepted reverse retry response")
	if outcome.resp == nil || outcome.err != nil {
		t.Fatalf("retry outcome: resp=%#v err=%v", outcome.resp, outcome.err)
	}
	if closeErr := outcome.resp.Body.Close(); closeErr != nil {
		t.Fatalf("close accepted retry response: %v", closeErr)
	}
	waitForChannel(t, responseBody.closed, "canceled retry response body close")
	select {
	case <-commits:
		t.Fatal("canceled retry committed partial egress")
	default:
	}
}

type cancellationBody struct {
	readStarted chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
}

func (b *cancellationBody) Read([]byte) (int, error) {
	select {
	case <-b.readStarted:
	default:
		close(b.readStarted)
	}
	<-b.closed
	return 0, io.EOF
}

func (b *cancellationBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestRoundTripWithBodyCompletionCancellationDoesNotPublishCapture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &cancellationBody{
		readStarted: make(chan struct{}),
		closed:      make(chan struct{}),
	}
	capture := newPayloadCaptureReadCloser(source, -1)
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://example.com", capture)
	emitter := newMockEventEmitter()
	lifecycle := newRequestLifecycle(emitter, request, ProxyTypeReverse, false)
	lifecycle.setReqBodyCapture(capture)
	readDone := make(chan struct{})
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		go func() {
			_, _ = io.ReadAll(req.Body)
			close(readDone)
		}()
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})

	result := make(chan struct {
		resp     *http.Response
		complete bool
	}, 1)
	go func() {
		resp, _, _, complete := roundTripWithBodyCompletionBudget(ctx, rt, request, time.Second)
		result <- struct {
			resp     *http.Response
			complete bool
		}{resp: resp, complete: complete}
	}()
	waitForChannel(t, source.readStarted, "blocked request read")
	cancel()
	outcome := waitForChannel(t, result, "canceled attempt")
	if outcome.complete {
		t.Fatal("canceled attempt reported a completed body")
	}
	if outcome.resp != nil {
		if closeErr := outcome.resp.Body.Close(); closeErr != nil {
			t.Fatalf("close canceled response: %v", closeErr)
		}
	}
	waitForChannel(t, readDone, "blocked read exit")
	lifecycle.finish()
	log := waitForChannel(t, emitter.logCh, "canceled request log")
	if log.ReqBody != nil || log.ReqBodyLen != 0 || log.ReqBodyTruncated {
		t.Fatalf("canceled request log contains partial body: len=%d truncated=%v body=%q", log.ReqBodyLen, log.ReqBodyTruncated, log.ReqBody)
	}
	if payload, total, truncated, ok := capture.Snapshot(); ok || payload != nil || total != 0 || truncated {
		t.Fatalf("abandoned capture was published: payload=%q total=%d truncated=%v ok=%v", payload, total, truncated, ok)
	}
}
