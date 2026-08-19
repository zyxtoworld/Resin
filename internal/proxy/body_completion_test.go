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

func TestForwardProxyDirectBodyCompletionUsesSharedOwner(t *testing.T) {
	allowBody := make(chan struct{})
	rt := &asyncBodyRoundTripper{
		allowBody:   allowBody,
		returned:    make(chan struct{}),
		readDone:    make(chan []byte, 1),
		headerBytes: make(chan int64, 1),
		response: &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("forward-ok")),
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
		t.Fatal("forward returned before the asynchronous body completed")
	default:
	}
	close(allowBody)
	if got := string(waitForChannel(t, rt.readDone, "forward body read")); got != "forward-body" {
		t.Fatalf("forward body: got %q, want %q", got, "forward-body")
	}
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
	rt := &asyncBodyRoundTripper{
		allowBody:   allowBody,
		returned:    make(chan struct{}),
		readDone:    make(chan []byte, 1),
		headerBytes: make(chan int64, 1),
		response: &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("reverse-ok")),
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
		t.Fatal("reverse returned before the asynchronous body completed")
	default:
	}
	close(allowBody)
	if got := string(waitForChannel(t, rt.readDone, "reverse body read")); got != "reverse-body" {
		t.Fatalf("reverse body: got %q, want %q", got, "reverse-body")
	}
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

func TestReverseRetryAsyncBodyCompletionEachAttemptCommitsOnce(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusBadGateway},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "retryable"},
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
	allowBody := []chan struct{}{make(chan struct{}), make(chan struct{})}
	returned := []chan struct{}{make(chan struct{}), make(chan struct{})}
	type bodyReadResult struct {
		attempt int
		body    string
	}
	bodyRead := make(chan bodyReadResult, 2)
	commits := make(chan int64, 2)
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
		transportFor: func(_ routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				trace := httptrace.ContextClientTrace(req.Context())
				attempt := int(calls.Add(1)) - 1
				if attempt >= len(allowBody) {
					t.Fatalf("unexpected retry attempt %d", attempt+1)
				}
				go func() {
					close(returned[attempt])
					<-allowBody[attempt]
					body, readErr := io.ReadAll(req.Body)
					closeErr := req.Body.Close()
					if readErr != nil || closeErr != nil {
						t.Errorf("attempt %d body completion: read=%v close=%v", attempt+1, readErr, closeErr)
					}
					if trace != nil {
						trace.GotConn(httptrace.GotConnInfo{})
						trace.WroteRequest(httptrace.WroteRequestInfo{})
					}
					bodyRead <- bodyReadResult{attempt: attempt, body: string(body)}
				}()
				status := http.StatusOK
				responseBody := "ok"
				if attempt == 0 {
					status = http.StatusBadGateway
					responseBody = "retryable"
				}
				return &http.Response{
					StatusCode: status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(responseBody)),
				}, nil
			})
		},
		onAttemptEgress: func(_, bodyBytes int64) {
			commits <- bodyBytes
		},
	}

	result := make(chan struct {
		resp *http.Response
		err  error
	}, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPost, "https://example.com/retry", strings.NewReader(payload))
		resp, roundTripErr := retry.RoundTrip(request)
		result <- struct {
			resp *http.Response
			err  error
		}{resp: resp, err: roundTripErr}
	}()

	waitForChannel(t, returned[0], "first retry RoundTrip return")
	select {
	case <-commits:
		t.Fatal("first retry committed before body completion")
	default:
	}
	close(allowBody[0])
	firstBody := waitForChannel(t, bodyRead, "first retry body")
	firstCommit := waitForChannel(t, commits, "first retry commit")
	if firstBody.attempt != 0 || firstBody.body != payload || firstCommit != int64(len(payload)) {
		t.Fatalf("first retry accounting: body=%+v commit=%d", firstBody, firstCommit)
	}

	waitForChannel(t, returned[1], "second retry RoundTrip return")
	select {
	case <-result:
		t.Fatal("retry completed before second body completion")
	default:
	}
	close(allowBody[1])
	secondBody := waitForChannel(t, bodyRead, "second retry body")
	secondCommit := waitForChannel(t, commits, "second retry commit")
	if secondBody.attempt != 1 || secondBody.body != payload || secondCommit != int64(len(payload)) {
		t.Fatalf("second retry accounting: body=%+v commit=%d", secondBody, secondCommit)
	}

	outcome := waitForChannel(t, result, "final retry response")
	if outcome.err != nil || outcome.resp == nil || outcome.resp.StatusCode != http.StatusOK {
		t.Fatalf("final retry outcome: resp=%#v err=%v", outcome.resp, outcome.err)
	}
	_ = outcome.resp.Body.Close()
	if got := calls.Load(); got != 2 {
		t.Fatalf("retry attempts: got %d, want 2", got)
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

func TestRoundTripWithBodyCompletionNeverCloseFailsClosed(t *testing.T) {
	responseBody := &completionTrackingBody{closed: make(chan struct{})}
	rt := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: responseBody}, nil
	})
	request := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("never-close"))
	resp, err, bytes, complete := roundTripWithBodyCompletionBudget(
		request.Context(), rt, request, 25*time.Millisecond,
	)
	if complete || !errors.Is(err, errAttemptBodyQuiescence) || resp != nil || bytes != 0 {
		t.Fatalf("outcome: resp=%#v err=%v bytes=%d complete=%v", resp, err, bytes, complete)
	}
	waitForChannel(t, responseBody.closed, "response body close")
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
	outcome := waitForChannel(t, result, "canceled reverse retry")
	if outcome.resp != nil || !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("retry outcome: resp=%#v err=%v", outcome.resp, outcome.err)
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

	result := make(chan bool, 1)
	go func() {
		_, _, _, complete := roundTripWithBodyCompletionBudget(ctx, rt, request, time.Second)
		result <- complete
	}()
	waitForChannel(t, source.readStarted, "blocked request read")
	cancel()
	if waitForChannel(t, result, "canceled attempt") {
		t.Fatal("canceled attempt reported a completed body")
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
