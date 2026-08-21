package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	M "github.com/sagernet/sing/common/metadata"
)

type retryLatencyIdentityRecorder struct {
	mu      sync.Mutex
	entries []*node.NodeEntry
	hashes  []node.Hash
}

func (r *retryLatencyIdentityRecorder) submit(fn func()) {
	fn()
}

func (r *retryLatencyIdentityRecorder) RecordResultForEntry(node.Hash, *node.NodeEntry, bool) bool {
	return true
}

func (r *retryLatencyIdentityRecorder) RecordLatencyForEntry(hash node.Hash, entry *node.NodeEntry, _ string, _ *time.Duration) bool {
	r.mu.Lock()
	r.hashes = append(r.hashes, hash)
	r.entries = append(r.entries, entry)
	r.mu.Unlock()
	return true
}

func (r *retryLatencyIdentityRecorder) RecordPassiveResultForEntry(string, node.Hash, *node.NodeEntry, bool) bool {
	return true
}

func (r *retryLatencyIdentityRecorder) snapshot() ([]node.Hash, []*node.NodeEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]node.Hash(nil), r.hashes...), append([]*node.NodeEntry(nil), r.entries...)
}

func TestReverseRetryRoundTripper_LatencyReporterUsesCurrentAttemptEntry(t *testing.T) {
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
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	secondEntry := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/retry")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	firstEntry := initial.SelectedEntry()
	if firstEntry == nil {
		t.Fatal("initial route did not expose selected entry")
	}
	if firstEntry == secondEntry {
		baseHash := node.HashFromRawOptions([]byte(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
		var found bool
		secondEntry, found = env.pool.GetEntry(baseHash)
		if !found {
			t.Fatal("base entry not found")
		}
	}

	health := &retryLatencyIdentityRecorder{}
	traceOwner := newUpstreamRequestTrace()
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: firstEntry},
		decorateAttempt: func(req *http.Request, candidate routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			return decorateReverseUpstreamAttempt(req, candidate, traceOwner, health, "https", "example.com")
		},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil || trace.GotFirstResponseByte == nil {
					t.Fatal("retry attempt missing client latency trace")
				}
				trace.GotFirstResponseByte()
				if attempts.Add(1) == 1 {
					return &http.Response{
						StatusCode: http.StatusBadGateway,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader("retryable")),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
				}, nil
			})
		},
	}

	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/retry", nil))
	if err != nil {
		t.Fatalf("retry RoundTrip: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("retry response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()

	hashes, entries := health.snapshot()
	if len(entries) != 2 || len(hashes) != 2 {
		t.Fatalf("latency samples: hashes=%d entries=%d, want 2", len(hashes), len(entries))
	}
	if entries[0] != firstEntry || entries[1] != secondEntry {
		t.Fatalf("latency entry attribution: got [%p %p], want [%p %p]", entries[0], entries[1], firstEntry, secondEntry)
	}
	if hashes[0] != initial.NodeHash || hashes[1] != secondEntry.Hash {
		t.Fatalf("latency hash attribution: got [%s %s], want [%s %s]", hashes[0].Hex(), hashes[1].Hex(), initial.NodeHash.Hex(), secondEntry.Hash.Hex())
	}
}

func TestReverseRetryRoundTripper_SuccessResponseWithoutHTTPTraceCommitsOnce(t *testing.T) {
	env := newProxyE2EEnv(t)
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/no-trace")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	entry := initial.SelectedEntry()
	if entry == nil {
		t.Fatal("initial route did not expose selected entry")
	}

	var commits atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: entry},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			// Deliberately do not attach the attempt trace. This transport returns
			// a successful response without emitting any httptrace callbacks.
			return req, newUpstreamRequestTrace().newAttempt()
		},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
		onAttemptEgress: func(_, _ int64) {
			commits.Add(1)
		},
	}

	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/no-trace", nil))
	if err != nil {
		t.Fatalf("retry RoundTrip: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("retry response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := commits.Load(); got != 1 {
		t.Fatalf("egress commits: got %d, want 1", got)
	}
}

type responseHeaderTimeoutError struct{}

func (responseHeaderTimeoutError) Error() string   { return "response header timeout" }
func (responseHeaderTimeoutError) Timeout() bool   { return true }
func (responseHeaderTimeoutError) Temporary() bool { return true }

func TestReverseRetryRoundTripper_TransportFailureRuleRetriesDistinctNodes(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "response-header-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			FailureKinds: []string{"response_header_timeout"},
		},
		Action: model.PlatformResponseRuleAction{
			Type:          "cooldown_then_retry_next",
			CooldownScope: "egress_ip",
			Fallback:      "fixed_duration",
			FixedDuration: "1m",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", "127.0.0.1:1")

	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/timeout")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	entry := initial.SelectedEntry()
	if entry == nil {
		t.Fatal("initial route did not expose selected entry")
	}
	initial.RetryBudget = 3

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var attempts []node.Hash
	var attemptIPs []netip.Addr
	var egressCommits atomic.Int32
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: entry},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
		},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts = append(attempts, candidate.Entry.Hash)
				attemptIPs = append(attemptIPs, candidate.Route.EgressIP)
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil {
					t.Fatal("transport failure attempt missing request trace")
				}
				trace.GotConn(httptrace.GotConnInfo{})
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				if len(attempts) < 3 {
					return nil, responseHeaderTimeoutError{}
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
		onAttemptEgress: func(_, _ int64) {
			egressCommits.Add(1)
		},
	}

	resp, err := retry.RoundTrip(httptest.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/timeout", nil))
	if err != nil {
		t.Fatalf("transport-policy retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("transport-policy response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if len(attempts) != 3 {
		t.Fatalf("attempt count: got %d, want 3", len(attempts))
	}
	if attempts[0] == attempts[1] || attempts[0] == attempts[2] || attempts[1] == attempts[2] {
		t.Fatalf("attempts reused an entry: %v", attempts)
	}
	if got := egressCommits.Load(); got != 3 {
		t.Fatalf("egress commits: got %d, want 3", got)
	}
	if !retry.promotable {
		t.Fatal("successful final attempt was not promotable")
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok {
		t.Fatal("platform cooldown snapshot unavailable")
	}
	cooled := make(map[netip.Addr]bool, len(cooldowns))
	for _, cooldown := range cooldowns {
		if cooldown.Scope == platform.ResponseRuleScopeEgressIP {
			cooled[cooldown.EgressIP] = true
		}
	}
	if len(cooldowns) != 2 || !cooled[attemptIPs[0]] || !cooled[attemptIPs[1]] || cooled[attemptIPs[2]] {
		t.Fatalf("platform cooldowns: got %v, attempts=%v ips=%v", cooled, attempts, attemptIPs)
	}
}

func TestReverseRetryRoundTripper_StartedResponseCoolsWithoutRetry(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "idle-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"idle_timeout"}},
		Action: model.PlatformResponseRuleAction{
			Type:          "cooldown_then_retry_next",
			CooldownScope: "egress_ip",
			Fallback:      "fixed_duration",
			FixedDuration: "1m",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/idle-timeout")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RetryBudget = 2
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := newUpstreamRequestTrace().newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
		},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil || trace.GotFirstResponseByte == nil {
					t.Fatal("idle timeout attempt missing request/response trace")
				}
				trace.GotConn(httptrace.GotConnInfo{})
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				trace.GotFirstResponseByte()
				attempts.Add(1)
				return nil, responseHeaderTimeoutError{}
			})
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = retry.RoundTrip(httptest.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/idle-timeout", nil))
	if err == nil {
		t.Fatal("started-response transport failure unexpectedly succeeded")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("started-response failure retried: got %d attempts, want 1", got)
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != initial.EgressIP {
		t.Fatalf("started-response cooldowns: ok=%v cooldowns=%v", ok, cooldowns)
	}
}

func TestForwardProxy_TransportFailureRuleRetriesAfterConnectTimeout(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "connect-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			FailureKinds: []string{"connect_timeout"},
		},
		Action: model.PlatformResponseRuleAction{
			Type:          "cooldown_then_retry_next",
			CooldownScope: "egress_ip",
			Fallback:      "fixed_duration",
			FixedDuration: "1m",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", "127.0.0.1:1")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/connect-timeout")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if !env.router.CommitRouteForAccount(initial, "account") {
		t.Fatal("failed to establish deterministic sticky initial route")
	}
	var dialed []node.Hash
	env.pool.Range(func(_ node.Hash, entry *node.NodeEntry) bool {
		if entry == initial.SelectedEntry() {
			setProxyE2EEntryDialFunc(t, entry, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				dialed = append(dialed, entry.Hash)
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: responseHeaderTimeoutError{}}
			})
			return true
		}
		setProxyE2EEntryDialTarget(t, entry, upstream.Listener.Addr().String())
		return true
	})

	proxy := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/connect-timeout", nil)
	request.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
	writer := httptest.NewRecorder()
	proxy.ServeHTTP(writer, request)
	if writer.Code != http.StatusOK || writer.Body.String() != "ok" {
		t.Fatalf("forward transport retry: status=%d body=%q", writer.Code, writer.Body.String())
	}
	if len(dialed) != 1 || dialed[0] != initial.NodeHash {
		t.Fatalf("dial attempts: got %v, want only initial timeout %s", dialed, initial.NodeHash.Hex())
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != initial.EgressIP {
		t.Fatalf("connect-timeout cooldowns: ok=%v cooldowns=%v", ok, cooldowns)
	}
}

func TestReverseRetryRoundTripper_WaitsForRequestBodyCompletionBeforeEgressCommit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		responseOK  bool
		wantErr     bool
		wantBodyLen int64
	}{
		{name: "successful response", responseOK: true},
		{name: "written request response error", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			initial, err := env.router.RouteRequest("plat", "account", "https://example.com/body-completion")
			if err != nil {
				t.Fatalf("initial route: %v", err)
			}
			entry := initial.SelectedEntry()
			if entry == nil {
				t.Fatal("initial route did not expose selected entry")
			}

			const payload = "request-body-must-finish"
			allowBody := make(chan struct{})
			roundTripReturned := make(chan struct{})
			bodyRead := make(chan int64, 1)
			committed := make(chan int64, 1)

			traceOwner := newUpstreamRequestTrace()
			retry := &reverseRetryRoundTripper{
				router:  env.router,
				pool:    env.pool,
				initial: routedOutbound{Route: initial, Entry: entry},
				decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
					attempt := traceOwner.newAttempt()
					return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
				},
				transportFor: func(routedOutbound) http.RoundTripper {
					return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
						trace := httptrace.ContextClientTrace(req.Context())
						closeBody := &traceOnCloseBody{inner: req.Body, trace: trace}
						req.Body = closeBody
						go func() {
							<-allowBody
							body, readErr := io.ReadAll(req.Body)
							closeErr := req.Body.Close()
							if readErr != nil {
								t.Errorf("request body read: %v", readErr)
							}
							if closeErr != nil {
								t.Errorf("request body close: %v", closeErr)
							}
							bodyRead <- int64(len(body))
						}()
						close(roundTripReturned)
						if tc.responseOK {
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     make(http.Header),
								Body:       io.NopCloser(strings.NewReader("ok")),
							}, nil
						}
						return nil, errors.New("response read failed")
					})
				},
				onAttemptEgress: func(_, bodyBytes int64) {
					committed <- bodyBytes
				},
			}

			result := make(chan struct {
				resp *http.Response
				err  error
			}, 1)
			go func() {
				resp, roundTripErr := retry.RoundTrip(httptest.NewRequest(
					http.MethodPost,
					"https://example.com/body-completion",
					strings.NewReader(payload),
				))
				result <- struct {
					resp *http.Response
					err  error
				}{resp: resp, err: roundTripErr}
			}()

			select {
			case <-roundTripReturned:
			case <-time.After(time.Second):
				t.Fatal("transport did not return a response/error")
			}
			select {
			case got := <-committed:
				t.Fatalf("egress committed before request body completion: body bytes=%d", got)
			default:
			}

			close(allowBody)
			select {
			case got := <-bodyRead:
				if got != int64(len(payload)) {
					t.Fatalf("request body bytes read: got %d, want %d", got, len(payload))
				}
			case <-time.After(time.Second):
				t.Fatal("request body did not finish")
			}
			select {
			case got := <-committed:
				if got != int64(len(payload)) {
					t.Fatalf("egress body bytes: got %d, want %d", got, len(payload))
				}
			case <-time.After(time.Second):
				t.Fatal("egress was not committed after request body completion")
			}

			outcome := <-result
			if tc.wantErr {
				if outcome.err == nil {
					t.Fatal("RoundTrip error: got nil, want response read error")
				}
				if outcome.resp != nil {
					t.Fatalf("RoundTrip response: got %#v with error", outcome.resp)
				}
			} else {
				if outcome.err != nil {
					t.Fatalf("RoundTrip: %v", outcome.err)
				}
				if outcome.resp == nil || outcome.resp.StatusCode != http.StatusOK {
					t.Fatalf("RoundTrip response: %#v, want 200", outcome.resp)
				}
				_ = outcome.resp.Body.Close()
			}
		})
	}
}

func TestReverseRetryRoundTripper_CancelAfterResponseBodyDoesNotReplay(t *testing.T) {
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
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &cancelOnEOFBody{reader: strings.NewReader("retryable"), cancel: cancel}
	var calls atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				_, _ = io.ReadAll(req.Body)
				_ = req.Body.Close()
				calls.Add(1)
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Header:     make(http.Header),
					Body:       body,
				}, nil
			})
		},
	}
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(`{"stream":true}`)).WithContext(ctx)
	resp, err := retry.RoundTrip(req)
	if !errors.Is(err, context.Canceled) || resp != nil {
		t.Fatalf("canceled retry: resp=%v err=%v", resp, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("canceled retry attempts: got %d, want 1", got)
	}
	if got := body.closeCalls.Load(); got != 1 {
		t.Fatalf("canceled response body close calls: got %d, want 1", got)
	}
}

func TestReverseRetryRoundTripper_OversizedRequestBodyDoesNotReplay(t *testing.T) {
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
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}

	var calls atomic.Int32
	responseBody := io.NopCloser(strings.NewReader("retryable"))
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				_, _ = io.ReadAll(req.Body)
				_ = req.Body.Close()
				calls.Add(1)
				return &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header), Body: responseBody}, nil
			})
		},
	}
	payload := strings.Repeat("x", responseRuleRetryBodyLimit+1)
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(payload))
	resp, err := retry.RoundTrip(req)
	if err != nil || resp == nil {
		t.Fatalf("oversized retry: resp=%v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("oversized retry status: got %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("oversized retry attempts: got %d, want 1", got)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type cancelOnEOFBody struct {
	reader     io.Reader
	cancel     context.CancelFunc
	closeCalls atomic.Int32
}

func (b *cancelOnEOFBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.cancel()
	}
	return n, err
}

func (b *cancelOnEOFBody) Close() error {
	b.closeCalls.Add(1)
	return nil
}
