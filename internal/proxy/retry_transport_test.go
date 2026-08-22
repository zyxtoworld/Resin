package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/Resinat/Resin/internal/routing"
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

type closeWithoutEOFBody struct {
	payload []byte
	read    atomic.Bool
}

func (b *closeWithoutEOFBody) Read(p []byte) (int, error) {
	if b.read.Swap(true) {
		return 0, io.EOF
	}
	n := copy(p, b.payload)
	return n, nil
}

func (b *closeWithoutEOFBody) Close() error { return nil }

type closeErrorBody struct {
	closeWithoutEOFBody
}

func (*closeErrorBody) Close() error { return errors.New("request body close failed") }

func captureBodyForTest(t *testing.T, body io.ReadCloser, expectedLength int64, readToEOF bool) *replayBodyCapture {
	t.Helper()
	capture := newReplayBodyCapture(body, expectedLength)
	buf := make([]byte, 64)
	if _, err := capture.Read(buf); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("capture body read: %v", err)
	}
	if readToEOF {
		for {
			_, err := capture.Read(buf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("capture body read: %v", err)
			}
		}
	}
	if err := capture.Close(); err != nil {
		t.Fatalf("capture body close: %v", err)
	}
	return capture
}

func TestReplayBodyCapture_CloseCompletionRequiresTrustedLength(t *testing.T) {
	tests := []struct {
		name           string
		body           io.ReadCloser
		expectedLength int64
		readToEOF      bool
		wantReplayable bool
	}{
		{
			name:           "zero length is unknown",
			body:           &closeWithoutEOFBody{payload: []byte("body")},
			expectedLength: 0,
			wantReplayable: false,
		},
		{
			name:           "negative length is unknown",
			body:           &closeWithoutEOFBody{payload: []byte("body")},
			expectedLength: -1,
			wantReplayable: false,
		},
		{
			name:           "short body",
			body:           &closeWithoutEOFBody{payload: []byte("short")},
			expectedLength: int64(len("short-long")),
			readToEOF:      true,
			wantReplayable: false,
		},
		{
			name:           "long body",
			body:           &closeWithoutEOFBody{payload: []byte("short-long")},
			expectedLength: int64(len("short")),
			wantReplayable: false,
		},
		{
			name:           "positive exact length",
			body:           &closeWithoutEOFBody{payload: []byte("body")},
			expectedLength: int64(len("body")),
			wantReplayable: true,
		},
		{
			name:           "positive exact length reaches eof",
			body:           io.NopCloser(strings.NewReader("body")),
			expectedLength: int64(len("body")),
			readToEOF:      true,
			wantReplayable: true,
		},
		{
			name:           "unknown length reaches eof",
			body:           io.NopCloser(strings.NewReader("body")),
			expectedLength: -1,
			readToEOF:      true,
			wantReplayable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := captureBodyForTest(t, tt.body, tt.expectedLength, tt.readToEOF)
			if got := capture.Replayable(); got != tt.wantReplayable {
				t.Fatalf("Replayable() = %v, want %v", got, tt.wantReplayable)
			}
		})
	}
}

func TestReplayBodyCapture_CloseErrorIsNotReplayable(t *testing.T) {
	capture := newReplayBodyCapture(&closeErrorBody{closeWithoutEOFBody{payload: []byte("body")}}, int64(len("body")))
	buf := make([]byte, len("body"))
	if n, err := capture.Read(buf); err != nil || n != len(buf) {
		t.Fatalf("capture body read: n=%d err=%v", n, err)
	}
	if err := capture.Close(); err == nil {
		t.Fatal("capture body close unexpectedly succeeded")
	}
	if capture.Replayable() {
		t.Fatal("capture with close error is replayable")
	}
}

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
	plat.ProxyRequestTotalTimeoutNs = int64(2 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", "127.0.0.1:1")

	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/timeout")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 3 {
		t.Fatalf("initial route retry budget: got %d, want at least 3 distinct candidates", initial.RetryBudget)
	}
	entry := initial.SelectedEntry()
	if entry == nil {
		t.Fatal("initial route did not expose selected entry")
	}

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

func TestReverseRetryRoundTripperLargeCandidateBudgetDoesNotDivideFirstAttempt(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "response-header-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"response_header_timeout"}},
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
	plat.ProxyRequestTotalTimeoutNs = int64(8 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequestForProxy("plat", "large-budget", "https://example.com/large-budget")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RetryBudget = 1444

	var attempts atomic.Int32
	var firstAttemptBudget time.Duration
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			trace := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), trace.clientTrace())), trace
		},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempt := attempts.Add(1)
				if deadline, ok := req.Context().Deadline(); ok && attempt == 1 {
					firstAttemptBudget = time.Until(deadline)
				}
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil {
					t.Fatal("attempt missing request trace")
				}
				trace.GotConn(httptrace.GotConnInfo{})
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				if attempt == 1 {
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
	}

	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/large-budget", nil))
	if err != nil {
		t.Fatalf("bounded retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("bounded retry response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempt count: got %d, want exactly one retry", got)
	}
	if firstAttemptBudget < time.Second {
		t.Fatalf("first attempt budget = %s, candidate count must not create a millisecond deadline", firstAttemptBudget)
	}
}

func TestReverseRetryRoundTripper_RetriesPastTenDistinct429NodesAndPromotesSticky(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "rate-limit", Enabled: true,
		Match: model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusTooManyRequests}},
		Action: model.PlatformResponseRuleAction{
			Type: "cooldown_then_retry_next", CooldownScope: "egress_ip",
			Fallback: "fixed_duration", FixedDuration: "1m",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	plat.ProxyRequestTotalTimeoutNs = int64(5 * time.Second)
	plat.ProxyRequestMaxAttempts = 0
	for i := 0; i < 10; i++ {
		setupResponseRetryNode(t, env,
			fmt.Sprintf(`{"type":"stub","server":"127.0.1.%d","server_port":%d}`, i+1, i+2),
			fmt.Sprintf("203.0.113.%d", i+11), "127.0.0.1:1")
	}
	initial, err := env.router.RouteRequest("plat", "eleven-node-account", "https://example.com/eleven")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 11 {
		t.Fatalf("initial retry budget: got %d, want at least 11", initial.RetryBudget)
	}

	var attempts atomic.Int32
	attemptedIPs := make([]netip.Addr, 0, 11)
	var lastRoute routing.RouteResult
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		onRoute: func(route routing.RouteResult, _ *node.NodeEntry) { lastRoute = route },
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			trace := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), trace.clientTrace())), trace
		},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attemptedIPs = append(attemptedIPs, candidate.Route.EgressIP)
				attempt := attempts.Add(1)
				if attempt <= 10 {
					return &http.Response{
						StatusCode: http.StatusTooManyRequests,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader("rate limited")),
						Request:    req,
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
	}
	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/eleven", nil))
	if err != nil {
		t.Fatalf("eleven-node retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("eleven-node response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 11 {
		t.Fatalf("attempt count: got %d, want 11", got)
	}
	seen := make(map[netip.Addr]struct{}, len(attemptedIPs))
	for _, ip := range attemptedIPs {
		if _, exists := seen[ip]; exists {
			t.Fatalf("retry reused egress IP %s: %v", ip, attemptedIPs)
		}
		seen[ip] = struct{}{}
	}
	if len(seen) != 11 {
		t.Fatalf("distinct egress IPs: got %d, want 11", len(seen))
	}
	if lastRoute.EgressIP != attemptedIPs[len(attemptedIPs)-1] {
		t.Fatalf("last route IP = %s, final attempt IP = %s", lastRoute.EgressIP, attemptedIPs[len(attemptedIPs)-1])
	}
	if !env.router.CommitRouteForAccount(lastRoute, "eleven-node-account") {
		t.Fatal("final successful route was not committed as sticky")
	}
	sticky, err := env.router.RouteRequest("plat", "eleven-node-account", "https://example.com/eleven")
	if err != nil {
		t.Fatalf("sticky route: %v", err)
	}
	if sticky.EgressIP != lastRoute.EgressIP {
		t.Fatalf("sticky egress IP = %s, want %s", sticky.EgressIP, lastRoute.EgressIP)
	}
}

func TestReverseRetryRoundTripper_ExplicitAttemptTimeoutAdvances(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(2 * time.Second)
	plat.ProxyRequestAttemptTimeoutNs = int64(25 * time.Millisecond)
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "response-header-timeout", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{FailureKinds: []string{"response_header_timeout"}},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "attempt-timeout", "https://example.com/attempt-timeout")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	var attempts atomic.Int32
	var firstElapsed time.Duration
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempt := attempts.Add(1)
				started := time.Now()
				if attempt == 1 {
					trace := httptrace.ContextClientTrace(req.Context())
					if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil {
						t.Fatal("attempt missing request trace")
					}
					trace.GotConn(httptrace.GotConnInfo{})
					trace.WroteRequest(httptrace.WroteRequestInfo{})
					<-req.Context().Done()
					firstElapsed = time.Since(started)
					return nil, responseHeaderTimeoutError{}
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
			})
		},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			trace := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), trace.clientTrace())), trace
		},
	}
	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/attempt-timeout", nil))
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("attempt timeout retry: resp=%#v err=%v", resp, err)
	}
	_ = resp.Body.Close()
	if attempts.Load() != 2 {
		t.Fatalf("attempt count: got %d, want 2", attempts.Load())
	}
	if firstElapsed < 15*time.Millisecond || firstElapsed > 250*time.Millisecond {
		t.Fatalf("first attempt duration = %s, want explicit per-attempt timeout", firstElapsed)
	}
}

func TestReverseRetryRoundTripper_ExplicitMaxAttemptsStopsExactly(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(2 * time.Second)
	plat.ProxyRequestMaxAttempts = 2
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", "127.0.0.1:1")
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retry-429", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusTooManyRequests}},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	initial, err := env.router.RouteRequest("plat", "max-attempts", "https://example.com/max-attempts")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router: env.router, pool: env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				return &http.Response{StatusCode: http.StatusTooManyRequests, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("retry")), Request: req}, nil
			})
		},
	}
	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/max-attempts", nil))
	if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("max attempts result: resp=%#v err=%v", resp, err)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempt count: got %d, want exactly 2", got)
	}
}

func TestReverseRetryRoundTripper_RetriesBodyClosedAtContentLength(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "generic-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"timeout"}},
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
	plat.ProxyRequestTotalTimeoutNs = int64(2 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", "127.0.0.1:1")

	initial, err := env.router.RouteRequest("plat", "body-account", "https://example.com/body-timeout")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 2 {
		t.Fatalf("initial route retry budget: got %d, want at least 2 distinct candidates", initial.RetryBudget)
	}
	entry := initial.SelectedEntry()
	if entry == nil {
		t.Fatal("initial route did not expose selected entry")
	}

	payload := []byte("request-body-with-known-length")
	var attempts atomic.Int32
	var attempted []node.Hash
	var readBodies [][]byte
	var bodyMu sync.Mutex
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
				attempts.Add(1)
				attempted = append(attempted, candidate.Entry.Hash)
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil {
					t.Fatal("body retry attempt missing request trace")
				}
				trace.GotConn(httptrace.GotConnInfo{})
				body := make([]byte, len(payload))
				n, readErr := req.Body.Read(body)
				if readErr != nil || n != len(payload) {
					t.Fatalf("request body read: n=%d err=%v", n, readErr)
				}
				bodyMu.Lock()
				readBodies = append(readBodies, append([]byte(nil), body...))
				bodyMu.Unlock()
				if closeErr := req.Body.Close(); closeErr != nil {
					t.Fatalf("request body close: %v", closeErr)
				}
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				if attempts.Load() == 1 {
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
	}

	req := httptest.NewRequest(http.MethodPost, "https://example.com/body-timeout", &closeWithoutEOFBody{payload: payload})
	req.ContentLength = int64(len(payload))
	resp, err := retry.RoundTrip(req)
	if err != nil {
		t.Fatalf("body retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("body retry response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempt count: got %d, want 2", got)
	}
	if attempted[0] == attempted[1] {
		t.Fatalf("retry reused entry %s", attempted[0].Hex())
	}
	if len(readBodies) != 2 || string(readBodies[0]) != string(payload) || string(readBodies[1]) != string(payload) {
		t.Fatalf("request bodies: got %q, want two complete copies", readBodies)
	}
}

func TestReverseRetryRoundTripper_TimeoutBeforeBodyReadCoolsAndRetriesReplayablePost(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	other := platform.NewPlatform("other-id", "other", nil, nil)
	env.pool.RegisterPlatform(other)
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "generic-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"timeout"}},
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
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")

	initial, err := env.router.RouteRequest("plat", "body-account", "https://example.com/ccload-like")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 2 {
		t.Fatalf("initial route retry budget: got %d, want at least 2 distinct candidates", initial.RetryBudget)
	}
	initialEntry := initial.SelectedEntry()
	if initialEntry == nil {
		t.Fatal("initial route did not expose selected entry")
	}

	payload := []byte(`{"model":"generic","stream":false,"input":"timeout-retry"}`)
	var attempts atomic.Int32
	var attempted []node.Hash
	var bodies [][]byte
	var finalRoute routing.RouteResult
	var finalEntry *node.NodeEntry
	var committedAttempts atomic.Int32
	var committedBodyBytes atomic.Int64
	traceOwner := newUpstreamRequestTrace()
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initialEntry},
		onAttemptEgress: func(_, bodyBytes int64) {
			committedAttempts.Add(1)
			committedBodyBytes.Add(bodyBytes)
		},
		onRoute: func(route routing.RouteResult, entry *node.NodeEntry) {
			finalRoute = route
			finalEntry = entry
		},
		decorateAttempt: func(req *http.Request, _ routedOutbound) (*http.Request, *upstreamRequestAttemptTrace) {
			attempt := traceOwner.newAttempt()
			return req.WithContext(httptrace.WithClientTrace(req.Context(), attempt.clientTrace())), attempt
		},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempt := attempts.Add(1)
				attempted = append(attempted, candidate.Entry.Hash)
				if attempt == 1 {
					// This is the production failure shape: transport returns a
					// timeout before it reads or closes the POST body.
					return nil, responseHeaderTimeoutError{}
				}
				body, readErr := io.ReadAll(req.Body)
				closeErr := req.Body.Close()
				if readErr != nil || closeErr != nil {
					t.Fatalf("retry body completion: read=%v close=%v", readErr, closeErr)
				}
				bodies = append(bodies, body)
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil {
					t.Fatal("successful retry attempt missing request trace")
				}
				trace.GotConn(httptrace.GotConnInfo{})
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
	}

	started := time.Now()
	req := httptest.NewRequest(http.MethodPost, "https://example.com/ccload-like", strings.NewReader(string(payload)))
	req.ContentLength = int64(len(payload))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	}
	resp, err := retry.RoundTrip(req)
	if err != nil {
		t.Fatalf("timeout retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("timeout retry response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("retry exceeded platform total budget: %s", elapsed)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempt count: got %d, want 2", got)
	}
	if len(attempted) != 2 || attempted[0] == attempted[1] {
		t.Fatalf("retry did not choose a distinct entry: %v", attempted)
	}
	if len(bodies) != 1 || string(bodies[0]) != string(payload) {
		t.Fatalf("retry body: got %q, want one exact payload", bodies)
	}
	if committedAttempts.Load() != 1 || committedBodyBytes.Load() != int64(len(payload)) {
		t.Fatalf("egress commits: attempts=%d body_bytes=%d, want one exact successful body", committedAttempts.Load(), committedBodyBytes.Load())
	}
	if finalEntry == nil || finalEntry.Hash != attempted[1] {
		t.Fatalf("final route entry does not match successful attempt")
	}
	if !env.router.CommitRouteForAccount(finalRoute, "body-account") {
		t.Fatal("successful route was not committed")
	}
	sticky, err := env.router.RouteRequest("plat", "body-account", "https://example.com/ccload-like")
	if err != nil {
		t.Fatalf("sticky route: %v", err)
	}
	if sticky.SelectedEntry() != finalEntry || sticky.EgressIP != finalRoute.EgressIP {
		t.Fatalf("sticky route changed after successful retry: entry=%v ip=%s", sticky.SelectedEntry(), sticky.EgressIP)
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != initial.EgressIP {
		t.Fatalf("timeout cooldowns: ok=%v cooldowns=%v initial_ip=%s", ok, cooldowns, initial.EgressIP)
	}
	otherCooldowns, otherOK := env.router.SnapshotResponseCooldownsForPlatform("other-id", time.Now())
	if otherOK && len(otherCooldowns) != 0 {
		t.Fatalf("timeout cooldown leaked to other platform: %v", otherCooldowns)
	}
}

func TestReverseRetryRoundTripper_RetriesSmallUnknownLengthBodyAfterUnreadTimeout(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "generic-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"timeout"}},
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
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")

	initial, err := env.router.RouteRequest("plat", "chunked-account", "https://example.com/chunked")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 2 {
		t.Fatalf("initial route retry budget: got %d, want at least 2 distinct candidates", initial.RetryBudget)
	}
	initialEntry := initial.SelectedEntry()
	if initialEntry == nil {
		t.Fatal("initial route did not expose selected entry")
	}

	payload := []byte(`{"model":"generic","stream":false,"input":"chunked-timeout-retry"}`)
	var attempts atomic.Int32
	var attempted []node.Hash
	var bodies [][]byte
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initialEntry},
		transportFor: func(candidate routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempt := attempts.Add(1)
				attempted = append(attempted, candidate.Entry.Hash)
				if attempt == 1 {
					// The production failure shape: a chunked/unknown-length body is
					// still untouched when the first transport attempt times out.
					return nil, responseHeaderTimeoutError{}
				}
				body, readErr := io.ReadAll(req.Body)
				closeErr := req.Body.Close()
				if readErr != nil || closeErr != nil {
					t.Fatalf("retry body completion: read=%v close=%v", readErr, closeErr)
				}
				bodies = append(bodies, body)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
	}

	requestBody := io.NopCloser(bytes.NewReader(payload))
	req := httptest.NewRequest(http.MethodPost, "https://example.com/chunked", requestBody)
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	}
	if req.ContentLength >= 0 {
		t.Fatalf("test request unexpectedly has known content length: %d", req.ContentLength)
	}
	resp, err := retry.RoundTrip(req)
	if err != nil {
		t.Fatalf("unknown-length timeout retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("unknown-length timeout response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempt count: got %d, want 2", got)
	}
	if len(attempted) != 2 || attempted[0] == attempted[1] {
		t.Fatalf("retry did not choose a distinct entry: %v", attempted)
	}
	if len(bodies) != 1 || !bytes.Equal(bodies[0], payload) {
		t.Fatalf("retry body: got %q, want one exact payload", bodies)
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != initial.EgressIP {
		t.Fatalf("timeout cooldowns: ok=%v cooldowns=%v initial_ip=%s", ok, cooldowns, initial.EgressIP)
	}
}

func TestReverseRetryRoundTripper_RetriesUnknownEmptyBodyAfterUnreadTimeout(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "generic-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"timeout"}},
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
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")

	initial, err := env.router.RouteRequest("plat", "empty-account", "https://example.com/empty")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 2 {
		t.Fatalf("initial route retry budget: got %d, want at least 2 distinct candidates", initial.RetryBudget)
	}
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if attempts.Add(1) == 1 {
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
	}
	req := httptest.NewRequest(http.MethodPost, "https://example.com/empty", io.NopCloser(strings.NewReader("")))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.GetBody = func() (io.ReadCloser, error) {
		return http.NoBody, nil
	}
	resp, err := retry.RoundTrip(req)
	if err != nil {
		t.Fatalf("unknown empty body retry: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("unknown empty body response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempt count: got %d, want 2", got)
	}
}

type gatedKnownLengthBody struct {
	chunks  [][]byte
	gates   []chan struct{}
	index   atomic.Int32
	closed  chan struct{}
	closeMu sync.Once
}

type blockingKnownLengthBody struct {
	readEntered chan struct{}
	readDone    chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	mu          sync.Mutex
	maxReadSize int
}

func (b *blockingKnownLengthBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if len(p) > b.maxReadSize {
		b.maxReadSize = len(p)
	}
	b.mu.Unlock()
	select {
	case <-b.readEntered:
	default:
		close(b.readEntered)
	}
	<-b.closed
	close(b.readDone)
	return 0, context.Canceled
}

func (b *blockingKnownLengthBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func (b *blockingKnownLengthBody) maxRead() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxReadSize
}

func TestCaptureKnownLengthRequestBody_GrowsWithBytesNotDeclaredCapacity(t *testing.T) {
	body := &blockingKnownLengthBody{
		readEntered: make(chan struct{}),
		readDone:    make(chan struct{}),
		closed:      make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodPost, "https://example.com/declared-large", body)
	req.ContentLength = responseRuleRetryBodyLimit
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, _, _, err := captureRequestBodyForRetry(ctx, req)
		result <- err
	}()
	waitForChannel(t, body.readEntered, "known length body read")
	if got := body.maxRead(); got >= responseRuleRetryBodyLimit {
		t.Fatalf("first read buffer preallocated declared body: got %d bytes, limit %d", got, responseRuleRetryBodyLimit)
	}
	cancel()
	if err := waitForChannel(t, result, "known length body cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("known length body cancellation: %v", err)
	}
	waitForChannel(t, body.readDone, "known length body read exit")
	select {
	case <-body.closed:
	default:
		t.Fatal("known length body was not closed")
	}
}

func TestCaptureRequestBodyForRetryUnknownLengthCancellationClosesOwner(t *testing.T) {
	body := &blockingKnownLengthBody{
		readEntered: make(chan struct{}),
		readDone:    make(chan struct{}),
		closed:      make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodPost, "https://example.com/unknown-cancel", body)
	req.ContentLength = -1
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, _, err := captureRequestBodyForRetry(ctx, req)
		result <- err
	}()
	waitForChannel(t, body.readEntered, "unknown body read")
	cancel()
	if err := waitForChannel(t, result, "unknown body cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unknown body cancellation: %v", err)
	}
	waitForChannel(t, body.readDone, "unknown body read exit")
	select {
	case <-body.closed:
	default:
		t.Fatal("unknown body was not closed")
	}
}

func TestReverseRetryRoundTripper_OverLimitUnknownBodyStreamsOnceWithoutRetry(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	initial, err := env.router.RouteRequest("plat", "large-account", "https://example.com/large")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	payload := bytes.Repeat([]byte("x"), responseRuleRetryBodyLimit+1)
	var attempts atomic.Int32
	var received []byte
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				var readErr error
				received, readErr = io.ReadAll(req.Body)
				if readErr != nil {
					t.Fatalf("large body read: %v", readErr)
				}
				if err := req.Body.Close(); err != nil {
					t.Fatalf("large body close: %v", err)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("ok")),
					Request:    req,
				}, nil
			})
		},
	}
	req := httptest.NewRequest(http.MethodPost, "https://example.com/large", io.NopCloser(bytes.NewReader(payload)))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	resp, err := retry.RoundTrip(req)
	if err != nil {
		t.Fatalf("large body first attempt: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("large body response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("large body attempts: got %d, want 1", got)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("large body bytes changed: got=%d want=%d", len(received), len(payload))
	}
}

func (b *gatedKnownLengthBody) Read(p []byte) (int, error) {
	i := int(b.index.Add(1)) - 1
	if i >= len(b.chunks) {
		return 0, io.EOF
	}
	select {
	case <-b.gates[i]:
	case <-b.closed:
		return 0, context.Canceled
	}
	return copy(p, b.chunks[i]), nil
}

func (b *gatedKnownLengthBody) Close() error {
	b.closeMu.Do(func() { close(b.closed) })
	return nil
}

func TestReverseRetryRoundTripper_SlowKnownBodyUsesRequestBudgetNotCleanupBudget(t *testing.T) {
	env := newProxyE2EEnv(t)
	initial, err := env.router.RouteRequest("plat", "slow-body", "https://example.com/slow-body")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 1 {
		t.Fatalf("initial route retry budget: got %d", initial.RetryBudget)
	}
	const payload = "slow-known-body"
	body := &gatedKnownLengthBody{
		chunks: [][]byte{[]byte("slow-"), []byte("known-"), []byte("body")},
		gates:  []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})},
		closed: make(chan struct{}),
	}
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				got, readErr := io.ReadAll(req.Body)
				if readErr != nil || string(got) != payload {
					t.Fatalf("slow body at transport: got=%q err=%v", got, readErr)
				}
				_ = req.Body.Close()
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
			})
		},
	}
	result := make(chan struct {
		resp *http.Response
		err  error
	}, 1)
	started := time.Now()
	go func() {
		req := httptest.NewRequest(http.MethodPost, "https://example.com/slow-body", body)
		req.ContentLength = int64(len(payload))
		resp, roundTripErr := retry.RoundTrip(req)
		result <- struct {
			resp *http.Response
			err  error
		}{resp: resp, err: roundTripErr}
	}()
	select {
	case <-time.After(1100 * time.Millisecond):
		close(body.gates[0])
	case <-result:
		t.Fatal("request completed before the deliberately slow body was released")
	}
	close(body.gates[1])
	close(body.gates[2])
	outcome := waitForChannel(t, result, "slow known body request")
	if outcome.err != nil || outcome.resp == nil || outcome.resp.StatusCode != http.StatusOK {
		t.Fatalf("slow known body outcome: resp=%#v err=%v", outcome.resp, outcome.err)
	}
	_ = outcome.resp.Body.Close()
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Fatalf("slow body test did not cross the old 1s cleanup budget: %s", elapsed)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("slow body attempts: got %d, want 1", got)
	}
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("slow request body was not closed")
	}
}

type cancelableKnownLengthBody struct {
	readStarted chan struct{}
	readDone    chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
}

func (b *cancelableKnownLengthBody) Read([]byte) (int, error) {
	select {
	case <-b.readStarted:
	default:
		close(b.readStarted)
	}
	<-b.closed
	close(b.readDone)
	return 0, io.EOF
}

func (b *cancelableKnownLengthBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestReverseRetryRoundTripper_KnownBodyBudgetDeadlineAbortsBeforeAttempt(t *testing.T) {
	env := newProxyE2EEnv(t)
	initial, err := env.router.RouteRequest("plat", "deadline-body", "https://example.com/deadline-body")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RequestTotalTimeout = 100 * time.Millisecond
	body := &cancelableKnownLengthBody{
		readStarted: make(chan struct{}),
		readDone:    make(chan struct{}),
		closed:      make(chan struct{}),
	}
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("unexpected"))}, nil
			})
		},
	}
	result := make(chan error, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "https://example.com/deadline-body", body)
		req.ContentLength = 4
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("body")), nil
		}
		_, roundTripErr := retry.RoundTrip(req)
		result <- roundTripErr
	}()
	waitForChannel(t, body.readStarted, "known body read")
	select {
	case roundTripErr := <-result:
		if !errors.Is(roundTripErr, context.DeadlineExceeded) {
			t.Fatalf("deadline body error: %v", roundTripErr)
		}
	case <-time.After(time.Second):
		t.Fatal("known body did not stop at request budget")
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("deadline body reached upstream: %d attempts", got)
	}
	waitForChannel(t, body.readDone, "deadline body read exit")
	select {
	case <-body.closed:
	default:
		t.Fatal("deadline body was not closed")
	}
}

func TestReverseRetryRoundTripper_KnownBodyCallerCancellationAbortsBeforeAttempt(t *testing.T) {
	env := newProxyE2EEnv(t)
	initial, err := env.router.RouteRequest("plat", "canceled-body", "https://example.com/canceled-body")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	body := &cancelableKnownLengthBody{
		readStarted: make(chan struct{}),
		readDone:    make(chan struct{}),
		closed:      make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return nil, errors.New("unexpected upstream attempt")
			})
		},
	}
	result := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/canceled-body", body)
		req.ContentLength = 4
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("body")), nil
		}
		_, roundTripErr := retry.RoundTrip(req)
		result <- roundTripErr
	}()
	waitForChannel(t, body.readStarted, "cancellable body read")
	cancel()
	if roundTripErr := waitForChannel(t, result, "canceled known body request"); !errors.Is(roundTripErr, context.Canceled) {
		t.Fatalf("canceled body error: %v", roundTripErr)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("canceled body reached upstream: %d attempts", got)
	}
	waitForChannel(t, body.readDone, "canceled body read exit")
	select {
	case <-body.closed:
	default:
		t.Fatal("canceled body was not closed")
	}
}

func TestReverseRetryRoundTripper_UnreplayableTimeoutStillCoolsWithoutRetry(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "generic-timeout", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"timeout"}},
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
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "unknown-body", "https://example.com/unreplayable")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 2 {
		t.Fatalf("initial route retry budget: got %d, want at least 2", initial.RetryBudget)
	}
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
			return roundTripperFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return nil, responseHeaderTimeoutError{}
			})
		},
	}
	largePayload := bytes.Repeat([]byte("x"), responseRuleRetryBodyLimit+1)
	req := httptest.NewRequest(http.MethodPost, "https://example.com/unreplayable", io.NopCloser(bytes.NewReader(largePayload)))
	req.ContentLength = -1
	_, err = retry.RoundTrip(req)
	if _, ok := err.(responseHeaderTimeoutError); !ok {
		t.Fatalf("unreplayable timeout error: got %T %v", err, err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("unreplayable timeout retried: got %d attempts, want 1", got)
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != initial.EgressIP {
		t.Fatalf("unreplayable timeout cooldowns: ok=%v cooldowns=%v initial_ip=%s", ok, cooldowns, initial.EgressIP)
	}
}

func TestReverseRetryRoundTripper_StartedResponseUsesCapturedBudgetWithoutRetry(t *testing.T) {
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
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "started-response", "https://example.com/started")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RetryBudget < 2 {
		t.Fatalf("initial route retry budget: got %d, want at least 2", initial.RetryBudget)
	}
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
					t.Fatal("started-response attempt missing trace callbacks")
				}
				trace.GotConn(httptrace.GotConnInfo{})
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				trace.GotFirstResponseByte()
				attempts.Add(1)
				return nil, responseHeaderTimeoutError{}
			})
		},
	}
	_, err = retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/started", nil))
	if err == nil {
		t.Fatal("started-response timeout unexpectedly succeeded")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("started-response timeout retried: got %d attempts, want 1", got)
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != initial.EgressIP {
		t.Fatalf("started-response cooldowns: ok=%v cooldowns=%v initial_ip=%s", ok, cooldowns, initial.EgressIP)
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
	plat.ProxyRequestTotalTimeoutNs = int64(time.Second)
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
		ProxyToken:          "tok",
		Router:              env.router,
		Pool:                env.pool,
		RequestTotalTimeout: time.Second,
	})
	// A real net/http server request normally has no context deadline. The
	// proxy must use its configured total budget to make this retry bounded.
	request := httptest.NewRequest(http.MethodGet, "http://example.com/connect-timeout", nil)
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

func TestReverseRetryRoundTripper_UnconfiguredPlatformDoesNotRetryWithCallerDeadline(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusBadGateway}},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	plat.ProxyRequestTotalTimeoutNs = 0
	// The caller deadline proves that this request is bounded, but it must not
	// enable retry-next for a platform that has no persisted platform budget.
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/unconfigured")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("retryable")),
					Request:    req,
				}, nil
			})
		},
	}
	initial.RetryBudget = 2
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := retry.RoundTrip(httptest.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/unconfigured", nil))
	if err != nil {
		t.Fatalf("unconfigured retry returned error: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("unconfigured response: %#v, want first 502", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("unconfigured platform attempts: got %d, want 1", got)
	}
}

func TestReverseRetryRoundTripper_UnconfiguredPlatformKeepsFullCallerLifetime(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = 0
	for i, raw := range []string{
		`{"type":"stub","server":"127.0.0.2","server_port":2}`,
		`{"type":"stub","server":"127.0.0.3","server_port":3}`,
		`{"type":"stub","server":"127.0.0.4","server_port":4}`,
		`{"type":"stub","server":"127.0.0.5","server_port":5}`,
	} {
		setupResponseRetryNode(t, env, raw, fmt.Sprintf("203.0.113.%d", 11+i), "127.0.0.1:1")
	}
	initial, err := env.router.RouteRequestForProxy("plat", "account", "https://example.com/full-lifetime")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	initial.RetryBudget = 5

	callerCtx, callerCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer callerCancel()
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				timer := time.NewTimer(120 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader("ok")),
						Request:    req,
					}, nil
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			})
		},
	}

	resp, err := retry.RoundTrip(httptest.NewRequestWithContext(callerCtx, http.MethodGet, "https://example.com/full-lifetime", nil))
	if err != nil {
		t.Fatalf("unconfigured platform request: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("unconfigured platform response: %#v, want 200", resp)
	}
	_ = resp.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("unconfigured platform attempts: got %d, want 1", got)
	}
}

func TestForwardProxy_StartedResponseIdleTimeoutDoesNotRetry(t *testing.T) {
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
	plat.ProxyRequestTotalTimeoutNs = int64(3 * time.Second)
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", "127.0.0.1:1")
	routed, routeErr := resolveRoutedOutbound(env.router, env.pool, "plat", "started-idle", "example.com")
	if routeErr != nil {
		t.Fatalf("initial route: %v", routeErr)
	}
	if !env.router.CommitRouteForAccount(routed.Route, "started-idle") {
		t.Fatal("failed to pin initial route for transport injection")
	}

	var attempts atomic.Int32
	fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	transport := fp.outboundHTTPTransport(routed)
	transport.RegisterProtocol("http", roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		trace := httptrace.ContextClientTrace(req.Context())
		if trace == nil || trace.GotConn == nil || trace.WroteRequest == nil || trace.GotFirstResponseByte == nil {
			t.Fatal("started-idle forward attempt missing trace callbacks")
		}
		trace.GotConn(httptrace.GotConnInfo{})
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		trace.GotFirstResponseByte()
		return nil, deadlineExceededErr{}
	}))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/started-idle", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat.started-idle", "tok"))
	response := httptest.NewRecorder()
	fp.ServeHTTP(response, req)
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("started-idle status: got %d body=%q resin=%q attempts=%d, want 504", response.Code, response.Body.String(), response.Header().Get("X-Resin-Error"), attempts.Load())
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("started-idle response retried: got %d attempts, want 1", got)
	}
	cooldowns, ok := env.router.SnapshotResponseCooldownsForPlatform("plat-id", time.Now())
	if !ok || len(cooldowns) != 1 || cooldowns[0].EgressIP != routed.Route.EgressIP {
		t.Fatalf("started-idle cooldowns: ok=%v cooldowns=%v initial_ip=%s", ok, cooldowns, routed.Route.EgressIP)
	}
}

func TestForwardProxy_UnconfiguredPlatformKeepsFullCallerLifetime(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = 0
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case <-req.Context().Done():
			return
		}
	}))
	defer upstream.Close()

	baseRaw := `{"type":"stub","server":"127.0.0.1","server_port":1}`
	baseHash := node.HashFromRawOptions(json.RawMessage(baseRaw))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base entry not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())
	for i, raw := range []string{
		`{"type":"stub","server":"127.0.0.2","server_port":2}`,
		`{"type":"stub","server":"127.0.0.3","server_port":3}`,
		`{"type":"stub","server":"127.0.0.4","server_port":4}`,
		`{"type":"stub","server":"127.0.0.5","server_port":5}`,
	} {
		entry := setupResponseRetryNode(t, env, raw, fmt.Sprintf("203.0.113.%d", 11+i), upstream.Listener.Addr().String())
		if entry == nil {
			t.Fatal("retry entry was nil")
		}
	}

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
	})
	callerCtx, callerCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer callerCancel()
	req := httptest.NewRequestWithContext(callerCtx, http.MethodGet, "http://example.com/full-lifetime", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	response := httptest.NewRecorder()
	fp.ServeHTTP(response, req)
	if response.Code != http.StatusOK || response.Body.String() != "ok" {
		t.Fatalf("unconfigured platform forward response: status=%d body=%q", response.Code, response.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("unconfigured platform forward attempts: got %d, want 1", got)
	}
}

func TestForwardProxy_CancelsRequestBudgetOnImmediateFailure(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(5 * time.Minute)
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("deterministic dial failure")
	})

	budgetCreated := make(chan context.Context, 1)
	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken:          "tok",
		Router:              env.router,
		Pool:                env.pool,
		RequestTotalTimeout: 10 * time.Minute,
		onRequestBudgetCreated: func(ctx context.Context) {
			budgetCreated <- ctx
		},
	})
	req := httptest.NewRequest(http.MethodGet, "http://example.com/budget-cleanup", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	response := httptest.NewRecorder()
	fp.ServeHTTP(response, req)
	if response.Code == http.StatusOK {
		t.Fatal("immediate dial failure unexpectedly returned 200")
	}
	var budgetCtx context.Context
	select {
	case budgetCtx = <-budgetCreated:
	default:
		t.Fatal("forward proxy did not create a platform request budget")
	}
	select {
	case <-budgetCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("request budget remained live after immediate handler failure")
	}
}

func TestReverseRetryRoundTripper_ReleasedRetryBudgetDoesNotCutOffStreamingBody(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(40 * time.Millisecond)
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/stream")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	requestContext := make(chan context.Context, 1)
	body := &gatedStreamBody{data: []byte("stream-after-budget"), allow: make(chan struct{})}
	retry := &reverseRetryRoundTripper{
		router:              env.router,
		pool:                env.pool,
		initial:             routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		requestTotalTimeout: 2 * time.Second,
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				requestContext <- req.Context()
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body, Request: req}, nil
			})
		},
	}
	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/stream", nil))
	if err != nil || resp == nil {
		t.Fatalf("stream response: resp=%v err=%v", resp, err)
	}
	ctx := <-requestContext
	timer := time.NewTimer(80 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatalf("attempt context expired after response headers: %v", ctx.Err())
	case <-timer.C:
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("attempt context expired after response headers: %v", err)
	}
	close(body.allow)
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		t.Fatalf("read streaming body: %v", readErr)
	}
	if string(data) != "stream-after-budget" {
		t.Fatalf("stream body: got %q", data)
	}
	_ = resp.Body.Close()
}

func TestReverseRetryRoundTripper_NoNextRoutePreservesResponseBodyContext(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.ProxyRequestTotalTimeoutNs = int64(40 * time.Millisecond)
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retry-without-next", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusBadGateway}},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	initial, err := env.router.RouteRequestForProxy("plat", "account", "https://example.com/no-next")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if initial.RequestTotalTimeout != 40*time.Millisecond {
		t.Fatalf("initial route budget = %s, want 40ms", initial.RequestTotalTimeout)
	}
	if _, matched := initial.ResponseRules.Match(http.StatusBadGateway, nil, true, make(http.Header), time.Now()); !matched {
		t.Fatal("initial route did not carry the retry response rule")
	}
	initial.RetryBudget = 2
	body := &contextAwareBody{allow: make(chan struct{}), data: []byte("fallback-body")}
	var attempts atomic.Int32
	retry := &reverseRetryRoundTripper{
		router:  env.router,
		pool:    env.pool,
		initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
		transportFor: func(routedOutbound) http.RoundTripper {
			return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				if req.Body != nil && req.Body != http.NoBody {
					if _, err := io.Copy(io.Discard, req.Body); err != nil {
						return nil, err
					}
					if err := req.Body.Close(); err != nil {
						return nil, err
					}
				}
				body.ctx = req.Context()
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Header:     make(http.Header),
					Body:       body,
					Request:    req,
				}, nil
			})
		},
	}

	resp, err := retry.RoundTrip(httptest.NewRequest(http.MethodPost, "https://example.com/no-next", strings.NewReader("request-body")))
	if err != nil {
		t.Fatalf("fallback response: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("fallback response: %#v, want 502", resp)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("fallback transport attempts = %d, want 1", got)
	}
	if body.ctx == nil {
		t.Fatal("fallback response body did not receive request context")
	}
	watchdog := time.NewTimer(100 * time.Millisecond)
	defer watchdog.Stop()
	select {
	case <-body.ctx.Done():
		t.Fatal("advance failure canceled the accepted response body context")
	case <-watchdog.C:
	}
	close(body.allow)
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read fallback response body: %v", err)
	}
	_ = resp.Body.Close()
	if string(data) != "fallback-body" {
		t.Fatalf("fallback response body: got %q, want fallback-body", data)
	}
}

func TestReverseRetryRoundTripper_ReleasesRequestBudgetOnTerminalPaths(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *http.Response
		err      error
	}{
		{
			name: "transport error",
			err:  errors.New("terminal transport error"),
		},
		{
			name: "no response body",
			response: &http.Response{
				StatusCode: http.StatusNoContent,
				Header:     make(http.Header),
				Body:       http.NoBody,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			plat, ok := env.pool.GetPlatform("plat-id")
			if !ok {
				t.Fatal("platform not found")
			}
			plat.ProxyRequestTotalTimeoutNs = int64(5 * time.Minute)
			initial, err := env.router.RouteRequestForProxy("plat", "account", "https://example.com/terminal")
			if err != nil {
				t.Fatalf("initial route: %v", err)
			}
			var attemptCtx context.Context
			retry := &reverseRetryRoundTripper{
				router:  env.router,
				pool:    env.pool,
				initial: routedOutbound{Route: initial, Entry: initial.SelectedEntry()},
				transportFor: func(routedOutbound) http.RoundTripper {
					return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
						attemptCtx = req.Context()
						if tc.response == nil {
							return nil, tc.err
						}
						response := *tc.response
						response.Request = req
						return &response, nil
					})
				},
			}
			_, _ = retry.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/terminal", nil))
			if attemptCtx == nil {
				t.Fatal("terminal attempt did not receive a context")
			}
			select {
			case <-attemptCtx.Done():
			case <-time.After(250 * time.Millisecond):
				t.Fatal("terminal reverse attempt budget remained live")
			}
		})
	}
}

type contextAwareBody struct {
	ctx   context.Context
	allow chan struct{}
	data  []byte
	done  bool
}

func (b *contextAwareBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-b.allow:
		b.done = true
		return copy(p, b.data), nil
	}
}

func (b *contextAwareBody) Close() error { return nil }

type gatedStreamBody struct {
	data  []byte
	off   int
	allow chan struct{}
}

func (b *gatedStreamBody) Read(p []byte) (int, error) {
	<-b.allow
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

func (b *gatedStreamBody) Close() error { return nil }

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
			if tc.wantErr {
				select {
				case got := <-committed:
					if got != int64(len(payload)) {
						t.Fatalf("egress body bytes: got %d, want %d", got, len(payload))
					}
				case <-time.After(time.Second):
					t.Fatal("egress was not committed after request body completion")
				}
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
				if closeErr := outcome.resp.Body.Close(); closeErr != nil {
					t.Fatalf("close response: %v", closeErr)
				}
				select {
				case got := <-committed:
					if got != int64(len(payload)) {
						t.Fatalf("egress body bytes: got %d, want %d", got, len(payload))
					}
				case <-time.After(time.Second):
					t.Fatal("egress was not committed after response close")
				}
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
