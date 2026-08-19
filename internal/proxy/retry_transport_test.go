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
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
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
