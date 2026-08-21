package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type proxyE2EEnv struct {
	pool   *topology.GlobalNodePool
	router *routing.Router
	sub    *subscription.Subscription
}

func newProxyE2EEnv(t *testing.T) *proxyE2EEnv {
	t.Helper()

	subMgr := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})

	plat := platform.NewPlatform("plat-id", "plat", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	// Test platforms opt into a bounded retry budget explicitly; production
	// platforms default to zero and therefore remain fail-closed.
	plat.ProxyRequestTotalTimeoutNs = int64(2 * time.Second)
	plat.ReverseProxyMissAction = "TREAT_AS_EMPTY"
	pool.RegisterPlatform(plat)

	sub := subscription.NewSubscription("sub-1", "sub-1", "https://example.com", true, false)
	subMgr.Register(sub)

	raw := json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node not found in pool")
	}

	obMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	obMgr.EnsureNodeOutbound(hash)
	if !entry.HasOutbound() {
		t.Fatal("outbound should be initialized")
	}

	entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	if entry.LatencyTable == nil {
		t.Fatal("latency table should be initialized")
	}
	entry.LatencyTable.Update("example.com", 20*time.Millisecond, 10*time.Minute)
	pool.RecordResult(hash, true)

	pool.NotifyNodeDirty(hash)
	if !plat.View().Contains(hash) {
		t.Fatal("node should be in platform routable view")
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return []string{"example.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
	})

	return &proxyE2EEnv{
		pool:   pool,
		router: router,
		sub:    sub,
	}
}

func setProxyE2EOutboundDialFunc(
	t *testing.T,
	env *proxyE2EEnv,
	dialFunc func(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error),
) {
	t.Helper()

	raw := json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`)
	hash := node.HashFromRawOptions(raw)
	entry, ok := env.pool.GetEntry(hash)
	if !ok {
		t.Fatal("node not found in pool")
	}
	ob := &mockOutbound{dialFunc: dialFunc}
	var wrapped adapter.Outbound = ob
	entry.Outbound.Store(&wrapped)
}

func setProxyE2EEntryDialTarget(t *testing.T, entry *node.NodeEntry, target string) {
	t.Helper()
	ob := &mockOutbound{dialFunc: func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}}
	var wrapped adapter.Outbound = ob
	entry.Outbound.Store(&wrapped)
}

func setProxyE2EEntryDialFunc(t *testing.T, entry *node.NodeEntry, dialFunc func(context.Context, string, M.Socksaddr) (net.Conn, error)) {
	t.Helper()
	ob := &mockOutbound{dialFunc: dialFunc}
	var wrapped adapter.Outbound = ob
	entry.Outbound.Store(&wrapped)
}

func setupResponseRetryNode(t *testing.T, env *proxyE2EEnv, raw, ip, target string) *node.NodeEntry {
	t.Helper()
	rawOptions := json.RawMessage(raw)
	hash := node.HashFromRawOptions(rawOptions)
	env.sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	env.pool.AddNodeFromSub(hash, rawOptions, "sub-1")
	entry, ok := env.pool.GetEntry(hash)
	if !ok {
		t.Fatalf("response retry node %s not found", hash.Hex())
	}
	entry.SetEgressIP(netip.MustParseAddr(ip))
	entry.LatencyTable.Update("example.com", 20*time.Millisecond, 10*time.Minute)
	setProxyE2EEntryDialTarget(t, entry, target)
	env.pool.RecordResult(hash, true)
	env.pool.NotifyNodeDirty(hash)
	return entry
}

func TestForwardProxy_ResponsePolicyCooldownThenRetryPromotesStickyOwner(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	compiledRules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "rate-limit", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusTooManyRequests},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "quota-limited"},
		},
		Action: model.PlatformResponseRuleAction{
			Type: "cooldown_then_retry_next", CooldownScope: "egress_ip", Fallback: "next_utc_midnight",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = compiledRules

	var requests atomic.Int32
	var bodyMu sync.Mutex
	var attempts []struct{ label, body string }
	makeUpstream := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			body, readErr := io.ReadAll(req.Body)
			if readErr != nil {
				http.Error(w, "body read failed", http.StatusBadRequest)
				return
			}
			bodyMu.Lock()
			attempts = append(attempts, struct{ label, body string }{label: label, body: string(body)})
			bodyMu.Unlock()
			if requests.Add(1) <= 2 {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"type":"quota-limited"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
	}
	upstreams := []*httptest.Server{makeUpstream("base"), makeUpstream("second"), makeUpstream("third")}
	defer func() {
		for _, upstream := range upstreams {
			upstream.Close()
		}
	}()

	baseRaw := `{"type":"stub","server":"127.0.0.1","server_port":1}`
	baseHash := node.HashFromRawOptions(json.RawMessage(baseRaw))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstreams[0].Listener.Addr().String())
	second := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstreams[1].Listener.Addr().String())
	third := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", upstreams[2].Listener.Addr().String())

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
	})
	initialRoute, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial sticky route: %v", err)
	}
	payload := `{"model":"generic","stream":true}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
	w := httptest.NewRecorder()
	fp.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d; body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("upstream requests: got %d, want 3", got)
	}
	bodyMu.Lock()
	gotAttempts := append([]struct{ label, body string }(nil), attempts...)
	bodyMu.Unlock()
	if len(gotAttempts) != 3 {
		t.Fatalf("attempts: got %#v", gotAttempts)
	}
	seenLabels := make(map[string]struct{}, len(gotAttempts))
	for _, attempt := range gotAttempts {
		if attempt.body != payload {
			t.Fatalf("replayed POST body: got %q, want %q", attempt.body, payload)
		}
		seenLabels[attempt.label] = struct{}{}
	}
	if len(seenLabels) != 3 {
		t.Fatalf("retry reused an egress: attempts=%#v", gotAttempts)
	}
	if gotAttempts[0].label == gotAttempts[2].label || gotAttempts[1].label == gotAttempts[2].label {
		t.Fatalf("successful retry did not use a third distinct egress: %#v", gotAttempts)
	}

	// Change non-sticky latency/order inputs. A real second proxy request must
	// still use the third successful entry, not merely happen to select it.
	baseEntry.LatencyTable.Update("example.com", 2*time.Second, time.Minute)
	second.LatencyTable.Update("example.com", time.Nanosecond, time.Minute)
	third.LatencyTable.Update("example.com", 2*time.Second, time.Minute)
	env.pool.NotifyNodeDirty(baseHash)
	env.pool.NotifyNodeDirty(second.Hash)
	env.pool.NotifyNodeDirty(third.Hash)
	secondReq := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(payload))
	secondReq.Header.Set("Content-Type", "application/json")
	secondReq.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
	secondWriter := httptest.NewRecorder()
	fp.ServeHTTP(secondWriter, secondReq)
	if secondWriter.Code != http.StatusOK {
		t.Fatalf("sticky second request status: got %d body=%s", secondWriter.Code, secondWriter.Body.String())
	}
	bodyMu.Lock()
	finalAttempt := attempts[len(attempts)-1]
	bodyMu.Unlock()
	if finalAttempt.label != gotAttempts[2].label {
		t.Fatalf("successful IP was not sticky: first success=%q second request=%q", gotAttempts[2].label, finalAttempt.label)
	}
	_ = initialRoute
}

func TestForwardProxy_ResponsePolicyRetryOnlyUsesNextEntryAndPromotesSticky(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retry-only", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusBadGateway},
			Body:        &model.PlatformResponseBodyMatch{Op: "regex", Value: `retryable`},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	var requestCount atomic.Int32
	var mu sync.Mutex
	var labels []string
	makeUpstream := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.ReadAll(req.Body)
			mu.Lock()
			labels = append(labels, label)
			mu.Unlock()
			if requestCount.Add(1) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"error":"retryable"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
	}
	baseUpstream := makeUpstream("base")
	secondUpstream := makeUpstream("second")
	defer baseUpstream.Close()
	defer secondUpstream.Close()

	baseRaw := `{"type":"stub","server":"127.0.0.1","server_port":1}`
	baseHash := node.HashFromRawOptions(json.RawMessage(baseRaw))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, baseUpstream.Listener.Addr().String())
	second := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", secondUpstream.Listener.Addr().String())

	fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	payload := `{"safe":true}`
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
		writer := httptest.NewRecorder()
		fp.ServeHTTP(writer, req)
		return writer
	}
	first := request()
	if first.Code != http.StatusOK || first.Body.String() != "ok" {
		t.Fatalf("retry-only first response: status=%d body=%q", first.Code, first.Body.String())
	}
	mu.Lock()
	firstLabels := append([]string(nil), labels...)
	mu.Unlock()
	if len(firstLabels) != 2 || firstLabels[0] == firstLabels[1] {
		t.Fatalf("retry-only did not switch exact entry: %#v", firstLabels)
	}
	if baseEntry.IsCircuitOpen() || second.IsCircuitOpen() {
		t.Fatal("retry-only polluted global health circuit")
	}
	secondResponse := request()
	if secondResponse.Code != http.StatusOK {
		t.Fatalf("sticky retry-only response: status=%d body=%q", secondResponse.Code, secondResponse.Body.String())
	}
	mu.Lock()
	lastLabel := labels[len(labels)-1]
	mu.Unlock()
	if lastLabel != firstLabels[1] {
		t.Fatalf("retry-only success did not become sticky: first=%q second=%q", firstLabels[1], lastLabel)
	}
}

func TestForwardProxy_RetryEgressBytesIgnoreAttemptThatWasNotWritten(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retry-only", Enabled: true,
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

	var firstHeaderLen atomic.Int64
	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		firstHeaderLen.Store(headerWireLen(req.Header))
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("retryable"))
	}))
	defer firstUpstream.Close()

	baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	otherRaw := `{"type":"stub","server":"127.0.0.2","server_port":2}`
	otherEntry := setupResponseRetryNode(t, env, otherRaw, "203.0.113.11", "127.0.0.1:1")
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/retry")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	firstEntry := initial.SelectedEntry()
	if firstEntry == nil {
		t.Fatal("initial route did not expose selected entry")
	}
	setProxyE2EEntryDialTarget(t, firstEntry, firstUpstream.Listener.Addr().String())

	if otherEntry == firstEntry {
		otherEntry = baseEntry
	}
	setProxyE2EEntryDialFunc(t, otherEntry, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("retry fixture: dial before request write")
	})

	emitter := newMockEventEmitter()
	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Events:     emitter,
	})
	req := httptest.NewRequest(http.MethodGet, "http://example.com/retry", nil)
	req.Header.Set("X-Test-Header", "retry-trace")
	req.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
	wantEgressBytes := headerWireLen(prepareForwardOutboundRequest(req).Header)
	w := httptest.NewRecorder()
	fp.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d, want %d; body=%q", w.Code, http.StatusBadGateway, w.Body.String())
	}
	select {
	case logEv := <-emitter.logCh:
		if firstHeaderLen.Load() <= 0 || wantEgressBytes <= 0 {
			t.Fatal("first upstream did not observe request headers")
		}
		if logEv.EgressBytes != wantEgressBytes {
			t.Fatalf("EgressBytes: got %d, want %d for the only written attempt", logEv.EgressBytes, wantEgressBytes)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected forward log event")
	}
}

func TestForwardProxy_ResponsePolicyAll429ExhaustsFixedSnapshot(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "rate-limit", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusTooManyRequests}, Body: &model.PlatformResponseBodyMatch{Op: "contains", Value: "quota-limited"}},
		Action: model.PlatformResponseRuleAction{Type: "cooldown_then_retry_next", CooldownScope: "egress_ip", Fallback: "next_utc_midnight"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	var requests atomic.Int32
	var mu sync.Mutex
	var bodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, readErr := io.ReadAll(req.Body)
		if readErr != nil {
			t.Errorf("upstream body read: %v", readErr)
			return
		}
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		requests.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"quota-limited"}`))
	}))
	defer upstream.Close()
	baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstream.Listener.Addr().String())
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", upstream.Listener.Addr().String())

	fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	payload := `{"safe":true}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
	writer := httptest.NewRecorder()
	fp.ServeHTTP(writer, req)
	if writer.Code != http.StatusTooManyRequests {
		t.Fatalf("all-429 status: got %d, want 429", writer.Code)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("all-429 attempts: got %d, want exactly 3", got)
	}
	mu.Lock()
	gotBodies := append([]string(nil), bodies...)
	mu.Unlock()
	if len(gotBodies) != 3 || gotBodies[0] != payload || gotBodies[1] != payload || gotBodies[2] != payload {
		t.Fatalf("all-429 request bodies: %#v", gotBodies)
	}
}

func TestForwardProxy_ResponsePolicyAllFailuresDoNotCommitSticky(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing bool
	}{
		{name: "new account", existing: false},
		{name: "existing owner", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			plat, ok := env.pool.GetPlatform("plat-id")
			if !ok {
				t.Fatal("platform not found")
			}
			rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
				ID: "retry-only", Enabled: true,
				Match: model.PlatformResponseRuleMatch{
					StatusCodes: []int{http.StatusBadGateway},
					Body:        &model.PlatformResponseBodyMatch{Op: "regex", Value: `retryable`},
				},
				Action: model.PlatformResponseRuleAction{Type: "retry_next"},
			}})
			if err != nil {
				t.Fatalf("CompileResponseRules: %v", err)
			}
			plat.ResponseRules = rules

			var requests atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if _, err := io.ReadAll(req.Body); err != nil {
					t.Errorf("upstream body read: %v", err)
				}
				requests.Add(1)
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"error":"retryable"}`))
			}))
			defer upstream.Close()

			baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
			baseEntry, ok := env.pool.GetEntry(baseHash)
			if !ok {
				t.Fatal("base node not found")
			}
			setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())
			setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstream.Listener.Addr().String())
			setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", upstream.Listener.Addr().String())

			account := "all-fail-new"
			var before *model.Lease
			if tc.existing {
				account = "all-fail-existing"
				if _, err := env.router.RouteRequest("plat", account, "https://example.com/api"); err != nil {
					t.Fatalf("create existing sticky owner: %v", err)
				}
				before = env.router.ReadLease(model.LeaseKey{PlatformID: "plat-id", Account: account})
				if before == nil {
					t.Fatal("existing sticky owner was not created")
				}
			}

			fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
			req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(`{"safe":true}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Proxy-Authorization", basicAuth("plat."+account, "tok"))
			writer := httptest.NewRecorder()
			fp.ServeHTTP(writer, req)
			if writer.Code != http.StatusBadGateway {
				t.Fatalf("all-failure status: got %d body=%q", writer.Code, writer.Body.String())
			}
			if got := requests.Load(); got != 3 {
				t.Fatalf("all-failure attempts: got %d, want exactly 3", got)
			}

			after := env.router.ReadLease(model.LeaseKey{PlatformID: "plat-id", Account: account})
			if !tc.existing {
				if after != nil {
					t.Fatalf("failed new-account chain committed sticky owner: %+v", after)
				}
				return
			}
			if after == nil {
				t.Fatal("failed chain deleted the existing sticky owner")
			}
			if *after != *before {
				t.Fatalf("failed chain overwrote existing sticky owner: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestForwardProxy_ResponsePolicyConcurrentFailuresDoNotChainProvisionalSticky(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retry-only", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusBadGateway},
			Body:        &model.PlatformResponseBodyMatch{Op: "regex", Value: `retryable`},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	requestAEntered := make(chan struct{})
	requestBEntered := make(chan struct{})
	allowResponseA := make(chan struct{})
	allowResponseB := make(chan struct{})
	var requestAOnce, requestBOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, err := io.ReadAll(req.Body); err != nil {
			t.Errorf("upstream body read: %v", err)
		}
		switch req.Header.Get("X-Test-Request") {
		case "A":
			requestAOnce.Do(func() { close(requestAEntered); <-allowResponseA })
		case "B":
			requestBOnce.Do(func() { close(requestBEntered); <-allowResponseB })
		}
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"retryable"}`))
	}))
	defer upstream.Close()

	baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstream.Listener.Addr().String())
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", upstream.Listener.Addr().String())

	fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	run := func(id string) <-chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(`{"safe":true}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Test-Request", id)
			req.Header.Set("Proxy-Authorization", basicAuth("plat.concurrent", "tok"))
			writer := httptest.NewRecorder()
			fp.ServeHTTP(writer, req)
			done <- writer
		}()
		return done
	}

	doneA := run("A")
	select {
	case <-requestAEntered:
	case <-time.After(time.Second):
		t.Fatal("request A did not reach upstream")
	}

	doneB := run("B")
	select {
	case <-requestBEntered:
	case <-time.After(time.Second):
		t.Fatal("request B did not reach upstream")
	}

	close(allowResponseA)
	select {
	case response := <-doneA:
		if response.Code != http.StatusBadGateway {
			t.Fatalf("request A status: got %d", response.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("request A did not finish")
	}
	close(allowResponseB)
	select {
	case response := <-doneB:
		if response.Code != http.StatusBadGateway {
			t.Fatalf("request B status: got %d", response.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("request B did not finish")
	}

	if lease := env.router.ReadLease(model.LeaseKey{PlatformID: "plat-id", Account: "concurrent"}); lease != nil {
		t.Fatalf("concurrent failed requests left provisional sticky lease: %+v", lease)
	}
}

func TestForwardProxy_ResponsePolicyConcurrentSuccessAndFailureOnlySuccessCommits(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		failureCompletesFirst bool
	}{
		{name: "failure completes first", failureCompletesFirst: true},
		{name: "success completes first", failureCompletesFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			plat, ok := env.pool.GetPlatform("plat-id")
			if !ok {
				t.Fatal("platform not found")
			}
			rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
				ID: "retry-only", Enabled: true,
				Match: model.PlatformResponseRuleMatch{
					StatusCodes: []int{http.StatusBadGateway},
					Body:        &model.PlatformResponseBodyMatch{Op: "regex", Value: `retryable`},
				},
				Action: model.PlatformResponseRuleAction{Type: "retry_next"},
			}})
			if err != nil {
				t.Fatalf("CompileResponseRules: %v", err)
			}
			plat.ResponseRules = rules

			requestAEntered := make(chan struct{})
			requestBEntered := make(chan struct{})
			allowResponseA := make(chan struct{})
			allowResponseB := make(chan struct{})
			var requestAOnce, requestBOnce sync.Once
			var labelsMu sync.Mutex
			var successfulLabel string
			servers := make([]*httptest.Server, 0, 3)
			labels := []string{"base", "second", "third"}
			for _, label := range labels {
				label := label
				servers = append(servers, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if _, err := io.ReadAll(req.Body); err != nil {
						t.Errorf("upstream body read: %v", err)
					}
					switch req.Header.Get("X-Test-Request") {
					case "A":
						requestAOnce.Do(func() { close(requestAEntered); <-allowResponseA })
						labelsMu.Lock()
						successfulLabel = label
						labelsMu.Unlock()
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte("success"))
					case "B":
						requestBOnce.Do(func() { close(requestBEntered); <-allowResponseB })
						w.WriteHeader(http.StatusBadGateway)
						_, _ = w.Write([]byte(`{"error":"retryable"}`))
					default:
						http.Error(w, "unexpected request", http.StatusBadRequest)
					}
				})))
			}
			defer func() {
				for _, server := range servers {
					server.Close()
				}
			}()

			baseRaw := `{"type":"stub","server":"127.0.0.1","server_port":1}`
			baseHash := node.HashFromRawOptions(json.RawMessage(baseRaw))
			baseEntry, ok := env.pool.GetEntry(baseHash)
			if !ok {
				t.Fatal("base node not found")
			}
			setProxyE2EEntryDialTarget(t, baseEntry, servers[0].Listener.Addr().String())
			second := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", servers[1].Listener.Addr().String())
			third := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", servers[2].Listener.Addr().String())

			fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
			run := func(id string) <-chan *httptest.ResponseRecorder {
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(`{"safe":true}`))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("X-Test-Request", id)
					req.Header.Set("Proxy-Authorization", basicAuth("plat.concurrent-success", "tok"))
					writer := httptest.NewRecorder()
					fp.ServeHTTP(writer, req)
					done <- writer
				}()
				return done
			}

			doneA := run("A")
			select {
			case <-requestAEntered:
			case <-time.After(time.Second):
				t.Fatal("successful request did not reach upstream")
			}
			doneB := run("B")
			select {
			case <-requestBEntered:
			case <-time.After(time.Second):
				t.Fatal("failing request did not reach upstream")
			}

			wait := func(done <-chan *httptest.ResponseRecorder, want int, name string) {
				t.Helper()
				select {
				case response := <-done:
					if response.Code != want {
						t.Fatalf("%s status: got %d, want %d", name, response.Code, want)
					}
				case <-time.After(time.Second):
					t.Fatalf("%s did not finish", name)
				}
			}
			if tc.failureCompletesFirst {
				close(allowResponseB)
				wait(doneB, http.StatusBadGateway, "failing request")
				close(allowResponseA)
				wait(doneA, http.StatusOK, "successful request")
			} else {
				close(allowResponseA)
				wait(doneA, http.StatusOK, "successful request")
				close(allowResponseB)
				wait(doneB, http.StatusBadGateway, "failing request")
			}

			labelsMu.Lock()
			gotLabel := successfulLabel
			labelsMu.Unlock()
			wantIP := map[string]netip.Addr{
				"base":   baseEntry.GetEgressIP(),
				"second": second.GetEgressIP(),
				"third":  third.GetEgressIP(),
			}[gotLabel]
			if !wantIP.IsValid() {
				t.Fatalf("successful upstream label was not captured: %q", gotLabel)
			}
			lease := env.router.ReadLease(model.LeaseKey{PlatformID: "plat-id", Account: "concurrent-success"})
			if lease == nil {
				t.Fatal("successful request did not commit a sticky owner")
			}
			if lease.EgressIP != wantIP.String() {
				t.Fatalf("failed request changed sticky owner: label=%q lease=%+v", gotLabel, lease)
			}
		})
	}
}

func TestForwardProxy_ResponsePolicyCommitsStickyBeforeStreamingBody(t *testing.T) {
	env := newProxyE2EEnv(t)
	baseRaw := `{"type":"stub","server":"127.0.0.1","server_port":1}`
	baseHash := node.HashFromRawOptions(json.RawMessage(baseRaw))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}

	releaseFirstBody := make(chan struct{})
	firstBodyReleased := false
	labels := []string{"base", "second"}
	servers := make([]*httptest.Server, 0, len(labels))
	for _, label := range labels {
		label := label
		servers = append(servers, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Upstream-Label", label)
			w.WriteHeader(http.StatusOK)
			flusher, ok := w.(http.Flusher)
			if !ok {
				t.Errorf("upstream %s does not support flushing", label)
				return
			}
			_, _ = io.WriteString(w, "data: first\n\n")
			_, _ = io.WriteString(w, strings.Repeat("x", 32*1024))
			flusher.Flush()
			if req.Header.Get("X-Test-Stream") == "first" {
				<-releaseFirstBody
				return
			}
			_, _ = io.WriteString(w, "data: complete\n\n")
			flusher.Flush()
		})))
	}
	defer func() {
		if !firstBodyReleased {
			close(releaseFirstBody)
		}
		for _, server := range servers {
			server.Close()
		}
	}()

	setProxyE2EEntryDialTarget(t, baseEntry, servers[0].Listener.Addr().String())
	secondEntry := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", servers[1].Listener.Addr().String())

	fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	proxyServer := httptest.NewServer(fp)
	defer proxyServer.Close()
	proxyURL, err := url.Parse(proxyServer.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	newRequest := func(stream string) *http.Request {
		req, err := http.NewRequest(http.MethodGet, "http://example.com/api", nil)
		if err != nil {
			t.Fatalf("build streaming request: %v", err)
		}
		req.Host = "example.com"
		req.Header.Set("Proxy-Authorization", basicAuth("plat.stream", "tok"))
		req.Header.Set("X-Test-Stream", stream)
		return req
	}

	firstResp, err := client.Do(newRequest("first"))
	if err != nil {
		t.Fatalf("first streaming request: %v", err)
	}
	defer firstResp.Body.Close()
	if firstResp.StatusCode != http.StatusOK {
		t.Fatalf("first streaming status: got %d", firstResp.StatusCode)
	}
	firstLabel := firstResp.Header.Get("X-Upstream-Label")
	if firstLabel != "base" && firstLabel != "second" {
		t.Fatalf("first upstream label: %q", firstLabel)
	}
	firstEntry := map[string]*node.NodeEntry{"base": baseEntry, "second": secondEntry}[firstLabel]
	otherEntry := secondEntry
	if firstEntry == secondEntry {
		otherEntry = baseEntry
	}
	// Make a non-sticky selection prefer the other entry. A sticky hit must
	// still use firstEntry while its response body remains open.
	firstEntry.LatencyTable.Update("example.com", 2*time.Second, time.Hour)
	otherEntry.LatencyTable.Update("example.com", time.Nanosecond, time.Hour)
	env.pool.NotifyNodeDirty(firstEntry.Hash)
	env.pool.NotifyNodeDirty(otherEntry.Hash)

	secondResp, err := client.Do(newRequest("second"))
	if err != nil {
		t.Fatalf("second streaming request: %v", err)
	}
	secondBody, readErr := io.ReadAll(secondResp.Body)
	_ = secondResp.Body.Close()
	if readErr != nil {
		t.Fatalf("read second streaming response: %v", readErr)
	}
	if secondResp.StatusCode != http.StatusOK || len(secondBody) == 0 {
		t.Fatalf("second streaming response: status=%d body=%q", secondResp.StatusCode, string(secondBody))
	}
	if got := secondResp.Header.Get("X-Upstream-Label"); got != firstLabel {
		t.Fatalf("sticky owner not committed at response headers: first=%q second=%q", firstLabel, got)
	}
	close(releaseFirstBody)
	firstBodyReleased = true
	_, _ = io.ReadAll(firstResp.Body)
}

func TestForwardProxy_ResponsePolicyOversizedBodyDoesNotReplay(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "rate-limit", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusTooManyRequests}, Body: &model.PlatformResponseBodyMatch{Op: "contains", Value: "quota-limited"}},
		Action: model.PlatformResponseRuleAction{Type: "cooldown_then_retry_next", CooldownScope: "egress_ip", Fallback: "next_utc_midnight"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		requests.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"quota-limited"}`))
	}))
	defer upstream.Close()
	baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())
	setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstream.Listener.Addr().String())

	fp := NewForwardProxy(ForwardProxyConfig{ProxyToken: "tok", Router: env.router, Pool: env.pool})
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api", strings.NewReader(strings.Repeat("x", responseRuleRetryBodyLimit+1)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Proxy-Authorization", basicAuth("plat.account", "tok"))
	writer := httptest.NewRecorder()
	fp.ServeHTTP(writer, req)
	if writer.Code != http.StatusTooManyRequests {
		t.Fatalf("oversized body status: got %d, want 429", writer.Code)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("oversized body was replayed: upstream requests=%d", got)
	}
}

type replacingHealthRecorder struct {
	pool        *topology.GlobalNodePool
	rawOptions  json.RawMessage
	subID       string
	replaced    chan struct{}
	completed   chan struct{}
	replaceOnce sync.Once
	doneOnce    sync.Once
}

func (r *replacingHealthRecorder) replaceNode(hash node.Hash) {
	r.replaceOnce.Do(func() {
		r.pool.RemoveNodeFromSub(hash, r.subID)
		r.pool.AddNodeFromSub(hash, r.rawOptions, r.subID)
		close(r.replaced)
	})
}

func (r *replacingHealthRecorder) complete() {
	r.doneOnce.Do(func() { close(r.completed) })
}

func (r *replacingHealthRecorder) RecordResultForEntry(hash node.Hash, expected *node.NodeEntry, success bool) bool {
	r.replaceNode(hash)
	applied := r.pool.RecordResultForEntry(hash, expected, success)
	r.complete()
	return applied
}

func (r *replacingHealthRecorder) RecordLatencyForEntry(hash node.Hash, expected *node.NodeEntry, rawTarget string, latency *time.Duration) bool {
	return r.pool.RecordLatencyForEntry(hash, expected, rawTarget, latency)
}

func (r *replacingHealthRecorder) RecordPassiveResultForEntry(platformID string, hash node.Hash, expected *node.NodeEntry, success bool) bool {
	r.replaceNode(hash)
	if success || !r.passiveCircuitBreakerDisabled(platformID) {
		applied := r.pool.RecordResultForEntry(hash, expected, success)
		r.complete()
		return applied
	}
	r.complete()
	return false
}

func (r *replacingHealthRecorder) passiveCircuitBreakerDisabled(platformID string) bool {
	// The test uses an enabled platform, so passive feedback is always applied
	// by the real pool on the old implementation.
	return false
}

func TestForwardProxy_DoesNotApplyPassiveResultToRecreatedEntry(t *testing.T) {
	env := newProxyE2EEnv(t)
	raw := json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`)
	hash := node.HashFromRawOptions(raw)
	oldEntry, ok := env.pool.GetEntry(hash)
	if !ok {
		t.Fatal("node not found in pool")
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	health := &replacingHealthRecorder{
		pool:       env.pool,
		rawOptions: raw,
		subID:      "sub-1",
		replaced:   make(chan struct{}),
		completed:  make(chan struct{}),
	}
	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
	})

	req := httptest.NewRequest(http.MethodGet, upstream.URL+"/health-race", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	w := httptest.NewRecorder()
	fp.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	select {
	case <-health.completed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for passive health callback")
	}
	select {
	case <-health.replaced:
	case <-time.After(time.Second):
		t.Fatal("health callback did not replace the node")
	}

	newEntry, ok := env.pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("expected a recreated node entry")
	}
	if !newEntry.IsCircuitOpen() {
		t.Fatal("passive result from the old request cleared the recreated entry circuit")
	}
}

func TestRecordPassiveResultAsync_UsesCapturedPlatformPolicy(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	plat.PassiveCircuitBreakerDisabled = true

	route, err := env.router.RouteRequest("plat", "", "https://example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	entry := route.SelectedEntry()
	if entry == nil {
		t.Fatal("route did not carry selected entry")
	}
	if !route.PassiveCircuitBreakerDisabled {
		t.Fatal("route did not capture disabled passive policy")
	}

	// Replace the platform policy before the admitted asynchronous callback
	// runs. The old ID-based callback would read the new enabled policy and
	// count this failure.
	plat.PassiveCircuitBreakerDisabled = false
	owner := NewHealthWriteOwner(env.pool)
	recordPassiveResultAsync(owner, route, entry, false)
	owner.CloseAndWait()

	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("late passive failure used replacement policy: failure count=%d, want 0", got)
	}
}

func TestHealthWriteOwner_BarriersProductionDirtyFlush(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("persistence bootstrap: %v", err)
	}
	defer closer.Close()

	subManager := topology.NewSubscriptionManager()
	var callbackCalls atomic.Int32
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	callbackDone := make(chan struct{})
	var callbackOnce sync.Once
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return time.Minute },
		OnNodeDynamicChanged: func(hash node.Hash) {
			if callbackCalls.Add(1) == 1 {
				callbackOnce.Do(func() { close(callbackEntered) })
				<-callbackRelease
				engine.MarkNodeDynamic(hash.Hex())
				close(callbackDone)
			}
		},
	})

	sub := subscription.NewSubscription("sub-health", "sub-health", "https://example.com", true, false)
	subManager.Register(sub)
	raw := json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node not found in pool")
	}

	readers := state.CacheReaders{
		ReadNodeStatic: func(key string) *model.NodeStatic {
			if key != hash.Hex() {
				return nil
			}
			return &model.NodeStatic{
				Hash:        key,
				RawOptions:  append(json.RawMessage(nil), entry.RawOptions...),
				CreatedAtNs: entry.CreatedAt.UnixNano(),
			}
		},
		ReadNodeDynamic: func(key string) *model.NodeDynamic {
			if key != hash.Hex() {
				return nil
			}
			return &model.NodeDynamic{
				Hash:             key,
				FailureCount:     int(entry.FailureCount.Load()),
				CircuitOpenSince: entry.CircuitOpenSince.Load(),
			}
		},
		ReadNodeLatency:      func(state.NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(state.LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(state.SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
	engine.MarkNodeStatic(hash.Hex())
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("initial static flush: %v", err)
	}

	worker := state.NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 1 << 20 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	worker.Start()
	defer worker.Stop()

	owner := NewHealthWriteOwner(pool)
	recordPassiveResultAsync(owner, routing.RouteResult{NodeHash: hash}, entry, true)
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("production pool callback did not start")
	}

	ownerDone := make(chan struct{})
	go func() {
		owner.CloseAndWait()
		close(ownerDone)
	}()
	select {
	case <-owner.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("health owner did not close admission")
	}
	select {
	case <-ownerDone:
		t.Fatal("owner returned before admitted dirty write completed")
	default:
	}

	close(callbackRelease)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("production pool callback did not mark dynamic state")
	}
	select {
	case <-ownerDone:
	case <-time.After(time.Second):
		t.Fatal("health owner did not drain admitted write")
	}

	worker.Stop()
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("final flush left %d dirty entries", dirty)
	}
	dynamics, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("load dynamic cache: %v", err)
	}
	if len(dynamics) != 1 || dynamics[0].Hash != hash.Hex() || dynamics[0].CircuitOpenSince != 0 {
		t.Fatalf("final flush did not persist health update: %+v", dynamics)
	}

	recordPassiveResultAsync(owner, routing.RouteResult{NodeHash: hash}, entry, true)
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("health write started after owner close: callback calls=%d", got)
	}
}

func TestForwardProxy_E2EHTTPSuccess(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Proxy-Authorization"); got != "" {
			t.Fatalf("Proxy-Authorization leaked to upstream: %q", got)
		}
		if got := r.URL.Path; got != "/v1/ping" {
			t.Fatalf("unexpected path: %q", got)
		}
		if got := r.URL.RawQuery; got != "q=1" {
			t.Fatalf("unexpected query: %q", got)
		}
		w.Header().Set("X-Upstream", "ok")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("forward-e2e"))
	}))
	defer upstream.Close()

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     &mockHealthRecorder{},
		Events:     emitter,
	})

	req := httptest.NewRequest(http.MethodGet, upstream.URL+"/v1/ping?q=1", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	req.Header.Set("X-Test", "1")
	w := httptest.NewRecorder()

	fp.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusCreated, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	if got := w.Header().Get("X-Upstream"); got != "ok" {
		t.Fatalf("X-Upstream: got %q, want %q", got, "ok")
	}
	if got := w.Body.String(); got != "forward-e2e" {
		t.Fatalf("body: got %q, want %q", got, "forward-e2e")
	}

	select {
	case logEv := <-emitter.logCh:
		if logEv.EgressBytes <= 0 {
			t.Fatalf("EgressBytes: got %d, want > 0", logEv.EgressBytes)
		}
		if logEv.IngressBytes < int64(len("forward-e2e")) {
			t.Fatalf("IngressBytes: got %d, want >= %d", logEv.IngressBytes, len("forward-e2e"))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected forward log event")
	}
}

func TestForwardProxy_RequestLogDoesNotExposeTargetURLCredentials(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	target, err := url.Parse(upstream.URL + "/sub/secret-target-path?token=secret-target-query")
	if err != nil {
		t.Fatalf("parse target: %v", err)
	}
	target.User = url.UserPassword("alice", "secret-target-password")

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     &mockHealthRecorder{},
		Events:     emitter,
	})
	req := httptest.NewRequest(http.MethodGet, target.String(), nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	w := httptest.NewRecorder()
	fp.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, body=%q", w.Code, w.Body.String())
	}

	select {
	case logEv := <-emitter.logCh:
		expectedOrigin := target.Scheme + "://" + target.Host
		if logEv.TargetURL != expectedOrigin {
			t.Fatalf("request log target URL: got %q, want origin %q", logEv.TargetURL, expectedOrigin)
		}
		for _, secret := range []string{"alice", "secret-target-password", "secret-target-path", "secret-target-query"} {
			if strings.Contains(logEv.TargetURL, secret) {
				t.Fatalf("request log exposed target URL credential %q: %q", secret, logEv.TargetURL)
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected forward log event")
	}
}

func TestForwardProxy_E2EHTTPBypassDialsDirect(t *testing.T) {
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Proxy-Authorization"); got != "" {
			t.Fatalf("Proxy-Authorization leaked to direct upstream: %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("direct-forward"))
	}))
	defer upstream.Close()

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken:       "tok",
		Events:           emitter,
		ProxyBypassRules: []string{"127.*"},
	})

	req := httptest.NewRequest(http.MethodGet, upstream.URL+"/direct", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	w := httptest.NewRecorder()

	fp.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusOK, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	if got := w.Body.String(); got != "direct-forward" {
		t.Fatalf("body: got %q, want %q", got, "direct-forward")
	}

	select {
	case logEv := <-emitter.logCh:
		if logEv.NodeHash != "" {
			t.Fatalf("direct bypass should not record a routed node, got %q", logEv.NodeHash)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected direct forward log event")
	}
}

func TestForwardProxy_E2EHTTPDialTimeout_ZeroEgress(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, deadlineExceededErr{}
	})

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})

	body := strings.Repeat("a", 256*1024)
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/upload", strings.NewReader(body))
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	fp.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusGatewayTimeout, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	if got := w.Header().Get("X-Resin-Error"); got != "UPSTREAM_TIMEOUT" {
		t.Fatalf("X-Resin-Error: got %q, want %q", got, "UPSTREAM_TIMEOUT")
	}

	select {
	case logEv := <-emitter.logCh:
		if logEv.ResinError != "UPSTREAM_TIMEOUT" {
			t.Fatalf("ResinError: got %q, want %q", logEv.ResinError, "UPSTREAM_TIMEOUT")
		}
		if logEv.UpstreamStage != "forward_roundtrip" {
			t.Fatalf("UpstreamStage: got %q, want %q", logEv.UpstreamStage, "forward_roundtrip")
		}
		if logEv.EgressBytes != 0 {
			t.Fatalf("EgressBytes: got %d, want 0 for dial-timeout before request write", logEv.EgressBytes)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected forward log event")
	}
}

func TestForwardProxy_E2EHTTPClientCanceledBeforeResponse(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/v1/cancel", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	fp.ServeHTTP(w, req)

	select {
	case logEv := <-emitter.logCh:
		if !logEv.NetOK {
			t.Fatal("client-canceled forward HTTP should log net_ok=true")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected forward log event")
	}

	time.Sleep(50 * time.Millisecond)
	if health.resultCalls.Load() != 0 {
		t.Fatalf("client-canceled forward HTTP should not record health result, got %d calls", health.resultCalls.Load())
	}
}

func TestReverseProxy_E2ESuccess(t *testing.T) {
	env := newProxyE2EEnv(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/items" {
			t.Fatalf("unexpected path: %q", got)
		}
		if got := r.URL.RawQuery; got != "k=v" {
			t.Fatalf("unexpected query: %q", got)
		}
		if got := r.Header.Get("X-Forwarded-Host"); got != "" {
			t.Fatalf("X-Forwarded-Host should be stripped, got %q", got)
		}
		if got := r.Header.Get("X-Real-IP"); got != "" {
			t.Fatalf("X-Real-IP should be stripped, got %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("reverse-e2e"))
	}))
	defer upstream.Close()

	host := strings.TrimPrefix(upstream.URL, "http://")
	path := fmt.Sprintf("/tok/plat:acct/http/%s/api/v1/items?k=v", host)

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events:         NoOpEventEmitter{},
	})

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Forwarded-Host", "should-strip")
	req.Header.Set("X-Real-IP", "1.2.3.4")
	w := httptest.NewRecorder()

	rp.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusOK, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	if got := w.Body.String(); got != "reverse-e2e" {
		t.Fatalf("body: got %q, want %q", got, "reverse-e2e")
	}
}

func TestReverseProxy_ResponsePolicyRetryOnlyUsesNextEntryAndPromotesSticky(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "retryable-upstream", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusBadGateway},
			Body:        &model.PlatformResponseBodyMatch{Op: "regex", Value: `retryable`},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	var firstHits, secondHits atomic.Int32
	var bodyMu sync.Mutex
	var bodies []string
	newUpstream := func(label string, hits *atomic.Int32, status int, responseBody string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			body, readErr := io.ReadAll(req.Body)
			if readErr != nil {
				http.Error(w, "body read failed", http.StatusBadRequest)
				return
			}
			hits.Add(1)
			bodyMu.Lock()
			bodies = append(bodies, label+":"+string(body))
			bodyMu.Unlock()
			w.Header().Set("X-Upstream-Attempt", label)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(responseBody))
		}))
	}
	firstUpstream := newUpstream("first", &firstHits, http.StatusBadGateway, "retryable failure")
	secondUpstream := newUpstream("second", &secondHits, http.StatusOK, "reverse-retry-ok")
	defer firstUpstream.Close()
	defer secondUpstream.Close()

	second := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", secondUpstream.Listener.Addr().String())
	baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
	base, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	firstEntry := initial.SelectedEntry()
	if firstEntry == nil {
		t.Fatal("initial route did not expose selected entry")
	}
	setProxyE2EEntryDialTarget(t, firstEntry, firstUpstream.Listener.Addr().String())
	if firstEntry == base {
		setProxyE2EEntryDialTarget(t, second, secondUpstream.Listener.Addr().String())
	} else {
		setProxyE2EEntryDialTarget(t, base, secondUpstream.Listener.Addr().String())
	}

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events:         NoOpEventEmitter{},
	})
	reverseSrv := httptest.NewServer(rp)
	defer reverseSrv.Close()

	targetHost := strings.TrimPrefix(firstUpstream.URL, "http://")
	requestURL := reverseSrv.URL + "/tok/plat:account/http/" + targetHost + "/v1/retry"
	payload := `{"model":"generic","stream":true}`
	doRequest := func() *http.Response {
		req, err := http.NewRequest(http.MethodPost, requestURL, strings.NewReader(payload))
		if err != nil {
			t.Fatalf("build reverse request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("reverse request: %v", err)
		}
		return resp
	}

	firstResp := doRequest()
	firstBody, err := io.ReadAll(firstResp.Body)
	_ = firstResp.Body.Close()
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK || string(firstBody) != "reverse-retry-ok" {
		t.Fatalf("first response: status=%d body=%q", firstResp.StatusCode, string(firstBody))
	}
	if got := firstResp.Header.Get("X-Upstream-Attempt"); got != "second" {
		t.Fatalf("first response header: got %q, want second", got)
	}

	secondResp := doRequest()
	secondBody, err := io.ReadAll(secondResp.Body)
	_ = secondResp.Body.Close()
	if err != nil {
		t.Fatalf("read second response: %v", err)
	}
	if secondResp.StatusCode != http.StatusOK || string(secondBody) != "reverse-retry-ok" {
		t.Fatalf("second response: status=%d body=%q", secondResp.StatusCode, string(secondBody))
	}
	if got := secondResp.Header.Get("X-Upstream-Attempt"); got != "second" {
		t.Fatalf("second response header: got %q, want second", got)
	}
	if got := firstHits.Load(); got != 1 {
		t.Fatalf("first upstream requests: got %d, want 1", got)
	}
	if got := secondHits.Load(); got != 2 {
		t.Fatalf("second upstream requests: got %d, want 2", got)
	}
	if firstEntry.IsCircuitOpen() {
		t.Fatal("retry-only must not globally cool down the first entry")
	}
	bodyMu.Lock()
	gotBodies := append([]string(nil), bodies...)
	bodyMu.Unlock()
	for _, got := range gotBodies {
		if got != "first:"+payload && got != "second:"+payload {
			t.Fatalf("upstream body: got %q, want %q", got, payload)
		}
	}
}

func TestReverseProxy_RetryEgressBytesCountOnlyWrittenAttempts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		secondWrites bool
		wantAttempts int64
		wantStatus   int
	}{
		{name: "second dial fails before write", secondWrites: false, wantAttempts: 1, wantStatus: http.StatusBadGateway},
		{name: "both attempts write", secondWrites: true, wantAttempts: 2, wantStatus: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			plat, ok := env.pool.GetPlatform("plat-id")
			if !ok {
				t.Fatal("platform not found")
			}
			rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
				ID: "retry-only", Enabled: true,
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

			var retryMode atomic.Bool
			firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if !retryMode.Load() {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("baseline"))
					return
				}
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("retryable"))
			}))
			defer firstUpstream.Close()
			secondUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			}))
			defer secondUpstream.Close()

			baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
			baseEntry, ok := env.pool.GetEntry(baseHash)
			if !ok {
				t.Fatal("base node not found")
			}
			otherEntry := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", secondUpstream.Listener.Addr().String())
			initial, err := env.router.RouteRequest("plat", "account", "https://example.com/retry")
			if err != nil {
				t.Fatalf("initial route: %v", err)
			}
			firstEntry := initial.SelectedEntry()
			if firstEntry == nil {
				t.Fatal("initial route did not expose selected entry")
			}
			setProxyE2EEntryDialTarget(t, firstEntry, firstUpstream.Listener.Addr().String())
			if otherEntry == firstEntry {
				otherEntry = baseEntry
			}
			setProxyE2EEntryDialTarget(t, otherEntry, secondUpstream.Listener.Addr().String())
			if !tc.secondWrites {
				setProxyE2EEntryDialFunc(t, otherEntry, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
					return nil, errors.New("reverse retry fixture: dial before request write")
				})
			}

			emitter := newMockEventEmitter()
			rp := NewReverseProxy(ReverseProxyConfig{
				ProxyToken:     "tok",
				Router:         env.router,
				Pool:           env.pool,
				PlatformLookup: env.pool,
				Events:         emitter,
			})
			targetHost := strings.TrimPrefix(firstUpstream.URL, "http://")
			makeRequest := func() *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodGet, "/tok/plat:account/http/"+targetHost+"/retry", nil)
				req.Header.Set("X-Test-Header", "reverse-retry-trace")
				w := httptest.NewRecorder()
				rp.ServeHTTP(w, req)
				return w
			}

			baseline := makeRequest()
			if baseline.Code != http.StatusOK {
				t.Fatalf("baseline status: got %d, want %d; body=%q", baseline.Code, http.StatusOK, baseline.Body.String())
			}
			var baselineLog RequestLogEntry
			select {
			case baselineLog = <-emitter.logCh:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("expected baseline reverse log event")
			}
			if baselineLog.EgressBytes <= 0 {
				t.Fatalf("baseline EgressBytes: got %d, want > 0", baselineLog.EgressBytes)
			}
			retryMode.Store(true)
			w := makeRequest()

			if w.Code != tc.wantStatus {
				t.Fatalf("status: got %d, want %d; body=%q", w.Code, tc.wantStatus, w.Body.String())
			}
			select {
			case logEv := <-emitter.logCh:
				want := baselineLog.EgressBytes * tc.wantAttempts
				if logEv.EgressBytes != want {
					t.Fatalf("EgressBytes: got %d, want %d", logEv.EgressBytes, want)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("expected reverse log event")
			}
		})
	}
}

func TestReverseProxy_ResponsePolicyAllFailuresDoNotCommitSticky(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing bool
	}{
		{name: "new account", existing: false},
		{name: "existing owner", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newProxyE2EEnv(t)
			plat, ok := env.pool.GetPlatform("plat-id")
			if !ok {
				t.Fatal("platform not found")
			}
			rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
				ID: "retry-only", Enabled: true,
				Match: model.PlatformResponseRuleMatch{
					StatusCodes: []int{http.StatusBadGateway},
					Body:        &model.PlatformResponseBodyMatch{Op: "regex", Value: `retryable`},
				},
				Action: model.PlatformResponseRuleAction{Type: "retry_next"},
			}})
			if err != nil {
				t.Fatalf("CompileResponseRules: %v", err)
			}
			plat.ResponseRules = rules

			var requests atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if _, err := io.ReadAll(req.Body); err != nil {
					t.Errorf("upstream body read: %v", err)
				}
				requests.Add(1)
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"error":"retryable"}`))
			}))
			defer upstream.Close()

			baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
			baseEntry, ok := env.pool.GetEntry(baseHash)
			if !ok {
				t.Fatal("base node not found")
			}
			setProxyE2EEntryDialTarget(t, baseEntry, upstream.Listener.Addr().String())
			setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstream.Listener.Addr().String())
			setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", upstream.Listener.Addr().String())

			account := "reverse-new"
			var before *model.Lease
			if tc.existing {
				account = "reverse-existing"
				if _, err := env.router.RouteRequest("plat", account, "https://example.com/api"); err != nil {
					t.Fatalf("create existing sticky owner: %v", err)
				}
				before = env.router.ReadLease(model.LeaseKey{PlatformID: "plat-id", Account: account})
				if before == nil {
					t.Fatal("existing sticky owner was not created")
				}
			}

			rp := NewReverseProxy(ReverseProxyConfig{
				ProxyToken:     "tok",
				Router:         env.router,
				Pool:           env.pool,
				PlatformLookup: env.pool,
				Health:         &mockHealthRecorder{},
				Events:         NoOpEventEmitter{},
			})
			reverseSrv := httptest.NewServer(rp)
			defer reverseSrv.Close()
			targetHost := strings.TrimPrefix(upstream.URL, "http://")
			requestURL := reverseSrv.URL + "/tok/plat:" + account + "/http/" + targetHost + "/v1/all-fail"
			req, err := http.NewRequest(http.MethodPost, requestURL, strings.NewReader(`{"stream":true}`))
			if err != nil {
				t.Fatalf("build reverse request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("reverse request: %v", err)
			}
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				t.Fatalf("read reverse response: %v", readErr)
			}
			if resp.StatusCode != http.StatusBadGateway || !strings.Contains(string(body), "retryable") {
				t.Fatalf("all-failure response: status=%d body=%q", resp.StatusCode, string(body))
			}
			if got := requests.Load(); got != 3 {
				t.Fatalf("all-failure attempts: got %d, want exactly 3", got)
			}

			after := env.router.ReadLease(model.LeaseKey{PlatformID: "plat-id", Account: account})
			if !tc.existing {
				if after != nil {
					t.Fatalf("failed new-account chain committed sticky owner: %+v", after)
				}
				return
			}
			if after == nil || *after != *before {
				t.Fatalf("failed chain changed existing sticky owner: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestReverseProxy_ResponsePolicyCooldownThenRetryUsesThreeEntrySnapshot(t *testing.T) {
	env := newProxyE2EEnv(t)
	plat, ok := env.pool.GetPlatform("plat-id")
	if !ok {
		t.Fatal("platform not found")
	}
	rules, err := platform.CompileResponseRules("plat-id", []model.PlatformResponseRule{{
		ID: "quota-window", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			StatusCodes: []int{http.StatusTooManyRequests},
			Body:        &model.PlatformResponseBodyMatch{Op: "contains", Value: "quota-limited"},
		},
		Action: model.PlatformResponseRuleAction{
			Type: "cooldown_then_retry_next", CooldownScope: "egress_ip", Fallback: "next_utc_midnight",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	var attempts atomic.Int32
	var bodyMu sync.Mutex
	var labels []string
	var bodies []string
	newUpstream := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			body, readErr := io.ReadAll(req.Body)
			if readErr != nil {
				http.Error(w, "body read failed", http.StatusBadRequest)
				return
			}
			attempt := attempts.Add(1)
			bodyMu.Lock()
			labels = append(labels, label)
			bodies = append(bodies, string(body))
			bodyMu.Unlock()
			w.Header().Set("X-Upstream-Attempt", label)
			if attempt < 3 {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"quota-limited"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("reverse-three-entry-ok"))
		}))
	}
	upstreams := []*httptest.Server{newUpstream("ip-1"), newUpstream("ip-2"), newUpstream("ip-3")}
	defer func() {
		for _, upstream := range upstreams {
			upstream.Close()
		}
	}()

	second := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", upstreams[1].Listener.Addr().String())
	third := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.3","server_port":3}`, "203.0.113.12", upstreams[2].Listener.Addr().String())
	baseHash := node.HashFromRawOptions(json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`))
	base, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}
	initial, err := env.router.RouteRequest("plat", "account", "https://example.com/api")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	firstEntry := initial.SelectedEntry()
	if firstEntry == nil {
		t.Fatal("initial route did not expose selected entry")
	}
	setProxyE2EEntryDialTarget(t, firstEntry, upstreams[0].Listener.Addr().String())
	remaining := make([]*node.NodeEntry, 0, 2)
	for _, entry := range []*node.NodeEntry{base, second, third} {
		if entry != firstEntry {
			remaining = append(remaining, entry)
		}
	}
	if len(remaining) != 2 {
		t.Fatalf("initial route entry was not one of the three candidates: %p", firstEntry)
	}
	setProxyE2EEntryDialTarget(t, remaining[0], upstreams[1].Listener.Addr().String())
	setProxyE2EEntryDialTarget(t, remaining[1], upstreams[2].Listener.Addr().String())

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events:         NoOpEventEmitter{},
	})
	reverseSrv := httptest.NewServer(rp)
	defer reverseSrv.Close()
	targetHost := strings.TrimPrefix(upstreams[0].URL, "http://")
	requestURL := reverseSrv.URL + "/tok/plat:account/http/" + targetHost + "/v1/three-entry"
	payload := `{"model":"generic","stream":true}`
	doRequest := func() *http.Response {
		req, err := http.NewRequest(http.MethodPost, requestURL, strings.NewReader(payload))
		if err != nil {
			t.Fatalf("build reverse request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("reverse request: %v", err)
		}
		return resp
	}

	firstResp := doRequest()
	firstBody, err := io.ReadAll(firstResp.Body)
	_ = firstResp.Body.Close()
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK || string(firstBody) != "reverse-three-entry-ok" {
		t.Fatalf("first response: status=%d body=%q", firstResp.StatusCode, string(firstBody))
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("first request attempts: got %d, want 3", got)
	}
	bodyMu.Lock()
	firstLabels := append([]string(nil), labels...)
	firstBodies := append([]string(nil), bodies...)
	bodyMu.Unlock()
	if len(firstLabels) != 3 || firstLabels[0] == firstLabels[1] || firstLabels[0] == firstLabels[2] || firstLabels[1] == firstLabels[2] {
		t.Fatalf("retry candidates were not distinct: %#v", firstLabels)
	}
	for _, body := range firstBodies {
		if body != payload {
			t.Fatalf("replayed body: got %q, want %q", body, payload)
		}
	}
	if got := firstResp.Header.Get("X-Upstream-Attempt"); got != firstLabels[2] {
		t.Fatalf("final response header: got %q, want %q", got, firstLabels[2])
	}

	secondResp := doRequest()
	secondBody, err := io.ReadAll(secondResp.Body)
	_ = secondResp.Body.Close()
	if err != nil {
		t.Fatalf("read second response: %v", err)
	}
	if secondResp.StatusCode != http.StatusOK || string(secondBody) != "reverse-three-entry-ok" {
		t.Fatalf("second response: status=%d body=%q", secondResp.StatusCode, string(secondBody))
	}
	bodyMu.Lock()
	secondLabel := labels[len(labels)-1]
	bodyMu.Unlock()
	if secondLabel != firstLabels[2] {
		t.Fatalf("successful retry was not sticky: first success=%q second request=%q", firstLabels[2], secondLabel)
	}
}

func TestReverseProxy_E2EHTTPBypassDialsDirect(t *testing.T) {
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/direct" {
			t.Fatalf("unexpected path: %q", got)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("direct-reverse"))
	}))
	defer upstream.Close()

	host := strings.TrimPrefix(upstream.URL, "http://")
	path := fmt.Sprintf("/tok/plat:acct/http/%s/api/direct", host)

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:       "tok",
		Events:           emitter,
		ProxyBypassRules: []string{"127.*"},
	})

	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()

	rp.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusAccepted, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	if got := w.Body.String(); got != "direct-reverse" {
		t.Fatalf("body: got %q, want %q", got, "direct-reverse")
	}

	select {
	case logEv := <-emitter.logCh:
		if logEv.NodeHash != "" {
			t.Fatalf("direct bypass should not record a routed node, got %q", logEv.NodeHash)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected direct reverse log event")
	}
}

func TestReverseProxy_E2EDirectWrittenRequestErrorCountsEgress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer listener.Close()

	requestRead := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			requestRead <- acceptErr
			return
		}
		defer conn.Close()
		incoming, readErr := http.ReadRequest(bufio.NewReader(conn))
		if readErr != nil {
			requestRead <- readErr
			return
		}
		_, bodyErr := io.Copy(io.Discard, incoming.Body)
		_ = incoming.Body.Close()
		requestRead <- bodyErr
	}()

	emitter := newMockEventEmitter()
	host := listener.Addr().String()
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:       "tok",
		Events:           emitter,
		ProxyBypassRules: []string{"127.*"},
	})
	payload := "written-before-response-error"
	req := httptest.NewRequest(http.MethodPost, "/tok/plat:acct/http/"+host+"/broken", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	rp.ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		t.Fatal("written request with upstream response error must not return a successful response")
	}

	select {
	case err := <-requestRead:
		if err != nil {
			t.Fatalf("upstream request read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream did not observe the complete request")
	}
	select {
	case logEv := <-emitter.logCh:
		if logEv.EgressBytes < int64(len(payload)) {
			t.Fatalf("EgressBytes: got %d, want >= %d after written response error", logEv.EgressBytes, len(payload))
		}
	case <-time.After(time.Second):
		t.Fatal("expected reverse log event")
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("upstream handler did not finish")
	}
}

func TestReverseProxy_E2EDialTimeout_ZeroEgress(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, deadlineExceededErr{}
	})

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         health,
		Events:         emitter,
	})

	body := strings.Repeat("b", 256*1024)
	req := httptest.NewRequest(http.MethodPost, "/tok/plat:acct/http/example.com/upload", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	rp.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusGatewayTimeout, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	if got := w.Header().Get("X-Resin-Error"); got != "UPSTREAM_TIMEOUT" {
		t.Fatalf("X-Resin-Error: got %q, want %q", got, "UPSTREAM_TIMEOUT")
	}

	select {
	case logEv := <-emitter.logCh:
		if logEv.ResinError != "UPSTREAM_TIMEOUT" {
			t.Fatalf("ResinError: got %q, want %q", logEv.ResinError, "UPSTREAM_TIMEOUT")
		}
		if logEv.UpstreamStage != "reverse_roundtrip" {
			t.Fatalf("UpstreamStage: got %q, want %q", logEv.UpstreamStage, "reverse_roundtrip")
		}
		if logEv.EgressBytes != 0 {
			t.Fatalf("EgressBytes: got %d, want 0 for dial-timeout before request write", logEv.EgressBytes)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected reverse log event")
	}
}

func TestReverseProxy_E2EClientCanceledBeforeResponse(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         health,
		Events:         emitter,
	})

	req := httptest.NewRequest(http.MethodGet, "/tok/plat:acct/http/example.com/v1/cancel", nil)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	rp.ServeHTTP(w, req)

	select {
	case logEv := <-emitter.logCh:
		if !logEv.NetOK {
			t.Fatal("client-canceled reverse request should log net_ok=true")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected reverse log event")
	}

	time.Sleep(50 * time.Millisecond)
	if health.resultCalls.Load() != 0 {
		t.Fatalf("client-canceled reverse request should not record health result, got %d calls", health.resultCalls.Load())
	}
}

func TestReverseProxy_E2ECapturesDetailPayloads(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()

	upstreamBody := "reverse-body-payload"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/items" {
			t.Fatalf("unexpected path: %q", got)
		}
		w.Header().Set("X-Upstream-Header", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	host := strings.TrimPrefix(upstream.URL, "http://")
	path := fmt.Sprintf("/tok/plat:acct/http/%s/api/v1/items", host)
	reqBody := "request-body-data"

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events: ConfigAwareEventEmitter{
			Base: emitter,
			RequestLogConfigProvider: func() RequestLogRuntimeConfig {
				return RequestLogRuntimeConfig{
					Enabled:             true,
					DetailEnabled:       true,
					ReqHeadersMaxBytes:  -1,
					ReqBodyMaxBytes:     -1,
					RespHeadersMaxBytes: -1,
					RespBodyMaxBytes:    -1,
				}
			},
		},
	})

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Client-Header", "capture")
	w := httptest.NewRecorder()

	rp.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want %d (body=%q, resinErr=%q)",
			w.Code, http.StatusCreated, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}

	select {
	case logEv := <-emitter.logCh:
		if len(logEv.ReqHeaders) == 0 || logEv.ReqHeadersLen == 0 {
			t.Fatalf("expected req headers capture, got len=%d payload=%d", logEv.ReqHeadersLen, len(logEv.ReqHeaders))
		}
		if string(logEv.ReqBody) != reqBody {
			t.Fatalf("ReqBody: got %q, want %q", string(logEv.ReqBody), reqBody)
		}
		if logEv.ReqBodyLen != len(reqBody) || logEv.ReqBodyTruncated {
			t.Fatalf("ReqBody meta: len=%d truncated=%v, want len=%d truncated=false",
				logEv.ReqBodyLen, logEv.ReqBodyTruncated, len(reqBody))
		}
		if len(logEv.RespHeaders) == 0 || logEv.RespHeadersLen == 0 {
			t.Fatalf("expected resp headers capture, got len=%d payload=%d", logEv.RespHeadersLen, len(logEv.RespHeaders))
		}
		if !strings.Contains(string(logEv.RespHeaders), "X-Upstream-Header: yes") {
			t.Fatalf("RespHeaders missing upstream header, payload=%q", string(logEv.RespHeaders))
		}
		if string(logEv.RespBody) != upstreamBody {
			t.Fatalf("RespBody: got %q, want %q", string(logEv.RespBody), upstreamBody)
		}
		if logEv.RespBodyLen != len(upstreamBody) || logEv.RespBodyTruncated {
			t.Fatalf("RespBody meta: len=%d truncated=%v, want len=%d truncated=false",
				logEv.RespBodyLen, logEv.RespBodyTruncated, len(upstreamBody))
		}
		if logEv.EgressBytes < int64(len(reqBody)) {
			t.Fatalf("EgressBytes: got %d, want >= %d", logEv.EgressBytes, len(reqBody))
		}
		if logEv.IngressBytes < int64(len(upstreamBody)) {
			t.Fatalf("IngressBytes: got %d, want >= %d", logEv.IngressBytes, len(upstreamBody))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected reverse log event")
	}
}

func TestReverseProxy_E2EDetailCaptureRedactsCredentialHeaders(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer request-secret" {
			t.Errorf("upstream Authorization: got %q, want original credential", got)
		}
		if got := r.Header.Get("Cookie"); got != "session=request-secret" {
			t.Errorf("upstream Cookie: got %q, want original credential", got)
		}
		w.Header().Set("Set-Cookie", "session=upstream-secret; Secure")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	host := strings.TrimPrefix(upstream.URL, "http://")
	path := fmt.Sprintf("/tok/plat:acct/http/%s/secure", host)
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events: ConfigAwareEventEmitter{
			Base: emitter,
			RequestLogConfigProvider: func() RequestLogRuntimeConfig {
				return RequestLogRuntimeConfig{
					Enabled:             true,
					DetailEnabled:       true,
					ReqHeadersMaxBytes:  -1,
					ReqBodyMaxBytes:     -1,
					RespHeadersMaxBytes: -1,
					RespBodyMaxBytes:    -1,
				}
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer request-secret")
	req.Header.Set("Cookie", "session=request-secret")
	w := httptest.NewRecorder()
	rp.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, body=%q", w.Code, w.Body.String())
	}

	select {
	case logEv := <-emitter.logCh:
		payload := string(logEv.ReqHeaders) + string(logEv.RespHeaders)
		for _, secret := range []string{"request-secret", "upstream-secret", "Bearer"} {
			if strings.Contains(payload, secret) {
				t.Fatalf("request log exposed credential header value %q in %q", secret, payload)
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected reverse log event")
	}
}

func TestReverseProxy_ResponsePolicyCommitsStickyForUpgradeBeforeTunnel(t *testing.T) {
	env := newProxyE2EEnv(t)
	baseRaw := `{"type":"stub","server":"127.0.0.1","server_port":1}`
	baseHash := node.HashFromRawOptions(json.RawMessage(baseRaw))
	baseEntry, ok := env.pool.GetEntry(baseHash)
	if !ok {
		t.Fatal("base node not found")
	}

	releaseUpgrade := make(chan struct{})
	labels := []string{"base", "second"}
	servers := make([]*httptest.Server, 0, len(labels))
	for _, label := range labels {
		label := label
		servers = append(servers, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("upstream %s does not support hijacking", label)
				return
			}
			conn, brw, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("upstream %s hijack: %v", label, err)
				return
			}
			defer conn.Close()
			_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
			_, _ = brw.WriteString("Connection: Upgrade\r\n")
			_, _ = brw.WriteString("Upgrade: websocket\r\n")
			_, _ = brw.WriteString("X-Upstream-Label: " + label + "\r\n\r\n")
			if err := brw.Flush(); err != nil {
				t.Errorf("upstream %s flush upgrade response: %v", label, err)
				return
			}
			<-releaseUpgrade
		})))
	}
	defer func() {
		close(releaseUpgrade)
		for _, server := range servers {
			server.Close()
		}
	}()

	setProxyE2EEntryDialTarget(t, baseEntry, servers[0].Listener.Addr().String())
	secondEntry := setupResponseRetryNode(t, env, `{"type":"stub","server":"127.0.0.2","server_port":2}`, "203.0.113.11", servers[1].Listener.Addr().String())

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events:         NoOpEventEmitter{},
	})
	reverseServer := httptest.NewServer(rp)
	defer reverseServer.Close()
	reverseAddr := strings.TrimPrefix(reverseServer.URL, "http://")
	upstreamHost := strings.TrimPrefix(servers[0].URL, "http://")
	openUpgrade := func() (net.Conn, string) {
		t.Helper()
		conn, err := net.Dial("tcp", reverseAddr)
		if err != nil {
			t.Fatalf("dial reverse proxy: %v", err)
		}
		request := fmt.Sprintf(
			"GET /tok/plat:acct/http/%s/ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
			upstreamHost,
			reverseAddr,
		)
		if _, err := conn.Write([]byte(request)); err != nil {
			_ = conn.Close()
			t.Fatalf("write upgrade request: %v", err)
		}
		reader := bufio.NewReader(conn)
		statusLine, err := reader.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			t.Fatalf("read upgrade status: %v", err)
		}
		if !strings.Contains(statusLine, "101 Switching Protocols") {
			_ = conn.Close()
			t.Fatalf("unexpected upgrade status: %q", statusLine)
		}
		label := ""
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				_ = conn.Close()
				t.Fatalf("read upgrade headers: %v", err)
			}
			if strings.HasPrefix(strings.ToLower(line), "x-upstream-label:") {
				label = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
			}
			if line == "\r\n" {
				break
			}
		}
		return conn, label
	}

	firstConn, firstLabel := openUpgrade()
	defer firstConn.Close()
	if firstLabel != "base" && firstLabel != "second" {
		t.Fatalf("first upgrade upstream label: %q", firstLabel)
	}
	firstEntry := map[string]*node.NodeEntry{"base": baseEntry, "second": secondEntry}[firstLabel]
	otherEntry := secondEntry
	if firstEntry == secondEntry {
		otherEntry = baseEntry
	}
	firstEntry.LatencyTable.Update("example.com", 2*time.Second, time.Hour)
	otherEntry.LatencyTable.Update("example.com", time.Nanosecond, time.Hour)
	env.pool.NotifyNodeDirty(firstEntry.Hash)
	env.pool.NotifyNodeDirty(otherEntry.Hash)

	secondConn, secondLabel := openUpgrade()
	defer secondConn.Close()
	if secondLabel != firstLabel {
		t.Fatalf("upgrade sticky owner was not committed at 101 headers: first=%q second=%q", firstLabel, secondLabel)
	}
}

func TestReverseProxy_E2EWebSocketUpgrade_WithDetailCapture(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()

	tunnelPayload := strings.Repeat("c", 200*1024)
	tunnelAck := strings.Repeat("s", 180*1024)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := strings.ToLower(r.Header.Get("Upgrade")); got != "websocket" {
			t.Fatalf("Upgrade header: got %q, want %q", got, "websocket")
		}
		if got := strings.ToLower(r.Header.Get("Connection")); !strings.Contains(got, "upgrade") {
			t.Fatalf("Connection header should include upgrade, got %q", got)
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("upstream does not support hijack")
		}
		conn, brw, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("upstream hijack: %v", err)
		}
		defer conn.Close()

		_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		_, _ = brw.WriteString("Connection: Upgrade\r\n")
		_, _ = brw.WriteString("Upgrade: websocket\r\n")
		_, _ = brw.WriteString("\r\n")
		if err := brw.Flush(); err != nil {
			t.Fatalf("upstream flush upgrade response: %v", err)
		}

		buf := make([]byte, len(tunnelPayload))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("upstream read tunneled payload: %v", err)
		}
		if got := string(buf); got != tunnelPayload {
			t.Fatalf("upstream payload: got %q, want %q", got, tunnelPayload)
		}
		if _, err := conn.Write([]byte(tunnelAck)); err != nil {
			t.Fatalf("upstream write tunneled ack: %v", err)
		}
	}))
	defer upstream.Close()

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events: ConfigAwareEventEmitter{
			Base: emitter,
			RequestLogConfigProvider: func() RequestLogRuntimeConfig {
				return RequestLogRuntimeConfig{
					Enabled:             true,
					DetailEnabled:       true,
					ReqHeadersMaxBytes:  -1,
					ReqBodyMaxBytes:     -1,
					RespHeadersMaxBytes: -1,
					RespBodyMaxBytes:    -1,
				}
			},
		},
	})
	reverseSrv := httptest.NewServer(rp)
	defer reverseSrv.Close()

	reverseAddr := strings.TrimPrefix(reverseSrv.URL, "http://")
	clientConn, err := net.Dial("tcp", reverseAddr)
	if err != nil {
		t.Fatalf("dial reverse proxy: %v", err)
	}
	defer clientConn.Close()

	upstreamHost := strings.TrimPrefix(upstream.URL, "http://")
	req := fmt.Sprintf(
		"GET /tok/plat:acct/http/%s/ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
		upstreamHost,
		reverseAddr,
	)
	if _, err := clientConn.Write([]byte(req)); err != nil {
		t.Fatalf("write upgrade request: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read upgrade status line: %v", err)
	}
	if !strings.Contains(statusLine, "101 Switching Protocols") {
		t.Fatalf("unexpected status line: %q", statusLine)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read upgrade headers: %v", err)
		}
		if strings.HasPrefix(strings.ToLower(line), "x-resin-error:") {
			t.Fatalf("unexpected resin error header on upgrade success: %q", line)
		}
		if line == "\r\n" {
			break
		}
	}

	if _, err := clientConn.Write([]byte(tunnelPayload)); err != nil {
		t.Fatalf("write tunneled payload: %v", err)
	}
	ack := make([]byte, len(tunnelAck))
	if _, err := io.ReadFull(reader, ack); err != nil {
		t.Fatalf("read tunneled ack: %v", err)
	}
	if got := string(ack); got != tunnelAck {
		t.Fatalf("tunneled ack: got %q, want %q", got, tunnelAck)
	}

	_ = clientConn.Close()

	select {
	case logEv := <-emitter.logCh:
		if logEv.HTTPStatus != http.StatusSwitchingProtocols {
			t.Fatalf("HTTPStatus: got %d, want %d", logEv.HTTPStatus, http.StatusSwitchingProtocols)
		}
		if !logEv.NetOK {
			t.Fatal("NetOK: got false, want true")
		}
		if len(logEv.RespHeaders) == 0 || logEv.RespHeadersLen == 0 {
			t.Fatalf("expected resp headers capture, got len=%d payload=%d", logEv.RespHeadersLen, len(logEv.RespHeaders))
		}
		if !strings.Contains(strings.ToLower(string(logEv.RespHeaders)), "upgrade: websocket") {
			t.Fatalf("RespHeaders missing upgrade header, payload=%q", string(logEv.RespHeaders))
		}
		if len(logEv.RespBody) != 0 || logEv.RespBodyLen != 0 || logEv.RespBodyTruncated {
			t.Fatalf(
				"expected empty resp body capture for upgrade, got len=%d payload=%d truncated=%v",
				logEv.RespBodyLen,
				len(logEv.RespBody),
				logEv.RespBodyTruncated,
			)
		}
		if logEv.EgressBytes < int64(len(tunnelPayload)) {
			t.Fatalf("EgressBytes: got %d, want >= %d", logEv.EgressBytes, len(tunnelPayload))
		}
		if logEv.IngressBytes < int64(len(tunnelAck)) {
			t.Fatalf("IngressBytes: got %d, want >= %d", logEv.IngressBytes, len(tunnelAck))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected reverse log event for websocket upgrade")
	}
}

func TestReverseProxy_E2EWebSocketUpgradeClientCloseDrainsBackend(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	upstreamUpgraded := make(chan struct{})
	upstreamClosed := make(chan struct{})
	var upstreamUpgradeOnce sync.Once
	var upstreamCloseOnce sync.Once

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("upstream does not support hijack")
			return
		}
		conn, brw, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("upstream hijack: %v", err)
			return
		}
		defer conn.Close()
		defer upstreamCloseOnce.Do(func() { close(upstreamClosed) })
		_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		_, _ = brw.WriteString("Connection: Upgrade\r\n")
		_, _ = brw.WriteString("Upgrade: websocket\r\n")
		_, _ = brw.WriteString("\r\n")
		if err := brw.Flush(); err != nil {
			t.Errorf("upstream upgrade flush: %v", err)
			return
		}
		upstreamUpgradeOnce.Do(func() { close(upstreamUpgraded) })
		_, _ = io.Copy(io.Discard, conn)
	}))
	defer upstream.Close()

	upstreamAddr := strings.TrimPrefix(upstream.URL, "http://")
	setProxyE2EOutboundDialFunc(t, env, func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, upstreamAddr)
	})

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         &mockHealthRecorder{},
		Events:         emitter,
	})
	reverseSrv := httptest.NewServer(rp)
	defer reverseSrv.Close()

	reverseAddr := strings.TrimPrefix(reverseSrv.URL, "http://")
	clientConn, err := net.Dial("tcp", reverseAddr)
	if err != nil {
		t.Fatalf("dial reverse proxy: %v", err)
	}
	defer clientConn.Close()

	upstreamHost := strings.TrimPrefix(upstream.URL, "http://")
	request := fmt.Sprintf(
		"GET /tok/plat:acct/http/%s/ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
		upstreamHost,
		reverseAddr,
	)
	if _, err := clientConn.Write([]byte(request)); err != nil {
		t.Fatalf("write upgrade request: %v", err)
	}
	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read upgrade status: %v", err)
	}
	if !strings.Contains(statusLine, "101 Switching Protocols") {
		t.Fatalf("unexpected upgrade status: %q", statusLine)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatalf("read upgrade headers: %v", readErr)
		}
		if line == "\r\n" {
			break
		}
	}
	select {
	case <-upstreamUpgraded:
	case <-time.After(time.Second):
		t.Fatal("upstream upgrade did not complete")
	}

	// httputil.ReverseProxy owns the request-context cancellation watcher for
	// 101 upgrades. Closing the client must close the backend and let the
	// handler's request lifecycle finish without a late copier.
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}
	select {
	case <-upstreamClosed:
	case <-time.After(time.Second):
		t.Fatal("reverse upgrade backend was not closed after client cancellation")
	}
	select {
	case logEvent := <-emitter.logCh:
		if logEvent.HTTPStatus != http.StatusSwitchingProtocols {
			t.Fatalf("HTTPStatus: got %d, want %d", logEvent.HTTPStatus, http.StatusSwitchingProtocols)
		}
	case <-time.After(time.Second):
		t.Fatal("reverse upgrade handler did not finish after client cancellation")
	}
}

func TestForwardProxy_CONNECTTunnelSemantics(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()

	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		conn, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn) // echo
	}()

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})
	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	proxyAddr := strings.TrimPrefix(proxySrv.URL, "http://")
	clientConn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()

	targetAddr := targetLn.Addr().String()
	req := fmt.Sprintf(
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n",
		targetAddr,
		targetAddr,
		basicAuth("plat", "tok"),
	)
	if _, err := clientConn.Write([]byte(req)); err != nil {
		t.Fatalf("write connect request: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	if !strings.Contains(statusLine, "200 Connection Established") {
		t.Fatalf("unexpected CONNECT status line: %q", statusLine)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "x-resin-error:") {
			t.Fatalf("unexpected HTTP semantic error after CONNECT success: %q", line)
		}
	}

	const payload = "ping-through-tunnel"
	if _, err := clientConn.Write([]byte(payload)); err != nil {
		t.Fatalf("write tunneled payload: %v", err)
	}
	echo := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, echo); err != nil {
		t.Fatalf("read tunneled echo: %v", err)
	}
	if got := string(echo); got != payload {
		t.Fatalf("echo payload: got %q, want %q", got, payload)
	}

	_ = clientConn.Close()
	<-targetDone

	select {
	case logEv := <-emitter.logCh:
		if !logEv.NetOK {
			t.Fatal("CONNECT log net_ok: got false, want true")
		}
		if logEv.EgressBytes != int64(len(payload)) {
			t.Fatalf("EgressBytes: got %d, want %d", logEv.EgressBytes, len(payload))
		}
		if logEv.IngressBytes != int64(len(payload)) {
			t.Fatalf("IngressBytes: got %d, want %d", logEv.IngressBytes, len(payload))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected CONNECT log event")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if health.resultCalls.Load() > 0 {
			if health.lastSuccess.Load() != 1 {
				t.Fatalf("RecordResult lastSuccess: got %d, want 1", health.lastSuccess.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected RecordResult call for CONNECT success")
}

func TestForwardProxy_CONNECTPreservesBufferedClientBytesAfterHijack(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()

	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		conn, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})
	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	proxyAddr := strings.TrimPrefix(proxySrv.URL, "http://")
	clientConn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()

	targetAddr := targetLn.Addr().String()
	const payload = "prefetched-before-connect-response"
	req := fmt.Sprintf(
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n%s",
		targetAddr,
		targetAddr,
		basicAuth("plat", "tok"),
		payload,
	)
	if _, err := clientConn.Write([]byte(req)); err != nil {
		t.Fatalf("write coalesced connect request: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	if !strings.Contains(statusLine, "200 Connection Established") {
		t.Fatalf("unexpected CONNECT status line: %q", statusLine)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}

	echo := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, echo); err != nil {
		t.Fatalf("read tunneled echo: %v", err)
	}
	if got := string(echo); got != payload {
		t.Fatalf("echo payload: got %q, want %q", got, payload)
	}

	_ = clientConn.Close()
	<-targetDone

	select {
	case logEv := <-emitter.logCh:
		if !logEv.NetOK {
			t.Fatal("coalesced CONNECT should log net_ok=true")
		}
		if logEv.EgressBytes != int64(len(payload)) {
			t.Fatalf("EgressBytes: got %d, want %d", logEv.EgressBytes, len(payload))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected CONNECT log event")
	}
}

func TestForwardProxy_CONNECTClientCanceledBeforeResponse(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})

	req := httptest.NewRequest(http.MethodConnect, "http://example.com:443", nil)
	req.Host = "example.com:443"
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	fp.ServeHTTP(w, req)

	select {
	case logEv := <-emitter.logCh:
		if !logEv.NetOK {
			t.Fatal("client-canceled CONNECT should log net_ok=true")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected CONNECT log event")
	}

	time.Sleep(50 * time.Millisecond)
	if health.resultCalls.Load() != 0 {
		t.Fatalf("client-canceled CONNECT should not record health result, got %d calls", health.resultCalls.Load())
	}
}

func TestForwardProxy_CONNECTContextCancelBeforeTrafficDoesNotPenalizeNode(t *testing.T) {
	env := newProxyE2EEnv(t)
	upstreamConn, upstreamPeer := net.Pipe()
	defer upstreamPeer.Close()
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return upstreamConn, nil
	})

	targetHealth := &mockPassiveHealthRecorder{}
	healthOwner := NewHealthWriteOwner(targetHealth)
	defer healthOwner.CloseAndWait()
	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     healthOwner,
	})

	request := httptest.NewRequest(http.MethodConnect, "http://example.com:443", nil)
	request.Host = "example.com:443"
	request.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))
	requestCtx, cancelRequest := context.WithCancel(request.Context())
	defer cancelRequest()
	request = request.WithContext(requestCtx)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	writer := &hijackTestResponseWriter{
		conn: serverConn,
		rw:   bufio.NewReadWriter(bufio.NewReader(serverConn), bufio.NewWriter(serverConn)),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fp.ServeHTTP(writer, request)
	}()

	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT status: %v", err)
	}
	if !strings.Contains(statusLine, "200 Connection Established") {
		t.Fatalf("unexpected CONNECT status: %q", statusLine)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatalf("read CONNECT headers: %v", readErr)
		}
		if line == "\r\n" {
			break
		}
	}

	// No bytes have crossed either direction. Cancellation must close both
	// copies and release the upstream lease, but it is not an upstream node
	// failure and must not call RecordPassiveResultForEntry(false).
	cancelRequest()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Forward CONNECT did not return after request cancellation")
	}
	healthOwner.CloseAndWait()
	if got := targetHealth.passiveCalls.Load(); got != 0 {
		t.Fatalf("canceled CONNECT recorded %d passive health results, want 0", got)
	}
}

func TestForwardProxy_CONNECTResponseFlushFailureDoesNotPenalizeNode(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	upstreamConn, upstreamPeer := net.Pipe()
	defer upstreamPeer.Close()

	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return upstreamConn, nil
	})

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})

	req := httptest.NewRequest(http.MethodConnect, "http://example.com:443", nil)
	req.Host = "example.com:443"
	req.Header.Set("Proxy-Authorization", basicAuth("plat", "tok"))

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	failingConn := &failOnWriteConn{Conn: serverConn, failAt: 1}
	w := &hijackTestResponseWriter{
		conn: failingConn,
		rw:   bufio.NewReadWriter(bufio.NewReader(failingConn), bufio.NewWriter(failingConn)),
	}

	fp.ServeHTTP(w, req)

	select {
	case logEv := <-emitter.logCh:
		if logEv.NetOK {
			t.Fatal("CONNECT response flush failure should log net_ok=false")
		}
		if logEv.ResinError != ErrUpstreamRequestFailed.ResinError {
			t.Fatalf("ResinError: got %q, want %q", logEv.ResinError, ErrUpstreamRequestFailed.ResinError)
		}
		if logEv.UpstreamStage != "connect_client_response_flush" {
			t.Fatalf("UpstreamStage: got %q, want %q", logEv.UpstreamStage, "connect_client_response_flush")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected CONNECT log event")
	}

	time.Sleep(50 * time.Millisecond)
	if health.resultCalls.Load() != 0 {
		t.Fatalf("CONNECT response flush failure should not record health result, got %d calls", health.resultCalls.Load())
	}
}

func TestForwardProxy_CONNECTZeroTrafficMarkedFailed(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()

	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		conn, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})
	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	proxyAddr := strings.TrimPrefix(proxySrv.URL, "http://")
	clientConn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}

	targetAddr := targetLn.Addr().String()
	req := fmt.Sprintf(
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n",
		targetAddr,
		targetAddr,
		basicAuth("plat", "tok"),
	)
	if _, err := clientConn.Write([]byte(req)); err != nil {
		t.Fatalf("write connect request: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	if !strings.Contains(statusLine, "200 Connection Established") {
		t.Fatalf("unexpected CONNECT status line: %q", statusLine)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	_ = clientConn.Close()
	<-targetDone

	select {
	case logEv := <-emitter.logCh:
		if logEv.HTTPStatus != http.StatusOK {
			t.Fatalf("HTTPStatus: got %d, want %d", logEv.HTTPStatus, http.StatusOK)
		}
		if logEv.NetOK {
			t.Fatal("CONNECT zero-traffic log net_ok: got true, want false")
		}
		if logEv.ResinError != "UPSTREAM_REQUEST_FAILED" {
			t.Fatalf("CONNECT zero-traffic resin_error: got %q, want %q", logEv.ResinError, "UPSTREAM_REQUEST_FAILED")
		}
		if logEv.UpstreamStage != "connect_zero_traffic" {
			t.Fatalf("CONNECT zero-traffic upstream_stage: got %q, want %q", logEv.UpstreamStage, "connect_zero_traffic")
		}
		if logEv.EgressBytes != 0 || logEv.IngressBytes != 0 {
			t.Fatalf("CONNECT zero-traffic bytes: ingress=%d egress=%d, want both 0", logEv.IngressBytes, logEv.EgressBytes)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected CONNECT log event")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if health.resultCalls.Load() > 0 {
			if health.lastSuccess.Load() != 0 {
				t.Fatalf("RecordResult lastSuccess: got %d, want 0", health.lastSuccess.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected RecordResult call for CONNECT zero-traffic failure")
}

func TestForwardProxy_CONNECTHalfTrafficNotMarkedZeroTraffic(t *testing.T) {
	env := newProxyE2EEnv(t)
	emitter := newMockEventEmitter()
	health := &mockHealthRecorder{}

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()

	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		conn, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("server-push"))
	}()

	fp := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     health,
		Events:     emitter,
	})
	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	proxyAddr := strings.TrimPrefix(proxySrv.URL, "http://")
	clientConn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()

	targetAddr := targetLn.Addr().String()
	req := fmt.Sprintf(
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n",
		targetAddr,
		targetAddr,
		basicAuth("plat", "tok"),
	)
	if _, err := clientConn.Write([]byte(req)); err != nil {
		t.Fatalf("write connect request: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	if !strings.Contains(statusLine, "200 Connection Established") {
		t.Fatalf("unexpected CONNECT status line: %q", statusLine)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}

	payload := make([]byte, len("server-push"))
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatalf("read tunnel payload: %v", err)
	}
	if string(payload) != "server-push" {
		t.Fatalf("payload: got %q, want %q", string(payload), "server-push")
	}
	_ = clientConn.Close()
	<-targetDone

	select {
	case logEv := <-emitter.logCh:
		if logEv.NetOK {
			t.Fatal("CONNECT half-traffic log net_ok: got true, want false")
		}
		if logEv.UpstreamStage != "connect_no_egress_traffic" {
			t.Fatalf("CONNECT half-traffic upstream_stage: got %q, want %q", logEv.UpstreamStage, "connect_no_egress_traffic")
		}
		if logEv.UpstreamStage == "connect_zero_traffic" {
			t.Fatal("CONNECT half-traffic must not be marked as connect_zero_traffic")
		}
		if logEv.IngressBytes == 0 || logEv.EgressBytes != 0 {
			t.Fatalf("CONNECT half-traffic bytes: ingress=%d egress=%d, want ingress>0 and egress=0", logEv.IngressBytes, logEv.EgressBytes)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected CONNECT log event")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if health.resultCalls.Load() > 0 {
			if health.lastSuccess.Load() != 0 {
				t.Fatalf("RecordResult lastSuccess: got %d, want 0", health.lastSuccess.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected RecordResult call for CONNECT half-traffic failure")
}
