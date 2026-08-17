package proxy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

type requestLogServiceEmitter struct {
	service *requestlog.Service
}

func (e requestLogServiceEmitter) EmitRequestFinished(proxy.RequestFinishedEvent) {}

func (e requestLogServiceEmitter) EmitRequestLog(entry proxy.RequestLogEntry) {
	e.service.EmitRequestLog(entry)
}

func TestProxyRequestLogDatabaseContainsNoCredentialProjectionValues(t *testing.T) {
	logRepo := requestlog.NewRepo(t.TempDir(), 1<<20, 2)
	if err := logRepo.Open(); err != nil {
		t.Fatalf("open requestlog repo: %v", err)
	}
	t.Cleanup(func() { _ = logRepo.Close() })
	logService := requestlog.NewService(requestlog.ServiceConfig{
		Repo:          logRepo,
		QueueSize:     8,
		FlushBatch:    8,
		FlushInterval: time.Hour,
	})
	logService.Start()

	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
	platformConfig := platform.NewPlatform("platform-id", "plat", nil, nil)
	platformConfig.StickyTTLNs = int64(time.Hour)
	platformConfig.ReverseProxyMissAction = "TREAT_AS_EMPTY"
	pool.RegisterPlatform(platformConfig)

	sub := subscription.NewSubscription("sub-1", "sub-1", "https://example.com", true, false)
	subManager.Register(sub)
	raw := json.RawMessage(`{"type":"stub","server":"127.0.0.1","server_port":1}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node not found in pool")
	}
	outboundManager := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	outboundManager.EnsureNodeOutbound(hash)
	t.Cleanup(func() { outboundManager.RetireAllOutboundsAndWait() })
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	entry.LatencyTable.Update("example.com", 20*time.Millisecond, 10*time.Minute)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return []string{"example.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
	})
	emitter := requestLogServiceEmitter{service: logService}
	events := proxy.ConfigAwareEventEmitter{
		Base: emitter,
		RequestLogConfigProvider: func() proxy.RequestLogRuntimeConfig {
			return proxy.RequestLogRuntimeConfig{
				Enabled:             true,
				DetailEnabled:       true,
				ReqHeadersMaxBytes:  -1,
				ReqBodyMaxBytes:     -1,
				RespHeadersMaxBytes: -1,
				RespBodyMaxBytes:    -1,
			}
		},
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" && got != "Bearer request-secret" {
			t.Errorf("upstream Authorization: got %q, want original credential", got)
		}
		if values := strings.Join(r.Header.Values("Cookie"), "\n"); values != "" {
			if !strings.Contains(values, "session=request-secret-1") || !strings.Contains(values, "other=request-secret-2") {
				t.Errorf("upstream Cookie values changed: %q", values)
			}
		}
		w.Header().Add("Set-Cookie", "session=upstream-secret")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	forward := proxy.NewForwardProxy(proxy.ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     router,
		Pool:       pool,
		Events:     events,
	})
	target, err := url.Parse(upstream.URL + "/private?trace=1")
	if err != nil {
		t.Fatalf("parse target: %v", err)
	}
	target.User = url.UserPassword("alice", "target-secret")
	forwardReq := httptest.NewRequest(http.MethodGet, target.String(), nil)
	forwardReq.Header.Set("Proxy-Authorization", "Basic cGxhdDp0b2s=")
	forwardResp := httptest.NewRecorder()
	forward.ServeHTTP(forwardResp, forwardReq)
	if forwardResp.Code != http.StatusOK {
		t.Fatalf("forward status: got %d, body=%q", forwardResp.Code, forwardResp.Body.String())
	}

	reverse := proxy.NewReverseProxy(proxy.ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         router,
		Pool:           pool,
		PlatformLookup: pool,
		Events:         events,
	})
	host := strings.TrimPrefix(upstream.URL, "http://")
	reverseReq := httptest.NewRequest(http.MethodGet, "/tok/plat:acct/http/"+host+"/secure", nil)
	reverseReq.Header["aUtHoRiZaTiOn"] = []string{"Bearer request-secret"}
	reverseReq.Header["cOoKiE"] = []string{"session=request-secret-1", "other=request-secret-2"}
	reverseReq.Header.Set("X-Trace-Id", "keep-this-header")
	reverseResp := httptest.NewRecorder()
	reverse.ServeHTTP(reverseResp, reverseReq)
	if reverseResp.Code != http.StatusOK {
		t.Fatalf("reverse status: got %d, body=%q", reverseResp.Code, reverseResp.Body.String())
	}

	if err := logService.StopContext(context.Background()); err != nil {
		t.Fatalf("stop requestlog service: %v", err)
	}
	rows, _, _, err := logRepo.List(requestlog.ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list request logs: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("request log rows: got %d, want 2", len(rows))
	}
	foundOrdinaryHeader := false
	for _, row := range rows {
		if strings.Contains(row.TargetURL, "target-secret") {
			t.Fatalf("request log target URL leaked credential: %q", row.TargetURL)
		}
		payload, err := logRepo.GetPayloads(row.ID)
		if err != nil {
			t.Fatalf("get payload for %s: %v", row.ID, err)
		}
		if payload == nil {
			continue
		}
		allPayload := string(payload.ReqHeaders) + string(payload.RespHeaders)
		for _, secret := range []string{"request-secret", "upstream-secret", "target-secret"} {
			if strings.Contains(allPayload, secret) {
				t.Fatalf("request log payload leaked %q: %q", secret, allPayload)
			}
		}
		foundOrdinaryHeader = foundOrdinaryHeader || strings.Contains(allPayload, "keep-this-header")
	}
	if !foundOrdinaryHeader {
		t.Fatal("request log payload lost the ordinary X-Trace-Id header")
	}
}
