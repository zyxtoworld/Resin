package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func addNodeForNodeListTest(t *testing.T, cp *service.ControlPlaneService, sub *subscription.Subscription, raw string, egressIP string) {
	addNodeForNodeListTestWithTag(t, cp, sub, raw, egressIP, "tag")
}

func addNodeForNodeListTestWithTag(
	t *testing.T,
	cp *service.ControlPlaneService,
	sub *subscription.Subscription,
	raw string,
	egressIP string,
	tag string,
) {
	t.Helper()

	hash := node.HashFromRawOptions([]byte(raw))
	cp.Pool.AddNodeFromSub(hash, []byte(raw), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})

	if egressIP == "" {
		return
	}
	entry, ok := cp.Pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after add", hash.Hex())
	}
	entry.SetEgressIP(netip.MustParseAddr(egressIP))
}

func markNodeHealthyForNodeListTest(t *testing.T, cp *service.ControlPlaneService, raw string) {
	t.Helper()

	hash := node.HashFromRawOptions([]byte(raw))
	entry, ok := cp.Pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after add", hash.Hex())
	}
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.CircuitOpenSince.Store(0)
}

func TestHandleListNodes_TagKeywordFiltersByNodeName(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)

	subA := subscription.NewSubscription("11111111-1111-1111-1111-111111111111", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(subA)

	addNodeForNodeListTestWithTag(t, cp, subA, `{"type":"ss","server":"1.1.1.1","port":443}`, "", "hongkong-fast-01")
	addNodeForNodeListTestWithTag(t, cp, subA, `{"type":"ss","server":"2.2.2.2","port":443}`, "", "japan-slow-01")

	rec := doJSONRequest(
		t,
		srv,
		http.MethodGet,
		"/api/v1/nodes?subscription_id="+subA.ID+"&tag_keyword=FAST",
		nil,
		true,
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("list nodes with tag_keyword status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSONMap(t, rec)
	if body["total"] != float64(1) {
		t.Fatalf("tag_keyword total: got %v, want 1", body["total"])
	}
}

func TestHandleListNodes_UniqueEgressIPsUsesFilteredResult(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)

	subA := subscription.NewSubscription("11111111-1111-1111-1111-111111111111", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("22222222-2222-2222-2222-222222222222", "sub-b", "https://example.com/b", true, false)
	cp.SubMgr.Register(subA)
	cp.SubMgr.Register(subB)

	const rawA1 = `{"type":"ss","server":"1.1.1.1","port":443}`
	const rawA2 = `{"type":"ss","server":"2.2.2.2","port":443}`
	const rawA3 = `{"type":"ss","server":"3.3.3.3","port":443}`
	const rawA4 = `{"type":"ss","server":"4.4.4.4","port":443}`
	const rawB1 = `{"type":"ss","server":"5.5.5.5","port":443}`

	addNodeForNodeListTest(t, cp, subA, rawA1, "203.0.113.10")
	addNodeForNodeListTest(t, cp, subA, rawA2, "203.0.113.10")
	addNodeForNodeListTest(t, cp, subA, rawA3, "203.0.113.11")
	addNodeForNodeListTest(t, cp, subA, rawA4, "")
	addNodeForNodeListTest(t, cp, subB, rawB1, "203.0.113.99")

	// Healthy condition: has outbound + not circuit-open.
	markNodeHealthyForNodeListTest(t, cp, rawA1)
	markNodeHealthyForNodeListTest(t, cp, rawA2)

	rec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/nodes?subscription_id="+subA.ID, nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("list nodes status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSONMap(t, rec)
	if body["total"] != float64(4) {
		t.Fatalf("total: got %v, want 4", body["total"])
	}
	if body["unique_egress_ips"] != float64(2) {
		t.Fatalf("unique_egress_ips: got %v, want 2", body["unique_egress_ips"])
	}
	if body["unique_healthy_egress_ips"] != float64(1) {
		t.Fatalf("unique_healthy_egress_ips: got %v, want 1", body["unique_healthy_egress_ips"])
	}

	rec = doJSONRequest(
		t,
		srv,
		http.MethodGet,
		"/api/v1/nodes?subscription_id="+subA.ID+"&limit=1",
		nil,
		true,
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("list nodes paged status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body = decodeJSONMap(t, rec)
	if body["total"] != float64(4) {
		t.Fatalf("paged total: got %v, want 4", body["total"])
	}
	if body["unique_egress_ips"] != float64(2) {
		t.Fatalf("paged unique_egress_ips: got %v, want 2", body["unique_egress_ips"])
	}
	if body["unique_healthy_egress_ips"] != float64(1) {
		t.Fatalf("paged unique_healthy_egress_ips: got %v, want 1", body["unique_healthy_egress_ips"])
	}

	rec = doJSONRequest(
		t,
		srv,
		http.MethodGet,
		"/api/v1/nodes?subscription_id="+subA.ID+"&egress_ip=203.0.113.10",
		nil,
		true,
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("list nodes with egress filter status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body = decodeJSONMap(t, rec)
	if body["total"] != float64(2) {
		t.Fatalf("filtered total: got %v, want 2", body["total"])
	}
	if body["unique_egress_ips"] != float64(1) {
		t.Fatalf("filtered unique_egress_ips: got %v, want 1", body["unique_egress_ips"])
	}
	if body["unique_healthy_egress_ips"] != float64(1) {
		t.Fatalf("filtered unique_healthy_egress_ips: got %v, want 1", body["unique_healthy_egress_ips"])
	}
}

func TestHandleListNodes_IncludesReferenceLatencyMs(t *testing.T) {
	srv, cp, runtimeCfg := newControlPlaneTestServer(t)

	cfg := config.NewDefaultRuntimeConfig()
	cfg.LatencyAuthorities = []string{"cloudflare.com", "github.com", "google.com"}
	runtimeCfg.Store(cfg)

	subA := subscription.NewSubscription("11111111-1111-1111-1111-111111111111", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(subA)

	raw := `{"type":"ss","server":"1.1.1.1","port":443}`
	hash := node.HashFromRawOptions([]byte(raw))
	addNodeForNodeListTest(t, cp, subA, raw, "203.0.113.10")

	entry, ok := cp.Pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after add", hash.Hex())
	}
	entry.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{
		Ewma:        40 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{
		Ewma:        80 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        10 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	rec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/nodes?subscription_id="+subA.ID, nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("list nodes status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	body := decodeJSONMap(t, rec)
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items mismatch: got %T len=%d", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["reference_latency_ms"] != float64(60) {
		t.Fatalf("reference_latency_ms: got %v, want 60", item["reference_latency_ms"])
	}
}

func TestHandleProbeEgress_ReturnsRegion(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)

	sub := subscription.NewSubscription("11111111-1111-1111-1111-111111111111", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(sub)

	raw := []byte(`{"type":"ss","server":"1.1.1.1","port":443}`)
	hash := node.HashFromRawOptions(raw)
	cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})

	entry, ok := cp.Pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after add", hash.Hex())
	}
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)

	cp.ProbeMgr = probe.NewProbeManager(probe.ProbeConfig{
		Pool: cp.Pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			return []byte("ip=203.0.113.88\nloc=JP"), 25 * time.Millisecond, nil
		},
	})

	rec := doJSONRequest(t, srv, http.MethodPost, "/api/v1/nodes/"+hash.Hex()+"/actions/probe-egress", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("probe-egress status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSONMap(t, rec)
	if body["egress_ip"] != "203.0.113.88" {
		t.Fatalf("egress_ip: got %v, want %q", body["egress_ip"], "203.0.113.88")
	}
	if body["region"] != "jp" {
		t.Fatalf("region: got %v, want %q", body["region"], "jp")
	}
}

func testProbeHandlerHonorsRequestCancellation(
	t *testing.T,
	path string,
	handler func(*service.ControlPlaneService) http.HandlerFunc,
) {
	t.Helper()
	_, cp, _ := newControlPlaneTestServer(t)

	sub := subscription.NewSubscription("11111111-1111-1111-1111-111111111111", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"1.1.1.1","port":443}`)
	hash := node.HashFromRawOptions(raw)
	cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	entry, ok := cp.Pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing after add", hash.Hex())
	}
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.FailureCount.Store(1)
	entry.CircuitOpenSince.Store(0)
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	entry.SetEgressRegion("us")
	beforeStats := node.DomainLatencyStats{
		Ewma:        80 * time.Millisecond,
		LastUpdated: time.Unix(100, 0),
	}
	entry.LatencyTable.LoadEntry("cloudflare.com", beforeStats)
	entry.LatencyTable.LoadEntry("gstatic.com", beforeStats)

	fetchStarted := make(chan struct{})
	mgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: cp.Pool,
		Fetcher: func(ctx context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchStarted)
			<-ctx.Done()
			return nil, 0, ctx.Err()
		},
	})
	cp.ProbeMgr = mgr

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, path, nil).WithContext(requestCtx)
	req.SetPathValue("hash", hash.Hex())
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler(cp).ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("probe handler did not enter the real fetcher")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		// Keep the test bounded under the old implementation so the failure
		// reports the missing request-context propagation instead of hanging.
		mgr.Stop()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("probe handler remained blocked even after manager shutdown")
		}
		t.Fatal("probe handler ignored the canceled request context")
	}
	mgr.Stop()
	if got := entry.FailureCount.Load(); got != 1 {
		t.Fatalf("failure count after request cancellation: got %d, want 1", got)
	}
	if got := entry.CircuitOpenSince.Load(); got != 0 {
		t.Fatalf("circuit state after request cancellation: got %d, want closed", got)
	}
	if got := entry.GetEgressIP(); got != netip.MustParseAddr("203.0.113.10") {
		t.Fatalf("egress IP after request cancellation: got %v, want 203.0.113.10", got)
	}
	if got := entry.GetEgressRegion(); got != "us" {
		t.Fatalf("egress region after request cancellation: got %q, want us", got)
	}
	for _, domain := range []string{"cloudflare.com", "gstatic.com"} {
		got, ok := entry.LatencyTable.GetDomainStats(domain)
		if !ok {
			t.Fatalf("latency for %s disappeared after request cancellation", domain)
		}
		if got != beforeStats {
			t.Fatalf("latency for %s after request cancellation: got %+v, want %+v", domain, got, beforeStats)
		}
	}
}

func TestHandleProbeEgressHonorsRequestCancellation(t *testing.T) {
	testProbeHandlerHonorsRequestCancellation(
		t,
		"/api/v1/nodes/test/actions/probe-egress",
		HandleProbeEgress,
	)
}

func TestHandleProbeLatencyHonorsRequestCancellation(t *testing.T) {
	testProbeHandlerHonorsRequestCancellation(
		t,
		"/api/v1/nodes/test/actions/probe-latency",
		HandleProbeLatency,
	)
}

func TestHandleListNodes_EnabledFilter(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)

	subEnabled := subscription.NewSubscription("11111111-1111-1111-1111-111111111111", "sub-enabled", "https://example.com/a", true, false)
	subDisabled := subscription.NewSubscription("22222222-2222-2222-2222-222222222222", "sub-disabled", "https://example.com/b", false, false)
	cp.SubMgr.Register(subEnabled)
	cp.SubMgr.Register(subDisabled)

	addNodeForNodeListTestWithTag(t, cp, subEnabled, `{"type":"ss","server":"1.1.1.1","port":443}`, "", "enabled-tag")
	addNodeForNodeListTestWithTag(t, cp, subDisabled, `{"type":"ss","server":"2.2.2.2","port":443}`, "", "disabled-tag")

	rec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/nodes?enabled=true", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("enabled=true status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSONMap(t, rec)
	if body["total"] != float64(1) {
		t.Fatalf("enabled=true total: got %v, want 1", body["total"])
	}

	rec = doJSONRequest(t, srv, http.MethodGet, "/api/v1/nodes?enabled=false", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("enabled=false status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body = decodeJSONMap(t, rec)
	if body["total"] != float64(1) {
		t.Fatalf("enabled=false total: got %v, want 1", body["total"])
	}
}
