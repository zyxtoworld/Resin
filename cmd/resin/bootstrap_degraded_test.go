package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/api"
	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
)

type bootstrapRecoveryBuilder struct {
	fail   atomic.Bool
	builds atomic.Int32
	closes atomic.Int32
}

func (b *bootstrapRecoveryBuilder) Build(raw json.RawMessage) (adapter.Outbound, error) {
	b.builds.Add(1)
	if b.fail.Load() && strings.Contains(string(raw), "bootstrap-bad") {
		return nil, errors.New("unknown uTLS fingerprint")
	}
	return &bootstrapCleanupTrackingOutbound{closes: &b.closes}, nil
}

func bootstrapNodeFixture(t *testing.T, raws map[string]json.RawMessage) (
	*state.StateEngine,
	*topology.SubscriptionManager,
	*topology.GlobalNodePool,
	*config.EnvConfig,
) {
	t.Helper()
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "bootstrap-degraded-sub"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "Bootstrap degraded",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(time.Hour),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	static := make([]model.NodeStatic, 0, len(raws))
	bindings := make([]model.SubscriptionNode, 0, len(raws))
	for name, raw := range raws {
		hash := node.HashFromRawOptions(raw)
		static = append(static, model.NodeStatic{
			Hash:        hash.Hex(),
			RawOptions:  raw,
			CreatedAtNs: now,
		})
		bindings = append(bindings, model.SubscriptionNode{
			SubscriptionID: subID,
			NodeHash:       hash.Hex(),
			Tags:           []string{name},
		})
	}
	if err := engine.BulkUpsertNodesStatic(static); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes(bindings); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	dynamics := make([]model.NodeDynamic, 0, len(raws))
	for _, raw := range raws {
		dynamics = append(dynamics, model.NodeDynamic{
			Hash:             node.HashFromRawOptions(raw).Hex(),
			CircuitOpenSince: 0,
		})
	}
	if err := engine.BulkUpsertNodesDynamic(dynamics); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}
	return engine, subManager, pool, envCfg
}

func TestBootstrapNodes_IsolatesBadOutboundAndKeepsGoodRoute(t *testing.T) {
	goodRaw := json.RawMessage(`{"type":"bootstrap-good"}`)
	badRaw := json.RawMessage(`{"type":"bootstrap-bad"}`)
	engine, subManager, pool, envCfg := bootstrapNodeFixture(t, map[string]json.RawMessage{
		"good": goodRaw,
		"bad":  badRaw,
	})

	builder := &bootstrapRecoveryBuilder{}
	builder.fail.Store(true)
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	if err := bootstrapNodes(
		engine,
		pool,
		subManager,
		outboundMgr,
		envCfg,
		config.NewDefaultRuntimeConfig().LatencyAuthorities,
	); err != nil {
		t.Fatalf("mixed bootstrap must remain available: %v", err)
	}
	t.Cleanup(outboundMgr.RetireAllOutboundsAndWait)

	goodHash := node.HashFromRawOptions(goodRaw)
	badHash := node.HashFromRawOptions(badRaw)
	goodEntry, ok := pool.GetEntry(goodHash)
	if !ok || !goodEntry.HasOutbound() {
		t.Fatal("healthy bootstrap node did not publish an outbound")
	}
	badEntry, ok := pool.GetEntry(badHash)
	if !ok {
		t.Fatal("bad bootstrap node disappeared")
	}
	if badEntry.HasOutbound() {
		t.Fatal("bad bootstrap node published an outbound")
	}
	if !strings.Contains(badEntry.GetLastError(), "unknown uTLS fingerprint") {
		t.Fatalf("bad bootstrap error = %q, want visible build error", badEntry.GetLastError())
	}

	// A routable node also needs the same probe-derived fields that production
	// uses before a platform view can select it.
	goodEntry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	badEntry.SetEgressIP(netip.MustParseAddr("203.0.113.11"))
	goodEntry.LatencyTable.Update("example.com", 25*time.Millisecond, time.Hour)
	badEntry.LatencyTable.Update("example.com", 25*time.Millisecond, time.Hour)
	pool.RebuildAllPlatforms()
	selectedHash, selectedEntry, err := pool.PickDefaultPlatformOutbound(context.Background())
	if err != nil {
		t.Fatalf("healthy node was not selectable: %v", err)
	}
	if selectedHash != goodHash || selectedEntry != goodEntry {
		t.Fatalf("selected node = (%s, %p), want healthy (%s, %p)", selectedHash.Hex(), selectedEntry, goodHash.Hex(), goodEntry)
	}
	defaultPlatform, ok := pool.GetPlatform(platform.DefaultPlatformID)
	if !ok || defaultPlatform.View().Contains(badHash) {
		t.Fatal("bad bootstrap node appeared in the routable platform view")
	}

	// Correcting the subscription content must recover through the production
	// refresh path, not by calling the outbound manager directly.
	builder.fail.Store(false)
	pool.SetOnNodeAddedRuntime(func(hash node.Hash, expected *node.NodeEntry) {
		outboundMgr.EnsureNodeOutboundForEntry(hash, expected)
	})
	pool.SetOnNodeRemoved(func(_ node.Hash, entry *node.NodeEntry) {
		outboundMgr.RemoveNodeOutbound(entry)
	})
	refreshed, ok := subManager.Get("bootstrap-degraded-sub")
	if !ok {
		t.Fatal("bootstrap subscription missing before recovery refresh")
	}
	refreshed.SetSourceType(subscription.SourceTypeLocal)
	fixedContent := `{"outbounds":[{"type":"shadowsocks","tag":"bootstrap-fixed","server":"1.2.3.4","server_port":443}]}`
	refreshed.SetContent(fixedContent)
	parsedFixed, err := subscription.ParseGeneralSubscription([]byte(fixedContent))
	if err != nil || len(parsedFixed) != 1 {
		t.Fatalf("parse corrected subscription: %v (nodes=%d)", err, len(parsedFixed))
	}
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subManager,
		Pool:       pool,
	})
	defer scheduler.Stop()
	if admitted, err := scheduler.UpdateSubscriptionContextResult(context.Background(), refreshed); !admitted || err != nil {
		t.Fatalf("corrected subscription refresh = admitted %v, err %v", admitted, err)
	}

	fixedRaw := parsedFixed[0].RawOptions
	fixedHash := node.HashFromRawOptions(fixedRaw)
	fixedEntry, ok := pool.GetEntry(fixedHash)
	deadline := time.Now().Add(2 * time.Second)
	for ok && !fixedEntry.HasOutbound() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !ok || !fixedEntry.HasOutbound() {
		t.Fatal("corrected subscription did not publish a recovered outbound")
	}
	if _, ok := pool.GetEntry(badHash); ok {
		t.Fatal("stale bad node remained after corrected subscription refresh")
	}
	if got := fixedEntry.GetLastError(); got != "" {
		t.Fatalf("recovered node retained LastError %q", got)
	}
	if status := (&resinApp{topoRuntime: &topologyRuntime{pool: pool}}).healthzStatus(); status.Status != "ok" || status.DegradedNodes != 0 {
		t.Fatalf("health remained degraded after recovery: %+v", status)
	}
	fixedEntry.SetEgressIP(netip.MustParseAddr("203.0.113.12"))
	fixedEntry.LatencyTable.Update("example.com", 25*time.Millisecond, time.Hour)
	pool.RecordResultForEntry(fixedHash, fixedEntry, true)
	pool.RebuildAllPlatforms()
	if _, selected, err := pool.PickDefaultPlatformOutbound(context.Background()); err != nil || selected != fixedEntry {
		t.Fatalf("recovered node was not selectable: entry=%p err=%v healthy=%v disabled=%v outbound=%v latency=%v egress=%v subscriptions=%v managed=%v", selected, err, fixedEntry.IsHealthy(), pool.IsNodeDisabled(fixedHash), fixedEntry.HasOutbound(), fixedEntry.HasLatency(), fixedEntry.GetEgressIP(), fixedEntry.SubscriptionIDs(), refreshed.ManagedNodes().Size())
	}
	if !defaultPlatform.View().Contains(fixedHash) {
		t.Fatal("recovered node did not enter the routable platform view")
	}
}

func TestBootstrapNodes_AllBadFailsClosedWithoutBootstrapError(t *testing.T) {
	badRaw := json.RawMessage(`{"type":"bootstrap-bad-only"}`)
	engine, subManager, pool, envCfg := bootstrapNodeFixture(t, map[string]json.RawMessage{
		"bad": badRaw,
	})

	builder := &bootstrapRecoveryBuilder{}
	builder.fail.Store(true)
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	if err := bootstrapNodes(
		engine,
		pool,
		subManager,
		outboundMgr,
		envCfg,
		config.NewDefaultRuntimeConfig().LatencyAuthorities,
	); err != nil {
		t.Fatalf("all-bad bootstrap must start in degraded mode: %v", err)
	}
	t.Cleanup(outboundMgr.RetireAllOutboundsAndWait)

	pool.RebuildAllPlatforms()
	if _, _, err := pool.PickDefaultPlatformOutbound(context.Background()); !errors.Is(err, topology.ErrNoAvailableOutbound) {
		t.Fatalf("all-bad picker error = %v, want ErrNoAvailableOutbound", err)
	}
	badHash := node.HashFromRawOptions(badRaw)
	badEntry, ok := pool.GetEntry(badHash)
	if !ok || !strings.Contains(badEntry.GetLastError(), "unknown uTLS fingerprint") {
		t.Fatalf("all-bad node error = %q, want visible build error", badEntry.GetLastError())
	}
	app := &resinApp{topoRuntime: &topologyRuntime{pool: pool}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	api.HandleHealthzWithStatus(app.healthzStatus).ServeHTTP(rec, req)
	var health api.HealthzStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode degraded health response: %v", err)
	}
	if rec.Code != http.StatusOK || health.Status != "degraded" || health.DegradedNodes != 1 {
		t.Fatalf("degraded health response = status %d %+v, want 200 degraded/1", rec.Code, health)
	}
}
