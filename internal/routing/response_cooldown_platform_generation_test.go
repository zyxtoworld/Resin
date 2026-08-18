package routing

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestRouter_LateResponseAfterRealPlatformReplaceCannotCoolNewGeneration(t *testing.T) {
	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"platform-generation-cooldown-sub",
		"Platform Generation Cooldown",
		"https://example.invalid/subscription",
		true,
		false,
	)
	subManager.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := json.RawMessage(`{"type":"platform-generation-cooldown"}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
	pool.AddNodeFromSub(hash, raw, sub.ID)

	entry, ok := pool.GetEntry(hash)
	if !ok || entry == nil {
		t.Fatal("setup: node entry was not added")
	}
	entry.CircuitOpenSince.Store(0)
	entry.SetEgressIP(netip.MustParseAddr("198.51.100.240"))
	entry.LatencyTable.Update("example.com", 50*time.Millisecond, time.Minute)
	noop := testutil.NewNoopOutbound()
	entry.Outbound.Store(&noop)

	oldPlatform := platform.NewPlatform(
		"platform-generation-cooldown",
		"Old Platform Generation",
		nil,
		nil,
	)
	if err := pool.RegisterPlatform(oldPlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if !oldPlatform.View().Contains(hash) {
		t.Fatal("setup: old platform view did not contain the routable node")
	}

	router := newTestRouter(pool, nil)
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	router.clock = func() time.Time { return now }
	oldRoute, err := router.RouteRequestForProxy(oldPlatform.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("route on old platform: %v", err)
	}

	checked := make(chan struct{})
	allowMark := make(chan struct{})
	router.beforeResponseCooldownMarkHook = func() {
		close(checked)
		<-allowMark
	}
	defer func() { router.beforeResponseCooldownMarkHook = nil }()

	quarantineDone := make(chan struct{})
	go func() {
		router.QuarantineRoute(oldRoute, platform.ResponseRuleScopeEgressIP, now.Add(time.Minute))
		close(quarantineDone)
	}()
	select {
	case <-checked:
	case <-time.After(time.Second):
		close(allowMark)
		t.Fatal("old response did not reach the cooldown publication barrier")
	}

	newPlatform := platform.NewPlatform(
		oldPlatform.ID,
		"New Platform Generation",
		nil,
		nil,
	)
	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(newPlatform) }()
	select {
	case err := <-replaceDone:
		if err != nil {
			close(allowMark)
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(time.Second):
		close(allowMark)
		t.Fatal("real platform replacement did not complete while the old response was paused")
	}

	close(allowMark)
	select {
	case <-quarantineDone:
	case <-time.After(time.Second):
		t.Fatal("late old response did not finish after platform replacement")
	}

	newRoute, err := router.RouteRequestForProxy(newPlatform.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("new platform generation was incorrectly cooled by the old response: %v", err)
	}
	if newRoute.PlatformID != newPlatform.ID || newRoute.selectedEntry != entry {
		t.Fatalf("new platform route = %+v, want platform %q and entry %p", newRoute, newPlatform.ID, entry)
	}
}
