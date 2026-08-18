package routing

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
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

func TestCommitRouteForAccount_RejectsPlatformReplacementBetweenValidationAndLeaseWrite(t *testing.T) {
	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"commit-route-platform-generation-sub",
		"Commit Route Platform Generation",
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
	raw := json.RawMessage(`{"type":"commit-route-platform-generation"}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
	pool.AddNodeFromSub(hash, raw, sub.ID)

	entry, ok := pool.GetEntry(hash)
	if !ok || entry == nil {
		t.Fatal("setup: node entry was not added")
	}
	entry.CircuitOpenSince.Store(0)
	entry.SetEgressIP(netip.MustParseAddr("198.51.100.241"))
	entry.LatencyTable.Update("example.com", 50*time.Millisecond, time.Minute)
	noop := testutil.NewNoopOutbound()
	entry.Outbound.Store(&noop)

	oldPlatform := platform.NewPlatform(
		"commit-route-platform-generation",
		"Commit Route Old Platform",
		nil,
		nil,
	)
	oldPlatform.StickyTTLNs = int64(time.Hour)
	if err := pool.RegisterPlatform(oldPlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if !oldPlatform.View().Contains(hash) {
		t.Fatal("setup: old platform view did not contain the routable node")
	}

	var events []LeaseEvent
	router := newTestRouter(pool, func(event LeaseEvent) {
		events = append(events, event)
	})
	route, err := router.RouteRequestForProxy(oldPlatform.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if route.selectedEntry != entry {
		t.Fatal("initial route did not retain the exact entry generation")
	}
	if route.platform == nil {
		t.Fatal("initial route did not retain the platform generation")
	}
	if current, ok := pool.GetPlatform(oldPlatform.ID); !ok || current != route.platform {
		t.Fatalf("initial route platform = %p, current = %p, ok=%v", route.platform, current, ok)
	}

	checked := make(chan struct{})
	allowCommit := make(chan struct{})
	router.beforeStickyLeaseCommitOwnerHook = func() {
		close(checked)
		<-allowCommit
	}
	defer func() { router.beforeStickyLeaseCommitOwnerHook = nil }()

	commitDone := make(chan bool, 1)
	go func() {
		commitDone <- router.CommitRouteForAccount(route, "commit-route-account")
	}()
	select {
	case <-checked:
	case <-time.After(time.Second):
		select {
		case committed := <-commitDone:
			t.Fatalf("commit returned before the post-validation barrier: %v", committed)
		default:
		}
		t.Fatal("commit did not reach the post-validation barrier")
	}

	newPlatform := platform.NewPlatform(
		oldPlatform.ID,
		"Commit Route New Platform",
		nil,
		nil,
	)
	newPlatform.StickyTTLNs = oldPlatform.StickyTTLNs
	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(newPlatform) }()
	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(time.Second):
		close(allowCommit)
		t.Fatal("platform replacement did not complete while commit was paused")
	}

	close(allowCommit)
	if committed := <-commitDone; committed {
		t.Fatal("stale route committed a sticky lease after platform replacement")
	}
	if got := router.ReadLease(model.LeaseKey{
		PlatformID: oldPlatform.ID,
		Account:    "commit-route-account",
	}); got != nil {
		t.Fatalf("stale route polluted the replacement platform state: %+v", got)
	}
	if len(events) != 0 {
		t.Fatalf("stale route emitted lease events: %+v", events)
	}
	if current, ok := pool.GetPlatform(oldPlatform.ID); !ok || current != newPlatform {
		t.Fatal("replacement platform was not the published generation")
	}
}

func TestCommitRouteForAccount_RejectsEntryReplacementBetweenValidationAndLeaseWrite(t *testing.T) {
	p := newRouterTestPool()
	plat := platform.NewPlatform("commit-route-entry-generation", "Commit Route Entry Generation", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	p.addPlatform(plat)
	raw := `{"type":"commit-route-entry-generation"}`
	hash, oldEntry := newRoutableEntry(t, raw, "198.51.100.242")
	p.addEntry(hash, oldEntry)
	p.rebuildPlatformView(plat)

	events := make([]LeaseEvent, 0, 1)
	router := newTestRouter(p, func(event LeaseEvent) { events = append(events, event) })
	route, err := router.RouteRequestForProxy(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if route.selectedEntry != oldEntry {
		t.Fatal("initial route did not retain the old entry generation")
	}

	checked := make(chan struct{})
	allowCommit := make(chan struct{})
	router.beforeStickyLeaseCommitOwnerHook = func() {
		close(checked)
		<-allowCommit
	}
	defer func() { router.beforeStickyLeaseCommitOwnerHook = nil }()

	commitDone := make(chan bool, 1)
	go func() { commitDone <- router.CommitRouteForAccount(route, "entry-generation-account") }()
	select {
	case <-checked:
	case <-time.After(time.Second):
		close(allowCommit)
		t.Fatal("commit did not reach the owner admission barrier")
	}

	newEntry := newHealthyEntryForHash(t, hash, []byte(raw), "198.51.100.243")
	p.addEntry(hash, newEntry)
	close(allowCommit)
	if committed := <-commitDone; committed {
		t.Fatal("stale entry generation committed a sticky lease")
	}
	if got := router.ReadLease(model.LeaseKey{
		PlatformID: plat.ID,
		Account:    "entry-generation-account",
	}); got != nil {
		t.Fatalf("stale entry generation polluted routing state: %+v", got)
	}
	if len(events) != 0 {
		t.Fatalf("stale entry generation emitted lease events: %+v", events)
	}
	if current, ok := p.GetEntry(hash); !ok || current != newEntry {
		t.Fatal("replacement entry was not the current pool generation")
	}
}

func TestCommitRouteForAccount_RejectsChangedEgressIPBeforeLeaseWrite(t *testing.T) {
	p := newRouterTestPool()
	plat := platform.NewPlatform("commit-route-egress-generation", "Commit Route Egress Generation", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	p.addPlatform(plat)
	raw := `{"type":"commit-route-egress-generation"}`
	hash, entry := newRoutableEntry(t, raw, "198.51.100.244")
	p.addEntry(hash, entry)
	p.rebuildPlatformView(plat)

	events := make([]LeaseEvent, 0, 1)
	router := newTestRouter(p, func(event LeaseEvent) { events = append(events, event) })
	route, err := router.RouteRequestForProxy(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if route.EgressIP != netip.MustParseAddr("198.51.100.244") {
		t.Fatalf("initial route egress = %s, want 198.51.100.244", route.EgressIP)
	}

	checked := make(chan struct{})
	allowCommit := make(chan struct{})
	router.beforeStickyLeaseCommitOwnerHook = func() {
		close(checked)
		<-allowCommit
	}
	defer func() { router.beforeStickyLeaseCommitOwnerHook = nil }()

	commitDone := make(chan bool, 1)
	go func() { commitDone <- router.CommitRouteForAccount(route, "egress-generation-account") }()
	select {
	case <-checked:
	case <-time.After(time.Second):
		close(allowCommit)
		t.Fatal("commit did not reach the owner admission barrier")
	}

	entry.SetEgressIP(netip.MustParseAddr("198.51.100.245"))
	close(allowCommit)
	if committed := <-commitDone; committed {
		t.Fatal("route committed an obsolete egress IP")
	}
	if got := router.ReadLease(model.LeaseKey{
		PlatformID: plat.ID,
		Account:    "egress-generation-account",
	}); got != nil {
		t.Fatalf("obsolete egress IP polluted routing state: %+v", got)
	}
	if len(events) != 0 {
		t.Fatalf("obsolete egress IP emitted lease events: %+v", events)
	}
}

func TestCommitRouteForAccount_FailsClosedWithoutGenerationOwners(t *testing.T) {
	p := newRouterTestPool()
	plat := platform.NewPlatform("commit-route-owner-required", "Commit Route Owner Required", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	p.addPlatform(plat)
	raw := `{"type":"commit-route-owner-required"}`
	hash, entry := newRoutableEntry(t, raw, "198.51.100.246")
	p.addEntry(hash, entry)
	p.rebuildPlatformView(plat)

	route, err := newTestRouter(p, nil).RouteRequestForProxy(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}

	var events []LeaseEvent
	router := newTestRouter(&poolAccessorOnly{inner: p}, func(event LeaseEvent) {
		events = append(events, event)
	})
	if router.CommitRouteForAccount(route, "owner-required-account") {
		t.Fatal("sticky lease committed without both generation owners")
	}
	if got := router.ReadLease(model.LeaseKey{
		PlatformID: plat.ID,
		Account:    "owner-required-account",
	}); got != nil {
		t.Fatalf("owner-less pool received a sticky lease: %+v", got)
	}
	if len(events) != 0 {
		t.Fatalf("owner-less pool emitted lease events: %+v", events)
	}
}
