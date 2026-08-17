package routing

import (
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func TestResponseCooldownsScopesUseIndependentKeys(t *testing.T) {
	cooldowns := NewResponseCooldowns()
	nodeA := node.HashFromRawOptions([]byte(`{"id":"node-a"}`))
	nodeB := node.HashFromRawOptions([]byte(`{"id":"node-b"}`))
	ipA := netip.MustParseAddr("198.51.100.50")
	ipB := netip.MustParseAddr("198.51.100.51")
	until := time.Now().Add(time.Minute)

	cooldowns.Mark(platform.ResponseRuleScopeNode, nodeA, ipA, until)
	if !cooldowns.IsCooling(nodeA, ipA, time.Now()) {
		t.Fatal("node scope did not cool the marked node")
	}
	if cooldowns.IsCooling(nodeB, ipA, time.Now()) {
		t.Fatal("node scope leaked to another node sharing the egress IP")
	}
	if !cooldowns.IsCooling(nodeA, ipB, time.Now()) {
		t.Fatal("node scope did not follow the marked node across an IP argument")
	}

	cooldowns.Mark(platform.ResponseRuleScopeEgressIP, nodeA, ipA, until)
	if !cooldowns.IsCooling(nodeB, ipA, time.Now()) {
		t.Fatal("egress scope did not cool another node on the same IP")
	}
	if cooldowns.IsCooling(nodeB, ipB, time.Now()) {
		t.Fatal("egress scope leaked to a different egress IP")
	}
	if cooldowns.IsCooling(nodeA, ipA, until) {
		t.Fatal("cooldown remained active at its exact deadline")
	}
}

func TestResponseCooldowns_EgressCooldownSurvivesEntryRebuild(t *testing.T) {
	cooldowns := NewResponseCooldowns()
	hash := node.HashFromRawOptions([]byte(`{"id":"egress-generation"}`))
	oldEntry := &node.NodeEntry{}
	newEntry := &node.NodeEntry{}
	ip := netip.MustParseAddr("198.51.100.60")
	now := time.Unix(1234, 0)
	until := now.Add(time.Hour)

	cooldowns.markForEntry(platform.ResponseRuleScopeEgressIP, hash, oldEntry, ip, until, now)
	if !cooldowns.IsCoolingForEntry(hash, oldEntry, ip, now) {
		t.Fatal("old entry was not cooled")
	}
	if !cooldowns.IsCoolingForEntry(hash, newEntry, ip, now) {
		t.Fatal("published IP cooldown did not survive entry rebuild")
	}
	if !cooldowns.IsCooling(hash, ip, now) {
		t.Fatal("generic inspection lost the active exact egress cooldown")
	}

}

func TestResponseCooldowns_IsCoolingRemovesExpiredEntries(t *testing.T) {
	cooldowns := NewResponseCooldowns()
	hash := node.HashFromRawOptions([]byte(`{"id":"expired"}`))
	ip := netip.MustParseAddr("198.51.100.53")
	now := time.Unix(123, 456)

	markNow := now.Add(-time.Second)
	cooldowns.markAt(platform.ResponseRuleScopeNode, hash, ip, now.Add(-time.Nanosecond), markNow)
	cooldowns.markAt(platform.ResponseRuleScopeEgressIP, hash, ip, now.Add(-time.Nanosecond), markNow)

	if cooldowns.IsCooling(hash, ip, now) {
		t.Fatal("expired cooldown was reported as active")
	}
	if _, ok := cooldowns.byNode[hash]; ok {
		t.Fatal("expired node cooldown was retained")
	}
	if _, ok := cooldowns.byEgress[ip]; ok {
		t.Fatal("expired egress cooldown was retained")
	}
}

func TestResponseCooldowns_IsCoolingPrunesExpiredUnrelatedEntries(t *testing.T) {
	cooldowns := NewResponseCooldowns()
	staleHash := node.HashFromRawOptions([]byte(`{"id":"stale-node"}`))
	staleIP := netip.MustParseAddr("198.51.100.54")
	otherHash := node.HashFromRawOptions([]byte(`{"id":"other-node"}`))
	otherIP := netip.MustParseAddr("198.51.100.55")
	now := time.Unix(456, 789)

	markNow := now.Add(-time.Second)
	cooldowns.markAt(platform.ResponseRuleScopeNode, staleHash, staleIP, now.Add(-time.Nanosecond), markNow)
	cooldowns.markAt(platform.ResponseRuleScopeEgressIP, staleHash, staleIP, now.Add(-time.Nanosecond), markNow)

	if cooldowns.IsCooling(otherHash, otherIP, now) {
		t.Fatal("unrelated expired cooldown was reported as active")
	}
	if _, ok := cooldowns.byNode[staleHash]; ok {
		t.Fatal("unrelated expired node cooldown was retained")
	}
	if _, ok := cooldowns.byEgress[staleIP]; ok {
		t.Fatal("unrelated expired egress cooldown was retained")
	}
}

func TestResponseCooldowns_ExtendingCooldownPreservesLatestDeadline(t *testing.T) {
	cooldowns := NewResponseCooldowns()
	hash := node.HashFromRawOptions([]byte(`{"id":"renewed"}`))
	base := time.Unix(789, 123)

	cooldowns.markAt(platform.ResponseRuleScopeNode, hash, netip.Addr{}, base.Add(time.Second), base)
	cooldowns.markAt(platform.ResponseRuleScopeNode, hash, netip.Addr{}, base.Add(10*time.Second), base.Add(500*time.Millisecond))

	if !cooldowns.IsCooling(hash, netip.Addr{}, base.Add(2*time.Second)) {
		t.Fatal("renewed node cooldown was removed by the old expiry")
	}
}

func TestResponseCooldownsConcurrentMarkAndRead(t *testing.T) {
	cooldowns := NewResponseCooldowns()
	hash := node.HashFromRawOptions([]byte(`{"id":"concurrent"}`))
	ip := netip.MustParseAddr("198.51.100.52")

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			scope := platform.ResponseRuleScopeNode
			if i%2 == 0 {
				scope = platform.ResponseRuleScopeEgressIP
			}
			cooldowns.Mark(scope, hash, ip, time.Now().Add(time.Minute))
		}(i)
		go func() {
			defer wg.Done()
			_ = cooldowns.IsCooling(hash, ip, time.Now())
		}()
	}
	wg.Wait()

	if !cooldowns.IsCooling(hash, ip, time.Now()) {
		t.Fatal("concurrent mark did not leave an active cooldown")
	}
}

func TestRouter_ResponseCooldownSkipsEgressIPAndRestoresAfterExpiry(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-response", "Plat-Response", nil, nil)
	pool.addPlatform(plat)

	blockedHash, blockedEntry := newRoutableEntry(t, `{"id":"blocked"}`, "198.51.100.10")
	availableHash, availableEntry := newRoutableEntry(t, `{"id":"available"}`, "198.51.100.11")
	pool.addEntry(blockedHash, blockedEntry)
	pool.addEntry(availableHash, availableEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	router.clock = func() time.Time { return now }
	route := RouteResult{
		PlatformID:    plat.ID,
		NodeHash:      blockedHash,
		EgressIP:      blockedEntry.GetEgressIP(),
		selectedEntry: blockedEntry,
	}
	until := now.Add(time.Minute)
	router.QuarantineRoute(route, platform.ResponseRuleScopeEgressIP, until)

	for i := 0; i < 20; i++ {
		got, err := router.RouteRequest(plat.Name, "", "https://example.com")
		if err != nil {
			t.Fatalf("route %d: %v", i, err)
		}
		if got.NodeHash != availableHash {
			t.Fatalf("route %d selected cooled node %s, want %s", i, got.NodeHash.Hex(), availableHash.Hex())
		}
	}

	if blockedEntry.GetEgressIP() != netip.MustParseAddr("198.51.100.10") {
		t.Fatal("test setup unexpectedly changed blocked egress IP")
	}
	now = until
	seenBlocked := false
	for i := 0; i < 100; i++ {
		got, err := router.RouteRequest(plat.Name, "", "https://example.com")
		if err != nil {
			t.Fatalf("route after expiry %d: %v", i, err)
		}
		if got.NodeHash == blockedHash {
			seenBlocked = true
			break
		}
	}
	if !seenBlocked {
		t.Fatal("cooled node did not become eligible after expiry")
	}
}

func TestRouter_RouteRequestNextDoesNotCreateCooldown(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-retry-only", "Plat-Retry-Only", nil, nil)
	plat.PassiveCircuitBreakerDisabled = true
	pool.addPlatform(plat)

	firstHash, firstEntry := newRoutableEntry(t, `{"id":"retry-only-first"}`, "198.51.100.80")
	secondHash, secondEntry := newRoutableEntry(t, `{"id":"retry-only-second"}`, "198.51.100.81")
	pool.addEntry(firstHash, firstEntry)
	pool.addEntry(secondHash, secondEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	router.nodeTagResolver = func(hash node.Hash, _ *node.NodeEntry) string {
		return "tag:" + hash.Hex()
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	router.clock = func() time.Time { return now }
	initial, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	next, err := router.RouteRequestNext(initial, RouteRetryExclusions{
		Entries:   []*node.NodeEntry{initial.selectedEntry},
		EgressIPs: []netip.Addr{initial.EgressIP},
	})
	if err != nil {
		t.Fatalf("retry-only next route: %v", err)
	}
	if next.NodeHash == initial.NodeHash || next.EgressIP == initial.EgressIP {
		t.Fatalf("retry-only selected an attempted candidate: initial=%+v next=%+v", initial, next)
	}
	if !next.PassiveCircuitBreakerDisabled {
		t.Fatal("retry-only route lost platform passive-circuit-breaker setting")
	}
	if want := "tag:" + next.NodeHash.Hex(); next.NodeTag != want {
		t.Fatalf("retry-only route lost exact node tag: got %q want %q", next.NodeTag, want)
	}
	cooldowns := router.responseCooldowns(plat.ID)
	if cooldowns.IsCoolingForEntry(initial.NodeHash, initial.selectedEntry, initial.EgressIP, now) {
		t.Fatal("retry-only route selection created a cooldown for the attempted entry")
	}
}

func TestRouter_ResponseCooldownPersistsAcrossSameIPEndpointRebuild(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-egress-rebuild", "Plat-Egress-Rebuild", nil, nil)
	pool.addPlatform(plat)

	const raw = `{"id":"egress-rebuild"}`
	hash, oldEntry := newRoutableEntry(t, raw, "198.51.100.70")
	pool.addEntry(hash, oldEntry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	router.clock = func() time.Time { return now }

	initial, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	until := now.Add(time.Minute)
	router.QuarantineRoute(initial, platform.ResponseRuleScopeEgressIP, until)

	newEntry := newHealthyEntryForHash(t, hash, []byte(raw), "198.51.100.70")
	pool.addEntry(hash, newEntry)
	pool.rebuildPlatformView(plat)
	if _, err := router.RouteRequest(plat.Name, "", "https://example.com"); !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("same-IP replacement ignored published cooldown: %v", err)
	}

	now = until
	got, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("route after cooldown expiry: %v", err)
	}
	if got.selectedEntry != newEntry {
		t.Fatalf("route after expiry used entry %p, want rebuilt entry %p", got.selectedEntry, newEntry)
	}
}

func TestRouter_LateOldEgressResponseCannotCoolRebuiltEntry(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-late-egress", "Plat-Late-Egress", nil, nil)
	pool.addPlatform(plat)

	const raw = `{"id":"late-egress"}`
	hash, oldEntry := newRoutableEntry(t, raw, "198.51.100.71")
	pool.addEntry(hash, oldEntry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	router.clock = func() time.Time { return now }

	oldRoute, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	checked := make(chan struct{})
	allow := make(chan struct{})
	router.beforeResponseCooldownMarkHook = func() {
		close(checked)
		<-allow
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
		t.Fatal("old response did not reach the publication barrier")
	}

	newEntry := newHealthyEntryForHash(t, hash, []byte(raw), "198.51.100.71")
	pool.addEntry(hash, newEntry)
	pool.rebuildPlatformView(plat)
	close(allow)
	select {
	case <-quarantineDone:
	case <-time.After(time.Second):
		t.Fatal("late old response did not finish")
	}

	got, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("late old response cooled rebuilt entry: %v", err)
	}
	if got.selectedEntry != newEntry {
		t.Fatalf("late response route used entry %p, want rebuilt entry %p", got.selectedEntry, newEntry)
	}
}

func TestRouter_ResponseCooldownInvalidatesStickyLease(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-sticky-response", "Plat-Sticky-Response", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	blockedHash, blockedEntry := newRoutableEntry(t, `{"id":"sticky-blocked"}`, "198.51.100.20")
	availableHash, availableEntry := newRoutableEntry(t, `{"id":"sticky-available"}`, "198.51.100.21")
	pool.addEntry(blockedHash, blockedEntry)
	pool.addEntry(availableHash, availableEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	first, err := router.RouteRequest(plat.Name, "account-1", "https://example.com")
	if err != nil {
		t.Fatalf("create lease: %v", err)
	}
	state, _ := router.states.Load(plat.ID)
	lease, ok := state.Leases.GetLease("account-1")
	if !ok {
		t.Fatal("expected sticky lease")
	}
	if first.NodeHash != lease.NodeHash {
		t.Fatalf("lease mismatch: route=%s lease=%s", first.NodeHash.Hex(), lease.NodeHash.Hex())
	}

	expectedOther := blockedHash
	if first.NodeHash == blockedHash {
		expectedOther = availableHash
	}
	router.QuarantineRoute(first, platform.ResponseRuleScopeEgressIP, time.Now().Add(time.Minute))
	second, err := router.RouteRequest(plat.Name, "account-1", "https://example.com")
	if err != nil {
		t.Fatalf("route after sticky cooldown: %v", err)
	}
	if second.NodeHash != expectedOther {
		t.Fatalf("sticky route ignored cooldown: first=%s got=%s want=%s", first.NodeHash.Hex(), second.NodeHash.Hex(), expectedOther.Hex())
	}
}

func TestRouter_ResponseCooldownNodeScopePreservesSameIPEgress(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-node-response", "Plat-Node-Response", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	firstHash, firstEntry := newRoutableEntry(t, `{"id":"node-scope-first"}`, "198.51.100.30")
	secondHash, secondEntry := newRoutableEntry(t, `{"id":"node-scope-second"}`, "198.51.100.30")
	pool.addEntry(firstHash, firstEntry)
	pool.addEntry(secondHash, secondEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	initial, err := router.RouteRequest(plat.Name, "node-scope-account", "https://example.com")
	if err != nil {
		t.Fatalf("initial sticky route: %v", err)
	}
	otherHash := secondHash
	if initial.NodeHash == secondHash {
		otherHash = firstHash
	}

	router.QuarantineRoute(initial, platform.ResponseRuleScopeNode, time.Now().Add(time.Minute))
	sticky, err := router.RouteRequest(plat.Name, "node-scope-account", "https://example.com")
	if err != nil {
		t.Fatalf("sticky route after node cooldown: %v", err)
	}
	if sticky.NodeHash != otherHash {
		t.Fatalf("node-scope sticky route: got %s, want same-IP node %s", sticky.NodeHash.Hex(), otherHash.Hex())
	}

	random, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("random route after node cooldown: %v", err)
	}
	if random.NodeHash != otherHash {
		t.Fatalf("node-scope random route: got %s, want same-IP node %s", random.NodeHash.Hex(), otherHash.Hex())
	}
}
