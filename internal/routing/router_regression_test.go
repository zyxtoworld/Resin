package routing

import (
	"encoding/json"
	"errors"
	"net/netip"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/testutil"
)

type routerTestPool struct {
	mu          sync.RWMutex
	entries     map[node.Hash]*node.NodeEntry
	platsByID   map[string]*platform.Platform
	platsByName map[string]*platform.Platform
}

func newRouterTestPool() *routerTestPool {
	return &routerTestPool{
		entries:     make(map[node.Hash]*node.NodeEntry),
		platsByID:   make(map[string]*platform.Platform),
		platsByName: make(map[string]*platform.Platform),
	}
}

func (p *routerTestPool) addPlatform(plat *platform.Platform) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.platsByID[plat.ID] = plat
	p.platsByName[plat.Name] = plat
}

func (p *routerTestPool) removePlatform(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plat, ok := p.platsByID[id]
	if !ok {
		return
	}
	delete(p.platsByID, id)
	if current, ok := p.platsByName[plat.Name]; ok && current == plat {
		delete(p.platsByName, plat.Name)
	}
}

func (p *routerTestPool) addEntry(h node.Hash, entry *node.NodeEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries[h] = entry
}

func (p *routerTestPool) removeEntry(h node.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.entries, h)
}

func (p *routerTestPool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.entries[hash]
	return e, ok
}

func (p *routerTestPool) WithCurrentEntry(hash node.Hash, expected *node.NodeEntry, fn func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.entries[hash]
	if !ok || current != expected || fn == nil {
		return false
	}
	fn()
	return true
}

func (p *routerTestPool) IsNodeDisabled(node.Hash) bool { return false }

func (p *routerTestPool) GetPlatform(id string) (*platform.Platform, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	plat, ok := p.platsByID[id]
	return plat, ok
}

func (p *routerTestPool) GetPlatformByName(name string) (*platform.Platform, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	plat, ok := p.platsByName[name]
	return plat, ok
}

func (p *routerTestPool) RangePlatforms(fn func(*platform.Platform) bool) {
	p.mu.RLock()
	plats := make([]*platform.Platform, 0, len(p.platsByID))
	for _, plat := range p.platsByID {
		plats = append(plats, plat)
	}
	p.mu.RUnlock()

	for _, plat := range plats {
		if !fn(plat) {
			return
		}
	}
}

func (p *routerTestPool) rebuildPlatformView(plat *platform.Platform) {
	p.mu.RLock()
	snapshot := make(map[node.Hash]*node.NodeEntry, len(p.entries))
	for h, e := range p.entries {
		snapshot[h] = e
	}
	p.mu.RUnlock()

	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) {
			for h, e := range snapshot {
				if !fn(h, e) {
					return
				}
			}
		},
		func(_ string, _ node.Hash) (string, bool, []string, bool) { return "", true, nil, true },
		func(_ netip.Addr) string { return "" },
	)
}

func newRoutableEntry(t *testing.T, raw, ip string) (node.Hash, *node.NodeEntry) {
	t.Helper()
	rawOpts := json.RawMessage(raw)
	h := node.HashFromRawOptions(rawOpts)
	return h, newHealthyEntryForHash(t, h, rawOpts, ip)
}

func newHealthyEntryForHash(t *testing.T, h node.Hash, rawOpts json.RawMessage, ip string) *node.NodeEntry {
	t.Helper()
	e := node.NewNodeEntry(h, rawOpts, time.Now(), 16)
	// Empty platform regex still requires at least one enabled subscription.
	e.AddSubscriptionID("sub-test")

	parsedIP, err := netip.ParseAddr(ip)
	if err != nil {
		t.Fatalf("parse ip %q: %v", ip, err)
	}
	e.SetEgressIP(parsedIP)

	// Keep at least one latency sample so the node remains routable.
	e.LatencyTable.Update("cloudflare.com", 100*time.Millisecond, 10*time.Minute)
	waitForDomainLatency(t, e, "cloudflare.com")

	// Any non-nil outbound value is enough for platform filtering.
	ob := testutil.NewNoopOutbound()
	e.Outbound.Store(&ob)

	return e
}

func waitForDomainLatency(t *testing.T, e *node.NodeEntry, domain string) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.LatencyTable != nil {
			if _, ok := e.LatencyTable.GetDomainStats(domain); ok {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if e.LatencyTable == nil {
		t.Fatalf("latency sample for domain %q did not become visible: latency table is nil", domain)
	}
	t.Fatalf(
		"latency sample for domain %q did not become visible in time: table size=%d",
		domain,
		e.LatencyTable.Size(),
	)
}

func newTestRouter(pool PoolAccessor, onEvent LeaseEventFunc) *Router {
	return NewRouter(RouterConfig{
		Pool:         pool,
		Authorities:  func() []string { return []string{"cloudflare.com"} },
		P2CWindow:    func() time.Duration { return 10 * time.Minute },
		OnLeaseEvent: onEvent,
	})
}

func TestRouteRequestKeepsPublishedViewDuringFullRebuild(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("p-rebuild-gap", "RebuildGap", nil, nil)
	pool.addPlatform(plat)
	h, entry := newRoutableEntry(t, `{"type":"rebuild-gap"}`, "203.0.113.20")
	pool.addEntry(h, entry)
	pool.rebuildPlatformView(plat)
	if got := plat.View().Size(); got != 1 {
		t.Fatalf("setup view size = %d, want 1", got)
	}

	router := newTestRouter(pool, nil)
	routeResolved := make(chan struct{})
	router.afterPlatformResolveHook = func(*platform.Platform) {
		close(routeResolved)
	}

	scanEntered := make(chan struct{})
	releaseScan := make(chan struct{})
	rebuildDone := make(chan struct{})
	go func() {
		plat.FullRebuild(func(fn func(node.Hash, *node.NodeEntry) bool) {
			close(scanEntered)
			<-releaseScan
			fn(h, entry)
		}, func(_ string, _ node.Hash) (string, bool, []string, bool) {
			return "", true, nil, true
		}, func(_ netip.Addr) string { return "" })
		close(rebuildDone)
	}()

	select {
	case <-scanEntered:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not enter pool scan")
	}

	routeDone := make(chan error, 1)
	go func() {
		_, err := router.RouteRequest(plat.Name, "", "https://example.com")
		routeDone <- err
	}()
	select {
	case <-routeResolved:
	case <-time.After(time.Second):
		t.Fatal("route did not resolve platform")
	}
	close(releaseScan)
	select {
	case <-rebuildDone:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not finish after scan release")
	}
	select {
	case err := <-routeDone:
		if err != nil {
			t.Fatalf("route failed while full rebuild was scanning: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("route did not finish after the rebuilt view was published")
	}
}

func TestRouteRequest_SameIPRotationPrefersTargetLatencySample(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-1", "Plat-1", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	currentHash, currentEntry := newRoutableEntry(t, `{"id":"current"}`, "198.51.100.77")
	candidateA, entryA := newRoutableEntry(t, `{"id":"candidate-a"}`, "198.51.100.77")
	candidateB, entryB := newRoutableEntry(t, `{"id":"candidate-b"}`, "198.51.100.77")
	pool.addEntry(currentHash, currentEntry)
	pool.addEntry(candidateA, entryA)
	pool.addEntry(candidateB, entryB)
	pool.rebuildPlatformView(plat)
	if !plat.View().Contains(currentHash) || !plat.View().Contains(candidateA) || !plat.View().Contains(candidateB) {
		t.Fatalf(
			"rebuild did not include expected nodes: size=%d current(lat=%v,out=%v) a(lat=%v,out=%v) b(lat=%v,out=%v)",
			plat.View().Size(),
			currentEntry.HasLatency(), currentEntry.HasOutbound(),
			entryA.HasLatency(), entryA.HasOutbound(),
			entryB.HasLatency(), entryB.HasOutbound(),
		)
	}

	// Force lease invalidation while keeping same-IP candidates in view.
	currentEntry.CircuitOpenSince.Store(time.Now().UnixNano())
	plat.NotifyDirty(
		currentHash,
		pool.GetEntry,
		func(_ string, _ node.Hash) (string, bool, []string, bool) { return "", true, nil, true },
		func(_ netip.Addr) string { return "" },
	)

	order := make([]node.Hash, 0, 2)
	plat.View().Range(func(h node.Hash) bool {
		if h == candidateA || h == candidateB {
			order = append(order, h)
		}
		return true
	})
	if len(order) != 2 {
		t.Fatalf("expected 2 same-ip candidates in view, got %d", len(order))
	}

	entries := map[node.Hash]*node.NodeEntry{
		candidateA: entryA,
		candidateB: entryB,
	}
	noSampleHash := order[0]
	preferredHash := order[1]
	entries[preferredHash].LatencyTable.Update("example.com", 20*time.Millisecond, 10*time.Minute)
	waitForDomainLatency(t, entries[preferredHash], "example.com")
	_ = noSampleHash // intentionally keep target-domain latency empty

	router := newTestRouter(pool, nil)
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})

	originalLease := Lease{
		NodeHash:       currentHash,
		EgressIP:       currentEntry.GetEgressIP(),
		ExpiryNs:       time.Now().Add(time.Hour).UnixNano(),
		LastAccessedNs: time.Now().UnixNano(),
	}
	state.Leases.CreateLease("acct-rotation", originalLease)

	res, err := router.RouteRequest(plat.Name, "acct-rotation", "https://example.com/path")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	if res.NodeHash != preferredHash {
		t.Fatalf("rotation picked %s, want %s (candidate with target-domain latency)", res.NodeHash.Hex(), preferredHash.Hex())
	}
	if res.SelectedEntry() != entries[preferredHash] {
		t.Fatal("same-ip rotation did not carry the exact selected entry identity")
	}
	if res.LeaseCreated {
		t.Fatal("same-ip rotation should update existing lease, not create a new one")
	}

	updatedLease, ok := state.Leases.GetLease("acct-rotation")
	if !ok {
		t.Fatal("lease should still exist after rotation")
	}
	if updatedLease.NodeHash != preferredHash {
		t.Fatalf("lease node hash = %s, want %s", updatedLease.NodeHash.Hex(), preferredHash.Hex())
	}
	if updatedLease.ExpiryNs != originalLease.ExpiryNs {
		t.Fatalf("same-ip rotation must not change expiry: got %d want %d", updatedLease.ExpiryNs, originalLease.ExpiryNs)
	}
}

func TestRouteRequest_SelectedNodeRemovedAfterPick_EmitsLeaseRemove(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-2", "Plat-2", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	selectedHash, selectedEntry := newRoutableEntry(t, `{"id":"selected"}`, "203.0.113.20")
	pool.addEntry(selectedHash, selectedEntry)
	pool.rebuildPlatformView(plat)
	if !plat.View().Contains(selectedHash) {
		t.Fatalf(
			"setup expected selected hash in view: size=%d hasLatency=%v hasOutbound=%v",
			plat.View().Size(),
			selectedEntry.HasLatency(),
			selectedEntry.HasOutbound(),
		)
	}

	// Simulate stale view: node stays in platform view but has been removed from pool.
	pool.removeEntry(selectedHash)

	var events []LeaseEvent
	router := newTestRouter(pool, func(e LeaseEvent) {
		events = append(events, e)
	})
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})

	oldIP := netip.MustParseAddr("203.0.113.9")
	oldHash := node.HashFromRawOptions(json.RawMessage(`{"id":"old-lease-node"}`))
	state.Leases.CreateLease("acct-race", Lease{
		NodeHash:       oldHash,
		EgressIP:       oldIP,
		ExpiryNs:       time.Now().Add(time.Hour).UnixNano(),
		LastAccessedNs: time.Now().UnixNano(),
	})

	if got := state.IPLoadStats.Get(oldIP); got != 1 {
		t.Fatalf("setup ip load: got %d, want 1", got)
	}

	_, err := router.RouteRequest(plat.Name, "acct-race", "https://example.com")
	if err == nil {
		t.Fatal("expected route error when selected node disappears")
	}
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := state.Leases.GetLease("acct-race"); ok {
		t.Fatal("lease should be removed when re-route fails after invalidation")
	}
	if got := state.IPLoadStats.Get(oldIP); got != 0 {
		t.Fatalf("ip load should decrement exactly once, got %d", got)
	}

	foundRemove := false
	for _, e := range events {
		if e.Type == LeaseRemove && e.Account == "acct-race" && e.NodeHash == oldHash && e.EgressIP == oldIP {
			foundRemove = true
			break
		}
	}
	if !foundRemove {
		t.Fatal("expected LeaseRemove event when old lease is dropped")
	}
}

func TestRouteRequest_RejectsReplacementWithStalePlatformViewIdentity(t *testing.T) {
	pool := newRouterTestPool()
	var currentTag = "allowed"
	plat := platform.NewPlatformWithTagFilter(
		"plat-stale-identity",
		"Plat-Stale-Identity",
		node.TagFilter{Must: []*regexp.Regexp{regexp.MustCompile(`^sub-test/allowed$`)}},
		nil,
	)
	payload := json.RawMessage(`{"id":"same-hash-route"}`)
	h := node.HashFromRawOptions(payload)
	oldEntry := newHealthyEntryForHash(t, h, payload, "203.0.113.201")

	subLookup := func(_ string, _ node.Hash) (string, bool, []string, bool) {
		return "sub-test", true, []string{currentTag}, true
	}
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) {
			fn(h, oldEntry)
		},
		subLookup,
		func(_ netip.Addr) string { return "" },
	)
	if !plat.View().Contains(h) {
		t.Fatal("setup expected old entry in platform view")
	}

	currentTag = "excluded"
	newEntry := newHealthyEntryForHash(t, h, payload, "203.0.113.202")
	pool.addEntry(h, newEntry)
	pool.addPlatform(plat)

	router := newTestRouter(pool, nil)
	_, err := router.RouteRequest(plat.Name, "", "https://cloudflare.com/")
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("stale platform view selected replacement entry: err=%v", err)
	}
}

func newFilteredReplacementFixture(
	t *testing.T,
	platformID string,
	platformName string,
	oldIP string,
	newIP string,
) (*routerTestPool, *platform.Platform, node.Hash, *node.NodeEntry, *node.NodeEntry, func()) {
	t.Helper()
	pool := newRouterTestPool()
	currentTag := "allowed"
	plat := platform.NewPlatformWithTagFilter(
		platformID,
		platformName,
		node.TagFilter{Must: []*regexp.Regexp{regexp.MustCompile(`^sub-test/allowed$`)}},
		nil,
	)
	payload := json.RawMessage(`{"id":"same-hash-sticky"}`)
	h := node.HashFromRawOptions(payload)
	oldEntry := newHealthyEntryForHash(t, h, payload, oldIP)
	subLookup := func(_ string, _ node.Hash) (string, bool, []string, bool) {
		return "sub-test", true, []string{currentTag}, true
	}
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) { fn(h, oldEntry) },
		subLookup,
		func(_ netip.Addr) string { return "" },
	)
	if !plat.View().Contains(h) {
		t.Fatal("setup expected old entry in filtered platform view")
	}

	currentTag = "excluded"
	newEntry := newHealthyEntryForHash(t, h, payload, newIP)
	pool.addEntry(h, newEntry)
	pool.addPlatform(plat)
	return pool, plat, h, oldEntry, newEntry, func() {
		plat.NotifyDirty(
			h,
			pool.GetEntry,
			subLookup,
			func(_ netip.Addr) string { return "" },
		)
	}
}

func TestRouteRequest_StickyHitRejectsReplacementWithStalePlatformViewIdentity(t *testing.T) {
	pool, plat, h, oldEntry, _, converge := newFilteredReplacementFixture(
		t,
		"plat-stale-sticky-hit",
		"Plat-Stale-Sticky-Hit",
		"203.0.113.211",
		"203.0.113.211",
	)
	defer converge()

	router := newTestRouter(pool, nil)
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})
	state.Leases.CreateLease("stale-hit-account", Lease{
		NodeHash:       h,
		EgressIP:       oldEntry.GetEgressIP(),
		ExpiryNs:       time.Now().Add(time.Hour).UnixNano(),
		LastAccessedNs: time.Now().UnixNano(),
	})

	_, err := router.RouteRequest(plat.Name, "stale-hit-account", "https://cloudflare.com/")
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("stale platform view renewed replacement lease: err=%v", err)
	}
	if _, ok := state.Leases.GetLease("stale-hit-account"); ok {
		t.Fatal("stale lease should be removed after its replacement is rejected")
	}
}

func TestRouteRequest_SameIPRotationRejectsReplacementWithStalePlatformViewIdentity(t *testing.T) {
	pool := newRouterTestPool()
	currentTag := "allowed"
	plat := platform.NewPlatformWithTagFilter(
		"plat-stale-rotation",
		"Plat-Stale-Rotation",
		node.TagFilter{Must: []*regexp.Regexp{regexp.MustCompile(`^sub-test/allowed$`)}},
		nil,
	)
	currentHash, currentEntry := newRoutableEntry(t, `{"id":"rotation-current"}`, "203.0.113.221")
	candidatePayload := json.RawMessage(`{"id":"rotation-candidate"}`)
	candidateHash := node.HashFromRawOptions(candidatePayload)
	candidateEntry := newHealthyEntryForHash(t, candidateHash, candidatePayload, "203.0.113.221")
	pool.addEntry(currentHash, currentEntry)
	pool.addEntry(candidateHash, candidateEntry)
	subLookup := func(_ string, _ node.Hash) (string, bool, []string, bool) {
		return "sub-test", true, []string{currentTag}, true
	}
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) {
			fn(currentHash, currentEntry)
			fn(candidateHash, candidateEntry)
		},
		subLookup,
		func(_ netip.Addr) string { return "" },
	)
	// The current lease is invalidated by removing only its hash from the
	// published view; the candidate remains in the old view snapshot.
	plat.NotifyDirty(
		currentHash,
		func(h node.Hash) (*node.NodeEntry, bool) {
			if h == currentHash {
				return nil, false
			}
			return pool.GetEntry(h)
		},
		subLookup,
		func(_ netip.Addr) string { return "" },
	)
	if plat.View().Contains(currentHash) || !plat.View().Contains(candidateHash) {
		t.Fatal("setup did not leave only the candidate in the stale view")
	}

	currentTag = "excluded"
	pool.addEntry(candidateHash, newHealthyEntryForHash(t, candidateHash, candidatePayload, "203.0.113.221"))
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})
	state.Leases.CreateLease("stale-rotation-account", Lease{
		NodeHash:       currentHash,
		EgressIP:       currentEntry.GetEgressIP(),
		ExpiryNs:       time.Now().Add(time.Hour).UnixNano(),
		LastAccessedNs: time.Now().UnixNano(),
	})

	_, err := router.RouteRequest(plat.Name, "stale-rotation-account", "https://cloudflare.com/")
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("stale platform view rotated onto replacement: err=%v", err)
	}
	if _, ok := state.Leases.GetLease("stale-rotation-account"); ok {
		t.Fatal("stale lease should be removed after its rotation candidate is rejected")
	}
}
