package service

import (
	"context"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func newNodeListTestPool(subMgr *topology.SubscriptionManager) *topology.GlobalNodePool {
	return topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
}

func addRoutableNodeForSubscription(
	t *testing.T,
	pool *topology.GlobalNodePool,
	sub *subscription.Subscription,
	raw []byte,
	egressIP string,
) node.Hash {
	return addRoutableNodeForSubscriptionWithTag(t, pool, sub, raw, egressIP, "tag")
}

func addRoutableNodeForSubscriptionWithTag(
	t *testing.T,
	pool *topology.GlobalNodePool,
	sub *subscription.Subscription,
	raw []byte,
	egressIP string,
	tag string,
) node.Hash {
	t.Helper()

	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s not found after add", hash.Hex())
	}
	entry.SetEgressIP(netip.MustParseAddr(egressIP))
	if entry.LatencyTable == nil {
		t.Fatalf("node %s latency table not initialized", hash.Hex())
	}
	entry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	return hash
}

func TestListNodes_PlatformAndSubscriptionFiltersReturnIntersection(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	plat := platform.NewPlatform("plat-1", "plat", nil, nil)
	pool.RegisterPlatform(plat)

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	hashA := addRoutableNodeForSubscription(
		t,
		pool,
		subA,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.10",
	)
	_ = addRoutableNodeForSubscription(
		t,
		pool,
		subB,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.11",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}
	filters := NodeFilters{
		PlatformID:     &plat.ID,
		SubscriptionID: &subA.ID,
	}

	nodes, err := cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("intersection size = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != hashA.Hex() {
		t.Fatalf("intersection node hash = %q, want %q", nodes[0].NodeHash, hashA.Hex())
	}
}

func TestListNodes_SubscriptionFilterSkipsStaleManagedNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	staleHash := node.HashFromRawOptions([]byte(`{"type":"ss","server":"9.9.9.9","port":443}`))
	sub.ManagedNodes().StoreNode(staleHash, subscription.ManagedNode{Tags: []string{"stale"}})

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}
	filters := NodeFilters{
		SubscriptionID: &sub.ID,
	}

	nodes, err := cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes with stale hash: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes with stale managed hash = %d, want 0", len(nodes))
	}

	liveHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.20",
	)

	nodes, err = cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes with stale+live hashes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes with stale+live hashes = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != liveHash.Hex() {
		t.Fatalf("live node hash = %q, want %q", nodes[0].NodeHash, liveHash.Hex())
	}
}

func TestListNodes_SubscriptionFilterSkipsEvictedManagedNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	raw := []byte(`{"type":"ss","server":"7.7.7.7","port":443}`)
	hash := addRoutableNodeForSubscriptionWithTag(t, pool, subA, raw, "203.0.113.40", "a-tag")
	pool.AddNodeFromSub(hash, raw, subB.ID)
	subB.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"b-tag"}})

	managedA, ok := subA.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("subA managed node missing before eviction")
	}
	managedA.Evicted = true
	subA.ManagedNodes().StoreNode(hash, managedA)
	pool.RemoveNodeFromSub(hash, subA.ID)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	filtersA := NodeFilters{SubscriptionID: &subA.ID}
	nodesA, err := cp.ListNodes(filtersA)
	if err != nil {
		t.Fatalf("ListNodes subA: %v", err)
	}
	if len(nodesA) != 0 {
		t.Fatalf("subA filtered nodes = %d, want 0", len(nodesA))
	}

	filtersB := NodeFilters{SubscriptionID: &subB.ID}
	nodesB, err := cp.ListNodes(filtersB)
	if err != nil {
		t.Fatalf("ListNodes subB: %v", err)
	}
	if len(nodesB) != 1 || nodesB[0].NodeHash != hash.Hex() {
		t.Fatalf("subB filtered nodes = %+v, want [%s]", nodesB, hash.Hex())
	}
}

func TestGetNode_TagIncludesSubscriptionNamePrefix(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if len(got.Tags) != 1 {
		t.Fatalf("tags len = %d, want 1", len(got.Tags))
	}
	if got.Tags[0].Tag != "sub-a/tag" {
		t.Fatalf("tag = %q, want %q", got.Tags[0].Tag, "sub-a/tag")
	}
}

func TestGetNode_ReferenceLatencyMsUsesAuthorityAverage(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{
		Ewma:        40 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{
		Ewma:        60 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        5 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	runtimeCfg := &atomic.Pointer[config.RuntimeConfig]{}
	cfg := config.NewDefaultRuntimeConfig()
	cfg.LatencyAuthorities = []string{"cloudflare.com", "github.com", "google.com"}
	runtimeCfg.Store(cfg)

	cp := &ControlPlaneService{
		Pool:       pool,
		SubMgr:     subMgr,
		GeoIP:      &geoip.Service{},
		RuntimeCfg: runtimeCfg,
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.ReferenceLatencyMs == nil {
		t.Fatal("reference_latency_ms should be present")
	}
	if *got.ReferenceLatencyMs != 50 {
		t.Fatalf("reference_latency_ms = %v, want 50", *got.ReferenceLatencyMs)
	}
}

func TestListNodes_ProbedSinceUsesLastLatencyProbeAttempt(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}

	latencyAttempt := time.Now().Add(-2 * time.Minute).UnixNano()
	entry.LastLatencyProbeAttempt.Store(latencyAttempt)
	// Keep egress update older to ensure filter is using LastLatencyProbeAttempt.
	entry.LastEgressUpdate.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	before := time.Unix(0, latencyAttempt).Add(-1 * time.Minute)
	nodes, err := cp.ListNodes(NodeFilters{ProbedSince: &before})
	if err != nil {
		t.Fatalf("ListNodes(before): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(before) len = %d, want 1", len(nodes))
	}

	after := time.Unix(0, latencyAttempt).Add(1 * time.Minute)
	nodes, err = cp.ListNodes(NodeFilters{ProbedSince: &after})
	if err != nil {
		t.Fatalf("ListNodes(after): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ListNodes(after) len = %d, want 0", len(nodes))
	}
}

func TestListNodes_TagKeywordFuzzyMatchIsCaseInsensitive(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	matchHash := addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
		"hongkong-fast-01",
	)
	_ = addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.31",
		"japan-slow-01",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	keyword := "FAST"
	nodes, err := cp.ListNodes(NodeFilters{TagKeyword: &keyword})
	if err != nil {
		t.Fatalf("ListNodes(tag_keyword): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(tag_keyword) len = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != matchHash.Hex() {
		t.Fatalf("ListNodes(tag_keyword) hash = %q, want %q", nodes[0].NodeHash, matchHash.Hex())
	}
}

func TestListNodes_RegionFilterAndSummaryPreferStoredRegion(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.40",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.SetEgressRegion("jp")

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{}, // empty service returns "", forcing stored-region path
	}

	region := "jp"
	nodes, err := cp.ListNodes(NodeFilters{Region: &region})
	if err != nil {
		t.Fatalf("ListNodes(region): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != hash.Hex() {
		t.Fatalf("region-filtered nodes = %+v, want [%s]", nodes, hash.Hex())
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Region != "jp" {
		t.Fatalf("summary region: got %q, want %q", got.Region, "jp")
	}
}

func TestListNodes_EnabledFilter(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	subEnabled := subscription.NewSubscription("sub-enabled", "sub-enabled", "https://example.com/enabled", true, false)
	subDisabled := subscription.NewSubscription("sub-disabled", "sub-disabled", "https://example.com/disabled", false, false)
	subMgr.Register(subEnabled)
	subMgr.Register(subDisabled)

	enabledHash := addRoutableNodeForSubscription(
		t,
		pool,
		subEnabled,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.70",
	)
	disabledHash := addRoutableNodeForSubscription(
		t,
		pool,
		subDisabled,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.71",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	enabled := true
	nodes, err := cp.ListNodes(NodeFilters{Enabled: &enabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=true): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != enabledHash.Hex() {
		t.Fatalf("enabled filter result = %+v, want [%s]", nodes, enabledHash.Hex())
	}

	disabled := false
	nodes, err = cp.ListNodes(NodeFilters{Enabled: &disabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=false): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != disabledHash.Hex() {
		t.Fatalf("disabled filter result = %+v, want [%s]", nodes, disabledHash.Hex())
	}
}

func TestProbeEgress_ReturnsRegion(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.60",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{}, // empty service keeps focus on stored region from loc
		ProbeMgr: probe.NewProbeManager(probe.ProbeConfig{
			Pool: pool,
			Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
				return []byte("ip=198.51.100.88\nloc=JP"), 20 * time.Millisecond, nil
			},
		}),
	}

	got, err := cp.ProbeEgress(hash.Hex())
	if err != nil {
		t.Fatalf("ProbeEgress: %v", err)
	}
	if got.EgressIP != "198.51.100.88" {
		t.Fatalf("egress_ip: got %q, want %q", got.EgressIP, "198.51.100.88")
	}
	if got.Region != "jp" {
		t.Fatalf("region: got %q, want %q", got.Region, "jp")
	}
}

func TestProbeEgressRejectsNodeGenerationChangedAfterLookup(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-generation", "sub-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"198.51.100.10","port":443}`)
	hash := addRoutableNodeForSubscription(t, pool, sub, raw, "203.0.113.80")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry not found")
	}
	oldEntry.SetEgressRegion("old-region")

	var fetchCalls atomic.Int32
	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalls.Add(1)
			return []byte("ip=198.51.100.81\nloc=US"), 20 * time.Millisecond, nil
		},
	})
	cp := &ControlPlaneService{
		Pool:     pool,
		SubMgr:   subMgr,
		GeoIP:    &geoip.Service{},
		ProbeMgr: probeMgr,
	}
	lookupDone := make(chan struct{})
	allowProbe := make(chan struct{})
	cp.beforeProbeManagerCallHook = func() {
		close(lookupDone)
		<-allowProbe
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := cp.ProbeEgress(hash.Hex())
		resultCh <- err
	}()
	select {
	case <-lookupDone:
	case <-time.After(time.Second):
		t.Fatal("probe did not reach the post-lookup seam")
	}

	// Recreate the same hash while the service still holds the old entry.
	pool.RemoveNodeFromSub(hash, sub.ID)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	newOutbound := testutil.NewNoopOutbound()
	newEntry.Outbound.Store(&newOutbound)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowProbe)
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("ProbeEgress succeeded after its captured node entry was replaced")
		}
	case <-time.After(time.Second):
		t.Fatal("ProbeEgress did not finish after releasing the seam")
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("probe fetched replacement entry after generation change: %d calls", got)
	}
}

func TestProbeLatencyRejectsNodeGenerationChangedAfterLookup(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-latency-generation", "sub-latency-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"198.51.100.11","port":443}`)
	hash := addRoutableNodeForSubscription(t, pool, sub, raw, "203.0.113.81")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry not found")
	}

	var fetchCalls atomic.Int32
	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalls.Add(1)
			return []byte("OK"), 20 * time.Millisecond, nil
		},
	})
	cp := &ControlPlaneService{
		Pool:     pool,
		SubMgr:   subMgr,
		ProbeMgr: probeMgr,
	}
	lookupDone := make(chan struct{})
	allowProbe := make(chan struct{})
	cp.beforeProbeManagerCallHook = func() {
		close(lookupDone)
		<-allowProbe
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := cp.ProbeLatency(hash.Hex())
		resultCh <- err
	}()
	select {
	case <-lookupDone:
	case <-time.After(time.Second):
		t.Fatal("latency probe did not reach the post-lookup seam")
	}

	pool.RemoveNodeFromSub(hash, sub.ID)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	newOutbound := testutil.NewNoopOutbound()
	newEntry.Outbound.Store(&newOutbound)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowProbe)
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("ProbeLatency succeeded after its captured node entry was replaced")
		}
	case <-time.After(time.Second):
		t.Fatal("ProbeLatency did not finish after releasing the seam")
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("latency probe fetched replacement entry after generation change: %d calls", got)
	}
}

func TestProbeEgressRejectsNodeGenerationChangedDuringFetch(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-egress-fetch-generation", "sub-egress-fetch-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"198.51.100.14","port":443}`)
	hash := addRoutableNodeForSubscription(t, pool, sub, raw, "203.0.113.84")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry not found")
	}

	fetchStarted := make(chan struct{})
	allowFetch := make(chan struct{})
	var fetchCalls atomic.Int32
	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalls.Add(1)
			close(fetchStarted)
			<-allowFetch
			return []byte("ip=198.51.100.84\nloc=CA"), 20 * time.Millisecond, nil
		},
	})
	cp := &ControlPlaneService{Pool: pool, SubMgr: subMgr, ProbeMgr: probeMgr, GeoIP: &geoip.Service{}}
	resultCh := make(chan error, 1)
	go func() {
		_, err := cp.ProbeEgress(hash.Hex())
		resultCh <- err
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("egress probe did not enter fetch")
	}

	pool.RemoveNodeFromSub(hash, sub.ID)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	newOutbound := testutil.NewNoopOutbound()
	newEntry.Outbound.Store(&newOutbound)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowFetch)
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("ProbeEgress returned an old-generation result after replacement during fetch")
		}
	case <-time.After(time.Second):
		t.Fatal("ProbeEgress did not finish after releasing fetch")
	}
	probeMgr.Stop()
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("egress fetch calls = %d, want exactly one old-generation fetch", got)
	}
	if got := newEntry.GetEgressIP(); got.IsValid() {
		t.Fatalf("stale egress probe polluted replacement entry: %v", got)
	}
	if newEntry.HasLatency() {
		t.Fatal("stale egress probe wrote latency to replacement entry")
	}
}

func TestProbeLatencyRejectsNodeGenerationChangedDuringFetch(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-latency-fetch-generation", "sub-latency-fetch-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"198.51.100.15","port":443}`)
	hash := addRoutableNodeForSubscription(t, pool, sub, raw, "203.0.113.85")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry not found")
	}

	fetchStarted := make(chan struct{})
	allowFetch := make(chan struct{})
	var fetchCalls atomic.Int32
	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalls.Add(1)
			close(fetchStarted)
			<-allowFetch
			return []byte("OK"), 20 * time.Millisecond, nil
		},
	})
	cp := &ControlPlaneService{Pool: pool, SubMgr: subMgr, ProbeMgr: probeMgr}
	resultCh := make(chan error, 1)
	go func() {
		_, err := cp.ProbeLatency(hash.Hex())
		resultCh <- err
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("latency probe did not enter fetch")
	}

	pool.RemoveNodeFromSub(hash, sub.ID)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	newOutbound := testutil.NewNoopOutbound()
	newEntry.Outbound.Store(&newOutbound)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowFetch)
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("ProbeLatency returned an old-generation result after replacement during fetch")
		}
	case <-time.After(time.Second):
		t.Fatal("ProbeLatency did not finish after releasing fetch")
	}
	probeMgr.Stop()
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("latency fetch calls = %d, want exactly one old-generation fetch", got)
	}
	if newEntry.HasLatency() {
		t.Fatal("stale latency probe polluted replacement entry")
	}
}

func TestProbeEgressRejectsNodeGenerationChangedBeforeResponse(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-egress-final-generation", "sub-egress-final-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"198.51.100.12","port":443}`)
	hash := addRoutableNodeForSubscription(t, pool, sub, raw, "203.0.113.82")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry not found")
	}

	var fetchCalls atomic.Int32
	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalls.Add(1)
			return []byte("ip=198.51.100.82\nloc=CA"), 20 * time.Millisecond, nil
		},
	})
	cp := &ControlPlaneService{Pool: pool, SubMgr: subMgr, ProbeMgr: probeMgr, GeoIP: &geoip.Service{}}
	managerDone := make(chan struct{})
	allowResponse := make(chan struct{})
	cp.afterProbeManagerResultHook = func() {
		close(managerDone)
		<-allowResponse
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := cp.ProbeEgress(hash.Hex())
		resultCh <- err
	}()
	select {
	case <-managerDone:
	case <-time.After(time.Second):
		t.Fatal("egress probe did not reach the post-manager seam")
	}

	pool.RemoveNodeFromSub(hash, sub.ID)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	newOutbound := testutil.NewNoopOutbound()
	newEntry.Outbound.Store(&newOutbound)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowResponse)
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("ProbeEgress returned a result after its captured entry was replaced")
		}
	case <-time.After(time.Second):
		t.Fatal("ProbeEgress did not finish after releasing the response seam")
	}
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("egress fetch calls = %d, want exactly one old-generation fetch", got)
	}
	if got := newEntry.GetEgressIP(); got.IsValid() {
		t.Fatalf("post-manager generation change polluted replacement egress IP: %v", got)
	}
}

func TestProbeLatencyRejectsNodeGenerationChangedBeforeResponse(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-latency-final-generation", "sub-latency-final-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"198.51.100.13","port":443}`)
	hash := addRoutableNodeForSubscription(t, pool, sub, raw, "203.0.113.83")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry not found")
	}

	var fetchCalls atomic.Int32
	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalls.Add(1)
			return []byte("OK"), 20 * time.Millisecond, nil
		},
	})
	cp := &ControlPlaneService{Pool: pool, SubMgr: subMgr, ProbeMgr: probeMgr}
	managerDone := make(chan struct{})
	allowResponse := make(chan struct{})
	cp.afterProbeManagerResultHook = func() {
		close(managerDone)
		<-allowResponse
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := cp.ProbeLatency(hash.Hex())
		resultCh <- err
	}()
	select {
	case <-managerDone:
	case <-time.After(time.Second):
		t.Fatal("latency probe did not reach the post-manager seam")
	}

	pool.RemoveNodeFromSub(hash, sub.ID)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	newOutbound := testutil.NewNoopOutbound()
	newEntry.Outbound.Store(&newOutbound)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowResponse)
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("ProbeLatency returned a result after its captured entry was replaced")
		}
	case <-time.After(time.Second):
		t.Fatal("ProbeLatency did not finish after releasing the response seam")
	}
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("latency fetch calls = %d, want exactly one old-generation fetch", got)
	}
	if newEntry.HasLatency() {
		t.Fatal("post-manager generation change polluted replacement latency")
	}
}
