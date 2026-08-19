package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func newTestPool(subMgr *SubscriptionManager) *GlobalNodePool {
	return NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
}

// --- Pool tests ---

func TestPool_AddNodeFromSub_Idempotent(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	// Set up managed nodes so MatchRegexs can see them.
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"us-node"}})
	sub.SwapManagedNodes(mn)

	// Add twice — should be idempotent.
	pool.AddNodeFromSub(h, raw, "s1")
	pool.AddNodeFromSub(h, raw, "s1")

	if pool.Size() != 1 {
		t.Fatalf("expected 1 node, got %d", pool.Size())
	}

	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.SubscriptionCount() != 1 {
		t.Fatalf("expected 1 sub ref, got %d", entry.SubscriptionCount())
	}
}

func TestPool_AddNodeRuntimeCallbackDoesNotBlockOtherLifecycleMutations(t *testing.T) {
	runtimeEntered := make(chan struct{})
	releaseRuntime := make(chan struct{})
	runtimeOnce := sync.Once{}

	pool := NewGlobalNodePool(PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeAddedRuntime: func(node.Hash, *node.NodeEntry) {
			runtimeOnce.Do(func() { close(runtimeEntered) })
			<-releaseRuntime
		},
	})
	raw := json.RawMessage(`{"type":"runtime-callback","server":"198.51.100.20"}`)
	hash := node.HashFromRawOptions(raw)

	addDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(hash, raw, "runtime-callback-sub")
		close(addDone)
	}()
	select {
	case <-runtimeEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime callback did not start")
	}

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, "runtime-callback-sub")
		close(removeDone)
	}()
	select {
	case <-removeDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("node lifecycle mutation blocked behind runtime preparation")
	}

	close(releaseRuntime)
	select {
	case <-addDone:
	case <-time.After(time.Second):
		t.Fatal("add did not finish after runtime preparation release")
	}
}

func TestPool_AddNodeFromSub_NewNodeStartsCircuitOpen(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	pool.AddNodeFromSub(h, raw, "s1")

	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("newly added node should start circuit-open")
	}
	if entry.CircuitOpenSince.Load() <= 0 {
		t.Fatalf("CircuitOpenSince should be set, got %d", entry.CircuitOpenSince.Load())
	}
}

func TestPool_AddNodeFromSub_ReAddDoesNotResetCircuitOpenSince(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub1 := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	sub2 := subscription.NewSubscription("s2", "Sub2", "url", true, false)
	subMgr.Register(sub1)
	subMgr.Register(sub2)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"same"}`)
	h := node.HashFromRawOptions(raw)

	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	originalCircuitSince := entry.CircuitOpenSince.Load()
	if originalCircuitSince <= 0 {
		t.Fatalf("CircuitOpenSince should be set on first add, got %d", originalCircuitSince)
	}

	// Re-add from another subscription should only add reference, not reinitialize state.
	pool.AddNodeFromSub(h, raw, "s2")
	if got := entry.CircuitOpenSince.Load(); got != originalCircuitSince {
		t.Fatalf("CircuitOpenSince should not reset on re-add: got %d, want %d", got, originalCircuitSince)
	}
}

func TestPool_RemoveNodeFromSub_Idempotent(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	// Remove nonexistent — should not panic.
	pool.RemoveNodeFromSub(h, "s1")

	pool.AddNodeFromSub(h, raw, "s1")
	pool.RemoveNodeFromSub(h, "s1")
	pool.RemoveNodeFromSub(h, "s1") // idempotent

	if pool.Size() != 0 {
		t.Fatalf("expected 0 nodes, got %d", pool.Size())
	}
}

func TestPool_RecreatedNodeWaitsForRemovedEntryCleanup(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"cleanup-order","server":"198.51.100.10"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "s1")
	removedEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("setup: node not found")
	}

	cleanupEntered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	pool.SetOnNodeRemoved(func(_ node.Hash, entry *node.NodeEntry) {
		if entry != removedEntry {
			t.Fatalf("removed callback received unexpected entry: got %p want %p", entry, removedEntry)
		}
		close(cleanupEntered)
		<-releaseCleanup
	})

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, "s1")
		close(removeDone)
	}()
	select {
	case <-cleanupEntered:
	case <-time.After(time.Second):
		t.Fatal("remove did not reach the lifecycle cleanup callback")
	}

	addDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(hash, raw, "s1")
		close(addDone)
	}()
	select {
	case <-addDone:
		t.Fatal("recreated node became visible before removed-entry cleanup completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseCleanup)
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("remove did not finish after cleanup release")
	}
	select {
	case <-addDone:
	case <-time.After(time.Second):
		t.Fatal("recreated node did not finish after cleanup release")
	}

	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == removedEntry {
		t.Fatal("expected a distinct recreated node entry")
	}
}

func TestPool_RemoveNodeFromSub_RetiresOutboundBeforeRemovalCallback(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"remove-before-retire","server":"198.51.100.11"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("setup: node not found")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	pool.SetOnNodeRemoved(func(_ node.Hash, removed *node.NodeEntry) {
		if removed != entry {
			t.Errorf("removed callback entry = %p, want %p", removed, entry)
		}
		close(callbackEntered)
		<-releaseCallback
	})

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, sub.ID)
		close(removeDone)
	}()

	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("remove did not reach callback")
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("node should already be absent when removal callback runs")
	}

	_, release, acquired := entry.AcquireOutbound()
	if acquired {
		release()
		t.Fatal("removed entry admitted a new outbound lease before removal returned")
	}

	close(releaseCallback)
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("remove did not finish after callback release")
	}
}

func TestPool_RemoveNodeFromSub_DeleteVisibilityImpliesOutboundRetired(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-delete-point", "DeletePoint", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"remove-delete-point","server":"198.51.100.12"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("setup: node not found")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)

	deletePublished := make(chan struct{})
	releaseDeleteHook := make(chan struct{})
	var releaseOnce sync.Once
	pool.afterNodeDeleteComputeHook = func(gotHash node.Hash, gotEntry *node.NodeEntry) {
		if gotHash != hash || gotEntry != entry {
			t.Errorf("delete hook got (%s, %p), want (%s, %p)", gotHash.Hex(), gotEntry, hash.Hex(), entry)
		}
		close(deletePublished)
		<-releaseDeleteHook
	}
	defer releaseOnce.Do(func() { close(releaseDeleteHook) })

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, sub.ID)
		close(removeDone)
	}()
	select {
	case <-deletePublished:
	case <-time.After(time.Second):
		t.Fatal("remove did not publish DeleteOp boundary")
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("node remained visible after DeleteOp boundary")
	}

	_, release, acquired := entry.AcquireOutbound()
	if acquired {
		release()
	}
	releaseOnce.Do(func() { close(releaseDeleteHook) })
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("remove did not finish after DeleteOp boundary release")
	}
	if acquired {
		t.Fatal("entry admitted a new outbound lease after DeleteOp became visible")
	}
}

func TestPool_CrossSubDedup(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub1 := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	sub2 := subscription.NewSubscription("s2", "Sub2", "url", true, false)
	subMgr.Register(sub1)
	subMgr.Register(sub2)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"same"}`)
	h := node.HashFromRawOptions(raw)

	pool.AddNodeFromSub(h, raw, "s1")
	pool.AddNodeFromSub(h, raw, "s2")

	if pool.Size() != 1 {
		t.Fatalf("expected 1 deduped node, got %d", pool.Size())
	}

	entry, _ := pool.GetEntry(h)
	if entry.SubscriptionCount() != 2 {
		t.Fatalf("expected 2 sub refs, got %d", entry.SubscriptionCount())
	}

	// Remove one sub ref — node should remain.
	pool.RemoveNodeFromSub(h, "s1")
	if pool.Size() != 1 {
		t.Fatal("node should remain after removing one sub ref")
	}

	// Remove last ref — node should be deleted.
	pool.RemoveNodeFromSub(h, "s2")
	if pool.Size() != 0 {
		t.Fatal("node should be deleted when all refs removed")
	}
}

func TestPool_ConcurrentAddRemove(t *testing.T) {
	subMgr := NewSubscriptionManager()
	for i := 0; i < 10; i++ {
		sub := subscription.NewSubscription(fmt.Sprintf("s%d", i), fmt.Sprintf("Sub%d", i), "url", true, false)
		subMgr.Register(sub)
	}

	pool := newTestPool(subMgr)
	hashes := make([]node.Hash, 100)
	raws := make([]json.RawMessage, 100)
	for i := range hashes {
		raw := json.RawMessage(fmt.Sprintf(`{"type":"ss","n":%d}`, i))
		hashes[i] = node.HashFromRawOptions(raw)
		raws[i] = raw
	}

	var wg sync.WaitGroup
	// 10 goroutines add nodes concurrently.
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(subIdx int) {
			defer wg.Done()
			subID := fmt.Sprintf("s%d", subIdx)
			for i := subIdx * 10; i < (subIdx+1)*10; i++ {
				pool.AddNodeFromSub(hashes[i], raws[i], subID)
			}
		}(g)
	}
	wg.Wait()

	if pool.Size() != 100 {
		t.Fatalf("expected 100 nodes, got %d", pool.Size())
	}

	// Concurrently remove all.
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(subIdx int) {
			defer wg.Done()
			subID := fmt.Sprintf("s%d", subIdx)
			for i := subIdx * 10; i < (subIdx+1)*10; i++ {
				pool.RemoveNodeFromSub(hashes[i], subID)
			}
		}(g)
	}
	wg.Wait()

	if pool.Size() != 0 {
		t.Fatalf("expected 0 nodes after concurrent remove, got %d", pool.Size())
	}
}

func TestPool_PlatformNotifyOnAddRemove(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	// Create a platform with no filters (everything passes regex/region checks).
	plat := platform.NewPlatform("p1", "TestPlat", nil, nil)
	pool.RegisterPlatform(plat)

	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	// Set managed nodes for sub.
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)

	// Create entry with all conditions met for routing.
	pool.AddNodeFromSub(h, raw, "s1")

	// The node won't be in the view yet because it has no latency/outbound.
	if plat.View().Size() != 0 {
		t.Fatal("new node without latency/outbound should not be in view")
	}

	// Set latency+outbound on entry, then re-trigger dirty.
	entry, _ := pool.GetEntry(h)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)

	// Re-add triggers NotifyDirty.
	pool.AddNodeFromSub(h, raw, "s1")
	if plat.View().Size() != 1 {
		t.Fatal("node with all conditions should be in view after re-add")
	}

	// Remove → should leave view.
	pool.RemoveNodeFromSub(h, "s1")
	if plat.View().Size() != 0 {
		t.Fatal("deleted node should be removed from view")
	}
}

func TestPool_NotifyNodeDirty_UpdatesPlatformsInParallel(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	subMgr.Register(sub)

	releaseGeoLookup := make(chan struct{})
	allGeoLookupStarted := make(chan struct{})
	var geoLookupCalls atomic.Int32
	var blockGeoLookup atomic.Bool

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup: subMgr.Lookup,
		GeoLookup: func(addr netip.Addr) string {
			if !blockGeoLookup.Load() {
				return "us"
			}
			if geoLookupCalls.Add(1) == 2 {
				close(allGeoLookupStarted)
			}
			<-releaseGeoLookup
			return "us"
		},
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)

	plat1 := platform.NewPlatform("p1", "P1", nil, []string{"us"})
	plat2 := platform.NewPlatform("p2", "P2", nil, []string{"us"})
	pool.RegisterPlatform(plat1)
	pool.RegisterPlatform(plat2)
	geoLookupCalls.Store(0)
	blockGeoLookup.Store(true)

	done := make(chan struct{})
	go func() {
		pool.NotifyNodeDirty(h)
		close(done)
	}()

	select {
	case <-allGeoLookupStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected platform dirty notifications to run in parallel")
	}

	select {
	case <-done:
		t.Fatal("NotifyNodeDirty should wait for in-flight platform notifications")
	default:
	}

	close(releaseGeoLookup)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("NotifyNodeDirty did not finish after releasing geo lookups")
	}

	if got := geoLookupCalls.Load(); got != 2 {
		t.Fatalf("expected 2 geo lookup calls, got %d", got)
	}
}

func TestPool_RebuildAllPlatforms_UpdatesInParallel(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	subMgr.Register(sub)

	releaseGeoLookup := make(chan struct{})
	allGeoLookupStarted := make(chan struct{})
	var geoLookupCalls atomic.Int32
	var blockGeoLookup atomic.Bool

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup: subMgr.Lookup,
		GeoLookup: func(addr netip.Addr) string {
			if !blockGeoLookup.Load() {
				return "us"
			}
			if geoLookupCalls.Add(1) == 2 {
				close(allGeoLookupStarted)
			}
			<-releaseGeoLookup
			return "us"
		},
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)

	plat1 := platform.NewPlatform("p1", "P1", nil, []string{"us"})
	plat2 := platform.NewPlatform("p2", "P2", nil, []string{"us"})
	pool.RegisterPlatform(plat1)
	pool.RegisterPlatform(plat2)
	geoLookupCalls.Store(0)
	blockGeoLookup.Store(true)

	done := make(chan struct{})
	go func() {
		pool.RebuildAllPlatforms()
		close(done)
	}()

	select {
	case <-allGeoLookupStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected platform rebuilds to run in parallel")
	}

	select {
	case <-done:
		t.Fatal("RebuildAllPlatforms should wait for in-flight rebuilds")
	default:
	}

	close(releaseGeoLookup)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RebuildAllPlatforms did not finish after releasing geo lookups")
	}

	if got := geoLookupCalls.Load(); got != 2 {
		t.Fatalf("expected 2 geo lookup calls, got %d", got)
	}
	if !plat1.View().Contains(h) || !plat2.View().Contains(h) {
		t.Fatal("rebuild should populate both platform views")
	}
}

func TestScheduler_SetSubscriptionEnabledSkipsUnregisteredSnapshot(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-rebuild-delete", "Provider", "url", false, false)
	subMgr.Register(sub)

	var deleted atomic.Bool
	var rebuiltAfterDelete atomic.Bool
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup: subMgr.Lookup,
		GeoLookup: func(netip.Addr) string {
			if deleted.Load() {
				rebuiltAfterDelete.Store(true)
			}
			return "us"
		},
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := json.RawMessage(`{"type":"ss","server":"198.51.100.42"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("setup: node entry not found")
	}
	entry.CircuitOpenSince.Store(0)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)

	plat := platform.NewPlatform("p-rebuild-delete", "RebuildDelete", nil, []string{"us"})
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	sched := newTestScheduler(subMgr, pool, nil)

	rebuildReached := make(chan struct{})
	releaseRebuild := make(chan struct{})
	pool.beforePlatformRebuildAllHook = func() {
		close(rebuildReached)
		<-releaseRebuild
	}
	defer func() {
		pool.beforePlatformRebuildAllHook = nil
		select {
		case <-releaseRebuild:
		default:
			close(releaseRebuild)
		}
	}()

	rebuildDone := make(chan struct{})
	go func() {
		sched.SetSubscriptionEnabled(sub, true)
		close(rebuildDone)
	}()
	select {
	case <-rebuildReached:
	case <-time.After(time.Second):
		t.Fatal("RebuildAllPlatforms did not reach the snapshot boundary")
	}

	deleteDone := make(chan struct{})
	go func() {
		pool.UnregisterPlatform(plat.ID)
		deleted.Store(true)
		close(deleteDone)
	}()
	select {
	case <-deleteDone:
	case <-time.After(time.Second):
		t.Fatal("UnregisterPlatform did not complete before releasing the rebuild")
	}
	close(releaseRebuild)

	select {
	case <-rebuildDone:
	case <-time.After(time.Second):
		t.Fatal("RebuildAllPlatforms did not finish after release")
	}
	if rebuiltAfterDelete.Load() {
		t.Fatal("RebuildAllPlatforms rebuilt a platform after it was unregistered")
	}
}

func TestPool_RegexFilteredPlatform(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	// Platform with "us" regex filter.
	plat := platform.NewPlatform("p1", "US-Only", []*regexp.Regexp{regexp.MustCompile("us")}, nil)
	pool.RegisterPlatform(plat)

	h1 := node.HashFromRawOptions([]byte(`{"type":"ss","n":"us"}`))
	h2 := node.HashFromRawOptions([]byte(`{"type":"ss","n":"jp"}`))

	// Setup managedNodes with appropriate tags.
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h1, subscription.ManagedNode{Tags: []string{"us-node"}})
	mn.StoreNode(h2, subscription.ManagedNode{Tags: []string{"jp-node"}})
	sub.SwapManagedNodes(mn)

	// Make both fully routable.
	for _, h := range []node.Hash{h1, h2} {
		pool.AddNodeFromSub(h, nil, "s1")
		entry, _ := pool.GetEntry(h)
		entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
			Ewma:        100 * time.Millisecond,
			LastUpdated: time.Now(),
		})
		ob := testutil.NewNoopOutbound()
		entry.Outbound.Store(&ob)
		entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
		pool.RecordResult(h, true)
		// Re-trigger dirty to pick up latency/outbound.
		pool.AddNodeFromSub(h, nil, "s1")
	}

	// Only us-node should be in view ("Provider/us-node" matches "us").
	if plat.View().Size() != 1 {
		t.Fatalf("expected 1 node in filtered view, got %d", plat.View().Size())
	}
	if !plat.View().Contains(h1) {
		t.Fatal("us-node should be in view")
	}
	if plat.View().Contains(h2) {
		t.Fatal("jp-node should NOT be in view")
	}
}

func TestPool_PlatformLookupByIDAndName(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	plat := platform.NewPlatform("p-lookup", "LookupPlat", nil, nil)
	pool.RegisterPlatform(plat)

	gotByID, ok := pool.GetPlatform("p-lookup")
	if !ok || gotByID != plat {
		t.Fatal("GetPlatform should return registered platform by ID")
	}

	gotByName, ok := pool.GetPlatformByName("LookupPlat")
	if !ok || gotByName != plat {
		t.Fatal("GetPlatformByName should return registered platform by name")
	}
}

func TestPool_ResolveNodeDisplayTag_PreferEarliestEnabledSubscriptionThenMinTag(t *testing.T) {
	subMgr := NewSubscriptionManager()

	older := subscription.NewSubscription("sub-old", "Z-Provider", "url", true, false)
	older.CreatedAtNs = 100
	older.SetEnabled(false)

	newer := subscription.NewSubscription("sub-new", "A-Provider", "url", true, false)
	newer.CreatedAtNs = 200

	subMgr.Register(older)
	subMgr.Register(newer)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	oldManaged := subscription.NewManagedNodes()
	oldManaged.StoreNode(h, subscription.ManagedNode{Tags: []string{"zz", "aa"}})
	older.SwapManagedNodes(oldManaged)

	newManaged := subscription.NewManagedNodes()
	newManaged.StoreNode(h, subscription.ManagedNode{Tags: []string{"00"}})
	newer.SwapManagedNodes(newManaged)

	pool.AddNodeFromSub(h, raw, older.ID)
	pool.AddNodeFromSub(h, raw, newer.ID)

	got := pool.ResolveNodeDisplayTag(h)
	want := "A-Provider/00"
	if got != want {
		t.Fatalf("ResolveNodeDisplayTag = %q, want %q", got, want)
	}

	if v := pool.ResolveNodeDisplayTag(node.Zero); v != "" {
		t.Fatalf("ResolveNodeDisplayTag(unknown) = %q, want empty", v)
	}
}

func TestPool_ResolveNodeDisplayTag_AllDisabled_FallbackToLegacyRule(t *testing.T) {
	subMgr := NewSubscriptionManager()

	older := subscription.NewSubscription("sub-old", "Z-Provider", "url", false, false)
	older.CreatedAtNs = 100
	newer := subscription.NewSubscription("sub-new", "A-Provider", "url", false, false)
	newer.CreatedAtNs = 200

	subMgr.Register(older)
	subMgr.Register(newer)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	oldManaged := subscription.NewManagedNodes()
	oldManaged.StoreNode(h, subscription.ManagedNode{Tags: []string{"zz", "aa"}})
	older.SwapManagedNodes(oldManaged)

	newManaged := subscription.NewManagedNodes()
	newManaged.StoreNode(h, subscription.ManagedNode{Tags: []string{"00"}})
	newer.SwapManagedNodes(newManaged)

	pool.AddNodeFromSub(h, raw, older.ID)
	pool.AddNodeFromSub(h, raw, newer.ID)

	got := pool.ResolveNodeDisplayTag(h)
	want := "Z-Provider/aa"
	if got != want {
		t.Fatalf("ResolveNodeDisplayTag = %q, want %q", got, want)
	}
}

func TestPool_IsNodeDisabled(t *testing.T) {
	subMgr := NewSubscriptionManager()
	disabled := subscription.NewSubscription("sub-disabled", "Disabled", "url", false, false)
	enabled := subscription.NewSubscription("sub-enabled", "Enabled", "url", true, false)
	subMgr.Register(disabled)
	subMgr.Register(enabled)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	disabledManaged := subscription.NewManagedNodes()
	disabledManaged.StoreNode(h, subscription.ManagedNode{Tags: []string{"d-tag"}})
	disabled.SwapManagedNodes(disabledManaged)

	enabledManaged := subscription.NewManagedNodes()
	enabledManaged.StoreNode(h, subscription.ManagedNode{Tags: []string{"e-tag"}})
	enabled.SwapManagedNodes(enabledManaged)

	pool.AddNodeFromSub(h, raw, disabled.ID)
	pool.AddNodeFromSub(h, raw, enabled.ID)

	if pool.IsNodeDisabled(h) {
		t.Fatal("node should be enabled while at least one holder subscription is enabled")
	}

	enabled.SetEnabled(false)
	if !pool.IsNodeDisabled(h) {
		t.Fatal("node should be disabled when all holder subscriptions are disabled")
	}
}

func TestPool_MakeHealthyAndEnabledEvaluator_ExcludesDisabledNodes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	enabledSub := subscription.NewSubscription("sub-enabled", "Enabled", "url", true, false)
	disabledSub := subscription.NewSubscription("sub-disabled", "Disabled", "url", false, false)
	subMgr.Register(enabledSub)
	subMgr.Register(disabledSub)

	pool := newTestPool(subMgr)

	healthyRaw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	healthyHash := node.HashFromRawOptions(healthyRaw)
	pool.AddNodeFromSub(healthyHash, healthyRaw, enabledSub.ID)
	enabledSub.ManagedNodes().StoreNode(healthyHash, subscription.ManagedNode{Tags: []string{"healthy"}})
	healthyEntry, ok := pool.GetEntry(healthyHash)
	if !ok {
		t.Fatal("healthy entry missing")
	}
	healthyOutbound := testutil.NewNoopOutbound()
	healthyEntry.Outbound.Store(&healthyOutbound)
	pool.RecordResult(healthyHash, true)

	disabledRaw := json.RawMessage(`{"type":"ss","server":"2.2.2.2"}`)
	disabledHash := node.HashFromRawOptions(disabledRaw)
	pool.AddNodeFromSub(disabledHash, disabledRaw, disabledSub.ID)
	disabledSub.ManagedNodes().StoreNode(disabledHash, subscription.ManagedNode{Tags: []string{"disabled"}})
	disabledEntry, ok := pool.GetEntry(disabledHash)
	if !ok {
		t.Fatal("disabled entry missing")
	}
	disabledOutbound := testutil.NewNoopOutbound()
	disabledEntry.Outbound.Store(&disabledOutbound)
	pool.RecordResult(disabledHash, true)

	isHealthyAndEnabled := pool.MakeHealthyAndEnabledEvaluator()
	if !isHealthyAndEnabled(healthyEntry) {
		t.Fatal("enabled healthy node should count as healthy")
	}
	if isHealthyAndEnabled(disabledEntry) {
		t.Fatal("disabled node should not count as healthy")
	}
}

func TestPool_RangePlatforms_UsesSnapshotAndDoesNotDeadlock(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	plat1 := platform.NewPlatform("p-range-1", "Range-1", nil, nil)
	plat2 := platform.NewPlatform("p-range-2", "Range-2", nil, nil)
	pool.RegisterPlatform(plat1)
	pool.RegisterPlatform(plat2)

	done := make(chan struct{})
	go func() {
		pool.RangePlatforms(func(_ *platform.Platform) bool {
			// Mutating during range should not deadlock because RangePlatforms
			// iterates on a snapshot.
			pool.UnregisterPlatform("p-range-2")
			return true
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RangePlatforms deadlocked while unregistering during callback")
	}

	if _, ok := pool.GetPlatform("p-range-2"); ok {
		t.Fatal("platform should be removed after unregister")
	}
}

func TestPool_RegisterPlatformRejectsConflictsWithoutMutation(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	first := platform.NewPlatform("p-first", "SameName", nil, nil)
	if err := pool.RegisterPlatform(first); err != nil {
		t.Fatalf("RegisterPlatform first: %v", err)
	}

	duplicateID := platform.NewPlatform("p-first", "OtherName", nil, nil)
	if err := pool.RegisterPlatform(duplicateID); !errors.Is(err, ErrPlatformAlreadyRegistered) {
		t.Fatalf("duplicate ID error = %v, want ErrPlatformAlreadyRegistered", err)
	}
	if got, ok := pool.GetPlatform("p-first"); !ok || got != first {
		t.Fatal("duplicate ID changed the existing ID mapping")
	}
	if _, ok := pool.GetPlatformByName("OtherName"); ok {
		t.Fatal("duplicate ID installed a new name mapping")
	}

	duplicateName := platform.NewPlatform("p-second", "SameName", nil, nil)
	if err := pool.RegisterPlatform(duplicateName); !errors.Is(err, ErrPlatformNameConflict) {
		t.Fatalf("duplicate name error = %v, want ErrPlatformNameConflict", err)
	}
	if got, ok := pool.GetPlatformByName("SameName"); !ok || got != first {
		t.Fatal("duplicate name changed the existing name mapping")
	}
	if _, ok := pool.GetPlatform("p-second"); ok {
		t.Fatal("duplicate name installed an ID mapping")
	}

	if err := pool.RegisterPlatform(nil); !errors.Is(err, ErrInvalidPlatform) {
		t.Fatalf("nil platform error = %v, want ErrInvalidPlatform", err)
	}
}

func TestPool_RegisterPlatformRebuildsAfterPrePublishNodeChange(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-register-race", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"node"}})

	entry := node.NewNodeEntry(hash, raw, time.Now(), 16)
	entry.AddSubscriptionID(sub.ID)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        50 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.CircuitOpenSince.Store(time.Now().UnixNano())
	pool.LoadNodeFromBootstrap(entry)

	plat := platform.NewPlatform("p-register-race", "Register-Race", nil, nil)
	// This is the CreatePlatform shape: prepare the view before publishing the
	// platform object. At this point the node is intentionally not routable.
	pool.RebuildPlatform(plat)
	if plat.View().Contains(hash) {
		t.Fatal("pre-publish rebuild unexpectedly included the unhealthy node")
	}

	// The node becomes routable while the platform is still unpublished. Its
	// notification has no registered recipient and must be recovered by the
	// registration publish itself.
	entry.CircuitOpenSince.Store(0)
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	pool.NotifyNodeDirty(hash)

	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if !plat.View().Contains(hash) {
		t.Fatal("registered platform lost a node change that occurred before publish")
	}
}

func TestPool_ReplacePlatform_Success(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	oldPlat := platform.NewPlatform("p-replace", "OldName", nil, nil)
	pool.RegisterPlatform(oldPlat)

	nextPlat := platform.NewPlatform("p-replace", "NewName", nil, nil)
	if err := pool.ReplacePlatform(nextPlat); err != nil {
		t.Fatalf("ReplacePlatform error: %v", err)
	}

	gotByID, ok := pool.GetPlatform("p-replace")
	if !ok || gotByID != nextPlat {
		t.Fatal("GetPlatform should return replaced platform by ID")
	}

	if _, ok := pool.GetPlatformByName("OldName"); ok {
		t.Fatal("old name mapping should be removed")
	}
	gotByName, ok := pool.GetPlatformByName("NewName")
	if !ok || gotByName != nextPlat {
		t.Fatal("new name mapping should point to replaced platform")
	}
}

func TestPool_ReplacePlatform_WaitsForPlatformReadOwner(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)
	oldPlat := platform.NewPlatform("p-read-owner", "ReadOwner", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	readEntered := make(chan struct{})
	allowRead := make(chan struct{})
	readDone := make(chan bool, 1)
	go func() {
		readDone <- pool.WithPlatformReadByName("ReadOwner", func(got *platform.Platform) {
			if got != oldPlat {
				t.Errorf("read callback got platform %p, want old %p", got, oldPlat)
			}
			close(readEntered)
			<-allowRead
		})
	}()
	<-readEntered

	replaceAttempted := make(chan struct{})
	allowReplaceLock := make(chan struct{})
	var attemptOnce sync.Once
	pool.beforePlatformReplaceLockHook = func() {
		attemptOnce.Do(func() { close(replaceAttempted) })
		<-allowReplaceLock
	}
	defer func() {
		pool.beforePlatformReplaceLockHook = nil
		select {
		case <-allowRead:
		default:
			close(allowRead)
		}
		select {
		case <-allowReplaceLock:
		default:
			close(allowReplaceLock)
		}
	}()

	nextPlat := platform.NewPlatform("p-read-owner", "ReadOwnerNext", nil, nil)
	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(nextPlat) }()
	<-replaceAttempted
	close(allowReplaceLock)

	select {
	case err := <-replaceDone:
		t.Fatalf("ReplacePlatform committed while platform read owner was held: %v", err)
	default:
	}

	close(allowRead)
	if ok := <-readDone; !ok {
		t.Fatal("WithPlatformReadByName unexpectedly reported missing platform")
	}
	if err := <-replaceDone; err != nil {
		t.Fatalf("ReplacePlatform: %v", err)
	}
	got, ok := pool.GetPlatform("p-read-owner")
	if !ok || got != nextPlat {
		t.Fatalf("replacement not published after read owner release: got=%p ok=%v", got, ok)
	}
}

func TestPool_WithPlatformReadByID_HoldsExactGeneration(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)
	oldPlat := platform.NewPlatform("p-read-id-owner", "ReadIDOwner", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	readEntered := make(chan struct{})
	allowRead := make(chan struct{})
	readDone := make(chan bool, 1)
	go func() {
		readDone <- pool.WithPlatformReadByID(oldPlat.ID, oldPlat, func() {
			close(readEntered)
			<-allowRead
		})
	}()
	select {
	case <-readEntered:
	case <-time.After(time.Second):
		t.Fatal("platform read owner did not enter")
	}

	replaceAttempted := make(chan struct{})
	pool.beforePlatformReplaceLockHook = func() { close(replaceAttempted) }
	defer func() {
		pool.beforePlatformReplaceLockHook = nil
		select {
		case <-allowRead:
		default:
			close(allowRead)
		}
	}()

	nextPlat := platform.NewPlatform(oldPlat.ID, "ReadIDOwnerNext", nil, nil)
	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(nextPlat) }()
	select {
	case <-replaceAttempted:
	case <-time.After(time.Second):
		t.Fatal("platform replacement did not reach its publication lock")
	}
	select {
	case err := <-replaceDone:
		t.Fatalf("ReplacePlatform committed while exact platform read owner was held: %v", err)
	default:
	}

	close(allowRead)
	select {
	case ok := <-readDone:
		if !ok {
			t.Fatal("WithPlatformReadByID unexpectedly rejected the current generation")
		}
	case <-time.After(time.Second):
		t.Fatal("platform read owner did not release")
	}
	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReplacePlatform did not finish after exact read owner release")
	}
	if got, ok := pool.GetPlatform(oldPlat.ID); !ok || got != nextPlat {
		t.Fatalf("replacement not published after exact read owner release: got=%p ok=%v", got, ok)
	}
}

func TestPool_ReplacePlatform_NameConflict(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	platA := platform.NewPlatform("p-a", "A", nil, nil)
	platB := platform.NewPlatform("p-b", "B", nil, nil)
	pool.RegisterPlatform(platA)
	pool.RegisterPlatform(platB)

	conflict := platform.NewPlatform("p-a", "B", nil, nil)
	err := pool.ReplacePlatform(conflict)
	if err == nil || !errors.Is(err, ErrPlatformNameConflict) {
		t.Fatalf("ReplacePlatform error = %v, want ErrPlatformNameConflict", err)
	}

	gotA, ok := pool.GetPlatform("p-a")
	if !ok || gotA != platA {
		t.Fatal("platform p-a should remain unchanged on conflict")
	}
	gotByNameA, ok := pool.GetPlatformByName("A")
	if !ok || gotByNameA != platA {
		t.Fatal("name mapping A should remain unchanged on conflict")
	}
	gotByNameB, ok := pool.GetPlatformByName("B")
	if !ok || gotByNameB != platB {
		t.Fatal("name mapping B should remain unchanged on conflict")
	}
}

func TestPool_ReplacePlatform_NotRegistered(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := newTestPool(subMgr)

	err := pool.ReplacePlatform(platform.NewPlatform("missing", "Nope", nil, nil))
	if err == nil || !errors.Is(err, ErrPlatformNotRegistered) {
		t.Fatalf("ReplacePlatform error = %v, want ErrPlatformNotRegistered", err)
	}
}

func TestPool_ReplacePlatform_RebuildsViewBeforePublish(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	oldPlat := platform.NewPlatform("p-rebuild", "Old", nil, nil)
	pool.RegisterPlatform(oldPlat)

	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"us-node"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")
	entry, _ := pool.GetEntry(h)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)
	pool.NotifyNodeDirty(h)

	// New platform requires "us" in tag. If ReplacePlatform skipped rebuild,
	// its view would remain empty here.
	nextPlat := platform.NewPlatform(
		"p-rebuild",
		"New",
		[]*regexp.Regexp{regexp.MustCompile("us")},
		nil,
	)
	if err := pool.ReplacePlatform(nextPlat); err != nil {
		t.Fatalf("ReplacePlatform error: %v", err)
	}

	got, ok := pool.GetPlatform("p-rebuild")
	if !ok || got != nextPlat {
		t.Fatal("expected replaced platform by ID")
	}
	if !got.View().Contains(h) {
		t.Fatal("replaced platform view should include routable us-node")
	}
}

func TestPool_ReplacePlatformDoesNotPublishStaleSameHashEntry(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-replace-entry-generation", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entryA, ok := pool.GetEntry(h)
	if !ok || entryA == nil {
		t.Fatal("initial node entry not found")
	}
	entryA.CircuitOpenSince.Store(0)
	outboundA := testutil.NewNoopOutbound()
	entryA.Outbound.Store(&outboundA)
	entryA.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryA.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	oldPlat := platform.NewPlatform("p-replace-entry-generation", "Old", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform old: %v", err)
	}
	pool.NotifyNodeDirty(h)
	if !oldPlat.View().Contains(h) {
		t.Fatal("initial platform view should contain entry A")
	}

	nextPlat := platform.NewPlatform(
		oldPlat.ID,
		"New",
		nil,
		[]string{"us"},
	)
	rebuildEntered := make(chan struct{})
	allowRebuild := make(chan struct{})
	var releaseRebuildOnce sync.Once
	releaseRebuild := func() { releaseRebuildOnce.Do(func() { close(allowRebuild) }) }
	defer releaseRebuild()
	var rebuildOnce sync.Once
	pool.geoLookup = func(netip.Addr) string {
		rebuildOnce.Do(func() {
			close(rebuildEntered)
			<-allowRebuild
		})
		return "us"
	}

	removed := make(chan struct{})
	var removedOnce sync.Once
	pool.afterNodeDeleteComputeHook = func(hash node.Hash, _ *node.NodeEntry) {
		if hash == h {
			removedOnce.Do(func() { close(removed) })
		}
	}
	mutationNotifications := atomic.Int32{}
	mutationsReady := make(chan struct{})
	var mutationsReadyOnce sync.Once
	pool.beforePlatformNotifyLockHook = func() {
		if mutationNotifications.Add(1) == 2 {
			mutationsReadyOnce.Do(func() { close(mutationsReady) })
		}
	}

	nextNotifyEntered := make(chan struct{})
	allowNextNotify := make(chan struct{})
	var releaseNextNotifyOnce sync.Once
	releaseNextNotify := func() { releaseNextNotifyOnce.Do(func() { close(allowNextNotify) }) }
	defer releaseNextNotify()
	var nextNotifyOnce sync.Once
	pool.beforePlatformNotifyHook = func(plat *platform.Platform) {
		if plat != nextPlat {
			return
		}
		nextNotifyOnce.Do(func() {
			close(nextNotifyEntered)
			<-allowNextNotify
		})
	}
	defer func() {
		pool.geoLookup = func(netip.Addr) string { return "us" }
		pool.afterNodeDeleteComputeHook = nil
		pool.beforePlatformNotifyLockHook = nil
		pool.beforePlatformNotifyHook = nil
	}()

	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(nextPlat) }()
	select {
	case <-rebuildEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement rebuild did not evaluate the captured entry")
	}

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(h, sub.ID)
		close(removeDone)
	}()
	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("node removal did not publish its delete")
	}

	addDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(h, raw, sub.ID)
		close(addDone)
	}()
	select {
	case <-mutationsReady:
	case <-time.After(2 * time.Second):
		t.Fatal("node replacement did not reach both notification admissions")
	}
	entryB, ok := pool.GetEntry(h)
	if !ok || entryB == nil || entryB == entryA {
		t.Fatalf("same-hash replacement did not publish a new entry: got=%p old=%p ok=%v", entryB, entryA, ok)
	}
	entryB.CircuitOpenSince.Store(0)
	outboundB := testutil.NewNoopOutbound()
	entryB.Outbound.Store(&outboundB)
	entryB.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryB.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	releaseRebuild()
	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not publish after rebuild release")
	}
	select {
	case <-nextNotifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement notifications did not reach the new platform")
	}

	foundB := false
	for _, published := range nextPlat.SnapshotViewEntries() {
		if published.Hash != h {
			continue
		}
		if published.Entry == entryA {
			t.Fatalf("new platform published stale same-hash entry A after entry B replaced it")
		}
		if published.Entry != entryB {
			t.Fatalf("new platform published unexpected same-hash entry %p, want current entry B %p", published.Entry, entryB)
		}
		foundB = true
	}
	if !foundB {
		t.Fatal("new platform did not publish current same-hash entry B")
	}

	releaseNextNotify()
	select {
	case <-removeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("node removal notification did not finish")
	}
	select {
	case <-addDone:
	case <-time.After(2 * time.Second):
		t.Fatal("node add notification did not finish")
	}
}

func TestPool_ReplacePlatformRepeatedNodeChangesFailClosedAndRecover(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-replace-entry-stale", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entryA, ok := pool.GetEntry(h)
	if !ok || entryA == nil {
		t.Fatal("initial node entry not found")
	}
	entryA.CircuitOpenSince.Store(0)
	outboundA := testutil.NewNoopOutbound()
	entryA.Outbound.Store(&outboundA)
	entryA.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryA.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{Ewma: 100 * time.Millisecond, LastUpdated: time.Now()})

	oldPlat := platform.NewPlatform("p-replace-entry-stale", "Old", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform old: %v", err)
	}
	pool.NotifyNodeDirty(h)
	if !oldPlat.View().Contains(h) {
		t.Fatal("initial platform view should contain entry A")
	}

	nextPlat := platform.NewPlatform(oldPlat.ID, "New", nil, []string{"us"})
	firstRebuildEntered := make(chan struct{})
	allowFirstRebuild := make(chan struct{})
	secondRebuildEntered := make(chan struct{})
	allowSecondRebuild := make(chan struct{})
	var geoCalls atomic.Int32
	pool.geoLookup = func(netip.Addr) string {
		switch geoCalls.Add(1) {
		case 1:
			close(firstRebuildEntered)
			<-allowFirstRebuild
		case 2:
			close(secondRebuildEntered)
			<-allowSecondRebuild
		}
		return "us"
	}
	defer func() { pool.geoLookup = func(netip.Addr) string { return "us" } }()
	deleted := make(chan struct{}, 2)
	pool.afterNodeDeleteComputeHook = func(hash node.Hash, _ *node.NodeEntry) {
		if hash == h {
			deleted <- struct{}{}
		}
	}
	added := make(chan struct{}, 2)
	pool.onNodeAdded = func(hash node.Hash) {
		if hash == h {
			added <- struct{}{}
		}
	}
	defer func() {
		pool.afterNodeDeleteComputeHook = nil
		pool.onNodeAdded = nil
	}()

	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(nextPlat) }()
	select {
	case <-firstRebuildEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first replacement rebuild did not enter")
	}

	firstRemoveDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(h, sub.ID)
		close(firstRemoveDone)
	}()
	select {
	case <-deleted:
	case <-time.After(2 * time.Second):
		t.Fatal("first node deletion did not reach its linearization hook")
	}
	firstAddDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(h, raw, sub.ID)
		close(firstAddDone)
	}()
	select {
	case <-added:
	case <-time.After(2 * time.Second):
		t.Fatal("first node add did not publish entry B")
	}
	entryB, ok := pool.GetEntry(h)
	if !ok || entryB == nil || entryB == entryA {
		t.Fatal("first same-hash replacement did not publish entry B")
	}
	entryB.CircuitOpenSince.Store(0)
	outboundB := testutil.NewNoopOutbound()
	entryB.Outbound.Store(&outboundB)
	entryB.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryB.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{Ewma: 100 * time.Millisecond, LastUpdated: time.Now()})
	close(allowFirstRebuild)

	select {
	case <-secondRebuildEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second replacement rebuild did not enter")
	}
	secondRemoveDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(h, sub.ID)
		close(secondRemoveDone)
	}()
	select {
	case <-deleted:
	case <-time.After(2 * time.Second):
		t.Fatal("second node deletion did not reach its linearization hook")
	}
	secondAddDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(h, raw, sub.ID)
		close(secondAddDone)
	}()
	select {
	case <-added:
	case <-time.After(2 * time.Second):
		t.Fatal("second node add did not publish entry C")
	}
	entryC, ok := pool.GetEntry(h)
	if !ok || entryC == nil || entryC == entryB {
		t.Fatal("second same-hash replacement did not publish entry C")
	}
	entryC.CircuitOpenSince.Store(0)
	outboundC := testutil.NewNoopOutbound()
	entryC.Outbound.Store(&outboundC)
	entryC.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryC.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{Ewma: 100 * time.Millisecond, LastUpdated: time.Now()})
	close(allowSecondRebuild)

	select {
	case err := <-replaceDone:
		if !errors.Is(err, ErrPlatformRebuildStale) {
			t.Fatalf("ReplacePlatform error = %v, want ErrPlatformRebuildStale", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not fail closed after repeated node changes")
	}
	for _, published := range oldPlat.SnapshotViewEntries() {
		if published.Hash == h && published.Entry != entryA {
			t.Fatalf("failed replacement changed old platform view to unverified entry %p", published.Entry)
		}
	}
	if oldPlat.View().Contains(h) == false {
		t.Fatal("failed replacement must preserve the last verified old view")
	}
	select {
	case <-firstRemoveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first deletion notification did not finish")
	}
	select {
	case <-secondRemoveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second deletion notification did not finish")
	}
	select {
	case <-firstAddDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first node add notification did not finish")
	}
	select {
	case <-secondAddDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second node add notification did not finish")
	}

	if err := pool.RebuildPlatform(oldPlat); err != nil {
		t.Fatalf("rebuild after stabilization: %v", err)
	}
	entries := oldPlat.SnapshotViewEntries()
	if len(entries) != 1 || entries[0].Hash != h || entries[0].Entry != entryC {
		t.Fatalf("recovery published %+v, want current entry C", entries)
	}
}

func TestPool_StructuralChangeTailNotifyDoesNotCreateSecondGeneration(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-structural-generation-once", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entryA, ok := pool.GetEntry(h)
	if !ok || entryA == nil {
		t.Fatal("initial node entry not found")
	}
	entryA.CircuitOpenSince.Store(0)
	outboundA := testutil.NewNoopOutbound()
	entryA.Outbound.Store(&outboundA)
	entryA.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryA.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	oldPlat := platform.NewPlatform("p-structural-generation-once", "Old", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform old: %v", err)
	}
	nextPlat := platform.NewPlatform(oldPlat.ID, "New", nil, []string{"us"})

	firstEvaluation := make(chan struct{})
	allowFirstEvaluation := make(chan struct{})
	secondEvaluation := make(chan struct{})
	allowSecondEvaluation := make(chan struct{})
	var geoCalls atomic.Int32
	pool.geoLookup = func(netip.Addr) string {
		switch geoCalls.Add(1) {
		case 1:
			close(firstEvaluation)
			<-allowFirstEvaluation
		case 2:
			close(secondEvaluation)
			<-allowSecondEvaluation
		}
		return "us"
	}

	removeNotifyEntered := make(chan struct{})
	addNotifyEntered := make(chan struct{})
	allowRemoveNotify := make(chan struct{})
	allowAddNotify := make(chan struct{})
	var notifyCalls atomic.Int32
	pool.beforePlatformNotifyLockHook = func() {
		switch notifyCalls.Add(1) {
		case 1:
			close(removeNotifyEntered)
			<-allowRemoveNotify
		case 2:
			close(addNotifyEntered)
			<-allowAddNotify
		}
	}

	addedRuntimeEntered := make(chan struct{})
	allowAddedRuntime := make(chan struct{})
	pool.SetOnNodeAddedRuntime(func(hash node.Hash, _ *node.NodeEntry) {
		if hash != h {
			return
		}
		close(addedRuntimeEntered)
		<-allowAddedRuntime
	})
	defer func() {
		pool.geoLookup = func(netip.Addr) string { return "us" }
		pool.beforePlatformNotifyLockHook = nil
		pool.SetOnNodeAddedRuntime(nil)
		select {
		case <-allowFirstEvaluation:
		default:
			close(allowFirstEvaluation)
		}
		select {
		case <-allowSecondEvaluation:
		default:
			close(allowSecondEvaluation)
		}
		select {
		case <-allowRemoveNotify:
		default:
			close(allowRemoveNotify)
		}
		select {
		case <-allowAddNotify:
		default:
			close(allowAddNotify)
		}
		select {
		case <-allowAddedRuntime:
		default:
			close(allowAddedRuntime)
		}
	}()

	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(nextPlat) }()
	select {
	case <-firstEvaluation:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not reach first evaluation")
	}

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(h, sub.ID)
		close(removeDone)
	}()
	select {
	case <-removeNotifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("remove tail notification did not reach its barrier")
	}

	addDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(h, raw, sub.ID)
		close(addDone)
	}()
	select {
	case <-addedRuntimeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement add did not reach runtime preparation")
	}
	entryB, ok := pool.GetEntry(h)
	if !ok || entryB == nil || entryB == entryA {
		t.Fatalf("same-hash replacement did not publish entry B: got=%p old=%p ok=%v", entryB, entryA, ok)
	}
	entryB.CircuitOpenSince.Store(0)
	outboundB := testutil.NewNoopOutbound()
	entryB.Outbound.Store(&outboundB)
	entryB.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entryB.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	close(allowFirstEvaluation)
	select {
	case <-secondEvaluation:
	case <-time.After(2 * time.Second):
		t.Fatal("retry rebuild did not capture entry B")
	}

	// The add has already advanced the structural generation. Its runtime
	// callback is deliberately held between that linearization point and the
	// tail notification, so the retry can capture B before the tail runs.
	close(allowAddedRuntime)
	select {
	case <-addNotifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("add tail notification did not reach its barrier")
	}
	close(allowSecondEvaluation)

	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform returned %v after one structural change", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not finish")
	}

	if !nextPlat.View().Contains(h) {
		t.Fatal("replacement did not publish the current same-hash entry")
	}
	published := nextPlat.SnapshotViewEntries()
	for _, candidate := range published {
		if candidate.Hash == h && candidate.Entry != entryB {
			t.Fatalf("replacement published stale entry %p, want current entry B %p", candidate.Entry, entryB)
		}
	}

	close(allowRemoveNotify)
	close(allowAddNotify)
	select {
	case <-removeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("remove tail notification did not finish")
	}
	select {
	case <-addDone:
	case <-time.After(2 * time.Second):
		t.Fatal("add tail notification did not finish")
	}
}

func TestPool_ReplacePlatformDoesNotPublishStaleHealthDecision(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-replace-health-generation", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entry, ok := pool.GetEntry(h)
	if !ok || entry == nil {
		t.Fatal("node entry not found")
	}
	entry.CircuitOpenSince.Store(0)
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{Ewma: 100 * time.Millisecond, LastUpdated: time.Now()})

	oldPlat := platform.NewPlatform("p-replace-health-generation", "Old", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform old: %v", err)
	}
	pool.NotifyNodeDirty(h)
	if !oldPlat.View().Contains(h) {
		t.Fatal("initial platform view should contain the healthy entry")
	}

	nextPlat := platform.NewPlatform(oldPlat.ID, "New", nil, []string{"us"})
	evaluationEntered := make(chan struct{})
	allowEvaluation := make(chan struct{})
	var geoOnce sync.Once
	pool.geoLookup = func(netip.Addr) string {
		geoOnce.Do(func() {
			close(evaluationEntered)
			<-allowEvaluation
		})
		return "us"
	}
	defer func() { pool.geoLookup = func(netip.Addr) string { return "us" } }()

	notifyAttempted := make(chan struct{})
	allowNotify := make(chan struct{})
	var notifyOnce sync.Once
	pool.beforePlatformNotifyLockHook = func() {
		notifyOnce.Do(func() {
			close(notifyAttempted)
			<-allowNotify
		})
	}
	defer func() {
		pool.beforePlatformNotifyLockHook = nil
		select {
		case <-allowNotify:
		default:
			close(allowNotify)
		}
	}()

	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(nextPlat) }()
	select {
	case <-evaluationEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement rebuild did not evaluate the entry")
	}

	// The rebuild has already passed the entry health check and is blocked in
	// GeoIP evaluation. The real outbound health path now makes the same entry
	// non-routable and enters NotifyNodeDirty, but is held before notification
	// admission so the replacement cannot rely on that later callback.
	entry.Outbound.Store(nil)
	notifyDone := make(chan struct{})
	go func() {
		pool.NotifyNodeDirty(h)
		close(notifyDone)
	}()
	select {
	case <-notifyAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("health notification did not reach its admission boundary")
	}
	close(allowEvaluation)

	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not complete while health notification was held")
	}
	if nextPlat.View().Contains(h) {
		t.Fatal("replacement published stale routable decision before NotifyNodeDirty completed")
	}

	close(allowNotify)
	select {
	case <-notifyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("health notification did not finish after admission release")
	}
}

func TestPool_ReplacePlatform_SerializesNodeNotificationWithPublish(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-replace-race", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h2 := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h2, subscription.ManagedNode{Tags: []string{"node-2"}})
	sub.SwapManagedNodes(mn)

	// Add h2 before installing the seams. It is present in the pool but not
	// routable until the replacement has finished its rebuild.
	pool.AddNodeFromSub(h2, raw, sub.ID)
	entry, ok := pool.GetEntry(h2)
	if !ok {
		t.Fatal("h2 entry not found")
	}
	entry.CircuitOpenSince.Store(0)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	oldPlat := platform.NewPlatform("p-replace-race", "Old", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform old: %v", err)
	}
	nextPlat := platform.NewPlatform(
		"p-replace-race",
		"New",
		[]*regexp.Regexp{regexp.MustCompile("node-2")},
		nil,
	)

	rebuildReached := make(chan struct{})
	releaseRebuild := make(chan struct{})
	notifyAttempted := make(chan struct{})
	snapshotReached := make(chan struct{})
	var notifyAttemptOnce sync.Once
	var snapshotOnce sync.Once

	// Every initialization operation is complete before either seam is
	// installed. The notification is allowed to reach the publication owner,
	// but is not awaited before replacement is released: the fixed path waits
	// there by design.
	pool.beforePlatformSnapshotLockHook = func() {
		snapshotOnce.Do(func() { close(snapshotReached) })
	}
	pool.beforePlatformNotifyLockHook = func() {
		notifyAttemptOnce.Do(func() { close(notifyAttempted) })
	}
	pool.afterPlatformRebuildHook = func() {
		close(rebuildReached)
		<-releaseRebuild
	}
	defer func() {
		pool.beforePlatformSnapshotLockHook = nil
		pool.beforePlatformNotifyLockHook = nil
		pool.afterPlatformRebuildHook = nil
		select {
		case <-releaseRebuild:
		default:
			close(releaseRebuild)
		}
	}()

	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- pool.ReplacePlatform(nextPlat)
	}()

	select {
	case <-rebuildReached:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not reach the rebuild/publish boundary")
	}
	if nextPlat.View().Contains(h2) {
		t.Fatal("next platform should exclude h2 while it is not routable")
	}

	// Make h2 routable only after next's rebuild has taken its snapshot.
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))

	notifyDone := make(chan struct{})
	go func() {
		pool.NotifyNodeDirty(h2)
		close(notifyDone)
	}()

	select {
	case <-notifyAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("node notification did not reach platform publication owner")
	}

	// Do not wait for NotifyNodeDirty before releasing replacement: the fixed
	// implementation intentionally blocks its publication read owner until
	// publish.
	close(releaseRebuild)

	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not complete")
	}
	select {
	case <-notifyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("node notification did not complete")
	}
	select {
	case <-snapshotReached:
	default:
		t.Fatal("node notification completed without reaching platform snapshot")
	}

	got, ok := pool.GetPlatform("p-replace-race")
	if !ok || got != nextPlat {
		t.Fatal("replacement did not publish next platform")
	}
	if !nextPlat.View().Contains(h2) {
		t.Fatal("published next platform lost the concurrent node notification")
	}
}

func TestPool_ReplacePlatform_DoesNotLoseNotificationCapturedBeforePublish(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s-replace-before-publish", "Provider", "url", true, false)
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("node entry not found")
	}
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	oldPlat := platform.NewPlatform("p-replace-before-publish", "Old", nil, nil)
	if err := pool.RegisterPlatform(oldPlat); err != nil {
		t.Fatalf("RegisterPlatform old: %v", err)
	}
	nextPlat := platform.NewPlatform(
		"p-replace-before-publish",
		"New",
		[]*regexp.Regexp{regexp.MustCompile("node")},
		nil,
	)

	snapshotCaptured := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	var snapshotBoundaryOnce sync.Once
	pool.beforePlatformSnapshotLockHook = func() {
		snapshotBoundaryOnce.Do(func() {
			close(snapshotCaptured)
			<-releaseSnapshot
		})
	}
	snapshotCopied := make(chan struct{})
	releaseCopied := make(chan struct{})
	var snapshotCopiedOnce sync.Once
	pool.afterPlatformSnapshotHook = func() {
		snapshotCopiedOnce.Do(func() {
			close(snapshotCopied)
			<-releaseCopied
		})
	}
	notifyReached := make(chan struct{})
	releaseNotify := make(chan struct{})
	replaceQueued := make(chan struct{})
	var replaceQueuedOnce sync.Once
	pool.beforePlatformNotifyHook = func(plat *platform.Platform) {
		if plat != oldPlat {
			return
		}
		close(notifyReached)
		<-releaseNotify
	}
	pool.platformBatchMu.afterWriterQueued = func() {
		replaceQueuedOnce.Do(func() { close(replaceQueued) })
	}
	rebuildReached := make(chan struct{})
	releaseRebuild := make(chan struct{})
	pool.afterPlatformRebuildHook = func() {
		close(rebuildReached)
		<-releaseRebuild
	}
	defer func() {
		pool.beforePlatformSnapshotLockHook = nil
		pool.afterPlatformSnapshotHook = nil
		pool.beforePlatformNotifyHook = nil
		pool.platformBatchMu.afterWriterQueued = nil
		pool.afterPlatformRebuildHook = nil
		select {
		case <-releaseSnapshot:
		default:
			close(releaseSnapshot)
		}
		select {
		case <-releaseCopied:
		default:
			close(releaseCopied)
		}
		select {
		case <-releaseNotify:
		default:
			close(releaseNotify)
		}
		select {
		case <-releaseRebuild:
		default:
			close(releaseRebuild)
		}
	}()

	notifyDone := make(chan struct{})
	go func() {
		pool.NotifyNodeDirty(h)
		close(notifyDone)
	}()
	select {
	case <-snapshotCaptured:
	case <-time.After(2 * time.Second):
		t.Fatal("notification did not reach platform snapshot boundary")
	}
	close(releaseSnapshot)
	select {
	case <-snapshotCopied:
	case <-time.After(2 * time.Second):
		t.Fatal("notification did not capture platform snapshot")
	}
	close(releaseCopied)
	select {
	case <-notifyReached:
	case <-time.After(2 * time.Second):
		t.Fatal("notification did not reach the old platform")
	}

	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- pool.ReplacePlatform(nextPlat)
	}()
	select {
	case <-replaceQueued:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement writer did not queue behind the notification owner")
	}
	select {
	case <-rebuildReached:
		t.Fatal("replacement rebuilt while the notification owner was still active")
	default:
	}
	// The node becomes routable while the notification owner is active and the
	// replacement writer is queued. Once the owner is released, the candidate
	// rebuild must observe this complete state before it is published.
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entry.CircuitOpenSince.Store(0)
	close(releaseNotify)
	select {
	case <-notifyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("captured notification did not finish")
	}
	select {
	case <-rebuildReached:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not reach rebuild/publish boundary")
	}

	close(releaseRebuild)

	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not complete")
	}
	if !nextPlat.View().Contains(h) {
		t.Fatal("published replacement lost a notification captured before publish")
	}
}

// --- Subscription operation-lock tests ---

func TestSubscription_WithOpLock(t *testing.T) {
	mgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	mgr.Register(sub)

	// WithOpLock should serialize.
	counter := 0
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub.WithOpLock(func() {
				counter++
			})
		}()
	}
	wg.Wait()

	if counter != 100 {
		t.Fatalf("expected 100, got %d (serialization broken)", counter)
	}
}

func TestSubscription_WithOpLockAfterUnregister(t *testing.T) {
	mgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "Sub1", "url", true, false)
	mgr.Register(sub)

	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})

	var firstWG sync.WaitGroup
	firstWG.Add(1)
	go func() {
		defer firstWG.Done()
		sub.WithOpLock(func() {
			close(firstEntered)
			// Simulate delete path that unregisters while holding the lock.
			mgr.Unregister("s1")
			<-firstRelease
		})
	}()

	<-firstEntered

	go sub.WithOpLock(func() {
		close(secondEntered)
	})

	select {
	case <-secondEntered:
		close(firstRelease)
		firstWG.Wait()
		t.Fatal("second WithOpLock entered before first lock holder exited")
	case <-time.After(100 * time.Millisecond):
		// expected: second goroutine must block on the same lock
	}

	close(firstRelease)
	firstWG.Wait()

	select {
	case <-secondEntered:
		// expected
	case <-time.After(time.Second):
		t.Fatal("second WithOpLock did not enter after first lock holder exited")
	}
}

// --- Ephemeral Cleaner tests ---

func TestEphemeralCleaner_EvictsCircuitBroken(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "EphSub", "url", true, true) // ephemeral
	sub.SetEphemeralNodeEvictDelayNs(int64(1 * time.Minute))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)

	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")

	// Circuit-break the node for longer than evict delay.
	entry, _ := pool.GetEntry(h)
	entry.CircuitOpenSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	// Node should be evicted.
	if pool.Size() != 0 {
		t.Fatal("circuit-broken node should be evicted from ephemeral sub")
	}

	// Managed node stays in view, but must be marked evicted.
	managed, ok := sub.ManagedNodes().LoadNode(h)
	if !ok {
		t.Fatal("managed node should remain after eviction")
	}
	if !managed.Evicted {
		t.Fatal("managed node should be marked evicted")
	}
}

func TestEphemeralCleaner_SkipsNonEphemeral(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "RegularSub", "url", true, false) // NOT ephemeral
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")

	entry, _ := pool.GetEntry(h)
	entry.CircuitOpenSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	// Node should NOT be evicted since sub is not ephemeral.
	if pool.Size() != 1 {
		t.Fatal("non-ephemeral sub nodes should not be evicted")
	}
}

func TestEphemeralCleaner_SkipsRecentCircuitBreak(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "EphSub", "url", true, true)
	sub.SetEphemeralNodeEvictDelayNs(int64(1 * time.Minute))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)

	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node-1"}})
	sub.SwapManagedNodes(mn)

	pool.AddNodeFromSub(h, raw, "s1")

	// Circuit-break recently (less than evict delay).
	entry, _ := pool.GetEntry(h)
	entry.CircuitOpenSince.Store(time.Now().Add(-10 * time.Second).UnixNano())

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	// Should NOT be evicted yet.
	if pool.Size() != 1 {
		t.Fatal("recently circuit-broken node should not be evicted yet")
	}
}
