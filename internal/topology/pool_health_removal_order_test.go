package topology

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
)

func TestHealthResultRemovalDoesNotLeaveLateDynamicDirtyWrite(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	var pool *GlobalNodePool
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	readers := state.CacheReaders{
		ReadNodeDynamic: func(hash string) *model.NodeDynamic {
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			return &model.NodeDynamic{
				Hash:                               hash,
				FailureCount:                       int(entry.FailureCount.Load()),
				CircuitOpenSince:                   entry.CircuitOpenSince.Load(),
				LastLatencyProbeAttemptNs:          entry.LastLatencyProbeAttempt.Load(),
				LastAuthorityLatencyProbeAttemptNs: entry.LastAuthorityLatencyProbeAttempt.Load(),
				LastEgressUpdateAttemptNs:          entry.LastEgressUpdateAttempt.Load(),
			}
		},
	}

	callbackSawLive := make(chan bool, 1)
	pool = NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged: func(hash node.Hash) {
			if !engine.MarkNodeDynamic(hash.Hex()) {
				return
			}
			_, live := pool.GetEntry(hash)
			callbackSawLive <- live
		},
		OnNodeRemoved: func(hash node.Hash, _ *node.NodeEntry) {
			engine.MarkNodeDynamicDelete(hash.Hex())
		},
	})

	raw := []byte(`{"type":"ss","server":"1.1.1.1"}`)
	h := node.HashFromRawOptions(raw)
	sub.SwapManagedNodes(subscription.NewManagedNodes())
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	if err := pool.RegisterPlatform(platform.NewPlatform("p-health-order", "HealthOrder", nil, nil)); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	// Seed the cache row before the interleaving. The test must check the real
	// delete path, not only the in-memory dirty set.
	if !engine.MarkNodeDynamic(h.Hex()) {
		t.Fatal("seed MarkNodeDynamic was rejected")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("seed FlushDirtySets: %v", err)
	}

	notifyEntered := make(chan struct{})
	allowNotify := make(chan struct{})
	var notifyCalls atomic.Int32
	var releaseOnce sync.Once
	pool.beforePlatformNotifyHook = func(*platform.Platform) {
		if notifyCalls.Add(1) != 1 {
			return
		}
		close(notifyEntered)
		<-allowNotify
	}
	defer func() { releaseOnce.Do(func() { close(allowNotify) }) }()

	healthDone := make(chan bool, 1)
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("node entry not found")
	}
	go func() {
		healthDone <- pool.RecordResultForEntry(h, entry, true)
	}()
	select {
	case <-notifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("health result did not reach the notification gate")
	}

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(h, sub.ID)
		close(removeDone)
	}()

	// A removal may not pass the health event owner while the Compute result
	// is waiting for its persistence callback. The old implementation returns
	// here and leaves a late dynamic upsert behind the delete callback.
	removedBeforeHealthCallback := false
	select {
	case <-removeDone:
		removedBeforeHealthCallback = true
	case <-time.After(200 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(allowNotify) })
	select {
	case applied := <-healthDone:
		if !applied {
			t.Fatal("health result was not applied")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health result did not finish")
	}
	select {
	case <-removeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("node removal did not finish")
	}
	select {
	case live := <-callbackSawLive:
		if !live {
			t.Fatal("health callback ran after node removal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health callback was not delivered")
	}

	if removedBeforeHealthCallback {
		t.Fatal("node removal completed before the health callback was delivered")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final FlushDirtySets: %v", err)
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("late health dirty write remains after delete: %d", dirty)
	}
	dynamics, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(dynamics) != 0 {
		t.Fatalf("deleted node was persisted after late health callback: %+v", dynamics)
	}
}

func TestHealthLatencyRemovalDoesNotLeaveLateLatencyDirtyWrite(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	var pool *GlobalNodePool
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	readers := state.CacheReaders{
		ReadNodeLatency: func(key model.NodeLatencyKey) *model.NodeLatency {
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok || entry.LatencyTable == nil {
				return nil
			}
			stats, ok := entry.LatencyTable.GetDomainStats(key.Domain)
			if !ok {
				return nil
			}
			return &model.NodeLatency{
				NodeHash:      key.NodeHash,
				Domain:        key.Domain,
				EwmaNs:        int64(stats.Ewma),
				LastUpdatedNs: stats.LastUpdated.UnixNano(),
			}
		},
	}

	callbackSawLive := make(chan bool, 1)
	dynamicEntered := make(chan struct{})
	allowDynamic := make(chan struct{})
	var dynamicOnce sync.Once
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(allowDynamic) }) }()
	pool = NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged: func(node.Hash) {
			dynamicOnce.Do(func() {
				close(dynamicEntered)
				<-allowDynamic
			})
		},
		OnNodeLatencyChanged: func(hash node.Hash, domain string) {
			if !engine.MarkNodeLatency(hash.Hex(), domain) {
				return
			}
			_, live := pool.GetEntry(hash)
			callbackSawLive <- live
		},
		OnNodeRemoved: func(hash node.Hash, entry *node.NodeEntry) {
			if entry == nil || entry.LatencyTable == nil {
				return
			}
			entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
				engine.MarkNodeLatencyDelete(hash.Hex(), domain)
				return true
			})
		},
	})

	raw := []byte(`{"type":"ss","server":"2.2.2.2"}`)
	h := node.HashFromRawOptions(raw)
	sub.SwapManagedNodes(subscription.NewManagedNodes())
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(h, raw, sub.ID)
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("node entry not found")
	}
	seedLatency := 25 * time.Millisecond
	entry.LatencyTable.LoadEntry("seed.example.org", node.DomainLatencyStats{
		Ewma:        seedLatency,
		LastUpdated: time.Now(),
	})
	if !engine.MarkNodeLatency(h.Hex(), "example.org") {
		t.Fatal("seed MarkNodeLatency was rejected")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("seed FlushDirtySets: %v", err)
	}

	latency := 35 * time.Millisecond
	healthDone := make(chan bool, 1)
	go func() {
		healthDone <- pool.RecordLatencyForEntry(h, entry, "new.example.com", &latency)
	}()
	select {
	case <-dynamicEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("latency result did not reach the dynamic callback gate")
	}

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(h, sub.ID)
		close(removeDone)
	}()
	removedBeforeLatencyCallback := false
	select {
	case <-removeDone:
		removedBeforeLatencyCallback = true
	case <-time.After(200 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(allowDynamic) })
	select {
	case applied := <-healthDone:
		if !applied {
			t.Fatal("latency result was not applied")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("latency result did not finish")
	}
	select {
	case <-removeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("node removal did not finish")
	}
	select {
	case live := <-callbackSawLive:
		if !live {
			t.Fatal("latency callback ran after node removal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("latency callback was not delivered")
	}

	if removedBeforeLatencyCallback {
		t.Fatal("node removal completed before the latency callback was delivered")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final FlushDirtySets: %v", err)
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("late latency dirty write remains after delete: %d", dirty)
	}
	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	if len(latencies) != 0 {
		t.Fatalf("deleted node latency was persisted after late callback: %+v", latencies)
	}
}
