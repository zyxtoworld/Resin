package topology

import (
	"context"
	"errors"
	"net/netip"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
)

func TestSchedulerRefreshCannotBeFlushedBetweenPoolMutations(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"scheduler-persistence-interleave",
		"scheduler-persistence-interleave",
		"https://example.com/sub",
		true,
		false,
	)
	sub.SetFetchConfig(sub.URL(), int64(time.Hour))
	subMgr.Register(sub)

	oldRaw := `{"type":"shadowsocks","tag":"old","server":"1.1.1.1","server_port":443}`
	newRaw := `{"type":"vmess","tag":"new","server":"2.2.2.2","server_port":443}`
	oldHash := node.HashFromRawOptions([]byte(oldRaw))
	newHash := node.HashFromRawOptions([]byte(newRaw))
	var gateNewAdd atomic.Bool
	newAddMarked := make(chan struct{})
	allowNewAddReturn := make(chan struct{})
	var newAddOnce sync.Once
	releaseNewAdd := func() { close(allowNewAddReturn) }
	t.Cleanup(func() {
		newAddOnce.Do(func() { close(newAddMarked) })
		select {
		case <-allowNewAddReturn:
		default:
			close(allowNewAddReturn)
		}
	})

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChanged: func(subID string, hash node.Hash, added bool) {
			if added {
				if !engine.MarkSubscriptionNode(subID, hash.Hex()) {
					t.Errorf("MarkSubscriptionNode(%s) was rejected", hash.Hex())
				}
				if gateNewAdd.Load() && hash == newHash {
					newAddOnce.Do(func() { close(newAddMarked) })
					<-allowNewAddReturn
				}
				return
			}
			if !engine.MarkSubscriptionNodeDelete(subID, hash.Hex()) {
				t.Errorf("MarkSubscriptionNodeDelete(%s) was rejected", hash.Hex())
			}
		},
	})

	readers := state.CacheReaders{
		ReadSubscriptionNode: func(key state.SubscriptionNodeDirtyKey) *model.SubscriptionNode {
			managedSub := subMgr.Lookup(key.SubscriptionID)
			if managedSub == nil {
				return nil
			}
			hash, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			managed, ok := managedSub.ManagedNodes().LoadNode(hash)
			if !ok {
				return nil
			}
			return &model.SubscriptionNode{
				SubscriptionID: key.SubscriptionID,
				NodeHash:       key.NodeHash,
				Tags:           append([]string(nil), managed.Tags...),
				Evicted:        managed.Evicted,
			}
		},
	}

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		RunRefreshMutation: func(fn func(PersistenceAdmission)) bool {
			return engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
				fn(admission)
			})
		},
		Fetcher: func(context.Context, string) ([]byte, error) {
			return makeSubscriptionJSON(oldRaw), nil
		},
	})
	scheduler.UpdateSubscription(sub)
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("initial FlushDirtySets: %v", err)
	}

	gateNewAdd.Store(true)
	scheduler.Fetcher = func(context.Context, string) ([]byte, error) {
		return makeSubscriptionJSON(newRaw), nil
	}
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()
	select {
	case <-newAddMarked:
	case <-time.After(time.Second):
		t.Fatal("refresh did not reach the first new-node dirty callback")
	}

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 100*time.Millisecond)
	interleavedErr := engine.FlushDirtySetsContext(flushCtx, readers)
	cancelFlush()

	releaseNewAdd()
	if ok := <-refreshDone; !ok {
		t.Fatal("refresh was not admitted")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final FlushDirtySets: %v", err)
	}
	rows, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes after refresh: %v", err)
	}
	for _, row := range rows {
		if row.NodeHash == oldHash.Hex() {
			t.Fatal("old subscription-node row survived completed refresh")
		}
	}
	if !errors.Is(interleavedErr, context.DeadlineExceeded) {
		t.Fatalf("interleaved FlushDirtySets error = %v, want deadline while refresh admission is active", interleavedErr)
	}
}

func TestSchedulerRefreshPoolPersistenceDoesNotLoseMarksWhenShutdownClosesAdmission(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"scheduler-persistence-admission",
		"scheduler-persistence-admission",
		"https://example.com/sub",
		true,
		false,
	)
	sub.SetFetchConfig(sub.URL(), int64(time.Hour))
	subMgr.Register(sub)

	oldRaw := `{"type":"shadowsocks","tag":"old-admission","server":"1.1.1.1","server_port":443}`
	newRaw := `{"type":"vmess","tag":"new-admission","server":"2.2.2.2","server_port":443}`
	oldHash := node.HashFromRawOptions([]byte(oldRaw))
	newHash := node.HashFromRawOptions([]byte(newRaw))

	var gateNewNode atomic.Bool
	newNodeEntered := make(chan struct{})
	allowNewNode := make(chan struct{})
	var newNodeOnce sync.Once
	t.Cleanup(func() {
		newNodeOnce.Do(func() { close(newNodeEntered) })
		select {
		case <-allowNewNode:
		default:
			close(allowNewNode)
		}
	})

	var staticAccepted atomic.Bool
	var subscriptionAccepted atomic.Bool
	var finalAccepted atomic.Bool
	var legacyCallbackCalls atomic.Int32
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeAdded:            func(node.Hash) { legacyCallbackCalls.Add(1) },
		OnSubNodeChanged:       func(string, node.Hash, bool) { legacyCallbackCalls.Add(1) },
		OnFinalNodeRemoved:     func(string, node.Hash, *node.NodeEntry) { legacyCallbackCalls.Add(1) },
		OnNodeAddedWithPersistence: func(hash node.Hash, admission PersistenceAdmission) {
			if gateNewNode.Load() && hash == newHash {
				newNodeOnce.Do(func() { close(newNodeEntered) })
				<-allowNewNode
			}
			if admission.MarkNodeStatic(hash.Hex()) {
				staticAccepted.Store(true)
			}
		},
		OnSubNodeChangedWithPersistence: func(subID string, hash node.Hash, added bool, admission PersistenceAdmission) {
			if added {
				if admission.MarkSubscriptionNode(subID, hash.Hex()) {
					subscriptionAccepted.Store(true)
				}
				return
			}
			admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
		},
		OnFinalNodeRemovedWithPersistence: func(subID string, hash node.Hash, entry *node.NodeEntry, admission PersistenceAdmission) {
			accepted := admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
			accepted = admission.MarkNodeStaticDelete(hash.Hex()) && accepted
			accepted = admission.MarkNodeDynamicDelete(hash.Hex()) && accepted
			if entry != nil && entry.LatencyTable != nil {
				entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
					accepted = admission.MarkNodeLatencyDelete(hash.Hex(), domain) && accepted
					return true
				})
			}
			finalAccepted.Store(accepted)
		},
	})

	readers := state.CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			return &model.NodeStatic{Hash: hash, RawOptions: append([]byte(nil), entry.RawOptions...)}
		},
		ReadNodeDynamic: func(hash string) *model.NodeDynamic {
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			return &model.NodeDynamic{Hash: hash, FailureCount: int(entry.FailureCount.Load()), CircuitOpenSince: entry.CircuitOpenSince.Load()}
		},
		ReadNodeLatency: func(key state.NodeLatencyDirtyKey) *model.NodeLatency {
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
			return &model.NodeLatency{NodeHash: key.NodeHash, Domain: key.Domain, EwmaNs: int64(stats.Ewma), LastUpdatedNs: stats.LastUpdated.UnixNano()}
		},
		ReadSubscriptionNode: func(key state.SubscriptionNodeDirtyKey) *model.SubscriptionNode {
			managedSub := subMgr.Lookup(key.SubscriptionID)
			if managedSub == nil {
				return nil
			}
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			managed, ok := managedSub.ManagedNodes().LoadNode(h)
			if !ok {
				return nil
			}
			return &model.SubscriptionNode{
				SubscriptionID: key.SubscriptionID,
				NodeHash:       key.NodeHash,
				Tags:           append([]string(nil), managed.Tags...),
				Evicted:        managed.Evicted,
			}
		},
	}

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		RunRefreshMutation: func(fn func(PersistenceAdmission)) bool {
			return engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
				fn(admission)
			})
		},
		Fetcher: func(context.Context, string) ([]byte, error) {
			return makeSubscriptionJSON(oldRaw), nil
		},
	})
	if !scheduler.UpdateSubscription(sub) {
		t.Fatal("initial refresh was not admitted")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("initial FlushDirtySets: %v", err)
	}
	oldEntry, ok := pool.GetEntry(oldHash)
	if !ok || oldEntry.LatencyTable == nil {
		t.Fatal("old entry latency table not initialized")
	}
	oldEntry.LatencyTable.LoadEntry("old-admission.example", node.DomainLatencyStats{
		Ewma:        25 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	if !engine.MarkNodeDynamic(oldHash.Hex()) || !engine.MarkNodeLatency(oldHash.Hex(), "old-admission.example") {
		t.Fatal("seed dynamic/latency marks were rejected")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("seed dynamic/latency FlushDirtySets: %v", err)
	}
	staticAccepted.Store(false)
	subscriptionAccepted.Store(false)

	gateNewNode.Store(true)
	scheduler.Fetcher = func(context.Context, string) ([]byte, error) {
		return makeSubscriptionJSON(newRaw), nil
	}
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()
	select {
	case <-newNodeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach new-node callback")
	}

	// Close the global gate while the outer mutation is still active. The
	// following wait must remain blocked until the callback releases the
	// admitted mutation; writer-aware callbacks may still mark through the
	// already-owned admission.
	engine.CloseDirtyWriteAdmission()
	closeStarted := make(chan struct{})
	closeDone := make(chan struct{})
	go func() {
		close(closeStarted)
		engine.CloseDirtyWriteAdmissionAndWait()
		close(closeDone)
	}()
	<-closeStarted
	select {
	case <-closeDone:
		t.Fatal("shutdown closed dirty admission before the admitted refresh callback returned")
	default:
	}
	close(allowNewNode)
	if ok := <-refreshDone; !ok {
		t.Fatal("refresh was not admitted")
	}
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not wait for the admitted refresh")
	}

	if !staticAccepted.Load() {
		t.Fatal("new node static mark was rejected after the refresh admission was granted")
	}
	if !subscriptionAccepted.Load() {
		t.Fatal("new subscription-node mark was rejected after the refresh admission was granted")
	}
	if !finalAccepted.Load() {
		t.Fatal("final node-removal marks were rejected after the refresh admission was granted")
	}
	if calls := legacyCallbackCalls.Load(); calls != 0 {
		t.Fatalf("legacy pool callbacks were invoked alongside writer-aware callbacks: %d", calls)
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final FlushDirtySets: %v", err)
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("dirty marks remain after final refresh flush: %d", dirty)
	}
	staticRows, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	for _, row := range staticRows {
		if row.Hash == oldHash.Hex() {
			t.Fatal("old static node row survived completed refresh")
		}
	}
	foundNewStatic := false
	for _, row := range staticRows {
		if row.Hash == newHash.Hex() {
			foundNewStatic = true
		}
	}
	if !foundNewStatic {
		t.Fatal("new static node row was not persisted")
	}
	dynamicRows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	for _, row := range dynamicRows {
		if row.Hash == oldHash.Hex() {
			t.Fatal("old dynamic node row survived completed refresh")
		}
	}
	latencyRows, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	for _, row := range latencyRows {
		if row.NodeHash == oldHash.Hex() {
			t.Fatal("old node-latency row survived completed refresh")
		}
	}
	subRows, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	foundNewSub := false
	for _, row := range subRows {
		if row.NodeHash == oldHash.Hex() {
			t.Fatal("old subscription-node row survived completed refresh")
		}
		if row.NodeHash == newHash.Hex() {
			foundNewSub = true
		}
	}
	if !foundNewSub {
		t.Fatal("new subscription-node row was not persisted")
	}
}
