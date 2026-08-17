package main

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

// TestShutdownCacheFlushWaitsForAdmittedDeleteSubscription locks the
// state-owner -> dirty-admission -> cache-flush ordering used by the real
// DeleteSubscription callback. A delete that has already passed state-write
// admission must publish its cache delete before the final flush starts.
func TestShutdownCacheFlushWaitsForAdmittedDeleteSubscription(t *testing.T) {
	root := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(root, "state"),
		filepath.Join(root, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-delete-order",
		"shutdown-delete-order",
		"https://example.com/sub",
		true,
		true,
	)
	sub.SetFetchConfig(sub.URL(), int64(time.Minute))
	subMgr.Register(sub)
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               sub.ID,
		Name:             sub.Name(),
		SourceType:       sub.SourceType(),
		URL:              sub.URL(),
		Enabled:          true,
		Ephemeral:        true,
		UpdateIntervalNs: int64(time.Minute),
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	raw := []byte(`{"type":"shutdown-delete-order"}`)
	hash := node.HashFromRawOptions(raw)
	removeEntered := make(chan struct{})
	allowRemove := make(chan struct{})
	var removeOnce sync.Once
	var deleteMarkAccepted atomic.Bool
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if added {
				if !engine.MarkSubscriptionNode(subID, changedHash.Hex()) {
					t.Errorf("initial subscription-node mark was rejected")
				}
				return
			}
			removeOnce.Do(func() { close(removeEntered) })
			<-allowRemove
			deleteMarkAccepted.Store(engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex()))
		},
	})
	p.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"shutdown"}})

	var flushForShutdown atomic.Bool
	finalFlushEntered := make(chan struct{})
	var finalFlushOnce sync.Once
	const sentinel = "shutdown-delete-order-sentinel"
	readers := state.CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			if hash == sentinel && flushForShutdown.Load() {
				finalFlushOnce.Do(func() { close(finalFlushEntered) })
			}
			return nil
		},
		ReadNodeDynamic: func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency: func(state.NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:       func(state.LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(key state.SubscriptionNodeDirtyKey) *model.SubscriptionNode {
			current := subMgr.Lookup(key.SubscriptionID)
			if current == nil {
				return nil
			}
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			managed, ok := current.ManagedNodes().LoadNode(h)
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
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("initial FlushDirtySets: %v", err)
	}

	if _, err := engine.LoadAllSubscriptionNodes(); err != nil {
		t.Fatalf("verify initial subscription nodes: %v", err)
	}

	serviceCP := &service.ControlPlaneService{
		Engine: engine,
		Pool:   p,
		SubMgr: subMgr,
	}
	deleteResult := make(chan error, 1)
	h := newBlockingEndpointHarness(t, func() {
		deleteResult <- serviceCP.DeleteSubscription(sub.ID)
	})
	h.release()
	select {
	case <-removeEntered:
	case <-time.After(time.Second):
		t.Fatal("DeleteSubscription did not reach the production pool removal callback")
	}

	// The cache flush has one unrelated dirty key so the final reader gives us
	// an exact barrier. It must not be reached while the admitted delete still
	// owns the state mutation.
	flushForShutdown.Store(true)
	if !engine.MarkNodeStatic(sentinel) {
		t.Fatal("sentinel dirty mark was rejected before shutdown")
	}
	app := &resinApp{
		stateEngine:     engine,
		flushWorker:     state.NewCacheFlushWorker(engine, readers, func() int { return 1 << 20 }, func() time.Duration { return time.Hour }, time.Hour),
		endpointManager: h.manager,
		topoRuntime: &topologyRuntime{
			outboundMgr: outbound.NewOutboundManager(p, &testutil.StubOutboundBuilder{}),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("app.shutdown did not return after its bounded caller deadline")
	}
	select {
	case <-finalFlushEntered:
		t.Fatal("final cache flush started while admitted DeleteSubscription was still active")
	default:
	}

	close(allowRemove)
	h.waitHandler(t)
	select {
	case err := <-deleteResult:
		if err != nil {
			t.Fatalf("DeleteSubscription: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DeleteSubscription did not finish after releasing its callback")
	}
	if !deleteMarkAccepted.Load() {
		t.Fatal("delete dirty mark was rejected after the admitted state mutation")
	}

	select {
	case <-finalFlushEntered:
	case <-time.After(time.Second):
		t.Fatal("final cache flush did not start after DeleteSubscription completed")
	}
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}

	rows, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes after shutdown: %v", err)
	}
	for _, row := range rows {
		if row.SubscriptionID == sub.ID && row.NodeHash == hash.Hex() {
			t.Fatalf("deleted subscription-node row survived final flush: %+v", row)
		}
	}
}
