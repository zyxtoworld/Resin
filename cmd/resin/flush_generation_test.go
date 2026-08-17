package main

import (
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

func TestFlushReadersDoNotMixDeletedAndRecreatedNodeGeneration(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "flush-generation-sub"
	raw := []byte(`{"type":"flush-generation","server":"198.51.100.210","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	subManager := topology.NewSubscriptionManager()
	subManager.Register(subscription.NewSubscription(subID, "FlushGeneration", "https://example.com/sub", true, false))

	persistRemoved := func(_ string, h node.Hash, entry *node.NodeEntry) {
		markNodeRemovedDirty(engine, h, entry)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 8,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeAdded: func(h node.Hash) {
			if !engine.MarkNodeStatic(h.Hex()) {
				t.Errorf("initial/new static dirty mark rejected for %s", h.Hex())
			}
		},
		OnFinalNodeRemoved: persistRemoved,
	})

	pool.AddNodeFromSub(hash, raw, subID)
	entryA, ok := pool.GetEntry(hash)
	if !ok || entryA == nil {
		t.Fatal("initial node entry missing")
	}
	ipA := netip.MustParseAddr("198.51.100.211")
	pool.UpdateNodeEgressIPForEntry(hash, entryA, &ipA, nil)
	if !engine.MarkNodeDynamic(hash.Hex()) {
		t.Fatal("initial dynamic dirty mark rejected")
	}
	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		t.Fatalf("initial FlushDirtySets: %v", err)
	}

	if !engine.MarkNodeStatic(hash.Hex()) || !engine.MarkNodeDynamic(hash.Hex()) {
		t.Fatal("generation test dirty marks rejected")
	}

	dynamicReadEntered := make(chan struct{})
	allowDynamicRead := make(chan struct{})
	beforeFlushNodeDynamicReadHook = func(readHash string) {
		if readHash != hash.Hex() {
			return
		}
		close(dynamicReadEntered)
		<-allowDynamicRead
	}
	t.Cleanup(func() {
		beforeFlushNodeDynamicReadHook = nil
		select {
		case <-allowDynamicRead:
		default:
			close(allowDynamicRead)
		}
	})

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- engine.FlushDirtySets(newFlushReaders(pool, subManager, nil))
	}()
	select {
	case <-dynamicReadEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not reach dynamic reader gate")
	}

	mutationEntered := make(chan struct{})
	mutationDone := make(chan struct{})
	go func() {
		pool.WithRuntimeMutation(func() {
			close(mutationEntered)
			pool.RemoveNodeFromSub(hash, subID)
			pool.AddNodeFromSub(hash, raw, subID)
		})
		close(mutationDone)
	}()

	// The flush reader has not released its generation yet. The old reader
	// implementation has no runtime read owner, so the mutation enters here;
	// the fixed implementation must keep it out until the whole read batch is
	// released. This is a bounded deadlock check, not a scheduling sleep.
	select {
	case <-mutationEntered:
		t.Fatal("runtime mutation entered while one flush generation was being read")
	case <-time.After(250 * time.Millisecond):
	}

	close(allowDynamicRead)
	if err := <-flushDone; err != nil {
		t.Fatalf("generation FlushDirtySets: %v", err)
	}
	select {
	case <-mutationDone:
	case <-time.After(2 * time.Second):
		t.Fatal("delete/recreate mutation did not finish after flush read release")
	}

	staticRows, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	dynamicRows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(staticRows) != 1 || len(dynamicRows) != 1 {
		t.Fatalf("generation rows = static=%+v dynamic=%+v, want one row each", staticRows, dynamicRows)
	}
	if staticRows[0].Hash != hash.Hex() || dynamicRows[0].Hash != hash.Hex() {
		t.Fatalf("generation rows have wrong hash: static=%+v dynamic=%+v", staticRows, dynamicRows)
	}
	if staticRows[0].CreatedAtNs != entryA.CreatedAt.UnixNano() {
		t.Fatalf("static generation changed unexpectedly: got %d, want original %d", staticRows[0].CreatedAtNs, entryA.CreatedAt.UnixNano())
	}
	if dynamicRows[0].EgressIP != ipA.String() {
		t.Fatalf("dynamic generation changed unexpectedly: got %q, want original %q", dynamicRows[0].EgressIP, ipA.String())
	}
}
