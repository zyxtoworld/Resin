package service

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
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestCleanupSubscriptionCircuitOpenNodesContext_HonorsCancellationWhileRuntimeMutationIsBlocked(t *testing.T) {
	cp, subMgr, pool := newCleanupSubscriptionTestService()
	sub := subscription.NewSubscription("sub-runtime-cancel", "sub-runtime-cancel", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"ss","server":"8.8.8.8","port":443}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"blocked"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("missing cleanup candidate")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())

	runtimeHeld := make(chan struct{})
	allowRuntime := make(chan struct{})
	go pool.WithRuntimeMutation(func() {
		close(runtimeHeld)
		<-allowRuntime
	})
	<-runtimeHeld

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstScanDone := make(chan struct{})
	allowScan := make(chan struct{})
	type result struct {
		count int
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		count, err := cp.cleanupSubscriptionCircuitOpenNodesContextWithHook(ctx, sub.ID, func() {
			close(firstScanDone)
			<-allowScan
		})
		resultCh <- result{count: count, err: err}
	}()

	<-firstScanDone
	cancel()
	close(allowScan)

	select {
	case got := <-resultCh:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cleanup error = %v, want context.Canceled", got.err)
		}
		if got.count != 0 {
			t.Fatalf("cleanup count = %d after cancellation, want 0", got.count)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceled cleanup remained blocked on runtime mutation owner")
	}

	close(allowRuntime)
}

func newCleanupSubscriptionTestService() (*ControlPlaneService, *topology.SubscriptionManager, *topology.GlobalNodePool) {
	subMgr := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
	}
	return cp, subMgr, pool
}

func TestCleanupSubscriptionCircuitOpenNodes_RemovesCircuitAndOutboundFailureNodes(t *testing.T) {
	cp, subMgr, pool := newCleanupSubscriptionTestService()

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	circuitRaw := []byte(`{"type":"ss","server":"1.1.1.1","port":443}`)
	circuitHash := node.HashFromRawOptions(circuitRaw)
	pool.AddNodeFromSub(circuitHash, circuitRaw, subA.ID)
	subA.ManagedNodes().StoreNode(circuitHash, subscription.ManagedNode{Tags: []string{"circuit"}})
	circuitEntry, ok := pool.GetEntry(circuitHash)
	if !ok {
		t.Fatalf("missing circuit node %s in pool", circuitHash.Hex())
	}
	circuitEntry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())

	noOutboundErrorRaw := []byte(`{"type":"ss","server":"2.2.2.2","port":443}`)
	noOutboundErrorHash := node.HashFromRawOptions(noOutboundErrorRaw)
	pool.AddNodeFromSub(noOutboundErrorHash, noOutboundErrorRaw, subA.ID)
	subA.ManagedNodes().StoreNode(noOutboundErrorHash, subscription.ManagedNode{Tags: []string{"failed"}})
	noOutboundErrorEntry, ok := pool.GetEntry(noOutboundErrorHash)
	if !ok {
		t.Fatalf("missing outbound failure node %s in pool", noOutboundErrorHash.Hex())
	}
	noOutboundErrorEntry.SetLastError("outbound build failed")

	healthyRaw := []byte(`{"type":"ss","server":"3.3.3.3","port":443}`)
	healthyHash := node.HashFromRawOptions(healthyRaw)
	pool.AddNodeFromSub(healthyHash, healthyRaw, subA.ID)
	subA.ManagedNodes().StoreNode(healthyHash, subscription.ManagedNode{Tags: []string{"healthy"}})
	healthyEntry, ok := pool.GetEntry(healthyHash)
	if !ok {
		t.Fatalf("missing healthy node %s in pool", healthyHash.Hex())
	}
	outbound := testutil.NewNoopOutbound()
	healthyEntry.Outbound.Store(&outbound)
	healthyEntry.CircuitOpenSince.Store(0)

	sharedRaw := []byte(`{"type":"ss","server":"4.4.4.4","port":443}`)
	sharedHash := node.HashFromRawOptions(sharedRaw)
	pool.AddNodeFromSub(sharedHash, sharedRaw, subA.ID)
	pool.AddNodeFromSub(sharedHash, sharedRaw, subB.ID)
	subA.ManagedNodes().StoreNode(sharedHash, subscription.ManagedNode{Tags: []string{"shared-a"}})
	subB.ManagedNodes().StoreNode(sharedHash, subscription.ManagedNode{Tags: []string{"shared-b"}})
	sharedEntry, ok := pool.GetEntry(sharedHash)
	if !ok {
		t.Fatalf("missing shared node %s in pool", sharedHash.Hex())
	}
	sharedEntry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())

	cleanedCount, err := cp.CleanupSubscriptionCircuitOpenNodes(subA.ID)
	if err != nil {
		t.Fatalf("CleanupSubscriptionCircuitOpenNodes: %v", err)
	}
	if cleanedCount != 3 {
		t.Fatalf("cleaned_count = %d, want %d", cleanedCount, 3)
	}

	circuitManaged, ok := subA.ManagedNodes().LoadNode(circuitHash)
	if !ok || !circuitManaged.Evicted {
		t.Fatal("circuit node should remain in subA managed nodes and be marked evicted")
	}
	failedManaged, ok := subA.ManagedNodes().LoadNode(noOutboundErrorHash)
	if !ok || !failedManaged.Evicted {
		t.Fatal("no-outbound-error node should remain in subA managed nodes and be marked evicted")
	}
	sharedManaged, ok := subA.ManagedNodes().LoadNode(sharedHash)
	if !ok || !sharedManaged.Evicted {
		t.Fatal("shared node should remain in subA managed nodes and be marked evicted")
	}
	healthyManaged, ok := subA.ManagedNodes().LoadNode(healthyHash)
	if !ok {
		t.Fatal("healthy node should remain in subA managed nodes")
	}
	if healthyManaged.Evicted {
		t.Fatal("healthy node should not be marked evicted")
	}

	if _, ok := pool.GetEntry(circuitHash); ok {
		t.Fatal("circuit node should be removed from pool after subA cleanup")
	}
	if _, ok := pool.GetEntry(noOutboundErrorHash); ok {
		t.Fatal("no-outbound-error node should be removed from pool after subA cleanup")
	}

	sharedEntry, ok = pool.GetEntry(sharedHash)
	if !ok {
		t.Fatal("shared node should remain in pool because subB still references it")
	}
	sharedRefs := sharedEntry.SubscriptionIDs()
	if len(sharedRefs) != 1 || sharedRefs[0] != subB.ID {
		t.Fatalf("shared node refs = %v, want [%s]", sharedRefs, subB.ID)
	}
	if _, ok := subB.ManagedNodes().LoadNode(sharedHash); !ok {
		t.Fatal("shared node should remain in subB managed nodes")
	}

	cleanedCount, err = cp.CleanupSubscriptionCircuitOpenNodes(subA.ID)
	if err != nil {
		t.Fatalf("second CleanupSubscriptionCircuitOpenNodes: %v", err)
	}
	if cleanedCount != 0 {
		t.Fatalf("second cleaned_count = %d, want 0", cleanedCount)
	}
}

func TestCleanupSubscriptionCircuitOpenNodes_SecondConfirmSkipsRecoveredNodes(t *testing.T) {
	cp, subMgr, pool := newCleanupSubscriptionTestService()

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	recoveredRaw := []byte(`{"type":"ss","server":"5.5.5.5","port":443}`)
	recoveredHash := node.HashFromRawOptions(recoveredRaw)
	pool.AddNodeFromSub(recoveredHash, recoveredRaw, sub.ID)
	sub.ManagedNodes().StoreNode(recoveredHash, subscription.ManagedNode{Tags: []string{"recovering"}})
	recoveredEntry, ok := pool.GetEntry(recoveredHash)
	if !ok {
		t.Fatalf("missing recovering node %s in pool", recoveredHash.Hex())
	}
	recoveredEntry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())

	failedRaw := []byte(`{"type":"ss","server":"6.6.6.6","port":443}`)
	failedHash := node.HashFromRawOptions(failedRaw)
	pool.AddNodeFromSub(failedHash, failedRaw, sub.ID)
	sub.ManagedNodes().StoreNode(failedHash, subscription.ManagedNode{Tags: []string{"failed"}})
	failedEntry, ok := pool.GetEntry(failedHash)
	if !ok {
		t.Fatalf("missing failed node %s in pool", failedHash.Hex())
	}
	failedEntry.SetLastError("outbound build failed")

	hookCalled := false
	cleanedCount, err := cp.cleanupSubscriptionCircuitOpenNodesWithHook(sub.ID, func() {
		hookCalled = true
		// Simulate node recovery in TOCTOU window.
		recoveredEntry.CircuitOpenSince.Store(0)
	})
	if err != nil {
		t.Fatalf("cleanupSubscriptionCircuitOpenNodesWithHook: %v", err)
	}
	if !hookCalled {
		t.Fatal("betweenScans hook was not called")
	}
	if cleanedCount != 1 {
		t.Fatalf("cleaned_count = %d, want 1", cleanedCount)
	}

	if _, ok := sub.ManagedNodes().LoadNode(recoveredHash); !ok {
		t.Fatal("recovered node should remain in managed nodes after second confirmation")
	}
	if _, ok := pool.GetEntry(recoveredHash); !ok {
		t.Fatal("recovered node should remain in pool after second confirmation")
	}
	failedManaged, ok := sub.ManagedNodes().LoadNode(failedHash)
	if !ok {
		t.Fatal("failed node should remain in managed nodes")
	}
	if !failedManaged.Evicted {
		t.Fatal("failed node should be marked evicted")
	}
	if _, ok := pool.GetEntry(failedHash); ok {
		t.Fatal("failed node should be removed from pool")
	}
}

func TestCleanupSubscriptionCircuitOpenNodes_DoesNotEvictRecreatedSameHashEntry(t *testing.T) {
	cp, subMgr, pool := newCleanupSubscriptionTestService()

	sub := subscription.NewSubscription("sub-same-hash-generation", "sub-same-hash-generation", "https://example.com", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"shadowsocks","server":"7.7.7.7","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"generation-a"}})
	entryA, ok := pool.GetEntry(hash)
	if !ok || entryA == nil {
		t.Fatal("missing generation A entry")
	}
	entryA.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())

	cleanedCount, err := cp.cleanupSubscriptionCircuitOpenNodesWithHook(sub.ID, func() {
		pool.RemoveNodeFromSub(hash, sub.ID)
		pool.AddNodeFromSub(hash, raw, sub.ID)
	})
	if err != nil {
		t.Fatalf("cleanupSubscriptionCircuitOpenNodesWithHook: %v", err)
	}
	if cleanedCount != 0 {
		t.Fatalf("cleaned_count = %d, want 0 for recreated generation", cleanedCount)
	}

	entryB, ok := pool.GetEntry(hash)
	if !ok || entryB == nil {
		t.Fatal("recreated generation B entry was removed")
	}
	if entryB == entryA {
		t.Fatal("same-hash replacement did not create a new entry generation")
	}
	managed, ok := sub.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("managed node disappeared during same-hash replacement")
	}
	if managed.Evicted {
		t.Fatal("cleanup evicted recreated generation B using generation A candidate")
	}
}

func TestCleanupSubscriptionCircuitOpenNodes_SubscriptionNotFound(t *testing.T) {
	cp, _, _ := newCleanupSubscriptionTestService()

	_, err := cp.CleanupSubscriptionCircuitOpenNodes("missing-sub")
	if err == nil {
		t.Fatal("expected not found error")
	}
	var svcErr *ServiceError
	if !errors.As(err, &svcErr) {
		t.Fatalf("error type = %T, want *ServiceError", err)
	}
	if svcErr.Code != "NOT_FOUND" {
		t.Fatalf("error code = %q, want NOT_FOUND", svcErr.Code)
	}
}

type cleanupPersistenceFixture struct {
	engine  *state.StateEngine
	subMgr  *topology.SubscriptionManager
	sub     *subscription.Subscription
	pool    *topology.GlobalNodePool
	hash    node.Hash
	readers state.CacheReaders
}

func newCleanupPersistenceFixture(t *testing.T, afterRemove func()) *cleanupPersistenceFixture {
	return newCleanupPersistenceFixtureWithFinal(t, afterRemove, nil, nil)
}

func newCleanupPersistenceFixtureWithFinal(
	t *testing.T,
	afterRemove func(),
	onFinalNodeRemoved func(*state.StateEngine, string, node.Hash, *node.NodeEntry),
	onFinalNodeRemovedWithPersistence func(string, node.Hash, *node.NodeEntry, topology.PersistenceAdmission),
) *cleanupPersistenceFixture {
	t.Helper()
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"sub-cleanup-persistence-order",
		"cleanup-persistence-order",
		"https://example.com",
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
		UpdateIntervalNs: sub.UpdateIntervalNs(),
		Enabled:          true,
		Ephemeral:        true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	raw := []byte(`{"type":"cleanup-persistence-order"}`)
	hash := node.HashFromRawOptions(raw)
	var finalNodeRemoved func(string, node.Hash, *node.NodeEntry)
	if onFinalNodeRemoved != nil {
		finalNodeRemoved = func(subID string, changedHash node.Hash, removed *node.NodeEntry) {
			onFinalNodeRemoved(engine, subID, changedHash, removed)
		}
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if added {
				engine.MarkSubscriptionNode(subID, changedHash.Hex())
			} else {
				engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex())
				if afterRemove != nil {
					afterRemove()
				}
			}
		},
		OnFinalNodeRemoved:                finalNodeRemoved,
		OnFinalNodeRemovedWithPersistence: onFinalNodeRemovedWithPersistence,
	})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})

	readers := state.CacheReaders{
		ReadSubscriptionNode: func(key state.SubscriptionNodeDirtyKey) *model.SubscriptionNode {
			managedSub := subMgr.Lookup(key.SubscriptionID)
			if managedSub == nil {
				return nil
			}
			parsedHash, parseErr := node.ParseHex(key.NodeHash)
			if parseErr != nil {
				return nil
			}
			managed, ok := managedSub.ManagedNodes().LoadNode(parsedHash)
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
		t.Fatalf("flush initial subscription node: %v", err)
	}
	if engine.DirtyCount() != 0 {
		t.Fatalf("dirty entries remain after initial flush: %d", engine.DirtyCount())
	}

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("cleanup-persistence-order entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	return &cleanupPersistenceFixture{
		engine:  engine,
		subMgr:  subMgr,
		sub:     sub,
		pool:    pool,
		hash:    hash,
		readers: readers,
	}
}

// TestCleanupSubscriptionCircuitOpenNodes_DeleteWinsLatePersistenceMark uses
// the real cache dirty-set and the production-shaped pool callbacks. The
// delete callback is held after MarkSubscriptionNodeDelete and before the
// subscription is unregistered, so a late cleanup upsert is observable by a
// real FlushDirtySets call.
func TestCleanupSubscriptionCircuitOpenNodes_DeleteWinsLatePersistenceMark(t *testing.T) {
	var removeCallbacks atomic.Int32
	deleteMarkReached := make(chan struct{})
	allowDeleteReturn := make(chan struct{})
	var releaseDeleteOnce sync.Once
	releaseDelete := func() { releaseDeleteOnce.Do(func() { close(allowDeleteReturn) }) }
	defer releaseDelete()

	fixture := newCleanupPersistenceFixture(t, func() {
		if removeCallbacks.Add(1) == 2 {
			close(deleteMarkReached)
			<-allowDeleteReturn
		}
	})
	engine := fixture.engine
	subMgr := fixture.subMgr
	sub := fixture.sub
	pool := fixture.pool
	hash := fixture.hash
	readers := fixture.readers

	service := &ControlPlaneService{Engine: engine, Pool: pool, SubMgr: subMgr}
	cleanupBoundaryReached := make(chan struct{})
	allowCleanupReturn := make(chan struct{})
	var releaseCleanupOnce sync.Once
	releaseCleanup := func() { releaseCleanupOnce.Do(func() { close(allowCleanupReturn) }) }
	defer releaseCleanup()
	service.afterSubscriptionCleanupMutationHook = func() {
		close(cleanupBoundaryReached)
		<-allowCleanupReturn
	}

	type cleanupResult struct {
		count int
		err   error
	}
	cleanupDone := make(chan cleanupResult, 1)
	go func() {
		count, cleanupErr := service.CleanupSubscriptionCircuitOpenNodes(sub.ID)
		cleanupDone <- cleanupResult{count: count, err: cleanupErr}
	}()
	select {
	case <-cleanupBoundaryReached:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not reach its post-mutation boundary")
	}

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- service.DeleteSubscription(sub.ID) }()
	select {
	case <-deleteMarkReached:
	case <-time.After(time.Second):
		t.Fatal("delete path did not reach its delete mark")
	}
	releaseCleanup()

	select {
	case result := <-cleanupDone:
		if result.err != nil || result.count != 1 {
			t.Fatalf("cleanup result = (%d, %v), want (1, nil)", result.count, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after releasing its boundary")
	}

	// DeleteSubscription is still paused inside the production pool callback.
	// A stale upsert that followed the delete mark would recreate this row.
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush delete result: %v", err)
	}
	if engine.DirtyCount() != 0 {
		t.Fatalf("dirty entries remain after delete flush: %d", engine.DirtyCount())
	}
	rows, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	for _, row := range rows {
		if row.SubscriptionID == sub.ID && row.NodeHash == hash.Hex() {
			t.Fatal("subscription-node row survived delete-priority flush")
		}
	}

	releaseDelete()
	select {
	case deleteErr := <-deleteDone:
		if deleteErr != nil {
			t.Fatalf("DeleteSubscription: %v", deleteErr)
		}
	case <-time.After(time.Second):
		t.Fatal("DeleteSubscription did not finish")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if engine.DirtyCount() != 0 {
		t.Fatalf("dirty entries remain after final flush: %d", engine.DirtyCount())
	}
	rows, err = engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes after final flush: %v", err)
	}
	for _, row := range rows {
		if row.SubscriptionID == sub.ID && row.NodeHash == hash.Hex() {
			t.Fatal("deleted subscription-node row survived final flush")
		}
	}
}

// TestCleanupSubscriptionCircuitOpenNodes_PersistsEvictedNode locks the
// normal cleanup contract: the managed hash is retained and cache.db records
// Evicted=true until a later DeleteSubscription removes the subscription.
func TestCleanupSubscriptionCircuitOpenNodes_PersistsEvictedNode(t *testing.T) {
	fixture := newCleanupPersistenceFixture(t, nil)
	service := &ControlPlaneService{
		Engine: fixture.engine,
		Pool:   fixture.pool,
		SubMgr: fixture.subMgr,
	}

	count, err := service.CleanupSubscriptionCircuitOpenNodes(fixture.sub.ID)
	if err != nil || count != 1 {
		t.Fatalf("cleanup result = (%d, %v), want (1, nil)", count, err)
	}
	if err := fixture.engine.FlushDirtySets(fixture.readers); err != nil {
		t.Fatalf("flush evicted node: %v", err)
	}
	if fixture.engine.DirtyCount() != 0 {
		t.Fatalf("dirty entries remain after cleanup flush: %d", fixture.engine.DirtyCount())
	}

	rows, err := fixture.engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	for _, row := range rows {
		if row.SubscriptionID == fixture.sub.ID && row.NodeHash == fixture.hash.Hex() {
			if !row.Evicted {
				t.Fatal("cleanup persisted subscription-node row without Evicted=true")
			}
			return
		}
	}
	t.Fatal("cleanup did not retain the evicted subscription-node row")
}

func TestCleanupSubscriptionCircuitOpenNodes_RejectsClosedDirtyAdmissionBeforeMutation(t *testing.T) {
	fixture := newCleanupPersistenceFixture(t, nil)
	service := &ControlPlaneService{
		Engine: fixture.engine,
		Pool:   fixture.pool,
		SubMgr: fixture.subMgr,
	}

	fixture.engine.CloseDirtyWriteAdmission()
	if _, err := service.CleanupSubscriptionCircuitOpenNodes(fixture.sub.ID); err == nil {
		t.Fatal("cleanup unexpectedly succeeded after dirty-write admission closed")
	}

	managed, ok := fixture.sub.ManagedNodes().LoadNode(fixture.hash)
	if !ok {
		t.Fatal("managed node disappeared after rejected cleanup")
	}
	if managed.Evicted {
		t.Fatal("rejected cleanup mutated managed node")
	}
	if _, ok := fixture.pool.GetEntry(fixture.hash); !ok {
		t.Fatal("rejected cleanup removed node from pool")
	}
}

func TestCleanupSubscriptionCircuitOpenNodes_DirtyAdmissionCoversCompensation(t *testing.T) {
	removeEntered := make(chan struct{})
	allowRemove := make(chan struct{})
	var removeOnce sync.Once
	fixture := newCleanupPersistenceFixture(t, func() {
		removeOnce.Do(func() {
			close(removeEntered)
			<-allowRemove
		})
	})
	service := &ControlPlaneService{
		Engine: fixture.engine,
		Pool:   fixture.pool,
		SubMgr: fixture.subMgr,
	}

	cleanupDone := make(chan error, 1)
	go func() {
		_, err := service.CleanupSubscriptionCircuitOpenNodes(fixture.sub.ID)
		cleanupDone <- err
	}()
	select {
	case <-removeEntered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not reach the production pool delete callback")
	}

	fixture.engine.CloseDirtyWriteAdmission()
	stopDone := make(chan struct{})
	go func() {
		fixture.engine.CloseDirtyWriteAdmissionAndWait()
		close(stopDone)
	}()

	close(allowRemove)
	select {
	case err := <-cleanupDone:
		if err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after releasing the callback")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("dirty admission close did not finish after cleanup")
	}

	if err := fixture.engine.FlushDirtySets(fixture.readers); err != nil {
		t.Fatalf("flush compensated cleanup: %v", err)
	}
	rows, err := fixture.engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	for _, row := range rows {
		if row.SubscriptionID == fixture.sub.ID && row.NodeHash == fixture.hash.Hex() {
			if !row.Evicted {
				t.Fatal("cleanup compensation persisted without Evicted=true")
			}
			return
		}
	}
	t.Fatal("cleanup compensation was lost after dirty admission closed")
}

func TestCleanupSubscriptionCircuitOpenNodes_FinalRemovalSharesOuterDirtyAdmission(t *testing.T) {
	finalEntered := make(chan struct{})
	allowFinal := make(chan struct{})
	var finalOnce sync.Once
	legacyFinal := func(engine *state.StateEngine, subID string, hash node.Hash, entry *node.NodeEntry) {
		finalOnce.Do(func() { close(finalEntered) })
		<-allowFinal
		engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
			admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
			admission.MarkNodeStaticDelete(hash.Hex())
			admission.MarkNodeDynamicDelete(hash.Hex())
			if entry == nil || entry.LatencyTable == nil {
				return
			}
			entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
				admission.MarkNodeLatencyDelete(hash.Hex(), domain)
				return true
			})
		})
	}
	persistentFinal := func(subID string, hash node.Hash, entry *node.NodeEntry, admission topology.PersistenceAdmission) {
		finalOnce.Do(func() { close(finalEntered) })
		<-allowFinal
		admission.MarkSubscriptionNodeDelete(subID, hash.Hex())
		admission.MarkNodeStaticDelete(hash.Hex())
		admission.MarkNodeDynamicDelete(hash.Hex())
		if entry == nil || entry.LatencyTable == nil {
			return
		}
		entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
			admission.MarkNodeLatencyDelete(hash.Hex(), domain)
			return true
		})
	}
	fixture := newCleanupPersistenceFixtureWithFinal(t, nil, legacyFinal, persistentFinal)
	t.Cleanup(func() {
		select {
		case <-allowFinal:
		default:
			close(allowFinal)
		}
	})

	entry, ok := fixture.pool.GetEntry(fixture.hash)
	if !ok || entry.LatencyTable == nil {
		t.Fatal("cleanup entry or latency table missing")
	}
	entry.LatencyTable.LoadEntry("cleanup.example.org", node.DomainLatencyStats{
		Ewma:        25 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	hashHex := fixture.hash.Hex()
	if err := fixture.engine.CacheRepo.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  append([]byte(nil), entry.RawOptions...),
		CreatedAtNs: entry.CreatedAt.UnixNano(),
	}}); err != nil {
		t.Fatalf("seed static node: %v", err)
	}
	if err := fixture.engine.CacheRepo.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:             hashHex,
		FailureCount:     1,
		CircuitOpenSince: entry.CircuitOpenSince.Load(),
	}}); err != nil {
		t.Fatalf("seed dynamic node: %v", err)
	}
	if err := fixture.engine.CacheRepo.BulkUpsertNodeLatency([]model.NodeLatency{{
		NodeHash:      hashHex,
		Domain:        "cleanup.example.org",
		EwmaNs:        int64(25 * time.Millisecond),
		LastUpdatedNs: time.Now().UnixNano(),
	}}); err != nil {
		t.Fatalf("seed node latency: %v", err)
	}

	readers := fixture.readers
	readers.ReadNodeStatic = func(hash string) *model.NodeStatic {
		h, err := node.ParseHex(hash)
		if err != nil {
			return nil
		}
		e, ok := fixture.pool.GetEntry(h)
		if !ok {
			return nil
		}
		return &model.NodeStatic{
			Hash:        hash,
			RawOptions:  append([]byte(nil), e.RawOptions...),
			CreatedAtNs: e.CreatedAt.UnixNano(),
		}
	}
	readers.ReadNodeDynamic = func(hash string) *model.NodeDynamic {
		h, err := node.ParseHex(hash)
		if err != nil {
			return nil
		}
		e, ok := fixture.pool.GetEntry(h)
		if !ok {
			return nil
		}
		return &model.NodeDynamic{
			Hash:             hash,
			FailureCount:     int(e.FailureCount.Load()),
			CircuitOpenSince: e.CircuitOpenSince.Load(),
		}
	}
	readers.ReadNodeLatency = func(key state.NodeLatencyDirtyKey) *model.NodeLatency {
		h, err := node.ParseHex(key.NodeHash)
		if err != nil {
			return nil
		}
		e, ok := fixture.pool.GetEntry(h)
		if !ok || e.LatencyTable == nil {
			return nil
		}
		stats, ok := e.LatencyTable.GetDomainStats(key.Domain)
		if !ok {
			return nil
		}
		return &model.NodeLatency{
			NodeHash:      key.NodeHash,
			Domain:        key.Domain,
			EwmaNs:        int64(stats.Ewma),
			LastUpdatedNs: stats.LastUpdated.UnixNano(),
		}
	}

	service := &ControlPlaneService{
		Engine: fixture.engine,
		Pool:   fixture.pool,
		SubMgr: fixture.subMgr,
	}
	cleanupDone := make(chan error, 1)
	go func() {
		_, err := service.CleanupSubscriptionCircuitOpenNodes(fixture.sub.ID)
		cleanupDone <- err
	}()
	select {
	case <-finalEntered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not reach final-node removal callback")
	}

	fixture.engine.CloseDirtyWriteAdmission()
	closeDone := make(chan struct{})
	go func() {
		fixture.engine.CloseDirtyWriteAdmissionAndWait()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("dirty admission close passed the admitted cleanup before callback release")
	case <-time.After(100 * time.Millisecond):
	}
	close(allowFinal)

	select {
	case err := <-cleanupDone:
		if err != nil {
			t.Fatalf("CleanupSubscriptionCircuitOpenNodes: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after final callback release")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("dirty admission close did not finish after cleanup")
	}

	if err := fixture.engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush final removal: %v", err)
	}
	staticNodes, err := fixture.engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	dynamicNodes, err := fixture.engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	latencyNodes, err := fixture.engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	if len(staticNodes) != 0 || len(dynamicNodes) != 0 || len(latencyNodes) != 0 {
		t.Fatalf("final node removal lost delete marks after nested admission rejection: static=%+v dynamic=%+v latency=%+v", staticNodes, dynamicNodes, latencyNodes)
	}
}

func TestCleanupSubscriptionCircuitOpenNodes_RejectsSameIDReplacementABA(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	oldSub := subscription.NewSubscription("sub-aba", "old", "https://example.com/old", true, true)
	newSub := subscription.NewSubscription("sub-aba", "new", "https://example.com/new", true, true)
	subMgr.Register(oldSub)

	raw := []byte(`{"type":"cleanup-aba"}`)
	hash := node.HashFromRawOptions(raw)
	var dirtyMarks atomic.Int32
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChanged: func(_ string, _ node.Hash, added bool) {
			if !added {
				dirtyMarks.Add(1)
			}
		},
	})
	pool.AddNodeFromSub(hash, raw, oldSub.ID)
	newSub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"new"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("cleanup-aba entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	service := &ControlPlaneService{Pool: pool, SubMgr: subMgr}
	lookupReached := make(chan struct{})
	allowCleanupLookup := make(chan struct{})
	var hookMismatch atomic.Bool
	var releaseCleanupLookupOnce sync.Once
	releaseCleanupLookup := func() {
		releaseCleanupLookupOnce.Do(func() { close(allowCleanupLookup) })
	}
	defer releaseCleanupLookup()
	service.beforeSubscriptionCleanupLockHook = func(id string, got *subscription.Subscription) {
		if id != oldSub.ID || got != oldSub {
			hookMismatch.Store(true)
		}
		close(lookupReached)
		<-allowCleanupLookup
	}

	oldLockHeld := make(chan struct{})
	allowOldLockRelease := make(chan struct{})
	var releaseOldLockOnce sync.Once
	releaseOldLock := func() {
		releaseOldLockOnce.Do(func() { close(allowOldLockRelease) })
	}
	defer releaseOldLock()
	oldLockDone := make(chan struct{})
	go func() {
		oldSub.WithOpLock(func() {
			close(oldLockHeld)
			<-allowOldLockRelease
		})
		close(oldLockDone)
	}()
	select {
	case <-oldLockHeld:
	case <-time.After(time.Second):
		t.Fatal("old subscription lock was not acquired")
	}

	cleanupDone := make(chan error, 1)
	go func() {
		_, cleanupErr := service.CleanupSubscriptionCircuitOpenNodes(oldSub.ID)
		cleanupDone <- cleanupErr
	}()
	select {
	case <-lookupReached:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not complete its initial lookup")
	}

	// Replace the manager entry while cleanup still owns only the old
	// subscription's pre-lock snapshot and the old op lock is held.
	subMgr.Unregister(oldSub.ID)
	subMgr.Register(newSub)
	releaseCleanupLookup()
	releaseOldLock()

	select {
	case cleanupErr := <-cleanupDone:
		if cleanupErr == nil {
			t.Fatal("cleanup unexpectedly succeeded across same-ID replacement")
		}
		var svcErr *ServiceError
		if !errors.As(cleanupErr, &svcErr) || svcErr.Code != "NOT_FOUND" {
			t.Fatalf("cleanup error = %v, want NOT_FOUND", cleanupErr)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after old lock release")
	}
	select {
	case <-oldLockDone:
	case <-time.After(time.Second):
		t.Fatal("old subscription lock holder did not finish")
	}
	if hookMismatch.Load() {
		t.Fatal("cleanup did not take its initial lookup from the old subscription")
	}

	managed, ok := newSub.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("replacement subscription lost its managed node")
	}
	if managed.Evicted {
		t.Fatal("cleanup mutated the replacement subscription")
	}
	if got := dirtyMarks.Load(); got != 0 {
		t.Fatalf("cleanup dirty-marked replacement node %d times", got)
	}
	if _, ok := pool.GetEntry(hash); !ok {
		t.Fatal("cleanup removed the replacement subscription's pool entry")
	}
}
