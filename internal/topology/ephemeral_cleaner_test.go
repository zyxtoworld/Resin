package topology

import (
	"context"
	"net/netip"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
)

// TestEphemeralCleaner_TOCTOU_RecoveryBetweenScans verifies that a node
// recovering in the window between the first scan (evictSet) and the
// second check (confirmedEvict) is NOT evicted.
//
// Timeline:
//  1. Node is circuit-broken with stale CircuitOpenSince → enters evictSet.
//  2. betweenScans hook fires: clears CircuitOpenSince (simulating recovery).
//  3. Second check re-reads CircuitOpenSince=0 → node is NOT confirmed.
//  4. Node remains in subscription's managed nodes.
func TestEphemeralCleaner_TOCTOU_RecoveryBetweenScans(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-toctou", "ephemeral-sub", "http://example.com", true, true)
	sub.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"toctou-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"toctou-node"}`), sub.ID)

	// Populate subscription's managed nodes.
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}

	// Node is circuit-broken long enough to qualify for eviction.
	pastTime := time.Now().Add(-1 * time.Hour).UnixNano()
	entry.CircuitOpenSince.Store(pastTime)

	cleaner := NewEphemeralCleaner(subMgr, pool)

	// The hook fires between first scan and second check, simulating
	// a recovery that happens in the TOCTOU window.
	hookCalled := false
	cleaner.sweepWithHook(func() {
		hookCalled = true
		// Simulate recovery: clear circuit.
		entry.CircuitOpenSince.Store(0)
		entry.FailureCount.Store(0)
	})

	if !hookCalled {
		t.Fatal("betweenScans hook was not called — node may not have been a candidate")
	}

	// The node should still be in the subscription's managed nodes.
	_, still := sub.ManagedNodes().LoadNode(hash)
	if !still {
		t.Fatal("TOCTOU regression: recovered node was evicted from subscription")
	}

	// The node should still be in the pool.
	_, poolOK := pool.GetEntry(hash)
	if !poolOK {
		t.Fatal("TOCTOU regression: recovered node was removed from pool")
	}
}

// TestEphemeralCleaner_StopContextHonorsShutdownDeadlineWhileSweepIsAdmitted
// is the production shutdown contract. The sweep is the real worker path; the
// test only supplies the deterministic gate at the subscription boundary.
func TestEphemeralCleaner_StopContextHonorsShutdownDeadlineWhileSweepIsAdmitted(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})
	sub := subscription.NewSubscription("shutdown-ephemeral", "shutdown-ephemeral", "https://example.invalid/sub", true, true)
	sub.SetEphemeralNodeEvictDelayNs(int64(time.Second))
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"shutdown-ephemeral-node"}`))
	raw := []byte(`{"type":"shutdown-ephemeral-node"}`)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"shutdown"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.beforeSubscriptionLockHook = func(string, *subscription.Subscription) {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}

	// Admit one worker exactly as Start does, then run the production sweep.
	cleaner.lifecycleMu.Lock()
	cleaner.started = true
	cleaner.wg.Add(1)
	cleaner.lifecycleMu.Unlock()
	sweepDone := make(chan struct{})
	go func() {
		defer cleaner.wg.Done()
		cleaner.sweep()
		close(sweepDone)
	}()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = cleaner.StopContext(context.Background())
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("sweep did not reach the production subscription lock boundary")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopErr := make(chan error, 1)
	go func() { stopErr <- cleaner.StopContext(ctx) }()
	select {
	case err := <-stopErr:
		if err != context.DeadlineExceeded {
			t.Fatalf("StopContext error = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("StopContext did not honor the caller deadline")
	}

	select {
	case <-sweepDone:
		t.Fatal("admitted sweep completed before its release gate")
	default:
	}

	close(release)
	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("sweep did not finish after its release gate")
	}
	if err := cleaner.StopContext(context.Background()); err != nil {
		t.Fatalf("background StopContext: %v", err)
	}
	managed, ok := sub.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("sweep removed the managed node")
	}
	if managed.Evicted {
		t.Fatal("late sweep mutated runtime state after stop admission closed")
	}
}

type ephemeralPersistenceAdmissionRecorder struct {
	subscriptionUpserts atomic.Int32
	subscriptionDeletes atomic.Int32
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkNodeStatic(string) bool {
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkNodeStaticDelete(string) bool {
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkNodeDynamic(string) bool {
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkNodeDynamicDelete(string) bool {
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkNodeLatency(string, string) bool {
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkNodeLatencyDelete(string, string) bool {
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkSubscriptionNode(string, string) bool {
	r.subscriptionUpserts.Add(1)
	return true
}

func (r *ephemeralPersistenceAdmissionRecorder) MarkSubscriptionNodeDelete(string, string) bool {
	r.subscriptionDeletes.Add(1)
	return true
}

// TestEphemeralCleaner_PersistenceRunnerWaitsForAdmittedMutation proves that
// an admitted cleanup keeps its one persistence owner alive after the caller
// deadline expires. A late cleanup that never gets admitted is covered by the
// preceding test and must not mutate runtime state.
func TestEphemeralCleaner_PersistenceRunnerWaitsForAdmittedMutation(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("sub-admitted-cleanup", "admitted-cleanup", "https://example.invalid/sub", true, true)
	sub.SetEphemeralNodeEvictDelayNs(int64(time.Second))
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"admitted-cleanup"}`))
	raw := []byte(`{"type":"admitted-cleanup"}`)
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
		OnSubNodeChangedWithPersistence: func(_ string, _ node.Hash, added bool, admission PersistenceAdmission) {
			if added {
				admission.MarkSubscriptionNode(sub.ID, hash.Hex())
			} else {
				admission.MarkSubscriptionNodeDelete(sub.ID, hash.Hex())
			}
		},
	})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"admitted"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	recorder := new(ephemeralPersistenceAdmissionRecorder)
	runnerEntered := make(chan struct{})
	allowRunner := make(chan struct{})
	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.SetPersistenceMutationRunner(func(fn func(PersistenceAdmission)) bool {
		close(runnerEntered)
		<-allowRunner
		fn(recorder)
		return true
	})

	cleaner.lifecycleMu.Lock()
	cleaner.started = true
	cleaner.wg.Add(1)
	cleaner.lifecycleMu.Unlock()
	sweepDone := make(chan struct{})
	go func() {
		defer cleaner.wg.Done()
		cleaner.sweep()
		close(sweepDone)
	}()
	defer func() {
		select {
		case <-allowRunner:
		default:
			close(allowRunner)
		}
		_ = cleaner.StopContext(context.Background())
	}()

	select {
	case <-runnerEntered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not enter its persistence owner")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := cleaner.StopContext(ctx); err != context.DeadlineExceeded {
		t.Fatalf("StopContext error = %v, want %v", err, context.DeadlineExceeded)
	}

	close(allowRunner)
	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("admitted cleanup did not finish")
	}
	if err := cleaner.StopContext(context.Background()); err != nil {
		t.Fatalf("background StopContext: %v", err)
	}
	if got := recorder.subscriptionDeletes.Load(); got != 1 {
		t.Fatalf("subscription delete marks = %d, want 1", got)
	}
	if got := recorder.subscriptionUpserts.Load(); got != 1 {
		t.Fatalf("evicted subscription upsert marks = %d, want 1", got)
	}
	managed, ok := sub.ManagedNodes().LoadNode(hash)
	if !ok || !managed.Evicted {
		t.Fatalf("admitted cleanup did not publish evicted managed state: %+v, %v", managed, ok)
	}
}

// TestEphemeralCleaner_ConfirmedEviction verifies that a node that remains
// circuit-broken through both checks IS evicted correctly.
func TestEphemeralCleaner_ConfirmedEviction(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-evict", "ephemeral-sub", "http://example.com", true, true)
	sub.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"evict-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"evict-node"}`), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}

	pastTime := time.Now().Add(-1 * time.Hour).UnixNano()
	entry.CircuitOpenSince.Store(pastTime)

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	managed, still := sub.ManagedNodes().LoadNode(hash)
	if !still {
		t.Fatal("expected circuit-broken node to remain in subscription managed nodes")
	}
	if !managed.Evicted {
		t.Fatal("expected circuit-broken node to be marked evicted")
	}
}

func TestEphemeralCleaner_StopIsIdempotent(t *testing.T) {
	cleaner := NewEphemeralCleaner(NewSubscriptionManager(), NewGlobalNodePool(PoolConfig{
		MaxConsecutiveFailures: func() int { return 2 },
	}))

	start := make(chan struct{})
	results := make(chan any, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			defer func() {
				results <- recover()
			}()
			cleaner.Stop()
		}()
	}
	close(start)

	for i := 0; i < 2; i++ {
		if panicValue := <-results; panicValue != nil {
			t.Fatalf("concurrent Stop panicked: %v", panicValue)
		}
	}
}

func TestEphemeralCleaner_StopWaitsForConcurrentStartAdmission(t *testing.T) {
	cleaner := NewEphemeralCleaner(
		NewSubscriptionManager(),
		NewGlobalNodePool(PoolConfig{MaxConsecutiveFailures: func() int { return 2 }}),
	)
	startReached := make(chan struct{})
	releaseStart := make(chan struct{})
	cleaner.beforeStartAdmissionHook = func() {
		close(startReached)
		<-releaseStart
	}

	startDone := make(chan struct{})
	go func() {
		cleaner.Start()
		close(startDone)
	}()
	select {
	case <-startReached:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach admission boundary")
	}

	stopDone := make(chan struct{})
	go func() {
		cleaner.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned before concurrent Start was admitted")
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseStart)
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("Start did not finish after admission release")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after the admitted worker exited")
	}
}

func TestEphemeralCleaner_StopBeforeStartRejectsLaterStart(t *testing.T) {
	cleaner := NewEphemeralCleaner(
		NewSubscriptionManager(),
		NewGlobalNodePool(PoolConfig{MaxConsecutiveFailures: func() int { return 2 }}),
	)
	var startCalls atomic.Int32
	var sweepCalls atomic.Int32
	cleaner.beforeStartAdmissionHook = func() {
		startCalls.Add(1)
	}
	cleaner.beforeSubscriptionLockHook = func(string, *subscription.Subscription) {
		sweepCalls.Add(1)
	}

	cleaner.Stop()
	cleaner.Start()
	cleaner.Stop()

	if got := startCalls.Load(); got != 0 {
		t.Fatalf("Start after Stop entered admission hook %d times", got)
	}
	if got := sweepCalls.Load(); got != 0 {
		t.Fatalf("Start after Stop launched a sweep with %d subscription callbacks", got)
	}
}

func TestEphemeralCleaner_NoOutboundErrorEvicted(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-no-ob-err", "ephemeral-sub", "http://example.com", true, true)
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"no-outbound-error-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"no-outbound-error-node"}`), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.SetLastError("outbound build: boom")

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	managed, still := sub.ManagedNodes().LoadNode(hash)
	if !still {
		t.Fatal("expected no-outbound error node to remain in subscription managed nodes")
	}
	if !managed.Evicted {
		t.Fatal("expected no-outbound error node to be marked evicted")
	}
}

func TestEphemeralCleaner_NoOutboundWithoutErrorSkipped(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-no-ob-ok", "ephemeral-sub", "http://example.com", true, true)
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"no-outbound-without-error-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"no-outbound-without-error-node"}`), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	if _, still := sub.ManagedNodes().LoadNode(hash); !still {
		t.Fatal("node without outbound but no error should not be evicted")
	}
}

func TestEphemeralCleaner_TOCTOU_NoOutboundErrorRecoveredBetweenScans(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-no-ob-toctou", "ephemeral-sub", "http://example.com", true, true)
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"no-outbound-toctou-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"no-outbound-toctou-node"}`), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.SetLastError("outbound build: boom")

	cleaner := NewEphemeralCleaner(subMgr, pool)

	hookCalled := false
	cleaner.sweepWithHook(func() {
		hookCalled = true
		entry.SetLastError("")
	})

	if !hookCalled {
		t.Fatal("betweenScans hook was not called — node may not have been a candidate")
	}
	if _, still := sub.ManagedNodes().LoadNode(hash); !still {
		t.Fatal("TOCTOU regression: recovered no-outbound error node was evicted")
	}
}

// TestEphemeralCleaner_DoesNotMutateSubscriptionAfterUnregister proves that
// a sweep snapshot cannot mutate a subscription after its delete lifecycle has
// already unregistered it. The shared pool reference keeps the node visible,
// so the old implementation deterministically evicts the deleted subscription.
func TestEphemeralCleaner_DoesNotMutateSubscriptionAfterUnregister(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	deleted := subscription.NewSubscription("sub-deleted", "deleted", "http://example.com/deleted", true, true)
	holder := subscription.NewSubscription("sub-holder", "holder", "http://example.com/holder", true, false)
	deleted.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	subMgr.Register(deleted)
	subMgr.Register(holder)

	hash := node.HashFromRawOptions([]byte(`{"type":"shared-delete-race"}`))
	raw := []byte(`{"type":"shared-delete-race"}`)
	pool.AddNodeFromSub(hash, raw, deleted.ID)
	pool.AddNodeFromSub(hash, raw, holder.ID)
	deleted.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"deleted"}})
	holder.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"holder"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("shared entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	var evicted atomic.Int32
	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.SetOnNodeEvicted(func(subID string, got node.Hash) {
		if subID == deleted.ID && got == hash {
			evicted.Add(1)
		}
	})

	snapshotReady := make(chan struct{})
	releaseSweep := make(chan struct{})
	cleaner.beforeSubscriptionLockHook = func(id string, _ *subscription.Subscription) {
		if id != deleted.ID {
			return
		}
		close(snapshotReady)
		<-releaseSweep
	}

	sweepDone := make(chan struct{})
	go func() {
		cleaner.sweep()
		close(sweepDone)
	}()
	<-snapshotReady

	// This is the runtime half of ControlPlaneService.DeleteSubscription after
	// its database delete: remove the subscription's pool reference, then
	// unregister the object. The holder keeps the node in the pool.
	pool.RemoveNodeFromSub(hash, deleted.ID)
	subMgr.Unregister(deleted.ID)
	close(releaseSweep)

	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("sweep did not finish after lifecycle release")
	}

	managed, ok := deleted.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("deleted subscription managed node unexpectedly disappeared")
	}
	if managed.Evicted {
		t.Fatal("sweep mutated a subscription after it was unregistered")
	}
	if got := evicted.Load(); got != 0 {
		t.Fatalf("deleted subscription emitted %d eviction callbacks", got)
	}
	if _, ok := pool.GetEntry(hash); !ok {
		t.Fatal("shared holder lost its pool node")
	}
}

// TestEphemeralCleaner_PersistenceCallbackIsBeforeSubscriptionUnlock proves
// that the persistence dirty mark belongs to the same subscription mutation
// transaction as the eviction.  DeleteSubscription uses the same op lock; if
// the callback runs after unlock, delete can publish its delete mark first and
// the cleaner can then publish a stale upsert.
func TestEphemeralCleaner_PersistenceCallbackIsBeforeSubscriptionUnlock(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-eviction-order", "ephemeral", "https://example.com", true, true)
	sub.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	subMgr.Register(sub)

	raw := []byte(`{"type":"eviction-order"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("eviction-order entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	callbackStarted := make(chan struct{})
	allowCallback := make(chan struct{})
	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.SetOnNodeEvicted(func(string, node.Hash) {
		close(callbackStarted)
		<-allowCallback
	})

	sweepDone := make(chan struct{})
	go func() {
		cleaner.sweep()
		close(sweepDone)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("ephemeral cleaner did not reach persistence callback")
	}

	mutationEntered := make(chan struct{})
	mutationDone := make(chan struct{})
	go func() {
		sub.WithOpLock(func() {
			close(mutationEntered)
		})
		close(mutationDone)
	}()

	select {
	case <-mutationEntered:
		t.Fatal("subscription mutation entered while eviction persistence callback was still running")
	case <-time.After(time.Second):
	}

	close(allowCallback)
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("subscription mutation did not complete after persistence callback")
	}
	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("ephemeral cleaner sweep did not complete")
	}
}

// TestEphemeralCleaner_DeleteWinsLatePersistenceMark uses the real weak-
// persistence path. The production pool delete callback is held after its
// delete mark and before subscription unregister; a cleaner callback that runs
// after unlock would then publish a late upsert into cache.db.
func TestEphemeralCleaner_DeleteWinsLatePersistenceMark(t *testing.T) {
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
		"sub-eviction-persistence-order",
		"eviction-persistence-order",
		"https://example.com",
		true,
		true,
	)
	sub.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
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

	raw := []byte(`{"type":"eviction-persistence-order"}`)
	hash := node.HashFromRawOptions(raw)
	var removeCallbacks atomic.Int32
	deleteMarkReached := make(chan struct{})
	allowDeleteReturn := make(chan struct{})
	var releaseDeleteOnce sync.Once
	releaseDelete := func() { releaseDeleteOnce.Do(func() { close(allowDeleteReturn) }) }
	defer releaseDelete()

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if added {
				engine.MarkSubscriptionNode(subID, changedHash.Hex())
				return
			}
			engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex())
			if removeCallbacks.Add(1) == 2 {
				close(deleteMarkReached)
				<-allowDeleteReturn
			}
		},
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
		t.Fatal("eviction-persistence-order entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	cleanerCallbackStarted := make(chan struct{})
	allowCleanerCallback := make(chan struct{})
	var releaseCleanerOnce sync.Once
	releaseCleaner := func() { releaseCleanerOnce.Do(func() { close(allowCleanerCallback) }) }
	defer releaseCleaner()
	afterMutationReached := make(chan struct{})
	allowAfterMutation := make(chan struct{})
	var releaseAfterMutationOnce sync.Once
	releaseAfterMutation := func() {
		releaseAfterMutationOnce.Do(func() { close(allowAfterMutation) })
	}
	defer releaseAfterMutation()
	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.afterSubscriptionMutationHook = func() {
		close(afterMutationReached)
		<-allowAfterMutation
	}
	cleaner.SetOnNodeEvicted(func(subID string, evictedHash node.Hash) {
		close(cleanerCallbackStarted)
		<-allowCleanerCallback
		engine.MarkSubscriptionNode(subID, evictedHash.Hex())
	})

	cleanerDone := make(chan struct{})
	go func() {
		cleaner.sweep()
		close(cleanerDone)
	}()

	fixedImplementation := false
	select {
	case <-cleanerCallbackStarted:
		fixedImplementation = true
	case <-afterMutationReached:
	case <-time.After(time.Second):
		t.Fatal("cleaner reached neither mutation boundary")
	}

	deleteDone := make(chan error, 1)
	go func() {
		sub.WithOpLock(func() {
			if deleteErr := engine.DeleteSubscription(sub.ID); deleteErr != nil {
				deleteDone <- deleteErr
				return
			}
			pool.RemoveNodeFromSub(hash, sub.ID)
			subMgr.Unregister(sub.ID)
		})
		deleteDone <- nil
	}()

	// Drive both implementations through the same durable observation. The
	// fixed implementation entered the callback while holding the subscription
	// lock; the old implementation reached the post-lock hook first. The hook
	// makes that distinction with channels, without a timing guess.
	if fixedImplementation {
		// Fixed implementation: let the synchronous persistence mark finish,
		// then release the post-lock test hook.
		releaseCleaner()
		releaseAfterMutation()
	} else {
		// Old implementation: let it leave the lock, but keep its late upsert
		// blocked until the delete mark has been published.
		releaseAfterMutation()
		select {
		case <-cleanerCallbackStarted:
		case <-time.After(time.Second):
			t.Fatal("cleaner did not reach the persistence callback")
		}
		select {
		case <-deleteMarkReached:
		case <-time.After(time.Second):
			t.Fatal("delete path did not reach its delete mark")
		}
		releaseCleaner()
	}

	select {
	case <-cleanerDone:
	case <-time.After(time.Second):
		t.Fatal("cleaner did not finish after releasing its callback")
	}
	select {
	case <-deleteMarkReached:
	case <-time.After(time.Second):
		t.Fatal("delete path did not reach its delete mark")
	}

	// The delete callback is still paused before unregister. If a stale cleaner
	// upsert followed the delete mark, this real flush writes the evicted row
	// back to cache.db; the final row assertion below catches that result.
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush delete result: %v", err)
	}
	if engine.DirtyCount() != 0 {
		t.Fatalf("dirty entries remain after delete flush: %d", engine.DirtyCount())
	}
	releaseDelete()

	select {
	case deleteErr := <-deleteDone:
		if deleteErr != nil {
			t.Fatalf("delete subscription: %v", deleteErr)
		}
	case <-time.After(time.Second):
		t.Fatal("delete path did not finish")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if engine.DirtyCount() != 0 {
		t.Fatalf("dirty entries remain after final flush: %d", engine.DirtyCount())
	}
	rows, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	for _, row := range rows {
		if row.SubscriptionID == sub.ID && row.NodeHash == hash.Hex() {
			t.Fatal("deleted subscription-node row survived the final flush")
		}
	}
}

// TestEphemeralCleaner_NonEphemeralSkipped verifies non-ephemeral subs are skipped.
func TestEphemeralCleaner_NonEphemeralSkipped(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-persist", "persistent-sub", "http://example.com", true, false) // NOT ephemeral
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"persistent-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"persistent-node"}`), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}

	pastTime := time.Now().Add(-1 * time.Hour).UnixNano()
	entry.CircuitOpenSince.Store(pastTime)

	cleaner := NewEphemeralCleaner(subMgr, pool)
	cleaner.sweep()

	_, still := sub.ManagedNodes().LoadNode(hash)
	if !still {
		t.Fatal("non-ephemeral sub should not have nodes evicted")
	}
}

func TestEphemeralCleaner_DynamicEvictDelayPulled(t *testing.T) {
	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub := subscription.NewSubscription("sub-dynamic", "ephemeral-sub", "http://example.com", true, true)
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"dynamic-node"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"dynamic-node"}`), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag1"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	sub.SetEphemeralNodeEvictDelayNs(int64(10 * time.Minute))
	cleaner := NewEphemeralCleaner(subMgr, pool)

	// Delay too long: should not evict.
	cleaner.sweep()
	if _, still := sub.ManagedNodes().LoadNode(hash); !still {
		t.Fatal("node should not be evicted with long evict delay")
	}

	// Shrink delay dynamically: next sweep should evict.
	sub.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	cleaner.sweep()
	managed, still := sub.ManagedNodes().LoadNode(hash)
	if !still {
		t.Fatal("node should remain in managed nodes after being evicted")
	}
	if !managed.Evicted {
		t.Fatal("node should be marked evicted after evict delay shrinks")
	}
}

func TestEphemeralCleaner_SweepSubscriptionsInParallel(t *testing.T) {
	oldMaxProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	subMgr := NewSubscriptionManager()
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	sub1 := subscription.NewSubscription("sub-1", "ephemeral-1", "http://example.com/1", true, true)
	sub2 := subscription.NewSubscription("sub-2", "ephemeral-2", "http://example.com/2", true, true)
	sub1.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	sub2.SetEphemeralNodeEvictDelayNs(int64(30 * time.Second))
	subMgr.Register(sub1)
	subMgr.Register(sub2)

	hash1 := node.HashFromRawOptions([]byte(`{"type":"parallel-node-1"}`))
	hash2 := node.HashFromRawOptions([]byte(`{"type":"parallel-node-2"}`))

	pool.AddNodeFromSub(hash1, []byte(`{"type":"parallel-node-1"}`), sub1.ID)
	pool.AddNodeFromSub(hash2, []byte(`{"type":"parallel-node-2"}`), sub2.ID)
	sub1.ManagedNodes().StoreNode(hash1, subscription.ManagedNode{Tags: []string{"tag1"}})
	sub2.ManagedNodes().StoreNode(hash2, subscription.ManagedNode{Tags: []string{"tag2"}})

	entry1, ok := pool.GetEntry(hash1)
	if !ok {
		t.Fatal("entry1 not found")
	}
	entry2, ok := pool.GetEntry(hash2)
	if !ok {
		t.Fatal("entry2 not found")
	}

	pastTime := time.Now().Add(-1 * time.Hour).UnixNano()
	entry1.CircuitOpenSince.Store(pastTime)
	entry2.CircuitOpenSince.Store(pastTime)

	releaseHook := make(chan struct{})
	allStarted := make(chan struct{})
	var started atomic.Int32

	cleaner := NewEphemeralCleaner(subMgr, pool)
	done := make(chan struct{})
	go func() {
		cleaner.sweepWithHook(func() {
			if started.Add(1) == 2 {
				close(allStarted)
			}
			<-releaseHook
		})
		close(done)
	}()

	select {
	case <-allStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected ephemeral subscription sweeps to run in parallel")
	}

	select {
	case <-done:
		t.Fatal("sweepWithHook should wait for in-flight subscription sweeps")
	default:
	}

	close(releaseHook)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sweepWithHook did not finish after release")
	}

	if got := started.Load(); got != 2 {
		t.Fatalf("expected hook to run for 2 subscriptions, got %d", got)
	}
}
