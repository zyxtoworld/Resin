package routing

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func TestSnapshotIPLoadContextCancellationInterruptsLifecycleReadWait(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-ip-load-cancel", "Plat-IP-Load-Cancel", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)

	router.lifecycleMu.Lock()
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		router.lifecycleMu.Unlock()
	}
	defer release()

	lockAttempted := make(chan struct{})
	var attemptOnce sync.Once
	router.beforeIPLoadSnapshotLockHook = func() {
		attemptOnce.Do(func() { close(lockAttempted) })
	}
	defer func() { router.beforeIPLoadSnapshotLockHook = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type snapshotResult struct {
		err error
	}
	done := make(chan snapshotResult, 1)
	go func() {
		_, _, err := router.SnapshotIPLoadForPlatformContext(ctx, plat.ID)
		done <- snapshotResult{err: err}
	}()
	select {
	case <-lockAttempted:
	case <-time.After(time.Second):
		t.Fatal("IP-load snapshot did not reach lifecycle lock boundary")
	}
	cancel()

	timedOut := false
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled IP-load snapshot error = %v, want context.Canceled", result.err)
		}
	case <-time.After(time.Second):
		timedOut = true
	}
	if timedOut {
		release()
		<-done
		t.Fatal("canceled IP-load snapshot remained blocked until lifecycle writer release")
	}
}

func TestLeaseCallbackReadReentryDoesNotDeadlockWithPendingPlatformRemoval(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-callback-reentry", "Plat-Callback-Reentry", nil, nil)
	pool.addPlatform(plat)

	callbackEntered := make(chan struct{})
	allowCallbackRead := make(chan struct{})
	callbackDone := make(chan struct{})
	removalLockAcquired := make(chan struct{})
	var callbackOnce sync.Once

	var router *Router
	router = newTestRouter(pool, func(event LeaseEvent) {
		if event.Type != LeaseRemove {
			return
		}
		callbackOnce.Do(func() {
			close(callbackEntered)
			<-allowCallbackRead
			_ = router.ReadLease(model.LeaseKey{PlatformID: plat.ID, Account: event.Account})
			close(callbackDone)
		})
	})
	router.afterPlatformStateRemovalLockHook = func() {
		close(removalLockAcquired)
	}

	state := router.ensurePlatformState(plat.ID)
	state.Leases.CreateLease("callback-account", Lease{
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"callback"}`)),
		EgressIP:    netip.MustParseAddr("203.0.113.90"),
		CreatedAtNs: time.Now().Add(-time.Minute).UnixNano(),
		ExpiryNs:    time.Now().Add(time.Hour).UnixNano(),
	})

	deleteDone := make(chan struct{})
	go func() {
		router.DeleteLease(plat.ID, "callback-account")
		close(deleteDone)
	}()

	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("lease callback did not start")
	}

	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()

	select {
	case <-removalLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not acquire its exclusive lifecycle lock")
	}
	close(allowCallbackRead)

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("lease callback read re-entry deadlocked behind pending removal")
	}
	select {
	case <-deleteDone:
	case <-time.After(time.Second):
		t.Fatal("DeleteLease did not return")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("RemovePlatformState did not return")
	}
}

func TestAtomicPlatformReadDoesNotNestReadLockWhenRemovalWriterIsPending(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-atomic-read", "Plat-Atomic-Read", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	router.ensurePlatformState(plat.ID).Leases.CreateLease("atomic-account", Lease{
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"atomic-read"}`)),
		EgressIP:    netip.MustParseAddr("203.0.113.92"),
		CreatedAtNs: time.Now().Add(-time.Minute).UnixNano(),
		ExpiryNs:    time.Now().Add(time.Hour).UnixNano(),
	})

	readEntered := make(chan struct{})
	allowRead := make(chan struct{})
	writerAttempted := make(chan struct{})
	router.beforeAtomicPlatformReadHook = func() {
		close(readEntered)
		<-allowRead
	}
	router.beforePlatformStateRemovalLockHook = func() {
		close(writerAttempted)
	}

	type listResult struct {
		leases []model.Lease
		exists bool
	}
	listDone := make(chan listResult, 1)
	go func() {
		leases, exists := router.ListLeasesForPlatform(plat.ID)
		listDone <- listResult{leases: leases, exists: exists}
	}()
	select {
	case <-readEntered:
	case <-time.After(time.Second):
		t.Fatal("atomic platform read did not reach its in-lock seam")
	}

	// Match production deletion order: the pool is unregistered before the
	// Router's exclusive drain. The writer is now waiting while the read holds
	// lifecycleMu.RLock.
	pool.removePlatform(plat.ID)
	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()
	select {
	case <-writerAttempted:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not reach its exclusive lock boundary")
	}
	select {
	case <-listDone:
		t.Fatal("atomic platform read returned before its release gate")
	default:
	}

	close(allowRead)
	var listed listResult
	select {
	case listed = <-listDone:
	case <-time.After(time.Second):
		t.Fatal("atomic platform read deadlocked with a pending removal writer")
	}
	if !listed.exists || len(listed.leases) != 1 || listed.leases[0].Account != "atomic-account" {
		t.Fatalf("atomic platform read = %#v, want the pre-removal lease snapshot", listed)
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not finish after the read released")
	}
	if _, exists := router.ListLeasesForPlatform(plat.ID); exists {
		t.Fatal("atomic platform read reported a removed platform as present")
	}
}

func TestAtomicPlatformLeaseAPIsDistinguishEmptyStateFromMissingPlatform(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-empty-state", "Plat-Empty-State", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)

	leases, exists := router.ListLeasesForPlatform(plat.ID)
	if !exists || len(leases) != 0 {
		t.Fatalf("empty platform list = (%#v, %t), want empty result with platform present", leases, exists)
	}
	if lease, exists := router.ReadLeaseForPlatform(model.LeaseKey{PlatformID: plat.ID, Account: "missing"}); !exists || lease != nil {
		t.Fatalf("empty platform read = (%#v, %t), want nil lease with platform present", lease, exists)
	}
	if load, exists := router.SnapshotIPLoadForPlatform(plat.ID); !exists || len(load) != 0 {
		t.Fatalf("empty platform load = (%#v, %t), want empty load with platform present", load, exists)
	}
	if deleted, exists := router.DeleteLeaseForPlatform(plat.ID, "missing"); !exists || deleted {
		t.Fatalf("empty platform delete = (%t, %t), want no lease with platform present", deleted, exists)
	}
	if count, exists := router.DeleteAllLeasesForPlatform(plat.ID); !exists || count != 0 {
		t.Fatalf("empty platform delete-all = (%d, %t), want zero with platform present", count, exists)
	}
	if err := router.InheritLeaseForPlatform(plat.ID, "missing", "child", time.Now()); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("empty platform inherit error = %v, want ErrLeaseNotFound", err)
	}

	pool.removePlatform(plat.ID)
	if _, exists := router.ListLeasesForPlatform(plat.ID); exists {
		t.Fatal("missing platform list reported platform present")
	}
	if _, exists := router.ReadLeaseForPlatform(model.LeaseKey{PlatformID: plat.ID, Account: "missing"}); exists {
		t.Fatal("missing platform read reported platform present")
	}
	if _, exists := router.SnapshotIPLoadForPlatform(plat.ID); exists {
		t.Fatal("missing platform load reported platform present")
	}
	if deleted, exists := router.DeleteLeaseForPlatform(plat.ID, "missing"); exists || deleted {
		t.Fatalf("missing platform delete = (%t, %t), want absent platform", deleted, exists)
	}
	if count, exists := router.DeleteAllLeasesForPlatform(plat.ID); exists || count != 0 {
		t.Fatalf("missing platform delete-all = (%d, %t), want absent platform", count, exists)
	}
	if err := router.InheritLeaseForPlatform(plat.ID, "missing", "child", time.Now()); !errors.Is(err, ErrPlatformNotFound) {
		t.Fatalf("missing platform inherit error = %v, want ErrPlatformNotFound", err)
	}
}

func TestPlatformRemovalDrainsResolvedRouteBeforeRemovingState(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-route-removal-drain", "Plat-Route-Removal-Drain", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)
	hash, entry := newRoutableEntry(t, `{"id":"route-removal-drain"}`, "203.0.113.90")
	pool.addEntry(hash, entry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	routeResolved := make(chan struct{})
	allowRoute := make(chan struct{})
	var resolveOnce sync.Once
	router.afterPlatformResolveHook = func(*platform.Platform) {
		resolveOnce.Do(func() {
			close(routeResolved)
			<-allowRoute
		})
	}
	removalStarted := make(chan struct{})
	var removalStartOnce sync.Once
	router.beforePlatformStateRemovalLockHook = func() {
		removalStartOnce.Do(func() { close(removalStarted) })
	}
	removalLockAcquired := make(chan struct{})
	router.afterPlatformStateRemovalLockHook = func() {
		close(removalLockAcquired)
	}

	var routeResult RouteResult
	var routeErr error
	routeDone := make(chan struct{})
	go func() {
		routeResult, routeErr = router.RouteRequest(plat.Name, "route-account", "https://cloudflare.com/")
		close(routeDone)
	}()
	select {
	case <-routeResolved:
	case <-time.After(time.Second):
		t.Fatal("route did not reach the resolved-platform boundary")
	}

	// Match the service deletion order: stop new pool resolution before the
	// router's exclusive drain. The already-resolved route still owns RLock.
	pool.removePlatform(plat.ID)
	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()
	select {
	case <-removalStarted:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not start")
	}
	select {
	case <-routeDone:
		t.Fatal("resolved route returned before its release gate")
	default:
	}
	select {
	case <-removalLockAcquired:
		t.Fatal("platform removal acquired its exclusive lock before the route drained")
	default:
	}
	select {
	case <-removeDone:
		t.Fatal("platform removal returned before the resolved route drained")
	default:
	}

	close(allowRoute)
	select {
	case <-routeDone:
	case <-time.After(time.Second):
		t.Fatal("resolved route did not finish after release")
	}
	if routeErr != nil {
		t.Fatalf("RouteRequest: %v", routeErr)
	}
	if routeResult.PlatformID != plat.ID {
		t.Fatalf("route platform id = %q, want %q", routeResult.PlatformID, plat.ID)
	}
	select {
	case <-removalLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not acquire its exclusive lock after the route drained")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not finish")
	}

	if router.ReadLease(model.LeaseKey{PlatformID: plat.ID, Account: "route-account"}) != nil {
		t.Fatal("platform removal retained the lease created by the drained route")
	}
	if got := router.SnapshotIPLoad(plat.ID); len(got) != 0 {
		t.Fatalf("platform removal retained IP load state: %#v", got)
	}
	if router.RangeLeases(plat.ID, func(string, Lease) bool { return false }) {
		t.Fatal("platform removal retained routing state")
	}
	if _, ok := router.states.Load(plat.ID); ok {
		t.Fatal("platform removal retained the platform state map entry")
	}
}

func TestLeaseDeleteTicketPrecedesSameAccountUpsert(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-delete-ticket", "Plat-Delete-Ticket", nil, nil)
	pool.addPlatform(plat)

	var mu sync.Mutex
	var events []LeaseEvent
	allEvents := make(chan struct{})
	var eventOnce sync.Once
	router := newTestRouter(pool, func(event LeaseEvent) {
		mu.Lock()
		events = append(events, event)
		if len(events) == 2 {
			eventOnce.Do(func() { close(allEvents) })
		}
		mu.Unlock()
	})

	hash := node.HashFromRawOptions([]byte(`{"id":"delete-ticket"}`))
	ip := netip.MustParseAddr("203.0.113.91")
	state := router.ensurePlatformState(plat.ID)
	state.Leases.CreateLease("same-account", Lease{
		NodeHash:    hash,
		EgressIP:    ip,
		CreatedAtNs: time.Now().Add(-time.Minute).UnixNano(),
		ExpiryNs:    time.Now().Add(time.Hour).UnixNano(),
	})

	deleteLinearized := make(chan struct{})
	allowDelete := make(chan struct{})
	router.afterLeaseDeleteLinearizedHook = func() {
		close(deleteLinearized)
		<-allowDelete
	}

	deleteDone := make(chan bool, 1)
	go func() { deleteDone <- router.DeleteLease(plat.ID, "same-account") }()
	select {
	case <-deleteLinearized:
	case <-time.After(time.Second):
		t.Fatal("delete did not reach its Compute linearization point")
	}

	upsertDone := make(chan error, 1)
	upsertStarted := make(chan struct{})
	go func() {
		close(upsertStarted)
		upsertDone <- router.UpsertLease(model.Lease{
			PlatformID: plat.ID,
			Account:    "same-account",
			NodeHash:   hash.Hex(),
			EgressIP:   ip.String(),
		})
	}()
	<-upsertStarted
	select {
	case err := <-upsertDone:
		t.Fatalf("same-account upsert overtook the in-flight delete: %v", err)
	default:
	}
	close(allowDelete)

	if !<-deleteDone {
		t.Fatal("DeleteLease did not delete the lease")
	}
	if err := <-upsertDone; err != nil {
		t.Fatalf("UpsertLease: %v", err)
	}
	select {
	case <-allEvents:
	case <-time.After(time.Second):
		t.Fatal("did not observe both lease events")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %#v", len(events), events)
	}
	if events[0].Type != LeaseRemove || events[1].Type != LeaseCreate {
		t.Fatalf("same-account event order = [%v, %v], want [remove, create]", events[0].Type, events[1].Type)
	}
}

func TestLeaseCreateTicketPrecedesPlatformRemove(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-create-remove", "Plat-Create-Remove", nil, nil)
	pool.addPlatform(plat)

	var mu sync.Mutex
	var events []LeaseEvent
	allEvents := make(chan struct{})
	var eventOnce sync.Once
	router := newTestRouter(pool, func(event LeaseEvent) {
		mu.Lock()
		events = append(events, event)
		if len(events) == 2 {
			eventOnce.Do(func() { close(allEvents) })
		}
		mu.Unlock()
	})
	hash := node.HashFromRawOptions([]byte(`{"id":"create-remove"}`))
	ip := netip.MustParseAddr("203.0.113.99")

	createLinearized := make(chan struct{})
	allowCreate := make(chan struct{})
	router.afterLeaseUpsertLinearizedHook = func() {
		close(createLinearized)
		<-allowCreate
	}
	removalAttempted := make(chan struct{})
	router.beforePlatformStateRemovalLockHook = func() {
		select {
		case <-removalAttempted:
		default:
			close(removalAttempted)
		}
	}

	createDone := make(chan error, 1)
	go func() {
		createDone <- router.UpsertLease(model.Lease{
			PlatformID: plat.ID,
			Account:    "create-account",
			NodeHash:   hash.Hex(),
			EgressIP:   ip.String(),
		})
	}()
	select {
	case <-createLinearized:
	case <-time.After(time.Second):
		t.Fatal("create did not reach its Compute linearization point")
	}

	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()
	select {
	case <-removalAttempted:
	case <-time.After(time.Second):
		t.Fatal("remove did not reach its lifecycle boundary")
	}
	close(allowCreate)

	if err := <-createDone; err != nil {
		t.Fatalf("UpsertLease: %v", err)
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("RemovePlatformState did not return")
	}
	select {
	case <-allEvents:
	case <-time.After(time.Second):
		t.Fatal("did not observe create and remove events")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %#v", len(events), events)
	}
	if events[0].Type != LeaseCreate || events[1].Type != LeaseRemove {
		t.Fatalf("create/remove event order = [%v, %v], want [create, remove]", events[0].Type, events[1].Type)
	}
}

func TestLeaseEventsFromOneComputeKeepContiguousTicketOrder(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-event-batch", "Plat-Event-Batch", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)
	platB := platform.NewPlatform("plat-event-batch-b", "Plat-Event-Batch-B", nil, nil)
	pool.addPlatform(platB)
	hash, entry := newRoutableEntry(t, `{"id":"event-batch"}`, "203.0.113.102")
	pool.addEntry(hash, entry)
	pool.rebuildPlatformView(plat)

	var mu sync.Mutex
	var events []LeaseEvent
	allEvents := make(chan struct{})
	var allOnce sync.Once
	router := newTestRouter(pool, func(event LeaseEvent) {
		mu.Lock()
		events = append(events, event)
		if len(events) == 3 {
			allOnce.Do(func() { close(allEvents) })
		}
		mu.Unlock()
	})
	now := time.Now()
	state := router.ensurePlatformState(plat.ID)
	state.Leases.CreateLease("batch-a", Lease{
		NodeHash:    hash,
		EgressIP:    entry.GetEgressIP(),
		CreatedAtNs: now.Add(-time.Hour).UnixNano(),
		ExpiryNs:    now.Add(-time.Second).UnixNano(),
	})

	batchTicketed := make(chan struct{})
	allowBatch := make(chan struct{})
	var ticketHookCalls atomic.Int32
	bLinearized := make(chan struct{})
	router.afterLeaseUpsertLinearizedHook = func() {
		select {
		case <-bLinearized:
		default:
			close(bLinearized)
		}
	}
	router.afterLeaseEventBatchTicketHook = func() {
		if ticketHookCalls.Add(1) == 1 {
			close(batchTicketed)
			<-allowBatch
		}
	}

	routeDone := make(chan error, 1)
	go func() {
		_, err := router.RouteRequest(plat.Name, "batch-a", "https://cloudflare.com/")
		routeDone <- err
	}()
	select {
	case <-batchTicketed:
	case <-time.After(time.Second):
		t.Fatal("expired replacement did not assign its event batch")
	}

	upsertDone := make(chan error, 1)
	go func() {
		upsertDone <- router.UpsertLease(model.Lease{
			PlatformID: platB.ID,
			Account:    "batch-b",
			NodeHash:   hash.Hex(),
			EgressIP:   entry.GetEgressIP().String(),
		})
	}()
	select {
	case <-bLinearized:
	case <-time.After(time.Second):
		t.Fatal("independent upsert did not mutate while first Compute was paused")
	}
	select {
	case <-upsertDone:
		t.Fatal("independent upsert returned before the earlier event batch was released")
	default:
	}
	close(allowBatch)

	if err := <-routeDone; err != nil {
		t.Fatalf("replacement route: %v", err)
	}
	if err := <-upsertDone; err != nil {
		t.Fatalf("independent upsert: %v", err)
	}
	select {
	case <-allEvents:
	case <-time.After(time.Second):
		t.Fatal("did not observe all event callbacks")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3: %#v", len(events), events)
	}
	if events[0].Type != LeaseExpire || events[1].Type != LeaseCreate || events[2].Type != LeaseCreate {
		t.Fatalf("event order = [%v, %v, %v], want [expire, create, create]", events[0].Type, events[1].Type, events[2].Type)
	}
	if events[0].Account != "batch-a" || events[1].Account != "batch-a" || events[2].Account != "batch-b" {
		t.Fatalf("event accounts = [%q, %q, %q], want [batch-a, batch-a, batch-b]", events[0].Account, events[1].Account, events[2].Account)
	}
}

func TestRemovePlatformStateWaitsForSynchronousLeaseEvent(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-delete-barrier", "Plat-Delete-Barrier", nil, nil)
	pool.addPlatform(plat)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	router := newTestRouter(pool, func(event LeaseEvent) {
		if event.Type != LeaseRemove {
			return
		}
		close(callbackStarted)
		<-releaseCallback
	})
	state := router.ensurePlatformState(plat.ID)
	state.Leases.CreateLease("barrier-account", Lease{
		NodeHash: node.HashFromRawOptions([]byte(`{"id":"barrier"}`)),
		EgressIP: netip.MustParseAddr("203.0.113.92"),
		ExpiryNs: time.Now().Add(time.Hour).UnixNano(),
	})

	done := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(done)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("remove callback did not start")
	}
	select {
	case <-done:
		t.Fatal("RemovePlatformState returned before its synchronous callback completed")
	default:
	}
	close(releaseCallback)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RemovePlatformState did not return after callback release")
	}
}

func TestRouterStopWaitsForInFlightLeaseEvent(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-stop-event", "Plat-Stop-Event", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)
	hash, entry := newRoutableEntry(t, `{"id":"stop-event"}`, "203.0.113.120")
	pool.addEntry(hash, entry)
	pool.rebuildPlatformView(plat)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var callbackOnce sync.Once
	router := newTestRouter(pool, func(event LeaseEvent) {
		if event.Type != LeaseCreate {
			return
		}
		callbackOnce.Do(func() { close(callbackStarted) })
		<-releaseCallback
	})
	stopAdmissionClosed := make(chan struct{})
	router.afterStopAdmissionHook = func() { close(stopAdmissionClosed) }

	routeDone := make(chan error, 1)
	go func() {
		_, err := router.RouteRequest(plat.Name, "stop-account", "https://example.com/")
		routeDone <- err
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("lease callback did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		router.Stop()
		close(stopDone)
	}()
	select {
	case <-stopAdmissionClosed:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close route admission")
	}
	select {
	case <-stopDone:
		t.Fatal("Router.Stop returned before the in-flight lease callback completed")
	default:
	}

	close(releaseCallback)
	select {
	case <-routeDone:
	case <-time.After(time.Second):
		t.Fatal("RouteRequest did not finish after callback release")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Router.Stop did not finish after callback release")
	}
}

func TestLeaseCleanerDoesNotMutateAfterRouterStop(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-cleaner-after-stop", "Plat-Cleaner-After-Stop", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	state := router.ensurePlatformState(plat.ID)
	hash := node.HashFromRawOptions([]byte(`{"id":"cleaner-after-stop"}`))
	state.Leases.CreateLease("stopped-cleaner-account", Lease{
		NodeHash:    hash,
		EgressIP:    netip.MustParseAddr("203.0.113.122"),
		CreatedAtNs: time.Now().Add(-time.Minute).UnixNano(),
		ExpiryNs:    time.Now().Add(-time.Second).UnixNano(),
	})

	cleaner := NewLeaseCleaner(router)
	router.Stop()
	cleaner.sweep()

	if _, ok := state.Leases.GetLease("stopped-cleaner-account"); !ok {
		t.Fatal("LeaseCleaner mutated routing state after Router.Stop")
	}
}

func TestLeaseCleanerExpireTicketPrecedesPlatformRemove(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-expire-remove-order", "Plat-Expire-Remove-Order", nil, nil)
	pool.addPlatform(plat)

	var mu sync.Mutex
	var events []LeaseEvent
	allEvents := make(chan struct{})
	var allOnce sync.Once
	router := newTestRouter(pool, func(event LeaseEvent) {
		mu.Lock()
		events = append(events, event)
		if len(events) == 2 {
			allOnce.Do(func() { close(allEvents) })
		}
		mu.Unlock()
	})

	hash := node.HashFromRawOptions([]byte(`{"id":"expire-remove"}`))
	now := time.Now()
	state := router.ensurePlatformState(plat.ID)
	state.Leases.CreateLease("expired-account", Lease{
		NodeHash:    hash,
		EgressIP:    netip.MustParseAddr("203.0.113.93"),
		CreatedAtNs: now.Add(-time.Minute).UnixNano(),
		ExpiryNs:    now.Add(-time.Second).UnixNano(),
	})
	state.Leases.CreateLease("active-account", Lease{
		NodeHash:    hash,
		EgressIP:    netip.MustParseAddr("203.0.113.94"),
		CreatedAtNs: now.UnixNano(),
		ExpiryNs:    now.Add(time.Hour).UnixNano(),
	})

	expireLinearized := make(chan struct{})
	allowExpire := make(chan struct{})
	var expireOnce sync.Once
	router.afterLeaseDeleteLinearizedHook = func() {
		expireOnce.Do(func() {
			close(expireLinearized)
			<-allowExpire
		})
	}
	removalAttempted := make(chan struct{})
	router.beforePlatformStateRemovalLockHook = func() {
		select {
		case <-removalAttempted:
		default:
			close(removalAttempted)
		}
	}

	cleaner := NewLeaseCleaner(router)
	cleanerDone := make(chan struct{})
	go func() {
		cleaner.sweep()
		close(cleanerDone)
	}()
	select {
	case <-expireLinearized:
	case <-time.After(time.Second):
		t.Fatal("cleaner did not assign the expire ticket inside Compute")
	}

	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()
	select {
	case <-removalAttempted:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not reach its exclusive boundary")
	}
	close(allowExpire)

	select {
	case <-cleanerDone:
	case <-time.After(time.Second):
		t.Fatal("cleaner did not finish after releasing its Compute hook")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not finish")
	}
	select {
	case <-allEvents:
	case <-time.After(time.Second):
		t.Fatal("did not observe expire and remove events")
	}

	mu.Lock()
	defer mu.Unlock()
	if events[0].Type != LeaseExpire || events[0].Account != "expired-account" {
		t.Fatalf("first event = %#v, want expire for expired-account", events[0])
	}
	if events[1].Type != LeaseRemove || events[1].Account != "active-account" {
		t.Fatalf("second event = %#v, want remove for active-account", events[1])
	}
}

func TestLateQuarantineCannotRecreateRemovedPlatformState(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-late-quarantine", "Plat-Late-Quarantine", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	router.ensurePlatformState(plat.ID).ResponseCooldowns.Mark(
		platform.ResponseRuleScopeNode,
		node.HashFromRawOptions([]byte(`{"id":"late-quarantine"}`)),
		netip.MustParseAddr("203.0.113.100"),
		time.Now().Add(time.Minute),
	)

	pool.removePlatform(plat.ID)
	removalLockAcquired := make(chan struct{})
	allowRemoval := make(chan struct{})
	router.afterPlatformStateRemovalLockHook = func() {
		close(removalLockAcquired)
		<-allowRemoval
	}

	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()
	select {
	case <-removalLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not acquire lifecycle write lock")
	}

	quarantineDone := make(chan struct{})
	go func() {
		router.QuarantineRoute(RouteResult{
			PlatformID: plat.ID,
			NodeHash:   node.HashFromRawOptions([]byte(`{"id":"late-quarantine"}`)),
			EgressIP:   netip.MustParseAddr("203.0.113.100"),
		}, platform.ResponseRuleScopeNode, time.Now().Add(time.Minute))
		close(quarantineDone)
	}()
	select {
	case <-quarantineDone:
		t.Fatal("late quarantine bypassed the platform lifecycle write lock")
	default:
	}
	close(allowRemoval)

	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not finish")
	}
	select {
	case <-quarantineDone:
	case <-time.After(time.Second):
		t.Fatal("late quarantine did not finish after platform removal")
	}
	if _, ok := router.states.Load(plat.ID); ok {
		t.Fatal("late quarantine recreated removed platform state")
	}
}

func TestLateQuarantineFromReplacedEntryDoesNotCoolReplacement(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-late-quarantine-entry", "Plat-Late-Quarantine-Entry", nil, nil)
	pool.addPlatform(plat)

	const raw = `{"id":"late-quarantine-entry"}`
	hash, oldEntry := newRoutableEntry(t, raw, "203.0.113.110")
	pool.addEntry(hash, oldEntry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)

	route, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if route.selectedEntry != oldEntry {
		t.Fatal("initial route did not retain the old entry identity")
	}

	newEntry := newHealthyEntryForHash(t, hash, []byte(raw), "203.0.113.111")
	pool.addEntry(hash, newEntry)
	pool.rebuildPlatformView(plat)

	// This response belongs to oldEntry. It must not quarantine a newly
	// published entry that happens to have the same content hash.
	router.QuarantineRoute(route, platform.ResponseRuleScopeNode, time.Now().Add(time.Minute))
	got, err := router.RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("replacement route was cooled by a stale response: %v", err)
	}
	if got.NodeHash != hash || got.selectedEntry != newEntry {
		t.Fatalf("replacement route = hash %s entry %p, want hash %s entry %p", got.NodeHash.Hex(), got.selectedEntry, hash.Hex(), newEntry)
	}
}

func TestQuarantineReplacementBetweenExactCheckAndMarkDoesNotCoolNewEntry(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-quarantine-check-mark", "Plat-Quarantine-Check-Mark", nil, nil)
	pool.addPlatform(plat)

	const raw = `{"id":"quarantine-check-mark"}`
	hash, oldEntry := newRoutableEntry(t, raw, "203.0.113.120")
	pool.addEntry(hash, oldEntry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)

	route, err := router.RouteRequest(plat.Name, "initial", "https://example.com")
	if err != nil {
		t.Fatalf("initial route: %v", err)
	}
	if route.selectedEntry != oldEntry {
		t.Fatal("initial route did not retain the old entry identity")
	}

	checked := make(chan struct{})
	allowMark := make(chan struct{})
	router.beforeResponseCooldownMarkHook = func() {
		close(checked)
		<-allowMark
	}
	quarantineDone := make(chan struct{})
	go func() {
		router.QuarantineRoute(route, platform.ResponseRuleScopeNode, time.Now().Add(time.Minute))
		close(quarantineDone)
	}()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("quarantine did not reach the post-check boundary")
	}

	newEntry := newHealthyEntryForHash(t, hash, []byte(raw), "203.0.113.121")
	pool.addEntry(hash, newEntry)
	pool.rebuildPlatformView(plat)
	close(allowMark)
	select {
	case <-quarantineDone:
	case <-time.After(time.Second):
		t.Fatal("quarantine did not finish after replacement")
	}

	got, err := router.RouteRequest(plat.Name, "replacement", "https://example.com")
	if err != nil {
		t.Fatalf("replacement route was cooled by a stale response: %v", err)
	}
	if got.NodeHash != hash || got.selectedEntry != newEntry {
		t.Fatalf("replacement route = hash %s entry %p, want hash %s entry %p", got.NodeHash.Hex(), got.selectedEntry, hash.Hex(), newEntry)
	}
}

func TestLateOlderQuarantineCannotEraseNewerEntryCooldown(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-quarantine-order", "Plat-Quarantine-Order", nil, nil)
	pool.addPlatform(plat)

	const raw = `{"id":"quarantine-order"}`
	hash, oldEntry := newRoutableEntry(t, raw, "203.0.113.130")
	pool.addEntry(hash, oldEntry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)

	oldRoute := RouteResult{
		PlatformID:    plat.ID,
		NodeHash:      hash,
		EgressIP:      oldEntry.GetEgressIP(),
		selectedEntry: oldEntry,
		platform:      plat,
	}
	newEntry := newHealthyEntryForHash(t, hash, []byte(raw), "203.0.113.131")
	newRoute := oldRoute
	newRoute.EgressIP = newEntry.GetEgressIP()
	newRoute.selectedEntry = newEntry

	checked := make(chan struct{})
	allowOld := make(chan struct{})
	var hookCalls atomic.Int32
	router.beforeResponseCooldownMarkHook = func() {
		if hookCalls.Add(1) == 1 {
			close(checked)
			<-allowOld
		}
	}
	defer func() { router.beforeResponseCooldownMarkHook = nil }()

	base := time.Now()
	oldUntil := base.Add(2 * time.Minute)
	newUntil := base.Add(time.Minute)
	oldDone := make(chan struct{})
	go func() {
		router.QuarantineRoute(oldRoute, platform.ResponseRuleScopeNode, oldUntil)
		close(oldDone)
	}()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("old quarantine did not reach the exact-entry check boundary")
	}

	pool.addEntry(hash, newEntry)
	pool.rebuildPlatformView(plat)
	router.QuarantineRoute(newRoute, platform.ResponseRuleScopeNode, newUntil)
	cooldowns := router.ensurePlatformState(plat.ID).ResponseCooldowns
	if !cooldowns.IsCoolingForEntry(hash, newEntry, newEntry.GetEgressIP(), base) {
		t.Fatal("new entry cooldown was not recorded before the stale response resumed")
	}

	close(allowOld)
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("stale old quarantine did not finish")
	}
	if !cooldowns.IsCoolingForEntry(hash, newEntry, newEntry.GetEgressIP(), base) {
		t.Fatal("late older quarantine erased the newer entry cooldown")
	}
}

func TestLateUpsertCannotRecreateRemovedPlatformState(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-late-upsert", "Plat-Late-Upsert", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	router.ensurePlatformState(plat.ID)
	pool.removePlatform(plat.ID)

	removalLockAcquired := make(chan struct{})
	allowRemoval := make(chan struct{})
	router.afterPlatformStateRemovalLockHook = func() {
		close(removalLockAcquired)
		<-allowRemoval
	}
	removeDone := make(chan struct{})
	go func() {
		router.RemovePlatformState(plat.ID)
		close(removeDone)
	}()
	select {
	case <-removalLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not acquire lifecycle write lock")
	}

	hash := node.HashFromRawOptions([]byte(`{"id":"late-upsert"}`))
	upsertDone := make(chan error, 1)
	go func() {
		upsertDone <- router.UpsertLease(model.Lease{
			PlatformID: plat.ID,
			Account:    "late-account",
			NodeHash:   hash.Hex(),
			EgressIP:   "203.0.113.101",
		})
	}()
	select {
	case err := <-upsertDone:
		t.Fatalf("late upsert bypassed the removal write lock: %v", err)
	default:
	}
	close(allowRemoval)

	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("platform removal did not finish")
	}
	select {
	case err := <-upsertDone:
		if !errors.Is(err, ErrPlatformNotFound) {
			t.Fatalf("late UpsertLease error = %v, want ErrPlatformNotFound", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late upsert did not finish after platform removal")
	}
	if _, ok := router.states.Load(plat.ID); ok {
		t.Fatal("late upsert recreated removed platform state")
	}
}

func TestLeaseEventPanicAdvancesTicketAndRethrows(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-event-panic", "Plat-Event-Panic", nil, nil)
	pool.addPlatform(plat)
	hash := node.HashFromRawOptions([]byte(`{"id":"event-panic"}`))
	ip := netip.MustParseAddr("203.0.113.95")

	secondDelivered := make(chan struct{})
	var callbackCount int
	router := newTestRouter(pool, func(event LeaseEvent) {
		callbackCount++
		if callbackCount == 1 {
			panic("lease callback failure")
		}
		if callbackCount == 2 {
			close(secondDelivered)
		}
	})

	firstReturnedPanic := make(chan any, 1)
	go func() {
		defer func() { firstReturnedPanic <- recover() }()
		if err := router.UpsertLease(model.Lease{
			PlatformID: plat.ID,
			Account:    "panic-first",
			NodeHash:   hash.Hex(),
			EgressIP:   ip.String(),
		}); err != nil {
			t.Errorf("first UpsertLease: %v", err)
		}
	}()
	select {
	case recovered := <-firstReturnedPanic:
		if recovered != "lease callback failure" {
			t.Fatalf("first callback panic = %#v, want lease callback failure", recovered)
		}
	case <-time.After(time.Second):
		t.Fatal("first UpsertLease did not rethrow callback panic")
	}

	if err := router.UpsertLease(model.Lease{
		PlatformID: plat.ID,
		Account:    "panic-second",
		NodeHash:   hash.Hex(),
		EgressIP:   ip.String(),
	}); err != nil {
		t.Fatalf("second UpsertLease: %v", err)
	}
	select {
	case <-secondDelivered:
	case <-time.After(time.Second):
		t.Fatal("event after callback panic was not delivered")
	}
}

func TestLeaseOperationsReturnOnlyAfterTheirCallbacks(t *testing.T) {
	t.Run("route", func(t *testing.T) {
		pool := newRouterTestPool()
		plat := platform.NewPlatform("plat-sync-route", "Plat-Sync-Route", nil, nil)
		plat.StickyTTLNs = int64(time.Hour)
		pool.addPlatform(plat)
		hash, entry := newRoutableEntry(t, `{"id":"sync-route"}`, "203.0.113.96")
		pool.addEntry(hash, entry)
		pool.rebuildPlatformView(plat)
		assertLeaseOperationWaitsForCallback(t, func(onEvent LeaseEventFunc) *Router {
			return NewRouter(RouterConfig{
				Pool:         pool,
				Authorities:  func() []string { return []string{"cloudflare.com"} },
				P2CWindow:    func() time.Duration { return time.Hour },
				OnLeaseEvent: onEvent,
			})
		}, func(router *Router) {
			_, err := router.RouteRequest(plat.Name, "sync-account", "https://cloudflare.com/")
			if err != nil {
				t.Errorf("RouteRequest: %v", err)
			}
		})
	})

	t.Run("upsert", func(t *testing.T) {
		pool := newRouterTestPool()
		plat := platform.NewPlatform("plat-sync-upsert", "Plat-Sync-Upsert", nil, nil)
		pool.addPlatform(plat)
		hash := node.HashFromRawOptions([]byte(`{"id":"sync-upsert"}`))
		ip := netip.MustParseAddr("203.0.113.97")
		assertLeaseOperationWaitsForCallback(t, func(onEvent LeaseEventFunc) *Router {
			return newTestRouter(pool, onEvent)
		}, func(router *Router) {
			if err := router.UpsertLease(model.Lease{
				PlatformID: plat.ID,
				Account:    "sync-account",
				NodeHash:   hash.Hex(),
				EgressIP:   ip.String(),
			}); err != nil {
				t.Errorf("UpsertLease: %v", err)
			}
		})
	})

	t.Run("delete", func(t *testing.T) {
		pool := newRouterTestPool()
		plat := platform.NewPlatform("plat-sync-delete", "Plat-Sync-Delete", nil, nil)
		pool.addPlatform(plat)
		router := newTestRouter(pool, nil)
		state := router.ensurePlatformState(plat.ID)
		state.Leases.CreateLease("sync-account", Lease{
			NodeHash: node.HashFromRawOptions([]byte(`{"id":"sync-delete"}`)),
			EgressIP: netip.MustParseAddr("203.0.113.98"),
			ExpiryNs: time.Now().Add(time.Hour).UnixNano(),
		})
		assertLeaseOperationWaitsForCallback(t, func(onEvent LeaseEventFunc) *Router {
			router.onLeaseEvent = onEvent
			return router
		}, func(router *Router) {
			if !router.DeleteLease(plat.ID, "sync-account") {
				t.Errorf("DeleteLease returned false")
			}
		})
	})
}

func assertLeaseOperationWaitsForCallback(
	t *testing.T,
	makeRouter func(LeaseEventFunc) *Router,
	run func(*Router),
) {
	t.Helper()
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	var once sync.Once
	router := makeRouter(func(LeaseEvent) {
		once.Do(func() { close(callbackStarted) })
		<-releaseCallback
		close(callbackDone)
	})

	operationDone := make(chan struct{})
	go func() {
		run(router)
		close(operationDone)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("lease callback did not start")
	}
	select {
	case <-operationDone:
		t.Fatal("operation returned before its callback completed")
	default:
	}
	close(releaseCallback)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("lease callback did not finish")
	}
	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("operation did not return after callback completion")
	}
}
