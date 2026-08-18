package service

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/Resinat/Resin/internal/topology"
)

type subscriptionPatchFixture struct {
	cp            *ControlPlaneService
	sub           *subscription.Subscription
	engine        *state.StateEngine
	closeDB       func() error
	stopScheduler func()
}

func newSubscriptionPatchFixture(t *testing.T, withScheduler bool) subscriptionPatchFixture {
	t.Helper()
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	closed := false
	closeDB := func() error {
		if closed {
			return nil
		}
		closed = true
		return closer.Close()
	}
	t.Cleanup(func() { _ = closeDB() })

	subMgr := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 4,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return time.Minute },
	})
	sub := subscription.NewSubscription("sub-patch", "initial", "https://example.com/sub", true, false)
	sub.SetFetchConfig(sub.URL(), time.Minute.Nanoseconds())
	subMgr.Register(sub)
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               sub.ID,
		Name:             sub.Name(),
		SourceType:       sub.SourceType(),
		URL:              sub.URL(),
		UpdateIntervalNs: sub.UpdateIntervalNs(),
		Enabled:          sub.Enabled(),
		CreatedAtNs:      sub.CreatedAtNs,
		UpdatedAtNs:      sub.UpdatedAtNs,
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	var scheduler *topology.SubscriptionScheduler
	stopScheduler := func() {}
	if withScheduler {
		scheduler = topology.NewSubscriptionScheduler(topology.SchedulerConfig{
			SubManager: subMgr,
			Pool:       pool,
		})
		var stopOnce sync.Once
		stopScheduler = func() { stopOnce.Do(scheduler.Stop) }
		t.Cleanup(stopScheduler)
	}
	return subscriptionPatchFixture{
		cp: &ControlPlaneService{
			Engine:    engine,
			Pool:      pool,
			SubMgr:    subMgr,
			Scheduler: scheduler,
		},
		sub:           sub,
		engine:        engine,
		closeDB:       closeDB,
		stopScheduler: stopScheduler,
	}
}

func waitSubscriptionMutationGroup(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription PATCH goroutines did not finish")
	}
}

func TestUpdateSubscription_ConcurrentPatchesDoNotLoseFields(t *testing.T) {
	f := newSubscriptionPatchFixture(t, false)
	firstLoaded := make(chan struct{})
	secondBeforeLock := make(chan struct{})
	secondLoaded := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var releaseFirstOnce sync.Once
	var releaseSecondOnce sync.Once
	defer releaseFirstOnce.Do(func() { close(releaseFirst) })
	defer releaseSecondOnce.Do(func() { close(releaseSecond) })

	var (
		beforeLockCalls atomic.Int32
		loadCalls       atomic.Int32
	)
	f.cp.subscriptionMutationHook = func(stage subscriptionMutationStage) {
		switch stage {
		case subscriptionMutationBeforeLock:
			if beforeLockCalls.Add(1) == 2 {
				close(secondBeforeLock)
			}
		case subscriptionMutationAfterLoad:
			switch loadCalls.Add(1) {
			case 1:
				close(firstLoaded)
				<-releaseFirst
			case 2:
				close(secondLoaded)
				<-releaseSecond
			}
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	startPatch := func(patch string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(patch))
			errCh <- err
		}()
	}

	startPatch(`{"name":"renamed"}`)
	select {
	case <-firstLoaded:
	case <-time.After(time.Second):
		t.Fatal("first subscription PATCH did not reach its snapshot")
	}

	startPatch(`{"enabled":false}`)
	select {
	case <-secondBeforeLock:
	case <-time.After(time.Second):
		t.Fatal("second subscription PATCH did not reach beforeLock")
	}

	// This is a negative ordering assertion, with a deadline only as a test
	// failure bound: while the first request is held in afterLoad, the second
	// request must not reach afterLoad. The old implementation reaches it.
	reachedAfterLoadBeforeFirstRelease := false
	select {
	case <-secondLoaded:
		reachedAfterLoadBeforeFirstRelease = true
	case <-time.After(time.Second):
	}
	releaseFirstOnce.Do(func() { close(releaseFirst) })
	select {
	case <-secondLoaded:
	case <-time.After(time.Second):
		t.Fatal("second PATCH did not reach afterLoad after first PATCH committed")
	}
	releaseSecondOnce.Do(func() { close(releaseSecond) })
	waitSubscriptionMutationGroup(t, &wg)
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("UpdateSubscription: %v", err)
		}
	}

	if got := f.sub.Name(); got != "renamed" {
		t.Fatalf("concurrent patches lost name: got %q", got)
	}
	if f.sub.Enabled() {
		t.Fatal("concurrent patches lost enabled=false")
	}
	rows, err := f.engine.ListSubscriptions()
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "renamed" || rows[0].Enabled {
		t.Fatalf("persisted concurrent patch result lost a field: %+v", rows)
	}
	if reachedAfterLoadBeforeFirstRelease {
		t.Fatal("second PATCH reached afterLoad before first PATCH released opMu")
	}
}

func TestSubscriptionResponseDoesNotMixConcurrentPatchGeneration(t *testing.T) {
	f := newSubscriptionPatchFixture(t, false)
	nameRead := make(chan struct{})
	allowResponse := make(chan struct{})
	allowMutation := make(chan struct{})
	var nameReadOnce sync.Once
	f.cp.afterSubscriptionNameReadHook = func() {
		nameReadOnce.Do(func() {
			close(nameRead)
			<-allowResponse
		})
	}
	defer func() {
		select {
		case <-allowResponse:
		default:
			close(allowResponse)
		}
		select {
		case <-allowMutation:
		default:
			close(allowMutation)
		}
		f.cp.afterSubscriptionNameReadHook = nil
		f.cp.afterSubscriptionRuntimeMutationHook = nil
		f.cp.subscriptionMutationHook = nil
	}()

	mutationApplied := make(chan struct{})
	var mutationOnce sync.Once
	f.cp.afterSubscriptionRuntimeMutationHook = func() {
		mutationOnce.Do(func() {
			close(mutationApplied)
			<-allowMutation
		})
	}
	type readResult struct {
		response *SubscriptionResponse
		err      error
	}
	readDone := make(chan readResult, 1)
	go func() {
		response, err := f.cp.GetSubscription(f.sub.ID)
		readDone <- readResult{response: response, err: err}
	}()
	select {
	case <-nameRead:
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubscription did not pause after reading the old name")
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{
			"name":"updated-name",
			"url":"https://updated.example/sub",
			"enabled":false,
			"update_interval":"1m"
		}`))
		updateDone <- err
	}()
	select {
	case <-mutationApplied:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription PATCH did not reach its post-mutation gate")
	}

	close(allowResponse)
	var got readResult
	select {
	case got = <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubscription did not finish after releasing its snapshot")
	}
	if got.err != nil {
		t.Fatalf("GetSubscription: %v", got.err)
	}
	if got.response == nil {
		t.Fatal("GetSubscription returned nil response")
	}
	if got.response.Name != "initial" || got.response.URL != "https://example.com/sub" ||
		got.response.Enabled != true || got.response.UpdateInterval != time.Minute.String() {
		t.Fatalf("mixed subscription response: %+v", *got.response)
	}

	close(allowMutation)
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateSubscription: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("UpdateSubscription did not finish after response snapshot release")
	}
}

func TestListSubscriptionsEnabledFilterUsesSamePatchSnapshot(t *testing.T) {
	f := newSubscriptionPatchFixture(t, false)
	nameRead := make(chan struct{})
	allowResponse := make(chan struct{})
	var nameReadOnce sync.Once
	f.cp.afterSubscriptionNameReadHook = func() {
		nameReadOnce.Do(func() {
			close(nameRead)
			<-allowResponse
		})
	}
	mutationApplied := make(chan struct{})
	allowMutation := make(chan struct{})
	var mutationOnce sync.Once
	f.cp.afterSubscriptionRuntimeMutationHook = func() {
		mutationOnce.Do(func() {
			close(mutationApplied)
			<-allowMutation
		})
	}
	defer func() {
		select {
		case <-allowResponse:
		default:
			close(allowResponse)
		}
		select {
		case <-allowMutation:
		default:
			close(allowMutation)
		}
		f.cp.afterSubscriptionNameReadHook = nil
		f.cp.afterSubscriptionRuntimeMutationHook = nil
	}()

	listDone := make(chan struct{})
	var (
		got  []SubscriptionResponse
		lerr error
	)
	go func() {
		enabled := true
		got, lerr = f.cp.ListSubscriptions(&enabled)
		close(listDone)
	}()
	select {
	case <-nameRead:
	case <-time.After(2 * time.Second):
		t.Fatal("ListSubscriptions did not reach its response snapshot")
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"enabled":false}`))
		updateDone <- err
	}()
	select {
	case <-mutationApplied:
	case <-time.After(2 * time.Second):
		t.Fatal("enabled PATCH did not reach its post-mutation gate")
	}

	close(allowResponse)
	select {
	case <-listDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ListSubscriptions did not finish after releasing its snapshot")
	}
	if lerr != nil {
		t.Fatalf("ListSubscriptions: %v", lerr)
	}
	if len(got) != 1 || !got[0].Enabled {
		t.Fatalf("enabled list mixed its filter and response snapshots: %+v", got)
	}

	close(allowMutation)
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateSubscription: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("UpdateSubscription did not finish after list snapshot release")
	}
}

func TestSubscriptionResponseSnapshotDoesNotInvertRuntimeAndOperationLocks(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	readerEntered := make(chan struct{})
	allowReader := make(chan struct{})
	var readerOnce sync.Once
	f.cp.afterRuntimeReadLockHook = func() {
		readerOnce.Do(func() {
			close(readerEntered)
			<-allowReader
		})
	}
	writerRuntimeAttempted := make(chan struct{})
	var writerOnce sync.Once
	f.cp.afterSubscriptionPersistHook = func() {
		writerOnce.Do(func() {
			close(writerRuntimeAttempted)
			// The real PATCH immediately performs a runtime mutation after this
			// hook. Enter the same pool write owner here so the test can observe
			// that the PATCH already owns sub.opMu while runtime write admission
			// is waiting for the reader.
			f.cp.Pool.WithRuntimeMutation(func() {})
		})
	}
	defer func() {
		select {
		case <-allowReader:
		default:
			close(allowReader)
		}
		f.cp.afterRuntimeReadLockHook = nil
		f.cp.afterSubscriptionPersistHook = nil
	}()

	getDone := make(chan error, 1)
	go func() {
		_, err := f.cp.GetSubscription(f.sub.ID)
		getDone <- err
	}()
	select {
	case <-readerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubscription did not acquire runtime read owner")
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"enabled":false}`))
		updateDone <- err
	}()
	select {
	case <-writerRuntimeAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("PATCH did not reach its runtime owner")
	}

	// Once the read admission hook returns, a correct response no longer
	// acquires sub.opMu. It completes and releases runtimeBatchMu, allowing
	// the PATCH's already-admitted runtime write to finish.
	close(allowReader)
	select {
	case err := <-getDone:
		if err != nil {
			t.Fatalf("GetSubscription: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubscription deadlocked with PATCH runtime write")
	}
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateSubscription: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PATCH did not finish after response released runtime read")
	}
}

func TestListSubscriptionSnapshotDoesNotInvertRuntimeAndOperationLocks(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	readerEntered := make(chan struct{})
	allowReader := make(chan struct{})
	var readerOnce sync.Once
	f.cp.afterRuntimeReadLockHook = func() {
		readerOnce.Do(func() {
			close(readerEntered)
			<-allowReader
		})
	}
	writerRuntimeAttempted := make(chan struct{})
	var writerOnce sync.Once
	f.cp.afterSubscriptionPersistHook = func() {
		writerOnce.Do(func() {
			close(writerRuntimeAttempted)
			f.cp.Pool.WithRuntimeMutation(func() {})
		})
	}
	defer func() {
		select {
		case <-allowReader:
		default:
			close(allowReader)
		}
		f.cp.afterRuntimeReadLockHook = nil
		f.cp.afterSubscriptionPersistHook = nil
	}()

	listDone := make(chan error, 1)
	go func() {
		got, err := f.cp.ListSubscriptions(nil)
		if err == nil && len(got) != 1 {
			err = fmt.Errorf("ListSubscriptions returned %d rows, want 1", len(got))
		}
		listDone <- err
	}()
	select {
	case <-readerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("ListSubscriptions did not acquire runtime read owner")
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"enabled":false}`))
		updateDone <- err
	}()
	select {
	case <-writerRuntimeAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("PATCH did not reach its runtime owner")
	}

	close(allowReader)
	select {
	case err := <-listDone:
		if err != nil {
			t.Fatalf("ListSubscriptions: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListSubscriptions deadlocked with PATCH runtime write")
	}
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateSubscription: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PATCH did not finish after list released runtime read")
	}
}

func TestUpdateSubscription_NameAndEnabledPatchDoesNotDeadlock(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	done := make(chan struct{})
	var (
		response *SubscriptionResponse
		err      error
	)
	go func() {
		response, err = f.cp.UpdateSubscription(f.sub.ID, []byte(`{"name":"renamed","enabled":false}`))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("name/enabled PATCH deadlocked while holding subscription opMu")
	}
	if err != nil {
		t.Fatalf("UpdateSubscription: %v", err)
	}
	if response == nil || response.Name != "renamed" || response.Enabled {
		t.Fatalf("unexpected PATCH response: %+v", response)
	}
	if f.sub.Name() != "renamed" || f.sub.Enabled() {
		t.Fatalf("runtime mutation not applied: name=%q enabled=%v", f.sub.Name(), f.sub.Enabled())
	}
}

func TestUpdateSubscription_ReenableSideEffectIsJoinedBySchedulerStop(t *testing.T) {
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
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 4,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	sub := subscription.NewSubscription("sub-reenable-stop", "reenable-stop", "https://example.com/sub", false, false)
	sub.SetFetchConfig(sub.URL(), time.Minute.Nanoseconds())
	subMgr.Register(sub)
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               sub.ID,
		Name:             sub.Name(),
		SourceType:       sub.SourceType(),
		URL:              sub.URL(),
		Enabled:          false,
		UpdateIntervalNs: sub.UpdateIntervalNs(),
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	raw := []byte(`{"type":"reenable-stop"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"reenable"}})

	reenableEntered := make(chan struct{})
	allowReenable := make(chan struct{})
	var reenableOnce sync.Once
	var allowReenableOnce sync.Once
	releaseReenable := func() {
		allowReenableOnce.Do(func() { close(allowReenable) })
	}
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		OnSubReenabledNode: func(node.Hash, *node.NodeEntry) {
			reenableOnce.Do(func() { close(reenableEntered) })
			<-allowReenable
		},
	})
	t.Cleanup(func() {
		releaseReenable()
		scheduler.Stop()
	})
	scheduler.Start()

	cp := &ControlPlaneService{
		Engine:    engine,
		Pool:      pool,
		SubMgr:    subMgr,
		Scheduler: scheduler,
	}
	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdateSubscription(sub.ID, []byte(`{"enabled":true}`))
		updateDone <- updateErr
	}()
	select {
	case <-reenableEntered:
	case <-time.After(time.Second):
		t.Fatal("real subscription PATCH did not reach the re-enable runtime callback")
	}

	stopDone := make(chan struct{})
	go func() {
		scheduler.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("scheduler Stop returned while a service re-enable side effect was still running")
	case <-time.After(100 * time.Millisecond):
	}

	releaseReenable()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateSubscription: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UpdateSubscription did not finish after re-enable callback release")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("scheduler Stop did not finish after re-enable callback release")
	}
}

func TestUpdateSubscription_RejectsAfterSchedulerStopBeforePersistence(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	oldName := f.sub.Name()
	oldEnabled := f.sub.Enabled()
	f.stopScheduler()

	_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"name":"stopped-update","enabled":false}`))
	if err == nil {
		t.Fatal("UpdateSubscription unexpectedly succeeded after scheduler Stop")
	}
	assertServiceErrorCode(t, err, "INTERNAL")
	if f.sub.Name() != oldName || f.sub.Enabled() != oldEnabled {
		t.Fatalf("runtime subscription changed after stopped scheduler rejection: name=%q enabled=%v", f.sub.Name(), f.sub.Enabled())
	}
	rows, listErr := f.engine.ListSubscriptions()
	if listErr != nil {
		t.Fatalf("ListSubscriptions: %v", listErr)
	}
	for _, row := range rows {
		if row.ID == f.sub.ID && (row.Name != oldName || row.Enabled != oldEnabled) {
			t.Fatalf("stopped scheduler rejection persisted mutation: name=%q enabled=%v", row.Name, row.Enabled)
		}
	}
}

func TestUpdateSubscription_PersistFailureLeavesRuntimeUnchanged(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	oldName := f.sub.Name()
	oldURL := f.sub.URL()
	oldEnabled := f.sub.Enabled()
	oldUpdatedAt := f.sub.UpdatedAtNs
	if err := f.closeDB(); err != nil {
		t.Fatalf("close state database: %v", err)
	}

	_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"name":"should-not-apply","enabled":false,"url":"https://example.com/other"}`))
	if err == nil {
		t.Fatal("UpdateSubscription succeeded after the database was closed")
	}
	assertServiceErrorCode(t, err, "INTERNAL")
	if f.sub.Name() != oldName || f.sub.URL() != oldURL || f.sub.Enabled() != oldEnabled || f.sub.UpdatedAtNs != oldUpdatedAt {
		t.Fatalf("runtime changed after persistence failure: name=%q url=%q enabled=%v updated=%d", f.sub.Name(), f.sub.URL(), f.sub.Enabled(), f.sub.UpdatedAtNs)
	}
}

func TestUpdateSubscription_HoldsStateWriteAdmissionThroughRuntimePublish(t *testing.T) {
	f := newSubscriptionPatchFixture(t, false)
	persisted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	f.cp.afterSubscriptionPersistHook = func() {
		close(persisted)
		<-release
	}

	updateDone := make(chan error, 1)
	go func() {
		_, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"name":"shutdown-safe"}`))
		updateDone <- err
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("subscription update did not reach the post-persist runtime boundary")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	admissionDone := make(chan error, 1)
	go func() { admissionDone <- f.engine.CloseStateWriteAdmissionAndWait(closeCtx) }()
	<-closeCtx.Done()
	select {
	case err := <-admissionDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("state write admission closed before runtime publish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("state write admission waiter did not honor its deadline")
	}

	releaseOnce.Do(func() { close(release) })
	if err := <-updateDone; err != nil {
		t.Fatalf("UpdateSubscription: %v", err)
	}
	if err := f.engine.CloseStateWriteAdmissionAndWait(context.Background()); err != nil {
		t.Fatalf("final state write admission wait: %v", err)
	}
	if f.sub.Name() != "shutdown-safe" {
		t.Fatalf("runtime subscription mutation did not complete: %q", f.sub.Name())
	}
}

func TestCreateSubscription_HoldsStateWriteAdmissionThroughRuntimePublish(t *testing.T) {
	f := newSubscriptionPatchFixture(t, false)
	persisted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	f.cp.afterSubscriptionPersistHook = func() {
		close(persisted)
		<-release
	}

	name := "shutdown-create-safe"
	content := `{"outbounds":[]}`
	type createResult struct {
		response *SubscriptionResponse
		err      error
	}
	createDone := make(chan createResult, 1)
	go func() {
		response, err := f.cp.CreateSubscription(CreateSubscriptionRequest{
			Name:       &name,
			SourceType: func() *string { v := subscription.SourceTypeLocal; return &v }(),
			Content:    &content,
		})
		createDone <- createResult{response: response, err: err}
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("subscription create did not reach the post-persist runtime boundary")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	admissionDone := make(chan error, 1)
	go func() { admissionDone <- f.engine.CloseStateWriteAdmissionAndWait(closeCtx) }()
	<-closeCtx.Done()
	select {
	case err := <-admissionDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("state write admission closed before runtime registration: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("state write admission waiter did not honor its deadline")
	}

	releaseOnce.Do(func() { close(release) })
	created := <-createDone
	if created.err != nil {
		t.Fatalf("CreateSubscription: %v", created.err)
	}
	if f.engine.CloseStateWriteAdmissionAndWait(context.Background()) != nil {
		t.Fatal("final state write admission wait failed")
	}
	if created.response == nil || f.cp.SubMgr.Lookup(created.response.ID) == nil {
		t.Fatal("runtime subscription registration did not complete")
	}
	if f.cp.SubMgr.Size() != 2 {
		t.Fatalf("runtime subscription registration did not complete: size=%d", f.cp.SubMgr.Size())
	}
}

func TestSubscriptionMutationsPublishAfterRequestCancellationAtCommitBoundary(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		f := newSubscriptionPatchFixture(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.cp.afterSubscriptionPersistHook = cancel

		name := "cancel-after-create-commit"
		content := `{"outbounds":[]}`
		response, err := f.cp.CreateSubscriptionContext(ctx, CreateSubscriptionRequest{
			Name:       &name,
			SourceType: func() *string { v := subscription.SourceTypeLocal; return &v }(),
			Content:    &content,
		})
		if err != nil {
			t.Fatalf("CreateSubscriptionContext: %v", err)
		}
		if response == nil || f.cp.SubMgr.Lookup(response.ID) == nil {
			t.Fatalf("created subscription was not published: response=%+v", response)
		}
	})

	t.Run("update", func(t *testing.T) {
		f := newSubscriptionPatchFixture(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.cp.afterSubscriptionPersistHook = cancel

		response, err := f.cp.UpdateSubscriptionContext(ctx, f.sub.ID, []byte(`{"name":"cancel-after-update-commit"}`))
		if err != nil {
			t.Fatalf("UpdateSubscriptionContext: %v", err)
		}
		if response == nil || f.sub.Name() != "cancel-after-update-commit" {
			t.Fatalf("updated subscription was not published: response=%+v name=%q", response, f.sub.Name())
		}
	})

	t.Run("delete", func(t *testing.T) {
		f := newSubscriptionPatchFixture(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.cp.afterSubscriptionPersistHook = cancel

		if err := f.cp.DeleteSubscriptionContext(ctx, f.sub.ID); err != nil {
			t.Fatalf("DeleteSubscriptionContext: %v", err)
		}
		if f.cp.SubMgr.Lookup(f.sub.ID) != nil {
			t.Fatal("deleted subscription remained published in runtime")
		}
		rows, err := f.engine.ListSubscriptions()
		if err != nil {
			t.Fatalf("ListSubscriptions: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("deleted subscription remained in state: %+v", rows)
		}
	})
}

func TestUpdateSubscription_AsyncRefreshIsJoinedBySchedulerStop(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	f.cp.Scheduler.Fetcher = func(context.Context, string) ([]byte, error) {
		close(fetchStarted)
		<-releaseFetch
		return []byte(`{"outbounds":[{"type":"shadowsocks","tag":"async-node","server":"1.1.1.1","server_port":443}]}`), nil
	}

	if _, err := f.cp.UpdateSubscription(f.sub.ID, []byte(`{"url":"https://example.com/async"}`)); err != nil {
		t.Fatalf("UpdateSubscription: %v", err)
	}
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("async refresh did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		f.stopScheduler()
		close(stopDone)
	}()
	returnedBeforeRefresh := false
	select {
	case <-stopDone:
		returnedBeforeRefresh = true
	case <-time.After(time.Second):
	}
	close(releaseFetch)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("scheduler Stop did not return after async refresh completed")
	}
	if returnedBeforeRefresh {
		t.Fatal("scheduler Stop returned while service-started refresh was still running")
	}
}

func TestRefreshSubscription_ReportsStoppedScheduler(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	var fetchCalls atomic.Int32
	f.cp.Scheduler.Fetcher = func(context.Context, string) ([]byte, error) {
		fetchCalls.Add(1)
		return []byte(`{"outbounds":[]}`), nil
	}

	// Refresh is a synchronous control-plane action. Once the scheduler has
	// closed its admission, reporting success would claim a refresh that never
	// reached the Fetcher.
	f.stopScheduler()
	err := f.cp.RefreshSubscription(f.sub.ID)
	if err == nil {
		t.Fatal("RefreshSubscription unexpectedly succeeded after scheduler Stop")
	}
	assertServiceErrorCode(t, err, "INTERNAL")
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("stopped scheduler unexpectedly fetched subscription: %d calls", got)
	}
}

func TestRefreshSubscription_ReportsSynchronousFetchFailure(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	f.cp.Scheduler.Fetcher = func(context.Context, string) ([]byte, error) {
		return nil, errors.New("upstream unavailable")
	}

	err := f.cp.RefreshSubscription(f.sub.ID)
	if err == nil {
		t.Fatal("RefreshSubscription reported success after the synchronous fetch failed")
	}
	assertServiceErrorCode(t, err, "INTERNAL")
	if got := f.sub.GetLastError(); got != "upstream unavailable" {
		t.Fatalf("LastError = %q, want the fetch failure", got)
	}
}

func TestRefreshSubscription_ReportsRejectedPersistenceMutation(t *testing.T) {
	f := newSubscriptionPatchFixture(t, true)
	raw := []byte(`{"type":"shadowsocks","tag":"refresh-rejected","server":"1.1.1.1","server_port":443}`)
	f.cp.Scheduler = topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: f.cp.SubMgr,
		Pool:       f.cp.Pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return []byte(`{"outbounds":[{"type":"shadowsocks","tag":"refresh-rejected","server":"1.1.1.1","server_port":443}]}`), nil
		},
		RunRefreshMutation: func(func(topology.PersistenceAdmission)) bool {
			return false
		},
	})

	err := f.cp.RefreshSubscription(f.sub.ID)
	if err == nil {
		t.Fatal("RefreshSubscription reported success after persistence admission rejected the mutation")
	}
	assertServiceErrorCode(t, err, "INTERNAL")
	if _, ok := f.cp.Pool.GetEntry(node.HashFromRawOptions(raw)); ok {
		t.Fatal("rejected refresh published a runtime node")
	}
}

func TestDeleteSubscription_DropsInFlightRefreshResult(t *testing.T) {
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
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 4,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return time.Minute },
	})
	sub := subscription.NewSubscription("sub-delete-refresh", "delete-refresh", "https://example.com/sub", true, false)
	sub.SetFetchConfig(sub.URL(), int64(time.Minute))
	subMgr.Register(sub)
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               sub.ID,
		Name:             sub.Name(),
		URL:              sub.URL(),
		UpdateIntervalNs: sub.UpdateIntervalNs(),
		Enabled:          true,
		CreatedAtNs:      time.Now().UnixNano(),
		UpdatedAtNs:      time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			close(fetchStarted)
			<-releaseFetch
			return []byte(`{"outbounds":[{"type":"shadowsocks","tag":"late-node","server":"1.1.1.1","server_port":443}]}`), nil
		},
	})
	t.Cleanup(scheduler.Stop)
	cp := &ControlPlaneService{
		Engine:    engine,
		Pool:      pool,
		SubMgr:    subMgr,
		Scheduler: scheduler,
	}

	refreshDone := make(chan error, 1)
	go func() { refreshDone <- cp.RefreshSubscription(sub.ID) }()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("refresh fetch did not start")
	}

	if err := cp.DeleteSubscription(sub.ID); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	close(releaseFetch)
	if err := <-refreshDone; err != nil {
		t.Fatalf("RefreshSubscription: %v", err)
	}

	if _, ok := subMgr.Get(sub.ID); ok {
		t.Fatal("deleted subscription was reintroduced into the manager")
	}
	rows, err := engine.ListSubscriptions()
	if err != nil {
		t.Fatalf("ListSubscriptions after delete: %v", err)
	}
	for _, row := range rows {
		if row.ID == sub.ID {
			t.Fatal("deleted subscription remained persisted")
		}
	}
	if got := pool.Size(); got != 0 {
		t.Fatalf("in-flight refresh repopulated pool after delete: size=%d", got)
	}
}
