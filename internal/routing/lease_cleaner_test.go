package routing

import (
	"context"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func TestLeaseCleaner_StopWaitsForInFlightSweep(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-stop", "Plat-Stop", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)

	cleaner := newLeaseCleanerWithIntervals(router, time.Millisecond, 0)

	started := make(chan struct{})
	release := make(chan struct{})
	cleaner.sweepHook = func() {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
	}

	cleaner.Start()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("sweep did not start in time")
	}

	stopDone := make(chan struct{})
	go func() {
		cleaner.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop returned before in-flight sweep completed")
	case <-time.After(30 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after in-flight sweep completed")
	}
}

// TestLeaseCleaner_StopContextHonorsShutdownDeadlineWhileSweepIsAdmitted is
// the production shutdown contract. The sweep is the real worker path; the
// gate only fixes the point at which its Router lifecycle read is admitted.
func TestLeaseCleaner_StopContextHonorsShutdownDeadlineWhileSweepIsAdmitted(t *testing.T) {
	router := newTestRouter(newRouterTestPool(), nil)
	cleaner := newLeaseCleanerWithIntervals(router, time.Millisecond, 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	cleaner.sweepHook = func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}

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
		t.Fatal("lease cleaner sweep did not enter the admitted lifecycle boundary")
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

	close(release)
	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("sweep did not finish after its release gate")
	}
	if err := cleaner.StopContext(context.Background()); err != nil {
		t.Fatalf("background StopContext: %v", err)
	}
}

func TestLeaseCleaner_StopWaitsForConcurrentStartAdmission(t *testing.T) {
	router := newTestRouter(newRouterTestPool(), nil)
	cleaner := newLeaseCleanerWithIntervals(router, time.Millisecond, 0)
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

func TestLeaseCleaner_StopBeforeStartRejectsLaterStart(t *testing.T) {
	router := newTestRouter(newRouterTestPool(), nil)
	cleaner := newLeaseCleanerWithIntervals(router, time.Millisecond, 0)
	var startCalls atomic.Int32
	var sweepCalls atomic.Int32
	cleaner.beforeStartAdmissionHook = func() {
		startCalls.Add(1)
	}
	cleaner.sweepHook = func() {
		sweepCalls.Add(1)
	}

	cleaner.Stop()
	cleaner.Start()
	cleaner.Stop()

	if got := startCalls.Load(); got != 0 {
		t.Fatalf("Start after Stop entered admission hook %d times", got)
	}
	if got := sweepCalls.Load(); got != 0 {
		t.Fatalf("Start after Stop launched a sweep %d times", got)
	}
}

func TestLeaseCleaner_SweepPlatformsInParallel(t *testing.T) {
	oldMaxProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	pool := newRouterTestPool()
	platA := platform.NewPlatform("plat-a", "Plat-A", nil, nil)
	platB := platform.NewPlatform("plat-b", "Plat-B", nil, nil)
	pool.addPlatform(platA)
	pool.addPlatform(platB)

	releaseWorkers := make(chan struct{})
	allWorkersStarted := make(chan struct{})
	var workerCount atomic.Int32
	var eventCount atomic.Int32
	router := newTestRouter(pool, func(e LeaseEvent) {
		if e.Type != LeaseExpire {
			return
		}
		eventCount.Add(1)
	})

	now := time.Now()
	stateA, _ := router.states.LoadOrCompute(platA.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})
	stateB, _ := router.states.LoadOrCompute(platB.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})

	stateA.Leases.CreateLease("acct-a", Lease{
		NodeHash:       node.HashFromRawOptions([]byte(`{"id":"lease-a"}`)),
		EgressIP:       netip.MustParseAddr("203.0.113.10"),
		CreatedAtNs:    now.Add(-2 * time.Minute).UnixNano(),
		ExpiryNs:       now.Add(-1 * time.Minute).UnixNano(),
		LastAccessedNs: now.Add(-2 * time.Minute).UnixNano(),
	})
	stateB.Leases.CreateLease("acct-b", Lease{
		NodeHash:       node.HashFromRawOptions([]byte(`{"id":"lease-b"}`)),
		EgressIP:       netip.MustParseAddr("203.0.113.11"),
		CreatedAtNs:    now.Add(-2 * time.Minute).UnixNano(),
		ExpiryNs:       now.Add(-1 * time.Minute).UnixNano(),
		LastAccessedNs: now.Add(-2 * time.Minute).UnixNano(),
	})

	cleaner := NewLeaseCleaner(router)
	cleaner.sweepPlatformStateHook = func() {
		if workerCount.Add(1) == 2 {
			close(allWorkersStarted)
		}
		<-releaseWorkers
	}
	done := make(chan struct{})
	go func() {
		cleaner.sweep()
		close(done)
	}()

	select {
	case <-allWorkersStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected platform lease workers to run in parallel")
	}

	select {
	case <-done:
		t.Fatal("sweep should wait for in-flight platform sweeps")
	default:
	}

	close(releaseWorkers)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sweep did not finish after releasing lease-expire handlers")
	}

	if got := eventCount.Load(); got != 2 {
		t.Fatalf("expected 2 LeaseExpire events, got %d", got)
	}
}

func TestLeaseCleaner_ExpiresLeaseAtExactDeadline(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-exact-expiry", "Plat-Exact-Expiry", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	state, _ := router.states.LoadOrCompute(plat.ID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})

	now := time.Unix(1_700_000_000, 123).UTC()
	state.Leases.CreateLease("exact-expiry", Lease{
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"exact-expiry"}`)),
		EgressIP:    netip.MustParseAddr("203.0.113.12"),
		CreatedAtNs: now.Add(-time.Minute).UnixNano(),
		ExpiryNs:    now.UnixNano(),
	})

	cleaner := NewLeaseCleaner(router)
	cleaner.sweepPlatformState(plat.ID, state, now.UnixNano())
	if _, ok := state.Leases.GetLease("exact-expiry"); ok {
		t.Fatal("lease must be removed at its exact deadline")
	}
	if got := state.IPLoadStats.Get(netip.MustParseAddr("203.0.113.12")); got != 0 {
		t.Fatalf("expired lease IP load = %d, want 0", got)
	}
}
