package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

// This is the production shutdown path: LeaseCleaner has already removed the
// lease in Router state, but its synchronous persistence callback is still in
// flight when the bounded app shutdown returns. The callback must complete
// before dirty admission closes and the final cache flush runs.
func TestShutdownTracksLeaseCleanerContinuationBeforeCacheFlush(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	const platformID = "shutdown-lease-cleaner-platform"
	const account = "shutdown-lease-cleaner-account"
	lease := model.Lease{
		PlatformID:     platformID,
		Account:        account,
		NodeHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EgressIP:       "203.0.113.241",
		CreatedAtNs:    time.Now().Add(-time.Hour).UnixNano(),
		ExpiryNs:       time.Now().Add(-time.Minute).UnixNano(),
		LastAccessedNs: time.Now().Add(-time.Minute).UnixNano(),
	}

	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	plat := platform.NewPlatform(platformID, platformID, nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if got, ok := pool.GetPlatform(platformID); !ok || got != plat {
		t.Fatalf("registered platform lookup = (%p, %v), want (%p, true)", got, ok, plat)
	}
	probeLease := lease
	probeLease.Account = "shutdown-lease-cleaner-router-order"
	probeLease.ExpiryNs = time.Now().Add(time.Hour).UnixNano()

	callbackEntered := make(chan struct{})
	allowCallback := make(chan struct{})
	markResult := make(chan bool, 1)
	var callbackOnce sync.Once
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
		OnLeaseEvent: func(event routing.LeaseEvent) {
			if event.Type != routing.LeaseExpire {
				return
			}
			callbackOnce.Do(func() { close(callbackEntered) })
			<-allowCallback
			markResult <- engine.MarkLeaseDelete(event.PlatformID, event.Account)
		},
	})
	router.RestoreLeases([]model.Lease{lease, probeLease})
	if got := router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: account}); got == nil {
		t.Fatal("expired lease was not restored into Router state")
	}
	readers := newFlushReaders(pool, subManager, router)
	if !engine.MarkLease(platformID, account) {
		t.Fatal("initial lease dirty mark was rejected")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("initial lease flush: %v", err)
	}

	cleaner := routing.NewLeaseCleanerWithIntervals(router, time.Millisecond, 0)
	cleaner.Start()
	defer func() {
		select {
		case <-allowCallback:
		default:
			close(allowCallback)
		}
		_ = cleaner.StopContext(context.Background())
	}()

	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("real lease cleaner did not reach the lease-expire callback")
	}

	flushWorker := state.NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	app := &resinApp{
		stateEngine: engine,
		flushWorker: flushWorker,
		topoRuntime: &topologyRuntime{
			leaseCleaner: cleaner,
			router:       router,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before the lease-cleaner deadline")
	case <-ctx.Done():
	}

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after its caller deadline")
	}

	continuationDone := make(chan error, 1)
	go func() { continuationDone <- continuations.wait() }()
	select {
	case err := <-continuationDone:
		t.Fatalf("shutdown continuation completed while lease event was blocked: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	// The bounded shutdown call must not stop Router before the admitted
	// cleaner callback has completed. This pre-restored lease has no dirty
	// event of its own, so a pure read observes the required cleaner -> Router
	// ordering without waiting for a later lease-event ticket.
	if got := router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: probeLease.Account}); got == nil {
		t.Fatal("router stopped before lease-cleaner continuation completed")
	}

	close(allowCallback)
	select {
	case accepted := <-markResult:
		if !accepted {
			t.Fatal("admitted lease-expire dirty mark was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("lease-expire dirty mark did not finish")
	}
	select {
	case err := <-continuationDone:
		if err != nil {
			t.Fatalf("shutdown continuation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown continuation did not finish after lease callback release")
	}

	leases, err := engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("expired lease was resurrected by shutdown flush: %+v", leases)
	}
	if engine.MarkLease(platformID, account) {
		t.Fatal("dirty admission remained open after shutdown")
	}
	if err := router.UpsertLease(lease); !errors.Is(err, routing.ErrRouterStopped) {
		t.Fatalf("post-shutdown UpsertLease error = %v, want %v", err, routing.ErrRouterStopped)
	}
	if got := router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: probeLease.Account}); got != nil {
		t.Fatal("router remained readable after shutdown")
	}
}
