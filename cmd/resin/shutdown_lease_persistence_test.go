package main

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

func TestShutdownFinalCacheFlushPreservesActiveLeaseAfterRouterStops(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	plat := platform.NewPlatform("shutdown-lease-persist", "shutdown-lease-persist", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
		OnLeaseEvent: func(event routing.LeaseEvent) {
			switch event.Type {
			case routing.LeaseCreate, routing.LeaseTouch, routing.LeaseReplace:
				engine.MarkLease(event.PlatformID, event.Account)
			case routing.LeaseRemove, routing.LeaseExpire:
				engine.MarkLeaseDelete(event.PlatformID, event.Account)
			}
		},
	})

	lease := model.Lease{
		PlatformID:     plat.ID,
		Account:        "shutdown-account",
		NodeHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EgressIP:       "203.0.113.240",
		CreatedAtNs:    time.Now().UnixNano(),
		ExpiryNs:       time.Now().Add(time.Hour).UnixNano(),
		LastAccessedNs: time.Now().UnixNano(),
	}
	if err := router.UpsertLease(lease); err != nil {
		t.Fatalf("initial UpsertLease: %v", err)
	}
	readers := newFlushReaders(pool, subManager, router)
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("initial lease flush: %v", err)
	}

	lease.LastAccessedNs++
	if err := router.UpsertLease(lease); err != nil {
		t.Fatalf("touch UpsertLease: %v", err)
	}
	if engine.DirtyCount() == 0 {
		t.Fatal("touch did not create a dirty lease mark")
	}

	// Production shutdown stops the router before the final cache flush. The
	// persistence reader must still see active routing state; otherwise an
	// upsert is misclassified as a delete merely because the router is stopped.
	router.Stop()
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("final lease flush: %v", err)
	}

	got, err := engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}
	if len(got) != 1 || got[0].PlatformID != lease.PlatformID || got[0].Account != lease.Account {
		t.Fatalf("active lease after shutdown flush = %+v, want %+v", got, lease)
	}

	// The control-plane deletion order unregisters the platform before removing
	// its Router state. The persistence reader must fail closed in that gap and
	// the subsequent state removal must still produce a cache delete.
	pool.UnregisterPlatform(plat.ID)
	key := model.LeaseKey{PlatformID: lease.PlatformID, Account: lease.Account}
	if got := router.ReadLeaseForPersistence(key); got != nil {
		t.Fatalf("persistence reader returned an unregistered platform lease: %+v", got)
	}
	router.RemovePlatformState(plat.ID)
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("deleted lease flush: %v", err)
	}
	got, err = engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases after platform removal: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("platform removal left %d persisted leases: %+v", len(got), got)
	}
}
