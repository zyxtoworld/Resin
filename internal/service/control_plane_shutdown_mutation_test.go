package service

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

func TestDeletePlatformCompoundMutationDrainsBeforeCacheFinalFlush(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	const platformID = "shutdown-delete-platform"
	const account = "shutdown-delete-account"
	now := time.Now().UnixNano()
	platformRow := model.Platform{
		ID:                               platformID,
		Name:                             "shutdown-delete-platform",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      now,
	}
	if err := engine.UpsertPlatform(platformRow); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(platformRow)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	leaseNode := node.HashFromRawOptions([]byte(`{"type":"shutdown-delete-lease-node"}`))
	leaseRemoved := make(chan struct{})
	allowLeaseRemove := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(allowLeaseRemove) })
	leaseDeleteAccepted := make(chan bool, 1)
	router := routing.NewRouter(routing.RouterConfig{
		Pool: pool,
		OnLeaseEvent: func(event routing.LeaseEvent) {
			switch event.Type {
			case routing.LeaseCreate, routing.LeaseTouch, routing.LeaseReplace:
				if !engine.MarkLease(event.PlatformID, event.Account) {
					t.Errorf("seed lease dirty mark was rejected")
				}
			case routing.LeaseRemove, routing.LeaseExpire:
				close(leaseRemoved)
				<-allowLeaseRemove
				leaseDeleteAccepted <- engine.MarkLeaseDelete(event.PlatformID, event.Account)
			}
		},
	})
	if err := router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        account,
		NodeHash:       leaseNode.Hex(),
		EgressIP:       "203.0.113.10",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("seed lease: %v", err)
	}
	readers := state.CacheReaders{ReadLease: router.ReadLease}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush seed lease: %v", err)
	}

	service := &ControlPlaneService{Engine: engine, Pool: pool, Router: router}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- service.DeletePlatform(platformID) }()
	select {
	case <-leaseRemoved:
	case <-time.After(time.Second):
		t.Fatal("DeletePlatform did not reach the lease-remove callback")
	}

	// Model the production shutdown barrier after the state mutation has
	// entered its runtime cleanup. The state admission call is intentionally
	// bounded; the cache stop owner must still wait for this compound mutation
	// before closing dirty-write admission and doing its final flush.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	_ = engine.CloseStateWriteAdmissionAndWait(ctx)
	cancel()

	flushWorker := state.NewCacheFlushWorker(
		engine,
		readers,
		func() int { return 10000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushDone := make(chan error, 1)
	go func() { flushDone <- flushWorker.StopContext(context.Background()) }()
	select {
	case err := <-flushDone:
		t.Fatalf("cache stop returned before admitted platform cleanup finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(allowLeaseRemove) })
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeletePlatform: %v", err)
	}
	if accepted := <-leaseDeleteAccepted; !accepted {
		t.Fatal("lease delete dirty mark was rejected while the admitted platform mutation was finishing")
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("cache flush stop: %v", err)
	}

	leases, err := engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("deleted platform left %d persisted leases after final flush", len(leases))
	}
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("final flush left %d dirty entries", dirty)
	}
}

func TestDeleteLease_StateWriteAdmissionCoversLeaseEvent(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	const platformID = "shutdown-delete-lease-platform"
	const account = "shutdown-delete-lease-account"
	now := time.Now().UnixNano()
	platformRow := model.Platform{
		ID:                               platformID,
		Name:                             platformID,
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      now,
	}
	if err := engine.UpsertPlatform(platformRow); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(platformRow)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	leaseNode := node.HashFromRawOptions([]byte(`{"type":"shutdown-delete-lease-node"}`))
	leaseRemoved := make(chan struct{})
	allowLeaseRemove := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(allowLeaseRemove) })
	leaseDeleteAccepted := make(chan bool, 1)
	router := routing.NewRouter(routing.RouterConfig{
		Pool: pool,
		OnLeaseEvent: func(event routing.LeaseEvent) {
			switch event.Type {
			case routing.LeaseCreate, routing.LeaseTouch, routing.LeaseReplace:
				if !engine.MarkLease(event.PlatformID, event.Account) {
					t.Errorf("seed lease dirty mark was rejected")
				}
			case routing.LeaseRemove, routing.LeaseExpire:
				close(leaseRemoved)
				<-allowLeaseRemove
				leaseDeleteAccepted <- engine.MarkLeaseDelete(event.PlatformID, event.Account)
			}
		},
	})
	lease := model.Lease{
		PlatformID:     platformID,
		Account:        account,
		NodeHash:       leaseNode.Hex(),
		EgressIP:       "203.0.113.11",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}
	if err := router.UpsertLease(lease); err != nil {
		t.Fatalf("seed lease: %v", err)
	}
	readers := state.CacheReaders{ReadLease: router.ReadLease}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush seed lease: %v", err)
	}

	service := &ControlPlaneService{Engine: engine, Pool: pool, Router: router}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- service.DeleteLease(platformID, account) }()
	select {
	case <-leaseRemoved:
	case <-time.After(time.Second):
		t.Fatal("DeleteLease did not reach the lease-remove callback")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- engine.CloseStateWriteAdmissionAndWait(closeCtx) }()
	closeErr := <-closeDone
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("state admission close crossed the admitted lease deletion: got %v, want deadline exceeded", closeErr)
	}

	releaseOnce.Do(func() { close(allowLeaseRemove) })
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteLease: %v", err)
	}
	if accepted := <-leaseDeleteAccepted; !accepted {
		t.Fatal("lease delete dirty mark was rejected while the admitted lease mutation was finishing")
	}
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush deleted lease: %v", err)
	}
	leases, err := engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("deleted lease remained persisted after final flush: %d", len(leases))
	}
}
