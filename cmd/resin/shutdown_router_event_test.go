package main

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/topology"
)

// A route request can already be inside Router's synchronous lease-event
// callback when endpoint shutdown reaches the topology owner. The bounded
// shutdown caller must return at its deadline and join the Router stop owner
// before persistence closes.
func TestResinAppShutdownTracksInFlightRouterLeaseEventAfterDeadline(t *testing.T) {
	const (
		platformID = "shutdown-router-event-platform"
		account    = "shutdown-router-event-account"
	)

	subManager := topology.NewSubscriptionManager()
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	plat := platform.NewPlatform(platformID, platformID, nil, nil)
	if err := p.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	callbackEntered := make(chan struct{})
	allowCallback := make(chan struct{})
	var callbackOnce sync.Once
	var releaseOnce sync.Once
	releaseCallback := func() {
		releaseOnce.Do(func() { close(allowCallback) })
	}
	defer releaseCallback()

	router := routing.NewRouter(routing.RouterConfig{
		Pool: p,
		OnLeaseEvent: func(event routing.LeaseEvent) {
			if event.Type != routing.LeaseCreate || event.Account != account {
				return
			}
			callbackOnce.Do(func() { close(callbackEntered) })
			<-allowCallback
		},
	})
	hash := node.HashFromRawOptions([]byte(`{"id":"shutdown-router-event"}`))
	lease := model.Lease{
		PlatformID: platformID,
		Account:    account,
		NodeHash:   hash.Hex(),
		EgressIP:   "203.0.113.242",
	}
	sentinel := lease
	sentinel.Account = account + "-sentinel"
	router.RestoreLeases([]model.Lease{sentinel})

	routeDone := make(chan error, 1)
	go func() { routeDone <- router.UpsertLease(lease) }()
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("lease event callback did not start")
	}

	cleaner := routing.NewLeaseCleanerWithIntervals(router, time.Hour, 0)
	cleaner.Start()
	defer func() { _ = cleaner.StopContext(context.Background()) }()

	app := &resinApp{
		topoRuntime: &topologyRuntime{
			leaseCleaner: cleaner,
			router:       router,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	// ReadLease returns nil only after Router.Stop has closed mutation
	// admission. This is the deterministic point at which shutdown has reached
	// the synchronous Router stop owner.
	stopAdmission := make(chan struct{})
	go func() {
		for {
			if router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: sentinel.Account}) == nil {
				close(stopAdmission)
				return
			}
			runtime.Gosched()
		}
	}()
	select {
	case <-stopAdmission:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not close Router admission")
	}
	<-ctx.Done()

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
		if continuations.topology == nil {
			t.Fatal("shutdown did not register a topology continuation")
		}
	case <-time.After(time.Second):
		t.Fatal("app shutdown did not return after the caller deadline")
	}

	releaseCallback()
	select {
	case err := <-routeDone:
		if err != nil {
			t.Fatalf("UpsertLease: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lease event caller did not finish after release")
	}

	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuation: %v", err)
	}
}
