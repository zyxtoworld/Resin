package main

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestShutdownStopsRouterBeforeLateHTTPRouteMutation(t *testing.T) {
	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-route-sub",
		"shutdown-route-sub",
		"https://example.com",
		true,
		false,
	)
	subManager.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := []byte(`{"type":"shutdown-route-node"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("route node was not added to the pool")
	}
	entry.CircuitOpenSince.Store(0)
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.80"))
	entry.LatencyTable.Update("example.com", time.Millisecond, time.Minute)

	plat := platform.NewPlatform("shutdown-route-platform", "shutdown-route-platform", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if plat.View().Size() == 0 {
		t.Fatal("route fixture platform view is empty")
	}
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})
	app := &resinApp{topoRuntime: &topologyRuntime{router: router}}

	app.stopTopologyEventSources()

	result, err := router.RouteRequest(plat.Name, "late-account", "https://example.com")
	if err == nil {
		t.Fatalf("late RouteRequest unexpectedly succeeded: %+v", result)
	}
	if lease := router.ReadLease(model.LeaseKey{PlatformID: plat.ID, Account: "late-account"}); lease != nil {
		t.Fatalf("late RouteRequest created a lease after router shutdown: %+v", lease)
	}
}

func TestShutdownWaitsForRouterLeaseEventBeforeCacheAdmissionCloses(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-lease-event-sub",
		"shutdown-lease-event-sub",
		"https://example.com",
		true,
		false,
	)
	subManager.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"shutdown-lease-event-node"}`))
	ip := netip.MustParseAddr("203.0.113.121")
	persistedLease := model.Lease{
		PlatformID:  "shutdown-lease-event-platform",
		Account:     "shutdown-lease-event-account",
		NodeHash:    hash.Hex(),
		EgressIP:    ip.String(),
		CreatedAtNs: time.Now().UnixNano(),
		ExpiryNs:    time.Now().Add(time.Hour).UnixNano(),
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := []byte(`{"type":"shutdown-lease-event-node"}`)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("lease event route node was not added to the pool")
	}
	entry.CircuitOpenSince.Store(0)
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(ip)
	entry.LatencyTable.Update("example.com", time.Millisecond, time.Minute)

	plat := platform.NewPlatform(persistedLease.PlatformID, persistedLease.PlatformID, nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	marked := make(chan bool, 1)
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
		OnLeaseEvent: func(event routing.LeaseEvent) {
			if event.Type != routing.LeaseCreate {
				return
			}
			close(callbackStarted)
			<-releaseCallback
			marked <- engine.MarkLease(event.PlatformID, event.Account)
		},
	})

	routeDone := make(chan error, 1)
	go func() {
		_, routeErr := router.RouteRequest(plat.Name, persistedLease.Account, "https://example.com/")
		routeDone <- routeErr
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("lease event callback did not start")
	}

	flushWorker := state.NewCacheFlushWorker(
		engine,
		state.CacheReaders{
			ReadLease: func(key state.LeaseDirtyKey) *model.Lease {
				if key.PlatformID != persistedLease.PlatformID || key.Account != persistedLease.Account {
					return nil
				}
				lease := persistedLease
				return &lease
			},
		},
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushWorker.Start()
	h := newBlockingEndpointHarness(t, func() {})
	h.release()
	h.waitHandler(t)
	app := &resinApp{
		stateEngine:     engine,
		endpointManager: h.manager,
		flushWorker:     flushWorker,
		topoRuntime:     &topologyRuntime{router: router},
	}

	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(context.Background()) }()
	select {
	case <-shutdownDone:
		t.Fatal("app shutdown returned while Router lease callback was blocked")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseCallback)
	select {
	case err := <-routeDone:
		if err != nil {
			t.Fatalf("RouteRequest: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RouteRequest did not finish after lease callback release")
	}
	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("app shutdown did not finish after lease callback release")
	}
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}
	select {
	case ok := <-marked:
		if !ok {
			t.Fatal("lease event dirty mark was rejected before router event drain")
		}
	case <-time.After(time.Second):
		t.Fatal("lease event dirty mark did not run")
	}

	leases, err := engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}
	if len(leases) != 1 || leases[0].PlatformID != persistedLease.PlatformID || leases[0].Account != persistedLease.Account {
		t.Fatalf("lease event was not flushed: %+v", leases)
	}
}
