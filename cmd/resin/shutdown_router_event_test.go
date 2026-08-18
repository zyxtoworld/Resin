package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
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

func TestResinAppShutdownKeepsLeaseEventSinksOpenUntilTopologyCompletes(t *testing.T) {
	const (
		platformID = "shutdown-router-sink-platform"
		account    = "shutdown-router-sink-account"
	)

	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	metricsRepo, err := metrics.NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	metricsManager, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    metricsRepo,
		BucketSeconds:           300,
		ThroughputIntervalSec:   1,
		ThroughputRetentionSec:  2,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 2,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      2,
	})
	if err != nil {
		_ = metricsRepo.Close()
		t.Fatalf("NewManager: %v", err)
	}
	defer metricsManager.CloseContext(context.Background())

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
	allowMetrics := make(chan struct{})
	metricsObserved := make(chan int, 1)
	metricsClosed := make(chan struct{}, 2)
	var callbackOnce sync.Once
	var releaseOnce sync.Once
	releaseCallback := func() { releaseOnce.Do(func() { close(allowMetrics) }) }
	defer releaseCallback()

	app := &resinApp{
		stateEngine:    engine,
		metricsDB:      metricsRepo,
		metricsManager: metricsManager,
		flushWorker:    newStaticFlushWorker(t, engine),
	}
	app.beforeLeaseEventMetricsHook = func() {
		callbackOnce.Do(func() { close(callbackEntered) })
		<-allowMetrics
	}
	app.afterLeaseEventMetricsHook = func() {
		_, sampleCount, _, _, _ := metricsManager.SnapshotCurrentLeaseLifetimeBucket(platformID)
		metricsObserved <- sampleCount
	}
	app.afterMetricsManagerShutdownHook = func() { metricsClosed <- struct{}{} }
	stateCloseStarted := make(chan struct{})
	app.afterStateWriteAdmissionCloseHook = func() { close(stateCloseStarted) }

	router := routing.NewRouter(routing.RouterConfig{
		Pool:         p,
		OnLeaseEvent: func(event routing.LeaseEvent) { app.handleLeaseEvent(engine, event) },
	})
	app.topoRuntime = &topologyRuntime{router: router}
	lease := model.Lease{
		PlatformID:  platformID,
		Account:     account,
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"shutdown-router-sink"}`)).Hex(),
		EgressIP:    "203.0.113.243",
		CreatedAtNs: time.Now().Add(-time.Second).UnixNano(),
	}
	router.RestoreLeases([]model.Lease{lease})

	removeDone := make(chan error, 1)
	go func() {
		removed, platformExists := router.DeleteLeaseForPlatform(platformID, account)
		if !removed || !platformExists {
			removeDone <- errors.New("lease was not removed")
			return
		}
		removeDone <- nil
	}()
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("production lease callback did not reach the persistence/metrics boundary")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not reach its caller deadline")
	}

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after the caller deadline")
	}
	select {
	case <-stateCloseStarted:
		t.Fatal("state-write admission closed before the in-flight lease callback completed")
	default:
	}
	if err := engine.WithStateWriteAdmission(func() error { return nil }); err != nil {
		t.Fatalf("state-write admission closed before topology completion: %v", err)
	}
	if err := metricsRepo.WriteNodePoolSnapshot(time.Now().Unix(), 1, 1, 1); err != nil {
		t.Fatalf("metrics DB closed before topology completion: %v", err)
	}
	if !engine.MarkLeaseDelete(platformID, account) {
		t.Fatal("dirty-write admission closed before topology completion")
	}

	releaseCallback()
	if err := <-removeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case sampleCount := <-metricsObserved:
		if sampleCount != 1 {
			t.Fatalf("lease metrics sample count = %d, want 1", sampleCount)
		}
	case <-time.After(time.Second):
		t.Fatal("lease metrics callback did not complete")
	}
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuation: %v", err)
	}
	if err := metricsRepo.WriteNodePoolSnapshot(time.Now().Unix()+1, 1, 1, 1); err == nil {
		t.Fatal("metrics DB remained open after topology-dependent shutdown")
	}
	if engine.MarkLeaseDelete(platformID, account) {
		t.Fatal("late dirty-write admission remained open after topology-dependent shutdown")
	}
	select {
	case <-metricsClosed:
	default:
		t.Fatal("metrics manager close owner did not complete")
	}
	select {
	case <-metricsClosed:
		t.Fatal("metrics manager close owner ran more than once")
	default:
	}
}

func TestResinAppShutdownWaitsForRouterAndHTTPHandlersBeforeClosingSinks(t *testing.T) {
	const (
		platformID = "shutdown-router-http-sinks-platform"
		routerAcct = "shutdown-router-http-router-account"
		httpAcct   = "shutdown-router-http-handler-account"
	)

	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	metricsRepo, err := metrics.NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	metricsManager, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    metricsRepo,
		BucketSeconds:           300,
		ThroughputIntervalSec:   1,
		ThroughputRetentionSec:  2,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 2,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      2,
	})
	if err != nil {
		_ = metricsRepo.Close()
		t.Fatalf("NewManager: %v", err)
	}
	defer metricsManager.CloseContext(context.Background())

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

	routerEntered := make(chan struct{})
	allowRouter := make(chan struct{})
	handlerEntered := make(chan struct{})
	allowHandler := make(chan struct{})
	handlerWriteStarted := make(chan struct{})
	handlerWrites := make(chan error, 1)
	var handlerWriteCount atomic.Int32
	var routerOnce sync.Once
	var handlerOnce sync.Once
	var releaseRouterOnce sync.Once
	var releaseHandlerOnce sync.Once
	releaseRouter := func() { releaseRouterOnce.Do(func() { close(allowRouter) }) }
	releaseHandler := func() { releaseHandlerOnce.Do(func() { close(allowHandler) }) }
	defer releaseRouter()
	defer releaseHandler()

	app := &resinApp{
		stateEngine:    engine,
		metricsDB:      metricsRepo,
		metricsManager: metricsManager,
		flushWorker:    newStaticFlushWorker(t, engine),
	}
	app.beforeLeaseEventMetricsHook = func() {
		routerOnce.Do(func() { close(routerEntered) })
		<-allowRouter
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:         p,
		OnLeaseEvent: func(event routing.LeaseEvent) { app.handleLeaseEvent(engine, event) },
	})
	app.topoRuntime = &topologyRuntime{router: router}

	port := reserveTestPorts(t, 1)[0]
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerOnce.Do(func() { close(handlerEntered) })
		<-allowHandler
		close(handlerWriteStarted)
		handlerWriteCount.Add(1)
		if !engine.MarkLeaseDelete(platformID, httpAcct) {
			handlerWrites <- errors.New("HTTP handler state write was rejected")
			return
		}
		if err := metricsRepo.WriteNodePoolSnapshot(time.Now().Unix(), 1, 1, 1); err != nil {
			handlerWrites <- fmt.Errorf("HTTP handler metrics write: %w", err)
			return
		}
		handlerWrites <- nil
		w.WriteHeader(http.StatusNoContent)
	})
	manager := newEndpointRuntimeManager("127.0.0.1", "", nil, nil, apiHandler, nil, nil, nil)
	endpoint := service.NewDefaultEndpoint(port)
	if err := manager.ApplyEndpoint(endpoint); err != nil {
		t.Fatalf("ApplyEndpoint: %v", err)
	}
	manager.Start()
	app.endpointManager = manager
	t.Cleanup(func() {
		releaseRouter()
		releaseHandler()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})

	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(formatListenURL("127.0.0.1", port) + "/api/")
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-handlerEntered:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}

	lease := model.Lease{
		PlatformID:  platformID,
		Account:     routerAcct,
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"shutdown-router-http-sinks"}`)).Hex(),
		EgressIP:    "203.0.113.244",
		CreatedAtNs: time.Now().Add(-time.Second).UnixNano(),
	}
	router.RestoreLeases([]model.Lease{lease})
	removeDone := make(chan error, 1)
	go func() {
		removed, platformExists := router.DeleteLeaseForPlatform(platformID, routerAcct)
		if !removed || !platformExists {
			removeDone <- errors.New("router lease was not removed")
			return
		}
		removeDone <- nil
	}()
	select {
	case <-routerEntered:
	case <-time.After(time.Second):
		t.Fatal("Router lease callback did not start")
	}

	stateCloseStarted := make(chan struct{})
	app.afterStateWriteAdmissionCloseHook = func() { close(stateCloseStarted) }
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not reach its caller deadline")
	}
	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after the caller deadline")
	}
	if continuations.topologySinks == nil {
		t.Fatal("shutdown did not register topology sink continuation")
	}
	select {
	case <-stateCloseStarted:
		t.Fatal("state-write admission closed before either in-flight owner completed")
	default:
	}
	if err := metricsRepo.WriteNodePoolSnapshot(time.Now().Unix(), 1, 1, 1); err != nil {
		t.Fatalf("metrics DB closed while both owners were blocked: %v", err)
	}

	releaseRouter()
	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Router callback did not finish after release")
	}
	if continuations.topology == nil {
		t.Fatal("shutdown did not register topology continuation")
	}
	select {
	case err := <-continuations.topology:
		if err != nil {
			t.Fatalf("topology continuation: %v", err)
		}
		continuations.topology = nil
	case <-time.After(time.Second):
		t.Fatal("topology continuation did not finish after Router release")
	}
	select {
	case <-stateCloseStarted:
		t.Fatal("state-write admission closed while the HTTP handler was still admitted")
	case err := <-continuations.topologySinks:
		t.Fatalf("topology sinks closed while HTTP handler was still admitted: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	if err := metricsRepo.WriteNodePoolSnapshot(time.Now().Unix()+1, 1, 1, 1); err != nil {
		t.Fatalf("metrics DB closed after Router owner but before HTTP owner: %v", err)
	}

	releaseHandler()
	select {
	case <-handlerWriteStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not reach its write boundary")
	}
	select {
	case err := <-handlerWrites:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP handler writes did not complete")
	}
	if got := handlerWriteCount.Load(); got != 1 {
		t.Fatalf("HTTP handler write count = %d, want 1", got)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not finish after release")
	}
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuation: %v", err)
	}
	if err := metricsRepo.WriteNodePoolSnapshot(time.Now().Unix()+2, 1, 1, 1); err == nil {
		t.Fatal("metrics DB remained open after both owners completed")
	}
}
