package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestShutdownStopsProbeBeforeClosingDirtyAdmission(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	hash := node.HashFromRawOptions([]byte(`{"type":"shutdown-probe"}`))
	callbackEntered := make(chan struct{})
	allowCallback := make(chan struct{})
	marked := make(chan bool, 8)
	var callbackOnce sync.Once
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged: func(node.Hash) {
			callbackOnce.Do(func() {
				close(callbackEntered)
				<-allowCallback
			})
			marked <- engine.MarkNodeDynamic(hash.Hex())
		},
	})
	pool.AddNodeFromSub(hash, []byte(`{"type":"shutdown-probe"}`), "shutdown-sub")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("probe entry missing")
	}
	entry.CircuitOpenSince.Store(0)
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(context.Context, *node.NodeEntry, string) ([]byte, time.Duration, error) {
			return nil, 0, errors.New("probe failed")
		},
	})
	probeMgr.Start()
	probeMgr.TriggerImmediateEgressProbe(hash)
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("real probe did not reach the pool dirty callback")
	}

	flushWorker := state.NewCacheFlushWorker(
		engine,
		state.CacheReaders{
			ReadNodeDynamic: func(string) *model.NodeDynamic {
				return &model.NodeDynamic{
					Hash:             hash.Hex(),
					FailureCount:     int(entry.FailureCount.Load()),
					CircuitOpenSince: entry.CircuitOpenSince.Load(),
				}
			},
		},
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushWorker.Start()

	h := newBlockingEndpointHarness(t, func() {})
	app := &resinApp{
		stateEngine:     engine,
		geoSvc:          geoip.NewService(geoip.ServiceConfig{OpenDB: geoip.NoOpOpen}),
		endpointManager: h.manager,
		flushWorker:     flushWorker,
		topoRuntime:     &topologyRuntime{probeMgr: probeMgr},
	}
	sourcesStopEntered := make(chan struct{})
	app.beforeTopologyEventSourcesStopHook = func() {
		close(sourcesStopEntered)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	select {
	case <-sourcesStopEntered:
	case <-time.After(time.Second):
		h.release()
		t.Fatal("shutdown did not reach the event-source stop boundary")
	}
	// The callback is held across the exact point where the old order closed
	// dirty admission. The event source must be drained before that closure.
	close(allowCallback)

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		h.release()
		t.Fatal("shutdown did not finish after the probe callback was released")
	}
	h.release()
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}

	select {
	case ok := <-marked:
		if !ok {
			t.Fatal("probe dirty callback was rejected before event-source drain completed")
		}
	case <-time.After(time.Second):
		t.Fatal("probe dirty callback did not complete")
	}
	rows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(rows) != 1 || rows[0].Hash != hash.Hex() {
		t.Fatalf("probe dynamic state was not persisted: %+v", rows)
	}
}

func TestShutdownStopsTransportHealthWritesBeforeClosingDirtyAdmission(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	hash := node.HashFromRawOptions([]byte(`{"type":"shutdown-health"}`))
	callbackEntered := make(chan struct{})
	allowCallback := make(chan struct{})
	marked := make(chan bool, 1)
	var callbackOnce sync.Once
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged: func(node.Hash) {
			callbackOnce.Do(func() {
				close(callbackEntered)
				<-allowCallback
			})
			marked <- engine.MarkNodeDynamic(hash.Hex())
		},
	})
	pool.AddNodeFromSub(hash, []byte(`{"type":"shutdown-health"}`), "shutdown-sub")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("health entry missing")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	// A successful health result must clear a circuit and therefore emit the
	// same dynamic dirty callback used by production passive health feedback.
	entry.CircuitOpenSince.Store(1)

	healthOwner := proxy.NewHealthWriteOwner(pool)
	healthResult := make(chan bool, 1)
	go func() {
		healthResult <- healthOwner.RecordPassiveResultForEntry("", hash, entry, true)
	}()
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("health write did not reach the production pool callback")
	}

	flushWorker := state.NewCacheFlushWorker(
		engine,
		state.CacheReaders{
			ReadNodeDynamic: func(string) *model.NodeDynamic {
				return &model.NodeDynamic{
					Hash:             hash.Hex(),
					FailureCount:     int(entry.FailureCount.Load()),
					CircuitOpenSince: entry.CircuitOpenSince.Load(),
				}
			},
		},
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushWorker.Start()
	h := newBlockingEndpointHarness(t, func() {})
	transportPool := proxy.NewOutboundTransportPool(proxy.OutboundTransportConfig{})
	app := &resinApp{
		stateEngine:      engine,
		geoSvc:           geoip.NewService(geoip.ServiceConfig{OpenDB: geoip.NoOpOpen}),
		endpointManager:  h.manager,
		transportPool:    transportPool,
		healthWriteOwner: healthOwner,
		flushWorker:      flushWorker,
		topoRuntime:      &topologyRuntime{},
	}
	// This hook is immediately adjacent to the real transport shutdown call.
	// In the old order it runs after HTTP timeout closed dirty admission; in the
	// fixed order it runs before that barrier and releases the admitted callback.
	app.beforeTransportPoolShutdownHook = func() { close(allowCallback) }

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		h.release()
		t.Fatal("shutdown did not finish after the health callback was released")
	}
	h.release()
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}
	select {
	case ok := <-healthResult:
		if !ok {
			t.Fatal("health result was not applied")
		}
	case <-time.After(time.Second):
		t.Fatal("health write did not finish")
	}
	select {
	case ok := <-marked:
		if !ok {
			t.Fatal("health dirty callback was rejected before transport/health drain completed")
		}
	case <-time.After(time.Second):
		t.Fatal("health dirty callback did not complete")
	}
	rows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(rows) != 1 || rows[0].Hash != hash.Hex() || rows[0].CircuitOpenSince != 0 {
		t.Fatalf("health dynamic state was not persisted: %+v", rows)
	}
}

func TestShutdownTracksEphemeralCleanerContinuationBeforeCacheFlush(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-ephemeral-app",
		"shutdown-ephemeral-app",
		"https://example.invalid/sub",
		true,
		true,
	)
	sub.SetEphemeralNodeEvictDelayNs(int64(time.Second))
	subMgr.Register(sub)

	hash := node.HashFromRawOptions([]byte(`{"type":"shutdown-ephemeral-app"}`))
	raw := []byte(`{"type":"shutdown-ephemeral-app"}`)
	callbackEntered := make(chan struct{})
	allowCallback := make(chan struct{})
	markResult := make(chan bool, 1)
	var callbackOnce sync.Once
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChangedWithPersistence: func(_ string, _ node.Hash, added bool, admission topology.PersistenceAdmission) {
			if added {
				return
			}
			callbackOnce.Do(func() { close(callbackEntered) })
			<-allowCallback
			markResult <- admission.MarkSubscriptionNodeDelete(sub.ID, hash.Hex())
		},
	})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"shutdown"}})
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("ephemeral shutdown entry missing")
	}
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Hour).UnixNano())

	cleaner := topology.NewEphemeralCleanerWithIntervals(subMgr, pool, time.Millisecond, 0)
	cleaner.SetPersistenceMutationRunner(func(fn func(topology.PersistenceAdmission)) bool {
		return engine.WithDirtyWriteAdmission(func(admission *state.DirtyWriteAdmission) {
			fn(admission)
		})
	})
	cleaner.Start()
	defer cleaner.Stop()

	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("real ephemeral cleaner did not reach its pool persistence callback")
	}

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
		topoRuntime: &topologyRuntime{ephemeralCleaner: cleaner},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before its deadline while ephemeral persistence was admitted")
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
		t.Fatalf("shutdown continuation completed before admitted callback release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(allowCallback)
	select {
	case accepted := <-markResult:
		if !accepted {
			t.Fatal("already-admitted ephemeral dirty mark was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("ephemeral dirty mark did not finish")
	}
	select {
	case err := <-continuationDone:
		if err != nil {
			t.Fatalf("shutdown continuation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown continuation did not finish after callback release")
	}

	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("dirty writes remained after shutdown continuation: %d", got)
	}
	rows, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	if len(rows) != 1 || rows[0].SubscriptionID != sub.ID || rows[0].NodeHash != hash.Hex() || !rows[0].Evicted {
		t.Fatalf("ephemeral shutdown row = %+v, want one evicted row", rows)
	}
	if engine.MarkSubscriptionNode(sub.ID, hash.Hex()) {
		t.Fatal("dirty admission remained open after shutdown")
	}
}
