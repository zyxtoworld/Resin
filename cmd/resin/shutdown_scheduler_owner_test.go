package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
)

type shutdownGatedOutbound struct {
	testutil.NoopOutbound
	closed atomic.Int32
}

func (o *shutdownGatedOutbound) Close() error {
	o.closed.Add(1)
	return nil
}

type shutdownGatedBuilder struct {
	entered     chan struct{}
	release     chan struct{}
	built       chan *shutdownGatedOutbound
	enteredOnce sync.Once
}

func (b *shutdownGatedBuilder) Build(json.RawMessage) (adapter.Outbound, error) {
	b.enteredOnce.Do(func() { close(b.entered) })
	<-b.release
	outbound := &shutdownGatedOutbound{}
	b.built <- outbound
	return outbound, nil
}

func TestResinAppShutdownHonorsDeadlineWhileSchedulerRuntimePreparationBlocks(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subMgr.Lookup,
		MaxConsecutiveFailures: func() int {
			return 3
		},
		OnNodeAddedRuntime: func(_ node.Hash, _ *node.NodeEntry) {
			enteredOnce.Do(func() { close(entered) })
			<-release
		},
	})
	sub := subscription.NewSubscription("shutdown-scheduler", "shutdown-scheduler", "https://example.invalid/sub", true, false)
	subMgr.Register(sub)
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return []byte(`{"outbounds":[{"type":"shadowsocks","tag":"shutdown-scheduler-node","server":"1.1.1.1","server_port":443}]}`), nil
		},
	})
	scheduler.UpdateSubscriptionAsync(sub)
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("scheduler runtime preparation did not reach the production callback")
	}

	app := &resinApp{topoRuntime: &topologyRuntime{scheduler: scheduler}}
	schedulerStopReturned := make(chan struct{})
	app.afterTopologySchedulerStopHook = func() { close(schedulerStopReturned) }
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before its deadline while scheduler runtime preparation was blocked")
	case <-ctx.Done():
	}

	select {
	case <-schedulerStopReturned:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("scheduler stop did not reach its bounded return")
	}

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
		// The fixed owner returns at its bounded caller deadline and keeps the
		// admitted scheduler callback in shutdownContinuations.
	case <-time.After(time.Second):
		close(release)
		t.Fatal("shutdown did not honor its deadline")
	}

	close(release)
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}
}

func TestResinAppShutdownLateRuntimePreparationFailsClosedBeforeSinks(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}

	metricsRepo, err := metrics.NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		_ = closer.Close()
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
		_ = closer.Close()
		t.Fatalf("NewManager: %v", err)
	}

	var released sync.Once
	builder := &shutdownGatedBuilder{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		built:   make(chan *shutdownGatedOutbound, 1),
	}
	releaseBuilder := func() { released.Do(func() { close(builder.release) }) }
	defer releaseBuilder()
	defer func() {
		_ = metricsManager.CloseContext(context.Background())
		_ = metricsRepo.Close()
		_ = closer.Close()
	}()

	subMgr := topology.NewSubscriptionManager()
	var outboundManager *outbound.OutboundManager
	var probeManager *probe.ProbeManager
	probeEvents := atomic.Int32{}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	outboundManager = outbound.NewOutboundManager(pool, builder)
	probeManager = probe.NewProbeManager(probe.ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(context.Context, *node.NodeEntry, string) ([]byte, time.Duration, error) {
			return nil, 0, errors.New("unexpected late probe")
		},
		OnProbeEvent: func(string) { probeEvents.Add(1) },
	})
	// This is the production main.go callback: resource preparation is
	// synchronous, while the probe is only queued after outbound preparation.
	pool.SetOnNodeAddedRuntime(func(hash node.Hash, expected *node.NodeEntry) {
		outboundManager.EnsureNodeOutboundForEntry(hash, expected)
		probeManager.TriggerImmediateEgressProbeForEntry(hash, expected)
	})

	sub := subscription.NewSubscription("shutdown-sink-order", "shutdown-sink-order", "https://example.invalid/sub", true, false)
	subMgr.Register(sub)
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return []byte(`{"outbounds":[{"type":"shadowsocks","tag":"shutdown-sink-order-node","server":"1.1.1.1","server_port":443}]}`), nil
		},
	})
	scheduler.UpdateSubscriptionAsync(sub)
	select {
	case <-builder.entered:
	case <-time.After(time.Second):
		t.Fatal("production runtime callback did not enter outbound preparation")
	}

	flushWorker := state.NewCacheFlushWorker(
		engine,
		state.CacheReaders{},
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushWorker.Start()
	healthOwner := proxy.NewHealthWriteOwner(pool)
	app := &resinApp{
		stateEngine:      engine,
		metricsManager:   metricsManager,
		flushWorker:      flushWorker,
		healthWriteOwner: healthOwner,
		topoRuntime: &topologyRuntime{
			scheduler:   scheduler,
			probeMgr:    probeManager,
			outboundMgr: outboundManager,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		releaseBuilder()
		t.Fatal("shutdown remained blocked on admitted runtime preparation")
	}
	select {
	case <-builder.built:
		t.Fatal("runtime preparation completed before its explicit release")
	default:
	}

	// app.shutdown has already closed subordinate admissions by this point.
	// Releasing the admitted builder must therefore discard its candidate and
	// the callback's following probe enqueue must fail closed.
	releaseBuilder()
	var built *shutdownGatedOutbound
	select {
	case built = <-builder.built:
	case <-time.After(time.Second):
		t.Fatal("released runtime preparation did not finish")
	}
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}

	hash := node.HashFromRawOptions([]byte(`{"type":"shadowsocks","tag":"shutdown-sink-order-node","server":"1.1.1.1","server_port":443}`))
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("runtime node disappeared unexpectedly")
	}
	if entry.Outbound.Load() != nil {
		t.Fatal("late runtime preparation published an outbound after shutdown admission closed")
	}
	if got := built.closed.Load(); got != 1 {
		t.Fatalf("late outbound candidate close count = %d, want 1", got)
	}
	if got := probeEvents.Load(); got != 0 {
		t.Fatalf("late probe ran after ProbeManager stop: %d events", got)
	}
	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("late runtime callback polluted state dirty sets: %d", got)
	}
	if engine.MarkNodeStatic(hash.Hex()) {
		t.Fatal("state dirty admission remained open after shutdown")
	}
	if healthOwner.RecordPassiveResultForEntry("", hash, entry, true) {
		t.Fatal("health write admission remained open after shutdown")
	}
	_, beforeIngress, beforeEgress := metricsManager.SnapshotCurrentTrafficBucket()
	metricsManager.OnTrafficDelta(1, 1)
	_, afterIngress, afterEgress := metricsManager.SnapshotCurrentTrafficBucket()
	if afterIngress != beforeIngress || afterEgress != beforeEgress {
		t.Fatalf("metrics event was accepted after shutdown: before=(%d,%d) after=(%d,%d)", beforeIngress, beforeEgress, afterIngress, afterEgress)
	}
}
