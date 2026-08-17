package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

type blockingDirtyAdmission struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type persistenceCloseProbe struct {
	called chan context.Context
}

type dirtyAdmissionAction struct {
	action func()
}

func (a dirtyAdmissionAction) CloseDirtyWriteAdmission() {
	if a.action != nil {
		a.action()
	}
}

func (p *persistenceCloseProbe) Close() error { return nil }

func (p *persistenceCloseProbe) CloseContext(ctx context.Context) error {
	p.called <- ctx
	return nil
}

func (a *blockingDirtyAdmission) CloseDirtyWriteAdmission() {
	a.once.Do(func() { close(a.entered) })
}

func (a *blockingDirtyAdmission) CloseDirtyWriteAdmissionAndWait() {
	a.once.Do(func() { close(a.entered) })
	<-a.release
}

func newStaticFlushWorker(t *testing.T, engine *state.StateEngine) *state.CacheFlushWorker {
	t.Helper()
	return state.NewCacheFlushWorker(
		engine,
		state.CacheReaders{
			ReadNodeStatic: func(hash string) *model.NodeStatic {
				if hash == "late-handler-node" {
					return &model.NodeStatic{Hash: hash, RawOptions: []byte(`{}`), CreatedAtNs: 1}
				}
				return nil
			},
		},
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
}

func TestShutdownCoordinatorWaitsForAllContinuationsBeforePersistenceClose(t *testing.T) {
	cacheFlush := make(chan error, 1)
	requestLog := make(chan error, 1)
	metricsDone := make(chan error, 1)
	probe := &persistenceCloseProbe{called: make(chan context.Context, 1)}
	result := make(chan error, 1)
	go func() {
		result <- closePersistenceAfterShutdown(shutdownContinuations{
			cacheFlush: cacheFlush,
			requestLog: requestLog,
			metrics:    metricsDone,
		}, probe)
	}()

	assertNoPersistenceClose := func() {
		t.Helper()
		select {
		case <-probe.called:
			t.Fatal("shared persistence closed before all shutdown continuations completed")
		case <-time.After(50 * time.Millisecond):
		}
	}
	assertNoPersistenceClose()

	// Cache flush is the first dependency. Even if it completes, the process
	// must still join the two observability owners before closing shared DBs.
	cacheFlush <- nil
	assertNoPersistenceClose()
	requestLog <- nil
	assertNoPersistenceClose()
	metricsDone <- nil

	var closeCtx context.Context
	select {
	case closeCtx = <-probe.called:
	case <-time.After(time.Second):
		t.Fatal("shutdown coordinator did not close persistence after all owners completed")
	}
	if closeCtx == nil || closeCtx.Err() != nil {
		t.Fatalf("shared persistence close used canceled context: %v", closeCtx)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("shutdown coordinator: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown coordinator did not return after persistence close")
	}
}

func TestShutdownRejectsLateHTTPDirtyWriteAfterBarrier(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()
	flushWorker := newStaticFlushWorker(t, engine)
	flushWorker.Start()

	accepted := make(chan bool, 1)
	h := newBlockingEndpointHarness(t, func() {
		accepted <- engine.MarkNodeStatic("late-handler-node")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := h.manager.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded", err)
	}
	if err := drainHTTPHandlersBeforeSinks(ctx, h.manager, flushWorker); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handler drain error = %v, want deadline exceeded", err)
	}

	// The sink closes dirty-write admission before its final flush. The
	// already-running handler is allowed to finish, but its post-barrier mark
	// must be explicitly rejected rather than re-polluting cache state.
	h.release()
	h.waitHandler(t)
	select {
	case ok := <-accepted:
		if ok {
			t.Fatal("late handler dirty mark was admitted after the barrier")
		}
	case <-time.After(time.Second):
		t.Fatal("late handler dirty mark did not run")
	}
	flushWorker.Stop()
	if dirty := engine.DirtyCount(); dirty != 0 {
		t.Fatalf("late handler polluted dirty sets after final flush: %d", dirty)
	}
	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("late handler write appeared in cache after rejection: %+v", nodes)
	}
}

func TestShutdownMetricsTimeoutStartsEventualCloseContinuation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metrics.db")
	repo, err := metrics.NewMetricsRepo(dbPath)
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}

	var manager *metrics.Manager
	t.Cleanup(func() {
		if manager != nil {
			_ = manager.CloseContext(context.Background())
			return
		}
		_ = repo.Close()
	})
	var managerErr error
	manager, managerErr = metrics.NewManager(metrics.ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputRetentionSec:  1,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 1,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      1,
		LeasesIntervalSec:       1,
	})
	if managerErr != nil {
		t.Fatalf("NewManager: %v", managerErr)
	}
	manager.OnTrafficDelta(1, 2)

	blocker, err := state.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("state.OpenDB blocker: %v", err)
	}
	defer blocker.Close()
	tx, err := blocker.Begin()
	if err != nil {
		t.Fatalf("blocker Begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		"INSERT INTO metric_traffic_bucket (bucket_start_unix, ingress_bytes, egress_bytes) VALUES (?, ?, ?)",
		int64(1), int64(1), int64(1),
	); err != nil {
		t.Fatalf("blocker write: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownErr, continuation := closeMetricsManagerForShutdown(ctx, manager)
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("shutdown close error = %v, want deadline exceeded", shutdownErr)
	}
	if continuation == nil {
		t.Fatal("timed-out metrics shutdown did not register a continuation")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("release blocker: %v", err)
	}
	select {
	case continuationErr := <-continuation:
		if continuationErr != nil {
			t.Fatalf("continuation error = %v, want completed owner result", continuationErr)
		}
	case <-time.After(time.Second):
		t.Fatal("metrics shutdown continuation did not close the repo")
	}
	if _, err := repo.QueryTraffic(0, time.Now().Unix()); err == nil {
		t.Fatal("metrics repo remained usable after shutdown continuation")
	}
}

func TestShutdownRequestLogTimeoutStartsEventualCloseContinuation(t *testing.T) {
	logDir := t.TempDir()
	repo := requestlog.NewRepo(logDir, 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("repo.Open: %v", err)
	}
	service := requestlog.NewService(requestlog.ServiceConfig{
		Repo:          repo,
		QueueSize:     8,
		FlushBatch:    1000,
		FlushInterval: time.Hour,
	})
	service.EmitRequestLog(proxy.RequestLogEntry{
		ID:          "requestlog-shutdown-continuation",
		StartedAtNs: time.Now().UnixNano(),
		ProxyType:   proxy.ProxyTypeForward,
	})
	t.Cleanup(func() { _ = service.CloseContext(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownErr, continuation := closeRequestLogForShutdown(ctx, service)
	if !errors.Is(shutdownErr, context.Canceled) {
		t.Fatalf("shutdown close error = %v, want context canceled", shutdownErr)
	}
	if continuation == nil {
		t.Fatal("timed-out requestlog shutdown did not register a continuation")
	}
	select {
	case continuationErr := <-continuation:
		if continuationErr != nil {
			t.Fatalf("continuation error = %v, want completed owner result", continuationErr)
		}
	case <-time.After(time.Second):
		t.Fatal("requestlog shutdown continuation did not finish")
	}
	reopened := requestlog.NewRepo(logDir, 1<<20, 5)
	if err := reopened.Open(); err != nil {
		t.Fatalf("reopen requestlog repo: %v", err)
	}
	defer reopened.Close()
	row, err := reopened.GetByID("requestlog-shutdown-continuation")
	if err != nil {
		t.Fatalf("reopened GetByID: %v", err)
	}
	if row == nil {
		t.Fatal("requestlog continuation dropped queued entry")
	}
}

func TestShutdownDirtyAdmissionClosesWithoutWaitingAfterHTTPDrainDeadline(t *testing.T) {
	admission := &blockingDirtyAdmission{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	handlerAtDirty := make(chan struct{})
	allowHandlerReturn := make(chan struct{})
	var handlerOnce sync.Once
	h := newBlockingEndpointHarness(t, func() {
		// The handler has already passed its write admission in this production
		// shaped interleaving; keep it in flight so the bounded drain takes its
		// deadline path while the dirty write owner is still active.
		handlerOnce.Do(func() { close(handlerAtDirty) })
		<-allowHandlerReturn
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := h.manager.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded", err)
	}
	h.release()
	select {
	case <-handlerAtDirty:
	case <-time.After(time.Second):
		close(allowHandlerReturn)
		h.waitHandler(t)
		t.Fatal("handler did not reach the admitted dirty-write gate")
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- drainHTTPHandlersBeforeSinks(ctx, h.manager, admission) }()
	select {
	case err := <-drainDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler drain error = %v, want deadline exceeded", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(admission.release)
		close(allowHandlerReturn)
		h.waitHandler(t)
		select {
		case <-drainDone:
		case <-time.After(time.Second):
		}
		t.Fatal("handler drain waited past its expired deadline for an admitted dirty write")
	}

	close(allowHandlerReturn)
	h.waitHandler(t)
	select {
	case <-admission.entered:
	default:
		t.Fatal("dirty admission was not closed")
	}
}

func TestShutdownFlushesHTTPDirtyWriteBeforeDeadline(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()
	flushWorker := newStaticFlushWorker(t, engine)
	flushWorker.Start()

	h := newBlockingEndpointHarness(t, func() {
		if ok := engine.MarkNodeStatic("late-handler-node"); !ok {
			t.Error("handler mark was rejected before shutdown")
		}
	})
	h.release()
	h.waitHandler(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := drainHTTPHandlersBeforeSinks(ctx, h.manager, flushWorker); err != nil {
		t.Fatalf("handler drain: %v", err)
	}
	flushWorker.Stop()

	nodes, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Hash != "late-handler-node" {
		t.Fatalf("in-deadline handler write was not flushed: %+v", nodes)
	}
}

func TestShutdownRejectsLateHTTPStateWriteAfterBarrier(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	writeResult := make(chan error, 1)
	h := newBlockingEndpointHarness(t, func() {
		writeResult <- engine.UpsertSubscription(model.Subscription{
			ID:               "late-state-write",
			Name:             "late-state-write",
			SourceType:       "local",
			Content:          "{}",
			UpdateIntervalNs: int64(30 * time.Second),
			CreatedAtNs:      1,
			UpdatedAtNs:      1,
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := h.manager.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded", err)
	}
	if err := drainHTTPHandlersBeforePersistence(ctx, h.manager, nil, engine); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handler drain error = %v, want deadline exceeded", err)
	}

	// The handler is already beyond the bounded shutdown barrier. Strong state
	// writes must be rejected just like weak dirty marks; otherwise a late
	// control-plane request can mutate state.db while persistence is closing.
	h.release()
	h.waitHandler(t)
	select {
	case err := <-writeResult:
		if err == nil {
			t.Fatal("late handler state write succeeded after shutdown barrier")
		}
	case <-time.After(time.Second):
		t.Fatal("late handler state write did not finish")
	}
}

func TestShutdownClosesStateAdmissionBeforeDirtyAdmissionForControlPlaneDelete(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-delete-sub",
		"shutdown-delete-sub",
		"https://example.com",
		true,
		true,
	)
	sub.SetFetchConfig(sub.URL(), int64(time.Minute))
	subMgr.Register(sub)
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               sub.ID,
		Name:             sub.Name(),
		SourceType:       sub.SourceType(),
		URL:              sub.URL(),
		Enabled:          true,
		Ephemeral:        true,
		UpdateIntervalNs: int64(time.Minute),
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	raw := []byte(`{"type":"shutdown-delete-node"}`)
	hash := node.HashFromRawOptions(raw)
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxConsecutiveFailures: func() int { return 3 },
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if added {
				engine.MarkSubscriptionNode(subID, changedHash.Hex())
				return
			}
			engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex())
		},
	})
	p.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})
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
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("flush initial subscription node: %v", err)
	}

	serviceCP := &service.ControlPlaneService{
		Engine: engine,
		Pool:   p,
		SubMgr: subMgr,
	}
	h := newBlockingEndpointHarness(t, func() {})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := h.manager.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded", err)
	}

	deleteErr := make(chan error, 1)
	admission := dirtyAdmissionAction{action: func() {
		deleteErr <- serviceCP.DeleteSubscription(sub.ID)
	}}
	if err := drainHTTPHandlersBeforePersistence(ctx, h.manager, admission, engine); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handler drain error = %v, want deadline exceeded", err)
	}
	h.release()
	h.waitHandler(t)

	select {
	case err := <-deleteErr:
		if err == nil {
			t.Fatal("control-plane delete succeeded between state and dirty admission barriers")
		}
	case <-time.After(time.Second):
		t.Fatal("control-plane delete did not return")
	}
	subscriptions, err := engine.ListSubscriptions()
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}
	foundSubscription := false
	for _, persisted := range subscriptions {
		if persisted.ID == sub.ID {
			foundSubscription = true
			break
		}
	}
	if !foundSubscription {
		t.Fatal("subscription was deleted after rejected shutdown mutation")
	}
	if _, ok := p.GetEntry(hash); !ok {
		t.Fatal("node was removed after rejected shutdown mutation")
	}
	if subMgr.Lookup(sub.ID) != sub {
		t.Fatal("subscription runtime was unregistered after rejected shutdown mutation")
	}
}
