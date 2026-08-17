package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/service"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type shutdownStateOutbound struct {
	closeEntered   chan struct{}
	allowClose     chan struct{}
	closeCompleted chan struct{}
	enteredOnce    sync.Once
	completedOnce  sync.Once
}

func (o *shutdownStateOutbound) Type() string { return "shutdown-state" }
func (o *shutdownStateOutbound) Tag() string  { return "shutdown-state" }
func (o *shutdownStateOutbound) Network() []string {
	return []string{"tcp", "udp"}
}
func (o *shutdownStateOutbound) Dependencies() []string { return nil }
func (o *shutdownStateOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, nil
}
func (o *shutdownStateOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}
func (o *shutdownStateOutbound) Close() error {
	o.enteredOnce.Do(func() { close(o.closeEntered) })
	<-o.allowClose
	o.completedOnce.Do(func() { close(o.closeCompleted) })
	return nil
}

func TestShutdownWaitsForAdmittedSubscriptionCleanupBeforeOutboundShutdown(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-active-delete",
		"shutdown-active-delete",
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

	const raw = `{"type":"shutdown-active-delete"}`
	hash := node.HashFromRawOptions([]byte(raw))
	nodeRemovalEntered := make(chan struct{})
	allowNodeRemoval := make(chan struct{})
	var nodeRemovalOnce sync.Once
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subMgr.Lookup,
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if added {
				if !engine.MarkSubscriptionNode(subID, changedHash.Hex()) {
					t.Errorf("MarkSubscriptionNode was rejected")
				}
				return
			}
			if !engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex()) {
				t.Errorf("MarkSubscriptionNodeDelete was rejected")
			}
		},
		MaxConsecutiveFailures: func() int { return 3 },
	})
	p.AddNodeFromSub(hash, []byte(raw), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"shutdown"}})
	entry, ok := p.GetEntry(hash)
	if !ok {
		t.Fatal("node entry was not created")
	}
	closeGate := &shutdownStateOutbound{
		closeEntered:   make(chan struct{}),
		allowClose:     make(chan struct{}),
		closeCompleted: make(chan struct{}),
	}
	var rawOutbound adapter.Outbound = closeGate
	entry.Outbound.Store(&rawOutbound)

	manager := outbound.NewOutboundManager(p, &testutil.StubOutboundBuilder{})
	p.SetOnNodeRemoved(func(_ node.Hash, removed *node.NodeEntry) {
		nodeRemovalOnce.Do(func() { close(nodeRemovalEntered) })
		<-allowNodeRemoval
		manager.RemoveNodeOutbound(removed)
	})

	serviceCP := &service.ControlPlaneService{
		Engine: engine,
		Pool:   p,
		SubMgr: subMgr,
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- serviceCP.DeleteSubscription(sub.ID) }()
	select {
	case <-nodeRemovalEntered:
	case <-time.After(time.Second):
		t.Fatal("DeleteSubscription did not reach its node cleanup callback")
	}

	stateCloseReturned := make(chan struct{})
	stateOwnerScheduled := make(chan struct{})
	beforeOutbound := make(chan struct{})
	allowOutboundShutdown := make(chan struct{})
	app := &resinApp{
		stateEngine:     engine,
		endpointManager: newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil),
		topoRuntime:     &topologyRuntime{outboundMgr: manager},
	}
	app.afterStateWriteAdmissionCloseHook = func() {
		close(stateCloseReturned)
	}
	app.afterStateWriteTimeoutHook = func() {
		close(stateOwnerScheduled)
	}
	app.beforeOutboundShutdownHook = func() {
		close(beforeOutbound)
		<-allowOutboundShutdown
	}

	var releaseNodeRemovalOnce sync.Once
	var releaseOutboundShutdownOnce sync.Once
	var releaseCloseOnce sync.Once
	releaseNodeRemoval := func() {
		releaseNodeRemovalOnce.Do(func() { close(allowNodeRemoval) })
	}
	releaseOutboundShutdown := func() {
		releaseOutboundShutdownOnce.Do(func() { close(allowOutboundShutdown) })
	}
	releaseClose := func() {
		releaseCloseOnce.Do(func() { close(closeGate.allowClose) })
	}
	t.Cleanup(func() {
		releaseNodeRemoval()
		releaseOutboundShutdown()
		releaseClose()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()
	select {
	case <-stateCloseReturned:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not reach the state-write admission boundary")
	}
	select {
	case <-stateOwnerScheduled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not register a continuation for the admitted state write")
	}

	// The active DeleteSubscription has already committed its state mutation,
	// but its node cleanup callback is still admitted. Outbound shutdown must
	// not start while that callback can still register the entry retirement.
	select {
	case <-beforeOutbound:
		t.Fatal("outbound shutdown started before admitted subscription cleanup completed")
	default:
	}
	releaseNodeRemoval()

	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeleteSubscription: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DeleteSubscription did not finish after releasing cleanup")
	}
	select {
	case <-beforeOutbound:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not start outbound shutdown after cleanup completed")
	}
	select {
	case <-closeGate.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("late node retirement did not reach outbound Close")
	}
	releaseOutboundShutdown()

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after outbound retirement completed")
	}
	continuationDone := make(chan error, 1)
	go func() { continuationDone <- continuations.wait() }()
	select {
	case err := <-continuationDone:
		t.Fatalf("shutdown continuations returned before outbound retirement completed: %v", err)
	default:
	}
	releaseClose()
	select {
	case err := <-continuationDone:
		if err != nil {
			t.Fatalf("shutdown continuations: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown continuations did not finish after outbound retirement completed")
	}
	select {
	case <-closeGate.closeCompleted:
	default:
		t.Fatal("outbound retirement was not completed before shutdown returned")
	}
}

func TestShutdownWaitsForAdmittedDirtyCleanupBeforeOutboundClose(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	flushWorker := state.NewCacheFlushWorker(
		engine,
		state.CacheReaders{
			ReadNodeStatic:       func(string) *model.NodeStatic { return nil },
			ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
			ReadNodeLatency:      func(state.NodeLatencyDirtyKey) *model.NodeLatency { return nil },
			ReadLease:            func(state.LeaseDirtyKey) *model.Lease { return nil },
			ReadSubscriptionNode: func(state.SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
		},
		func() int { return 10_000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushWorker.Start()

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"shutdown-active-cleanup",
		"shutdown-active-cleanup",
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

	const raw = `{"type":"shutdown-active-cleanup"}`
	hash := node.HashFromRawOptions([]byte(raw))
	nodeRemovalEntered := make(chan struct{})
	allowNodeRemoval := make(chan struct{})
	var nodeRemovalOnce sync.Once
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subMgr.Lookup,
		OnSubNodeChanged: func(subID string, changedHash node.Hash, added bool) {
			if added {
				engine.MarkSubscriptionNode(subID, changedHash.Hex())
				return
			}
			engine.MarkSubscriptionNodeDelete(subID, changedHash.Hex())
		},
		MaxConsecutiveFailures: func() int { return 3 },
	})
	p.AddNodeFromSub(hash, []byte(raw), sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"cleanup"}})
	entry, ok := p.GetEntry(hash)
	if !ok {
		t.Fatal("node entry was not created")
	}
	closeGate := &shutdownStateOutbound{
		closeEntered:   make(chan struct{}),
		allowClose:     make(chan struct{}),
		closeCompleted: make(chan struct{}),
	}
	var rawOutbound adapter.Outbound = closeGate
	entry.Outbound.Store(&rawOutbound)

	manager := outbound.NewOutboundManager(p, &testutil.StubOutboundBuilder{})
	sentinelRaw := []byte(`{"type":"shutdown-sentinel"}`)
	sentinelEntry := node.NewNodeEntry(
		node.HashFromRawOptions(sentinelRaw),
		sentinelRaw,
		time.Now(),
		0,
	)
	sentinelClose := &shutdownStateOutbound{
		closeEntered:   make(chan struct{}),
		allowClose:     make(chan struct{}),
		closeCompleted: make(chan struct{}),
	}
	var rawSentinel adapter.Outbound = sentinelClose
	sentinelEntry.Outbound.Store(&rawSentinel)
	p.LoadNodeFromBootstrap(sentinelEntry)
	p.SetOnNodeRemoved(func(_ node.Hash, removed *node.NodeEntry) {
		nodeRemovalOnce.Do(func() { close(nodeRemovalEntered) })
		<-allowNodeRemoval
		manager.RemoveNodeOutbound(removed)
	})

	serviceCP := &service.ControlPlaneService{
		Engine: engine,
		Pool:   p,
		SubMgr: subMgr,
	}
	cleanupResult := make(chan error, 1)
	handlerCleanupStarted := make(chan struct{})
	var handlerCleanupOnce sync.Once
	h := newBlockingEndpointHarness(t, func() {
		handlerCleanupOnce.Do(func() { close(handlerCleanupStarted) })
		_, cleanupErr := serviceCP.CleanupSubscriptionCircuitOpenNodes(sub.ID)
		cleanupResult <- cleanupErr
	})
	h.release()
	select {
	case <-handlerCleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not enter subscription cleanup")
	}

	beforeOutbound := make(chan struct{})
	app := &resinApp{
		stateEngine:     engine,
		flushWorker:     flushWorker,
		endpointManager: h.manager,
		topoRuntime:     &topologyRuntime{outboundMgr: manager},
	}
	app.beforeOutboundShutdownHook = func() { close(beforeOutbound) }

	var releaseNodeRemovalOnce sync.Once
	var releaseCloseOnce sync.Once
	var releaseSentinelCloseOnce sync.Once
	releaseNodeRemoval := func() {
		releaseNodeRemovalOnce.Do(func() { close(allowNodeRemoval) })
	}
	releaseClose := func() {
		releaseCloseOnce.Do(func() { close(closeGate.allowClose) })
	}
	releaseSentinelClose := func() {
		releaseSentinelCloseOnce.Do(func() { close(sentinelClose.allowClose) })
	}
	t.Cleanup(func() {
		releaseNodeRemoval()
		releaseClose()
		releaseSentinelClose()
	})

	select {
	case <-nodeRemovalEntered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not reach the production node-removal callback")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()
	select {
	case <-beforeOutbound:
		t.Fatal("outbound shutdown started before admitted dirty cleanup finished")
	case <-time.After(100 * time.Millisecond):
	}

	// The dirty cleanup was admitted before the HTTP deadline. Outbound
	// shutdown must wait for this callback before taking its pool snapshot.
	releaseNodeRemoval()
	select {
	case cleanupErr := <-cleanupResult:
		if cleanupErr != nil {
			t.Fatalf("CleanupSubscriptionCircuitOpenNodes: %v", cleanupErr)
		}
	case <-time.After(time.Second):
		h.release()
		t.Fatal("dirty cleanup did not finish after releasing node removal")
	}
	h.release()
	h.waitHandler(t)
	select {
	case <-beforeOutbound:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not start outbound shutdown after dirty cleanup finished")
	}
	select {
	case <-sentinelClose.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("outbound shutdown did not reach its retirement snapshot")
	}
	select {
	case <-closeGate.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("late cleanup did not reach outbound Close")
	}
	releaseSentinelClose()

	var continuations shutdownContinuations
	select {
	case continuations = <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after the admitted cleanup finished")
	}
	continuationDone := make(chan error, 1)
	go func() { continuationDone <- continuations.wait() }()
	select {
	case err := <-continuationDone:
		t.Fatalf("shutdown continuations returned before late outbound Close: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseClose()
	select {
	case err := <-continuationDone:
		if err != nil {
			t.Fatalf("shutdown continuations: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown continuations did not finish after outbound Close")
	}
}
