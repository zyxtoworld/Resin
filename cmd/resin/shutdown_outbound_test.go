package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestShutdownRetiresPublishedNodeOutbounds(t *testing.T) {
	raw := []byte(`{"type":"shutdown-outbound"}`)
	hash := node.HashFromRawOptions(raw)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.AddNodeFromSub(hash, raw, "shutdown-sub")
	builder := &bootstrapCleanupTrackingBuilder{}
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	outboundMgr.EnsureNodeOutbound(hash)
	if builder.builds.Load() != 1 {
		t.Fatalf("outbound builds = %d, want 1", builder.builds.Load())
	}

	app := &resinApp{
		geoSvc:          geoip.NewService(geoip.ServiceConfig{OpenDB: geoip.NoOpOpen}),
		endpointManager: newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil),
		topoRuntime:     &topologyRuntime{outboundMgr: outboundMgr},
	}
	if continuations := app.shutdown(context.Background()); continuations.wait() != nil {
		t.Fatal("shutdown continuations returned an error")
	}
	if got := builder.closes.Load(); got != 1 {
		t.Fatalf("published outbound closes = %d, want 1 after normal shutdown", got)
	}
}

func TestShutdownRejectsLateOutboundBuildAfterRetirement(t *testing.T) {
	raw := []byte(`{"type":"shutdown-late-outbound"}`)
	hash := node.HashFromRawOptions(raw)
	lateRaw := []byte(`{"type":"shutdown-late-outbound-2"}`)
	lateHash := node.HashFromRawOptions(lateRaw)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.AddNodeFromSub(hash, raw, "shutdown-sub")
	builder := &bootstrapCleanupTrackingBuilder{}
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	pool.SetOnNodeAdded(func(hash node.Hash) {
		outboundMgr.EnsureNodeOutbound(hash)
	})
	outboundMgr.EnsureNodeOutbound(hash)

	nodeAdded := make(chan struct{})
	h := newBlockingEndpointHarness(t, func() {
		pool.AddNodeFromSub(lateHash, lateRaw, "shutdown-sub")
		close(nodeAdded)
	})
	app := &resinApp{
		geoSvc:          geoip.NewService(geoip.ServiceConfig{OpenDB: geoip.NoOpOpen}),
		endpointManager: h.manager,
		topoRuntime:     &topologyRuntime{outboundMgr: outboundMgr},
	}
	app.afterOutboundShutdownHook = func() {
		h.release()
		select {
		case <-nodeAdded:
		case <-time.After(time.Second):
			t.Fatal("late handler did not add the node after outbound retirement")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	continuations := app.shutdown(ctx)
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}
	h.waitHandler(t)
	if got := builder.builds.Load(); got != 1 {
		t.Fatalf("late outbound builds = %d, want no build after shutdown admission", got)
	}
	if got := builder.closes.Load(); got != 1 {
		t.Fatalf("outbound closes = %d, want the original outbound retired", got)
	}
}

func TestShutdownHonorsContextWhenOutboundLeaseIsStillHeld(t *testing.T) {
	raw := []byte(`{"type":"shutdown-held-outbound"}`)
	hash := node.HashFromRawOptions(raw)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.AddNodeFromSub(hash, raw, "shutdown-sub")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("shutdown test node was not added")
	}
	rawOutbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&rawOutbound)
	_, release, ready := entry.AcquireOutbound()
	if !ready {
		t.Fatal("shutdown test could not acquire outbound lease")
	}

	manager := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	app := &resinApp{
		endpointManager: newEndpointRuntimeManager("127.0.0.1", "", nil, nil, nil, nil, nil, nil),
		topoRuntime:     &topologyRuntime{pool: pool, outboundMgr: manager},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan struct{})
	go func() {
		_ = app.shutdown(ctx)
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before its context deadline while an outbound lease was held")
	case <-ctx.Done():
	}
	select {
	case <-shutdownDone:
		// The app shutdown owner honored its caller deadline.
	case <-time.After(250 * time.Millisecond):
		release()
		<-shutdownDone
		t.Fatal("shutdown remained blocked in outbound retirement after its context deadline")
	}

	// The background retirement owner must still observe the eventual lease
	// release and complete the adapter close exactly once.
	release()
	if err := manager.ShutdownContext(context.Background()); err != nil {
		t.Fatalf("background outbound retirement: %v", err)
	}
}

func TestCloseOutboundForShutdownDefersBuilderCloseUntilOwnerCompletes(t *testing.T) {
	raw := []byte(`{"type":"shutdown-builder-owner"}`)
	hash := node.HashFromRawOptions(raw)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.AddNodeFromSub(hash, raw, "shutdown-sub")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("shutdown test node was not added")
	}
	rawOutbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&rawOutbound)
	_, release, ready := entry.AcquireOutbound()
	if !ready {
		t.Fatal("shutdown test could not acquire outbound lease")
	}

	manager := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	closeCalled := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err, continuation := closeOutboundForShutdown(
		ctx,
		manager.ShutdownContext,
		func() error {
			close(closeCalled)
			return nil
		},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("closeOutboundForShutdown error = %v, want deadline exceeded", err)
	}
	if continuation == nil {
		t.Fatal("timed-out outbound shutdown did not register a continuation")
	}
	select {
	case <-closeCalled:
		t.Fatal("builder closed before the outbound owner completed")
	default:
	}

	release()
	select {
	case continuationErr := <-continuation:
		if continuationErr != nil {
			t.Fatalf("outbound shutdown continuation: %v", continuationErr)
		}
	case <-time.After(time.Second):
		t.Fatal("outbound shutdown continuation did not finish after lease release")
	}
	select {
	case <-closeCalled:
	case <-time.After(time.Second):
		t.Fatal("builder was not closed after outbound owner completion")
	}
}
