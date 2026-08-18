package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/state"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type bootstrapCleanupTrackingBuilder struct {
	builds atomic.Int32
	closes atomic.Int32
}

type bootstrapCleanupTrackingOutbound struct {
	closes *atomic.Int32
}

type bootstrapWarmupFailureBuilder struct {
	builds atomic.Int32
	closes atomic.Int32
}

type bootstrapOrderedFailureBuilder struct {
	firstRaw  string
	secondRaw string

	firstEntered  chan struct{}
	secondEntered chan struct{}
	firstRelease  chan struct{}
	secondRelease chan struct{}
	firstDone     chan struct{}
	secondDone    chan struct{}

	firstEnteredOnce  sync.Once
	secondEnteredOnce sync.Once
	firstDoneOnce     sync.Once
	secondDoneOnce    sync.Once
	firstReleaseOnce  sync.Once
	secondReleaseOnce sync.Once

	builds atomic.Int32
	closes atomic.Int32
}

func (b *bootstrapOrderedFailureBuilder) releaseFirst() {
	b.firstReleaseOnce.Do(func() { close(b.firstRelease) })
}

func (b *bootstrapOrderedFailureBuilder) releaseSecond() {
	b.secondReleaseOnce.Do(func() { close(b.secondRelease) })
}

func (b *bootstrapWarmupFailureBuilder) Build(raw json.RawMessage) (adapter.Outbound, error) {
	b.builds.Add(1)
	if strings.Contains(string(raw), "bootstrap-warmup-fail") {
		return nil, errors.New("bootstrap warmup build failed")
	}
	return &bootstrapCleanupTrackingOutbound{closes: &b.closes}, nil
}

func (b *bootstrapOrderedFailureBuilder) Build(raw json.RawMessage) (adapter.Outbound, error) {
	b.builds.Add(1)
	switch string(raw) {
	case b.firstRaw:
		b.firstEnteredOnce.Do(func() { close(b.firstEntered) })
		<-b.firstRelease
		b.firstDoneOnce.Do(func() { close(b.firstDone) })
		return nil, errors.New("first warmup build failed")
	case b.secondRaw:
		b.secondEnteredOnce.Do(func() { close(b.secondEntered) })
		<-b.secondRelease
		b.secondDoneOnce.Do(func() { close(b.secondDone) })
		return nil, errors.New("second warmup build failed")
	default:
		return &bootstrapCleanupTrackingOutbound{closes: &b.closes}, nil
	}
}

func (o *bootstrapCleanupTrackingOutbound) Type() string { return "bootstrap-cleanup-test" }
func (o *bootstrapCleanupTrackingOutbound) Tag() string  { return "bootstrap-cleanup-test" }
func (o *bootstrapCleanupTrackingOutbound) Network() []string {
	return []string{"tcp", "udp"}
}
func (o *bootstrapCleanupTrackingOutbound) Dependencies() []string { return nil }
func (o *bootstrapCleanupTrackingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("bootstrap cleanup test outbound cannot dial")
}
func (o *bootstrapCleanupTrackingOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("bootstrap cleanup test outbound cannot listen")
}
func (o *bootstrapCleanupTrackingOutbound) Close() error {
	o.closes.Add(1)
	return nil
}

func (b *bootstrapCleanupTrackingBuilder) Build(json.RawMessage) (adapter.Outbound, error) {
	b.builds.Add(1)
	return &bootstrapCleanupTrackingOutbound{closes: &b.closes}, nil
}

func TestBootstrapNodes_ErrorRetiresWarmupOutbounds(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	raw := json.RawMessage(`{"type":"bootstrap-cleanup-test"}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: time.Now().UnixNano(),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}

	cacheDB, err := state.OpenDB(filepath.Join(cacheDir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenDB(cache): %v", err)
	}
	if _, err := cacheDB.Exec("DROP TABLE subscription_nodes"); err != nil {
		_ = cacheDB.Close()
		t.Fatalf("drop subscription_nodes: %v", err)
	}
	if err := cacheDB.Close(); err != nil {
		t.Fatalf("close cache DB: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	builder := &bootstrapCleanupTrackingBuilder{}
	outboundMgr := outbound.NewOutboundManager(pool, builder)

	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err == nil {
		t.Fatal("bootstrapNodes unexpectedly succeeded after subscription_nodes was removed")
	}
	if builder.builds.Load() != 1 {
		t.Fatalf("warmup builds = %d, want 1", builder.builds.Load())
	}
	if builder.closes.Load() != builder.builds.Load() {
		t.Fatalf("warmup outbound closes = %d, want %d after bootstrap rollback", builder.closes.Load(), builder.builds.Load())
	}
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("warmed node %s disappeared unexpectedly", hash.Hex())
	}
	if entry.Outbound.Load() != nil {
		t.Fatal("failed bootstrap left an outbound published on the discarded node pool")
	}
}

func TestBootstrapNodes_FailsAndRetiresWhenWarmupBuildFails(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	workingRaw := json.RawMessage(`{"type":"bootstrap-warmup-working"}`)
	failingRaw := json.RawMessage(`{"type":"bootstrap-warmup-fail"}`)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{
		{Hash: node.HashFromRawOptions(workingRaw).Hex(), RawOptions: workingRaw, CreatedAtNs: time.Now().UnixNano()},
		{Hash: node.HashFromRawOptions(failingRaw).Hex(), RawOptions: failingRaw, CreatedAtNs: time.Now().UnixNano()},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	builder := &bootstrapWarmupFailureBuilder{}
	outboundMgr := outbound.NewOutboundManager(pool, builder)

	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err == nil {
		t.Fatal("bootstrapNodes unexpectedly succeeded after a warmup outbound build failed")
	}
	if got := builder.builds.Load(); got != 2 {
		t.Fatalf("warmup builds = %d, want 2", got)
	}
	if got := builder.closes.Load(); got != 1 {
		t.Fatalf("successful warmup outbound closes = %d, want 1 during rollback", got)
	}
	workingEntry, ok := pool.GetEntry(node.HashFromRawOptions(workingRaw))
	if !ok {
		t.Fatal("working bootstrap node disappeared unexpectedly")
	}
	if workingEntry.Outbound.Load() != nil {
		t.Fatal("failed bootstrap left a working outbound published")
	}
	failingEntry, ok := pool.GetEntry(node.HashFromRawOptions(failingRaw))
	if !ok {
		t.Fatal("failing bootstrap node disappeared unexpectedly")
	}
	if failingEntry.Outbound.Load() != nil {
		t.Fatal("failed warmup node published an outbound")
	}
}

func TestWarmupBootstrapOutbounds_ReturnsInputOrderErrorAfterReverseCompletion(t *testing.T) {
	previousMaxProcs := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousMaxProcs) })

	firstRaw := json.RawMessage(`{"type":"bootstrap-warmup-first-fail"}`)
	secondRaw := json.RawMessage(`{"type":"bootstrap-warmup-second-fail"}`)
	workingRaw := json.RawMessage(`{"type":"bootstrap-warmup-working"}`)
	firstHash := node.HashFromRawOptions(firstRaw)
	secondHash := node.HashFromRawOptions(secondRaw)
	workingHash := node.HashFromRawOptions(workingRaw)

	_, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	for _, item := range []struct {
		hash node.Hash
		raw  json.RawMessage
	}{
		{hash: firstHash, raw: firstRaw},
		{hash: secondHash, raw: secondRaw},
		{hash: workingHash, raw: workingRaw},
	} {
		pool.LoadNodeFromBootstrap(node.NewNodeEntry(item.hash, item.raw, time.Now(), 16))
	}

	builder := &bootstrapOrderedFailureBuilder{
		firstRaw:      string(firstRaw),
		secondRaw:     string(secondRaw),
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
		firstRelease:  make(chan struct{}),
		secondRelease: make(chan struct{}),
		firstDone:     make(chan struct{}),
		secondDone:    make(chan struct{}),
	}
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	t.Cleanup(func() {
		builder.releaseFirst()
		builder.releaseSecond()
		outboundMgr.RetireAllOutboundsAndWait()
	})

	warmupDone := make(chan error, 1)
	go func() {
		warmupDone <- warmupBootstrapOutbounds([]node.Hash{firstHash, secondHash, workingHash}, pool, outboundMgr)
	}()

	select {
	case <-builder.firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first failing build did not enter")
	}
	select {
	case <-builder.secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second failing build did not enter")
	}

	builder.releaseSecond()
	select {
	case <-builder.secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second failing build did not complete after release")
	}
	select {
	case <-builder.firstDone:
		t.Fatal("input-first failing build completed before its release")
	default:
	}

	builder.releaseFirst()
	var err error
	select {
	case err = <-warmupDone:
	case <-time.After(2 * time.Second):
		t.Fatal("warmup did not finish after all build gates were released")
	}
	if err == nil || !strings.Contains(err.Error(), "first warmup build failed") {
		t.Fatalf("warmup error = %v, want input-first failure", err)
	}
	if strings.Contains(err.Error(), "second warmup build failed") {
		t.Fatalf("warmup returned later input failure: %v", err)
	}
	if got := builder.builds.Load(); got != 3 {
		t.Fatalf("warmup builds = %d, want all 3 inputs processed", got)
	}

	outboundMgr.RetireAllOutboundsAndWait()
	if got := builder.closes.Load(); got != 1 {
		t.Fatalf("successful warmup outbound closes = %d, want 1", got)
	}
	pool.RangeNodes(func(hash node.Hash, entry *node.NodeEntry) bool {
		if entry.Outbound.Load() != nil {
			t.Errorf("node %s retained a published outbound after failed warmup", hash.Hex())
		}
		return true
	})
}

var _ adapter.Outbound = (*bootstrapCleanupTrackingOutbound)(nil)
var _ outbound.OutboundBuilder = (*bootstrapCleanupTrackingBuilder)(nil)
