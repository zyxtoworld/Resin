package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
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

var _ adapter.Outbound = (*bootstrapCleanupTrackingOutbound)(nil)
var _ outbound.OutboundBuilder = (*bootstrapCleanupTrackingBuilder)(nil)
