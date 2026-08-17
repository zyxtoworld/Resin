package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

func TestCloseUnstartedResourcesStopsStartedGeoIPService(t *testing.T) {
	cacheDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cacheDir, "country.mmdb"), []byte("test"), 0o600); err != nil {
		t.Fatalf("write GeoIP fixture: %v", err)
	}

	service := geoip.NewService(geoip.ServiceConfig{
		CacheDir:       cacheDir,
		UpdateSchedule: "@every 1h",
		OpenDB:         geoip.NoOpOpen,
	})
	t.Cleanup(service.Stop)
	if err := service.Start(); err != nil {
		t.Fatalf("GeoIP Start: %v", err)
	}

	app := &resinApp{geoSvc: service}
	app.closeUnstartedResources()

	if err := service.Start(); !errors.Is(err, context.Canceled) {
		t.Fatalf("GeoIP remained restartable after startup rollback: got %v, want context.Canceled", err)
	}
}

func TestCloseUnstartedResourcesFlushesBootstrapDirtyDeletes(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer func() { _ = closer.Close() }()

	const subID = "startup-rollback-sub"
	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.240","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "StartupRollback",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Second),
		Enabled:          true,
		CreatedAtNs:      1,
		UpdatedAtNs:      1,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: 1,
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hashHex,
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodeLatency([]model.NodeLatency{
		{NodeHash: hashHex, Domain: "keep.example", EwmaNs: 1, LastUpdatedNs: 2},
		{NodeHash: hashHex, Domain: "drop.example", EwmaNs: 1, LastUpdatedNs: 1},
	}); err != nil {
		t.Fatalf("BulkUpsertNodeLatency: %v", err)
	}

	subManager := topology.NewSubscriptionManager()
	subManager.Register(subscription.NewSubscription(subID, "StartupRollback", "https://example.com/sub", true, false))
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.AddNodeFromSub(hash, raw, subID)
	if err := restoreBootstrapNodeLatencies(engine, pool, 1, nil); err != nil {
		t.Fatalf("restoreBootstrapNodeLatencies: %v", err)
	}
	if dirty := engine.DirtyCount(); dirty != 1 {
		t.Fatalf("bootstrap trim dirty count = %d, want 1", dirty)
	}

	app := &resinApp{
		flushWorker: state.NewCacheFlushWorker(
			engine,
			state.CacheReaders{},
			func() int { return 10_000 },
			func() time.Duration { return time.Hour },
			time.Hour,
		),
	}
	app.closeUnstartedResources()
	if err := closer.Close(); err != nil {
		t.Fatalf("close first persistence owner: %v", err)
	}

	engine2, closer2, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("reopen persistence: %v", err)
	}
	defer func() { _ = closer2.Close() }()
	rows, err := engine2.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency after rollback: %v", err)
	}
	for _, row := range rows {
		if row.NodeHash == hashHex && row.Domain == "drop.example" {
			t.Fatal("bootstrap-trimmed latency row survived startup rollback")
		}
	}
}
