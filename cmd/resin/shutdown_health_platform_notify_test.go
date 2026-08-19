package main

import (
	"context"
	"encoding/json"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestShutdownDeadlineDoesNotWaitOnAdmittedHealthPlatformNotification(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	defer closer.Close()

	subManager := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("shutdown-health-platform", "shutdown-health-platform", "url", true, false)
	subManager.Register(sub)

	hash := node.HashFromRawOptions(json.RawMessage(`{"type":"shutdown-health-platform","server":"198.51.100.21"}`))
	raw := json.RawMessage(`{"type":"shutdown-health-platform","server":"198.51.100.21"}`)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"shutdown"}})

	geoEntered := make(chan struct{})
	allowGeo := make(chan struct{})
	markResult := make(chan bool, 1)
	var blockGeo atomic.Bool
	var geoOnce sync.Once
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subManager.Lookup,
		GeoLookup: func(netip.Addr) string {
			if blockGeo.Load() {
				geoOnce.Do(func() { close(geoEntered) })
				<-allowGeo
			}
			return "us"
		},
		MaxLatencyTableEntries: 8,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged: func(hash node.Hash) {
			markResult <- engine.MarkNodeDynamic(hash.Hex())
		},
	})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("health entry missing")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("198.51.100.77"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        20 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	plat := platform.NewPlatform("shutdown-health-platform", "shutdown-health-platform", nil, []string{"us"})
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	blockGeo.Store(true)
	entry.CircuitOpenSince.Store(1)
	healthOwner := proxy.NewHealthWriteOwner(pool)
	healthDone := make(chan bool, 1)
	go func() {
		healthDone <- healthOwner.RecordPassiveResultForEntry("", hash, entry, true)
	}()
	select {
	case <-geoEntered:
	case <-time.After(time.Second):
		close(allowGeo)
		t.Fatal("health write did not reach the real platform GeoLookup")
	}

	releaseGeo := sync.OnceFunc(func() { close(allowGeo) })
	defer releaseGeo()
	transportPool := proxy.NewOutboundTransportPool(proxy.OutboundTransportConfig{})
	defer transportPool.Shutdown()
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
		func() int { return 1000 },
		func() time.Duration { return time.Hour },
		time.Hour,
	)
	flushWorker.Start()
	defer flushWorker.StopContext(context.Background())
	app := &resinApp{
		stateEngine:      engine,
		healthWriteOwner: healthOwner,
		transportPool:    transportPool,
		flushWorker:      flushWorker,
		topoRuntime:      &topologyRuntime{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan shutdownContinuations, 1)
	go func() { shutdownDone <- app.shutdown(ctx) }()
	var continuations shutdownContinuations
	select {
	case <-ctx.Done():
		select {
		case continuations = <-shutdownDone:
		case <-time.After(250 * time.Millisecond):
			releaseGeo()
			t.Fatal("shutdown did not return after its deadline")
		}
	case <-time.After(time.Second):
		releaseGeo()
		t.Fatal("shutdown did not reach its deadline while the admitted notification was blocked")
	}

	releaseGeo()
	if err := continuations.wait(); err != nil {
		t.Fatalf("shutdown continuations: %v", err)
	}
	select {
	case applied := <-markResult:
		if !applied {
			t.Fatal("admitted health callback wrote after state admission closed")
		}
	case <-time.After(time.Second):
		t.Fatal("health callback did not reach state persistence")
	}
	select {
	case applied := <-healthDone:
		if !applied {
			t.Fatal("admitted health write was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("admitted health write did not finish")
	}
	rows, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(rows) != 1 || rows[0].Hash != hash.Hex() || rows[0].CircuitOpenSince != 0 {
		t.Fatalf("health dynamic state was not persisted: %+v", rows)
	}
}
