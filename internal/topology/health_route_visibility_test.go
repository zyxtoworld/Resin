package topology

import (
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func TestRouteRejectsCircuitOpenEntryBeforeViewNotificationCompletes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("health-route-sub", "HealthRoute", "url", true, false)
	subMgr.Register(sub)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 8,
		MaxConsecutiveFailures: func() int { return 1 },
	})
	raw := []byte(`{"type":"ss","server":"198.51.100.11"}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node entry not found")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("198.51.100.77"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        20 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	if !pool.RecordResultForEntry(hash, entry, true) {
		t.Fatal("initial health recovery was rejected")
	}

	plat := platform.NewPlatform("health-route-platform", "HealthRoute", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if got := plat.View().Size(); got != 1 {
		t.Fatalf("initial platform view size = %d, want 1", got)
	}

	notifyEntered := make(chan struct{})
	allowNotify := make(chan struct{})
	var notifyOnce sync.Once
	pool.beforePlatformNotifyHook = func(*platform.Platform) {
		notifyOnce.Do(func() {
			close(notifyEntered)
			<-allowNotify
		})
	}
	defer func() {
		select {
		case <-allowNotify:
		default:
			close(allowNotify)
		}
		pool.beforePlatformNotifyHook = nil
	}()

	healthDone := make(chan bool, 1)
	go func() {
		healthDone <- pool.RecordResultForEntry(hash, entry, false)
	}()
	select {
	case <-notifyEntered:
	case <-time.After(time.Second):
		t.Fatal("health update did not reach the platform-notification gate")
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("health update did not open the circuit before notification")
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})
	if _, err := router.RouteRequest(plat.Name, "", "example.com"); err == nil {
		t.Fatal("route selected a circuit-open entry while its view notification was blocked")
	}

	close(allowNotify)
	select {
	case applied := <-healthDone:
		if !applied {
			t.Fatal("health update was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("health update did not finish after releasing notification")
	}
}

func TestRouteRejectsDisabledEntryBeforeViewRebuildCompletes(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("disabled-route-sub", "DisabledRoute", "url", true, false)
	subMgr.Register(sub)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 8,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := []byte(`{"type":"ss","server":"198.51.100.12"}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"node"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node entry not found")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("198.51.100.78"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        20 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	if !pool.RecordResultForEntry(hash, entry, true) {
		t.Fatal("initial health recovery was rejected")
	}

	plat := platform.NewPlatform("disabled-route-platform", "DisabledRoute", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if got := plat.View().Size(); got != 1 {
		t.Fatalf("initial platform view size = %d, want 1", got)
	}

	rebuildEntered := make(chan struct{})
	allowRebuild := make(chan struct{})
	var rebuildOnce sync.Once
	pool.beforePlatformRebuildAllHook = func() {
		rebuildOnce.Do(func() {
			close(rebuildEntered)
			<-allowRebuild
		})
	}
	defer func() {
		select {
		case <-allowRebuild:
		default:
			close(allowRebuild)
		}
		pool.beforePlatformRebuildAllHook = nil
		pool.beforeRuntimeReadLockHook = nil
	}()

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
	})
	enableDone := make(chan struct{})
	go func() {
		scheduler.SetSubscriptionEnabled(sub, false)
		close(enableDone)
	}()
	select {
	case <-rebuildEntered:
	case <-time.After(time.Second):
		t.Fatal("subscription disable did not reach the platform-rebuild gate")
	}
	if sub.Enabled() {
		t.Fatal("subscription disable did not commit before rebuild")
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})
	routeDone := make(chan error, 1)
	go func() {
		_, err := router.RouteRequest(plat.Name, "", "example.com")
		routeDone <- err
	}()
	select {
	case err := <-routeDone:
		if !errors.Is(err, routing.ErrRuntimeGenerationBusy) {
			t.Fatalf("concurrent route error = %v, want runtime generation busy", err)
		}
	case <-time.After(time.Second):
		t.Fatal("route did not fail closed while the runtime generation was publishing")
	}

	close(allowRebuild)
	select {
	case <-enableDone:
	case <-time.After(time.Second):
		t.Fatal("subscription disable did not finish after releasing rebuild")
	}
	if _, err := router.RouteRequest(plat.Name, "", "example.com"); err == nil {
		t.Fatal("route succeeded after the subscription was disabled")
	}
}
