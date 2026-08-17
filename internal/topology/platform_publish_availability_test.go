package topology

import (
	"encoding/json"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func TestReplacePlatformDoesNotBlockRouteReadersWhileBuildingCandidate(t *testing.T) {
	runPlatformPublishAvailabilityTest(t, false)
}

func TestRegisterPlatformDoesNotBlockRouteReadersWhileBuildingCandidate(t *testing.T) {
	runPlatformPublishAvailabilityTest(t, true)
}

func runPlatformPublishAvailabilityTest(t *testing.T, register bool) {
	t.Helper()
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"platform-publish-availability-sub",
		"PlatformPublishAvailability",
		"https://example.invalid/platform-publish-availability",
		true,
		false,
	)
	subMgr.Register(sub)

	var blockGeo atomic.Bool
	geoEntered := make(chan struct{})
	allowGeo := make(chan struct{})
	var geoOnce sync.Once
	pool := newTestPool(subMgr)
	pool.geoLookup = func(netip.Addr) string {
		if blockGeo.Load() {
			geoOnce.Do(func() { close(geoEntered) })
			<-allowGeo
		}
		return "us"
	}

	raw := json.RawMessage(`{"type":"platform-publish-availability","server":"198.51.100.91","port":443}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
	entry := node.NewNodeEntry(hash, raw, time.Now(), 16)
	entry.AddSubscriptionID(sub.ID)
	var outbound = testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.91"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        50 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	pool.LoadNodeFromBootstrap(entry)

	platformID := "platform-publish-availability"
	current := platform.NewPlatform(platformID, "current", nil, nil)
	if err := pool.RegisterPlatform(current); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if !current.View().Contains(hash) {
		t.Fatal("fixture platform did not publish its initial routable view")
	}

	blockGeo.Store(true)
	nextID := platformID
	if register {
		nextID += "-new"
	}
	next := platform.NewPlatform(nextID, "next", nil, []string{"us"})
	replaceDone := make(chan error, 1)
	go func() {
		if register {
			replaceDone <- pool.RegisterPlatform(next)
			return
		}
		replaceDone <- pool.ReplacePlatform(next)
	}()
	select {
	case <-geoEntered:
	case <-time.After(time.Second):
		close(allowGeo)
		t.Fatal("replacement did not enter its candidate rebuild")
	}

	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return []string{"example.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
	})
	routeDone := make(chan error, 1)
	go func() {
		_, err := router.RouteRequest("current", "", "https://example.com")
		routeDone <- err
	}()

	select {
	case err := <-routeDone:
		if err != nil {
			t.Fatalf("route through the published platform failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(allowGeo)
		<-replaceDone
		t.Fatal("route reader blocked behind unpublished platform candidate rebuild")
	}

	close(allowGeo)
	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish after candidate rebuild release")
	}
}
