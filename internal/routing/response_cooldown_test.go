package routing

import (
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/platform"
)

func TestRouter_ResponseCooldownSkipsEgressIPAndRestoresAfterExpiry(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-response", "Plat-Response", nil, nil)
	pool.addPlatform(plat)

	blockedHash, blockedEntry := newRoutableEntry(t, `{"id":"blocked"}`, "198.51.100.10")
	availableHash, availableEntry := newRoutableEntry(t, `{"id":"available"}`, "198.51.100.11")
	pool.addEntry(blockedHash, blockedEntry)
	pool.addEntry(availableHash, availableEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	route := RouteResult{PlatformID: plat.ID, NodeHash: blockedHash, EgressIP: blockedEntry.GetEgressIP()}
	until := time.Now().Add(100 * time.Millisecond)
	router.QuarantineRoute(route, platform.ResponseRuleScopeEgressIP, until)

	for i := 0; i < 20; i++ {
		got, err := router.RouteRequest(plat.Name, "", "https://example.com")
		if err != nil {
			t.Fatalf("route %d: %v", i, err)
		}
		if got.NodeHash != availableHash {
			t.Fatalf("route %d selected cooled node %s, want %s", i, got.NodeHash.Hex(), availableHash.Hex())
		}
	}

	if blockedEntry.GetEgressIP() != netip.MustParseAddr("198.51.100.10") {
		t.Fatal("test setup unexpectedly changed blocked egress IP")
	}
	time.Sleep(120 * time.Millisecond)
	seenBlocked := false
	for i := 0; i < 100; i++ {
		got, err := router.RouteRequest(plat.Name, "", "https://example.com")
		if err != nil {
			t.Fatalf("route after expiry %d: %v", i, err)
		}
		if got.NodeHash == blockedHash {
			seenBlocked = true
			break
		}
	}
	if !seenBlocked {
		t.Fatal("cooled node did not become eligible after expiry")
	}
}

func TestRouter_ResponseCooldownInvalidatesStickyLease(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-sticky-response", "Plat-Sticky-Response", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	blockedHash, blockedEntry := newRoutableEntry(t, `{"id":"sticky-blocked"}`, "198.51.100.20")
	availableHash, availableEntry := newRoutableEntry(t, `{"id":"sticky-available"}`, "198.51.100.21")
	pool.addEntry(blockedHash, blockedEntry)
	pool.addEntry(availableHash, availableEntry)
	pool.rebuildPlatformView(plat)

	router := newTestRouter(pool, nil)
	first, err := router.RouteRequest(plat.Name, "account-1", "https://example.com")
	if err != nil {
		t.Fatalf("create lease: %v", err)
	}
	state, _ := router.states.Load(plat.ID)
	lease, ok := state.Leases.GetLease("account-1")
	if !ok {
		t.Fatal("expected sticky lease")
	}
	if first.NodeHash != lease.NodeHash {
		t.Fatalf("lease mismatch: route=%s lease=%s", first.NodeHash.Hex(), lease.NodeHash.Hex())
	}

	expectedOther := blockedHash
	if first.NodeHash == blockedHash {
		expectedOther = availableHash
	}
	router.QuarantineRoute(first, platform.ResponseRuleScopeEgressIP, time.Now().Add(time.Minute))
	second, err := router.RouteRequest(plat.Name, "account-1", "https://example.com")
	if err != nil {
		t.Fatalf("route after sticky cooldown: %v", err)
	}
	if second.NodeHash != expectedOther {
		t.Fatalf("sticky route ignored cooldown: first=%s got=%s want=%s", first.NodeHash.Hex(), second.NodeHash.Hex(), expectedOther.Hex())
	}
}
