package routing

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/topology"
)

func TestRouter_SnapshotLeasePageRejectsCursorAfterPlatformReplacement(t *testing.T) {
	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	router := NewRouter(RouterConfig{Pool: pool})

	const platformID = "lease-page-platform-generation"
	oldPlatform := platform.NewPlatform(platformID, "old-generation", nil, nil)
	if err := pool.RegisterPlatform(oldPlatform); err != nil {
		t.Fatalf("RegisterPlatform(old): %v", err)
	}

	now := time.Now().UnixNano()
	hash := node.HashFromRawOptions([]byte(`{"id":"lease-page-platform-generation-node"}`))
	for index, account := range []string{"old-0", "old-1"} {
		if err := router.UpsertLease(model.Lease{
			PlatformID:     platformID,
			Account:        account,
			NodeHash:       hash.Hex(),
			EgressIP:       "198.51.100.130",
			CreatedAtNs:    now,
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now + int64(index),
		}); err != nil {
			t.Fatalf("UpsertLease(%s): %v", account, err)
		}
	}

	query := LeasePageQuery{Limit: 1, SortBy: "account"}
	first, ok, err := router.SnapshotLeasePageForPlatform(platformID, query)
	if err != nil || !ok || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("old-generation first page = %#v ok=%v err=%v", first, ok, err)
	}

	// Match ControlPlaneService.DeletePlatform's runtime order: unregister the
	// pool platform first, then drain/delete the Router's complete state.
	pool.UnregisterPlatform(platformID)
	router.RemovePlatformState(platformID)

	newPlatform := platform.NewPlatform(platformID, "new-generation", nil, nil)
	if err := pool.RegisterPlatform(newPlatform); err != nil {
		t.Fatalf("RegisterPlatform(new): %v", err)
	}
	for index, account := range []string{"new-0", "new-1"} {
		if err := router.UpsertLease(model.Lease{
			PlatformID:     platformID,
			Account:        account,
			NodeHash:       hash.Hex(),
			EgressIP:       "198.51.100.131",
			CreatedAtNs:    now,
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now + int64(index),
		}); err != nil {
			t.Fatalf("UpsertLease(%s): %v", account, err)
		}
	}

	query.Cursor = first.NextCursor
	_, ok, err = router.SnapshotLeasePageForPlatform(platformID, query)
	if ok || !errors.Is(err, ErrLeaseCursorInvalid) {
		t.Fatalf("old-generation cursor accepted after replacement: ok=%v err=%v", ok, err)
	}
}
