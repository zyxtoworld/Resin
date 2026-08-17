package routing

import (
	"math"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func TestLeaseIsExpiredAtExactDeadline(t *testing.T) {
	now := time.Unix(1_700_000_000, 123).UTC()
	lease := Lease{ExpiryNs: now.UnixNano()}
	if !lease.IsExpired(now) {
		t.Fatal("lease must be expired at its exact deadline")
	}
}

func TestRouterCreateLeaseRejectsUnixNanoExpiryOverflow(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-expiry-overflow", "ExpiryOverflow", nil, nil)
	plat.StickyTTLNs = math.MaxInt64
	pool.addPlatform(plat)

	hash, entry := newRoutableEntry(t, `{"id":"expiry-overflow"}`, "198.51.100.31")
	pool.addEntry(hash, entry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)
	state := NewPlatformRoutingState()
	now := time.Unix(1_700_000_000, 123).UTC()

	lease, _, err := router.createLease(plat, state, "example.com", now, now.UnixNano())
	if err == nil {
		t.Fatalf("expected sticky ttl overflow error, got lease expiry %d", lease.ExpiryNs)
	}
}

func TestRouterReplacementDoesNotWrapMaxExpiry(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-expiry-replace", "ExpiryReplace", nil, nil)
	now := time.Unix(1_700_000_000, 123).UTC()
	plat.StickyTTLNs = math.MaxInt64 - now.UnixNano()
	pool.addPlatform(plat)

	_, entry := newRoutableEntry(t, `{"id":"expiry-replace"}`, "198.51.100.32")
	hash := entry.Hash
	pool.addEntry(hash, entry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)
	state := NewPlatformRoutingState()

	previous := Lease{
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"missing-previous"}`)),
		ExpiryNs:    math.MaxInt64,
		CreatedAtNs: now.UnixNano(),
	}
	newLease, _, result, err := router.decideStickyLease(
		plat,
		state,
		"acct-expiry-replace",
		"example.com",
		now,
		now.UnixNano(),
		previous,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("decideStickyLease: %v", err)
	}
	if !result.LeaseCreated {
		t.Fatal("expected replacement path to create a new lease")
	}
	if newLease.ExpiryNs <= now.UnixNano() {
		t.Fatalf("replacement expiry wrapped or expired: got %d, now %d", newLease.ExpiryNs, now.UnixNano())
	}
	if newLease.ExpiryNs != math.MaxInt64-1 {
		t.Fatalf("replacement expiry changed unexpectedly: got %d, want %d", newLease.ExpiryNs, int64(math.MaxInt64-1))
	}
}

func TestRouterReplacementRejectsMaxExpiryWithOnlyOneNanosecondHeadroom(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-expiry-one-ns", "ExpiryOneNS", nil, nil)
	now := time.Unix(0, math.MaxInt64-1).UTC()
	if now.UnixNano() != math.MaxInt64-1 {
		t.Fatalf("test clock UnixNano = %d, want %d", now.UnixNano(), int64(math.MaxInt64-1))
	}
	plat.StickyTTLNs = 1
	pool.addPlatform(plat)

	_, entry := newRoutableEntry(t, `{"id":"expiry-one-ns"}`, "198.51.100.33")
	pool.addEntry(entry.Hash, entry)
	pool.rebuildPlatformView(plat)
	router := newTestRouter(pool, nil)
	state := NewPlatformRoutingState()
	previous := Lease{
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"missing-previous-one-ns"}`)),
		ExpiryNs:    math.MaxInt64,
		CreatedAtNs: now.UnixNano(),
	}

	_, _, _, err := router.decideStickyLease(
		plat,
		state,
		"acct-expiry-one-ns",
		"example.com",
		now,
		now.UnixNano(),
		previous,
		true,
		nil,
	)
	if err == nil {
		t.Fatal("expected replacement to fail when MaxInt64-1 is already the current time")
	}
}
