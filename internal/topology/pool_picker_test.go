package topology

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func newPoolPickerTestPool(t *testing.T, filters []*regexp.Regexp) (*GlobalNodePool, *subscription.Subscription) {
	t.Helper()

	subManager := NewSubscriptionManager()
	sub := subscription.NewSubscription("picker-sub", "Picker Sub", "https://example.com", true, false)
	subManager.Register(sub)
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 8,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	defaultPlatform := platform.NewPlatform(
		platform.DefaultPlatformID,
		platform.DefaultPlatformName,
		filters,
		nil,
	)
	if err := pool.RegisterPlatform(defaultPlatform); err != nil {
		t.Fatalf("register default platform: %v", err)
	}
	return pool, sub
}

func addPickerTestNode(
	t *testing.T,
	pool *GlobalNodePool,
	sub *subscription.Subscription,
	raw string,
	tag string,
	ready bool,
) node.Hash {
	t.Helper()
	rawOptions := json.RawMessage(raw)
	hash := node.HashFromRawOptions(rawOptions)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})
	pool.AddNodeFromSub(hash, rawOptions, sub.ID)
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.CircuitOpenSince.Store(0)
	if ready {
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	entry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	return hash
}

func TestPickDefaultPlatformOutbound_SelectsOnlyReadyOutbound(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	addPickerTestNode(t, pool, sub, `{"picker":"not-ready"}`, "picker", false)
	ready := addPickerTestNode(t, pool, sub, `{"picker":"ready"}`, "picker", true)

	for i := 0; i < 20; i++ {
		got, _, err := pool.PickDefaultPlatformOutbound(context.Background())
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if got != ready {
			t.Fatalf("pick %d returned %s, want ready node %s", i, got.Hex(), ready.Hex())
		}
	}
}

func TestPickDefaultPlatformOutbound_AllNodesWithoutOutbound(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	addPickerTestNode(t, pool, sub, `{"picker":"not-ready-a"}`, "picker", false)
	addPickerTestNode(t, pool, sub, `{"picker":"not-ready-b"}`, "picker", false)

	_, _, err := pool.PickDefaultPlatformOutbound(context.Background())
	if !errors.Is(err, ErrNoAvailableOutbound) {
		t.Fatalf("PickDefaultPlatformOutbound error = %v, want ErrNoAvailableOutbound", err)
	}
}

func TestPickDefaultPlatformOutboundExcluding_SkipsAttemptedHash(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	first := addPickerTestNode(t, pool, sub, `{"picker":"first"}`, "picker", true)
	second := addPickerTestNode(t, pool, sub, `{"picker":"second"}`, "picker", true)

	got, _, err := pool.PickDefaultPlatformOutboundExcluding(context.Background(), []node.Hash{first})
	if err != nil {
		t.Fatalf("pick excluding first: %v", err)
	}
	if got != second {
		t.Fatalf("pick returned %s, want unattempted second node %s", got.Hex(), second.Hex())
	}

	if _, _, err := pool.PickDefaultPlatformOutboundExcluding(context.Background(), []node.Hash{first, second}); !errors.Is(err, ErrNoAvailableOutbound) {
		t.Fatalf("pick with all candidates excluded = %v, want ErrNoAvailableOutbound", err)
	}
}

func TestPickDefaultPlatformOutbound_RespectsDefaultFilters(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, []*regexp.Regexp{regexp.MustCompile("allowed")})
	addPickerTestNode(t, pool, sub, `{"picker":"excluded"}`, "excluded", true)
	allowed := addPickerTestNode(t, pool, sub, `{"picker":"allowed"}`, "allowed", true)

	for i := 0; i < 20; i++ {
		got, _, err := pool.PickDefaultPlatformOutbound(context.Background())
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if got != allowed {
			t.Fatalf("pick %d returned filtered node %s, want %s", i, got.Hex(), allowed.Hex())
		}
	}
}

func TestPickDefaultPlatformOutbound_EmptyView(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, []*regexp.Regexp{regexp.MustCompile("allowed")})
	addPickerTestNode(t, pool, sub, `{"picker":"excluded"}`, "excluded", true)

	_, _, err := pool.PickDefaultPlatformOutbound(context.Background())
	if !errors.Is(err, ErrNoAvailableOutbound) {
		t.Fatalf("PickDefaultPlatformOutbound error = %v, want ErrNoAvailableOutbound", err)
	}
}

func TestPickDefaultPlatformOutbound_DoesNotObserveInPlaceRebuildGap(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	defaultPlatform, ok := pool.GetPlatform(platform.DefaultPlatformID)
	if !ok {
		t.Fatal("default platform was not registered")
	}
	defaultPlatform.RegionFilters = []string{"us"}
	first := addPickerTestNode(t, pool, sub, `{"picker":"rebuild-first"}`, "picker", true)
	second := addPickerTestNode(t, pool, sub, `{"picker":"rebuild-second"}`, "picker", true)
	if !defaultPlatform.View().Contains(first) || !defaultPlatform.View().Contains(second) {
		t.Fatal("test nodes were not initially routable")
	}

	geoEntered := make(chan struct{})
	allowGeo := make(chan struct{})
	var geoOnce sync.Once
	pool.geoLookup = func(netip.Addr) string {
		geoOnce.Do(func() { close(geoEntered) })
		<-allowGeo
		return "us"
	}

	rebuildDone := make(chan struct{})
	go func() {
		pool.RebuildPlatform(defaultPlatform)
		close(rebuildDone)
	}()
	select {
	case <-geoEntered:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not reach its filter evaluation")
	}

	pickDone := make(chan error, 1)
	go func() {
		_, _, err := pool.PickDefaultPlatformOutbound(context.Background())
		pickDone <- err
	}()
	select {
	case err := <-pickDone:
		t.Fatalf("picker observed an in-place rebuild gap: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowGeo)
	select {
	case <-rebuildDone:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not complete")
	}
	select {
	case err := <-pickDone:
		if err != nil {
			t.Fatalf("picker after rebuild: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("picker did not finish after rebuild")
	}
}

func TestPickDefaultPlatformOutbound_RejectsStaleViewEntryIdentity(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, []*regexp.Regexp{regexp.MustCompile("allowed")})
	raw := json.RawMessage(`{"picker":"identity"}`)
	hash := node.HashFromRawOptions(raw)
	configureEntry := func(h node.Hash) {
		entry, ok := pool.GetEntry(h)
		if !ok {
			t.Fatalf("node %s missing during setup", h.Hex())
		}
		entry.CircuitOpenSince.Store(0)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
		entry.SetEgressIP(netip.MustParseAddr("203.0.113.11"))
		entry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	}
	pool.onNodeAdded = func(h node.Hash) { configureEntry(h) }
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"allowed"}})
	pool.AddNodeFromSub(hash, raw, sub.ID)
	oldEntry, ok := pool.GetEntry(hash)
	if !ok || !defaultViewContains(pool, hash) {
		t.Fatal("initial entry was not published in the default view")
	}

	// The new entry has the same content hash but no longer matches the
	// Default regex. Hold both node notifications before they take the
	// platform snapshot lock: the old view still contains hash while the new
	// entry is already live in the pool.
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"excluded"}})
	firstNotifyReached := make(chan struct{})
	secondNotifyReached := make(chan struct{})
	allowFirstNotify := make(chan struct{})
	allowSecondNotify := make(chan struct{})
	var notifyCalls atomic.Int32
	pool.beforePlatformSnapshotLockHook = func() {
		switch notifyCalls.Add(1) {
		case 1:
			close(firstNotifyReached)
			<-allowFirstNotify
		case 2:
			close(secondNotifyReached)
			<-allowSecondNotify
		}
	}
	defer func() {
		pool.beforePlatformSnapshotLockHook = nil
		select {
		case <-allowFirstNotify:
		default:
			close(allowFirstNotify)
		}
		select {
		case <-allowSecondNotify:
		default:
			close(allowSecondNotify)
		}
	}()

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, sub.ID)
		close(removeDone)
	}()
	select {
	case <-firstNotifyReached:
	case <-time.After(time.Second):
		t.Fatal("remove notification did not reach its barrier")
	}

	addDone := make(chan struct{})
	go func() {
		pool.AddNodeFromSub(hash, raw, sub.ID)
		close(addDone)
	}()
	select {
	case <-secondNotifyReached:
	case <-time.After(time.Second):
		t.Fatal("replacement notification did not reach its barrier")
	}

	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry || !newEntry.IsHealthy() {
		t.Fatal("replacement entry was not live and healthy in the stale-view window")
	}
	if _, _, err := pool.PickDefaultPlatformOutbound(context.Background()); !errors.Is(err, ErrNoAvailableOutbound) {
		t.Fatalf("picker selected stale view entry: %v", err)
	}

	close(allowFirstNotify)
	close(allowSecondNotify)
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("remove did not finish")
	}
	select {
	case <-addDone:
	case <-time.After(time.Second):
		t.Fatal("replacement add did not finish")
	}
}

func defaultViewContains(pool *GlobalNodePool, hash node.Hash) bool {
	plat, ok := pool.GetPlatform(platform.DefaultPlatformID)
	return ok && plat.View().Contains(hash)
}

func TestPickDefaultPlatformOutbound_ObservesCancellation(t *testing.T) {
	pool, _ := newPoolPickerTestPool(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := pool.PickDefaultPlatformOutbound(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PickDefaultPlatformOutbound error = %v, want context.Canceled", err)
	}
}
