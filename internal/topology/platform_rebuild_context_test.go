package topology

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func TestRebuildPlatformIfCurrentContextCancellationDoesNotWaitForReplaceLock(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"rebuild-lock-cancel-sub",
		"rebuild-lock-cancel-sub",
		"https://example.invalid/rebuild-lock-cancel",
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

	raw := []byte(`{"type":"ss","server":"198.51.100.92","port":443}`)
	hash := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
	entry := node.NewNodeEntry(hash, raw, time.Now(), 16)
	entry.AddSubscriptionID(sub.ID)
	var outbound = testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.92"))
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        50 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	pool.LoadNodeFromBootstrap(entry)

	platformID := "platform-rebuild-lock-cancel"
	current := platform.NewPlatform(platformID, "current", nil, []string{"us"})
	if err := pool.RegisterPlatform(current); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if !current.View().Contains(hash) {
		t.Fatal("fixture platform did not publish its initial routable view")
	}

	blockGeo.Store(true)
	next := platform.NewPlatform(platformID, "next", nil, []string{"us"})
	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplacePlatform(next) }()
	select {
	case <-geoEntered:
	case <-time.After(time.Second):
		close(allowGeo)
		t.Fatal("replacement did not enter its real platform rebuild")
	}

	readLockAttempted := make(chan struct{})
	var readOnce sync.Once
	pool.beforePlatformReadLockHook = func() {
		readOnce.Do(func() { close(readLockAttempted) })
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var releaseOnce sync.Once
	releaseGeo := func() {
		releaseOnce.Do(func() { close(allowGeo) })
	}
	defer releaseGeo()
	readDone := make(chan struct{})
	var readOK bool
	var readErr error
	go func() {
		readOK, readErr = pool.RebuildPlatformIfCurrentContext(ctx, platformID, current)
		close(readDone)
	}()
	select {
	case <-readLockAttempted:
	case <-time.After(time.Second):
		close(allowGeo)
		t.Fatal("rebuild request did not reach the platform read-lock boundary")
	}

	cancel()
	select {
	case <-readDone:
		if readOK || !errors.Is(readErr, context.Canceled) {
			releaseGeo()
			t.Fatalf("canceled rebuild result = (ok=%v, err=%v), want (false, context.Canceled)", readOK, readErr)
		}
	case <-time.After(time.Second):
		releaseGeo()
		<-replaceDone
		t.Fatal("canceled rebuild waited for ReplacePlatform's platform writer")
	}

	releaseGeo()
	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplacePlatform: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish after GeoLookup release")
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("canceled rebuild did not finish after replacement released platform lock")
	}
	if readOK || !errors.Is(readErr, context.Canceled) {
		t.Fatalf("canceled rebuild result = (ok=%v, err=%v), want (false, context.Canceled)", readOK, readErr)
	}
}
