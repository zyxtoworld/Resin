package platform

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestFullRebuildContextCancellationDoesNotWaitForViewWriter(t *testing.T) {
	p := NewPlatform("view-lock-cancel", "view-lock-cancel", nil, []string{"us"})
	hash := node.HashFromRawOptions([]byte(`{"type":"ss","server":"198.51.100.93"}`))
	entry := makeFullyRoutableEntry(hash, "sub")
	poolRange := func(fn func(node.Hash, *node.NodeEntry) bool) {
		fn(hash, entry)
	}
	if err := p.FullRebuildContext(context.Background(), poolRange, alwaysLookup, usGeoLookup); err != nil {
		t.Fatalf("initial FullRebuildContext: %v", err)
	}

	var blockGeo atomic.Bool
	geoEntered := make(chan struct{})
	allowGeo := make(chan struct{})
	var geoOnce sync.Once
	geoLookup := func(netip.Addr) string {
		if blockGeo.Load() {
			geoOnce.Do(func() { close(geoEntered) })
			<-allowGeo
		}
		return "us"
	}

	blockGeo.Store(true)
	notifyDone := make(chan struct{})
	go func() {
		p.NotifyDirty(hash, func(node.Hash) (*node.NodeEntry, bool) {
			return entry, true
		}, alwaysLookup, geoLookup)
		close(notifyDone)
	}()
	select {
	case <-geoEntered:
	case <-time.After(time.Second):
		close(allowGeo)
		t.Fatal("NotifyDirty did not enter its real GeoLookup path")
	}

	viewLockAttempted := make(chan struct{})
	var attemptOnce sync.Once
	p.beforeViewWriterLockHook = func() {
		attemptOnce.Do(func() { close(viewLockAttempted) })
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- p.FullRebuildContext(ctx, poolRange, alwaysLookup, geoLookup)
	}()
	select {
	case <-viewLockAttempted:
	case <-time.After(time.Second):
		close(allowGeo)
		t.Fatal("rebuild did not reach the view-lock boundary")
	}

	cancel()
	select {
	case err := <-rebuildDone:
		if !errors.Is(err, context.Canceled) {
			close(allowGeo)
			t.Fatalf("canceled rebuild error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		close(allowGeo)
		<-notifyDone
		t.Fatal("canceled rebuild waited for NotifyDirty's viewMu holder")
	}

	close(allowGeo)
	select {
	case <-notifyDone:
	case <-time.After(time.Second):
		t.Fatal("NotifyDirty did not finish after GeoLookup release")
	}
}
