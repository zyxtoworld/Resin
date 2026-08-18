package service

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

type previewFilterFixture struct {
	cp          *ControlPlaneService
	hkHash      string
	usHash      string
	unknownHash string
}

func buildPreviewFilterFixture(t *testing.T) previewFilterFixture {
	t.Helper()

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub-1", "sub-1", "https://example.com/sub", true, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})

	hkRaw := []byte(`{"type":"ss","server":"1.1.1.1","port":443}`)
	hkHash := node.HashFromRawOptions(hkRaw)
	pool.AddNodeFromSub(hkHash, hkRaw, sub.ID)
	sub.ManagedNodes().StoreNode(hkHash, subscription.ManagedNode{Tags: []string{"all", "hk"}})

	usRaw := []byte(`{"type":"ss","server":"2.2.2.2","port":443}`)
	usHash := node.HashFromRawOptions(usRaw)
	pool.AddNodeFromSub(usHash, usRaw, sub.ID)
	sub.ManagedNodes().StoreNode(usHash, subscription.ManagedNode{Tags: []string{"all", "us"}})

	unknownRaw := []byte(`{"type":"ss","server":"3.3.3.3","port":443}`)
	unknownHash := node.HashFromRawOptions(unknownRaw)
	pool.AddNodeFromSub(unknownHash, unknownRaw, sub.ID)
	sub.ManagedNodes().StoreNode(unknownHash, subscription.ManagedNode{Tags: []string{"all", "unknown"}})

	hkEntry, ok := pool.GetEntry(hkHash)
	if !ok {
		t.Fatal("hk entry missing")
	}
	hkOutbound := testutil.NewNoopOutbound()
	hkEntry.Outbound.Store(&hkOutbound)
	hkEntry.SetEgressIP(netip.MustParseAddr("1.1.1.1"))
	hkEntry.SetEgressRegion("hk")

	usEntry, ok := pool.GetEntry(usHash)
	if !ok {
		t.Fatal("us entry missing")
	}
	usOutbound := testutil.NewNoopOutbound()
	usEntry.Outbound.Store(&usOutbound)
	usEntry.SetEgressIP(netip.MustParseAddr("2.2.2.2"))
	usEntry.SetEgressRegion("us")

	unknownEntry, ok := pool.GetEntry(unknownHash)
	if !ok {
		t.Fatal("unknown entry missing")
	}
	unknownOutbound := testutil.NewNoopOutbound()
	unknownEntry.Outbound.Store(&unknownOutbound)
	unknownEntry.SetEgressIP(netip.MustParseAddr("3.3.3.3"))

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
	}
	return previewFilterFixture{
		cp:          cp,
		hkHash:      hkHash.Hex(),
		usHash:      usHash.Hex(),
		unknownHash: unknownHash.Hex(),
	}
}

func TestPreviewFilter_RegionNegation(t *testing.T) {
	fixture := buildPreviewFilterFixture(t)

	nodes, err := fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters:  []string{".*"},
			RegionFilters: []string{"!hk"},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != fixture.usHash {
		t.Fatalf("matched node = %s, want %s", nodes[0].NodeHash, fixture.usHash)
	}
	if nodes[0].NodeHash == fixture.hkHash {
		t.Fatalf("hk node %s should have been excluded", fixture.hkHash)
	}
}

func TestPreviewFilter_RegexRulesAnyMustAndMustNot(t *testing.T) {
	fixture := buildPreviewFilterFixture(t)

	nodes, err := fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters: []string{"hk", "us"},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter ANY: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ANY nodes len = %d, want 2", len(nodes))
	}

	nodes, err = fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters: []string{"hk", "us", `*^sub-1/`},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter MUST: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("MUST nodes len = %d, want 2", len(nodes))
	}

	nodes, err = fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters: []string{"all", "!hk", "!unknown"},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter MUST_NOT: %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != fixture.usHash {
		t.Fatalf("MUST_NOT nodes = %+v, want only %s", nodes, fixture.usHash)
	}
}

func TestPreviewFilter_RegionMixedIncludeExclude(t *testing.T) {
	fixture := buildPreviewFilterFixture(t)

	nodes, err := fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters:  []string{".*"},
			RegionFilters: []string{"hk", "!us"},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != fixture.hkHash {
		t.Fatalf("matched node = %s, want %s", nodes[0].NodeHash, fixture.hkHash)
	}

	nodes, err = fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters:  []string{".*"},
			RegionFilters: []string{"hk", "!hk"},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes len = %d, want 0", len(nodes))
	}
}

func TestPreviewFilter_RegionNegation_UnknownRegionExcluded(t *testing.T) {
	fixture := buildPreviewFilterFixture(t)

	nodes, err := fixture.cp.PreviewFilter(PreviewFilterRequest{
		PlatformSpec: &PlatformSpecFilter{
			RegexFilters:  []string{".*"},
			RegionFilters: []string{"!hk"},
		},
	})
	if err != nil {
		t.Fatalf("PreviewFilter: %v", err)
	}

	for _, node := range nodes {
		if node.NodeHash == fixture.unknownHash {
			t.Fatalf("node with unknown region %s should not match region filters", fixture.unknownHash)
		}
	}
}

func TestPreviewFilterContextRejectsInvalidRequestBeforeRuntimeMutation(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	service := &ControlPlaneService{Pool: pool, SubMgr: subMgr}

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		pool.WithRuntimeMutation(func() {
			close(writerEntered)
			<-releaseWriter
		})
		close(writerDone)
	}()
	select {
	case <-writerEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime mutation did not enter")
	}
	t.Cleanup(func() {
		select {
		case <-releaseWriter:
		default:
			close(releaseWriter)
		}
		select {
		case <-writerDone:
		case <-time.After(time.Second):
			t.Error("runtime mutation did not finish during cleanup")
		}
	})

	readAttempted := make(chan struct{})
	service.afterRuntimeReadAttemptHook = func(error) {
		close(readAttempted)
	}
	t.Cleanup(func() { service.afterRuntimeReadAttemptHook = nil })

	result := make(chan error, 1)
	go func() {
		_, err := service.PreviewFilterContext(context.Background(), PreviewFilterRequest{})
		result <- err
	}()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("invalid preview filter request unexpectedly succeeded")
		}
		var serviceErr *ServiceError
		if !errors.As(err, &serviceErr) || serviceErr.Code != "INVALID_ARGUMENT" {
			t.Fatalf("invalid preview filter error = %v, want INVALID_ARGUMENT", err)
		}
	case <-time.After(time.Second):
		t.Fatal("invalid preview filter waited for the unrelated runtime mutation")
	}

	select {
	case <-readAttempted:
		t.Fatal("invalid preview filter entered runtime read before validation")
	default:
	}
}
