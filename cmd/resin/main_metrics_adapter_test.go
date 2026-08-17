package main

import (
	"context"
	"net/netip"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestNodePoolStatsAdapter_HealthyNodesRequiresOutbound(t *testing.T) {
	subMgr, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	adapter := &runtimeStatsAdapter{pool: pool}

	enabledSub := subscription.NewSubscription("sub-enabled", "enabled", "https://example.com/enabled", true, false)
	disabledSub := subscription.NewSubscription("sub-disabled", "disabled", "https://example.com/disabled", false, false)
	subMgr.Register(enabledSub)
	subMgr.Register(disabledSub)

	healthyHash := node.HashFromRawOptions([]byte(`{"type":"direct","server":"1.1.1.1","port":443}`))
	healthy := node.NewNodeEntry(healthyHash, nil, time.Now(), 0)
	healthy.AddSubscriptionID(enabledSub.ID)
	enabledSub.ManagedNodes().StoreNode(healthyHash, subscription.ManagedNode{Tags: []string{"healthy"}})
	healthyOb := testutil.NewNoopOutbound()
	healthy.Outbound.Store(&healthyOb)
	healthy.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	pool.LoadNodeFromBootstrap(healthy)

	noOutboundHash := node.HashFromRawOptions([]byte(`{"type":"direct","server":"2.2.2.2","port":443}`))
	noOutbound := node.NewNodeEntry(noOutboundHash, nil, time.Now(), 0)
	noOutbound.AddSubscriptionID(enabledSub.ID)
	enabledSub.ManagedNodes().StoreNode(noOutboundHash, subscription.ManagedNode{Tags: []string{"no-outbound"}})
	noOutbound.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	pool.LoadNodeFromBootstrap(noOutbound)

	circuitOpenHash := node.HashFromRawOptions([]byte(`{"type":"direct","server":"3.3.3.3","port":443}`))
	circuitOpen := node.NewNodeEntry(circuitOpenHash, nil, time.Now(), 0)
	circuitOpen.AddSubscriptionID(enabledSub.ID)
	enabledSub.ManagedNodes().StoreNode(circuitOpenHash, subscription.ManagedNode{Tags: []string{"circuit-open"}})
	circuitOpenOb := testutil.NewNoopOutbound()
	circuitOpen.Outbound.Store(&circuitOpenOb)
	circuitOpen.SetEgressIP(netip.MustParseAddr("203.0.113.11"))
	circuitOpen.CircuitOpenSince.Store(time.Now().UnixNano())
	pool.LoadNodeFromBootstrap(circuitOpen)

	disabledHash := node.HashFromRawOptions([]byte(`{"type":"direct","server":"4.4.4.4","port":443}`))
	disabled := node.NewNodeEntry(disabledHash, nil, time.Now(), 0)
	disabled.AddSubscriptionID(disabledSub.ID)
	disabledSub.ManagedNodes().StoreNode(disabledHash, subscription.ManagedNode{Tags: []string{"disabled"}})
	disabledOb := testutil.NewNoopOutbound()
	disabled.Outbound.Store(&disabledOb)
	disabled.SetEgressIP(netip.MustParseAddr("203.0.113.12"))
	pool.LoadNodeFromBootstrap(disabled)

	if got, want := adapter.HealthyNodes(), 1; got != want {
		t.Fatalf("healthy_nodes: got %d, want %d", got, want)
	}
	if got, want := adapter.UniqueHealthyEgressIPCount(), 1; got != want {
		t.Fatalf("unique_healthy_egress_ips: got %d, want %d", got, want)
	}
}

func newRuntimeStatsGenerationFixture(t *testing.T) (*runtimeStatsAdapter, func()) {
	t.Helper()
	subMgr, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	sub := subscription.NewSubscription("stats-generation-sub", "Stats", "https://example.com/stats", true, false)
	subMgr.Register(sub)

	raw := []byte(`{"type":"direct","server":"198.51.100.40","port":443}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"allowed"}})

	entryA := node.NewNodeEntry(h, raw, time.Now(), 16)
	entryA.AddSubscriptionID(sub.ID)
	entryAOutbound := testutil.NewNoopOutbound()
	entryA.Outbound.Store(&entryAOutbound)
	entryA.SetEgressIP(netip.MustParseAddr("203.0.113.40"))
	entryA.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        25 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	pool.LoadNodeFromBootstrap(entryA)

	const platformID = "stats-generation-platform"
	platA := platform.NewPlatform(
		platformID,
		"Stats generation",
		[]*regexp.Regexp{regexp.MustCompile("allowed")},
		nil,
	)
	if err := pool.RegisterPlatform(platA); err != nil {
		t.Fatalf("RegisterPlatform(A): %v", err)
	}
	if !platA.View().Contains(h) {
		t.Fatal("platform A should contain entry A")
	}

	adapter := &runtimeStatsAdapter{
		pool:        pool,
		authorities: func() []string { return []string{"example.com"} },
	}
	installSwap := func() {
		var swapOnce sync.Once
		adapter.beforePlatformStatsViewSnapshotHook = func() {
			swapOnce.Do(func() {
				// This is the real lifecycle sequence: the metric has captured A's
				// platform pointer, then the platform and same-hash node are deleted
				// and recreated before the old view is traversed.
				pool.UnregisterPlatform(platformID)
				sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"excluded"}})
				pool.RemoveNodeFromSub(h, sub.ID)
				pool.AddNodeFromSub(h, raw, sub.ID)
				entryB, ok := pool.GetEntry(h)
				if !ok || entryB == entryA {
					t.Fatalf("same-hash replacement did not create entry B: ok=%v entry=%p old=%p", ok, entryB, entryA)
				}
				entryB.CircuitOpenSince.Store(0)
				entryBOutbound := testutil.NewNoopOutbound()
				entryB.Outbound.Store(&entryBOutbound)
				entryB.SetEgressIP(netip.MustParseAddr("203.0.113.41"))
				entryB.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
					Ewma:        25 * time.Millisecond,
					LastUpdated: time.Now(),
				})

				platB := platform.NewPlatform(
					platformID,
					"Stats generation",
					[]*regexp.Regexp{regexp.MustCompile("allowed")},
					nil,
				)
				if err := pool.RegisterPlatform(platB); err != nil {
					t.Fatalf("RegisterPlatform(B): %v", err)
				}
				if platB.View().Contains(h) {
					t.Fatal("platform B must exclude entry B")
				}
			})
		}
	}
	t.Cleanup(func() { adapter.beforePlatformStatsViewSnapshotHook = nil })
	return adapter, installSwap
}

func TestRuntimeStatsAdapter_PlatformEgressSkipsRecreatedEntryFromRetiredView(t *testing.T) {
	adapter, installSwap := newRuntimeStatsGenerationFixture(t)
	installSwap()

	got, ok := adapter.PlatformEgressIPCount("stats-generation-platform")
	if !ok {
		t.Fatal("PlatformEgressIPCount reported platform missing")
	}
	if got != 0 {
		t.Fatalf("PlatformEgressIPCount counted recreated entry from retired view: got %d, want 0", got)
	}
}

func TestRuntimeStatsAdapter_PlatformEWMAsSkipRecreatedEntryFromRetiredView(t *testing.T) {
	adapter, installSwap := newRuntimeStatsGenerationFixture(t)
	installSwap()

	if got := adapter.CollectNodeEWMAs("stats-generation-platform"); len(got) != 0 {
		t.Fatalf("CollectNodeEWMAs counted recreated entry from retired view: got %v, want empty", got)
	}
}

func TestRuntimeStatsAdapter_GlobalEgressCountDoesNotExposeRefreshGenerationGap(t *testing.T) {
	subMgr, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	sub := subscription.NewSubscription("stats-refresh-sub", "Stats refresh", "https://example.com/stats", true, false)
	subMgr.Register(sub)

	oldRaw := []byte(`{"type":"shadowsocks","tag":"stats-old","server":"198.51.100.50","server_port":443,"method":"aes-128-gcm","password":"stats-old-password"}`)
	newRaw := []byte(`{"type":"shadowsocks","tag":"stats-new","server":"198.51.100.51","server_port":443,"method":"aes-128-gcm","password":"stats-new-password"}`)
	oldHash := node.HashFromRawOptions(oldRaw)
	newHash := node.HashFromRawOptions(newRaw)
	sub.ManagedNodes().StoreNode(oldHash, subscription.ManagedNode{Tags: []string{"old"}})
	pool.AddNodeFromSub(oldHash, oldRaw, sub.ID)
	oldEntry, ok := pool.GetEntry(oldHash)
	if !ok {
		t.Fatal("old entry not found")
	}
	oldEntry.SetEgressIP(netip.MustParseAddr("203.0.113.50"))

	addEntered := make(chan struct{})
	allowAdd := make(chan struct{})
	var allowAddOnce sync.Once
	t.Cleanup(func() { allowAddOnce.Do(func() { close(allowAdd) }) })
	pool.SetOnNodeAdded(func(hash node.Hash) {
		if hash != newHash {
			return
		}
		entry, ok := pool.GetEntry(hash)
		if !ok {
			t.Errorf("new entry not found in OnNodeAdded")
			return
		}
		entry.SetEgressIP(netip.MustParseAddr("203.0.113.51"))
		close(addEntered)
		<-allowAdd
	})

	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return []byte(`{"outbounds":[{"type":"shadowsocks","tag":"stats-new","server":"198.51.100.51","server_port":443,"method":"aes-128-gcm","password":"stats-new-password"}]}`), nil
		},
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()

	select {
	case <-addEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the new-node callback")
	}

	adapter := &runtimeStatsAdapter{pool: pool}
	statsDone := make(chan int, 1)
	go func() { statsDone <- adapter.EgressIPCount() }()
	select {
	case got := <-statsDone:
		t.Fatalf("metrics observed mixed refresh generation: egress count=%d, want reader blocked", got)
	case <-time.After(100 * time.Millisecond):
	}

	allowAddOnce.Do(func() { close(allowAdd) })
	select {
	case <-refreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not complete")
	}
	select {
	case got := <-statsDone:
		if got != 1 {
			t.Fatalf("final metrics egress count=%d, want 1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metrics reader did not complete after refresh")
	}
}
