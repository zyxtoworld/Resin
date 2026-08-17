package routing

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestRouteResultNodeTagKeepsSelectedEntryGeneration(t *testing.T) {
	subManager := topology.NewSubscriptionManager()
	oldSub := subscription.NewSubscription(
		"node-tag-old-sub",
		"Old Provider",
		"https://old.example/subscribe",
		true,
		false,
	)
	newSub := subscription.NewSubscription(
		"node-tag-new-sub",
		"New Provider",
		"https://new.example/subscribe",
		true,
		false,
	)
	subManager.Register(oldSub)
	subManager.Register(newSub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := json.RawMessage(`{"type":"same-hash-node-tag"}`)
	hash := node.HashFromRawOptions(raw)
	oldSub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"old-tag"}})
	pool.AddNodeFromSub(hash, raw, oldSub.ID)
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old node entry was not added")
	}
	oldEntry.CircuitOpenSince.Store(0)
	oldEntry.SetEgressIP(netip.MustParseAddr("203.0.113.240"))
	oldEntry.LatencyTable.Update("example.com", time.Millisecond, time.Minute)
	noop := testutil.NewNoopOutbound()
	oldEntry.Outbound.Store(&noop)

	plat := platform.NewPlatform("node-tag-platform", "node-tag-platform", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if !plat.View().Contains(hash) {
		t.Fatal("platform view did not contain the initial node")
	}

	tagEntered := make(chan struct{})
	allowTag := make(chan struct{})
	router := NewRouter(RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
		NodeTagResolver: func(h node.Hash, expected *node.NodeEntry) string {
			close(tagEntered)
			<-allowTag
			return pool.ResolveNodeDisplayTagForEntry(h, expected)
		},
	})

	resultCh := make(chan RouteResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := router.RouteRequest(plat.Name, "", "https://example.com/")
		resultCh <- result
		errCh <- err
	}()

	select {
	case <-tagEntered:
	case <-time.After(time.Second):
		t.Fatal("route did not reach node-tag resolution")
	}

	newSub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"new-tag"}})
	pool.RemoveNodeFromSub(hash, oldSub.ID)
	pool.AddNodeFromSub(hash, raw, newSub.ID)
	newEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("new node entry was not added")
	}
	close(allowTag)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RouteRequest: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RouteRequest did not finish")
	}
	result := <-resultCh
	if result.NodeTag != "" {
		t.Fatalf("NodeTag = %q, want fail-closed empty tag after generation replacement", result.NodeTag)
	}
	if got := pool.ResolveNodeDisplayTagForEntry(hash, newEntry); got != "New Provider/new-tag" {
		t.Fatalf("new generation tag = %q, want New Provider/new-tag", got)
	}
}
