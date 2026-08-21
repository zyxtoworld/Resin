package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestGetPlatformRouteState_IsolatesPlatformsAndFailsAfterUnregister(t *testing.T) {
	cp, first := newLeaseInheritanceTestService()
	second := platform.NewPlatform("plat-2", "Second", nil, nil)
	if err := cp.Pool.RegisterPlatform(second); err != nil {
		t.Fatalf("register second platform: %v", err)
	}

	if err := cp.Router.UpsertLease(model.Lease{
		PlatformID:     first.ID,
		Account:        "account-one",
		NodeHash:       "00000000000000000000000000000000",
		EgressIP:       "198.51.100.80",
		ExpiryNs:       time.Now().Add(time.Hour).UnixNano(),
		LastAccessedNs: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("seed first lease: %v", err)
	}

	firstState, err := cp.GetPlatformRouteState(first.ID)
	if err != nil {
		t.Fatalf("first route state: %v", err)
	}
	if len(firstState.Leases.Items) != 1 || firstState.Leases.Items[0].PlatformID != first.ID {
		t.Fatalf("first platform leases = %#v, want one isolated lease", firstState.Leases)
	}
	secondState, err := cp.GetPlatformRouteState(second.ID)
	if err != nil {
		t.Fatalf("second route state: %v", err)
	}
	if len(secondState.Leases.Items) != 0 {
		t.Fatalf("second platform inherited first lease: %#v", secondState.Leases)
	}
	if _, err := cp.GetPlatformRouteStateContext(context.Background(), first.ID, PlatformRouteStateQuery{LeaseLimit: maxPlatformRouteStateLeasePage + 1}); err == nil {
		t.Fatal("route state accepted an unbounded lease page")
	}

	cp.Pool.UnregisterPlatform(first.ID)
	if _, err := cp.GetPlatformRouteState(first.ID); err == nil {
		t.Fatal("route state remained readable after platform unregister")
	}
}

func TestGetPlatformRouteState_ObservesTimeAfterRuntimeReadAdmission(t *testing.T) {
	cp, first := newLeaseInheritanceTestService()
	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	readAttempted := make(chan struct{})
	readAdmitted := make(chan time.Time, 1)
	requestDone := make(chan *PlatformRouteStateResponse, 1)
	requestErr := make(chan error, 1)

	cp.beforeRuntimeReadLockHook = func() { close(readAttempted) }
	cp.afterRuntimeReadLockHook = func() { readAdmitted <- time.Now().UTC() }
	t.Cleanup(func() {
		cp.beforeRuntimeReadLockHook = nil
		cp.afterRuntimeReadLockHook = nil
		select {
		case <-releaseWriter:
		default:
			close(releaseWriter)
		}
	})

	go cp.Pool.WithRuntimeMutation(func() {
		close(writerEntered)
		<-releaseWriter
	})
	select {
	case <-writerEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime writer did not acquire the mutation owner")
	}

	go func() {
		state, err := cp.GetPlatformRouteState(first.ID)
		if err != nil {
			requestErr <- err
			return
		}
		requestDone <- state
	}()
	select {
	case <-readAttempted:
	case <-time.After(time.Second):
		t.Fatal("route-state read did not attempt runtime admission")
	}
	select {
	case <-readAdmitted:
		t.Fatal("route-state read was admitted while the runtime writer was held")
	default:
	}

	releaseAt := time.Now().UTC()
	close(releaseWriter)
	var admittedAt time.Time
	select {
	case admittedAt = <-readAdmitted:
	case <-time.After(time.Second):
		t.Fatal("route-state read was not admitted after the writer released")
	}
	select {
	case err := <-requestErr:
		t.Fatalf("route-state read: %v", err)
	case state := <-requestDone:
		observedAt, err := time.Parse(time.RFC3339Nano, state.ObservedAt)
		if err != nil {
			t.Fatalf("parse observed_at: %v", err)
		}
		if observedAt.Before(admittedAt) || observedAt.Before(releaseAt) {
			t.Fatalf("observed_at=%s predates runtime read admission=%s/release=%s", observedAt, admittedAt, releaseAt)
		}
	case <-time.After(time.Second):
		t.Fatal("route-state read did not finish")
	}
}

func TestGetPlatformRouteStateDoesNotMixDuringPlatformReplacement(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"route-state-sub",
		"RouteStateSub",
		"https://example.com/route-state",
		true,
		false,
	)
	subMgr.Register(sub)
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})

	addNode := func(raw, tag, egress string) node.Hash {
		hash := node.HashFromRawOptions([]byte(raw))
		p.AddNodeFromSub(hash, []byte(raw), sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})
		entry, ok := p.GetEntry(hash)
		if !ok {
			t.Fatalf("node %s missing", tag)
		}
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
		entry.SetEgressIP(netip.MustParseAddr(egress))
		entry.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{
			Ewma:        50 * time.Millisecond,
			LastUpdated: time.Now(),
		})
		p.RecordResult(hash, true)
		return hash
	}
	oldHash := addNode(`{"type":"ss","server":"198.51.100.90","port":443}`, "old", "198.51.100.90")
	newHash := addNode(`{"type":"ss","server":"198.51.100.91","port":443}`, "new", "198.51.100.91")

	const platformID = "route-state-replacement"
	initial := model.Platform{
		ID:                               platformID,
		Name:                             "route-state-replacement",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{"old"},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
	}
	if err := engine.UpsertPlatform(initial); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(initial)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	if err := p.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        p,
		Authorities: func() []string { return []string{"cloudflare.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   p,
		SubMgr: subMgr,
		Router: router,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	initialState, err := cp.GetPlatformRouteState(platformID)
	if err != nil {
		t.Fatalf("initial route state: %v", err)
	}
	if len(initialState.Nodes) != 1 || initialState.Nodes[0].NodeHash != oldHash.Hex() {
		t.Fatalf("initial nodes = %#v, want only old generation %s", initialState.Nodes, oldHash.Hex())
	}

	nodesCopied := make(chan struct{})
	releaseNodes := make(chan struct{})
	var nodesOnce sync.Once
	cp.afterRouteStateNodesHook = func() {
		nodesOnce.Do(func() { close(nodesCopied) })
		<-releaseNodes
	}
	t.Cleanup(func() {
		cp.afterRouteStateNodesHook = nil
		select {
		case <-releaseNodes:
		default:
			close(releaseNodes)
		}
	})

	stateDone := make(chan struct {
		state *PlatformRouteStateResponse
		err   error
	}, 1)
	go func() {
		state, routeErr := cp.GetPlatformRouteState(platformID)
		stateDone <- struct {
			state *PlatformRouteStateResponse
			err   error
		}{state: state, err: routeErr}
	}()
	select {
	case <-nodesCopied:
	case <-time.After(time.Second):
		t.Fatal("route-state did not copy the old node view")
	}

	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdatePlatformContext(context.Background(), platformID, []byte(`{"regex_filters":["new"]}`))
		updateDone <- updateErr
	}()

	// Current code allows the replacement to publish while route-state is
	// paused after copying old nodes. A fixed implementation must serialize the
	// writer with this read owner. This timeout is only a deadlock watchdog; the
	// ordering is established by the channel gates above.
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdatePlatformContext: %v", err)
		}
		close(releaseNodes)
		t.Fatal("platform replacement completed while route-state held an old node snapshot")
	case <-time.After(time.Second):
		close(releaseNodes)
	}

	select {
	case result := <-stateDone:
		if result.err != nil {
			t.Fatalf("route-state: %v", result.err)
		}
		if len(result.state.Nodes) != 1 || result.state.Nodes[0].NodeHash != oldHash.Hex() {
			t.Fatalf("route-state nodes = %#v, want old generation before replacement", result.state.Nodes)
		}
	case <-time.After(time.Second):
		t.Fatal("route-state did not finish after releasing its read owner")
	}
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdatePlatformContext after route-state: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("platform replacement did not finish after route-state released")
	}
	current, ok := p.GetPlatform(platformID)
	if !ok || current == runtimePlatform || !current.View().Contains(newHash) {
		t.Fatalf("replacement platform view = %#v, want new generation containing %s", current, newHash.Hex())
	}
}

func TestPlatformRouteNodeStatus_CoversRuntimeStates(t *testing.T) {
	circuitSince := "2026-08-17T00:00:00Z"
	cases := []struct {
		name       string
		node       NodeSummary
		cooldowns  []PlatformCooldownSnapshot
		wantStatus string
	}{
		{name: "available", node: NodeSummary{NodeHash: "available", Enabled: true, HasOutbound: true}, wantStatus: "available"},
		{name: "cooling", node: NodeSummary{NodeHash: "cooling", Enabled: true, HasOutbound: true, EgressIP: "198.51.100.120"}, cooldowns: []PlatformCooldownSnapshot{{Scope: "egress_ip", EgressIP: "198.51.100.120"}}, wantStatus: "cooling"},
		{name: "circuit_open", node: NodeSummary{NodeHash: "circuit", Enabled: true, HasOutbound: true, CircuitOpenSince: &circuitSince}, cooldowns: []PlatformCooldownSnapshot{{Scope: "egress_ip", EgressIP: "198.51.100.121"}}, wantStatus: "circuit_open"},
		{name: "not_ready", node: NodeSummary{NodeHash: "not-ready", Enabled: true}, wantStatus: "not_ready"},
		{name: "disabled", node: NodeSummary{NodeHash: "disabled", Enabled: false, HasOutbound: true}, wantStatus: "disabled"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := platformRouteNodeStatus(testCase.node, testCase.cooldowns); got != testCase.wantStatus {
				t.Fatalf("status = %q, want %q", got, testCase.wantStatus)
			}
		})
	}
}

func TestGetPlatformRouteStateBoundsNodePayloadAtProductionScale(t *testing.T) {
	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("route-state-scale-sub", "RouteStateScale", "https://example.com/route-state-scale", true, false)
	cp.SubMgr.Register(sub)
	for i := 0; i < 1500; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"route-state-%d","type":"ss","server":"198.51.100.1","port":443}`, i))
		hash := node.HashFromRawOptions(raw)
		cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
		entry, ok := cp.Pool.GetEntry(hash)
		if !ok {
			t.Fatalf("scale fixture entry %d missing", i)
		}
		entry.CircuitOpenSince.Store(0)
		entry.SetEgressIP(netip.AddrFrom4([4]byte{198, 18, byte(i >> 8), byte(i)}))
		entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	plat.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })

	state, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{
		NodeLimit: 50,
	})
	if err != nil {
		t.Fatalf("route state at production scale: %v", err)
	}
	if state.NodesTotal != 1500 || len(state.Nodes) != 50 || state.NodesLimit != 50 || state.NodesNextCursor == "" || !state.NodesHasMore {
		t.Fatalf("bounded node page = total=%d len=%d limit=%d next_cursor=%t has_more=%t", state.NodesTotal, len(state.Nodes), state.NodesLimit, state.NodesNextCursor != "", state.NodesHasMore)
	}
	for _, item := range state.Nodes {
		if item.Status != "available" {
			t.Fatalf("node status = %q, want available for healthy scale fixture", item.Status)
		}
	}

	second, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{
		NodeLimit:  50,
		NodeCursor: state.NodesNextCursor,
	})
	if err != nil {
		t.Fatalf("last node page: %v", err)
	}
	if len(second.Nodes) != 50 || second.NodesNextCursor == "" || !second.NodesHasMore {
		t.Fatalf("second bounded node page = len=%d next_cursor=%t has_more=%t", len(second.Nodes), second.NodesNextCursor != "", second.NodesHasMore)
	}

	filtered, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{
		NodeLimit:  50,
		NodeStatus: "available",
	})
	if err != nil {
		t.Fatalf("filtered node page: %v", err)
	}
	if filtered.NodesTotal != 1500 || len(filtered.Nodes) != 50 || !filtered.NodesHasMore {
		t.Fatalf("filtered node page = total=%d len=%d has_more=%t", filtered.NodesTotal, len(filtered.Nodes), filtered.NodesHasMore)
	}
}

func TestGetPlatformRouteStateBoundsLargeCooldownAndLeasePayload(t *testing.T) {
	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("route-state-large-sub", "RouteStateLarge", "https://example.com/route-state-large", true, false)
	cp.SubMgr.Register(sub)
	hashes := make([]node.Hash, 0, 1500)
	for i := 0; i < 1500; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"route-state-large-%d","type":"ss","server":"198.51.100.1","port":443}`, i))
		hash := node.HashFromRawOptions(raw)
		hashes = append(hashes, hash)
		cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
		entry, ok := cp.Pool.GetEntry(hash)
		if !ok {
			t.Fatalf("large fixture entry %d missing", i)
		}
		entry.CircuitOpenSince.Store(0)
		entry.SetEgressIP(netip.AddrFrom4([4]byte{198, 18, byte(i >> 8), byte(i)}))
		entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	plat.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })

	until := time.Now().Add(time.Hour)
	for i, hash := range hashes {
		ip := netip.AddrFrom4([4]byte{198, 18, byte(i >> 8), byte(i)})
		err := cp.Router.UpsertLease(model.Lease{
			PlatformID:     plat.ID,
			Account:        fmt.Sprintf("route-state-account-%04d", i),
			NodeHash:       hash.Hex(),
			EgressIP:       ip.String(),
			ExpiryNs:       until.UnixNano(),
			LastAccessedNs: time.Now().UnixNano(),
		})
		if err != nil {
			t.Fatalf("seed lease %d: %v", i, err)
		}
	}
	_, err := cp.Router.WithPlatformResponseCooldownsContext(context.Background(), plat.ID, func(table *routing.ResponseCooldowns, _ uint64) error {
		for _, hash := range hashes {
			table.Mark(platform.ResponseRuleScopeNode, hash, netip.Addr{}, until)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed cooldowns: %v", err)
	}

	state, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{
		NodeLimit:  50,
		LeaseLimit: 50,
	})
	if err != nil {
		t.Fatalf("large route state: %v", err)
	}
	if state.NodesTotal != 1500 || len(state.Nodes) != 50 || state.CooldownsTotal != 1500 || len(state.Cooldowns) > 50 {
		t.Fatalf("large bounded state: nodes=%d/%d cooldowns=%d/%d", state.NodesTotal, len(state.Nodes), state.CooldownsTotal, len(state.Cooldowns))
	}
	if state.Leases.Total != 1500 || len(state.Leases.Items) != 50 {
		t.Fatalf("large lease page: total=%d items=%d", state.Leases.Total, len(state.Leases.Items))
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal route state: %v", err)
	}
	if len(raw) > 120*1024 {
		t.Fatalf("route state payload = %d bytes, want <= 122880", len(raw))
	}
}

func TestGetPlatformRouteStateNodeCursorTraversesBeyondLegacyWindow(t *testing.T) {
	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("route-state-cursor-large-sub", "RouteStateCursorLarge", "https://example.com/route-state-cursor-large", true, false)
	cp.SubMgr.Register(sub)
	const totalNodes = 5005
	for i := 0; i < totalNodes; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"route-state-cursor-large-%d","type":"ss","server":"198.51.100.1","port":443}`, i))
		hash := node.HashFromRawOptions(raw)
		cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
		entry, ok := cp.Pool.GetEntry(hash)
		if !ok {
			t.Fatalf("cursor scale fixture entry %d missing", i)
		}
		entry.CircuitOpenSince.Store(0)
		entry.SetEgressIP(netip.AddrFrom4([4]byte{198, 19, byte(i >> 8), byte(i)}))
		entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	plat.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })

	seen := make(map[string]struct{}, totalNodes)
	cursor := ""
	for pageNumber := 0; ; pageNumber++ {
		state, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{
			NodeLimit:  200,
			NodeCursor: cursor,
		})
		if err != nil {
			t.Fatalf("cursor page %d: %v", pageNumber, err)
		}
		if state.NodesTotal != totalNodes || len(state.Nodes) > 200 {
			t.Fatalf("cursor page %d bounds: total=%d items=%d", pageNumber, state.NodesTotal, len(state.Nodes))
		}
		for _, item := range state.Nodes {
			if _, exists := seen[item.NodeHash]; exists {
				t.Fatalf("cursor page %d repeated node %s", pageNumber, item.NodeHash)
			}
			seen[item.NodeHash] = struct{}{}
		}
		if !state.NodesHasMore {
			if state.NodesNextCursor != "" {
				t.Fatalf("final cursor page returned next cursor")
			}
			break
		}
		if state.NodesNextCursor == "" || state.NodesNextCursor == cursor {
			t.Fatalf("cursor page %d did not advance: %q", pageNumber, state.NodesNextCursor)
		}
		cursor = state.NodesNextCursor
	}
	if len(seen) != totalNodes {
		t.Fatalf("cursor traversal reached %d/%d nodes", len(seen), totalNodes)
	}
}

func TestGetPlatformRouteStateRejectsNodeCursorContractChanges(t *testing.T) {
	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("route-state-cursor-contract-sub", "RouteStateCursorContract", "https://example.com/route-state-cursor-contract", true, false)
	cp.SubMgr.Register(sub)
	for i := 0; i < 3; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"route-state-cursor-contract-%d","type":"ss"}`, i))
		hash := node.HashFromRawOptions(raw)
		cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
		entry, ok := cp.Pool.GetEntry(hash)
		if !ok {
			t.Fatalf("cursor contract fixture entry %d missing", i)
		}
		entry.CircuitOpenSince.Store(0)
		entry.SetEgressIP(netip.AddrFrom4([4]byte{198, 20, byte(i), byte(i + 1)}))
		entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	plat.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })
	first, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{NodeLimit: 2})
	if err != nil || first.NodesNextCursor == "" {
		t.Fatalf("first cursor page = %#v, err=%v", first, err)
	}
	if _, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{NodeLimit: 1, NodeCursor: first.NodesNextCursor}); err == nil {
		t.Fatal("cursor accepted a changed page size")
	}
	if _, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{NodeStatus: "available", NodeLimit: 2, NodeCursor: first.NodesNextCursor}); err == nil {
		t.Fatal("cursor accepted a changed status filter")
	}
}

func TestGetPlatformRouteStateNodeCursorFailsAfterPlatformDeleteAndRecreate(t *testing.T) {
	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("route-state-cursor-rebuild-sub", "RouteStateCursorRebuild", "https://example.com/route-state-cursor-rebuild", true, false)
	cp.SubMgr.Register(sub)
	for i := 0; i < 3; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"route-state-cursor-rebuild-%d","type":"ss"}`, i))
		hash := node.HashFromRawOptions(raw)
		cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
		entry, ok := cp.Pool.GetEntry(hash)
		if !ok {
			t.Fatalf("rebuild cursor fixture entry %d missing", i)
		}
		entry.CircuitOpenSince.Store(0)
		entry.SetEgressIP(netip.AddrFrom4([4]byte{198, 21, byte(i), byte(i + 1)}))
		entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	plat.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })
	first, err := cp.GetPlatformRouteStateContext(context.Background(), plat.ID, PlatformRouteStateQuery{NodeLimit: 2})
	if err != nil || first.NodesNextCursor == "" {
		t.Fatalf("initial rebuild cursor page = %#v, err=%v", first, err)
	}

	cp.Pool.UnregisterPlatform(plat.ID)
	cp.Router.RemovePlatformState(plat.ID)
	replacement := platform.NewPlatform(plat.ID, "RouteStateCursorRebuildNew", nil, nil)
	if err := cp.Pool.RegisterPlatform(replacement); err != nil {
		t.Fatalf("register replacement platform: %v", err)
	}
	replacement.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })
	if _, err := cp.GetPlatformRouteStateContext(context.Background(), replacement.ID, PlatformRouteStateQuery{NodeLimit: 2, NodeCursor: first.NodesNextCursor}); err == nil {
		t.Fatal("cursor from deleted platform generation was accepted after recreate")
	}
}

func BenchmarkCollectPlatformRouteNodePageTopK(b *testing.B) {
	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("route-state-benchmark-sub", "RouteStateBenchmark", "https://example.com/route-state-benchmark", true, false)
	cp.SubMgr.Register(sub)
	const totalNodes = 5005
	for i := 0; i < totalNodes; i++ {
		raw := []byte(fmt.Sprintf(`{"id":"route-state-benchmark-%d","type":"ss","server":"198.51.100.1","port":443}`, i))
		hash := node.HashFromRawOptions(raw)
		cp.Pool.AddNodeFromSub(hash, raw, sub.ID)
		sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
		entry, ok := cp.Pool.GetEntry(hash)
		if !ok {
			b.Fatalf("benchmark fixture entry %d missing", i)
		}
		entry.CircuitOpenSince.Store(0)
		entry.SetEgressIP(netip.AddrFrom4([4]byte{198, 22, byte(i >> 8), byte(i)}))
		entry.LatencyTable.Update("cloudflare.com", time.Millisecond, time.Minute)
		outbound := testutil.NewNoopOutbound()
		entry.Outbound.Store(&outbound)
	}
	plat.FullRebuild(cp.Pool.Range, cp.Pool.MakeSubLookup(), func(netip.Addr) string { return "us" })
	query := PlatformRouteStateQuery{NodeLimit: 200}
	cooldowns := routing.ResponseCooldownReadSnapshot{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, total, hasMore, _, _, _, err := cp.collectPlatformRouteNodePage(plat, cooldowns, query, nil, 200)
		if err != nil || total != totalNodes || !hasMore || len(page) != 200 {
			b.Fatalf("benchmark result: page=%d total=%d has_more=%t err=%v", len(page), total, hasMore, err)
		}
	}
}
