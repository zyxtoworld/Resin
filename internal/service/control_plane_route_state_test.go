package service

import (
	"context"
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
