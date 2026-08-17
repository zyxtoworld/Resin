package service

import (
	"context"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/platform"
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
