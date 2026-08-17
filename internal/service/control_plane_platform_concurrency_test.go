package service

import (
	"context"
	"errors"
	"math"
	"net/netip"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestPlatformMutationHoldsStateWriteAdmissionThroughRuntimePublish(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	persisted := make(chan struct{})
	releasePublish := make(chan struct{})
	var persistOnce sync.Once
	cp.afterPlatformPersistHook = func() {
		persistOnce.Do(func() { close(persisted) })
		<-releasePublish
	}

	name := "shutdown-platform-publish"
	createDone := make(chan error, 1)
	go func() {
		_, createErr := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
		createDone <- createErr
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("platform mutation did not reach the post-persist publish boundary")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	admissionDone := make(chan error, 1)
	go func() { admissionDone <- engine.CloseStateWriteAdmissionAndWait(closeCtx) }()

	var admissionErr error
	select {
	case admissionErr = <-admissionDone:
		if admissionErr == nil {
			t.Fatal("state write admission closed before runtime platform publish completed")
		}
		if !errors.Is(admissionErr, context.DeadlineExceeded) {
			t.Fatalf("CloseStateWriteAdmissionAndWait error = %v, want deadline exceeded", admissionErr)
		}
	case <-time.After(time.Second):
		t.Fatal("state write admission waiter did not honor its deadline")
	}
	close(releasePublish)

	if err := <-createDone; err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	if err := engine.CloseStateWriteAdmissionAndWait(context.Background()); err != nil {
		t.Fatalf("final state write admission wait: %v", err)
	}
	if _, ok := pool.GetPlatformByName(name); !ok {
		t.Fatal("runtime platform was not published after the mutation committed")
	}
}

func TestUpdatePlatformContext_WithLiveRequestContextReturnsSuccess(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	name := "live-request-context-platform"
	created, err := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
	if err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := cp.UpdatePlatformContext(requestCtx, created.ID, []byte(`{"sticky_ttl":"2h"}`)); err != nil {
		t.Fatalf("UpdatePlatformContext: %v", err)
	}
}

func TestUpdatePlatformContext_CancellationInterruptsPostLockModelRead(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	name := "platform-post-lock-read-cancel"
	created, err := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
	if err != nil {
		t.Fatalf("seed CreatePlatform: %v", err)
	}

	loadEntered := make(chan struct{})
	releaseLoad := make(chan struct{})
	var loadOnce sync.Once
	cp.beforePlatformModelReadHook = func(readCtx context.Context) {
		loadOnce.Do(func() { close(loadEntered) })
		if readCtx.Done() == nil {
			<-releaseLoad
			return
		}
		select {
		case <-readCtx.Done():
		case <-releaseLoad:
		}
	}
	defer func() {
		select {
		case <-releaseLoad:
		default:
			close(releaseLoad)
		}
	}()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdatePlatformContext(requestCtx, created.ID, []byte(`{"sticky_ttl":"2h"}`))
		updateDone <- updateErr
	}()

	select {
	case <-loadEntered:
	case <-time.After(time.Second):
		t.Fatal("UpdatePlatformContext did not reach post-lock model-read boundary")
	}
	cancel()
	returnedBeforeRelease := false
	select {
	case updateErr := <-updateDone:
		returnedBeforeRelease = true
		if !errors.Is(updateErr, context.Canceled) {
			t.Fatalf("UpdatePlatformContext error = %v, want context.Canceled", updateErr)
		}
	case <-time.After(500 * time.Millisecond):
	}
	close(releaseLoad)
	if !returnedBeforeRelease {
		select {
		case <-updateDone:
		case <-time.After(time.Second):
			t.Fatal("UpdatePlatformContext did not finish after releasing model-read seam")
		}
		t.Fatal("canceled UpdatePlatformContext remained blocked on non-context model read")
	}
}

func TestResetPlatformToDefaultContext_CancellationInterruptsPostLockNameRead(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	name := "platform-reset-post-lock-read-cancel"
	created, err := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
	if err != nil {
		t.Fatalf("seed CreatePlatform: %v", err)
	}

	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	var readOnce sync.Once
	cp.beforePlatformModelReadHook = func(readCtx context.Context) {
		readOnce.Do(func() { close(readEntered) })
		if readCtx.Done() == nil {
			<-releaseRead
			return
		}
		select {
		case <-readCtx.Done():
		case <-releaseRead:
		}
	}
	defer func() {
		select {
		case <-releaseRead:
		default:
			close(releaseRead)
		}
	}()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resetDone := make(chan error, 1)
	go func() {
		_, resetErr := cp.ResetPlatformToDefaultContext(requestCtx, created.ID)
		resetDone <- resetErr
	}()

	select {
	case <-readEntered:
	case <-time.After(time.Second):
		t.Fatal("ResetPlatformToDefaultContext did not reach post-lock name-read boundary")
	}
	cancel()

	returnedBeforeRelease := false
	select {
	case resetErr := <-resetDone:
		returnedBeforeRelease = true
		if !errors.Is(resetErr, context.Canceled) {
			t.Fatalf("ResetPlatformToDefaultContext error = %v, want context.Canceled", resetErr)
		}
	case <-time.After(500 * time.Millisecond):
	}
	close(releaseRead)
	if !returnedBeforeRelease {
		select {
		case <-resetDone:
		case <-time.After(time.Second):
			t.Fatal("ResetPlatformToDefaultContext did not finish after releasing name-read seam")
		}
		t.Fatal("canceled ResetPlatformToDefaultContext remained blocked on non-context name read")
	}
}

func TestUpdatePlatformContextCancellationWhileAnotherMutationOwnsPlatformLock(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	name := "platform-lock-owner"
	created, err := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
	if err != nil {
		t.Fatalf("seed CreatePlatform: %v", err)
	}

	persisted := make(chan struct{})
	releasePersist := make(chan struct{})
	var persistOnce sync.Once
	cp.afterPlatformPersistHook = func() {
		persistOnce.Do(func() { close(persisted) })
		<-releasePersist
	}
	var beforeLockCalls atomic.Int32
	secondBeforeLock := make(chan struct{})
	cp.platformMutationHook = func(stage platformMutationStage) {
		if stage == platformMutationBeforeLock && beforeLockCalls.Add(1) == 2 {
			close(secondBeforeLock)
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdatePlatformContext(context.Background(), created.ID, []byte(`{"sticky_ttl":"2h"}`))
		firstDone <- updateErr
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		close(releasePersist)
		t.Fatal("first platform mutation did not reach post-persist lock owner")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdatePlatformContext(requestCtx, created.ID, []byte(`{"sticky_ttl":"3h"}`))
		secondDone <- updateErr
	}()
	select {
	case <-secondBeforeLock:
	case <-time.After(time.Second):
		close(releasePersist)
		t.Fatal("second platform mutation did not reach the lock boundary")
	}
	cancel()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled second mutation error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		close(releasePersist)
		t.Fatal("canceled second platform mutation remained blocked on platform lock")
	}

	close(releasePersist)
	if err := <-firstDone; err != nil {
		t.Fatalf("first platform mutation: %v", err)
	}
}

func TestPlatformReadsDoNotMixPersistedAndRuntimeGenerations(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-read-generation"
	initial := model.Platform{
		ID:                               platformID,
		Name:                             "read-generation",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
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
	payload := []byte(`{"type":"ss","server":"198.51.100.10","port":443}`)
	hash := node.HashFromRawOptions(payload)
	entry := node.NewNodeEntry(hash, payload, time.Now(), 16)
	entry.AddSubscriptionID("read-generation-sub")
	entry.SetEgressIP(netip.MustParseAddr("198.51.100.11"))
	entry.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{
		Ewma:        50 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	// The bootstrap entry is healthy/routable for the old empty filter.
	entry.CircuitOpenSince.Store(0)
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"read-generation-sub", "read-generation-sub", "https://example.com/sub", true, false,
	)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{})
	subMgr.Register(sub)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
		SubLookup:              subMgr.Lookup,
	})
	pool.LoadNodeFromBootstrap(entry)
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	if got := runtimePlatform.View().Size(); got != 1 {
		t.Fatalf("initial runtime view size = %d, want 1", got)
	}

	cp := &ControlPlaneService{Engine: engine, Pool: pool}
	persisted := make(chan struct{})
	publishGate := make(chan struct{})
	var releasePublishOnce sync.Once
	releasePublish := func() { releasePublishOnce.Do(func() { close(publishGate) }) }
	defer releasePublish()
	var persistOnce sync.Once
	cp.afterPlatformPersistHook = func() {
		persistOnce.Do(func() { close(persisted) })
		<-publishGate
	}
	readAttempted := make(chan struct{})
	readGate := make(chan struct{})
	var allowReadOnce sync.Once
	allowRead := func() { allowReadOnce.Do(func() { close(readGate) }) }
	defer allowRead()
	var readOnce sync.Once
	cp.beforePlatformReadHook = func() {
		readOnce.Do(func() { close(readAttempted) })
		<-readGate
	}

	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdatePlatform(platformID, []byte(`{"regex_filters":["^does-not-match$"]}`))
		updateDone <- updateErr
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("UpdatePlatform did not reach post-persist boundary")
	}

	type platformReadResult struct {
		kind     string
		response *PlatformResponse
		list     []PlatformResponse
		err      error
	}
	readDone := make(chan platformReadResult, 2)
	go func() {
		response, err := cp.GetPlatform(platformID)
		readDone <- platformReadResult{kind: "get", response: response, err: err}
	}()
	go func() {
		list, err := cp.ListPlatforms()
		readDone <- platformReadResult{kind: "list", list: list, err: err}
	}()
	select {
	case <-readAttempted:
	case <-time.After(time.Second):
		t.Fatal("GetPlatform did not reach the read seam")
	}
	allowRead()

	var earlyResults []platformReadResult
	select {
	case result := <-readDone:
		if result.err != nil {
			t.Fatalf("%s platform read: %v", result.kind, result.err)
		}
		earlyResults = append(earlyResults, result)
		if result.kind == "get" {
			response := result.response
			if len(response.RegexFilters) != 1 || response.RegexFilters[0] != "^does-not-match$" || response.RoutableNodeCount != 1 {
				t.Fatalf("unexpected get before runtime publish: regex=%v routable=%d", response.RegexFilters, response.RoutableNodeCount)
			}
		} else {
			if len(result.list) != 1 || len(result.list[0].RegexFilters) != 1 || result.list[0].RegexFilters[0] != "^does-not-match$" || result.list[0].RoutableNodeCount != 1 {
				t.Fatalf("unexpected list before runtime publish: %+v", result.list)
			}
		}
		t.Errorf("%s platform read returned a mixed persisted/runtime generation", result.kind)
	case <-time.After(100 * time.Millisecond):
	}

	releasePublish()
	if err := <-updateDone; err != nil {
		t.Fatalf("UpdatePlatform: %v", err)
	}
	results := append([]platformReadResult(nil), earlyResults...)
	for len(results) < 2 {
		results = append(results, <-readDone)
	}
	for _, result := range results {
		if result.err != nil {
			t.Fatalf("%s platform read after publish: %v", result.kind, result.err)
		}
		if result.kind == "get" {
			response := result.response
			if len(response.RegexFilters) != 1 || response.RegexFilters[0] != "^does-not-match$" || response.RoutableNodeCount != 0 {
				t.Fatalf("final get=%+v, want new regex and zero routable nodes", response)
			}
			continue
		}
		if len(result.list) != 1 || len(result.list[0].RegexFilters) != 1 || result.list[0].RegexFilters[0] != "^does-not-match$" || result.list[0].RoutableNodeCount != 0 {
			t.Fatalf("final list=%+v, want new regex and zero routable nodes", result.list)
		}
	}
}

func TestPlatformReadContextCancellationWhileMutationOwnsPlatformLock(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	name := "platform-read-cancel"
	created, err := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
	if err != nil {
		t.Fatalf("seed CreatePlatform: %v", err)
	}

	persisted := make(chan struct{})
	releasePublish := make(chan struct{})
	var persistOnce sync.Once
	cp.afterPlatformPersistHook = func() {
		persistOnce.Do(func() { close(persisted) })
		<-releasePublish
	}
	defer func() {
		select {
		case <-releasePublish:
		default:
			close(releasePublish)
		}
	}()

	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := cp.UpdatePlatformContext(context.Background(), created.ID, []byte(`{"sticky_ttl":"2h"}`))
		updateDone <- updateErr
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("platform mutation did not reach post-persist owner")
	}

	readers := []struct {
		name string
		read func(context.Context) error
	}{
		{
			name: "list",
			read: func(ctx context.Context) error {
				_, err := cp.ListPlatformsContext(ctx)
				return err
			},
		},
		{
			name: "get",
			read: func(ctx context.Context) error {
				_, err := cp.GetPlatformContext(ctx, created.ID)
				return err
			},
		},
	}
	for _, reader := range readers {
		readStarted := make(chan struct{})
		var readOnce sync.Once
		cp.beforePlatformReadHook = func() {
			readOnce.Do(func() { close(readStarted) })
		}

		ctx, cancel := context.WithCancel(context.Background())
		readDone := make(chan error, 1)
		go func() { readDone <- reader.read(ctx) }()
		select {
		case <-readStarted:
		case <-time.After(time.Second):
			cancel()
			close(releasePublish)
			t.Fatalf("%s platform read did not reach its lock boundary", reader.name)
		}
		cancel()

		select {
		case readErr := <-readDone:
			if !errors.Is(readErr, context.Canceled) {
				t.Fatalf("%s platform read error = %v, want context.Canceled", reader.name, readErr)
			}
		case <-time.After(time.Second):
			close(releasePublish)
			t.Fatalf("canceled %s platform read remained blocked by the mutation owner", reader.name)
		}
	}

	close(releasePublish)
	if err := <-updateDone; err != nil {
		t.Fatalf("UpdatePlatformContext: %v", err)
	}
}

func TestCreatePlatform_RejectsStickyTTLThatOverflowsLeaseExpiry(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	name := "sticky-ttl-overflow"
	_, err = cp.CreatePlatform(CreatePlatformRequest{
		Name:      &name,
		StickyTTL: func() *string { v := time.Duration(math.MaxInt64).String(); return &v }(),
	})
	if err == nil {
		t.Fatal("CreatePlatform unexpectedly accepted an unrepresentable sticky lease expiry")
	}
	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 0 {
		t.Fatalf("rejected platform committed %d rows", len(platforms))
	}
}

func TestUpdatePlatform_ConcurrentPatchesDoNotLoseFields(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-concurrent-update"
	initial := model.Platform{
		ID:                               platformID,
		Name:                             "concurrent-update",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
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
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.RegisterPlatform(runtimePlatform)

	cp := &ControlPlaneService{Engine: engine, Pool: pool}
	firstLoaded := make(chan struct{})
	secondStarted := make(chan bool, 1)
	secondLoaded := make(chan struct{})
	releaseFirst := make(chan struct{})
	var beforeLockCalls atomic.Int32
	var afterLoadCalls atomic.Int32
	cp.platformMutationHook = func(stage platformMutationStage) {
		switch stage {
		case platformMutationBeforeLock:
			if beforeLockCalls.Add(1) != 2 {
				return
			}
			// The first call is paused after loading its snapshot. If the
			// mutation lock is real, it is held here; otherwise the second
			// call can load the same stale snapshot.
			serialized := !cp.platformMu.TryLock()
			if !serialized {
				cp.platformMu.Unlock()
			}
			secondStarted <- serialized
		case platformMutationAfterLoad:
			switch afterLoadCalls.Add(1) {
			case 1:
				close(firstLoaded)
				<-releaseFirst
			case 2:
				close(secondLoaded)
			}
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	startPatch := func(patch string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cp.UpdatePlatform(platformID, []byte(patch))
			errCh <- err
		}()
	}

	startPatch(`{"sticky_ttl":"2h"}`)
	<-firstLoaded
	startPatch(`{"allocation_policy":"PREFER_IDLE_IP"}`)
	serialized := <-secondStarted
	if serialized {
		close(releaseFirst)
	} else {
		// Without the production lock, this wait makes both calls hold the
		// same persisted snapshot before either is released to write.
		<-secondLoaded
		close(releaseFirst)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("UpdatePlatform: %v", err)
		}
	}

	got, err := engine.GetPlatform(platformID)
	if err != nil {
		t.Fatalf("GetPlatform: %v", err)
	}
	if got.StickyTTLNs != int64(2*time.Hour) ||
		got.AllocationPolicy != string(platform.AllocationPolicyPreferIdleIP) {
		t.Fatalf("concurrent patches lost an update: sticky_ttl=%d allocation_policy=%q", got.StickyTTLNs, got.AllocationPolicy)
	}
}

func TestUpdatePlatform_MissingRuntimeRejectedBeforePersistence(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-runtime-publish-failure"
	initial := model.Platform{
		ID:                               platformID,
		Name:                             "runtime-publish-failure",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
	}
	if err := engine.UpsertPlatform(initial); err != nil {
		t.Fatalf("seed platform: %v", err)
	}

	// Deliberately omit the runtime registration. The service preflight must
	// reject the mutation before the strong-persist write; this is not a
	// reachable post-persist runtime-publish failure in the production wiring.
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	cp := &ControlPlaneService{Engine: engine, Pool: pool}

	_, err = cp.UpdatePlatform(platformID, []byte(`{"sticky_ttl":"2h"}`))
	if err == nil {
		t.Fatal("UpdatePlatform unexpectedly succeeded without a runtime platform")
	}

	got, err := engine.GetPlatform(platformID)
	if err != nil {
		t.Fatalf("GetPlatform: %v", err)
	}
	if got.StickyTTLNs != initial.StickyTTLNs {
		t.Fatalf("database committed without runtime publish: sticky_ttl=%d, want %d", got.StickyTTLNs, initial.StickyTTLNs)
	}
}

func TestCreatePlatform_RuntimeOrphanNameConflictDoesNotCommitOrOverwriteRuntime(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const orphanID = "runtime-orphan"
	const nameValue = "runtime-orphan-name"
	orphan := platform.NewPlatform(orphanID, nameValue, nil, nil)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.RegisterPlatform(orphan)

	name := nameValue
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	if _, err := cp.CreatePlatform(CreatePlatformRequest{Name: &name}); err == nil {
		t.Fatal("CreatePlatform unexpectedly succeeded over a runtime-only same-name orphan")
	}

	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 0 {
		t.Fatalf("failed create committed %d platform rows", len(platforms))
	}
	got, ok := pool.GetPlatformByName(nameValue)
	if !ok || got != orphan {
		t.Fatalf("runtime name mapping changed: got=%p want=%p", got, orphan)
	}
}

func TestCreatePlatform_RuntimePublishFailureRollsBackPersistedRow(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	name := "late-runtime-conflict"
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		EnvCfg: &config.EnvConfig{
			DefaultPlatformStickyTTL:                        time.Hour,
			DefaultPlatformReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
			DefaultPlatformReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
			DefaultPlatformAllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		},
	}

	persisted := make(chan struct{})
	allowPublish := make(chan struct{})
	cp.afterPlatformPersistHook = func() {
		close(persisted)
		<-allowPublish
	}

	createDone := make(chan error, 1)
	go func() {
		_, createErr := cp.CreatePlatform(CreatePlatformRequest{Name: &name})
		createDone <- createErr
	}()
	select {
	case <-persisted:
	case <-time.After(time.Second):
		t.Fatal("CreatePlatform did not reach post-persist boundary")
	}

	lateRuntime := platform.NewPlatform("late-runtime-platform", name, nil, nil)
	if err := pool.RegisterPlatform(lateRuntime); err != nil {
		t.Fatalf("late runtime registration: %v", err)
	}
	close(allowPublish)

	if err := <-createDone; err == nil {
		t.Fatal("CreatePlatform unexpectedly succeeded after runtime publish conflict")
	}
	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 0 {
		t.Fatalf("runtime publish failure left %d persisted platform rows: %+v", len(platforms), platforms)
	}
	got, ok := pool.GetPlatformByName(name)
	if !ok || got != lateRuntime {
		t.Fatalf("late runtime mapping changed: got=%p want=%p", got, lateRuntime)
	}
}

func TestDeletePlatform_RemovesRouterStateAndPersistedLeases(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-delete-with-lease"
	const platformName = "delete-with-lease"
	const account = "orphan-account"
	now := time.Now().UnixNano()
	platformRow := model.Platform{
		ID:                               platformID,
		Name:                             platformName,
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      now,
	}
	if err := engine.UpsertPlatform(platformRow); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(platformRow)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	var removedAccounts []string
	router := routing.NewRouter(routing.RouterConfig{
		Pool: pool,
		OnLeaseEvent: func(event routing.LeaseEvent) {
			switch event.Type {
			case routing.LeaseCreate, routing.LeaseTouch, routing.LeaseReplace:
				engine.MarkLease(event.PlatformID, event.Account)
			case routing.LeaseRemove, routing.LeaseExpire:
				removedAccounts = append(removedAccounts, event.Account)
				engine.MarkLeaseDelete(event.PlatformID, event.Account)
			}
		},
	})
	leaseNode := node.HashFromRawOptions([]byte(`{"type":"lease-node"}`))
	if err := router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        account,
		NodeHash:       leaseNode.Hex(),
		EgressIP:       "203.0.113.10",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("UpsertLease: %v", err)
	}
	if err := engine.FlushDirtySets(state.CacheReaders{
		ReadLease: router.ReadLease,
	}); err != nil {
		t.Fatalf("flush initial lease: %v", err)
	}

	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		Router: router,
	}
	if err := cp.DeletePlatform(platformID); err != nil {
		t.Fatalf("DeletePlatform: %v", err)
	}
	if len(removedAccounts) != 1 || removedAccounts[0] != account {
		t.Fatalf("lease delete events = %#v, want exactly account %q", removedAccounts, account)
	}

	if router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: account}) != nil {
		t.Fatal("deleted platform retained an in-memory lease")
	}
	if _, ok := router.SnapshotIPLoad(platformID)[netip.MustParseAddr("203.0.113.10")]; ok {
		t.Fatal("deleted platform retained IP load state")
	}
	if router.RangeLeases(platformID, func(string, routing.Lease) bool { return false }) {
		t.Fatal("deleted platform retained router state")
	}
	if err := engine.FlushDirtySets(state.CacheReaders{ReadLease: router.ReadLease}); err != nil {
		t.Fatalf("flush deleted lease: %v", err)
	}
	leases, err := engine.LoadAllLeases()
	if err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("deleted platform left %d persisted leases", len(leases))
	}
}

func TestDeletePlatform_RangeLeasesFailsClosedAfterPoolUnregister(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const (
		platformID = "platform-range-delete-boundary"
		account    = "range-delete-account"
	)
	now := time.Now().UnixNano()
	row := model.Platform{
		ID:                               platformID,
		Name:                             "range-delete-boundary",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      now,
	}
	if err := engine.UpsertPlatform(row); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	plat, err := platform.BuildFromModel(row)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	leaseNode := node.HashFromRawOptions([]byte(`{"type":"range-delete-node"}`))
	if err := router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        account,
		NodeHash:       leaseNode.Hex(),
		EgressIP:       "203.0.113.21",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("UpsertLease: %v", err)
	}

	unregistered := make(chan struct{})
	allowStateRemoval := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowStateRemoval) }) }
	t.Cleanup(release)
	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   pool,
		Router: router,
	}
	cp.afterPlatformUnregisterHook = func() {
		close(unregistered)
		<-allowStateRemoval
	}

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- cp.DeletePlatform(platformID) }()
	select {
	case <-unregistered:
	case <-time.After(time.Second):
		t.Fatal("DeletePlatform did not reach the post-unregister boundary")
	}

	count := 0
	if ok := router.RangeLeases(platformID, func(string, routing.Lease) bool {
		count++
		return true
	}); ok || count != 0 {
		release()
		t.Fatalf("RangeLeases exposed unregistered platform state: ok=%v count=%d", ok, count)
	}
	release()
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeletePlatform: %v", err)
	}
}

func TestDeletePlatform_RejectsMissingRouterBeforePersistence(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-delete-missing-router"
	initial := model.Platform{
		ID:                               platformID,
		Name:                             "delete-missing-router",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      time.Now().UnixNano(),
	}
	if err := engine.UpsertPlatform(initial); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(initial)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	cp := &ControlPlaneService{Engine: engine, Pool: pool}
	if err := cp.DeletePlatform(platformID); err == nil {
		t.Fatal("DeletePlatform unexpectedly succeeded without Router")
	}
	if _, err := engine.GetPlatform(platformID); err != nil {
		t.Fatalf("missing Router deleted persisted platform: %v", err)
	}
	got, ok := pool.GetPlatform(platformID)
	if !ok || got != runtimePlatform {
		t.Fatalf("missing Router changed runtime platform: got=%p want=%p", got, runtimePlatform)
	}
}

func TestDeletePlatform_RejectsMissingRuntimeBeforePersistence(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-delete-missing-runtime"
	initial := model.Platform{
		ID:                               platformID,
		Name:                             "delete-missing-runtime",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      time.Now().UnixNano(),
	}
	if err := engine.UpsertPlatform(initial); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	cp := &ControlPlaneService{Engine: engine, Pool: pool, Router: router}

	if err := cp.DeletePlatform(platformID); err == nil {
		t.Fatal("DeletePlatform unexpectedly succeeded without a registered runtime platform")
	}
	if _, err := engine.GetPlatform(platformID); err != nil {
		t.Fatalf("missing runtime deleted persisted platform: %v", err)
	}
	if _, ok := pool.GetPlatform(platformID); ok {
		t.Fatal("missing runtime test unexpectedly published a platform")
	}
}

func TestDeletePlatform_PreservesNotFoundWhenRuntimeAndPersistenceAreMissing(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	cp := &ControlPlaneService{Engine: engine, Pool: pool, Router: router}

	err = cp.DeletePlatform("platform-missing-everywhere")
	serviceErr, ok := err.(*ServiceError)
	if !ok || serviceErr.Code != "NOT_FOUND" {
		t.Fatalf("DeletePlatform error = %T %v, want NOT_FOUND", err, err)
	}
}

func TestListLeasesReportsPlatformNotFoundAfterDeletion(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-list-delete-race"
	platformRow := model.Platform{
		ID:                               platformID,
		Name:                             "list-delete-race",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      time.Now().UnixNano(),
	}
	if err := engine.UpsertPlatform(platformRow); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(platformRow)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	now := time.Now().UnixNano()
	if err := router.UpsertLease(model.Lease{
		PlatformID:  platformID,
		Account:     "list-account",
		NodeHash:    node.HashFromRawOptions([]byte(`{"id":"list-delete"}`)).Hex(),
		EgressIP:    "203.0.113.40",
		CreatedAtNs: now,
		ExpiryNs:    now + int64(time.Hour),
	}); err != nil {
		t.Fatalf("UpsertLease: %v", err)
	}
	cp := &ControlPlaneService{Engine: engine, Pool: pool, Router: router}

	readChecked := make(chan struct{})
	releaseRead := make(chan struct{})
	cp.beforeLeaseServiceRouterReadHook = func() {
		close(readChecked)
		<-releaseRead
	}

	type listResult struct {
		leases []LeaseResponse
		err    error
	}
	listDone := make(chan listResult, 1)
	go func() {
		leases, listErr := cp.ListLeases(platformID)
		listDone <- listResult{leases: leases, err: listErr}
	}()
	select {
	case <-readChecked:
	case <-time.After(time.Second):
		t.Fatal("ListLeases did not reach the atomic Router-read barrier")
	}

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- cp.DeletePlatform(platformID) }()
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeletePlatform: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DeletePlatform did not finish while ListLeases was before the Router read")
	}
	close(releaseRead)

	var listed listResult
	select {
	case listed = <-listDone:
	case <-time.After(time.Second):
		t.Fatal("ListLeases did not finish after releasing the read barrier")
	}
	if listed.err == nil {
		t.Fatalf("ListLeases unexpectedly succeeded after platform deletion: %#v", listed.leases)
	}
	assertServiceErrorCode(t, listed.err, "NOT_FOUND")
}

func TestRebuildPlatformViewRejectsDeletedTarget(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(
		filepath.Join(dir, "state"),
		filepath.Join(dir, "cache"),
	)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const platformID = "platform-rebuild-delete-order"
	platformRow := model.Platform{
		ID:                               platformID,
		Name:                             "rebuild-delete-order",
		StickyTTLNs:                      int64(time.Hour),
		RegexFilters:                     []string{},
		RegionFilters:                    []string{},
		ResponseRules:                    []model.PlatformResponseRule{},
		ReverseProxyMissAction:           string(platform.ReverseProxyMissActionTreatAsEmpty),
		ReverseProxyEmptyAccountBehavior: string(platform.ReverseProxyEmptyAccountBehaviorRandom),
		AllocationPolicy:                 string(platform.AllocationPolicyBalanced),
		UpdatedAtNs:                      time.Now().UnixNano(),
	}
	if err := engine.UpsertPlatform(platformRow); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}
	runtimePlatform, err := platform.BuildFromModel(platformRow)
	if err != nil {
		t.Fatalf("BuildFromModel: %v", err)
	}
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	if err := pool.RegisterPlatform(runtimePlatform); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	cp := &ControlPlaneService{Engine: engine, Pool: pool, Router: router}

	rebuildEntered := make(chan struct{})
	allowRebuild := make(chan struct{})
	cp.beforePlatformRebuildHook = func() {
		close(rebuildEntered)
		<-allowRebuild
	}

	rebuildDone := make(chan error, 1)
	go func() { rebuildDone <- cp.RebuildPlatformView(platformID) }()
	select {
	case <-rebuildEntered:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not reach the controlled boundary")
	}

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- cp.DeletePlatform(platformID) }()
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeletePlatform: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DeletePlatform did not complete while rebuild was paused after its platform lookup")
	}
	close(allowRebuild)

	if err := <-rebuildDone; err == nil {
		t.Fatal("RebuildPlatform unexpectedly succeeded after DeletePlatform removed its target")
	}
}
