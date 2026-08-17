package service

import (
	"context"
	"net/netip"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

func TestListNodesDoesNotObserveSubscriptionRefreshHalfGeneration(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"runtime-batch-sub",
		"runtime-batch-sub",
		"https://example.com/runtime-batch",
		true,
		false,
	)
	subMgr.Register(sub)
	oldRaw := []byte(`{"type":"shadowsocks","tag":"old-generation","server":"1.1.1.1","server_port":443}`)
	newRaw := []byte(`{"type":"shadowsocks","tag":"new-generation","server":"2.2.2.2","server_port":443}`)
	newHash := node.HashFromRawOptions(newRaw)

	var refreshPhase atomic.Bool
	var notifyOnce sync.Once
	notifyEntered := make(chan struct{})
	allowNotify := make(chan struct{})
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subMgr.Lookup,
		GeoLookup: func(netip.Addr) string { return "us" },
		OnSubNodeChanged: func(_ string, hash node.Hash, added bool) {
			if !refreshPhase.Load() || !added || hash != newHash {
				return
			}
			notifyOnce.Do(func() {
				close(notifyEntered)
				<-allowNotify
			})
		},
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})

	platformID := "runtime-batch-platform"
	plat := platform.NewPlatform(
		platformID,
		"runtime-batch-platform",
		[]*regexp.Regexp{regexp.MustCompile("old-generation")},
		nil,
	)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	_ = addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		oldRaw,
		"203.0.113.20",
		"old-generation",
	)
	oldHash := node.HashFromRawOptions(oldRaw)
	if !plat.View().Contains(oldHash) {
		t.Fatal("initial platform view does not contain old generation")
	}

	service := &ControlPlaneService{Pool: pool, SubMgr: subMgr}
	t.Cleanup(func() {
		select {
		case <-allowNotify:
		default:
			close(allowNotify)
		}
	})

	refreshPhase.Store(true)
	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return newRawSubscriptionBody(newRaw), nil
		},
	})
	refreshDone := make(chan bool, 1)
	go func() {
		refreshDone <- scheduler.UpdateSubscription(sub)
	}()

	select {
	case <-notifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the first platform notification")
	}

	listDone := make(chan []NodeSummary, 1)
	listErr := make(chan error, 1)
	go func() {
		platformID := platformID
		got, err := service.ListNodes(NodeFilters{
			PlatformID: &platformID,
		})
		if err != nil {
			listErr <- err
			return
		}
		listDone <- got
	}()

	select {
	case got := <-listDone:
		t.Fatalf("ListNodes returned during the refresh mutation: got %d nodes", len(got))
	case err := <-listErr:
		t.Fatalf("ListNodes returned during the refresh mutation: %v", err)
	case <-time.After(100 * time.Millisecond):
		// The fixed implementation is blocked in runtimeBatchMu.RLock while
		// the refresh owns the write side. Release the mutation below.
	}
	close(allowNotify)

	select {
	case ok := <-refreshDone:
		if !ok {
			t.Fatal("refresh was not admitted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish")
	}

	select {
	case err := <-listErr:
		t.Fatalf("ListNodes: %v", err)
	case got := <-listDone:
		if len(got) != 0 {
			t.Fatalf("ListNodes observed a stale platform generation: got %d nodes", len(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListNodes did not finish after the refresh")
	}
}

func TestGetSubscriptionDoesNotReenterRuntimeReadWhenWriterPending(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("runtime-read-reentry", "runtime-read-reentry", "url", true, false)
	subMgr.Register(sub)
	service := &ControlPlaneService{Pool: pool, SubMgr: subMgr}

	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	outerReadEntered := make(chan struct{})
	releaseOuterRead := make(chan struct{})
	allowWriter := make(chan struct{})
	writerEntered := make(chan struct{})
	writerDone := make(chan struct{})
	getDone := make(chan error, 1)
	var readLockCount atomic.Int32

	service.afterRuntimeReadLockHook = func() {
		if readLockCount.Add(1) != 1 {
			return
		}
		close(outerReadEntered)
		<-releaseOuterRead
	}
	t.Cleanup(func() {
		select {
		case <-releaseOuterRead:
		default:
			close(releaseOuterRead)
		}
		select {
		case <-allowWriter:
		default:
			close(allowWriter)
		}
		service.afterRuntimeReadLockHook = nil
	})

	go func() {
		_, err := service.GetSubscription(sub.ID)
		getDone <- err
	}()
	select {
	case <-outerReadEntered:
	case <-time.After(time.Second):
		t.Fatal("GetSubscription did not acquire its outer runtime read lock")
	}

	go func() {
		pool.WithRuntimeMutation(func() {
			close(writerEntered)
			<-allowWriter
		})
		close(writerDone)
	}()
	// With one scheduler P, yielding here lets the writer execute the actual
	// Lock call and become writer-pending while the outer read remains held.
	runtime.Gosched()
	close(releaseOuterRead)

	select {
	case err := <-getDone:
		if err != nil {
			t.Fatalf("GetSubscription: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("GetSubscription self-deadlocked on a nested runtime RLock")
	}

	select {
	case <-writerEntered:
		t.Fatal("runtime writer completed before its gate was released")
	default:
	}
	close(allowWriter)
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("runtime writer did not finish after release")
	}
}

func TestControlPlaneCompositeReadersWaitForRuntimeBatch(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("runtime-read-sub", "runtime-read-sub", "url", true, false)
	subMgr.Register(sub)
	raw := []byte(`{"type":"shadowsocks","tag":"runtime-read","server":"1.1.1.1","server_port":443}`)
	hash := addRoutableNodeForSubscriptionWithTag(t, pool, sub, raw, "203.0.113.30", "runtime-read")
	plat := platform.NewPlatform("runtime-read-platform", "runtime-read-platform", nil, nil)
	if err := pool.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	service := &ControlPlaneService{Pool: pool, SubMgr: subMgr}
	hashText := hash.Hex()
	platformID := plat.ID

	readers := []struct {
		name string
		read func()
	}{
		{name: "ListSubscriptions", read: func() { _, _ = service.ListSubscriptions(nil) }},
		{name: "GetSubscription", read: func() { _, _ = service.GetSubscription(sub.ID) }},
		{name: "GetNode", read: func() { _, _ = service.GetNode(hashText) }},
		{name: "PreviewFilter", read: func() {
			_, _ = service.PreviewFilter(PreviewFilterRequest{
				PlatformSpec: &PlatformSpecFilter{},
			})
		}},
		{name: "ListNodes", read: func() {
			_, _ = service.ListNodes(NodeFilters{PlatformID: &platformID})
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			writerEntered := make(chan struct{})
			allowWriter := make(chan struct{})
			writerDone := make(chan struct{})
			go func() {
				pool.WithRuntimeMutation(func() {
					close(writerEntered)
					<-allowWriter
				})
				close(writerDone)
			}()
			<-writerEntered

			readerDone := make(chan struct{})
			go func() {
				reader.read()
				close(readerDone)
			}()
			select {
			case <-readerDone:
				t.Fatal("composite reader returned while runtime mutation was active")
			case <-time.After(100 * time.Millisecond):
			}

			close(allowWriter)
			select {
			case <-readerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("composite reader did not finish after runtime mutation")
			}
			select {
			case <-writerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("runtime mutation owner did not finish")
			}
		})
	}
}

func TestListLeasesDoesNotObserveMixedRuntimeGeneration(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"runtime-lease-read-sub",
		"runtime-lease-read-sub",
		"https://example.com/runtime-lease-read",
		true,
		false,
	)
	subMgr.Register(sub)

	oldRaw := []byte(`{"type":"shadowsocks","tag":"lease-old","server":"1.1.1.1","server_port":443}`)
	hash := node.HashFromRawOptions(oldRaw)
	var holdLookup atomic.Bool
	var lookupOnce sync.Once
	lookupEntered := make(chan struct{})
	allowLookup := make(chan struct{})
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: func(id string) *subscription.Subscription {
			got := subMgr.Lookup(id)
			if got == sub && holdLookup.Load() {
				lookupOnce.Do(func() {
					close(lookupEntered)
					<-allowLookup
				})
			}
			return got
		},
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
	p := platform.NewPlatform("runtime-lease-read-platform", "runtime-lease-read-platform", nil, nil)
	if err := pool.RegisterPlatform(p); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}
	pool.AddNodeFromSub(hash, oldRaw, sub.ID)
	managedOld := subscription.NewManagedNodes()
	managedOld.StoreNode(hash, subscription.ManagedNode{Tags: []string{"old-tag"}})
	sub.SwapManagedNodes(managedOld)

	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	now := time.Now().UnixNano()
	if err := router.UpsertLease(model.Lease{
		PlatformID:  p.ID,
		Account:     "lease-read-account",
		NodeHash:    hash.Hex(),
		EgressIP:    "203.0.113.77",
		CreatedAtNs: now,
		ExpiryNs:    now + int64(time.Hour),
	}); err != nil {
		t.Fatalf("UpsertLease: %v", err)
	}

	service := &ControlPlaneService{Pool: pool, SubMgr: subMgr, Router: router}
	holdLookup.Store(true)
	t.Cleanup(func() {
		holdLookup.Store(false)
		select {
		case <-allowLookup:
		default:
			close(allowLookup)
		}
	})

	type listResult struct {
		leases []LeaseResponse
		err    error
	}
	listDone := make(chan listResult, 1)
	go func() {
		leases, err := service.ListLeases(p.ID)
		listDone <- listResult{leases: leases, err: err}
	}()
	select {
	case <-lookupEntered:
	case <-time.After(time.Second):
		t.Fatal("ListLeases did not reach node-tag resolution")
	}

	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)
	writerEntered := make(chan struct{})
	writerDone := make(chan struct{})
	managedNew := subscription.NewManagedNodes()
	managedNew.StoreNode(hash, subscription.ManagedNode{Tags: []string{"new-tag"}})
	go func() {
		pool.WithRuntimeMutation(func() {
			close(writerEntered)
			sub.SwapManagedNodes(managedNew)
		})
		close(writerDone)
	}()
	runtime.Gosched()

	select {
	case <-writerEntered:
		close(allowLookup)
		<-listDone
		t.Fatal("ListLeases allowed a runtime refresh to commit while resolving its node tag")
	case <-time.After(200 * time.Millisecond):
		// The fixed service holds the runtime read owner through both the Router
		// lease snapshot and node-tag projection. The writer must remain queued.
	}
	close(allowLookup)

	var listed listResult
	select {
	case listed = <-listDone:
	case <-time.After(time.Second):
		t.Fatal("ListLeases did not finish after releasing node-tag resolution")
	}
	if listed.err != nil {
		t.Fatalf("ListLeases: %v", listed.err)
	}
	if len(listed.leases) != 1 || listed.leases[0].NodeTag != "runtime-lease-read-sub/old-tag" {
		t.Fatalf("ListLeases observed mixed generation: %#v", listed.leases)
	}
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("runtime mutation did not finish after ListLeases released")
	}
}

func TestRebuildPlatformViewWaitsForSubscriptionRuntimeBatch(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription(
		"runtime-rebuild-sub",
		"runtime-rebuild-sub",
		"https://example.com/runtime-rebuild",
		true,
		false,
	)
	subMgr.Register(sub)
	p := newNodeListTestPool(subMgr)
	platformID := "runtime-rebuild-platform"
	plat := platform.NewPlatform(
		platformID,
		platformID,
		[]*regexp.Regexp{regexp.MustCompile("new-generation")},
		nil,
	)
	if err := p.RegisterPlatform(plat); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	raw := []byte(`{"type":"shadowsocks","tag":"new-generation","server":"2.2.2.2","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	next := subscription.NewManagedNodes()
	next.StoreNode(hash, subscription.ManagedNode{Tags: []string{"new-generation"}})

	mutationEntered := make(chan struct{})
	allowMutation := make(chan struct{})
	mutationDone := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(allowMutation) })
	})

	go func() {
		p.WithRuntimeMutation(func() {
			sub.SwapManagedNodes(next)
			close(mutationEntered)
			<-allowMutation
			addRoutableNodeForSubscriptionWithTag(t, p, sub, raw, "203.0.113.230", "new-generation")
		})
		close(mutationDone)
	}()
	<-mutationEntered

	cp := &ControlPlaneService{Pool: p, SubMgr: subMgr}
	rebuildCallEntered := make(chan struct{})
	var rebuildCallOnce sync.Once
	signalRebuildCall := func() { rebuildCallOnce.Do(func() { close(rebuildCallEntered) }) }
	cp.beforePlatformRebuildHook = signalRebuildCall
	rebuildDone := make(chan error, 1)
	go func() { rebuildDone <- cp.RebuildPlatformView(platformID) }()
	<-rebuildCallEntered

	// The old implementation has no runtime-batch read boundary and returns
	// while the writer still owns the batch. A timeout here is only a progress
	// watchdog; the mutation gate below is the ordering primitive.
	select {
	case err := <-rebuildDone:
		releaseOnce.Do(func() { close(allowMutation) })
		<-mutationDone
		if err != nil {
			t.Fatalf("RebuildPlatformView: %v", err)
		}
		t.Fatal("RebuildPlatformView returned before subscription runtime batch committed")
	case <-time.After(200 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(allowMutation) })
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("subscription runtime batch did not finish after release")
	}
	select {
	case err := <-rebuildDone:
		if err != nil {
			t.Fatalf("RebuildPlatformView after batch: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RebuildPlatformView did not finish after runtime batch")
	}
	if !plat.View().Contains(hash) {
		t.Fatal("RebuildPlatformView did not publish the complete subscription generation")
	}
}

func newRawSubscriptionBody(raw []byte) []byte {
	return []byte(`{"outbounds":[` + string(raw) + `]}`)
}
