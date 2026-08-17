package topology

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
)

func TestSchedulerRefreshDoesNotExposeMixedRuntimeGeneration(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	plat, ok := pool.GetPlatform(platform.DefaultPlatformID)
	if !ok {
		t.Fatal("default platform was not registered")
	}
	oldRaw := `{"type":"shadowsocks","tag":"old-generation","server":"1.1.1.1","server_port":443}`
	newRaw := `{"type":"vmess","tag":"new-generation","server":"2.2.2.2","server_port":443}`
	oldHash := addPickerTestNode(t, pool, sub, oldRaw, "old-generation", true)
	newHash := node.HashFromRawOptions([]byte(newRaw))

	var notifyOnce sync.Once
	notifyEntered := make(chan struct{})
	allowNotify := make(chan struct{})
	pool.beforePlatformNotifyHook = func(got *platform.Platform) {
		if got != plat {
			return
		}
		notifyOnce.Do(func() {
			close(notifyEntered)
			<-allowNotify
		})
	}
	readAttempted := make(chan struct{})
	allowRead := make(chan struct{})
	readDone := make(chan struct{})
	var observedMixed atomic.Bool
	pool.beforeRuntimeReadLockHook = func() {
		close(readAttempted)
		<-allowRead
	}
	t.Cleanup(func() {
		select {
		case <-allowNotify:
		default:
			close(allowNotify)
		}
		pool.beforePlatformNotifyHook = nil
		pool.beforeRuntimeReadLockHook = nil
		select {
		case <-allowRead:
		default:
			close(allowRead)
		}
	})

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgrForBatchTest(sub),
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return makeSubscriptionJSON(newRaw), nil
		},
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()
	select {
	case <-notifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the first platform notification")
	}
	go func() {
		pool.WithRuntimeRead(func() {
			managed := sub.ManagedNodes()
			_, hasNew := managed.LoadNode(newHash)
			_, hasOld := managed.LoadNode(oldHash)
			if hasNew && !hasOld && plat.View().Contains(oldHash) && !plat.View().Contains(newHash) {
				observedMixed.Store(true)
			}
		})
		close(readDone)
	}()
	<-readAttempted
	close(allowRead)
	close(allowNotify)
	if ok := <-refreshDone; !ok {
		t.Fatal("refresh was not admitted")
	}
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime read did not finish after the refresh")
	}
	if observedMixed.Load() {
		t.Fatal("runtime read observed mixed ManagedNodes/platform generation")
	}
}

func TestSchedulerRefreshDoesNotHoldRuntimeReadLockDuringNodePreparation(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	newRaw := `{"type":"vmess","tag":"prepared-outside-batch","server":"2.2.2.2","server_port":443}`
	newHash := node.HashFromRawOptions([]byte(newRaw))

	prepEntered := make(chan struct{})
	allowPrep := make(chan struct{})
	var prepOnce sync.Once
	pool.SetOnNodeAddedRuntime(func(hash node.Hash, _ *node.NodeEntry) {
		if hash != newHash {
			return
		}
		prepOnce.Do(func() {
			close(prepEntered)
			<-allowPrep
		})
	})
	t.Cleanup(func() {
		select {
		case <-allowPrep:
		default:
			close(allowPrep)
		}
		pool.SetOnNodeAddedRuntime(nil)
	})

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgrForBatchTest(sub),
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return makeSubscriptionJSON(newRaw), nil
		},
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()
	select {
	case <-prepEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not enter node runtime preparation")
	}

	readDone := make(chan struct{})
	go func() {
		pool.WithRuntimeRead(func() {})
		close(readDone)
	}()
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime read remained blocked by external node preparation")
	}

	close(allowPrep)
	select {
	case ok := <-refreshDone:
		if !ok {
			t.Fatal("refresh was not admitted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish after node preparation")
	}
}

func TestSchedulerRefreshDoesNotHoldRuntimeReadLockDuringPlatformNotify(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	defaultPlatform, ok := pool.GetPlatform(platform.DefaultPlatformID)
	if !ok {
		t.Fatal("default platform was not registered")
	}
	oldRaw := `{"type":"vmess","tag":"notify-old","server":"203.0.113.81","server_port":443}`
	newRaw := `{"type":"vmess","tag":"notify-new","server":"203.0.113.82","server_port":443}`
	addPickerTestNode(t, pool, sub, oldRaw, "notify-old", true)

	allowNotify := make(chan struct{})
	notifyEntered := make(chan struct{})
	var notifyOnce sync.Once
	pool.beforePlatformNotifyHook = func(got *platform.Platform) {
		if got != defaultPlatform {
			return
		}
		notifyOnce.Do(func() {
			close(notifyEntered)
			<-allowNotify
		})
	}
	readerBlocked := make(chan struct{})
	var readerBlockedOnce sync.Once
	pool.runtimeBatchMu.afterReaderBlocked = func() {
		readerBlockedOnce.Do(func() { close(readerBlocked) })
	}
	defer func() {
		select {
		case <-allowNotify:
		default:
			close(allowNotify)
		}
		pool.beforePlatformNotifyHook = nil
		pool.runtimeBatchMu.afterReaderBlocked = nil
	}()

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgrForBatchTest(sub),
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return makeSubscriptionJSON(newRaw), nil
		},
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()
	select {
	case <-notifyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the real platform notification")
	}

	router := routing.NewRouter(routing.RouterConfig{Pool: pool})
	routeDone := make(chan error, 1)
	go func() {
		_, err := router.RouteRequest("", "notify-account", "https://example.com")
		routeDone <- err
	}()

	select {
	case err := <-routeDone:
		if err != nil && !errors.Is(err, routing.ErrRuntimeGenerationBusy) {
			t.Fatalf("RouteRequest during platform notification: %v", err)
		}
	case <-readerBlocked:
		t.Fatal("RouteRequest was blocked by a slow platform notification holding the runtime batch writer")
	case <-time.After(2 * time.Second):
		t.Fatal("RouteRequest did not reach a result while platform notification was gated")
	}

	close(allowNotify)
	select {
	case ok := <-refreshDone:
		if !ok {
			t.Fatal("refresh was not admitted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish after platform notification release")
	}
}

func TestSchedulerRuntimePreparationDoesNotCrossEntryGeneration(t *testing.T) {
	pool, sub := newPoolPickerTestPool(t, nil)
	raw := `{"type":"vmess","tag":"generation-owner","server":"203.0.113.77","server_port":443}`
	hash := node.HashFromRawOptions([]byte(raw))

	prepEntered := make(chan struct{})
	allowPrep := make(chan struct{})
	var firstPrepHook atomic.Bool
	var entryA *node.NodeEntry
	pool.beforeNodeAddedRuntimeHook = func(got node.Hash, expected *node.NodeEntry) {
		if got != hash || !firstPrepHook.CompareAndSwap(false, true) {
			return
		}
		entryA = expected
		close(prepEntered)
		<-allowPrep
	}
	callbacks := make(chan *node.NodeEntry, 4)
	pool.SetOnNodeAddedRuntime(func(_ node.Hash, expected *node.NodeEntry) {
		callbacks <- expected
	})
	t.Cleanup(func() {
		select {
		case <-allowPrep:
		default:
			close(allowPrep)
		}
		pool.beforeNodeAddedRuntimeHook = nil
		pool.SetOnNodeAddedRuntime(nil)
	})

	scheduler := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgrForBatchTest(sub),
		Pool:       pool,
		Fetcher: func(context.Context, string) ([]byte, error) {
			return makeSubscriptionJSON(raw), nil
		},
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- scheduler.UpdateSubscription(sub) }()
	select {
	case <-prepEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the deferred runtime preparation")
	}
	if entryA == nil {
		t.Fatal("runtime preparation did not capture the created entry")
	}

	pool.RemoveNodeFromSub(hash, sub.ID)
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("entry A remained after removing its only subscription")
	}
	pool.AddNodeFromSub(hash, []byte(raw), sub.ID)
	entryB, ok := pool.GetEntry(hash)
	if !ok || entryB == nil || entryB == entryA {
		t.Fatal("same-hash replacement did not create entry B")
	}

	select {
	case got := <-callbacks:
		if got != entryB {
			t.Fatalf("replacement callback used stale entry: got %p, want %p", got, entryB)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement entry did not receive its own runtime preparation")
	}
	close(allowPrep)

	select {
	case ok := <-refreshDone:
		if !ok {
			t.Fatal("refresh was not admitted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish after stale preparation was released")
	}
	select {
	case got := <-callbacks:
		t.Fatalf("stale entry received a late runtime callback: %p", got)
	default:
	}
}

func subMgrForBatchTest(sub *subscription.Subscription) *SubscriptionManager {
	mgr := NewSubscriptionManager()
	mgr.Register(sub)
	return mgr
}
