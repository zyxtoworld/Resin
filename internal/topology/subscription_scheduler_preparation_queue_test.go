package topology

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/runtimeguard"
	"github.com/Resinat/Resin/internal/subscription"
)

func TestSchedulerRuntimePreparationCommitGateAndFailureIsObservable(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("prep-commit", "Preparation commit", "https://example.test/prep", true, false)
	subMgr.Register(sub)
	pool := newTestPool(subMgr)
	raw := `{"type":"shadowsocks","tag":"prep-commit","server":"198.18.0.1","server_port":443}`
	hash := node.HashFromRawOptions([]byte(raw))

	var (
		eventsMu        sync.Mutex
		events          []RefreshEvent
		callbackEntered = make(chan struct{})
		callbackOnce    sync.Once
		commitChecked   atomic.Bool
	)
	persistence := func(got *subscription.Subscription) {
		if got.LastUpdatedNs.Load() == 0 || got.GetLastError() != "" {
			t.Errorf("persistence callback saw uncommitted state: updated=%d error=%q", got.LastUpdatedNs.Load(), got.GetLastError())
		}
		if _, ok := got.ManagedNodes().LoadNode(hash); !ok {
			t.Errorf("persistence callback did not see managed hash %s", hash.Hex())
		}
		eventsMu.Lock()
		for _, event := range events {
			if event.Stage == RefreshStageFinished {
				t.Errorf("commit finished event preceded persistence callback: %+v", event)
			}
		}
		eventsMu.Unlock()
		commitChecked.Store(true)
	}
	pool.SetOnNodeAddedRuntime(func(got node.Hash, entry *node.NodeEntry) {
		if got != hash {
			return
		}
		callbackOnce.Do(func() { close(callbackEntered) })
		entry.SetLastError("prep failed")
	})
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager:   subMgr,
		Pool:         pool,
		Fetcher:      func(context.Context, string) ([]byte, error) { return makeSubscriptionJSON(raw), nil },
		OnSubUpdated: persistence,
		OnRefreshEvent: func(event RefreshEvent) {
			eventsMu.Lock()
			events = append(events, event)
			eventsMu.Unlock()
		},
	})

	if !sched.UpdateSubscription(sub) {
		t.Fatal("refresh admission failed")
	}
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime preparation did not run")
	}
	if !commitChecked.Load() {
		t.Fatal("subscription persistence commit was not observed before preparation")
	}
	if sub.LastUpdatedNs.Load() == 0 || sub.GetLastError() != "" {
		t.Fatalf("successful refresh state = updated:%d error:%q", sub.LastUpdatedNs.Load(), sub.GetLastError())
	}

	sched.Stop()
	entry, ok := pool.GetEntry(hash)
	if !ok || entry.GetLastError() != "prep failed" {
		t.Fatalf("runtime failure was not recorded on node entry: ok=%v error=%q", ok, entry.GetLastError())
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	var prepEnd *RefreshEvent
	for i := range events {
		if events[i].Stage == RefreshStageRuntimePreparationEnd {
			copy := events[i]
			prepEnd = &copy
		}
	}
	if prepEnd == nil || prepEnd.Result != "error" || prepEnd.ParentCorrelationID == "" {
		t.Fatalf("runtime preparation failure event = %+v", prepEnd)
	}
}

func TestSchedulerRuntimePreparationTwoLargeSubscriptionsAreNotNodeCapped(t *testing.T) {
	const nodesPerSubscription = 4097
	subMgr := NewSubscriptionManager()
	first := subscription.NewSubscription("large-first", "Large first", "local://large-first", true, false)
	second := subscription.NewSubscription("large-second", "Large second", "local://large-second", true, false)
	subMgr.Register(first)
	subMgr.Register(second)
	pool := newTestPool(subMgr)

	var (
		calls     atomic.Int64
		active    atomic.Int32
		maxActive atomic.Int32
	)
	pool.SetOnNodeAddedRuntime(func(node.Hash, *node.NodeEntry) {
		current := active.Add(1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		calls.Add(1)
		active.Add(-1)
	})
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher: func(_ context.Context, url string) ([]byte, error) {
			if url == first.URL() {
				return largeSubscriptionJSON("first", nodesPerSubscription), nil
			}
			return largeSubscriptionJSON("second", nodesPerSubscription), nil
		},
	})

	results := make(chan bool, 2)
	go func() { results <- sched.UpdateSubscription(first) }()
	go func() { results <- sched.UpdateSubscription(second) }()
	for range 2 {
		select {
		case admitted := <-results:
			if !admitted {
				t.Fatal("large refresh admission failed")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("large refresh did not commit")
		}
	}
	sched.Stop()

	want := int64(nodesPerSubscription * 2)
	if got := calls.Load(); got != want {
		t.Fatalf("runtime preparation calls = %d, want all %d nodes", got, want)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("runtime preparation concurrency = %d, want one worker", got)
	}
	sched.runtimePrepMu.Lock()
	pending := len(sched.runtimePrepPending)
	workerRuns := sched.runtimePrepWorkerRuns
	sched.runtimePrepMu.Unlock()
	if pending != 0 || workerRuns != 1 {
		t.Fatalf("runtime queue state after Stop = pending:%d worker_runs:%d, want 0/1", pending, workerRuns)
	}
}

func TestSchedulerRuntimePreparationHighFrequencyCoalescesBySubscription(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("prep-coalesce", "Preparation coalesce", "local://coalesce", true, false)
	subMgr.Register(sub)
	pool := newTestPool(subMgr)
	raw := `{"type":"shadowsocks","tag":"prep-coalesce","server":"198.18.0.2","server_port":443}`
	hash := node.HashFromRawOptions([]byte(raw))

	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	var calls atomic.Int32
	pool.SetOnNodeAddedRuntime(func(node.Hash, *node.NodeEntry) {
		calls.Add(1)
		enteredOnce.Do(func() {
			close(entered)
			<-release
		})
	})
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    func(context.Context, string) ([]byte, error) { return makeSubscriptionJSON(raw), nil },
	})
	if !sched.UpdateSubscription(sub) {
		t.Fatal("initial refresh admission failed")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial preparation did not block")
	}
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("initial entry missing")
	}
	for generation := int64(2); generation <= 1001; generation++ {
		sub.MarkAppliedAttempt(generation)
		if !sched.enqueueRuntimePreparation(
			sub,
			sub.ConfigVersion(),
			generation,
			map[node.Hash]*node.NodeEntry{hash: entry},
			newRefreshTrace(context.Background(), sub.ID, generation, 0, 0, nil),
		) {
			t.Fatalf("generation %d was not admitted", generation)
		}
	}
	sched.runtimePrepMu.Lock()
	pending := len(sched.runtimePrepPending)
	order := len(sched.runtimePrepOrder) - sched.runtimePrepHead
	workerRuns := sched.runtimePrepWorkerRuns
	sched.runtimePrepMu.Unlock()
	if pending != 1 || order != 1 || workerRuns != 1 {
		t.Fatalf("high-frequency queue state = pending:%d order:%d worker_runs:%d, want 1/1/1", pending, order, workerRuns)
	}
	close(release)
	sched.Stop()
	if got := calls.Load(); got != 2 {
		t.Fatalf("coalesced preparation callback count = %d, want initial plus latest", got)
	}
}

func TestSchedulerRuntimePreparationDisableRaceIsFailClosed(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("prep-disable", "Preparation disable", "local://disable", true, false)
	subMgr.Register(sub)
	pool := newTestPool(subMgr)
	raw := `{"type":"shadowsocks","tag":"prep-disable","server":"198.18.0.3","server_port":443}`
	hash := node.HashFromRawOptions([]byte(raw))
	entered := make(chan struct{})
	release := make(chan struct{})
	var callbackCount atomic.Int32
	pool.beforeNodeAddedRuntimeHook = func(got node.Hash, _ *node.NodeEntry) {
		if got != hash {
			return
		}
		close(entered)
		<-release
	}
	pool.SetOnNodeAddedRuntime(func(node.Hash, *node.NodeEntry) { callbackCount.Add(1) })
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    func(context.Context, string) ([]byte, error) { return makeSubscriptionJSON(raw), nil },
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- sched.UpdateSubscription(sub) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("preparation did not reach identity re-check gate")
	}
	sub.SetEnabled(false)
	close(release)
	if !<-refreshDone {
		t.Fatal("refresh admission failed")
	}
	sched.Stop()
	if got := callbackCount.Load(); got != 0 {
		t.Fatalf("disabled subscription received runtime callback count %d", got)
	}
}

func TestSchedulerStopWhilePersistenceCallbackBlockedHasNoPreparationGate(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("prep-stop", "Preparation stop", "local://stop", true, false)
	subMgr.Register(sub)
	pool := newTestPool(subMgr)
	raw := `{"type":"shadowsocks","tag":"prep-stop","server":"198.18.0.4","server_port":443}`
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var callbackOnce sync.Once
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    func(context.Context, string) ([]byte, error) { return makeSubscriptionJSON(raw), nil },
		OnSubUpdated: func(*subscription.Subscription) {
			callbackOnce.Do(func() { close(callbackEntered) })
			<-releaseCallback
		},
	})
	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- sched.UpdateSubscription(sub) }()
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("persistence callback did not block")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := sched.StopContext(stopCtx); err == nil {
		t.Fatal("StopContext unexpectedly waited for blocked persistence callback")
	}
	sched.runtimePrepMu.Lock()
	pending := len(sched.runtimePrepPending)
	sched.runtimePrepMu.Unlock()
	if pending != 0 {
		t.Fatalf("preparation was admitted before commit callback returned: pending=%d", pending)
	}
	close(releaseCallback)
	if !<-refreshDone {
		t.Fatal("refresh admission failed after callback release")
	}
	sched.Stop()
}

func TestSchedulerRuntimePreparationDoesNotBlockRefreshDisableOrDelete(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("prep-epoch", "Preparation epoch", "local://epoch", true, false)
	subMgr.Register(sub)
	pool := newTestPool(subMgr)
	raw := `{"type":"shadowsocks","tag":"prep-epoch","server":"198.18.0.5","server_port":443}`
	hash := node.HashFromRawOptions([]byte(raw))
	builderEntered := make(chan struct{})
	releaseBuilder := make(chan struct{})
	var builderOnce sync.Once
	var published atomic.Int32
	pool.SetOnNodeAddedRuntimeGuarded(func(got node.Hash, _ *node.NodeEntry, guard *runtimeguard.Guard) {
		if got != hash {
			return
		}
		builderOnce.Do(func() { close(builderEntered) })
		<-releaseBuilder
		if guard.Allowed() {
			published.Add(1)
		}
	})
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    func(context.Context, string) ([]byte, error) { return makeSubscriptionJSON(raw), nil },
	})
	if !sched.UpdateSubscription(sub) {
		t.Fatal("initial refresh admission failed")
	}
	select {
	case <-builderEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime builder did not block")
	}

	refreshDone := make(chan bool, 1)
	go func() { refreshDone <- sched.UpdateSubscription(sub) }()
	select {
	case admitted := <-refreshDone:
		if !admitted {
			t.Fatal("second refresh admission failed")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second refresh waited behind the blocked runtime builder")
	}

	disableDone := make(chan struct{})
	go func() {
		sched.SetSubscriptionEnabled(sub, false)
		close(disableDone)
	}()
	select {
	case <-disableDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("disable waited behind the blocked runtime builder")
	}

	deleteDone := make(chan struct{})
	go func() {
		subMgr.Unregister(sub.ID)
		close(deleteDone)
	}()
	select {
	case <-deleteDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("delete waited behind the blocked runtime builder")
	}
	close(releaseBuilder)
	sched.Stop()
	if got := published.Load(); got != 0 {
		t.Fatalf("invalidated runtime builder published resources: %d", got)
	}
}

func TestSubscriptionManagerUnregisterExactKeepsSameIDReplacement(t *testing.T) {
	manager := NewSubscriptionManager()
	oldSub := subscription.NewSubscription("same-id", "old", "local://old", true, false)
	newSub := subscription.NewSubscription("same-id", "new", "local://new", true, false)
	manager.Register(oldSub)
	oldGuard := oldSub.RuntimePreparationGuard()
	manager.Register(newSub)
	if manager.UnregisterExact(oldSub.ID, oldSub) {
		t.Fatal("stale subscription instance removed its replacement")
	}
	if oldGuard == nil || oldGuard.Allowed() {
		t.Fatal("old subscription guard was not invalidated when replacement was registered")
	}
	if manager.Lookup(newSub.ID) != newSub {
		t.Fatal("same-ID replacement was not preserved")
	}
	newGuard := newSub.RuntimePreparationGuard()
	if !manager.UnregisterExact(newSub.ID, newSub) {
		t.Fatal("current subscription instance was not removed")
	}
	if newGuard == nil || newGuard.Allowed() {
		t.Fatal("removed subscription guard remained valid")
	}
}

func largeSubscriptionJSON(prefix string, count int) []byte {
	var builder strings.Builder
	builder.Grow(count * 120)
	serverSecondOctet := "18"
	if prefix == "second" {
		serverSecondOctet = "19"
	}
	builder.WriteString(`{"outbounds":[`)
	for i := 0; i < count; i++ {
		if i != 0 {
			builder.WriteByte(',')
		}
		third := (i / 250) % 250
		fourth := (i % 250) + 1
		builder.WriteString(`{"type":"shadowsocks","tag":"`)
		builder.WriteString(prefix)
		builder.WriteByte('-')
		builder.WriteString(strconv.Itoa(i))
		builder.WriteString(`","server":"198.`)
		builder.WriteString(serverSecondOctet)
		builder.WriteByte('.')
		builder.WriteString(strconv.Itoa(third))
		builder.WriteByte('.')
		builder.WriteString(strconv.Itoa(fourth))
		builder.WriteString(`","server_port":443}`)
	}
	builder.WriteString(`]}`)
	return []byte(builder.String())
}

func TestRuntimePreparationBatchDoesNotHideMixedStaleOutcome(t *testing.T) {
	var (
		mu     sync.Mutex
		result string
	)
	trace := newRefreshTrace(context.Background(), "mixed-batch", 1, 0, 0, func(event RefreshEvent) {
		if event.Stage != RefreshStageRuntimePreparationEnd {
			return
		}
		mu.Lock()
		result = event.Result
		mu.Unlock()
	})
	batch := newRuntimePreparationBatch(trace)
	batch.total = 2
	batch.remaining = 2
	batch.start()
	batch.complete("completed")
	batch.complete("stale")
	mu.Lock()
	defer mu.Unlock()
	if result != "stale" {
		t.Fatalf("mixed preparation result = %q, want stale", result)
	}
}
