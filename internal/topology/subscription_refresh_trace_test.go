package topology

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/subscription"
)

func TestSchedulerRefreshTraceCorrelatesConcurrentStages(t *testing.T) {
	subMgr := NewSubscriptionManager()
	first := subscription.NewSubscription("trace-first", "Trace first", "https://example.test/first", true, false)
	second := subscription.NewSubscription("trace-second", "Trace second", "https://example.test/second", true, false)
	subMgr.Register(first)
	subMgr.Register(second)

	var (
		mu     sync.Mutex
		events []RefreshEvent
	)
	fetchEntered := make(chan string, 2)
	releaseFetch := make(chan struct{})
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       newTestPool(subMgr),
		Fetcher: func(_ context.Context, url string) ([]byte, error) {
			fetchEntered <- url
			<-releaseFetch
			return makeSubscriptionJSON(`{"type":"shadowsocks","tag":"` + url + `","server":"1.1.1.1","server_port":443}`), nil
		},
		FetchTotalTimeout:      2 * time.Second,
		FetchAttemptTimeoutCap: 500 * time.Millisecond,
		OnRefreshEvent: func(event RefreshEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})

	results := make(chan bool, 2)
	go func() { results <- sched.UpdateSubscription(first) }()
	go func() { results <- sched.UpdateSubscription(second) }()
	for range 2 {
		select {
		case <-fetchEntered:
		case <-time.After(time.Second):
			t.Fatal("concurrent refresh did not reach fetch barrier")
		}
	}
	close(releaseFetch)
	for range 2 {
		select {
		case admitted := <-results:
			if !admitted {
				t.Fatal("refresh admission failed")
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent refresh did not finish")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("expected refresh trace events")
	}
	bySubscription := make(map[string]string)
	byCorrelation := make(map[string]string)
	for _, event := range events {
		if event.CorrelationID == "" {
			t.Fatalf("refresh event has empty correlation_id: %+v", event)
		}
		if previous := bySubscription[event.SubscriptionID]; previous != "" && previous != event.CorrelationID {
			t.Fatalf("subscription %q crossed correlation IDs: %q and %q", event.SubscriptionID, previous, event.CorrelationID)
		}
		bySubscription[event.SubscriptionID] = event.CorrelationID
		if previous := byCorrelation[event.CorrelationID]; previous != "" && previous != event.SubscriptionID {
			t.Fatalf("correlation ID %q crossed subscriptions: %q and %q", event.CorrelationID, previous, event.SubscriptionID)
		}
		byCorrelation[event.CorrelationID] = event.SubscriptionID
	}
	if len(bySubscription) != 2 || len(byCorrelation) != 2 {
		t.Fatalf("trace identity groups = subscriptions:%d correlations:%d, want 2/2", len(bySubscription), len(byCorrelation))
	}

	wantStages := []RefreshStage{
		RefreshStageStart,
		RefreshStageFetchStart,
		RefreshStageFetchEnd,
		RefreshStageParseStart,
		RefreshStageParseEnd,
		RefreshStageApplyStart,
		RefreshStageApplyEnd,
		RefreshStageRuntimePreparationStart,
		RefreshStageRuntimePreparationEnd,
		RefreshStageFinished,
	}
	byCorrelationStages := make(map[string][]RefreshStage)
	for _, event := range events {
		byCorrelationStages[event.CorrelationID] = append(byCorrelationStages[event.CorrelationID], event.Stage)
		if event.CallerDeadlineSet {
			t.Fatalf("background refresh unexpectedly inherited a caller deadline: %+v", event)
		}
		if event.FetchTotalTimeout != 2*time.Second || event.FetchAttemptTimeoutCap != 500*time.Millisecond {
			t.Fatalf("refresh budget trace = total:%s cap:%s", event.FetchTotalTimeout, event.FetchAttemptTimeoutCap)
		}
	}
	for correlationID, gotStages := range byCorrelationStages {
		if len(gotStages) != len(wantStages) {
			t.Fatalf("correlation ID %q has stages %v, want %v", correlationID, gotStages, wantStages)
		}
		for i, want := range wantStages {
			if gotStages[i] != want {
				t.Fatalf("correlation ID %q stage[%d] = %q, want %q; all=%v", correlationID, i, gotStages[i], want, gotStages)
			}
		}
	}
}
