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
			server := "1.1.1.1"
			if url == "https://example.test/second" {
				server = "1.1.1.2"
			}
			return makeSubscriptionJSON(`{"type":"shadowsocks","tag":"` + url + `","server":"` + server + `","server_port":443}`), nil
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
	sched.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("expected refresh trace events")
	}
	bySubscription := make(map[string]string)
	byCorrelation := make(map[string]string)
	commitCorrelations := make(map[string]struct{})
	preparationParents := make(map[string]string)
	for _, event := range events {
		if event.CorrelationID == "" {
			t.Fatalf("refresh event has empty correlation_id: %+v", event)
		}
		if event.ParentCorrelationID == "" {
			commitCorrelations[event.CorrelationID] = struct{}{}
			if previous := bySubscription[event.SubscriptionID]; previous != "" && previous != event.CorrelationID {
				t.Fatalf("subscription %q crossed commit correlation IDs: %q and %q", event.SubscriptionID, previous, event.CorrelationID)
			}
			bySubscription[event.SubscriptionID] = event.CorrelationID
		} else {
			preparationParents[event.CorrelationID] = event.ParentCorrelationID
		}
		if previous := byCorrelation[event.CorrelationID]; previous != "" && previous != event.SubscriptionID {
			t.Fatalf("correlation ID %q crossed subscriptions: %q and %q", event.CorrelationID, previous, event.SubscriptionID)
		}
		byCorrelation[event.CorrelationID] = event.SubscriptionID
	}
	if len(bySubscription) != 2 || len(commitCorrelations) != 2 || len(preparationParents) != 2 || len(byCorrelation) != 4 {
		t.Fatalf("trace identity groups = subscriptions:%d commits:%d preparations:%d correlations:%d, want 2/2/2/4", len(bySubscription), len(commitCorrelations), len(preparationParents), len(byCorrelation))
	}

	wantCommit := []RefreshStage{
		RefreshStageStart,
		RefreshStageFetchStart,
		RefreshStageFetchEnd,
		RefreshStageParseStart,
		RefreshStageParseEnd,
		RefreshStageApplyStart,
		RefreshStageApplyEnd,
		RefreshStageFinished,
	}
	wantPreparation := []RefreshStage{
		RefreshStageRuntimePreparationScheduled,
		RefreshStageRuntimePreparationStart,
		RefreshStageRuntimePreparationEnd,
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
		if preparationParents[correlationID] != "" {
			if !equalRefreshStages(gotStages, wantPreparation) {
				t.Fatalf("correlation ID %q has invalid preparation stages %v", correlationID, gotStages)
			}
			if _, ok := commitCorrelations[preparationParents[correlationID]]; !ok {
				t.Fatalf("preparation correlation ID %q has unknown parent %q", correlationID, preparationParents[correlationID])
			}
			continue
		}
		if len(gotStages) != len(wantCommit) {
			t.Fatalf("correlation ID %q has commit stages %v, want %v", correlationID, gotStages, wantCommit)
		}
		for i, wantStage := range wantCommit {
			if gotStages[i] != wantStage {
				t.Fatalf("correlation ID %q stage[%d] = %q, want %q; all=%v", correlationID, i, gotStages[i], wantStage, gotStages)
			}
		}
	}
}

func equalRefreshStages(got, want []RefreshStage) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
