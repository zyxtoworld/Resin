package metrics

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueryHistoryTrafficContextCancellationInterruptsHistoryBucketWriterWait(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}

	releaseFirst := make(chan struct{})
	var releaseFirstOnce sync.Once
	firstQueryHoldingRead := make(chan struct{})
	var queryCalls atomic.Int32
	repo.afterQueryHook = func(context.Context) {
		if queryCalls.Add(1) != 1 {
			return
		}
		close(firstQueryHoldingRead)
		<-releaseFirst
	}

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputIntervalSec:   1,
		ThroughputRetentionSec:  1,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 1,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      1,
	})
	t.Cleanup(func() {
		releaseFirstOnce.Do(func() { close(releaseFirst) })
		_ = mgr.CloseContext(context.Background())
	})

	var prepareCalls atomic.Int32
	secondPrepareAttempted := make(chan struct{})
	mgr.beforeHistoryPrepareHook = func() {
		if prepareCalls.Add(1) == 2 {
			close(secondPrepareAttempted)
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, queryErr := mgr.QueryHistoryTrafficContext(context.Background(), 0, time.Now().Unix())
		firstDone <- queryErr
	}()
	select {
	case <-firstQueryHoldingRead:
	case <-time.After(time.Second):
		t.Fatal("first history query did not hold the history bucket read owner")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, queryErr := mgr.QueryHistoryTrafficContext(requestCtx, 0, time.Now().Unix())
		secondDone <- queryErr
	}()
	select {
	case <-secondPrepareAttempted:
	case <-time.After(time.Second):
		t.Fatal("second history query did not attempt the exclusive preparation owner")
	}
	cancel()

	select {
	case queryErr := <-secondDone:
		if !errors.Is(queryErr, context.Canceled) {
			t.Fatalf("second history query error = %v, want context canceled", queryErr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("canceled history query remained blocked on the history bucket writer owner")
	}

	releaseFirstOnce.Do(func() { close(releaseFirst) })
	select {
	case queryErr := <-firstDone:
		if queryErr != nil {
			t.Fatalf("first history query: %v", queryErr)
		}
	case <-time.After(time.Second):
		t.Fatal("first history query did not finish after releasing its read owner")
	}
}
