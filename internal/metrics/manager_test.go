package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
)

type managerTestRuntimeStats struct {
	counts map[string]int
}

type generationAwareRuntimeStats struct {
	managerTestRuntimeStats
	snapshotCalls atomic.Int32
}

func (s *generationAwareRuntimeStats) TotalNodes() int                 { return 101 }
func (s *generationAwareRuntimeStats) HealthyNodes() int               { return 102 }
func (s *generationAwareRuntimeStats) EgressIPCount() int              { return 103 }
func (s *generationAwareRuntimeStats) UniqueHealthyEgressIPCount() int { return 104 }
func (s *generationAwareRuntimeStats) NodePoolSnapshot() (int, int, int, int) {
	s.snapshotCalls.Add(1)
	return 9, 7, 3, 2
}

func (managerTestRuntimeStats) TotalNodes() int    { return 9 }
func (managerTestRuntimeStats) HealthyNodes() int  { return 7 }
func (managerTestRuntimeStats) EgressIPCount() int { return 3 }
func (managerTestRuntimeStats) UniqueHealthyEgressIPCount() int {
	return 2
}

func (managerTestRuntimeStats) NodePoolSnapshot() (int, int, int, int) {
	return 9, 7, 3, 2
}

func (p managerTestRuntimeStats) LeaseCountsByPlatform() map[string]int {
	out := make(map[string]int, len(p.counts))
	for k, v := range p.counts {
		out[k] = v
	}
	return out
}

func (managerTestRuntimeStats) RoutableNodeCount(string) (int, bool) { return 0, false }
func (managerTestRuntimeStats) PlatformEgressIPCount(string) (int, bool) {
	return 0, false
}

func (managerTestRuntimeStats) PlatformNodePoolSnapshot(string) (int, int, bool) {
	return 0, 0, false
}
func (managerTestRuntimeStats) CollectNodeEWMAs(string) []float64 { return nil }

func mustNewManager(t *testing.T, cfg ManagerConfig) *Manager {
	t.Helper()
	mgr, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

func TestBuildPersistTaskUsesOneNodePoolSnapshot(t *testing.T) {
	stats := &generationAwareRuntimeStats{}
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
		RuntimeStats:            stats,
	})

	task := mgr.buildPersistTask(&BucketFlushData{BucketStartUnix: 42})
	if task == nil || task.NodePool == nil {
		t.Fatal("buildPersistTask did not include node-pool snapshot")
	}
	if got := *task.NodePool; got != (nodePoolSnapshot{TotalNodes: 9, HealthyNodes: 7, EgressIPCount: 3}) {
		t.Fatalf("node-pool persistence snapshot = %+v, want one-generation values", got)
	}
	if got := stats.snapshotCalls.Load(); got != 1 {
		t.Fatalf("NodePoolSnapshot calls = %d, want 1", got)
	}
}

func TestTakeSample_NormalizesThroughputToBPS(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   5,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	mgr.OnTrafficDelta(100, 250)
	mgr.takeThroughputSample(time.Unix(5, 0))

	sample, ok := mgr.ThroughputRing().Latest()
	if !ok {
		t.Fatal("expected sample in realtime ring")
	}
	if sample.IngressBPS != 20 {
		t.Fatalf("first sample ingress_bps: got %d, want %d", sample.IngressBPS, 20)
	}
	if sample.EgressBPS != 50 {
		t.Fatalf("first sample egress_bps: got %d, want %d", sample.EgressBPS, 50)
	}

	mgr.OnTrafficDelta(50, 150)
	mgr.takeThroughputSample(time.Unix(10, 0))

	sample, ok = mgr.ThroughputRing().Latest()
	if !ok {
		t.Fatal("expected sample in realtime ring")
	}
	if sample.IngressBPS != 10 {
		t.Fatalf("second sample ingress_bps: got %d, want %d", sample.IngressBPS, 10)
	}
	if sample.EgressBPS != 30 {
		t.Fatalf("second sample egress_bps: got %d, want %d", sample.EgressBPS, 30)
	}
}

func TestManagerStopWaitsForActiveHistoryQuery(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		_ = repo.Close()
	}()
	queryEntered := make(chan struct{})
	var enteredOnce sync.Once
	repo.afterQueryHook = func(ctx context.Context) {
		enteredOnce.Do(func() { close(queryEntered) })
		select {
		case <-ctx.Done():
		case <-release:
		}
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

	queryDone := make(chan error, 1)
	go func() {
		_, queryErr := mgr.QueryHistoryTraffic(0, time.Now().Unix())
		queryDone <- queryErr
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("history query did not acquire rows")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	queryFinished := false
	select {
	case <-stopDone:
		select {
		case <-queryDone:
			queryFinished = true
		case <-time.After(time.Second):
			t.Fatal("Manager.Stop returned before the active history query finished")
		}
	case <-time.After(time.Second):
		t.Fatal("Manager.Stop did not cancel the active history query")
	}

	releaseOnce.Do(func() { close(release) })
	if !queryFinished {
		select {
		case <-queryDone:
		case <-time.After(time.Second):
			t.Fatal("history query did not finish after release")
		}
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Manager.Stop did not finish after history query release")
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("MetricsRepo.Close after Manager.Stop: %v", err)
	}
}

func TestQueryHistoryTrafficContextCancellationReleasesRead(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

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

	queryEntered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	defer close(release)
	repo.afterQueryHook = func(ctx context.Context) {
		enteredOnce.Do(func() { close(queryEntered) })
		select {
		case <-ctx.Done():
		case <-release:
		}
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	queryDone := make(chan struct{})
	go func() {
		_, _ = mgr.QueryHistoryTrafficContext(requestCtx, 0, time.Now().Unix())
		close(queryDone)
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("history query did not acquire rows")
	}
	cancel()

	select {
	case <-queryDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("history query ignored caller cancellation")
	}
}

func TestQueryHistoryTrafficDoesNotLoseBucketDuringConcurrentFlush(t *testing.T) {
	const bucketSeconds = 3600
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	now := time.Now()
	currentStart := (now.Unix() / bucketSeconds) * bucketSeconds
	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = currentStart
	mgr.bucket.mu.Unlock()
	mgr.OnTrafficDelta(123, 0)

	queryRowsReady := make(chan struct{})
	allowQueryReturn := make(chan struct{})
	var queryHookOnce sync.Once
	repo.afterQueryHook = func(context.Context) {
		queryHookOnce.Do(func() { close(queryRowsReady) })
		<-allowQueryReturn
	}

	type queryResult struct {
		rows []TrafficBucketRow
		err  error
	}
	resultCh := make(chan queryResult, 1)
	go func() {
		rows, queryErr := mgr.QueryHistoryTraffic(currentStart, currentStart)
		resultCh <- queryResult{rows: rows, err: queryErr}
	}()

	select {
	case <-queryRowsReady:
	case <-time.After(time.Second):
		t.Fatal("history query did not reach the SQLite result boundary")
	}

	flushDone := make(chan struct{})
	go func() {
		mgr.advanceAndMaybeFlush(time.Unix(currentStart+bucketSeconds, 0))
		close(flushDone)
	}()
	select {
	case <-flushDone:
		t.Fatal("bucket flush overtook the in-flight history query")
	case <-time.After(time.Second):
	}
	close(allowQueryReturn)
	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent bucket flush did not complete after query release")
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("QueryHistoryTraffic: %v", result.err)
		}
		if len(result.rows) != 1 {
			t.Fatalf("history rows = %+v, want one current bucket row", result.rows)
		}
		if got := result.rows[0].BucketStartUnix; got != currentStart {
			t.Fatalf("bucket start = %d, want %d", got, currentStart)
		}
		if got := result.rows[0].IngressBytes; got != 123 {
			t.Fatalf("ingress bytes = %d, want 123", got)
		}
	case <-time.After(time.Second):
		t.Fatal("history query did not finish after releasing SQLite result boundary")
	}
}

func TestQueryHistoryTrafficDoesNotLoseBucketDuringConcurrentHistoryPrepare(t *testing.T) {
	const bucketSeconds = 3600
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	now := time.Now()
	currentStart := (now.Unix() / bucketSeconds) * bucketSeconds
	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = currentStart
	mgr.bucket.mu.Unlock()
	mgr.OnTrafficDelta(321, 0)

	q1RowsReady := make(chan struct{})
	allowQ1Return := make(chan struct{})
	var queryCount atomic.Int32
	repo.afterQueryHook = func(context.Context) {
		if queryCount.Add(1) == 1 {
			close(q1RowsReady)
			<-allowQ1Return
		}
	}

	q2PrepareAttempted := make(chan struct{})
	var prepareCount atomic.Int32
	mgr.beforeHistoryPrepareHook = func() {
		if prepareCount.Add(1) == 2 {
			close(q2PrepareAttempted)
		}
	}
	q2Rotated := make(chan struct{})
	var rotationOnce sync.Once
	mgr.afterBucketMaybeFlushHook = func() {
		rotationOnce.Do(func() { close(q2Rotated) })
	}

	type trafficQueryResult struct {
		rows []TrafficBucketRow
		err  error
	}
	q1Done := make(chan trafficQueryResult, 1)
	go func() {
		rows, queryErr := mgr.QueryHistoryTraffic(currentStart, currentStart)
		q1Done <- trafficQueryResult{rows: rows, err: queryErr}
	}()
	select {
	case <-q1RowsReady:
	case <-time.After(time.Second):
		t.Fatal("first history query did not reach the SQLite result boundary")
	}

	q2Done := make(chan error, 1)
	go func() {
		_, queryErr := mgr.queryHistoryTrafficAt(
			currentStart,
			currentStart,
			time.Unix(currentStart+bucketSeconds, 0),
		)
		q2Done <- queryErr
	}()
	select {
	case <-q2PrepareAttempted:
	case <-time.After(time.Second):
		t.Fatal("second history prepare did not reach the exclusive preparation boundary")
	}
	close(allowQ1Return)

	select {
	case result := <-q1Done:
		if result.err != nil {
			t.Fatalf("first QueryHistoryTraffic: %v", result.err)
		}
		if len(result.rows) != 1 || result.rows[0].IngressBytes != 321 {
			t.Fatalf("first history rows = %+v, want the pre-rotation bucket", result.rows)
		}
	case <-time.After(time.Second):
		t.Fatal("first history query did not finish after release")
	}
	select {
	case <-q2Rotated:
	case <-time.After(time.Second):
		t.Fatal("second history prepare did not rotate the due bucket")
	}
	select {
	case queryErr := <-q2Done:
		if queryErr != nil {
			t.Fatalf("second QueryHistoryTraffic: %v", queryErr)
		}
	case <-time.After(time.Second):
		t.Fatal("second history query did not finish")
	}
}

func TestManagerStopContextTimeoutDoesNotCloseRepoWhileHistoryQueryActive(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	var mgr *Manager
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if mgr != nil {
			_ = mgr.CloseContext(context.Background())
			return
		}
		_ = repo.Close()
	})
	queryEntered := make(chan struct{})
	var queryEnteredOnce sync.Once
	repo.afterQueryHook = func(context.Context) {
		queryEnteredOnce.Do(func() { close(queryEntered) })
		<-release
	}

	mgr = mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputIntervalSec:   1,
		ThroughputRetentionSec:  1,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 1,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      1,
	})

	queryDone := make(chan error, 1)
	go func() {
		_, queryErr := mgr.QueryHistoryTraffic(0, time.Now().Unix())
		queryDone <- queryErr
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("history query did not reach the gated rows")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- mgr.StopContext(stopCtx) }()
	<-stopCtx.Done()
	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopContext did not honor its caller deadline")
	}

	if err := repo.db.Ping(); err != nil {
		t.Fatalf("MetricsRepo was closed while the history query was active: %v", err)
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case <-queryDone:
	case <-time.After(time.Second):
		t.Fatal("history query did not finish after release")
	}
	if err := mgr.CloseContext(context.Background()); err != nil {
		t.Fatalf("final Manager.CloseContext error = %v, want completed owner result", err)
	}
	if err := repo.db.Ping(); err == nil {
		t.Fatal("MetricsRepo remained open after the completed Manager.CloseContext")
	}
}

func TestManagerStopOwnerUsesIndependentContextForFinalPersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metrics.db")
	repo, err := NewMetricsRepo(dbPath)
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	var mgr *Manager
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if mgr != nil {
			_ = mgr.CloseContext(context.Background())
			return
		}
		_ = repo.Close()
	})
	queryEntered := make(chan struct{})
	var queryEnteredOnce sync.Once
	repo.afterQueryHook = func(context.Context) {
		queryEnteredOnce.Do(func() { close(queryEntered) })
		<-release
	}

	mgr = mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputRetentionSec:  1,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 1,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      1,
		LeasesIntervalSec:       1,
	})
	mgr.OnTrafficDelta(123, 456)

	queryDone := make(chan error, 1)
	go func() {
		_, queryErr := mgr.QueryHistoryTraffic(0, time.Now().Unix())
		queryDone <- queryErr
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("history query did not reach the gated rows")
	}

	waiterCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- mgr.StopContext(waiterCtx) }()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopContext did not honor the waiter deadline")
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-queryDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("history query: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("history query did not finish after release")
	}

	if err := mgr.CloseContext(context.Background()); err != nil {
		t.Fatalf("final CloseContext error = %v, want completed owner result", err)
	}
	reopened, err := NewMetricsRepo(dbPath)
	if err != nil {
		t.Fatalf("reopen metrics repo: %v", err)
	}
	defer reopened.Close()
	rows, err := reopened.QueryTraffic(0, time.Now().Add(time.Hour).Unix())
	if err != nil {
		t.Fatalf("QueryTraffic after final owner drain: %v", err)
	}
	if len(rows) != 1 || rows[0].IngressBytes != 123 || rows[0].EgressBytes != 456 {
		t.Fatalf("final owner persistence = %+v, want one 123/456 row", rows)
	}
}

func TestManagerRejectsHistoryReadsAfterStop(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

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
	mgr.Stop()

	reads := []struct {
		name string
		read func() error
	}{
		{name: "traffic", read: func() error { _, err := mgr.QueryHistoryTraffic(0, time.Now().Unix()); return err }},
		{name: "requests", read: func() error { _, err := mgr.QueryHistoryRequests(0, time.Now().Unix(), ""); return err }},
		{name: "access latency", read: func() error {
			_, err := mgr.QueryHistoryAccessLatency(0, time.Now().Unix(), "")
			return err
		}},
		{name: "probes", read: func() error { _, err := mgr.QueryHistoryProbes(0, time.Now().Unix()); return err }},
		{name: "node pool", read: func() error { _, err := mgr.QueryHistoryNodePool(0, time.Now().Unix()); return err }},
		{name: "lease lifetime", read: func() error {
			_, err := mgr.QueryHistoryLeaseLifetime(0, time.Now().Unix(), "")
			return err
		}},
	}
	for _, tc := range reads {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.read(); err == nil {
				t.Fatal("history read succeeded after Manager.Stop")
			}
		})
	}
}

func TestNewManager_RejectsDurationOverflow(t *testing.T) {
	const overflowSeconds = 9223372037
	_, err := NewManager(ManagerConfig{
		BucketSeconds:           300,
		ThroughputIntervalSec:   overflowSeconds,
		ThroughputRetentionSec:  1,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 1,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      1,
	})
	if err == nil {
		t.Fatal("expected duration overflow to be rejected")
	}
	if !strings.Contains(err.Error(), "throughput interval seconds") {
		t.Fatalf("error = %q, want throughput interval context", err)
	}
}

func TestNewManager_RejectsLatencyHistogramCapacityOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	_, err := NewManager(ManagerConfig{
		LatencyBinMs:            2,
		LatencyOverflowMs:       maxInt,
		BucketSeconds:           300,
		ThroughputIntervalSec:   1,
		ThroughputRetentionSec:  1,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 1,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      1,
	})
	if err == nil {
		t.Fatal("expected latency histogram capacity overflow to be rejected")
	}
	if !strings.Contains(err.Error(), "latency histogram") {
		t.Fatalf("error = %q, want latency histogram context", err)
	}
}

func TestNewManager_RejectsRealtimeCapacityBeforeAllocation(t *testing.T) {
	base := ManagerConfig{
		BucketSeconds:           300,
		ThroughputIntervalSec:   1,
		ThroughputRetentionSec:  1,
		ConnectionsIntervalSec:  1,
		ConnectionsRetentionSec: 1,
		LeasesIntervalSec:       1,
		LeasesRetentionSec:      1,
	}
	tests := []struct {
		name string
		set  func(*ManagerConfig)
		want string
	}{
		{
			name: "throughput",
			set: func(cfg *ManagerConfig) {
				cfg.ThroughputRetentionSec = MaxRealtimeRingSamples + 1
			},
			want: "throughput realtime capacity",
		},
		{
			name: "connections",
			set: func(cfg *ManagerConfig) {
				cfg.ConnectionsRetentionSec = MaxRealtimeRingSamples + 1
			},
			want: "connections realtime capacity",
		},
		{
			name: "leases",
			set: func(cfg *ManagerConfig) {
				cfg.LeasesRetentionSec = MaxRealtimeRingSamples + 1
			},
			want: "leases realtime capacity",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.set(&cfg)
			_, err := NewManager(cfg)
			if err == nil {
				t.Fatal("expected realtime capacity overflow to be rejected before allocation")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want %s context", err, tc.want)
			}
		})
	}
}

func TestManager_StopRejectsLateEvents(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})

	mgr.OnTrafficDelta(100, 200)
	mgr.Stop()

	mgr.OnTrafficDelta(300, 400)
	_, ingress, egress := mgr.bucket.SnapshotTraffic()
	if ingress != 0 || egress != 0 {
		t.Fatalf("late traffic changed stopped manager: ingress=%d egress=%d", ingress, egress)
	}
}

func TestManager_StopRejectsAllLateEvents(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})
	mgr.Stop()

	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "late",
		NetOK:      true,
		DurationNs: int64(time.Millisecond),
	})
	mgr.OnTrafficDelta(1, 2)
	mgr.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionOpen)
	mgr.OnProbeEvent(ProbeEvent{Kind: ProbeKindEgress})
	mgr.OnLeaseEvent(LeaseMetricEvent{
		PlatformID: "late",
		Op:         LeaseOpRemove,
		LifetimeNs: int64(time.Second),
	})

	snapshot := mgr.Collector().Snapshot()
	if snapshot.Requests != 0 || snapshot.SuccessRequests != 0 ||
		snapshot.IngressBytes != 0 || snapshot.EgressBytes != 0 ||
		snapshot.InboundConns != 0 || snapshot.OutboundConns != 0 ||
		snapshot.ProbeEgress != 0 || snapshot.ProbeLatency != 0 {
		t.Fatalf("late event changed stopped collector: %+v", snapshot)
	}
	if _, ingress, egress := mgr.bucket.SnapshotTraffic(); ingress != 0 || egress != 0 {
		t.Fatalf("late traffic changed stopped bucket: ingress=%d egress=%d", ingress, egress)
	}
	if got := len(mgr.leaseSamplesCh); got != 0 {
		t.Fatalf("late lease event queued after Stop: %d", got)
	}
}

func TestManager_StopWaitsForConcurrentStartAdmission(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})

	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	mgr.beforeWorkerStartHook = func() {
		close(startEntered)
		<-releaseStart
	}
	defer func() {
		select {
		case <-releaseStart:
		default:
			close(releaseStart)
		}
	}()

	startDone := make(chan struct{})
	go func() {
		mgr.Start()
		close(startDone)
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach worker admission")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned before concurrent Start was admitted")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseStart)
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("Start did not finish after admission release")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after concurrent Start exited")
	}
}

func TestManager_StopBeforeStartRejectsLaterStart(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})
	var startCalls atomic.Int32
	mgr.beforeWorkerStartHook = func() { startCalls.Add(1) }

	mgr.Stop()
	mgr.Start()
	mgr.Stop()

	if got := startCalls.Load(); got != 0 {
		t.Fatalf("Start after Stop entered worker admission %d times", got)
	}
}

func TestManager_StopWaitsForInFlightHistoryFlushBeforeFinalDrain(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	const bucketSeconds = 60
	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})

	base := time.Unix(1_700_000_000, 0).UTC()
	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = base.Unix()
	mgr.bucket.mu.Unlock()
	mgr.OnTrafficDelta(100, 200)

	taskReady := make(chan struct{})
	releaseTask := make(chan struct{})
	mgr.beforePersistTaskEnqueueHook = func() {
		close(taskReady)
		<-releaseTask
	}

	queryDone := make(chan struct{})
	go func() {
		if err := mgr.prepareHistoryRead(context.Background(), base.Add(bucketSeconds*time.Second)); err != nil {
			t.Errorf("prepareHistoryRead: %v", err)
		}
		close(queryDone)
	}()
	select {
	case <-taskReady:
	case <-time.After(time.Second):
		t.Fatal("query did not reach persistence-task handoff")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned while an in-flight query still owned a flush task")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseTask)
	select {
	case <-queryDone:
	case <-time.After(time.Second):
		t.Fatal("query did not finish after persistence handoff release")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not drain the query flush task")
	}

	mgr.pendingMu.Lock()
	pending := len(mgr.pendingTasks)
	mgr.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("Stop left %d query flush task(s) pending", pending)
	}

	rows, err := repo.QueryTraffic(base.Unix(), base.Add(2*bucketSeconds*time.Second).Unix())
	if err != nil {
		t.Fatalf("QueryTraffic: %v", err)
	}
	if len(rows) != 1 || rows[0].IngressBytes != 100 || rows[0].EgressBytes != 200 {
		t.Fatalf("Stop lost query flush: %+v", rows)
	}
}

func TestManager_StopWaitsForAdmittedEventBeforeFinalFlush(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})
	bucketStart := mgr.bucket.CurrentBucketStartUnix()
	entered := make(chan struct{})
	release := make(chan struct{})
	mgr.collector.beforeRecordRequestHook = func() {
		close(entered)
		<-release
	}
	stopAdmissionClosed := make(chan struct{})
	mgr.beforeEventDrainHook = func() { close(stopAdmissionClosed) }

	eventDone := make(chan struct{})
	go func() {
		mgr.OnRequestFinished(proxy.RequestFinishedEvent{
			PlatformID: "p1",
			NetOK:      true,
			DurationNs: int64(25 * time.Millisecond),
		})
		close(eventDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("event did not enter collector")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-stopAdmissionClosed:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close event admission")
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned while an admitted event was still running")
	default:
	}

	close(release)
	select {
	case <-eventDone:
	case <-time.After(time.Second):
		t.Fatal("admitted event did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after the admitted event")
	}

	rows, err := repo.QueryRequests(bucketStart, bucketStart, "")
	if err != nil {
		t.Fatalf("QueryRequests: %v", err)
	}
	if len(rows) != 1 || rows[0].TotalRequests != 1 || rows[0].SuccessRequests != 1 {
		t.Fatalf("final flush lost admitted request: %+v", rows)
	}
}

func TestManager_ConcurrentStopWaitsForSameFinalFlush(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})
	bucketStart := mgr.bucket.CurrentBucketStartUnix()
	entered := make(chan struct{})
	release := make(chan struct{})
	mgr.collector.beforeRecordRequestHook = func() {
		close(entered)
		<-release
	}
	stopAdmissionClosed := make(chan struct{})
	mgr.beforeEventDrainHook = func() { close(stopAdmissionClosed) }

	eventDone := make(chan struct{})
	go func() {
		mgr.OnRequestFinished(proxy.RequestFinishedEvent{
			PlatformID: "p1",
			NetOK:      true,
			DurationNs: int64(25 * time.Millisecond),
		})
		close(eventDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("event did not enter collector")
	}

	stop1Done := make(chan struct{})
	stop2Done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stop1Done)
	}()
	select {
	case <-stopAdmissionClosed:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close event admission")
	}
	go func() {
		mgr.Stop()
		close(stop2Done)
	}()
	select {
	case <-stop1Done:
		t.Fatal("first Stop returned before the admitted event")
	case <-stop2Done:
		t.Fatal("second Stop returned before the first final flush")
	default:
	}

	close(release)
	select {
	case <-eventDone:
	case <-time.After(time.Second):
		t.Fatal("admitted event did not finish")
	}
	select {
	case <-stop1Done:
	case <-time.After(time.Second):
		t.Fatal("first Stop did not finish")
	}
	select {
	case <-stop2Done:
	case <-time.After(time.Second):
		t.Fatal("second Stop did not finish")
	}

	rows, err := repo.QueryRequests(bucketStart, bucketStart, "")
	if err != nil {
		t.Fatalf("QueryRequests: %v", err)
	}
	if len(rows) != 1 || rows[0].TotalRequests != 1 || rows[0].SuccessRequests != 1 {
		t.Fatalf("concurrent Stop lost final flush: %+v", rows)
	}
}

func TestTakeSample_ConnectionsAndLeasesUseDedicatedRings(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       7,
		RuntimeStats: managerTestRuntimeStats{
			counts: map[string]int{"p1": 3},
		},
	})

	mgr.OnConnectionLifecycle(proxy.ConnectionInbound, proxy.ConnectionOpen)
	mgr.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionOpen)
	mgr.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionOpen)

	mgr.takeConnectionsSample(time.Unix(10, 0))
	connSample, ok := mgr.ConnectionsRing().Latest()
	if !ok {
		t.Fatal("expected sample in connections ring")
	}
	if connSample.InboundConns != 1 || connSample.OutboundConns != 2 {
		t.Fatalf("connections sample mismatch: %+v", connSample)
	}

	mgr.takeLeasesSample(time.Unix(14, 0))
	leaseSample, ok := mgr.LeasesRing().Latest()
	if !ok {
		t.Fatal("expected sample in leases ring")
	}
	if leaseSample.LeasesByPlatform["p1"] != 3 {
		t.Fatalf("leases sample p1: got %d, want 3", leaseSample.LeasesByPlatform["p1"])
	}
}

func TestTakeConnectionsSample_UsesWindowMaxActiveConnections(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	mgr.OnConnectionLifecycle(proxy.ConnectionInbound, proxy.ConnectionOpen)
	mgr.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionOpen)
	mgr.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionOpen)
	mgr.OnConnectionLifecycle(proxy.ConnectionOutbound, proxy.ConnectionClose)
	mgr.takeConnectionsSample(time.Unix(5, 0))

	first, ok := mgr.ConnectionsRing().Latest()
	if !ok {
		t.Fatal("expected first sample in connections ring")
	}
	if first.InboundConns != 1 || first.OutboundConns != 2 {
		t.Fatalf("first sample mismatch: %+v", first)
	}

	// No lifecycle events in this window: values should reflect steady active counts.
	mgr.takeConnectionsSample(time.Unix(10, 0))
	second, ok := mgr.ConnectionsRing().Latest()
	if !ok {
		t.Fatal("expected second sample in connections ring")
	}
	if second.InboundConns != 1 || second.OutboundConns != 1 {
		t.Fatalf("second sample mismatch: %+v", second)
	}
}

func TestOnLeaseEvent_IgnoresNonPositiveLifetimeSamples(t *testing.T) {
	mgr := mustNewManager(t, ManagerConfig{
		BucketSeconds:           300,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	mgr.OnLeaseEvent(LeaseMetricEvent{PlatformID: "p1", Op: LeaseOpRemove, LifetimeNs: 0})
	mgr.OnLeaseEvent(LeaseMetricEvent{PlatformID: "p1", Op: LeaseOpExpire, LifetimeNs: -1})
	mgr.OnLeaseEvent(LeaseMetricEvent{PlatformID: "p1", Op: LeaseOpRemove, LifetimeNs: 1})
	mgr.syncCurrentBucketState()

	data := mgr.bucket.ForceFlush()
	if data == nil {
		t.Fatal("expected flushed bucket data")
	}
	acc, ok := data.LeaseLifetimes["p1"]
	if !ok {
		t.Fatal("expected p1 lease lifetime bucket")
	}
	if len(acc.Samples) != 1 || acc.Samples[0] != 1 {
		t.Fatalf("lease samples: got %+v, want [1]", acc.Samples)
	}
}

func TestFlushBucket_RetainsPendingTaskUntilRepoRecovers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metrics.db")
	repo, err := NewMetricsRepo(dbPath)
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	const bucketSeconds = 60
	flushNow := time.Unix(1_700_000_000, 0).UTC()
	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		LatencyBinMs:            100,
		LatencyOverflowMs:       300,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
		RuntimeStats:            managerTestRuntimeStats{},
	})

	mgr.OnTrafficDelta(100, 200)
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "plat-1",
		NetOK:      true,
		DurationNs: int64(120 * time.Millisecond),
	})
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "plat-1",
		NetOK:      false,
		DurationNs: int64(380 * time.Millisecond),
	})

	// Force the previous fixed-width bucket to be due for the fixed flush time.
	nowUnix := flushNow.Unix()
	bucketWidth := int64(bucketSeconds)
	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = (nowUnix/bucketWidth)*bucketWidth - bucketWidth
	mgr.bucket.mu.Unlock()

	if err := repo.Close(); err != nil {
		t.Fatalf("repo.Close: %v", err)
	}

	// First flush fails; task must remain pending (not discarded).
	mgr.flushBucketAt(flushNow)
	mgr.pendingMu.Lock()
	pendingAfterFailure := len(mgr.pendingTasks)
	mgr.pendingMu.Unlock()
	if pendingAfterFailure != 1 {
		t.Fatalf("pending task count after failure: got %d, want %d", pendingAfterFailure, 1)
	}

	// Reopen DB and retry; pending task should be drained.
	recoveredRepo, err := NewMetricsRepo(dbPath)
	if err != nil {
		t.Fatalf("recover NewMetricsRepo: %v", err)
	}
	defer recoveredRepo.Close()
	mgr.repo = recoveredRepo

	mgr.flushBucketAt(flushNow)
	mgr.pendingMu.Lock()
	pendingAfterRecover := len(mgr.pendingTasks)
	mgr.pendingMu.Unlock()
	if pendingAfterRecover != 0 {
		t.Fatalf("pending task count after recovery: got %d, want %d", pendingAfterRecover, 0)
	}

	from, to := int64(0), flushNow.Add(time.Minute).Unix()

	requestRows, err := recoveredRepo.QueryRequests(from, to, "plat-1")
	if err != nil {
		t.Fatalf("QueryRequests: %v", err)
	}
	if len(requestRows) != 1 {
		t.Fatalf("request rows len: got %d, want 1", len(requestRows))
	}
	if requestRows[0].TotalRequests != 2 || requestRows[0].SuccessRequests != 1 {
		t.Fatalf("request row mismatch: %+v", requestRows[0])
	}

	trafficRows, err := recoveredRepo.QueryTraffic(from, to)
	if err != nil {
		t.Fatalf("QueryTraffic: %v", err)
	}
	if len(trafficRows) != 1 {
		t.Fatalf("traffic rows len: got %d, want 1", len(trafficRows))
	}
	if trafficRows[0].IngressBytes != 100 || trafficRows[0].EgressBytes != 200 {
		t.Fatalf("traffic row mismatch: %+v", trafficRows[0])
	}

	nodePoolRows, err := recoveredRepo.QueryNodePool(from, to)
	if err != nil {
		t.Fatalf("QueryNodePool: %v", err)
	}
	if len(nodePoolRows) != 1 {
		t.Fatalf("node pool rows len: got %d, want 1", len(nodePoolRows))
	}
	if nodePoolRows[0].TotalNodes != 9 || nodePoolRows[0].HealthyNodes != 7 || nodePoolRows[0].EgressIPCount != 3 {
		t.Fatalf("node pool row mismatch: %+v", nodePoolRows[0])
	}

	globalLatencyRows, err := recoveredRepo.QueryAccessLatency(from, to, "")
	if err != nil {
		t.Fatalf("QueryAccessLatency(global): %v", err)
	}
	if len(globalLatencyRows) != 1 {
		t.Fatalf("global latency rows len: got %d, want 1", len(globalLatencyRows))
	}
	var globalBuckets []int64
	if err := json.Unmarshal([]byte(globalLatencyRows[0].BucketsJSON), &globalBuckets); err != nil {
		t.Fatalf("unmarshal global buckets: %v", err)
	}
	var globalTotal int64
	for _, c := range globalBuckets {
		globalTotal += c
	}
	if globalTotal != 2 {
		t.Fatalf("global latency sample count: got %d, want 2", globalTotal)
	}

	platformLatencyRows, err := recoveredRepo.QueryAccessLatency(from, to, "plat-1")
	if err != nil {
		t.Fatalf("QueryAccessLatency(platform): %v", err)
	}
	if len(platformLatencyRows) != 1 {
		t.Fatalf("platform latency rows len: got %d, want 1", len(platformLatencyRows))
	}
	var platformBuckets []int64
	if err := json.Unmarshal([]byte(platformLatencyRows[0].BucketsJSON), &platformBuckets); err != nil {
		t.Fatalf("unmarshal platform buckets: %v", err)
	}
	var platformTotal int64
	for _, c := range platformBuckets {
		platformTotal += c
	}
	if platformTotal != 2 {
		t.Fatalf("platform latency sample count: got %d, want 2", platformTotal)
	}
}

func TestManager_StopContextHonorsDeadlineDuringBlockedPersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metrics.db")
	repo, err := NewMetricsRepo(dbPath)
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	blocker, err := state.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("state.OpenDB blocker: %v", err)
	}
	defer blocker.Close()
	tx, err := blocker.Begin()
	if err != nil {
		t.Fatalf("blocker Begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		"INSERT INTO metric_traffic_bucket (bucket_start_unix, ingress_bytes, egress_bytes) VALUES (?, ?, ?)",
		int64(1), int64(1), int64(1),
	); err != nil {
		t.Fatalf("blocker write: %v", err)
	}

	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           60,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       1,
	})
	mgr.OnTrafficDelta(1, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- mgr.StopContext(ctx) }()

	select {
	case err := <-stopDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext error = %v, want context deadline exceeded", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StopContext ignored the shutdown deadline during blocked metrics persistence")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("release blocker: %v", err)
	}
}

func TestAdvanceAndMaybeFlush_PairsLatencyWithItsBucket(t *testing.T) {
	const bucketSeconds = 60
	base := time.Unix(1_700_000_040, 0).UTC()
	mgr := mustNewManager(t, ManagerConfig{
		LatencyBinMs:            10,
		LatencyOverflowMs:       100,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = base.Unix()
	mgr.bucket.mu.Unlock()

	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "plat-1",
		NetOK:      true,
		DurationNs: int64(20 * time.Millisecond),
	})

	maybeFlushed := make(chan struct{})
	allowDrain := make(chan struct{})
	mgr.afterBucketMaybeFlushHook = func() {
		close(maybeFlushed)
		<-allowDrain
	}

	firstDone := make(chan struct{})
	go func() {
		mgr.advanceAndMaybeFlush(base.Add(bucketSeconds * time.Second))
		close(firstDone)
	}()
	<-maybeFlushed

	requestAttempted := make(chan struct{})
	requestAdmitted := make(chan struct{})
	requestDone := make(chan struct{})
	mgr.collector.beforeRecordRequestHook = func() {
		close(requestAttempted)
	}
	mgr.collector.afterRecordRequestLockHook = func() {
		close(requestAdmitted)
	}
	go func() {
		mgr.OnRequestFinished(proxy.RequestFinishedEvent{
			PlatformID: "plat-1",
			NetOK:      true,
			DurationNs: int64(30 * time.Millisecond),
		})
		close(requestDone)
	}()
	<-requestAttempted
	select {
	case <-requestAdmitted:
		t.Fatal("request entered the flush window before latency drain")
	default:
	}
	close(allowDrain)
	select {
	case <-requestAdmitted:
	case <-time.After(time.Second):
		t.Fatal("request did not enter the window after flush released it")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request did not finish after flush released it")
	}
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first flush did not finish")
	}
	mgr.afterBucketMaybeFlushHook = nil
	mgr.advanceAndMaybeFlush(base.Add(2 * bucketSeconds * time.Second))

	mgr.pendingMu.Lock()
	tasks := append([]*persistTask(nil), mgr.pendingTasks...)
	mgr.pendingMu.Unlock()
	if len(tasks) != 2 {
		t.Fatalf("pending task count = %d, want 2", len(tasks))
	}

	latencyCounts := make(map[int64]int64, len(tasks))
	for _, task := range tasks {
		var count int64
		for _, bucketCount := range task.GlobalLatency {
			count += bucketCount
		}
		latencyCounts[task.Bucket.BucketStartUnix] = count
	}
	if got := latencyCounts[base.Unix()]; got != 1 {
		t.Fatalf("first bucket latency samples = %d, want 1", got)
	}
	if got := latencyCounts[base.Add(bucketSeconds*time.Second).Unix()]; got != 1 {
		t.Fatalf("second bucket latency samples = %d, want 1", got)
	}
	for _, task := range tasks {
		requests := task.Bucket.Requests
		if requests[""].Total != 1 || requests[""].Success != 1 {
			t.Fatalf("global request counts for bucket %d = %+v, want total/success 1/1", task.Bucket.BucketStartUnix, requests[""])
		}
		if requests["plat-1"].Total != 1 || requests["plat-1"].Success != 1 {
			t.Fatalf("platform request counts for bucket %d = %+v, want total/success 1/1", task.Bucket.BucketStartUnix, requests["plat-1"])
		}
		var platformLatency int64
		for _, bucketCount := range task.PlatformLatency["plat-1"] {
			platformLatency += bucketCount
		}
		if platformLatency != 1 {
			t.Fatalf("platform latency samples for bucket %d = %d, want 1", task.Bucket.BucketStartUnix, platformLatency)
		}
	}
}

func TestFlushBucket_FailedTaskDoesNotPublishPartialGeneration(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	defer repo.Close()

	const bucketSeconds = 60
	flushAt := time.Unix(1_700_000_000, 0).UTC()
	mgr := mustNewManager(t, ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
		RuntimeStats:            managerTestRuntimeStats{},
	})

	mgr.OnTrafficDelta(100, 200)
	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = flushAt.Unix() - bucketSeconds
	mgr.bucket.mu.Unlock()

	// Make the second part of one persist task fail after the bucket transaction
	// would otherwise have committed. A failed task must not expose a partial
	// bucket generation to readers; the whole task remains the retry ticket.
	if _, err := repo.db.Exec("DROP TABLE metric_node_pool_bucket"); err != nil {
		t.Fatalf("drop node-pool table: %v", err)
	}
	mgr.flushBucketAt(flushAt)

	var trafficRows int
	if err := repo.db.QueryRow("SELECT COUNT(*) FROM metric_traffic_bucket").Scan(&trafficRows); err != nil {
		t.Fatalf("count traffic rows after failed task: %v", err)
	}
	if trafficRows != 0 {
		t.Fatalf("failed metrics task published partial generation: traffic rows=%d, want 0", trafficRows)
	}
	mgr.pendingMu.Lock()
	pending := len(mgr.pendingTasks)
	mgr.pendingMu.Unlock()
	if pending != 1 {
		t.Fatalf("failed task ticket count=%d, want 1", pending)
	}

	if _, err := repo.db.Exec(`CREATE TABLE metric_node_pool_bucket (
		bucket_start_unix INTEGER PRIMARY KEY,
		total_nodes INTEGER NOT NULL DEFAULT 0,
		healthy_nodes INTEGER NOT NULL DEFAULT 0,
		egress_ip_count INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		t.Fatalf("restore node-pool table: %v", err)
	}
	mgr.flushBucketAt(flushAt)
	mgr.pendingMu.Lock()
	pending = len(mgr.pendingTasks)
	mgr.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("recovered task ticket count=%d, want 0", pending)
	}
	if err := repo.db.QueryRow("SELECT COUNT(*) FROM metric_traffic_bucket").Scan(&trafficRows); err != nil {
		t.Fatalf("count traffic rows after retry: %v", err)
	}
	if trafficRows != 1 {
		t.Fatalf("traffic rows after retry=%d, want 1", trafficRows)
	}
	var nodePoolRows int
	if err := repo.db.QueryRow("SELECT COUNT(*) FROM metric_node_pool_bucket").Scan(&nodePoolRows); err != nil {
		t.Fatalf("count node-pool rows after retry: %v", err)
	}
	if nodePoolRows != 1 {
		t.Fatalf("node-pool rows after retry=%d, want 1", nodePoolRows)
	}
}

func TestSnapshotCurrentAccessLatencyBucketKeepsStartAndHistogramTogether(t *testing.T) {
	const bucketSeconds = 3600
	now := time.Now()
	currentStart := (now.Unix() / bucketSeconds) * bucketSeconds
	mgr := mustNewManager(t, ManagerConfig{
		LatencyBinMs:            10,
		LatencyOverflowMs:       100,
		BucketSeconds:           bucketSeconds,
		ThroughputRetentionSec:  8,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 8,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      8,
		LeasesIntervalSec:       5,
	})

	mgr.bucket.mu.Lock()
	mgr.bucket.currentStart = currentStart
	mgr.bucket.mu.Unlock()
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		NetOK:      true,
		DurationNs: int64(20 * time.Millisecond),
	})

	advanceDone := make(chan struct{})
	allowSnapshotRead := make(chan struct{})
	var snapshotLockSignals atomic.Int32
	mgr.afterAccessLatencySnapshotLockHook = func() {
		if snapshotLockSignals.Add(1) == 1 {
			close(allowSnapshotRead)
		}
	}
	mgr.afterAccessLatencyBucketStartHook = func() {
		go func() {
			mgr.advanceAndMaybeFlush(time.Unix(currentStart+bucketSeconds, 0))
			close(advanceDone)
		}()
		select {
		case <-advanceDone:
		case <-allowSnapshotRead:
		}
	}

	type snapshotResult struct {
		start   int64
		buckets []int64
	}
	resultCh := make(chan snapshotResult, 1)
	go func() {
		start, buckets := mgr.SnapshotCurrentAccessLatencyBucket("")
		resultCh <- snapshotResult{start: start, buckets: buckets}
	}()

	var result snapshotResult
	select {
	case result = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("access-latency snapshot did not complete")
	}
	select {
	case <-advanceDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent bucket advance did not complete")
	}

	if result.start != currentStart {
		t.Fatalf("bucket start = %d, want %d", result.start, currentStart)
	}
	var samples int64
	for _, count := range result.buckets {
		samples += count
	}
	if samples != 1 {
		t.Fatalf("latency samples = %d, want 1 in the reported bucket", samples)
	}
}
