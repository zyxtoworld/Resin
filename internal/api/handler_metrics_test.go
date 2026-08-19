package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/proxy"
)

type testPlatformStats struct {
	platforms            map[string]struct{}
	totalNodes           int
	healthyNodes         int
	egressIPCount        int
	healthyEgressIPCount int
}

type interleavedNodePoolStats struct {
	mu            sync.Mutex
	generation    int
	firstCall     chan struct{}
	allowNextCall chan struct{}
	firstCallOnce sync.Once
}

func (s *interleavedNodePoolStats) TotalNodes() int {
	s.firstCallOnce.Do(func() { close(s.firstCall) })
	return 2
}

func (s *interleavedNodePoolStats) HealthyNodes() int {
	<-s.allowNextCall
	return s.currentHealthyNodes()
}

func (s *interleavedNodePoolStats) EgressIPCount() int {
	<-s.allowNextCall
	return s.currentEgressIPCount()
}

func (s *interleavedNodePoolStats) UniqueHealthyEgressIPCount() int {
	<-s.allowNextCall
	return s.currentHealthyEgressIPCount()
}

func (s *interleavedNodePoolStats) NodePoolSnapshot() (int, int, int, int) {
	s.firstCallOnce.Do(func() { close(s.firstCall) })
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation == 2 {
		return 0, 0, 0, 0
	}
	return 2, 1, 1, 1
}

func (s *interleavedNodePoolStats) currentHealthyNodes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation == 2 {
		return 0
	}
	return 1
}

func (s *interleavedNodePoolStats) currentEgressIPCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation == 2 {
		return 0
	}
	return 1
}

func (s *interleavedNodePoolStats) currentHealthyEgressIPCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation == 2 {
		return 0
	}
	return 1
}

func (s *interleavedNodePoolStats) LeaseCountsByPlatform() map[string]int { return nil }
func (s *interleavedNodePoolStats) RoutableNodeCount(string) (int, bool)  { return 0, false }
func (s *interleavedNodePoolStats) PlatformEgressIPCount(string) (int, bool) {
	return 0, false
}
func (s *interleavedNodePoolStats) PlatformNodePoolSnapshot(string) (int, int, bool) {
	return 0, 0, false
}
func (s *interleavedNodePoolStats) CollectNodeEWMAs(string) []float64 { return nil }

func (s testPlatformStats) TotalNodes() int    { return s.totalNodes }
func (s testPlatformStats) HealthyNodes() int  { return s.healthyNodes }
func (s testPlatformStats) EgressIPCount() int { return s.egressIPCount }
func (s testPlatformStats) UniqueHealthyEgressIPCount() int {
	return s.healthyEgressIPCount
}

func (s testPlatformStats) NodePoolSnapshot() (int, int, int, int) {
	return s.totalNodes, s.healthyNodes, s.egressIPCount, s.healthyEgressIPCount
}

func (s testPlatformStats) LeaseCountsByPlatform() map[string]int { return nil }

func (s testPlatformStats) RoutableNodeCount(platformID string) (int, bool) {
	_, ok := s.platforms[platformID]
	return 0, ok
}

func (s testPlatformStats) PlatformEgressIPCount(platformID string) (int, bool) {
	_, ok := s.platforms[platformID]
	return 0, ok
}

func (s testPlatformStats) PlatformNodePoolSnapshot(platformID string) (int, int, bool) {
	routable, ok := s.RoutableNodeCount(platformID)
	egress, _ := s.PlatformEgressIPCount(platformID)
	return routable, egress, ok
}

type testNodeLatencyProvider struct {
	global   []float64
	platform map[string][]float64
}

func (p testNodeLatencyProvider) CollectNodeEWMAs(platformID string) []float64 {
	if platformID == "" {
		return append([]float64(nil), p.global...)
	}
	if p.platform == nil {
		return nil
	}
	return append([]float64(nil), p.platform[platformID]...)
}

func newTestMetricsManager(t *testing.T, existingPlatforms ...string) *metrics.Manager {
	return newTestMetricsManagerWithNodeLatency(t, testNodeLatencyProvider{}, existingPlatforms...)
}

func newTestMetricsManagerWithNodeLatency(
	t *testing.T,
	provider testNodeLatencyProvider,
	existingPlatforms ...string,
) *metrics.Manager {
	t.Helper()

	repo, err := metrics.NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	platforms := make(map[string]struct{}, len(existingPlatforms))
	for _, id := range existingPlatforms {
		platforms[id] = struct{}{}
	}

	mgr, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    repo,
		LatencyBinMs:            100,
		LatencyOverflowMs:       3000,
		BucketSeconds:           3600,
		ThroughputRetentionSec:  16,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 16,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      16,
		LeasesIntervalSec:       7,
		RuntimeStats: testRuntimeStatsProvider{
			testPlatformStats:   testPlatformStats{platforms: platforms},
			testNodeLatencyData: provider,
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

func TestSnapshotNodePoolDoesNotMixRuntimeStatsGenerations(t *testing.T) {
	stats := &interleavedNodePoolStats{
		firstCall:     make(chan struct{}),
		allowNextCall: make(chan struct{}),
	}
	mgr, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    nil,
		BucketSeconds:           300,
		ThroughputRetentionSec:  1,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 1,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      1,
		LeasesIntervalSec:       1,
		RuntimeStats:            stats,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	go func() {
		<-stats.firstCall
		stats.mu.Lock()
		stats.generation = 2
		stats.mu.Unlock()
		close(stats.allowNextCall)
	}()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/snapshots/node-pool", nil)
	HandleSnapshotNodePool(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["total_nodes"] != float64(2) ||
		body["healthy_nodes"] != float64(1) ||
		body["egress_ip_count"] != float64(1) ||
		body["healthy_egress_ip_count"] != float64(1) {
		t.Fatalf("mixed node-pool snapshot: %+v", body)
	}
}

type testRuntimeStatsProvider struct {
	testPlatformStats
	testNodeLatencyData testNodeLatencyProvider
}

func (p testRuntimeStatsProvider) CollectNodeEWMAs(platformID string) []float64 {
	return p.testNodeLatencyData.CollectNodeEWMAs(platformID)
}

func assertNotFoundError(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	var body ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if body.Error.Code != "NOT_FOUND" {
		t.Fatalf("error.code: got %q, want %q", body.Error.Code, "NOT_FOUND")
	}
}

func assertInvalidArgumentError(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var body ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if body.Error.Code != "INVALID_ARGUMENT" {
		t.Fatalf("error.code: got %q, want %q", body.Error.Code, "INVALID_ARGUMENT")
	}
}

func TestMetricsHandlers_NonexistentPlatformReturnsNotFound(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")

	cases := []struct {
		name    string
		handler http.Handler
		path    string
	}{
		{
			name:    "realtime leases",
			handler: HandleRealtimeLeases(mgr),
			path:    "/api/v1/metrics/realtime/leases?platform_id=missing-platform",
		},
		{
			name:    "history requests",
			handler: HandleHistoryRequests(mgr),
			path:    "/api/v1/metrics/history/requests?platform_id=missing-platform",
		},
		{
			name:    "history access latency",
			handler: HandleHistoryAccessLatency(mgr),
			path:    "/api/v1/metrics/history/access-latency?platform_id=missing-platform",
		},
		{
			name:    "history lease lifetime",
			handler: HandleHistoryLeaseLifetime(mgr),
			path:    "/api/v1/metrics/history/lease-lifetime?platform_id=missing-platform",
		},
		{
			name:    "snapshot node latency distribution",
			handler: HandleSnapshotNodeLatencyDistribution(mgr),
			path:    "/api/v1/metrics/snapshots/node-latency-distribution?platform_id=missing-platform",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			tc.handler.ServeHTTP(rec, req)
			assertNotFoundError(t, rec)
		})
	}
}

func TestMetricsHandlers_HistoryTrafficRejectsPlatformDimension(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/history/traffic?platform_id=existing-platform",
		nil,
	)
	rec := httptest.NewRecorder()
	HandleHistoryTraffic(mgr).ServeHTTP(rec, req)

	assertInvalidArgumentError(t, rec)
}

func TestMetricsHandlers_GlobalEndpointsRejectPlatformDimension(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")

	cases := []struct {
		name    string
		handler http.Handler
		path    string
	}{
		{
			name:    "realtime throughput",
			handler: HandleRealtimeThroughput(mgr),
			path:    "/api/v1/metrics/realtime/throughput?platform_id=existing-platform",
		},
		{
			name:    "realtime connections",
			handler: HandleRealtimeConnections(mgr),
			path:    "/api/v1/metrics/realtime/connections?platform_id=existing-platform",
		},
		{
			name:    "history probes",
			handler: HandleHistoryProbes(mgr),
			path:    "/api/v1/metrics/history/probes?platform_id=existing-platform",
		},
		{
			name:    "history traffic",
			handler: HandleHistoryTraffic(mgr),
			path:    "/api/v1/metrics/history/traffic?platform_id=existing-platform",
		},
		{
			name:    "history node-pool",
			handler: HandleHistoryNodePool(mgr),
			path:    "/api/v1/metrics/history/node-pool?platform_id=existing-platform",
		},
		{
			name:    "snapshot node-pool",
			handler: HandleSnapshotNodePool(mgr),
			path:    "/api/v1/metrics/snapshots/node-pool?platform_id=existing-platform",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			tc.handler.ServeHTTP(rec, req)
			assertInvalidArgumentError(t, rec)
		})
	}
}

func TestMetricsHandlers_RealtimeStepSecondsMatchMetricIntervals(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	now := time.Now()
	mgr.ThroughputRing().Push(metrics.RealtimeSample{Timestamp: now, IngressBPS: 1, EgressBPS: 2})
	mgr.ConnectionsRing().Push(metrics.RealtimeSample{Timestamp: now, InboundConns: 3, OutboundConns: 4})
	mgr.LeasesRing().Push(metrics.RealtimeSample{
		Timestamp:        now,
		LeasesByPlatform: map[string]int{"existing-platform": 5},
	})

	cases := []struct {
		name     string
		handler  http.Handler
		path     string
		wantStep float64
	}{
		{
			name:     "throughput",
			handler:  HandleRealtimeThroughput(mgr),
			path:     "/api/v1/metrics/realtime/throughput",
			wantStep: 1,
		},
		{
			name:     "connections",
			handler:  HandleRealtimeConnections(mgr),
			path:     "/api/v1/metrics/realtime/connections",
			wantStep: 5,
		},
		{
			name:     "leases",
			handler:  HandleRealtimeLeases(mgr),
			path:     "/api/v1/metrics/realtime/leases?platform_id=existing-platform",
			wantStep: 7,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			tc.handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
			}

			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("unmarshal body: %v", err)
			}
			if body["step_seconds"] != tc.wantStep {
				t.Fatalf("step_seconds: got %v, want %v", body["step_seconds"], tc.wantStep)
			}
		})
	}
}

func TestMetricsHandlers_RealtimeLeasesWithoutPlatformAggregatesAllPlatforms(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	now := time.Now()
	mgr.LeasesRing().Push(metrics.RealtimeSample{
		Timestamp: now,
		LeasesByPlatform: map[string]int{
			"existing-platform": 5,
			"other-platform":    7,
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/realtime/leases", nil)
	rec := httptest.NewRecorder()
	HandleRealtimeLeases(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["platform_id"] != "" {
		t.Fatalf("platform_id: got %v, want empty string", body["platform_id"])
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["active_leases"] != float64(12) {
		t.Fatalf("active_leases: got %v, want 12", item["active_leases"])
	}
}

func TestMetricsHandlers_SnapshotNodePool_IncludesHealthyEgressIPCount(t *testing.T) {
	repo, err := metrics.NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	mgr, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    repo,
		BucketSeconds:           300,
		ThroughputRetentionSec:  1,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 1,
		ConnectionsIntervalSec:  1,
		LeasesRetentionSec:      1,
		LeasesIntervalSec:       1,
		RuntimeStats: testRuntimeStatsProvider{
			testPlatformStats: testPlatformStats{
				totalNodes:           20,
				healthyNodes:         15,
				egressIPCount:        6,
				healthyEgressIPCount: 4,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/snapshots/node-pool", nil)
	rec := httptest.NewRecorder()
	HandleSnapshotNodePool(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["total_nodes"] != float64(20) || body["healthy_nodes"] != float64(15) {
		t.Fatalf("snapshot node-pool values mismatch: %+v", body)
	}
	if body["egress_ip_count"] != float64(6) {
		t.Fatalf("egress_ip_count: got %v, want 6", body["egress_ip_count"])
	}
	if body["healthy_egress_ip_count"] != float64(4) {
		t.Fatalf("healthy_egress_ip_count: got %v, want 4", body["healthy_egress_ip_count"])
	}
}

func TestMetricsHandlers_HistoryAccessLatency_SeparatesOverflowBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")

	bucketStart := time.Now().Add(-30 * time.Minute).Unix()
	if err := mgr.Repo().WriteLatencyBucket(bucketStart, "", []int64{4, 5, 6}); err != nil {
		t.Fatalf("WriteLatencyBucket: %v", err)
	}

	from := url.QueryEscape(time.Unix(bucketStart-1, 0).UTC().Format(time.RFC3339Nano))
	to := url.QueryEscape(time.Unix(bucketStart+2, 0).UTC().Format(time.RFC3339Nano))
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/history/access-latency?from="+from+"&to="+to,
		nil,
	)
	rec := httptest.NewRecorder()
	HandleHistoryAccessLatency(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item := items[0].(map[string]any)

	if item["sample_count"] != float64(15) {
		t.Fatalf("sample_count: got %v, want 15", item["sample_count"])
	}
	if item["overflow_count"] != float64(6) {
		t.Fatalf("overflow_count: got %v, want 6", item["overflow_count"])
	}

	buckets, ok := item["buckets"].([]any)
	if !ok {
		t.Fatalf("buckets type: got %T", item["buckets"])
	}
	if len(buckets) != 2 {
		t.Fatalf("buckets len: got %d, want 2 (regular buckets only)", len(buckets))
	}
	if buckets[0].(map[string]any)["le_ms"] != float64(99) {
		t.Fatalf("bucket[0].le_ms: got %v, want 99", buckets[0].(map[string]any)["le_ms"])
	}
	if buckets[1].(map[string]any)["le_ms"] != float64(199) {
		t.Fatalf("bucket[1].le_ms: got %v, want 199", buckets[1].(map[string]any)["le_ms"])
	}
}

func TestMetricsHandlers_SnapshotNodeLatencyDistribution_NoDuplicateOverflowBoundary(t *testing.T) {
	mgr := newTestMetricsManagerWithNodeLatency(
		t,
		testNodeLatencyProvider{
			global: []float64{100, 3000, 3001},
		},
		"existing-platform",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/snapshots/node-latency-distribution",
		nil,
	)
	rec := httptest.NewRecorder()
	HandleSnapshotNodeLatencyDistribution(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}

	if body["sample_count"] != float64(3) {
		t.Fatalf("sample_count: got %v, want 3", body["sample_count"])
	}
	if body["overflow_count"] != float64(2) {
		t.Fatalf("overflow_count: got %v, want 2", body["overflow_count"])
	}

	buckets, ok := body["buckets"].([]any)
	if !ok {
		t.Fatalf("buckets type: got %T", body["buckets"])
	}
	le2999Count := 0
	var countAt99, countAt199, countAt2999 float64
	for _, raw := range buckets {
		b := raw.(map[string]any)
		le := b["le_ms"].(float64)
		count := b["count"].(float64)
		if le == 99 {
			countAt99 = count
		}
		if le == 199 {
			countAt199 = count
		}
		if le == 2999 {
			le2999Count++
			countAt2999 = count
		}
	}
	if le2999Count != 1 {
		t.Fatalf("le_ms=2999 bucket count: got %d, want 1", le2999Count)
	}
	if countAt99 != 0 {
		t.Fatalf("count at le_ms=99: got %v, want 0", countAt99)
	}
	if countAt199 != 1 {
		t.Fatalf("count at le_ms=199: got %v, want 1", countAt199)
	}
	if countAt2999 != 0 {
		t.Fatalf("count at le_ms=2999: got %v, want 0", countAt2999)
	}
}

func TestMetricsHandlers_SnapshotNodeLatencyDistribution_ExtremeHistogramBounds(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	binMs := maxInt/2 + 1
	overflowMs := maxInt

	repo, err := metrics.NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	mgr, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    repo,
		LatencyBinMs:            binMs,
		LatencyOverflowMs:       overflowMs,
		BucketSeconds:           3600,
		ThroughputRetentionSec:  16,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 16,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      16,
		LeasesIntervalSec:       7,
		RuntimeStats: testRuntimeStatsProvider{
			testNodeLatencyData: testNodeLatencyProvider{global: []float64{0}},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	rec := httptest.NewRecorder()
	HandleSnapshotNodeLatencyDistribution(mgr).ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/snapshots/node-latency-distribution",
		nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body struct {
		Buckets []map[string]any `json:"buckets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(body.Buckets) != 2 {
		t.Fatalf("regular bucket count: got %d, want 2 for overflow=%d/bin=%d", len(body.Buckets), overflowMs, binMs)
	}

	bounds, _ := buildLatencyHistogram([]int64{0, 0}, 0, binMs, overflowMs)
	if got := bounds[0]["le_ms"]; got != binMs-1 {
		t.Fatalf("first extreme bucket boundary: got %v, want %d", got, binMs-1)
	}
	if got := bounds[1]["le_ms"]; got != overflowMs-1 {
		t.Fatalf("last extreme bucket boundary: got %v, want %d", got, overflowMs-1)
	}
}

func TestMetricsHandlers_HistoryTraffic_IncludesCurrentUnflushedBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	mgr.OnTrafficDelta(100, 200)

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/history/traffic?from="+from+"&to="+to, nil)
	rec := httptest.NewRecorder()
	HandleHistoryTraffic(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["ingress_bytes"] != float64(100) {
		t.Fatalf("ingress_bytes: got %v, want 100", item["ingress_bytes"])
	}
	if item["egress_bytes"] != float64(200) {
		t.Fatalf("egress_bytes: got %v, want 200", item["egress_bytes"])
	}
}

func TestMetricsHandlers_HistoryTraffic_IncludesCurrentUnflushedZeroBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/history/traffic?from="+from+"&to="+to, nil)
	rec := httptest.NewRecorder()
	HandleHistoryTraffic(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["ingress_bytes"] != float64(0) {
		t.Fatalf("ingress_bytes: got %v, want 0", item["ingress_bytes"])
	}
	if item["egress_bytes"] != float64(0) {
		t.Fatalf("egress_bytes: got %v, want 0", item["egress_bytes"])
	}
}

func TestMetricsHandlers_HistoryTraffic_MergesPersistedAndCurrentBucket(t *testing.T) {
	stopped := newTestMetricsManager(t, "existing-platform")

	// Persist one partial bucket first.
	stopped.OnTrafficDelta(50, 70)
	stopped.Stop()

	// A stopped Manager rejects new events. A new runtime sharing the same
	// metrics DB represents the next live process and owns the current bucket.
	mgr, err := metrics.NewManager(metrics.ManagerConfig{
		Repo:                    stopped.Repo(),
		LatencyBinMs:            100,
		LatencyOverflowMs:       3000,
		BucketSeconds:           3600,
		ThroughputRetentionSec:  16,
		ThroughputIntervalSec:   1,
		ConnectionsRetentionSec: 16,
		ConnectionsIntervalSec:  5,
		LeasesRetentionSec:      16,
		LeasesIntervalSec:       7,
		RuntimeStats: testRuntimeStatsProvider{
			testPlatformStats: testPlatformStats{platforms: map[string]struct{}{
				"existing-platform": {},
			}},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Add more traffic into the same current bucket of the new runtime.
	mgr.OnTrafficDelta(100, 200)

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/history/traffic?from="+from+"&to="+to, nil)
	rec := httptest.NewRecorder()
	HandleHistoryTraffic(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["ingress_bytes"] != float64(150) {
		t.Fatalf("ingress_bytes: got %v, want 150", item["ingress_bytes"])
	}
	if item["egress_bytes"] != float64(270) {
		t.Fatalf("egress_bytes: got %v, want 270", item["egress_bytes"])
	}
}

func TestMetricsHandlers_HistoryRequests_IncludesCurrentUnflushedBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "existing-platform",
		NetOK:      true,
		DurationNs: int64(120 * time.Millisecond),
	})
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "existing-platform",
		NetOK:      false,
		DurationNs: int64(240 * time.Millisecond),
	})

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/history/requests?platform_id=existing-platform&from="+from+"&to="+to,
		nil,
	)
	rec := httptest.NewRecorder()
	HandleHistoryRequests(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["total_requests"] != float64(2) {
		t.Fatalf("total_requests: got %v, want 2", item["total_requests"])
	}
	if item["success_requests"] != float64(1) {
		t.Fatalf("success_requests: got %v, want 1", item["success_requests"])
	}
}

func TestMetricsHandlers_HistoryAccessLatency_IncludesCurrentUnflushedBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "existing-platform",
		NetOK:      true,
		DurationNs: int64(120 * time.Millisecond),
	})
	mgr.OnRequestFinished(proxy.RequestFinishedEvent{
		PlatformID: "existing-platform",
		NetOK:      false,
		DurationNs: int64(240 * time.Millisecond),
	})

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/history/access-latency?platform_id=existing-platform&from="+from+"&to="+to,
		nil,
	)
	rec := httptest.NewRecorder()
	HandleHistoryAccessLatency(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["sample_count"] != float64(2) {
		t.Fatalf("sample_count: got %v, want 2", item["sample_count"])
	}
	if item["overflow_count"] != float64(0) {
		t.Fatalf("overflow_count: got %v, want 0", item["overflow_count"])
	}
}

func TestMetricsHandlers_HistoryProbes_IncludesCurrentUnflushedBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	mgr.OnProbeEvent(metrics.ProbeEvent{Kind: metrics.ProbeKindEgress})
	mgr.OnProbeEvent(metrics.ProbeEvent{Kind: metrics.ProbeKindLatency})

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/history/probes?from="+from+"&to="+to, nil)
	rec := httptest.NewRecorder()
	HandleHistoryProbes(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["total_count"] != float64(2) {
		t.Fatalf("total_count: got %v, want 2", item["total_count"])
	}
}

func TestMetricsHandlers_HistoryNodePool_IncludesCurrentUnflushedBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/history/node-pool?from="+from+"&to="+to, nil)
	rec := httptest.NewRecorder()
	HandleHistoryNodePool(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["total_nodes"] != float64(0) {
		t.Fatalf("total_nodes: got %v, want 0", item["total_nodes"])
	}
	if item["healthy_nodes"] != float64(0) {
		t.Fatalf("healthy_nodes: got %v, want 0", item["healthy_nodes"])
	}
	if item["egress_ip_count"] != float64(0) {
		t.Fatalf("egress_ip_count: got %v, want 0", item["egress_ip_count"])
	}
}

func TestMetricsHandlers_HistoryLeaseLifetime_IncludesCurrentUnflushedBucket(t *testing.T) {
	mgr := newTestMetricsManager(t, "existing-platform")
	mgr.OnLeaseEvent(metrics.LeaseMetricEvent{
		PlatformID: "existing-platform",
		Op:         metrics.LeaseOpRemove,
		LifetimeNs: int64(30 * time.Second),
	})

	now := time.Now().UTC()
	from := url.QueryEscape(now.Add(-2 * time.Hour).Format(time.RFC3339Nano))
	to := url.QueryEscape(now.Add(1 * time.Minute).Format(time.RFC3339Nano))
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics/history/lease-lifetime?platform_id=existing-platform&from="+from+"&to="+to,
		nil,
	)
	rec := httptest.NewRecorder()
	HandleHistoryLeaseLifetime(mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items: got %T len=%d, want len=1", body["items"], len(items))
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("item type: got %T", items[0])
	}
	if item["sample_count"] != float64(1) {
		t.Fatalf("sample_count: got %v, want 1", item["sample_count"])
	}
	if item["p50_ms"] != float64(30000) {
		t.Fatalf("p50_ms: got %v, want 30000", item["p50_ms"])
	}
}
