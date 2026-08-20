package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

func (m *Manager) prepareHistoryRead(ctx context.Context, now time.Time) error {
	if hook := m.beforeHistoryPrepareHook; hook != nil {
		hook()
	}
	if err := m.historyBucketMu.LockContext(ctx); err != nil {
		return err
	}
	defer m.historyBucketMu.Unlock()
	return m.prepareHistoryReadNoBucketLock(ctx, now)
}

// prepareHistoryReadNoBucketLock prepares a history query while the caller
// owns historyBucketMu's write side. The caller must not invoke it while
// holding the read side: preparation may rotate buckets and persist tasks.
func (m *Manager) prepareHistoryReadNoBucketLock(ctx context.Context, now time.Time) error {
	if m.repo == nil {
		return fmt.Errorf("metrics repo is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Ensure current bucket state is advanced even if bucketLoop is delayed and
	// keep the persistence retry inside the same shutdown admission.
	admitted := m.withFlushAdmission(func() {
		if ctx.Err() != nil {
			return
		}
		m.advanceAndMaybeFlushNoAdmission(now)
		m.flushPendingTasksContext(ctx, "[metrics] history-triggered persistence failed, will retry next tick")
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if !admitted {
		return fmt.Errorf("metrics history flush admission is closed")
	}
	return nil
}

func (m *Manager) QueryHistoryTraffic(fromUnix, toUnix int64) ([]TrafficBucketRow, error) {
	return m.QueryHistoryTrafficContext(context.Background(), fromUnix, toUnix)
}

func (m *Manager) queryHistoryTrafficAt(fromUnix, toUnix int64, now time.Time) ([]TrafficBucketRow, error) {
	return m.queryHistoryTrafficAtContext(context.Background(), fromUnix, toUnix, now)
}

// QueryHistoryTrafficContext reads traffic history while honoring both the
// caller context and the metrics manager lifecycle context.
func (m *Manager) QueryHistoryTrafficContext(ctx context.Context, fromUnix, toUnix int64) ([]TrafficBucketRow, error) {
	return m.queryHistoryTrafficAtContext(ctx, fromUnix, toUnix, time.Now())
}

func (m *Manager) queryHistoryTrafficAtContext(ctx context.Context, fromUnix, toUnix int64, now time.Time) ([]TrafficBucketRow, error) {
	ctx, release, err := m.beginHistoryReadContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := m.prepareHistoryRead(ctx, now); err != nil {
		return nil, err
	}
	if err := m.historyBucketMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer m.historyBucketMu.RUnlock()
	rows, err := m.repo.queryTraffic(ctx, fromUnix, toUnix)
	if err != nil {
		return nil, err
	}

	currentBucketStart, currentIngress, currentEgress := m.bucket.SnapshotTraffic()
	if bucketInRangeUnix(currentBucketStart, fromUnix, toUnix) {
		merged := false
		for i := range rows {
			if rows[i].BucketStartUnix != currentBucketStart {
				continue
			}
			rows[i].IngressBytes += currentIngress
			rows[i].EgressBytes += currentEgress
			merged = true
			break
		}
		if !merged {
			rows = append(rows, TrafficBucketRow{
				BucketStartUnix: currentBucketStart,
				IngressBytes:    currentIngress,
				EgressBytes:     currentEgress,
			})
			sort.Slice(rows, func(i, j int) bool { return rows[i].BucketStartUnix < rows[j].BucketStartUnix })
		}
	}
	return rows, nil
}

func (m *Manager) QueryHistoryRequests(fromUnix, toUnix int64, platformID string) ([]RequestBucketRow, error) {
	return m.QueryHistoryRequestsContext(context.Background(), fromUnix, toUnix, platformID)
}

// QueryHistoryRequestsContext reads request history while honoring both the
// caller context and the metrics manager lifecycle context.
func (m *Manager) QueryHistoryRequestsContext(ctx context.Context, fromUnix, toUnix int64, platformID string) ([]RequestBucketRow, error) {
	ctx, release, err := m.beginHistoryReadContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := m.prepareHistoryRead(ctx, time.Now()); err != nil {
		return nil, err
	}
	if err := m.historyBucketMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer m.historyBucketMu.RUnlock()
	rows, err := m.repo.queryRequests(ctx, fromUnix, toUnix, platformID)
	if err != nil {
		return nil, err
	}

	currentBucketStart, currentTotal, currentSuccess := m.bucket.SnapshotRequests(platformID)
	if bucketInRangeUnix(currentBucketStart, fromUnix, toUnix) {
		merged := false
		for i := range rows {
			if rows[i].BucketStartUnix != currentBucketStart {
				continue
			}
			rows[i].TotalRequests += currentTotal
			rows[i].SuccessRequests += currentSuccess
			if rows[i].SuccessRequests > rows[i].TotalRequests {
				rows[i].SuccessRequests = rows[i].TotalRequests
			}
			merged = true
			break
		}
		if !merged {
			rows = append(rows, RequestBucketRow{
				BucketStartUnix: currentBucketStart,
				PlatformID:      platformID,
				TotalRequests:   currentTotal,
				SuccessRequests: currentSuccess,
			})
			sort.Slice(rows, func(i, j int) bool { return rows[i].BucketStartUnix < rows[j].BucketStartUnix })
		}
	}
	return rows, nil
}

func (m *Manager) QueryHistoryAccessLatency(fromUnix, toUnix int64, platformID string) ([]AccessLatencyBucketRow, error) {
	return m.QueryHistoryAccessLatencyContext(context.Background(), fromUnix, toUnix, platformID)
}

// QueryHistoryAccessLatencyContext reads access-latency history while
// honoring both the caller context and the metrics manager lifecycle context.
func (m *Manager) QueryHistoryAccessLatencyContext(ctx context.Context, fromUnix, toUnix int64, platformID string) ([]AccessLatencyBucketRow, error) {
	ctx, release, err := m.beginHistoryReadContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := m.prepareHistoryRead(ctx, time.Now()); err != nil {
		return nil, err
	}
	if err := m.historyBucketMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer m.historyBucketMu.RUnlock()
	rows, err := m.repo.queryAccessLatency(ctx, fromUnix, toUnix, platformID)
	if err != nil {
		return nil, err
	}

	m.stateMu.Lock()
	currentBucketStart, currentBuckets := m.snapshotCurrentAccessLatencyBucketLocked(platformID)
	m.stateMu.Unlock()
	if bucketInRangeUnix(currentBucketStart, fromUnix, toUnix) {
		merged := false
		for i := range rows {
			if rows[i].BucketStartUnix != currentBucketStart {
				continue
			}
			persisted := decodeLatencyBucketsJSON(rows[i].BucketsJSON)
			rows[i].BucketsJSON = encodeLatencyBucketsJSON(mergeLatencyBuckets(persisted, currentBuckets))
			merged = true
			break
		}
		if !merged {
			rows = append(rows, AccessLatencyBucketRow{
				BucketStartUnix: currentBucketStart,
				PlatformID:      platformID,
				BucketsJSON:     encodeLatencyBucketsJSON(currentBuckets),
			})
			sort.Slice(rows, func(i, j int) bool { return rows[i].BucketStartUnix < rows[j].BucketStartUnix })
		}
	}
	return rows, nil
}

func (m *Manager) QueryHistoryProbes(fromUnix, toUnix int64) ([]ProbeBucketRow, error) {
	return m.QueryHistoryProbesContext(context.Background(), fromUnix, toUnix)
}

// QueryHistoryProbesContext reads probe history while honoring both the
// caller context and the metrics manager lifecycle context.
func (m *Manager) QueryHistoryProbesContext(ctx context.Context, fromUnix, toUnix int64) ([]ProbeBucketRow, error) {
	ctx, release, err := m.beginHistoryReadContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := m.prepareHistoryRead(ctx, time.Now()); err != nil {
		return nil, err
	}
	if err := m.historyBucketMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer m.historyBucketMu.RUnlock()
	rows, err := m.repo.queryProbes(ctx, fromUnix, toUnix)
	if err != nil {
		return nil, err
	}

	currentBucketStart, currentTotal := m.bucket.SnapshotProbes()
	if bucketInRangeUnix(currentBucketStart, fromUnix, toUnix) {
		merged := false
		for i := range rows {
			if rows[i].BucketStartUnix != currentBucketStart {
				continue
			}
			rows[i].TotalCount += currentTotal
			merged = true
			break
		}
		if !merged {
			rows = append(rows, ProbeBucketRow{
				BucketStartUnix: currentBucketStart,
				TotalCount:      currentTotal,
			})
			sort.Slice(rows, func(i, j int) bool { return rows[i].BucketStartUnix < rows[j].BucketStartUnix })
		}
	}
	return rows, nil
}

func (m *Manager) QueryHistoryNodePool(fromUnix, toUnix int64) ([]NodePoolBucketRow, error) {
	return m.QueryHistoryNodePoolContext(context.Background(), fromUnix, toUnix)
}

// QueryHistoryNodePoolContext reads node-pool history while honoring both the
// caller context and the metrics manager lifecycle context.
func (m *Manager) QueryHistoryNodePoolContext(ctx context.Context, fromUnix, toUnix int64) ([]NodePoolBucketRow, error) {
	ctx, release, err := m.beginHistoryReadContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := m.prepareHistoryRead(ctx, time.Now()); err != nil {
		return nil, err
	}
	if err := m.historyBucketMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer m.historyBucketMu.RUnlock()
	rows, err := m.repo.queryNodePool(ctx, fromUnix, toUnix)
	if err != nil {
		return nil, err
	}

	currentBucketStart := m.bucket.CurrentBucketStartUnix()
	if m.runtimeStats != nil && bucketInRangeUnix(currentBucketStart, fromUnix, toUnix) {
		totalNodes, healthyNodes, egressIPCount, _ := m.runtimeStats.NodePoolSnapshot()
		merged := false
		for i := range rows {
			if rows[i].BucketStartUnix != currentBucketStart {
				continue
			}
			// Node-pool is a point-in-time snapshot; in-memory values override.
			rows[i].TotalNodes = totalNodes
			rows[i].HealthyNodes = healthyNodes
			rows[i].EgressIPCount = egressIPCount
			merged = true
			break
		}
		if !merged {
			rows = append(rows, NodePoolBucketRow{
				BucketStartUnix: currentBucketStart,
				TotalNodes:      totalNodes,
				HealthyNodes:    healthyNodes,
				EgressIPCount:   egressIPCount,
			})
			sort.Slice(rows, func(i, j int) bool { return rows[i].BucketStartUnix < rows[j].BucketStartUnix })
		}
	}
	return rows, nil
}

func (m *Manager) QueryHistoryLeaseLifetime(fromUnix, toUnix int64, platformID string) ([]LeaseLifetimeBucketRow, error) {
	return m.QueryHistoryLeaseLifetimeContext(context.Background(), fromUnix, toUnix, platformID)
}

// QueryHistoryLeaseLifetimeContext reads lease-lifetime history while
// honoring both the caller context and the metrics manager lifecycle context.
func (m *Manager) QueryHistoryLeaseLifetimeContext(ctx context.Context, fromUnix, toUnix int64, platformID string) ([]LeaseLifetimeBucketRow, error) {
	ctx, release, err := m.beginHistoryReadContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := m.prepareHistoryRead(ctx, time.Now()); err != nil {
		return nil, err
	}
	if err := m.historyBucketMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer m.historyBucketMu.RUnlock()
	rows, err := m.repo.queryLeaseLifetime(ctx, fromUnix, toUnix, platformID)
	if err != nil {
		return nil, err
	}

	currentBucketStart, samples := m.bucket.SnapshotLeaseLifetimeSamples(platformID)
	currentSampleCount := len(samples)
	currentP1, currentP5, currentP50 := computePercentiles(samples)
	if bucketInRangeUnix(currentBucketStart, fromUnix, toUnix) {
		merged := false
		for i := range rows {
			if rows[i].BucketStartUnix != currentBucketStart {
				continue
			}
			if rows[i].SampleCount == 0 && currentSampleCount > 0 {
				rows[i].SampleCount = currentSampleCount
				rows[i].P1Ms = currentP1
				rows[i].P5Ms = currentP5
				rows[i].P50Ms = currentP50
			}
			merged = true
			break
		}
		if !merged {
			rows = append(rows, LeaseLifetimeBucketRow{
				BucketStartUnix: currentBucketStart,
				PlatformID:      platformID,
				SampleCount:     currentSampleCount,
				P1Ms:            currentP1,
				P5Ms:            currentP5,
				P50Ms:           currentP50,
			})
			sort.Slice(rows, func(i, j int) bool { return rows[i].BucketStartUnix < rows[j].BucketStartUnix })
		}
	}
	return rows, nil
}

func bucketInRangeUnix(bucketStartUnix, fromUnix, toUnix int64) bool {
	return bucketStartUnix >= fromUnix && bucketStartUnix <= toUnix
}

func decodeLatencyBucketsJSON(raw string) []int64 {
	if raw == "" {
		return nil
	}
	var buckets []int64
	_ = json.Unmarshal([]byte(raw), &buckets)
	return buckets
}

func encodeLatencyBucketsJSON(buckets []int64) string {
	payload, err := json.Marshal(buckets)
	if err != nil {
		return "[]"
	}
	return string(payload)
}

func mergeLatencyBuckets(base, delta []int64) []int64 {
	size := len(base)
	if len(delta) > size {
		size = len(delta)
	}
	out := make([]int64, size)
	copy(out, base)
	for i := range delta {
		out[i] += delta[i]
	}
	return out
}
