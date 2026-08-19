package metrics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
)

// RuntimeStatsProvider supplies node-pool/platform/lease/latency stats from
// the in-memory topology runtime.
//
// HealthyNodes and UniqueHealthyEgressIPCount use the product-level
// "healthy-and-enabled" semantics, not the raw node-entry local health check.
type RuntimeStatsProvider interface {
	TotalNodes() int
	HealthyNodes() int
	EgressIPCount() int
	UniqueHealthyEgressIPCount() int
	NodePoolSnapshot() (totalNodes, healthyNodes, egressIPCount, healthyEgressIPCount int)
	LeaseCountsByPlatform() map[string]int
	RoutableNodeCount(platformID string) (int, bool)
	PlatformEgressIPCount(platformID string) (int, bool)
	PlatformNodePoolSnapshot(platformID string) (routableNodeCount int, egressIPCount int, ok bool)
	CollectNodeEWMAs(platformID string) []float64
}

// ManagerConfig configures the MetricsManager.
type ManagerConfig struct {
	Repo                    *MetricsRepo
	LatencyBinMs            int
	LatencyOverflowMs       int
	BucketSeconds           int
	ThroughputIntervalSec   int
	ThroughputRetentionSec  int
	ConnectionsIntervalSec  int
	ConnectionsRetentionSec int
	LeasesIntervalSec       int
	LeasesRetentionSec      int
	RuntimeStats            RuntimeStatsProvider
}

// Manager is the central metrics coordinator.
// It owns the Collector, BucketAggregator, RealtimeRing, and MetricsRepo.
// Background tickers drive realtime sampling and bucket flushes.
type Manager struct {
	collector *Collector
	bucket    *BucketAggregator
	// Separate realtime rings keep per-metric sampling intervals independent.
	throughputRing  *RealtimeRing
	connectionsRing *RealtimeRing
	leasesRing      *RealtimeRing
	repo            *MetricsRepo

	runtimeStats RuntimeStatsProvider

	throughputInterval  time.Duration
	connectionsInterval time.Duration
	leasesInterval      time.Duration
	bucketSeconds       int

	// Previous cumulative byte counts for delta calculation (throughput B/s).
	prevIngressBytes int64
	prevEgressBytes  int64

	// Baselines used to derive per-bucket deltas from cumulative collector counters.
	prevBucketGlobal    bucketCounterBaseline
	prevBucketPlatforms map[string]bucketCounterBaseline
	// Flush paths acquire stateMu before collector.requestWindowMu. Request
	// producers acquire only the collector read lock and never stateMu.
	stateMu sync.Mutex
	// historyBucketMu linearizes a history query's SQLite read with bucket
	// rotation and persistence. Readers hold it across the database query and
	// current-bucket merge; bucket mutation/flush paths take the write side.
	historyBucketMu sync.RWMutex

	// Lease lifetime samples are queued from routing hot-path and drained by
	// bucket loop to avoid lock contention in synchronous route handling.
	leaseSamplesCh      chan leaseLifetimeSample
	droppedLeaseSamples atomic.Int64

	// pendingTasks is an ordered retry queue for failed persistence writes.
	// Each task includes all writes for one bucket: primary bucket rows,
	// node-pool snapshot, and latency histograms.
	pendingMu    sync.Mutex
	pendingTasks []*persistTask
	persistMu    sync.Mutex

	stopCh      chan struct{}
	runCtx      context.Context
	runCancel   context.CancelFunc
	wg          sync.WaitGroup
	stopMu      sync.Mutex
	stopDone    chan struct{}
	stopErr     error
	stopping    bool
	repoCloseMu sync.Mutex
	repoClosed  bool
	// lifecycleMu serializes worker admission with Stop. Start counts every
	// worker before releasing this lock, so Stop cannot wait on an empty
	// WaitGroup and then race a late launch.
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	// historyMu owns admission for database-backed history reads. Stop closes
	// this admission and cancels the shared context before the metrics repo is
	// closed by the application.
	historyMu          sync.Mutex
	historyCond        *sync.Cond
	historyCtx         context.Context
	historyCancel      context.CancelFunc
	historyClosed      bool
	activeHistoryReads int
	// eventMu closes admission before shutdown's final flush. activeEvents
	// tracks handlers that already passed admission so no event can mutate
	// metrics after the final snapshot.
	eventMu      sync.Mutex
	eventCond    *sync.Cond
	eventsClosed bool
	activeEvents int
	// flushMu closes admission for all paths that can rotate a bucket or
	// publish/retry a persistence task. Stop waits for these paths before its
	// final aggregate and drain, so no task can appear after shutdown returns.
	flushMu       sync.Mutex
	flushCond     *sync.Cond
	flushClosed   bool
	activeFlushes int
	// Package-private seam for deterministic Stop admission tests.
	beforeEventDrainHook func()
	// Package-private seam for deterministic Start/Stop admission tests.
	beforeWorkerStartHook func()

	// Package-private seam for deterministic tests between bucket transition
	// and latency-counter draining.
	afterBucketMaybeFlushHook func()
	// Package-private seam for deterministic tests before a history read's
	// exclusive preparation. It runs before historyBucketMu is acquired.
	beforeHistoryPrepareHook func()
	// Package-private seam for deterministic tests between building a flush
	// task and publishing it to the pending persistence queue.
	beforePersistTaskEnqueueHook func()
	// Package-private seams for the access-latency snapshot consistency test.
	afterAccessLatencySnapshotLockHook func()
	afterAccessLatencyBucketStartHook  func()
}

type persistTask struct {
	Bucket          *BucketFlushData
	NodePool        *nodePoolSnapshot
	GlobalLatency   []int64
	PlatformLatency map[string][]int64
}

type nodePoolSnapshot struct {
	TotalNodes    int
	HealthyNodes  int
	EgressIPCount int
}

type bucketCounterBaseline struct {
	Requests     int64
	Success      int64
	ProbeEgress  int64
	ProbeLatency int64
}

type leaseLifetimeSample struct {
	PlatformID string
	LifetimeNs int64
}

const leaseSampleQueueSize = 8192

// NewManager creates a MetricsManager.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := ValidateDurationSeconds(cfg.BucketSeconds); err != nil {
		return nil, fmt.Errorf("bucket seconds: %w", err)
	}
	if err := ValidateDurationSeconds(cfg.ThroughputIntervalSec); err != nil {
		return nil, fmt.Errorf("throughput interval seconds: %w", err)
	}
	if err := ValidateDurationSeconds(cfg.ConnectionsIntervalSec); err != nil {
		return nil, fmt.Errorf("connections interval seconds: %w", err)
	}
	if err := ValidateDurationSeconds(cfg.LeasesIntervalSec); err != nil {
		return nil, fmt.Errorf("leases interval seconds: %w", err)
	}
	latencyBinMs := cfg.LatencyBinMs
	if latencyBinMs == 0 {
		latencyBinMs = defaultLatencyBinMs
	}
	latencyOverflowMs := cfg.LatencyOverflowMs
	if latencyOverflowMs == 0 {
		latencyOverflowMs = defaultLatencyOverflowMs
	}
	collector, err := NewCollector(latencyBinMs, latencyOverflowMs)
	if err != nil {
		return nil, err
	}
	throughputCapacity, err := RealtimeCapacity(cfg.ThroughputRetentionSec, cfg.ThroughputIntervalSec)
	if err != nil {
		return nil, fmt.Errorf("throughput realtime capacity: %w", err)
	}
	connectionsCapacity, err := RealtimeCapacity(cfg.ConnectionsRetentionSec, cfg.ConnectionsIntervalSec)
	if err != nil {
		return nil, fmt.Errorf("connections realtime capacity: %w", err)
	}
	leasesCapacity, err := RealtimeCapacity(cfg.LeasesRetentionSec, cfg.LeasesIntervalSec)
	if err != nil {
		return nil, fmt.Errorf("leases realtime capacity: %w", err)
	}
	throughputRing, err := NewRealtimeRing(throughputCapacity)
	if err != nil {
		return nil, fmt.Errorf("throughput realtime ring: %w", err)
	}
	connectionsRing, err := NewRealtimeRing(connectionsCapacity)
	if err != nil {
		return nil, fmt.Errorf("connections realtime ring: %w", err)
	}
	leasesRing, err := NewRealtimeRing(leasesCapacity)
	if err != nil {
		return nil, fmt.Errorf("leases realtime ring: %w", err)
	}
	historyCtx, historyCancel := context.WithCancel(context.Background())
	runCtx, runCancel := context.WithCancel(context.Background())
	m := &Manager{
		collector:           collector,
		bucket:              NewBucketAggregator(cfg.BucketSeconds),
		throughputRing:      throughputRing,
		connectionsRing:     connectionsRing,
		leasesRing:          leasesRing,
		repo:                cfg.Repo,
		runtimeStats:        cfg.RuntimeStats,
		throughputInterval:  time.Duration(cfg.ThroughputIntervalSec) * time.Second,
		connectionsInterval: time.Duration(cfg.ConnectionsIntervalSec) * time.Second,
		leasesInterval:      time.Duration(cfg.LeasesIntervalSec) * time.Second,
		bucketSeconds:       cfg.BucketSeconds,
		prevBucketPlatforms: make(map[string]bucketCounterBaseline),
		leaseSamplesCh:      make(chan leaseLifetimeSample, leaseSampleQueueSize),
		stopCh:              make(chan struct{}),
		runCtx:              runCtx,
		runCancel:           runCancel,
		historyCtx:          historyCtx,
		historyCancel:       historyCancel,
	}
	m.eventCond = sync.NewCond(&m.eventMu)
	m.flushCond = sync.NewCond(&m.flushMu)
	m.historyCond = sync.NewCond(&m.historyMu)
	return m, nil
}

// Start launches background tickers for realtime sampling and bucket flushing.
func (m *Manager) Start() {
	m.lifecycleMu.Lock()
	if m.started || m.stopped {
		m.lifecycleMu.Unlock()
		return
	}
	m.started = true
	if hook := m.beforeWorkerStartHook; hook != nil {
		hook()
	}
	m.wg.Add(4)
	m.lifecycleMu.Unlock()

	go m.throughputLoop()
	go m.connectionsLoop()
	go m.leasesLoop()
	go m.bucketLoop()
}

// Stop signals background workers to stop, flushes any remaining bucket data, and waits.
func (m *Manager) Stop() {
	_ = m.StopContext(context.Background())
}

// StopContext stops background workers and initiates the final persistence
// drain. The caller's ctx only bounds how long that caller waits; the single
// shutdown owner continues with its own context after an early waiter returns.
func (m *Manager) StopContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.stopMu.Lock()
	if m.stopping {
		done := m.stopDone
		m.stopMu.Unlock()
		select {
		case <-done:
			m.stopMu.Lock()
			err := m.stopErr
			m.stopMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.stopping = true
	done := make(chan struct{})
	m.stopDone = done
	m.stopMu.Unlock()

	go func() {
		// The first caller owns the shutdown sequence, but not its waiter
		// deadline. Once admitted, the owner must finish the final persistence
		// drain even if that caller returns on context cancellation.
		err := m.stopOwner(context.Background())
		m.stopMu.Lock()
		m.stopErr = err
		close(done)
		m.stopMu.Unlock()
	}()

	select {
	case <-done:
		m.stopMu.Lock()
		err := m.stopErr
		m.stopMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseContext stops the manager and closes its repository only after the
// single stop owner has completed. A caller whose context expires may return
// while the owner is still draining an admitted history read; in that case
// the repository remains open for the owner and a later caller can finish the
// same shutdown sequence.
func (m *Manager) CloseContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stopErr := m.StopContext(ctx)
	if !m.stopCompleted() {
		return stopErr
	}

	m.repoCloseMu.Lock()
	defer m.repoCloseMu.Unlock()
	if m.repoClosed {
		return stopErr
	}
	var closeErr error
	if m.repo != nil {
		closeErr = m.repo.Close()
	}
	if closeErr == nil {
		m.repoClosed = true
	}
	return errors.Join(stopErr, closeErr)
}

func (m *Manager) stopCompleted() bool {
	m.stopMu.Lock()
	done := m.stopDone
	m.stopMu.Unlock()
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func (m *Manager) stopOwner(ctx context.Context) error {
	m.lifecycleMu.Lock()
	m.stopped = true
	m.lifecycleMu.Unlock()
	if m.runCancel != nil {
		m.runCancel()
	}

	m.closeHistoryReadAdmissionAndWait()

	m.eventMu.Lock()
	m.eventsClosed = true
	hook := m.beforeEventDrainHook
	m.eventMu.Unlock()
	if hook != nil {
		hook()
	}

	m.eventMu.Lock()
	for m.activeEvents != 0 {
		m.eventCond.Wait()
	}
	m.eventMu.Unlock()

	m.flushMu.Lock()
	m.flushClosed = true
	for m.activeFlushes != 0 {
		m.flushCond.Wait()
	}
	m.flushMu.Unlock()

	close(m.stopCh)
	m.wg.Wait()

	// Aggregate any final deltas into current in-memory bucket before force flush.
	m.historyBucketMu.Lock()
	m.stateMu.Lock()
	m.collector.requestWindowMu.Lock()
	m.aggregateCollectorDeltasIntoBucketLocked()
	m.drainLeaseLifetimeSamplesLocked()
	data := m.bucket.ForceFlush()
	var task *persistTask
	if data != nil {
		task = m.buildPersistTask(data)
	}
	m.collector.requestWindowMu.Unlock()
	m.stateMu.Unlock()
	m.historyBucketMu.Unlock()

	// Final bucket flush on shutdown (enqueue; drain below with bounded retry).
	if task != nil {
		m.enqueuePersistTask(task)
	}

	// Drain pending tasks with bounded, cancellable retries.
	return m.drainPendingTasksContext(ctx, 3, 500*time.Millisecond)
}

func (m *Manager) beginHistoryRead() (context.Context, func(), error) {
	return m.beginHistoryReadContext(context.Background())
}

func (m *Manager) beginHistoryReadContext(parent context.Context) (context.Context, func(), error) {
	if m == nil {
		return nil, func() {}, fmt.Errorf("metrics manager is nil")
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return nil, func() {}, err
	}
	m.historyMu.Lock()
	if m.historyClosed {
		m.historyMu.Unlock()
		return nil, func() {}, fmt.Errorf("metrics history query admission is closed")
	}
	m.activeHistoryReads++
	lifetimeCtx := m.historyCtx
	m.historyMu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stopAfterFunc := context.AfterFunc(lifetimeCtx, cancel)

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			stopAfterFunc()
			cancel()
			m.historyMu.Lock()
			m.activeHistoryReads--
			if m.activeHistoryReads == 0 {
				m.historyCond.Broadcast()
			}
			m.historyMu.Unlock()
		})
	}
	return ctx, release, nil
}

func (m *Manager) closeHistoryReadAdmissionAndWait() {
	m.historyMu.Lock()
	if !m.historyClosed {
		m.historyClosed = true
		if m.historyCancel != nil {
			m.historyCancel()
		}
	}
	for m.activeHistoryReads != 0 {
		m.historyCond.Wait()
	}
	m.historyMu.Unlock()
}

// --- Event handlers (hot-path, called by proxy/routing/probe) ---

func (m *Manager) withEventAdmission(fn func()) {
	if m == nil || fn == nil {
		return
	}
	m.eventMu.Lock()
	if m.eventsClosed {
		m.eventMu.Unlock()
		return
	}
	m.activeEvents++
	m.eventMu.Unlock()
	defer func() {
		m.eventMu.Lock()
		m.activeEvents--
		if m.activeEvents == 0 {
			m.eventCond.Broadcast()
		}
		m.eventMu.Unlock()
	}()
	fn()
}

// withFlushAdmission admits one complete bucket/persistence operation. The
// caller must keep all work that can enqueue or write a persistTask inside fn.
func (m *Manager) withFlushAdmission(fn func()) bool {
	if m == nil || fn == nil {
		return false
	}
	m.flushMu.Lock()
	if m.flushClosed {
		m.flushMu.Unlock()
		return false
	}
	m.activeFlushes++
	m.flushMu.Unlock()
	defer func() {
		m.flushMu.Lock()
		m.activeFlushes--
		if m.activeFlushes == 0 {
			m.flushCond.Broadcast()
		}
		m.flushMu.Unlock()
	}()
	fn()
	return true
}

// OnRequestFinished records request completion metrics.
func (m *Manager) OnRequestFinished(ev proxy.RequestFinishedEvent) {
	m.withEventAdmission(func() {
		latencyMs := ev.DurationNs / 1e6
		m.collector.RecordRequest(ev.PlatformID, ev.NetOK, latencyMs, ev.IsConnect)
	})
}

// OnTrafficDelta records global traffic bytes (implements proxy.MetricsEventSink).
func (m *Manager) OnTrafficDelta(ingressBytes, egressBytes int64) {
	m.withEventAdmission(func() {
		m.collector.RecordTraffic(ingressBytes, egressBytes)
		m.bucket.AddTraffic(ingressBytes, egressBytes)
	})
}

// OnConnectionLifecycle records connection open/close (implements proxy.MetricsEventSink).
func (m *Manager) OnConnectionLifecycle(direction proxy.ConnectionDirection, op proxy.ConnectionOp) {
	m.withEventAdmission(func() {
		var delta int64
		switch op {
		case proxy.ConnectionOpen:
			delta = 1
		case proxy.ConnectionClose:
			delta = -1
		default:
			return
		}
		m.collector.RecordConnection(direction, delta)
	})
}

// OnProbeEvent records a probe attempt.
func (m *Manager) OnProbeEvent(ev ProbeEvent) {
	m.withEventAdmission(func() {
		m.collector.RecordProbe(ev.Kind)
	})
}

// OnLeaseEvent records lease lifecycle for metrics.
func (m *Manager) OnLeaseEvent(ev LeaseMetricEvent) {
	m.withEventAdmission(func() {
		if ev.Op.HasLifetimeSample() && ev.LifetimeNs > 0 {
			select {
			case m.leaseSamplesCh <- leaseLifetimeSample{
				PlatformID: ev.PlatformID,
				LifetimeNs: ev.LifetimeNs,
			}:
			default:
				m.droppedLeaseSamples.Add(1)
			}
		}
	})
}

// --- Query methods (for API handlers) ---

// Collector returns the underlying collector for snapshot access.
func (m *Manager) Collector() *Collector { return m.collector }

// ThroughputRing returns the realtime throughput ring buffer.
func (m *Manager) ThroughputRing() *RealtimeRing { return m.throughputRing }

// ConnectionsRing returns the realtime connections ring buffer.
func (m *Manager) ConnectionsRing() *RealtimeRing { return m.connectionsRing }

// LeasesRing returns the realtime leases ring buffer.
func (m *Manager) LeasesRing() *RealtimeRing { return m.leasesRing }

// Repo returns the metrics repo for historical queries.
func (m *Manager) Repo() *MetricsRepo { return m.repo }

// BucketSeconds returns the configured bucket duration in seconds.
func (m *Manager) BucketSeconds() int { return m.bucketSeconds }

// ThroughputIntervalSeconds returns the configured throughput realtime interval in seconds.
func (m *Manager) ThroughputIntervalSeconds() int { return int(m.throughputInterval.Seconds()) }

// ConnectionsIntervalSeconds returns the configured connections realtime interval in seconds.
func (m *Manager) ConnectionsIntervalSeconds() int { return int(m.connectionsInterval.Seconds()) }

// LeasesIntervalSeconds returns the configured leases realtime interval in seconds.
func (m *Manager) LeasesIntervalSeconds() int { return int(m.leasesInterval.Seconds()) }

// RuntimeStats returns the runtime stats provider.
func (m *Manager) RuntimeStats() RuntimeStatsProvider { return m.runtimeStats }

// SnapshotCurrentTrafficBucket returns unflushed global traffic in current bucket.
func (m *Manager) SnapshotCurrentTrafficBucket() (bucketStartUnix, ingressBytes, egressBytes int64) {
	m.advanceAndMaybeFlush(time.Now())
	return m.bucket.SnapshotTraffic()
}

// SnapshotCurrentRequestsBucket returns unflushed requests in current bucket.
// platformID="" means global scope.
func (m *Manager) SnapshotCurrentRequestsBucket(platformID string) (bucketStartUnix, totalRequests, successRequests int64) {
	m.advanceAndMaybeFlush(time.Now())
	return m.bucket.SnapshotRequests(platformID)
}

// SnapshotCurrentProbeBucket returns unflushed probe count in current bucket.
func (m *Manager) SnapshotCurrentProbeBucket() (bucketStartUnix, totalCount int64) {
	m.advanceAndMaybeFlush(time.Now())
	return m.bucket.SnapshotProbes()
}

// SnapshotCurrentAccessLatencyBucket returns the in-progress latency histogram
// for current bucket. platformID="" means global scope.
func (m *Manager) SnapshotCurrentAccessLatencyBucket(platformID string) (bucketStartUnix int64, buckets []int64) {
	m.advanceAndMaybeFlush(time.Now())
	// Keep the bucket identity paired with its in-memory histogram. A concurrent
	// flush must not rotate and drain the bucket between these two reads.
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if hook := m.afterAccessLatencySnapshotLockHook; hook != nil {
		hook()
	}
	return m.snapshotCurrentAccessLatencyBucketLocked(platformID)
}

// snapshotCurrentAccessLatencyBucketLocked returns the current bucket start
// and histogram as one state snapshot. The caller must hold stateMu; the
// collector methods then take requestWindowMu in the established order.
func (m *Manager) snapshotCurrentAccessLatencyBucketLocked(platformID string) (bucketStartUnix int64, buckets []int64) {
	bucketStartUnix = m.bucket.CurrentBucketStartUnix()
	if hook := m.afterAccessLatencyBucketStartHook; hook != nil {
		hook()
	}

	if platformID == "" {
		snap := m.collector.Snapshot()
		return bucketStartUnix, append([]int64(nil), snap.LatencyBuckets...)
	}

	snap, ok := m.collector.PlatformSnapshot(platformID)
	if !ok {
		globalSnap := m.collector.Snapshot()
		return bucketStartUnix, make([]int64, len(globalSnap.LatencyBuckets))
	}
	return bucketStartUnix, append([]int64(nil), snap.LatencyBuckets...)
}

// SnapshotCurrentNodePoolBucket returns a node-pool snapshot for current bucket.
func (m *Manager) SnapshotCurrentNodePoolBucket() (bucketStartUnix int64, totalNodes, healthyNodes, egressIPCount int, ok bool) {
	m.advanceAndMaybeFlush(time.Now())
	bucketStartUnix = m.bucket.CurrentBucketStartUnix()

	if m.runtimeStats == nil {
		return bucketStartUnix, 0, 0, 0, false
	}
	totalNodes, healthyNodes, egressIPCount, _ = m.runtimeStats.NodePoolSnapshot()
	return bucketStartUnix, totalNodes, healthyNodes, egressIPCount, true
}

// SnapshotCurrentLeaseLifetimeBucket returns lease lifetime percentiles for the
// in-progress current bucket and platform.
func (m *Manager) SnapshotCurrentLeaseLifetimeBucket(platformID string) (
	bucketStartUnix int64,
	sampleCount int,
	p1Ms, p5Ms, p50Ms float64,
) {
	m.advanceAndMaybeFlush(time.Now())
	bucketStartUnix, samples := m.bucket.SnapshotLeaseLifetimeSamples(platformID)
	if len(samples) == 0 {
		return bucketStartUnix, 0, 0, 0, 0
	}
	p1Ms, p5Ms, p50Ms = computePercentiles(samples)
	return bucketStartUnix, len(samples), p1Ms, p5Ms, p50Ms
}

// --- Background loops ---

func (m *Manager) throughputLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.throughputInterval)
	defer ticker.Stop()

	for {
		select {
		case ts := <-ticker.C:
			m.takeThroughputSample(ts)
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) connectionsLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.connectionsInterval)
	defer ticker.Stop()

	for {
		select {
		case ts := <-ticker.C:
			m.takeConnectionsSample(ts)
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) leasesLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.leasesInterval)
	defer ticker.Stop()

	for {
		select {
		case ts := <-ticker.C:
			m.takeLeasesSample(ts)
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) bucketLoop() {
	defer m.wg.Done()

	// Align the first tick to the next bucket boundary.
	// DESIGN.md: bucket_start_unix = (ts_unix / N) * N.
	now := time.Now().Unix()
	bucketSec := int64(m.bucketSeconds)
	nextBoundary := ((now / bucketSec) + 1) * bucketSec
	initialDelay := time.Duration(nextBoundary-now) * time.Second

	select {
	case <-time.After(initialDelay):
		m.flushBucket()
	case <-m.stopCh:
		return
	}

	ticker := time.NewTicker(time.Duration(m.bucketSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.flushBucket()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) takeThroughputSample(ts time.Time) {
	snap := m.collector.Snapshot()

	// Compute per-sample delta and normalize to bytes-per-second.
	deltaIngress := snap.IngressBytes - m.prevIngressBytes
	deltaEgress := snap.EgressBytes - m.prevEgressBytes
	m.prevIngressBytes = snap.IngressBytes
	m.prevEgressBytes = snap.EgressBytes
	if deltaIngress < 0 {
		deltaIngress = 0
	}
	if deltaEgress < 0 {
		deltaEgress = 0
	}
	sampleSec := int64(m.throughputInterval / time.Second)
	if sampleSec <= 0 {
		sampleSec = 1
	}
	ingressBPS := deltaIngress / sampleSec
	egressBPS := deltaEgress / sampleSec

	m.throughputRing.Push(RealtimeSample{
		Timestamp:  ts,
		IngressBPS: ingressBPS,
		EgressBPS:  egressBPS,
	})
}

func (m *Manager) takeConnectionsSample(ts time.Time) {
	inboundMax, outboundMax := m.collector.SwapConnectionWindowMax()

	m.connectionsRing.Push(RealtimeSample{
		Timestamp:     ts,
		InboundConns:  inboundMax,
		OutboundConns: outboundMax,
	})
}

func (m *Manager) takeLeasesSample(ts time.Time) {
	var leases map[string]int
	if m.runtimeStats != nil {
		leases = maps.Clone(m.runtimeStats.LeaseCountsByPlatform())
	}

	m.leasesRing.Push(RealtimeSample{
		Timestamp:        ts,
		LeasesByPlatform: leases,
	})
}

func (m *Manager) flushBucket() {
	m.flushBucketAtContext(m.runCtx, time.Now())
}

func (m *Manager) flushBucketAt(now time.Time) {
	m.flushBucketAtContext(context.Background(), now)
}

func (m *Manager) flushBucketAtContext(ctx context.Context, now time.Time) {
	m.historyBucketMu.Lock()
	defer m.historyBucketMu.Unlock()
	m.withFlushAdmission(func() {
		m.advanceAndMaybeFlushNoAdmission(now)
		m.flushPendingTasksContext(ctx, "[metrics] bucket persistence failed, will retry next tick")
	})
}

func (m *Manager) aggregateCollectorDeltasIntoBucketLocked() {
	// Caller must hold stateMu and collector.requestWindowMu (read or write).
	currentGlobal := m.collector.snapshotUnlocked()
	globalBase := m.prevBucketGlobal
	globalCurrent := baselineFromSnapshot(currentGlobal)

	globalRequestsDelta := nonNegativeDelta(globalCurrent.Requests, globalBase.Requests)
	globalSuccessDelta := nonNegativeDelta(globalCurrent.Success, globalBase.Success)
	if globalSuccessDelta > globalRequestsDelta {
		globalSuccessDelta = globalRequestsDelta
	}
	globalProbeDelta := nonNegativeDelta(
		globalCurrent.ProbeEgress+globalCurrent.ProbeLatency,
		globalBase.ProbeEgress+globalBase.ProbeLatency,
	)

	currentPlatforms := m.collector.platformSnapshotsUnlocked()
	nextPlatformBaseline := make(map[string]bucketCounterBaseline, len(currentPlatforms))

	var sumPlatformRequests int64
	var sumPlatformSuccess int64

	for pid, snap := range currentPlatforms {
		cur := baselineFromSnapshot(snap)
		prev := m.prevBucketPlatforms[pid]
		nextPlatformBaseline[pid] = cur

		requestDelta := nonNegativeDelta(cur.Requests, prev.Requests)
		successDelta := nonNegativeDelta(cur.Success, prev.Success)
		if successDelta > requestDelta {
			successDelta = requestDelta
		}

		if requestDelta != 0 {
			m.bucket.AddRequestCounts(pid, requestDelta, successDelta)
		}

		sumPlatformRequests += requestDelta
		sumPlatformSuccess += successDelta
	}

	globalOnlyRequests := nonNegativeDelta(globalRequestsDelta, sumPlatformRequests)
	globalOnlySuccess := nonNegativeDelta(globalSuccessDelta, sumPlatformSuccess)
	if globalOnlySuccess > globalOnlyRequests {
		globalOnlySuccess = globalOnlyRequests
	}
	if globalOnlyRequests != 0 {
		m.bucket.AddRequestCounts("", globalOnlyRequests, globalOnlySuccess)
	}

	if globalProbeDelta != 0 {
		m.bucket.AddProbeCount(globalProbeDelta)
	}

	m.prevBucketGlobal = globalCurrent
	m.prevBucketPlatforms = nextPlatformBaseline
}

func (m *Manager) drainLeaseLifetimeSamplesLocked() {
	for {
		select {
		case sample := <-m.leaseSamplesCh:
			m.bucket.AddLeaseLifetime(sample.PlatformID, sample.LifetimeNs)
		default:
			dropped := m.droppedLeaseSamples.Swap(0)
			if dropped > 0 {
				log.Printf("[metrics] dropped %d lease lifetime samples due to full queue", dropped)
			}
			return
		}
	}
}

func (m *Manager) syncCurrentBucketState() {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.collector.requestWindowMu.RLock()
	defer m.collector.requestWindowMu.RUnlock()
	m.aggregateCollectorDeltasIntoBucketLocked()
	m.drainLeaseLifetimeSamplesLocked()
}

func (m *Manager) advanceAndMaybeFlush(now time.Time) {
	m.historyBucketMu.Lock()
	defer m.historyBucketMu.Unlock()
	m.withFlushAdmission(func() {
		m.advanceAndMaybeFlushNoAdmission(now)
	})
}

func (m *Manager) advanceAndMaybeFlushNoAdmission(now time.Time) {
	m.stateMu.Lock()
	m.collector.requestWindowMu.Lock()
	m.aggregateCollectorDeltasIntoBucketLocked()
	m.drainLeaseLifetimeSamplesLocked()
	data := m.bucket.MaybeFlush(now)
	var task *persistTask
	if hook := m.afterBucketMaybeFlushHook; hook != nil && data != nil {
		hook()
	}
	if data != nil {
		// Pair the latency counters with the bucket transition while the same
		// state owner is held. A concurrent history read or bucket tick must not
		// be able to flush the next bucket first and steal these samples.
		task = m.buildPersistTask(data)
	}
	m.collector.requestWindowMu.Unlock()
	m.stateMu.Unlock()
	if task != nil {
		if hook := m.beforePersistTaskEnqueueHook; hook != nil {
			hook()
		}
		m.enqueuePersistTask(task)
	}
}

func (m *Manager) flushPendingTasks(errPrefix string) {
	m.flushPendingTasksContext(context.Background(), errPrefix)
}

func (m *Manager) flushPendingTasksContext(ctx context.Context, errPrefix string) {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()

	for {
		task, ok := m.peekPendingTask()
		if !ok {
			return
		}
		if err := m.writePersistTaskContext(ctx, task); err != nil {
			if errPrefix != "" {
				log.Printf("%s: %v", errPrefix, err)
			}
			return
		}
		m.popPendingTask()
	}
}

func baselineFromSnapshot(s CountersSnapshot) bucketCounterBaseline {
	return bucketCounterBaseline{
		Requests:     s.Requests,
		Success:      s.SuccessRequests,
		ProbeEgress:  s.ProbeEgress,
		ProbeLatency: s.ProbeLatency,
	}
}

func nonNegativeDelta(current, previous int64) int64 {
	delta := current - previous
	if delta < 0 {
		return 0
	}
	return delta
}

func (m *Manager) buildPersistTask(data *BucketFlushData) *persistTask {
	if data == nil {
		return nil
	}
	task := &persistTask{
		Bucket:          data,
		GlobalLatency:   m.collector.swapLatencyBucketsUnlocked(),
		PlatformLatency: m.collector.platformSwapAllUnlocked(),
	}
	if m.runtimeStats != nil {
		totalNodes, healthyNodes, egressIPCount, _ := m.runtimeStats.NodePoolSnapshot()
		task.NodePool = &nodePoolSnapshot{
			TotalNodes:    totalNodes,
			HealthyNodes:  healthyNodes,
			EgressIPCount: egressIPCount,
		}
	}
	return task
}

func (m *Manager) writePersistTask(task *persistTask) error {
	return m.writePersistTaskContext(context.Background(), task)
}

func (m *Manager) writePersistTaskContext(ctx context.Context, task *persistTask) error {
	if task == nil || task.Bucket == nil {
		return nil
	}
	if m.repo == nil {
		return fmt.Errorf("metrics repo is nil")
	}
	return m.repo.WritePersistTaskContext(
		ctx,
		task.Bucket,
		task.NodePool,
		task.GlobalLatency,
		task.PlatformLatency,
	)
}

func (m *Manager) enqueuePersistTask(task *persistTask) {
	if task == nil {
		return
	}
	m.pendingMu.Lock()
	m.pendingTasks = append(m.pendingTasks, task)
	m.pendingMu.Unlock()
}

func (m *Manager) peekPendingTask() (*persistTask, bool) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	if len(m.pendingTasks) == 0 {
		return nil, false
	}
	return m.pendingTasks[0], true
}

func (m *Manager) popPendingTask() {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	if len(m.pendingTasks) == 0 {
		return
	}
	m.pendingTasks[0] = nil
	m.pendingTasks = m.pendingTasks[1:]
}

func (m *Manager) drainPendingTasks(maxAttempts int, retryDelay time.Duration) {
	_ = m.drainPendingTasksContext(context.Background(), maxAttempts, retryDelay)
}

func (m *Manager) drainPendingTasksContext(ctx context.Context, maxAttempts int, retryDelay time.Duration) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.persistMu.Lock()
	defer m.persistMu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		task, ok := m.peekPendingTask()
		if !ok {
			return nil
		}

		success := false
		for attempt := 0; attempt < maxAttempts; attempt++ {
			if err := m.writePersistTaskContext(ctx, task); err != nil {
				log.Printf("[metrics] shutdown persistence attempt %d failed: %v", attempt+1, err)
				if attempt+1 < maxAttempts {
					timer := time.NewTimer(retryDelay)
					select {
					case <-timer.C:
					case <-ctx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						return ctx.Err()
					}
				}
				continue
			}
			success = true
			break
		}
		if !success {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("metrics persistence failed after %d attempts", maxAttempts)
		}
		m.popPendingTask()
	}
}
