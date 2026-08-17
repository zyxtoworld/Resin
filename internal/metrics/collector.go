package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Resinat/Resin/internal/metricsconfig"
)

// Collector holds hot-path atomic counters for global and per-platform metrics.
// RecordRequest uses one read-side request window so a flush cannot split a
// request's global/platform counts from its latency samples.
type Collector struct {
	requestWindowMu sync.RWMutex
	global          *counters
	platform        sync.Map // string -> *counters

	// Package-private seams for deterministic request-window tests.
	beforeRecordRequestHook    func()
	afterRecordRequestLockHook func()
}

// counters holds atomic counters for one measurement scope (global or per-platform).
type counters struct {
	requests        atomic.Int64
	successRequests atomic.Int64
	ingressBytes    atomic.Int64
	egressBytes     atomic.Int64
	inboundConns    atomic.Int64
	outboundConns   atomic.Int64
	// Window peaks for realtime connection sampling. These track the maximum
	// active connections observed since the previous sample.
	inboundConnsPeak  atomic.Int64
	outboundConnsPeak atomic.Int64
	probeEgress       atomic.Int64
	probeLatency      atomic.Int64

	// Latency histogram: fixed-bucket durations.
	// Each regular bucket[i] = count of requests with latency in
	// [i*binWidth, (i+1)*binWidth). The last bucket is overflow (>= overflowMs).
	latencyBuckets []atomic.Int64
	latencyBinMs   int
	latencyOverMs  int
}

// CountersSnapshot is a point-in-time snapshot of counters for reading.
type CountersSnapshot struct {
	Requests        int64
	SuccessRequests int64
	IngressBytes    int64
	EgressBytes     int64
	InboundConns    int64
	OutboundConns   int64
	ProbeEgress     int64
	ProbeLatency    int64
	LatencyBuckets  []int64
	LatencyBinMs    int
	LatencyOverMs   int
}

const (
	defaultLatencyBinMs      = 50
	defaultLatencyOverflowMs = 5000
)

// NewCollector creates a new Collector with the given latency histogram
// parameters. It validates the bucket count before allocating any counters.
func NewCollector(latencyBinMs, latencyOverflowMs int) (*Collector, error) {
	bucketCount, err := metricsconfig.LatencyHistogramBucketCount(latencyBinMs, latencyOverflowMs)
	if err != nil {
		return nil, fmt.Errorf("latency histogram: %w", err)
	}
	return &Collector{
		global: newCounters(latencyBinMs, latencyOverflowMs, bucketCount),
	}, nil
}

func newCounters(binMs, overMs, bucketCount int) *counters {
	return &counters{
		latencyBuckets: make([]atomic.Int64, bucketCount),
		latencyBinMs:   binMs,
		latencyOverMs:  overMs,
	}
}

func (c *Collector) getOrCreatePlatform(platformID string) *counters {
	if platformID == "" {
		return nil
	}
	if v, ok := c.platform.Load(platformID); ok {
		return v.(*counters)
	}
	nc := newCounters(c.global.latencyBinMs, c.global.latencyOverMs, len(c.global.latencyBuckets))
	actual, _ := c.platform.LoadOrStore(platformID, nc)
	return actual.(*counters)
}

// RecordRequest records a completed request.
func (c *Collector) RecordRequest(platformID string, success bool, latencyMs int64, isConnect bool) {
	if hook := c.beforeRecordRequestHook; hook != nil {
		hook()
	}
	c.requestWindowMu.RLock()
	if hook := c.afterRecordRequestLockHook; hook != nil {
		hook()
	}
	defer c.requestWindowMu.RUnlock()

	c.global.requests.Add(1)
	if success {
		c.global.successRequests.Add(1)
	}
	if !isConnect && latencyMs >= 0 {
		c.recordLatency(c.global, latencyMs)
	}

	if pc := c.getOrCreatePlatform(platformID); pc != nil {
		pc.requests.Add(1)
		if success {
			pc.successRequests.Add(1)
		}
		if !isConnect && latencyMs >= 0 {
			c.recordLatency(pc, latencyMs)
		}
	}
}

func (c *Collector) recordLatency(ct *counters, ms int64) {
	overflowIdx := len(ct.latencyBuckets) - 1
	if overflowIdx < 0 {
		return
	}

	// Overflow bucket counts samples >= overflow_ms.
	if ms >= int64(ct.latencyOverMs) {
		ct.latencyBuckets[overflowIdx].Add(1)
		return
	}

	// Regular buckets are [lower, upper) with bin width.
	idx := 0
	if ms >= 0 {
		idx = int(ms / int64(ct.latencyBinMs))
	}
	if idx >= overflowIdx {
		idx = overflowIdx - 1
	}
	if idx < 0 {
		idx = 0
	}

	ct.latencyBuckets[idx].Add(1)
}

// RecordTraffic records global traffic bytes.
func (c *Collector) RecordTraffic(ingress, egress int64) {
	c.global.ingressBytes.Add(ingress)
	c.global.egressBytes.Add(egress)
}

// RecordConnection records a connection lifecycle event.
func (c *Collector) RecordConnection(dir ConnectionDirection, delta int64) {
	if dir == ConnInbound {
		current := c.global.inboundConns.Add(delta)
		if delta > 0 {
			recordPeak(&c.global.inboundConnsPeak, current)
		}
	} else {
		current := c.global.outboundConns.Add(delta)
		if delta > 0 {
			recordPeak(&c.global.outboundConnsPeak, current)
		}
	}
}

func recordPeak(peak *atomic.Int64, value int64) {
	for {
		current := peak.Load()
		if value <= current {
			return
		}
		if peak.CompareAndSwap(current, value) {
			return
		}
	}
}

// SwapConnectionWindowMax atomically returns the maximum active connections
// observed since the previous call, then resets next-window baselines to the
// current active connection counts.
func (c *Collector) SwapConnectionWindowMax() (inboundMax, outboundMax int64) {
	inboundCurrent := c.global.inboundConns.Load()
	outboundCurrent := c.global.outboundConns.Load()

	inboundMax = c.global.inboundConnsPeak.Swap(inboundCurrent)
	outboundMax = c.global.outboundConnsPeak.Swap(outboundCurrent)

	if inboundCurrent > inboundMax {
		inboundMax = inboundCurrent
	}
	if outboundCurrent > outboundMax {
		outboundMax = outboundCurrent
	}
	return inboundMax, outboundMax
}

// RecordProbe records a probe attempt.
func (c *Collector) RecordProbe(kind ProbeKind) {
	switch kind {
	case ProbeKindEgress:
		c.global.probeEgress.Add(1)
	case ProbeKindLatency:
		c.global.probeLatency.Add(1)
	}
}

// Snapshot returns a point-in-time snapshot of the global counters.
func (c *Collector) Snapshot() CountersSnapshot {
	c.requestWindowMu.RLock()
	defer c.requestWindowMu.RUnlock()
	return c.snapshotUnlocked()
}

// PlatformSnapshot returns a snapshot for a specific platform.
func (c *Collector) PlatformSnapshot(platformID string) (CountersSnapshot, bool) {
	c.requestWindowMu.RLock()
	defer c.requestWindowMu.RUnlock()
	return c.platformSnapshotUnlocked(platformID)
}

func (c *Collector) snapshotUnlocked() CountersSnapshot {
	return snapshot(c.global)
}

func (c *Collector) platformSnapshotUnlocked(platformID string) (CountersSnapshot, bool) {
	v, ok := c.platform.Load(platformID)
	if !ok {
		return CountersSnapshot{}, false
	}
	return snapshot(v.(*counters)), true
}

// PlatformSnapshots returns snapshots for all known platforms.
func (c *Collector) PlatformSnapshots() map[string]CountersSnapshot {
	c.requestWindowMu.RLock()
	defer c.requestWindowMu.RUnlock()
	return c.platformSnapshotsUnlocked()
}

func (c *Collector) platformSnapshotsUnlocked() map[string]CountersSnapshot {
	result := make(map[string]CountersSnapshot)
	c.platform.Range(func(key, value any) bool {
		result[key.(string)] = snapshot(value.(*counters))
		return true
	})
	return result
}

func snapshot(ct *counters) CountersSnapshot {
	s := CountersSnapshot{
		Requests:        ct.requests.Load(),
		SuccessRequests: ct.successRequests.Load(),
		IngressBytes:    ct.ingressBytes.Load(),
		EgressBytes:     ct.egressBytes.Load(),
		InboundConns:    ct.inboundConns.Load(),
		OutboundConns:   ct.outboundConns.Load(),
		ProbeEgress:     ct.probeEgress.Load(),
		ProbeLatency:    ct.probeLatency.Load(),
		LatencyBuckets:  make([]int64, len(ct.latencyBuckets)),
		LatencyBinMs:    ct.latencyBinMs,
		LatencyOverMs:   ct.latencyOverMs,
	}
	for i := range ct.latencyBuckets {
		s.LatencyBuckets[i] = ct.latencyBuckets[i].Load()
	}
	return s
}

// swapLatencyBuckets atomically swaps each bucket counter to 0
// and returns the deltas since last swap.
func swapLatencyBuckets(ct *counters) []int64 {
	deltas := make([]int64, len(ct.latencyBuckets))
	for i := range ct.latencyBuckets {
		deltas[i] = ct.latencyBuckets[i].Swap(0)
	}
	return deltas
}

// SwapLatencyBuckets atomically drains the global latency histogram, returning
// per-bucket counts accumulated since the last call. The counters are reset to 0
// so the next call only captures new samples.
func (c *Collector) SwapLatencyBuckets() []int64 {
	c.requestWindowMu.Lock()
	defer c.requestWindowMu.Unlock()
	return c.swapLatencyBucketsUnlocked()
}

func (c *Collector) swapLatencyBucketsUnlocked() []int64 {
	return swapLatencyBuckets(c.global)
}

// PlatformSwapLatencyBuckets does the same for a specific platform.
func (c *Collector) PlatformSwapLatencyBuckets(platformID string) ([]int64, bool) {
	c.requestWindowMu.Lock()
	defer c.requestWindowMu.Unlock()
	return c.platformSwapLatencyBucketsUnlocked(platformID)
}

func (c *Collector) platformSwapLatencyBucketsUnlocked(platformID string) ([]int64, bool) {
	v, ok := c.platform.Load(platformID)
	if !ok {
		return nil, false
	}
	return swapLatencyBuckets(v.(*counters)), true
}

// PlatformSwapAll atomically drains latency histograms for all known platforms.
func (c *Collector) PlatformSwapAll() map[string][]int64 {
	c.requestWindowMu.Lock()
	defer c.requestWindowMu.Unlock()
	return c.platformSwapAllUnlocked()
}

func (c *Collector) platformSwapAllUnlocked() map[string][]int64 {
	result := make(map[string][]int64)
	c.platform.Range(func(key, value any) bool {
		result[key.(string)] = swapLatencyBuckets(value.(*counters))
		return true
	})
	return result
}
