package probe

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

// storeOutbound sets a non-nil outbound on the entry.
func storeOutbound(entry *node.NodeEntry) {
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
}

func TestProbePeriodicScanDoesNotProbeRecreatedEntry(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := []byte(`{"type":"periodic-scan-generation"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok || oldEntry == nil {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)

	fetchCalled := make(chan *node.NodeEntry, 1)
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, entry *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			fetchCalled <- entry
			return []byte("ip=203.0.113.200\nloc=US"), time.Millisecond, nil
		},
		MaxEgressTestInterval: func() time.Duration { return 24 * time.Hour },
	})
	t.Cleanup(mgr.Stop)

	scanEntered := make(chan struct{})
	releaseScan := make(chan struct{})
	var scanOnce sync.Once
	mgr.beforeScanEnqueueHook = func(got node.Hash, expected *node.NodeEntry) {
		if got != hash || expected != oldEntry {
			return
		}
		scanOnce.Do(func() {
			close(scanEntered)
			<-releaseScan
		})
	}

	scanDone := make(chan struct{})
	go func() {
		mgr.scanEgress()
		close(scanDone)
	}()
	select {
	case <-scanEntered:
	case <-time.After(time.Second):
		t.Fatal("periodic egress scan did not reach enqueue gate")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == nil || newEntry == oldEntry {
		t.Fatal("same-hash replacement did not create a new entry")
	}
	storeOutbound(newEntry)

	close(releaseScan)
	select {
	case <-scanDone:
	case <-time.After(time.Second):
		t.Fatal("periodic egress scan did not finish")
	}

	task, ok := mgr.taskQueue.Dequeue()
	if !ok {
		t.Fatal("periodic scan did not enqueue a probe")
	}
	mgr.executeTask(task)
	select {
	case got := <-fetchCalled:
		t.Fatalf("periodic scan probed recreated entry %p, want stale task dropped (entry=%p)", got, newEntry)
	default:
	}
}

type trackingAfterFuncContext struct {
	context.Context

	mu         sync.Mutex
	nextID     int
	active     map[int]func()
	done       chan struct{}
	cancelOnce sync.Once
	registered atomic.Int32
	stopped    atomic.Int32
	invoked    atomic.Int32
}

func newTrackingAfterFuncContext() *trackingAfterFuncContext {
	return &trackingAfterFuncContext{
		Context: context.Background(),
		active:  make(map[int]func()),
		done:    make(chan struct{}),
	}
}

func (c *trackingAfterFuncContext) Done() <-chan struct{} {
	return c.done
}

func (c *trackingAfterFuncContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (c *trackingAfterFuncContext) cancel() {
	c.cancelOnce.Do(func() {
		close(c.done)
		c.invokePending()
	})
}

func (c *trackingAfterFuncContext) AfterFunc(fn func()) func() bool {
	c.mu.Lock()
	id := c.nextID
	c.nextID++
	c.active[id] = fn
	c.mu.Unlock()
	c.registered.Add(1)

	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.active[id]; !ok {
			return false
		}
		delete(c.active, id)
		c.stopped.Add(1)
		return true
	}
}

func (c *trackingAfterFuncContext) invokePending() {
	c.mu.Lock()
	pending := make([]func(), 0, len(c.active))
	for id, fn := range c.active {
		delete(c.active, id)
		pending = append(pending, fn)
	}
	c.mu.Unlock()
	for _, fn := range pending {
		c.invoked.Add(1)
		fn()
	}
}

func TestSynchronousProbeStopsManagerAfterFuncRegistration(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	hash := node.HashFromRawOptions([]byte(`{"type":"after-func-cleanup"}`))
	raw := []byte(`{"type":"after-func-cleanup"}`)
	pool.AddNodeFromSub(hash, raw, "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	managerContext := newTrackingAfterFuncContext()
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(context.Context, *node.NodeEntry, string) ([]byte, time.Duration, error) {
			return []byte("ip=203.0.113.20\nloc=US"), 10 * time.Millisecond, nil
		},
	})
	mgr.probeCtx = managerContext

	if _, err := mgr.ProbeEgressSync(hash); err != nil {
		t.Fatalf("ProbeEgressSync: %v", err)
	}
	mgr.Stop()

	if got := managerContext.registered.Load(); got != 1 {
		t.Fatalf("AfterFunc registrations = %d, want 1", got)
	}
	if got := managerContext.stopped.Load(); got != 1 {
		t.Fatalf("AfterFunc stops = %d, want 1", got)
	}
	managerContext.cancel()
	if got := managerContext.invoked.Load(); got != 0 {
		t.Fatalf("stopped AfterFunc callbacks invoked = %d, want 0", got)
	}
}

func TestSynchronousProbeStopCancelsAndReleasesAfterFunc(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	hash := node.HashFromRawOptions([]byte(`{"type":"after-func-stop"}`))
	raw := []byte(`{"type":"after-func-stop"}`)
	pool.AddNodeFromSub(hash, raw, "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	managerContext := newTrackingAfterFuncContext()
	fetchStarted := make(chan struct{})
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(ctx context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchStarted)
			<-ctx.Done()
			return nil, 0, ctx.Err()
		},
	})
	// Replace both lifecycle values so Stop drives the controllable context
	// used by this test, while the production shape remains unchanged.
	mgr.probeCtx = managerContext
	mgr.probeCancel = managerContext.cancel

	probeDone := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeEgressSync(hash)
		probeDone <- err
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("synchronous probe did not enter Fetcher")
	}

	mgr.Stop()
	select {
	case err := <-probeDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("probe error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous probe did not finish after Stop")
	}

	if got := managerContext.registered.Load(); got != 1 {
		t.Fatalf("AfterFunc registrations = %d, want 1", got)
	}
	if got := managerContext.invoked.Load(); got != 1 {
		t.Fatalf("AfterFunc invocations = %d, want 1", got)
	}
	if got := managerContext.stopped.Load(); got != 0 {
		t.Fatalf("AfterFunc stops after manager cancellation = %d, want 0", got)
	}
	managerContext.mu.Lock()
	active := len(managerContext.active)
	managerContext.mu.Unlock()
	if active != 0 {
		t.Fatalf("active AfterFunc registrations after Stop = %d, want 0", active)
	}
}

// TestProbeEgress_Success verifies that a successful egress probe calls
// RecordResult(true), RecordLatency("cloudflare.com"), and UpdateNodeEgressIP.
func TestProbeEgress_Success(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"egress-ok"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"egress-ok"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	traceBody := []byte("fl=123\nip=203.0.113.1\nloc=US\nts=1234567890")
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return traceBody, 42 * time.Millisecond, nil
		},
	})

	mgr.probeEgress(hash, entry)

	// Verify RecordResult(true) was applied.
	if entry.FailureCount.Load() != 0 {
		t.Fatalf("expected 0 failures, got %d", entry.FailureCount.Load())
	}
	if entry.CircuitOpenSince.Load() != 0 {
		t.Fatal("circuit should not be open")
	}

	// Verify UpdateNodeEgressIP.
	got := entry.GetEgressIP()
	want := netip.MustParseAddr("203.0.113.1")
	if got != want {
		t.Fatalf("egress IP: got %v, want %v", got, want)
	}
	if got := entry.GetEgressRegion(); got != "us" {
		t.Fatalf("egress region: got %q, want %q", got, "us")
	}

	// Verify RecordLatency for cloudflare.com.
	if !entry.HasLatency() {
		t.Fatal("expected latency data")
	}
	stats, ok := entry.LatencyTable.GetDomainStats("cloudflare.com")
	if !ok {
		t.Fatal("expected cloudflare.com latency entry")
	}
	if stats.Ewma != 42*time.Millisecond {
		t.Fatalf("ewma: got %v, want %v", stats.Ewma, 42*time.Millisecond)
	}
}

// TestProbeEgress_Failure verifies that a failed egress probe calls
// RecordResult(false) and accumulates failure count.
func TestProbeEgress_Failure(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"egress-fail"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"egress-fail"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return nil, 0, errors.New("connection refused")
		},
	})

	mgr.probeEgress(hash, entry)

	if entry.FailureCount.Load() != 1 {
		t.Fatalf("expected 1 failure, got %d", entry.FailureCount.Load())
	}

	// No latency or egress IP should be recorded.
	if entry.HasLatency() {
		t.Fatal("should not have latency on failure")
	}
}

// TestProbeEgress_CircuitBreak verifies consecutive failures trigger circuit break.
func TestProbeEgress_CircuitBreak(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"egress-circuit"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"egress-circuit"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return nil, 0, errors.New("timeout")
		},
	})

	mgr.probeEgress(hash, entry)
	mgr.probeEgress(hash, entry)

	if entry.CircuitOpenSince.Load() == 0 {
		t.Fatal("circuit should be open after 2 consecutive failures")
	}
}

func TestProbeEgress_DoesNotWriteRecreatedEntry(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"probe-recreated-entry"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)

	fetchStarted := make(chan struct{})
	fetchDone := make(chan struct{})
	releaseFetch := make(chan struct{})
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchStarted)
			<-releaseFetch
			close(fetchDone)
			return []byte("fl=123\nip=203.0.113.1\nloc=US"), 42 * time.Millisecond, nil
		},
	})
	mgr.Start()

	mgr.TriggerImmediateEgressProbe(hash)
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("egress fetch did not start")
	}

	// Remove the exact entry captured by the worker and recreate the same hash
	// while its network request is still in flight.
	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("recreated entry not found")
	}
	if newEntry == oldEntry {
		t.Fatal("node recreation reused the old entry")
	}

	close(releaseFetch)
	select {
	case <-fetchDone:
	case <-time.After(time.Second):
		t.Fatal("egress fetch did not finish")
	}
	mgr.Stop()

	if newEntry.CircuitOpenSince.Load() == 0 {
		t.Fatal("stale probe cleared the recreated entry circuit")
	}
	if got := newEntry.GetEgressIP(); got.IsValid() {
		t.Fatalf("stale probe wrote egress IP %v to recreated entry", got)
	}
	if newEntry.HasLatency() {
		t.Fatal("stale probe wrote latency to recreated entry")
	}
}

func TestProbeLatency_DoesNotWriteRecreatedEntry(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"latency-recreated-entry"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)

	fetchStarted := make(chan struct{})
	fetchDone := make(chan struct{})
	releaseFetch := make(chan struct{})
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchStarted)
			<-releaseFetch
			close(fetchDone)
			return []byte("OK"), 42 * time.Millisecond, nil
		},
	})
	mgr.Start()

	mgr.TriggerImmediateLatencyProbe(hash)
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("latency fetch did not start")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("recreated entry not found")
	}
	if newEntry == oldEntry {
		t.Fatal("node recreation reused the old entry")
	}

	close(releaseFetch)
	select {
	case <-fetchDone:
	case <-time.After(time.Second):
		t.Fatal("latency fetch did not finish")
	}
	mgr.Stop()

	if newEntry.CircuitOpenSince.Load() == 0 {
		t.Fatal("stale latency probe cleared the recreated entry circuit")
	}
	if newEntry.HasLatency() {
		t.Fatal("stale latency probe wrote latency to recreated entry")
	}
}

func TestProbeEgressSyncForEntryFetcherUsesCapturedEntry(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"probe-fetch-entry"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)

	fetchEntered := make(chan struct{})
	allowResolve := make(chan struct{})
	selectedEntry := make(chan *node.NodeEntry, 1)
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, entry *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchEntered)
			<-allowResolve
			selectedEntry <- entry
			return []byte("fl=123\nip=203.0.113.1\nloc=US"), 42 * time.Millisecond, nil
		},
	})

	resultCh := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeEgressSyncForEntry(hash, oldEntry)
		resultCh <- err
	}()
	select {
	case <-fetchEntered:
	case <-time.After(time.Second):
		t.Fatal("egress probe did not enter fetch")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	storeOutbound(newEntry)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowResolve)
	var gotEntry *node.NodeEntry
	select {
	case gotEntry = <-selectedEntry:
	case <-time.After(time.Second):
		t.Fatal("fetcher did not select an entry")
	}
	if gotEntry != oldEntry {
		t.Fatalf("fetcher used replacement entry %p, want captured entry %p", gotEntry, oldEntry)
	}
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("probe returned a result after node generation changed")
		}
	case <-time.After(time.Second):
		t.Fatal("egress probe did not finish")
	}
	mgr.Stop()
}

func TestProbeLatencySyncForEntryFetcherUsesCapturedEntry(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"probe-fetch-entry-latency"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)

	fetchEntered := make(chan struct{})
	allowResolve := make(chan struct{})
	selectedEntry := make(chan *node.NodeEntry, 1)
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, entry *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchEntered)
			<-allowResolve
			selectedEntry <- entry
			return []byte("OK"), 42 * time.Millisecond, nil
		},
	})

	resultCh := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeLatencySyncForEntry(hash, oldEntry)
		resultCh <- err
	}()
	select {
	case <-fetchEntered:
	case <-time.After(time.Second):
		t.Fatal("latency probe did not enter fetch")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	storeOutbound(newEntry)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	close(allowResolve)
	var gotEntry *node.NodeEntry
	select {
	case gotEntry = <-selectedEntry:
	case <-time.After(time.Second):
		t.Fatal("fetcher did not select an entry")
	}
	if gotEntry != oldEntry {
		t.Fatalf("fetcher used replacement entry %p, want captured entry %p", gotEntry, oldEntry)
	}
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("probe returned a result after node generation changed")
		}
	case <-time.After(time.Second):
		t.Fatal("latency probe did not finish")
	}
	mgr.Stop()
}

func TestProbeEgressSyncForEntryRejectsGenerationChangedAfterHealthWrite(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"probe-egress-post-health-generation"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)
	oldEntry.FailureCount.Store(1)

	recorded := make(chan struct{})
	allowPostProcessing := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(allowPostProcessing) })
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			return []byte("ip=203.0.113.91\nloc=US"), 42 * time.Millisecond, nil
		},
	})
	mgr.afterProbeResultRecordHook = func() {
		close(recorded)
		<-allowPostProcessing
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeEgressSyncForEntry(hash, oldEntry)
		resultCh <- err
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("egress probe did not reach the post-health-write seam")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	storeOutbound(newEntry)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	releaseOnce.Do(func() { close(allowPostProcessing) })
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("egress probe returned an old-generation result after replacement")
		}
		if !errors.Is(err, errProbeEntryNotLive) {
			t.Fatalf("egress probe error = %v, want %v", err, errProbeEntryNotLive)
		}
	case <-time.After(time.Second):
		t.Fatal("egress probe did not finish after releasing the seam")
	}
	mgr.Stop()
	if got := newEntry.GetEgressIP(); got.IsValid() {
		t.Fatalf("stale egress probe polluted replacement entry: %v", got)
	}
	if newEntry.HasLatency() {
		t.Fatal("stale egress probe polluted replacement latency")
	}
}

func TestProbeLatencySyncForEntryRejectsGenerationChangedAfterHealthWrite(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"probe-latency-post-health-generation"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	oldEntry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("old entry not found")
	}
	storeOutbound(oldEntry)
	oldEntry.FailureCount.Store(1)

	recorded := make(chan struct{})
	allowPostProcessing := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(allowPostProcessing) })
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			return []byte("OK"), 42 * time.Millisecond, nil
		},
	})
	mgr.afterProbeResultRecordHook = func() {
		close(recorded)
		<-allowPostProcessing
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeLatencySyncForEntry(hash, oldEntry)
		resultCh <- err
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("latency probe did not reach the post-health-write seam")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	newEntry, ok := pool.GetEntry(hash)
	if !ok || newEntry == oldEntry {
		t.Fatal("same-hash node was not recreated with a new entry")
	}
	storeOutbound(newEntry)
	if !pool.RecordResultForEntry(hash, newEntry, true) {
		t.Fatal("new entry health setup was rejected")
	}

	releaseOnce.Do(func() { close(allowPostProcessing) })
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("latency probe returned an old-generation result after replacement")
		}
		if !errors.Is(err, errProbeEntryNotLive) {
			t.Fatalf("latency probe error = %v, want %v", err, errProbeEntryNotLive)
		}
	case <-time.After(time.Second):
		t.Fatal("latency probe did not finish after releasing the seam")
	}
	mgr.Stop()
	if newEntry.HasLatency() {
		t.Fatal("stale latency probe polluted replacement entry")
	}
}

// TestProbeLatency_Success verifies latency probe writes RecordResult+RecordLatency.
func TestProbeLatency_Success(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"latency-ok"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"latency-ok"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return []byte("OK"), 50 * time.Millisecond, nil
		},
	})

	mgr.probeLatency(hash, entry, "https://www.gstatic.com/generate_204")

	if entry.FailureCount.Load() != 0 {
		t.Fatalf("expected 0 failures, got %d", entry.FailureCount.Load())
	}

	// Should have latency recorded.
	if !entry.HasLatency() {
		t.Fatal("expected latency data")
	}
}

// TestProbeLatency_Failure verifies latency probe failure calls RecordResult(false).
func TestProbeLatency_Failure(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"latency-fail"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"latency-fail"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return nil, 0, errors.New("tls handshake failed")
		},
	})

	mgr.probeLatency(hash, entry, "https://www.gstatic.com/generate_204")

	if entry.FailureCount.Load() != 1 {
		t.Fatalf("expected 1 failure, got %d", entry.FailureCount.Load())
	}
}

// TestProbeEgress_ZeroLatencyIgnored verifies that a successful probe with a
// non-positive latency sample does not write latency stats.
func TestProbeEgress_ZeroLatencyIgnored(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"egress-zero-latency"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"egress-zero-latency"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	traceBody := []byte("fl=123\nip=203.0.113.1\nts=1234567890")
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return traceBody, 0, nil
		},
	})

	mgr.probeEgress(hash, entry)

	if entry.FailureCount.Load() != 0 {
		t.Fatalf("expected 0 failures, got %d", entry.FailureCount.Load())
	}
	if entry.HasLatency() {
		t.Fatal("zero latency sample should be ignored")
	}
	if got := entry.GetEgressIP(); got != netip.MustParseAddr("203.0.113.1") {
		t.Fatalf("egress IP: got %v, want 203.0.113.1", got)
	}
}

func TestProbeEgress_WithoutLoc_ClearsStoredRegion(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"egress-clear-loc"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"egress-clear-loc"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)
	entry.SetEgressRegion("jp")

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return []byte("ip=203.0.113.1"), 10 * time.Millisecond, nil
		},
	})

	mgr.probeEgress(hash, entry)

	if got := entry.GetEgressRegion(); got != "" {
		t.Fatalf("egress region: got %q, want empty", got)
	}
}

// TestProbeLatency_ZeroLatencyIgnored verifies that successful latency probes
// skip latency writeback when sample <= 0.
func TestProbeLatency_ZeroLatencyIgnored(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"latency-zero"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"latency-zero"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return []byte("ok"), 0, nil
		},
	})

	mgr.probeLatency(hash, entry, "https://www.gstatic.com/generate_204")

	if entry.FailureCount.Load() != 0 {
		t.Fatalf("expected 0 failures, got %d", entry.FailureCount.Load())
	}
	if entry.HasLatency() {
		t.Fatal("zero latency sample should be ignored")
	}
}

// TestProbeEgress_NilFetcher verifies graceful handling of nil fetcher.
func TestProbeEgress_NilFetcher(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"nil-fetcher"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"nil-fetcher"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}

	mgr := NewProbeManager(ProbeConfig{Pool: pool}) // no Fetcher
	mgr.probeEgress(hash, entry)                    // should not panic
}

// TestTriggerImmediateEgressProbe_WithFetcher is an integration test
// verifying async probe + health writeback.
func TestTriggerImmediateEgressProbe_WithFetcher(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"trigger-egress"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"trigger-egress"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var called sync.WaitGroup
	called.Add(1)
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			defer called.Done()
			return []byte("ip=198.51.100.1"), 10 * time.Millisecond, nil
		},
	})
	mgr.Start()
	defer mgr.Stop()

	mgr.TriggerImmediateEgressProbe(hash)
	called.Wait()

	// Allow goroutines to complete.
	time.Sleep(20 * time.Millisecond)

	got := entry.GetEgressIP()
	if got != netip.MustParseAddr("198.51.100.1") {
		t.Fatalf("egress IP: got %v, want 198.51.100.1", got)
	}
}

func TestTriggerImmediateEgressProbeForEntryRejectsRecreatedHash(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := []byte(`{"type":"probe-generation"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	entryA, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry A not found")
	}
	storeOutbound(entryA)

	var calls atomic.Int32
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("ip=198.51.100.10"), time.Millisecond, nil
		},
	})
	if !mgr.enqueueProbe(hash, probeTaskKindEgress, probePriorityNormal, entryA) {
		t.Fatal("exact-generation probe was not queued")
	}

	pool.RemoveNodeFromSub(hash, "sub1")
	pool.AddNodeFromSub(hash, raw, "sub1")
	entryB, ok := pool.GetEntry(hash)
	if !ok || entryB == entryA {
		t.Fatal("same-hash replacement did not create entry B")
	}
	storeOutbound(entryB)

	task, ok := mgr.taskQueue.Dequeue()
	if !ok {
		t.Fatal("queued exact-generation task was not dequeued")
	}
	state, ok := mgr.markTaskRunning(task.key)
	if !ok {
		t.Fatal("queued exact-generation task state was lost")
	}
	mgr.executeTask(task)
	mgr.finishTask(task.key, state)
	if got := calls.Load(); got != 0 {
		t.Fatalf("stale task probed replacement generation: %d calls", got)
	}
}

func TestTriggerImmediateLatencyProbe_WithFetcher(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub1", "sub1", "url", true, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"trigger-latency"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"trigger-latency"}`), "sub1")
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var called sync.WaitGroup
	called.Add(1)
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			defer called.Done()
			return []byte("ok"), 25 * time.Millisecond, nil
		},
	})
	mgr.Start()
	defer mgr.Stop()

	mgr.TriggerImmediateLatencyProbe(hash)
	called.Wait()
	time.Sleep(20 * time.Millisecond)

	if !entry.HasLatency() {
		t.Fatal("expected latency data after immediate latency probe")
	}
}

func TestScanEgress_SkipsDisabledNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub1", "sub1", "url", false, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"scan-egress-disabled"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"scan-egress-disabled"}`), "sub1")
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var calls atomic.Int32
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("ip=198.51.100.1"), 10 * time.Millisecond, nil
		},
	})

	mgr.scanEgress()
	time.Sleep(30 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("disabled node should be skipped by scanEgress, calls=%d", got)
	}
}

func TestScanLatency_SkipsDisabledNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub1", "sub1", "url", false, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"scan-latency-disabled"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"scan-latency-disabled"}`), "sub1")
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var calls atomic.Int32
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("ok"), 15 * time.Millisecond, nil
		},
	})

	mgr.scanLatency()
	time.Sleep(30 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("disabled node should be skipped by scanLatency, calls=%d", got)
	}
}

func TestQueuedAsyncProbe_SkipsNodeDisabledBeforeExecution(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub1", "sub1", "url", true, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"queued-disabled-before-execution"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"queued-disabled-before-execution"}`), "sub1")
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"tag"}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var calls atomic.Int32
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("ip=198.51.100.1"), 10 * time.Millisecond, nil
		},
	})
	defer mgr.Stop()

	if ok := mgr.enqueueProbe(hash, probeTaskKindEgress, probePriorityNormal); !ok {
		t.Fatal("enqueue should succeed")
	}

	sub.SetEnabled(false)
	mgr.Start()
	time.Sleep(50 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected disabled queued node to be skipped, calls=%d", got)
	}
}

func TestProbeManager_StopWaitsImmediateProbe(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"stop-immediate"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"stop-immediate"}`), "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			return []byte("ip=203.0.113.1"), 10 * time.Millisecond, nil
		},
	})
	mgr.Start()

	mgr.TriggerImmediateEgressProbe(hash)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("immediate probe did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop returned before in-flight immediate probe finished")
	case <-time.After(30 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete after in-flight immediate probe finished")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 probe call, got %d", got)
	}
}

func TestProbeQueue_DequeueChoosesNormalWhenSelectorRequests(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hashHigh := node.HashFromRawOptions([]byte(`{"type":"queue-high"}`))
	hashNormal := node.HashFromRawOptions([]byte(`{"type":"queue-normal"}`))
	pool.AddNodeFromSub(hashHigh, []byte(`{"type":"queue-high"}`), "sub1")
	pool.AddNodeFromSub(hashNormal, []byte(`{"type":"queue-normal"}`), "sub1")

	entryHigh, ok := pool.GetEntry(hashHigh)
	if !ok {
		t.Fatal("high entry not found")
	}
	storeOutbound(entryHigh)
	entryNormal, ok := pool.GetEntry(hashNormal)
	if !ok {
		t.Fatal("normal entry not found")
	}
	storeOutbound(entryNormal)

	order := make(chan node.Hash, 2)
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		ChooseNormalWhenBoth: func() bool {
			return true
		},
		Fetcher: func(_ context.Context, entry *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			order <- entry.Hash
			return []byte("ip=198.51.100.20"), 10 * time.Millisecond, nil
		},
	})
	defer mgr.Stop()

	if ok := mgr.enqueueProbe(hashHigh, probeTaskKindEgress, probePriorityHigh); !ok {
		t.Fatal("enqueue high should succeed")
	}
	if ok := mgr.enqueueProbe(hashNormal, probeTaskKindEgress, probePriorityNormal); !ok {
		t.Fatal("enqueue normal should succeed")
	}

	mgr.Start()

	select {
	case got := <-order:
		if got != hashNormal {
			t.Fatalf("first dequeued hash = %s, want normal %s", got.Hex(), hashNormal.Hex())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first dequeued task")
	}
}

func TestProbeQueue_HighUpgradeOfQueuedNormalRunsFirstWithoutExtraRun(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hashOther := node.HashFromRawOptions([]byte(`{"type":"queue-upgrade-other"}`))
	hashTarget := node.HashFromRawOptions([]byte(`{"type":"queue-upgrade-target"}`))
	pool.AddNodeFromSub(hashOther, []byte(`{"type":"queue-upgrade-other"}`), "sub1")
	pool.AddNodeFromSub(hashTarget, []byte(`{"type":"queue-upgrade-target"}`), "sub1")

	entryOther, ok := pool.GetEntry(hashOther)
	if !ok {
		t.Fatal("other entry not found")
	}
	storeOutbound(entryOther)
	entryTarget, ok := pool.GetEntry(hashTarget)
	if !ok {
		t.Fatal("target entry not found")
	}
	storeOutbound(entryTarget)

	order := make(chan node.Hash, 3)
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		ChooseNormalWhenBoth: func() bool {
			return false
		},
		Fetcher: func(_ context.Context, entry *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			order <- entry.Hash
			return []byte("ip=198.51.100.21"), 10 * time.Millisecond, nil
		},
	})
	defer mgr.Stop()

	if ok := mgr.enqueueProbe(hashOther, probeTaskKindEgress, probePriorityNormal); !ok {
		t.Fatal("enqueue other normal should succeed")
	}
	if ok := mgr.enqueueProbe(hashTarget, probeTaskKindEgress, probePriorityNormal); !ok {
		t.Fatal("enqueue target normal should succeed")
	}
	if ok := mgr.enqueueProbe(hashTarget, probeTaskKindEgress, probePriorityHigh); !ok {
		t.Fatal("enqueue target high upgrade should add a high-priority token")
	}

	mgr.Start()

	select {
	case got := <-order:
		if got != hashTarget {
			t.Fatalf("first executed hash = %s, want upgraded target %s", got.Hex(), hashTarget.Hex())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upgraded task")
	}

	select {
	case got := <-order:
		if got != hashOther {
			t.Fatalf("second executed hash = %s, want other %s", got.Hex(), hashOther.Hex())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second executed task")
	}

	select {
	case got := <-order:
		t.Fatalf("unexpected extra probe execution for %s", got.Hex())
	case <-time.After(200 * time.Millisecond):
	}
}

func TestProbeQueue_FullDropsWithoutBlocking(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash1 := node.HashFromRawOptions([]byte(`{"type":"queue-full-1"}`))
	hash2 := node.HashFromRawOptions([]byte(`{"type":"queue-full-2"}`))
	pool.AddNodeFromSub(hash1, []byte(`{"type":"queue-full-1"}`), "sub1")
	pool.AddNodeFromSub(hash2, []byte(`{"type":"queue-full-2"}`), "sub1")

	mgr := NewProbeManager(ProbeConfig{
		Pool:          pool,
		Concurrency:   1,
		QueueCapacity: 1,
	})

	if ok := mgr.enqueueProbe(hash1, probeTaskKindEgress, probePriorityNormal); !ok {
		t.Fatal("first enqueue should succeed")
	}
	if ok := mgr.enqueueProbe(hash2, probeTaskKindEgress, probePriorityNormal); ok {
		t.Fatal("second enqueue should be dropped when queue is full")
	}
}

func TestProbeManager_StopDropsPendingTaskState(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	hash := node.HashFromRawOptions([]byte(`{"type":"stop-drops-task-state"}`))

	mgr := NewProbeManager(ProbeConfig{Pool: pool})
	key := probeTaskKey{hash: hash, kind: probeTaskKindEgress}
	if !mgr.enqueueProbe(hash, probeTaskKindEgress, probePriorityNormal) {
		t.Fatal("pending probe enqueue should succeed")
	}
	if _, ok := mgr.taskStates.Load(key); !ok {
		t.Fatal("pending probe should have task state before Stop")
	}

	mgr.Stop()

	if _, ok := mgr.taskStates.Load(key); ok {
		t.Fatal("Stop retained state for a dropped pending probe")
	}
}

func TestProbeSync_BypassesAsyncWorkerLimit(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hashAsync := node.HashFromRawOptions([]byte(`{"type":"sync-bypass-async"}`))
	hashSync := node.HashFromRawOptions([]byte(`{"type":"sync-bypass-sync"}`))
	pool.AddNodeFromSub(hashAsync, []byte(`{"type":"sync-bypass-async"}`), "sub1")
	pool.AddNodeFromSub(hashSync, []byte(`{"type":"sync-bypass-sync"}`), "sub1")

	entryAsync, ok := pool.GetEntry(hashAsync)
	if !ok {
		t.Fatal("async entry not found")
	}
	storeOutbound(entryAsync)
	entrySync, ok := pool.GetEntry(hashSync)
	if !ok {
		t.Fatal("sync entry not found")
	}
	storeOutbound(entrySync)

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		Fetcher: func(_ context.Context, entry *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			if entry.Hash == hashAsync {
				startedOnce.Do(func() { close(started) })
				<-release
				return []byte("ip=198.51.100.31"), 20 * time.Millisecond, nil
			}
			return []byte("ip=198.51.100.32"), 5 * time.Millisecond, nil
		},
	})
	mgr.Start()
	defer mgr.Stop()

	mgr.TriggerImmediateEgressProbe(hashAsync)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("async worker did not start")
	}

	done := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeEgressSync(hashSync)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProbeEgressSync error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ProbeEgressSync blocked by async worker limit")
	}

	close(release)
}

func TestProbeManager_StopWaitsForSynchronousProbe(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	hash := node.HashFromRawOptions([]byte(`{"type":"sync-stop"}`))
	raw := []byte(`{"type":"sync-stop"}`)
	pool.AddNodeFromSub(hash, raw, "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFetch) })
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchStarted)
			<-releaseFetch
			return []byte("ip=203.0.113.10"), 10 * time.Millisecond, nil
		},
	})

	probeDone := make(chan error, 1)
	go func() {
		_, err := mgr.ProbeEgressSync(hash)
		probeDone <- err
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("synchronous probe did not enter Fetcher")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Stop returned while synchronous probe was in flight")
	case <-time.After(30 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseFetch) })
	select {
	case err := <-probeDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("synchronous probe failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous probe did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after synchronous probe completed")
	}
}

func TestProbeManager_StopDoesNotOutliveShutdownDeadlineForProductionFetcher(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	hash := node.HashFromRawOptions([]byte(`{"type":"probe-long-timeout"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"probe-long-timeout"}`), "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFetch) })
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		// This models the production app fetcher: its timeout is owned by the
		// fetcher and is deliberately much longer than the shutdown deadline.
		Fetcher: func(ctx context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			close(fetchStarted)
			select {
			case <-releaseFetch:
				return nil, 0, errors.New("released")
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil, 0, errors.New("probe timeout")
			}
		},
	})

	probeDone := make(chan struct{})
	go func() {
		_, _ = mgr.ProbeEgressSync(hash)
		close(probeDone)
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("synchronous probe did not enter Fetcher")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	shutdownDeadline := time.NewTimer(50 * time.Millisecond)
	defer shutdownDeadline.Stop()
	select {
	case <-stopDone:
	case <-shutdownDeadline.C:
		t.Fatal("Stop exceeded the application shutdown deadline while production fetcher was in flight")
	}
	select {
	case <-probeDone:
	case <-time.After(time.Second):
		t.Fatal("synchronous probe did not finish after Stop canceled its context")
	}
}

func TestProbeManager_StopCancellationDoesNotRecordNodeFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		async bool
		kind  string
	}{
		{name: "sync-egress", kind: "egress"},
		{name: "async-egress", async: true, kind: "egress"},
		{name: "sync-latency", kind: "latency"},
		{name: "async-latency", async: true, kind: "latency"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := topology.NewGlobalNodePool(topology.PoolConfig{
				MaxLatencyTableEntries: 16,
				MaxConsecutiveFailures: func() int { return 3 },
			})
			raw := []byte(`{"type":"probe-cancel-health"}`)
			hash := node.HashFromRawOptions(raw)
			pool.AddNodeFromSub(hash, raw, "sub1")
			entry, ok := pool.GetEntry(hash)
			if !ok {
				t.Fatal("entry not found")
			}
			storeOutbound(entry)
			entry.FailureCount.Store(1)
			entry.CircuitOpenSince.Store(0)
			entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
			entry.SetEgressRegion("us")
			beforeStats := node.DomainLatencyStats{
				Ewma:        80 * time.Millisecond,
				LastUpdated: time.Unix(100, 0),
			}
			entry.LatencyTable.LoadEntry("cloudflare.com", beforeStats)
			entry.LatencyTable.LoadEntry("gstatic.com", beforeStats)

			fetchStarted := make(chan struct{})
			mgr := NewProbeManager(ProbeConfig{
				Pool: pool,
				Fetcher: func(ctx context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
					close(fetchStarted)
					<-ctx.Done()
					// Return a transport-level cancellation error rather than ctx.Err().
					// The manager must use the parent context state to distinguish
					// shutdown cancellation from a real fetch failure.
					return nil, 0, errors.New("fetcher observed manager cancellation")
				},
				LatencyTestURL: func() string {
					return "https://www.gstatic.com/generate_204"
				},
			})

			probeDone := make(chan error, 1)
			if tc.async {
				mgr.Start()
				if tc.kind == "egress" {
					mgr.TriggerImmediateEgressProbe(hash)
				} else {
					mgr.TriggerImmediateLatencyProbe(hash)
				}
			} else {
				go func() {
					var err error
					if tc.kind == "egress" {
						_, err = mgr.ProbeEgressSync(hash)
					} else {
						_, err = mgr.ProbeLatencySync(hash)
					}
					probeDone <- err
				}()
			}

			select {
			case <-fetchStarted:
			case <-time.After(time.Second):
				t.Fatal("probe did not enter Fetcher")
			}
			mgr.Stop()
			if !tc.async {
				select {
				case <-probeDone:
				case <-time.After(time.Second):
					t.Fatal("synchronous probe did not finish after Stop")
				}
			}

			if got := entry.FailureCount.Load(); got != 1 {
				t.Fatalf("failure count after manager cancellation: got %d, want 1", got)
			}
			if got := entry.CircuitOpenSince.Load(); got != 0 {
				t.Fatalf("circuit state after manager cancellation: got %d, want closed", got)
			}
			if got := entry.GetEgressIP(); got != netip.MustParseAddr("203.0.113.10") {
				t.Fatalf("egress IP after manager cancellation: got %v, want 203.0.113.10", got)
			}
			if got := entry.GetEgressRegion(); got != "us" {
				t.Fatalf("egress region after manager cancellation: got %q, want us", got)
			}
			for _, domain := range []string{"cloudflare.com", "gstatic.com"} {
				got, ok := entry.LatencyTable.GetDomainStats(domain)
				if !ok {
					t.Fatalf("latency for %s disappeared after manager cancellation", domain)
				}
				if got != beforeStats {
					t.Fatalf("latency for %s after manager cancellation: got %+v, want %+v", domain, got, beforeStats)
				}
			}
		})
	}
}

func TestProbeManager_StopCancellationAfterHealthCommitSkipsPostProcessing(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
	}{
		{name: "egress", kind: "egress"},
		{name: "latency", kind: "latency"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := topology.NewGlobalNodePool(topology.PoolConfig{
				MaxLatencyTableEntries: 16,
				MaxConsecutiveFailures: func() int { return 3 },
			})
			raw := []byte(`{"type":"probe-stop-after-health"}`)
			hash := node.HashFromRawOptions(raw)
			pool.AddNodeFromSub(hash, raw, "sub1")
			entry, ok := pool.GetEntry(hash)
			if !ok {
				t.Fatal("entry not found")
			}
			storeOutbound(entry)
			entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
			entry.SetEgressRegion("us")
			beforeStats := node.DomainLatencyStats{
				Ewma:        80 * time.Millisecond,
				LastUpdated: time.Unix(100, 0),
			}
			domain := "gstatic.com"
			if tc.kind == "egress" {
				domain = egressTraceDomain
			}
			entry.LatencyTable.LoadEntry(domain, beforeStats)

			probeCtx, cancelProbe := context.WithCancel(context.Background())
			cancelObserved := make(chan struct{})
			var cancelOnce sync.Once
			mgr := NewProbeManager(ProbeConfig{
				Pool: pool,
				Fetcher: func(context.Context, *node.NodeEntry, string) ([]byte, time.Duration, error) {
					if tc.kind == "egress" {
						return []byte("ip=203.0.113.91\nloc=JP"), 42 * time.Millisecond, nil
					}
					return []byte("OK"), 42 * time.Millisecond, nil
				},
				LatencyTestURL: func() string {
					return "https://www.gstatic.com/generate_204"
				},
			})
			mgr.probeCtx = probeCtx
			mgr.probeCancel = func() {
				cancelProbe()
				cancelOnce.Do(func() { close(cancelObserved) })
			}

			healthCommitted := make(chan struct{})
			allowPostProcessing := make(chan struct{})
			mgr.afterProbeResultRecordHook = func() {
				close(healthCommitted)
				<-allowPostProcessing
			}

			mgr.Start()
			if tc.kind == "egress" {
				mgr.TriggerImmediateEgressProbe(hash)
			} else {
				mgr.TriggerImmediateLatencyProbe(hash)
			}
			select {
			case <-healthCommitted:
			case <-time.After(time.Second):
				t.Fatal("probe did not reach post-health-write seam")
			}

			stopDone := make(chan struct{})
			go func() {
				mgr.Stop()
				close(stopDone)
			}()
			select {
			case <-cancelObserved:
			case <-time.After(time.Second):
				t.Fatal("Stop did not cancel the probe owner")
			}
			select {
			case <-stopDone:
				t.Fatal("Stop returned before the post-processing seam was released")
			default:
			}

			close(allowPostProcessing)
			select {
			case <-stopDone:
			case <-time.After(time.Second):
				t.Fatal("Stop did not finish after post-processing was released")
			}

			if got := entry.GetEgressIP(); got != netip.MustParseAddr("203.0.113.10") {
				t.Fatalf("egress IP after cancellation: got %v, want 203.0.113.10", got)
			}
			if got := entry.GetEgressRegion(); got != "us" {
				t.Fatalf("egress region after cancellation: got %q, want us", got)
			}
			got, ok := entry.LatencyTable.GetDomainStats(domain)
			if !ok {
				t.Fatalf("latency for %s disappeared after cancellation", domain)
			}
			if got != beforeStats {
				t.Fatalf("latency for %s after cancellation: got %+v, want %+v", domain, got, beforeStats)
			}
		})
	}
}

func TestProbeManager_ChildFetcherCancellationStillRecordsFailure(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := []byte(`{"type":"probe-child-cancel"}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)
	entry.FailureCount.Store(0)
	entry.CircuitOpenSince.Store(0)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(ctx context.Context, _ *node.NodeEntry, _ string) ([]byte, time.Duration, error) {
			child, cancel := context.WithDeadline(ctx, time.Unix(0, 0))
			defer cancel()
			return nil, 0, child.Err()
		},
	})
	mgr.probeEgress(hash, entry)

	if got := entry.FailureCount.Load(); got != 1 {
		t.Fatalf("child fetcher cancellation failure count: got %d, want 1", got)
	}
	if got := entry.CircuitOpenSince.Load(); got != 0 {
		t.Fatalf("child fetcher cancellation opened circuit: got %d", got)
	}
}

func TestProbeLatencySync_ReturnsEWMAFromNormalizedDomain(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"latency-sync"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"latency-sync"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			return []byte("ok"), 80 * time.Millisecond, nil
		},
		LatencyTestURL: func() string {
			return "https://www.gstatic.com/generate_204"
		},
	})

	result, err := mgr.ProbeLatencySync(hash)
	if err != nil {
		t.Fatalf("ProbeLatencySync: %v", err)
	}
	if result.LatencyEwmaMs <= 0 {
		t.Fatalf("latency_ewma_ms = %f, want > 0", result.LatencyEwmaMs)
	}

	stats, ok := entry.LatencyTable.GetDomainStats("gstatic.com")
	if !ok {
		t.Fatal("expected normalized domain latency entry for gstatic.com")
	}
	if stats.Ewma != 80*time.Millisecond {
		t.Fatalf("stored EWMA = %v, want %v", stats.Ewma, 80*time.Millisecond)
	}
}

func TestProbeSync_EmitsProbeEvents(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	hash := node.HashFromRawOptions([]byte(`{"type":"probe-sync-events"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"probe-sync-events"}`), "sub1")

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var gotKinds []string
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ context.Context, _ *node.NodeEntry, url string) ([]byte, time.Duration, error) {
			switch url {
			case egressTraceURL:
				return []byte("ip=198.51.100.10"), 20 * time.Millisecond, nil
			default:
				return []byte("ok"), 30 * time.Millisecond, nil
			}
		},
		LatencyTestURL: func() string {
			return "https://www.gstatic.com/generate_204"
		},
		OnProbeEvent: func(kind string) {
			gotKinds = append(gotKinds, kind)
		},
	})

	if _, err := mgr.ProbeEgressSync(hash); err != nil {
		t.Fatalf("ProbeEgressSync: %v", err)
	}
	if _, err := mgr.ProbeLatencySync(hash); err != nil {
		t.Fatalf("ProbeLatencySync: %v", err)
	}

	wantKinds := []string{"egress", "latency"}
	if len(gotKinds) != len(wantKinds) {
		t.Fatalf("probe event count: got %d, want %d (kinds=%v)", len(gotKinds), len(wantKinds), gotKinds)
	}
	for i := range wantKinds {
		if gotKinds[i] != wantKinds[i] {
			t.Fatalf("probe event kind[%d]: got %q, want %q", i, gotKinds[i], wantKinds[i])
		}
	}
}

func TestIsLatencyProbeDue_UsesAttemptTimestamps(t *testing.T) {
	mgr := NewProbeManager(ProbeConfig{})
	hash := node.HashFromRawOptions([]byte(`{"type":"due-check"}`))
	entry := node.NewNodeEntry(hash, []byte(`{"type":"due-check"}`), time.Now(), 16)
	now := time.Now()

	// Seed a very recent latency-table sample; due-check should ignore this and
	// rely on attempt timestamps.
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        20 * time.Millisecond,
		LastUpdated: now,
	})

	entry.LastLatencyProbeAttempt.Store(now.Add(-10 * time.Minute).UnixNano())
	entry.LastAuthorityLatencyProbeAttempt.Store(now.Add(-10 * time.Minute).UnixNano())
	if !mgr.isLatencyProbeDue(entry, now, 5*time.Minute, 1*time.Hour, []string{"example.com"}, 15*time.Second) {
		t.Fatal("expected due=true when last latency attempt is stale")
	}

	entry.LastLatencyProbeAttempt.Store(now.Add(-1 * time.Minute).UnixNano())
	entry.LastAuthorityLatencyProbeAttempt.Store(now.Add(-2 * time.Hour).UnixNano())
	if !mgr.isLatencyProbeDue(entry, now, 5*time.Minute, 1*time.Hour, []string{"example.com"}, 15*time.Second) {
		t.Fatal("expected due=true when authority attempt is stale")
	}
}

// TestParseCloudflareTrace_Success verifies IP extraction from trace body.
func TestParseCloudflareTrace_Success(t *testing.T) {
	body := []byte("fl=abc\nip=1.2.3.4\nloc=US\nts=12345")
	addr, loc, err := ParseCloudflareTrace(body)
	if err != nil {
		t.Fatal(err)
	}
	if addr != netip.MustParseAddr("1.2.3.4") {
		t.Fatalf("got %v, want 1.2.3.4", addr)
	}
	if loc == nil || *loc != "us" {
		t.Fatalf("loc: got %v, want %q", loc, "us")
	}
}

func TestParseCloudflareTrace_WithoutLoc(t *testing.T) {
	body := []byte("fl=abc\nip=1.2.3.4\nts=12345")
	addr, loc, err := ParseCloudflareTrace(body)
	if err != nil {
		t.Fatal(err)
	}
	if addr != netip.MustParseAddr("1.2.3.4") {
		t.Fatalf("got %v, want 1.2.3.4", addr)
	}
	if loc != nil {
		t.Fatalf("loc: got %v, want nil", loc)
	}
}

// TestParseCloudflareTrace_NoIP verifies error when ip= field is missing.
func TestParseCloudflareTrace_NoIP(t *testing.T) {
	body := []byte("fl=abc\nts=12345")
	_, _, err := ParseCloudflareTrace(body)
	if err == nil {
		t.Fatal("expected error when ip field is missing")
	}
}
