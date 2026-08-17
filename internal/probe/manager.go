package probe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/puzpuzpuz/xsync/v4"
)

// Fetcher executes an HTTP request through the exact node entry accepted by
// the probe manager, returning response body and TLS handshake latency. The
// context is owned by the probe manager and is canceled before Stop waits for
// in-flight probes. Fetchers must not resolve a replacement by hash.
type Fetcher func(ctx context.Context, entry *node.NodeEntry, url string) (body []byte, latency time.Duration, err error)

// ProbeConfig configures the ProbeManager.
// Field names align 1:1 with RuntimeConfig to prevent mis-wiring.
type ProbeConfig struct {
	Pool        *topology.GlobalNodePool
	Concurrency int // number of async probe workers
	// QueueCapacity is the per-priority async queue capacity.
	// If <= 0, defaults to max(1024, Concurrency*4).
	QueueCapacity int

	// Fetcher executes HTTP via node hash. Injectable for testing.
	Fetcher Fetcher

	// Interval thresholds — closures for hot-reload from RuntimeConfig.
	MaxEgressTestInterval           func() time.Duration
	MaxLatencyTestInterval          func() time.Duration
	MaxAuthorityLatencyTestInterval func() time.Duration

	LatencyTestURL     func() string
	LatencyAuthorities func() []string

	// OnProbeEvent is called after each probe attempt completes (egress or latency).
	// The kind parameter is "egress" or "latency".
	OnProbeEvent func(kind string)

	// ChooseNormalWhenBoth chooses whether to pop normal-priority queue when
	// both high and normal queues are non-empty.
	// Nil defaults to 10% chance.
	ChooseNormalWhenBoth func() bool
}

// ProbeManager schedules and executes active probes against nodes in the pool.
// It holds a direct reference to *topology.GlobalNodePool (no interface).
type ProbeManager struct {
	pool        *topology.GlobalNodePool
	stopCh      chan struct{}
	probeCtx    context.Context
	probeCancel context.CancelFunc
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	fetcher     Fetcher
	workerCount int
	taskQueue   *probeTaskQueue
	taskStates  *xsync.Map[probeTaskKey, *probeTaskState]

	maxEgressTestInterval           func() time.Duration
	maxLatencyTestInterval          func() time.Duration
	maxAuthorityLatencyTestInterval func() time.Duration
	latencyTestURL                  func() string
	latencyAuthorities              func() []string
	onProbeEvent                    func(kind string)
	// Package-private test seam after the exact health result is committed and
	// before probe post-processing writes latency or egress metadata.
	afterProbeResultRecordHook func()
	// Package-private test seam immediately before a periodic scan enqueues a
	// due node. Production leaves it nil.
	beforeScanEnqueueHook func(node.Hash, *node.NodeEntry)
}

const (
	egressTraceURL        = "https://cloudflare.com/cdn-cgi/trace"
	egressTraceDomain     = "cloudflare.com"
	defaultLatencyTestURL = "https://www.gstatic.com/generate_204"
	defaultQueueCap       = 1024
)

var errProbeEntryNotLive = errors.New("probe entry is no longer live")

type probePriority uint8

const (
	probePriorityNormal probePriority = iota
	probePriorityHigh
)

type probeTaskKind uint8

const (
	probeTaskKindEgress probeTaskKind = iota
	probeTaskKindLatency
)

type probeTaskKey struct {
	hash     node.Hash
	kind     probeTaskKind
	expected *node.NodeEntry
}

type probeTask struct {
	key probeTaskKey
}

type probeTaskState struct {
	flags atomic.Uint32
}

const (
	taskFlagQueued uint32 = 1 << iota
	taskFlagRunning
	taskFlagDirty
	taskFlagDirtyHigh
	taskFlagQueuedHigh
)

type probeTaskBuffer struct {
	items []probeTask
	head  int
}

func (b *probeTaskBuffer) len() int {
	return len(b.items) - b.head
}

func (b *probeTaskBuffer) push(task probeTask) {
	b.items = append(b.items, task)
}

func (b *probeTaskBuffer) pop() probeTask {
	task := b.items[b.head]
	b.head++
	if b.head >= len(b.items) {
		b.items = nil
		b.head = 0
		return task
	}
	if b.head > 64 && b.head*2 >= len(b.items) {
		b.items = append([]probeTask(nil), b.items[b.head:]...)
		b.head = 0
	}
	return task
}

func (b *probeTaskBuffer) clear() {
	b.items = nil
	b.head = 0
}

func (b *probeTaskBuffer) drain() []probeTask {
	if b.len() == 0 {
		b.clear()
		return nil
	}
	pending := append([]probeTask(nil), b.items[b.head:]...)
	b.clear()
	return pending
}

type probeTaskQueue struct {
	mu                   sync.Mutex
	notEmpty             *sync.Cond
	high                 probeTaskBuffer
	normal               probeTaskBuffer
	highCap              int
	normalCap            int
	stopped              bool
	chooseNormalWhenBoth func() bool
}

func newProbeTaskQueue(highCap, normalCap int, chooseNormalWhenBoth func() bool) *probeTaskQueue {
	if highCap <= 0 {
		highCap = defaultQueueCap
	}
	if normalCap <= 0 {
		normalCap = defaultQueueCap
	}
	if chooseNormalWhenBoth == nil {
		chooseNormalWhenBoth = func() bool { return rand.IntN(10) == 0 }
	}
	q := &probeTaskQueue{
		highCap:              highCap,
		normalCap:            normalCap,
		chooseNormalWhenBoth: chooseNormalWhenBoth,
	}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *probeTaskQueue) Enqueue(task probeTask, priority probePriority) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}

	switch priority {
	case probePriorityHigh:
		if q.high.len() >= q.highCap {
			return false
		}
		q.high.push(task)
	default:
		if q.normal.len() >= q.normalCap {
			return false
		}
		q.normal.push(task)
	}

	q.notEmpty.Signal()
	return true
}

func (q *probeTaskQueue) Dequeue() (probeTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.stopped {
			return probeTask{}, false
		}

		highLen := q.high.len()
		normalLen := q.normal.len()
		switch {
		case highLen > 0 && normalLen > 0:
			if q.chooseNormalWhenBoth() {
				return q.normal.pop(), true
			}
			return q.high.pop(), true
		case highLen > 0:
			return q.high.pop(), true
		case normalLen > 0:
			return q.normal.pop(), true
		default:
			q.notEmpty.Wait()
		}
	}
}

func (q *probeTaskQueue) StopDropPending() []probeTask {
	q.mu.Lock()
	q.stopped = true
	pending := q.high.drain()
	pending = append(pending, q.normal.drain()...)
	q.mu.Unlock()
	q.notEmpty.Broadcast()
	return pending
}

type egressProbeErrorStage int

const (
	egressProbeNoError egressProbeErrorStage = iota
	egressProbeFetchError
	egressProbeParseError
)

// NewProbeManager creates a new ProbeManager.
func NewProbeManager(cfg ProbeConfig) *ProbeManager {
	conc := cfg.Concurrency
	if conc <= 0 {
		conc = 8
	}
	queueCap := cfg.QueueCapacity
	if queueCap <= 0 {
		queueCap = conc * 4
		if queueCap < defaultQueueCap {
			queueCap = defaultQueueCap
		}
	}

	probeCtx, probeCancel := context.WithCancel(context.Background())
	return &ProbeManager{
		pool:                            cfg.Pool,
		stopCh:                          make(chan struct{}),
		probeCtx:                        probeCtx,
		probeCancel:                     probeCancel,
		fetcher:                         cfg.Fetcher,
		workerCount:                     conc,
		taskQueue:                       newProbeTaskQueue(queueCap, queueCap, cfg.ChooseNormalWhenBoth),
		taskStates:                      xsync.NewMap[probeTaskKey, *probeTaskState](),
		maxEgressTestInterval:           cfg.MaxEgressTestInterval,
		maxLatencyTestInterval:          cfg.MaxLatencyTestInterval,
		maxAuthorityLatencyTestInterval: cfg.MaxAuthorityLatencyTestInterval,
		latencyTestURL:                  cfg.LatencyTestURL,
		latencyAuthorities:              cfg.LatencyAuthorities,
		onProbeEvent:                    cfg.OnProbeEvent,
	}
}

// SetOnProbeEvent sets the probe event callback. Must be called before Start.
func (m *ProbeManager) SetOnProbeEvent(fn func(kind string)) {
	m.onProbeEvent = fn
}

// Start launches the background probe workers.
func (m *ProbeManager) Start() {
	m.lifecycleMu.Lock()
	if m.started || m.stopped {
		m.lifecycleMu.Unlock()
		return
	}
	m.started = true
	m.wg.Add(1)
	m.wg.Add(1)
	m.wg.Add(m.workerCount)
	m.lifecycleMu.Unlock()

	go func() {
		defer m.wg.Done()
		scanloop.Run(m.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, m.scanEgress)
	}()

	go func() {
		defer m.wg.Done()
		scanloop.Run(m.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, m.scanLatency)
	}()

	for i := 0; i < m.workerCount; i++ {
		go func() {
			defer m.wg.Done()
			m.runProbeWorker()
		}()
	}
}

// Stop signals all probe workers to stop and waits for completion.
//
// Design note:
//   - In-flight worker tasks are drained before Stop returns.
//   - Pending queued tasks are dropped on stop.
//   - We intentionally do not reject post-stop triggers via extra manager-global
//     state; expected ownership is that callers stop upstream event sources first.
func (m *ProbeManager) Stop() {
	m.lifecycleMu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopCh)
		if m.probeCancel != nil {
			m.probeCancel()
		}
		for _, task := range m.taskQueue.StopDropPending() {
			m.dropPendingTaskState(task.key)
		}
	}
	m.lifecycleMu.Unlock()
	m.wg.Wait()
}

func (m *ProbeManager) beginSynchronousProbe() bool {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.stopped {
		return false
	}
	m.wg.Add(1)
	return true
}

// contextForSynchronousProbe combines a caller's cancellation with the
// manager lifecycle cancellation. The caller owns only the returned wait;
// ProbeManager.Stop remains able to cancel every admitted synchronous probe.
func (m *ProbeManager) contextForSynchronousProbe(parent context.Context) (context.Context, func()) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if m == nil || m.probeCtx == nil {
		return ctx, cancel
	}
	stopManagerCancel := context.AfterFunc(m.probeCtx, cancel)
	return ctx, func() {
		stopManagerCancel()
		cancel()
	}
}

// TriggerImmediateEgressProbe enqueues an async egress probe for a node.
// Caller returns immediately.
func (m *ProbeManager) TriggerImmediateEgressProbe(hash node.Hash) {
	m.enqueueProbe(hash, probeTaskKindEgress, probePriorityNormal)
}

// TriggerImmediateEgressProbeForEntry enqueues a probe only for the exact
// NodeEntry generation that created the request. A removed-and-recreated hash
// must not inherit the old generation's asynchronous probe.
func (m *ProbeManager) TriggerImmediateEgressProbeForEntry(hash node.Hash, expected *node.NodeEntry) {
	if expected == nil {
		return
	}
	m.enqueueProbe(hash, probeTaskKindEgress, probePriorityNormal, expected)
}

// TriggerImmediateLatencyProbe enqueues an async latency probe for a node.
// Caller returns immediately.
func (m *ProbeManager) TriggerImmediateLatencyProbe(hash node.Hash) {
	m.enqueueProbe(hash, probeTaskKindLatency, probePriorityNormal)
}

// TriggerImmediateLatencyProbeForEntry enqueues a latency probe only for the
// exact NodeEntry generation that requested it.
func (m *ProbeManager) TriggerImmediateLatencyProbeForEntry(hash node.Hash, expected *node.NodeEntry) {
	if expected == nil {
		return
	}
	m.enqueueProbe(hash, probeTaskKindLatency, probePriorityNormal, expected)
}

// EgressProbeResult holds the results of a synchronous egress probe.
type EgressProbeResult struct {
	EgressIP      string  `json:"egress_ip"`
	Region        string  `json:"region,omitempty"`
	LatencyEwmaMs float64 `json:"latency_ewma_ms"`
}

// ProbeEgressSync performs a blocking egress probe and returns the results.
// Used by API action endpoints that must return probe data synchronously.
func (m *ProbeManager) ProbeEgressSync(hash node.Hash) (*EgressProbeResult, error) {
	return m.ProbeEgressSyncContext(context.Background(), hash)
}

// ProbeEgressSyncForEntry performs a blocking egress probe only for the exact
// NodeEntry captured by the caller. A hash can be removed and recreated while
// a control-plane request is waiting; silently probing the replacement would
// return a result for a different node generation.
func (m *ProbeManager) ProbeEgressSyncForEntry(hash node.Hash, expected *node.NodeEntry) (*EgressProbeResult, error) {
	return m.ProbeEgressSyncForEntryContext(context.Background(), hash, expected)
}

// ProbeEgressSyncContext performs a blocking egress probe that is canceled by
// either ctx or ProbeManager.Stop.
func (m *ProbeManager) ProbeEgressSyncContext(ctx context.Context, hash node.Hash) (*EgressProbeResult, error) {
	return m.probeEgressSync(ctx, hash, nil)
}

// ProbeEgressSyncForEntryContext is the request-aware exact-entry egress probe
// used by control-plane handlers. It never falls back to a same-hash entry.
func (m *ProbeManager) ProbeEgressSyncForEntryContext(
	ctx context.Context,
	hash node.Hash,
	expected *node.NodeEntry,
) (*EgressProbeResult, error) {
	return m.probeEgressSync(ctx, hash, expected)
}

func (m *ProbeManager) probeEgressSync(ctx context.Context, hash node.Hash, expected *node.NodeEntry) (*EgressProbeResult, error) {
	if m.fetcher == nil {
		return nil, fmt.Errorf("no probe fetcher configured")
	}
	if !m.beginSynchronousProbe() {
		return nil, fmt.Errorf("probe manager stopped")
	}
	defer m.wg.Done()
	probeCtx, releaseProbeCtx := m.contextForSynchronousProbe(ctx)
	defer releaseProbeCtx()

	entry, ok := m.pool.GetEntry(hash)
	if !ok {
		return nil, fmt.Errorf("node not found")
	}
	if expected != nil && entry != expected {
		return nil, errProbeEntryNotLive
	}
	if entry.Outbound.Load() == nil {
		return nil, fmt.Errorf("node outbound not ready")
	}

	// Record synchronous probe attempts for metrics parity with async paths.
	if m.onProbeEvent != nil {
		m.onProbeEvent("egress")
	}

	ip, stage, err := m.performEgressProbe(probeCtx, hash, entry)
	if err != nil {
		if stage == egressProbeParseError {
			return nil, fmt.Errorf("parse egress IP: %w", err)
		}
		return nil, fmt.Errorf("egress probe failed: %w", err)
	}

	// Read back EWMA for cloudflare.com from the latency table.
	var ewmaMs float64
	if entry.LatencyTable != nil {
		if stats, ok := entry.LatencyTable.GetDomainStats(egressTraceDomain); ok {
			ewmaMs = float64(stats.Ewma) / float64(time.Millisecond)
		}
	}
	if current, ok := m.pool.GetEntry(hash); !ok || current != entry {
		return nil, errProbeEntryNotLive
	}

	return &EgressProbeResult{
		EgressIP:      ip.String(),
		LatencyEwmaMs: ewmaMs,
	}, nil
}

// LatencyProbeResult holds the results of a synchronous latency probe.
type LatencyProbeResult struct {
	LatencyEwmaMs float64 `json:"latency_ewma_ms"`
}

// ProbeLatencySync performs a blocking latency probe and returns the results.
func (m *ProbeManager) ProbeLatencySync(hash node.Hash) (*LatencyProbeResult, error) {
	return m.ProbeLatencySyncContext(context.Background(), hash)
}

// ProbeLatencySyncForEntry performs a blocking latency probe only for the
// exact NodeEntry captured by the caller.
func (m *ProbeManager) ProbeLatencySyncForEntry(hash node.Hash, expected *node.NodeEntry) (*LatencyProbeResult, error) {
	return m.ProbeLatencySyncForEntryContext(context.Background(), hash, expected)
}

// ProbeLatencySyncContext performs a blocking latency probe that is canceled
// by either ctx or ProbeManager.Stop.
func (m *ProbeManager) ProbeLatencySyncContext(ctx context.Context, hash node.Hash) (*LatencyProbeResult, error) {
	return m.probeLatencySync(ctx, hash, nil)
}

// ProbeLatencySyncForEntryContext is the request-aware exact-entry latency
// probe used by control-plane handlers. It never falls back to a same-hash
// entry.
func (m *ProbeManager) ProbeLatencySyncForEntryContext(
	ctx context.Context,
	hash node.Hash,
	expected *node.NodeEntry,
) (*LatencyProbeResult, error) {
	return m.probeLatencySync(ctx, hash, expected)
}

func (m *ProbeManager) probeLatencySync(ctx context.Context, hash node.Hash, expected *node.NodeEntry) (*LatencyProbeResult, error) {
	if m.fetcher == nil {
		return nil, fmt.Errorf("no probe fetcher configured")
	}
	if !m.beginSynchronousProbe() {
		return nil, fmt.Errorf("probe manager stopped")
	}
	defer m.wg.Done()
	probeCtx, releaseProbeCtx := m.contextForSynchronousProbe(ctx)
	defer releaseProbeCtx()

	entry, ok := m.pool.GetEntry(hash)
	if !ok {
		return nil, fmt.Errorf("node not found")
	}
	if expected != nil && entry != expected {
		return nil, errProbeEntryNotLive
	}
	if entry.Outbound.Load() == nil {
		return nil, fmt.Errorf("node outbound not ready")
	}

	testURL := m.currentLatencyTestURL()
	domain := netutil.ExtractDomain(testURL)

	// Record synchronous probe attempts for metrics parity with async paths.
	if m.onProbeEvent != nil {
		m.onProbeEvent("latency")
	}

	if err := m.performLatencyProbe(probeCtx, hash, entry, testURL); err != nil {
		return nil, fmt.Errorf("latency probe failed: %w", err)
	}

	// Read back EWMA.
	var ewmaMs float64
	if entry.LatencyTable != nil {
		if stats, ok := entry.LatencyTable.GetDomainStats(domain); ok {
			ewmaMs = float64(stats.Ewma) / float64(time.Millisecond)
		}
	}
	if current, ok := m.pool.GetEntry(hash); !ok || current != entry {
		return nil, errProbeEntryNotLive
	}

	return &LatencyProbeResult{
		LatencyEwmaMs: ewmaMs,
	}, nil
}

// scanEgress iterates all pool nodes and probes those due for egress check.
func (m *ProbeManager) scanEgress() {
	now := time.Now()
	interval := 24 * time.Hour // default MaxEgressTestInterval
	if m.maxEgressTestInterval != nil {
		interval = m.maxEgressTestInterval()
	}
	lookahead := 15 * time.Second
	subLookup := m.pool.MakeSubLookup()

	m.pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
		// Check stop signal.
		select {
		case <-m.stopCh:
			return false
		default:
		}

		if entry.IsDisabledBySubscriptions(subLookup) {
			return true // disabled node -> skip periodic probe
		}

		if entry.Outbound.Load() == nil {
			return true // skip nil outbound
		}

		// Check if due: lastAttempt + interval - lookahead <= now.
		lastCheck := entry.LastEgressUpdateAttempt.Load()
		if lastCheck > 0 {
			nextDue := time.Unix(0, lastCheck).Add(interval).Add(-lookahead)
			if now.Before(nextDue) {
				return true // not yet due
			}
		}

		if hook := m.beforeScanEnqueueHook; hook != nil {
			hook(h, entry)
		}
		m.enqueueProbe(h, probeTaskKindEgress, probePriorityNormal, entry)

		return true
	})
}

// scanLatency iterates all pool nodes and probes those due for latency check.
func (m *ProbeManager) scanLatency() {
	now := time.Now()
	maxLatencyInterval := 1 * time.Hour // default
	if m.maxLatencyTestInterval != nil {
		maxLatencyInterval = m.maxLatencyTestInterval()
	}
	maxAuthorityInterval := 3 * time.Hour // default
	if m.maxAuthorityLatencyTestInterval != nil {
		maxAuthorityInterval = m.maxAuthorityLatencyTestInterval()
	}
	lookahead := 15 * time.Second
	subLookup := m.pool.MakeSubLookup()
	var authorities []string
	if m.latencyAuthorities != nil {
		authorities = m.latencyAuthorities()
	}

	m.pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
		select {
		case <-m.stopCh:
			return false
		default:
		}

		if entry.IsDisabledBySubscriptions(subLookup) {
			return true // disabled node -> skip periodic probe
		}

		if entry.Outbound.Load() == nil {
			return true // skip nil outbound
		}

		if !m.isLatencyProbeDue(entry, now, maxLatencyInterval, maxAuthorityInterval, authorities, lookahead) {
			return true
		}

		if hook := m.beforeScanEnqueueHook; hook != nil {
			hook(h, entry)
		}
		m.enqueueProbe(h, probeTaskKindLatency, probePriorityNormal, entry)

		return true
	})
}

func (m *ProbeManager) runProbeWorker() {
	for {
		task, ok := m.taskQueue.Dequeue()
		if !ok {
			return
		}

		state, ok := m.markTaskRunning(task.key)
		if !ok {
			continue
		}

		m.executeTask(task)
		m.finishTask(task.key, state)
	}
}

func (m *ProbeManager) executeTask(task probeTask) {
	entry, ok := m.pool.GetEntry(task.key.hash)
	if !ok || (task.key.expected != nil && entry != task.key.expected) || entry.Outbound.Load() == nil {
		return
	}
	if entry.IsDisabledBySubscriptions(m.pool.MakeSubLookup()) {
		return
	}

	switch task.key.kind {
	case probeTaskKindEgress:
		m.probeEgress(task.key.hash, entry)
	case probeTaskKindLatency:
		m.probeLatency(task.key.hash, entry, m.currentLatencyTestURL())
	}
}

func (m *ProbeManager) enqueueProbe(hash node.Hash, kind probeTaskKind, priority probePriority, expected ...*node.NodeEntry) bool {
	var expectedEntry *node.NodeEntry
	if len(expected) != 0 {
		expectedEntry = expected[0]
	}
	key := probeTaskKey{hash: hash, kind: kind, expected: expectedEntry}
	state, _ := m.taskStates.LoadOrCompute(key, func() (*probeTaskState, bool) {
		return &probeTaskState{}, false
	})
	allowQueuedHighUpgrade := priority == probePriorityHigh

	for {
		flags := state.flags.Load()
		if flags&taskFlagRunning != 0 {
			next := flags | taskFlagDirty
			if priority == probePriorityHigh {
				next |= taskFlagDirtyHigh
			}
			if state.flags.CompareAndSwap(flags, next) {
				return false
			}
			continue
		}

		if flags&taskFlagQueued != 0 {
			// If a normal-priority task is already queued, add a high-priority token
			// so the next dequeue can observe the upgraded urgency. The stale normal
			// token will later no-op when it reaches a worker.
			if allowQueuedHighUpgrade && flags&taskFlagQueuedHigh == 0 {
				next := flags | taskFlagQueuedHigh
				if !state.flags.CompareAndSwap(flags, next) {
					continue
				}
				if m.taskQueue.Enqueue(probeTask{key: key}, probePriorityHigh) {
					return true
				}
				for {
					current := state.flags.Load()
					revert := current &^ taskFlagQueuedHigh
					if state.flags.CompareAndSwap(current, revert) {
						break
					}
				}
				allowQueuedHighUpgrade = false
				continue
			}

			next := flags | taskFlagDirty
			if priority == probePriorityHigh {
				next |= taskFlagDirtyHigh
			}
			if state.flags.CompareAndSwap(flags, next) {
				return false
			}
			continue
		}

		next := flags | taskFlagQueued
		if priority == probePriorityHigh {
			next |= taskFlagQueuedHigh
		} else {
			next &^= taskFlagQueuedHigh
		}
		if !state.flags.CompareAndSwap(flags, next) {
			continue
		}

		if m.taskQueue.Enqueue(probeTask{key: key}, priority) {
			return true
		}

		m.clearDroppedState(state)
		m.tryDeleteTaskState(key, state)
		return false
	}
}

func (m *ProbeManager) markTaskRunning(key probeTaskKey) (*probeTaskState, bool) {
	state, ok := m.taskStates.Load(key)
	if !ok {
		return nil, false
	}

	for {
		flags := state.flags.Load()
		if flags&taskFlagQueued == 0 {
			m.tryDeleteTaskState(key, state)
			return nil, false
		}
		next := (flags | taskFlagRunning) &^ (taskFlagQueued | taskFlagQueuedHigh)
		if state.flags.CompareAndSwap(flags, next) {
			return state, true
		}
	}
}

func (m *ProbeManager) finishTask(key probeTaskKey, state *probeTaskState) {
	requeue := false
	requeuePriority := probePriorityNormal

	for {
		flags := state.flags.Load()
		next := flags &^ taskFlagRunning
		requeue = false

		if flags&taskFlagDirty != 0 {
			requeue = true
			next |= taskFlagQueued
			next &^= taskFlagDirty
			if flags&taskFlagDirtyHigh != 0 {
				next |= taskFlagQueuedHigh
				next &^= taskFlagDirtyHigh
				requeuePriority = probePriorityHigh
			} else {
				next &^= taskFlagQueuedHigh
				requeuePriority = probePriorityNormal
			}
		} else {
			next &^= (taskFlagDirty | taskFlagDirtyHigh | taskFlagQueuedHigh)
		}

		if state.flags.CompareAndSwap(flags, next) {
			break
		}
	}

	if requeue {
		if !m.taskQueue.Enqueue(probeTask{key: key}, requeuePriority) {
			m.clearDroppedState(state)
		}
	}

	m.tryDeleteTaskState(key, state)
}

func (m *ProbeManager) clearDroppedState(state *probeTaskState) {
	for {
		flags := state.flags.Load()
		next := flags &^ (taskFlagQueued | taskFlagQueuedHigh | taskFlagDirty | taskFlagDirtyHigh)
		if state.flags.CompareAndSwap(flags, next) {
			return
		}
	}
}

func (m *ProbeManager) dropPendingTaskState(key probeTaskKey) {
	state, ok := m.taskStates.Load(key)
	if !ok {
		return
	}
	m.clearDroppedState(state)
	m.tryDeleteTaskState(key, state)
}

func (m *ProbeManager) tryDeleteTaskState(key probeTaskKey, state *probeTaskState) {
	m.taskStates.Compute(key, func(current *probeTaskState, loaded bool) (*probeTaskState, xsync.ComputeOp) {
		if !loaded || current != state {
			return current, xsync.CancelOp
		}
		if current.flags.Load() != 0 {
			return current, xsync.CancelOp
		}
		return nil, xsync.DeleteOp
	})
}

// isLatencyProbeDue checks whether a node needs a latency probe, based on
// last probe-attempt timestamps (not latency-table timestamps).
func (m *ProbeManager) isLatencyProbeDue(
	entry *node.NodeEntry,
	now time.Time,
	maxLatencyInterval, maxAuthorityInterval time.Duration,
	authorities []string,
	lookahead time.Duration,
) bool {
	lastAny := entry.LastLatencyProbeAttempt.Load()
	if lastAny == 0 {
		return true
	}
	anyDeadline := time.Unix(0, lastAny).Add(maxLatencyInterval).Add(-lookahead)
	if !now.Before(anyDeadline) {
		return true
	}

	if len(authorities) == 0 {
		return false
	}

	lastAuthority := entry.LastAuthorityLatencyProbeAttempt.Load()
	if lastAuthority == 0 {
		return true
	}
	authorityDeadline := time.Unix(0, lastAuthority).Add(maxAuthorityInterval).Add(-lookahead)
	return !now.Before(authorityDeadline)
}

// probeEgress performs a single egress probe against a node via Cloudflare trace.
// Writes back: RecordResult, RecordLatency (cloudflare.com), UpdateNodeEgressIP.
func (m *ProbeManager) probeEgress(hash node.Hash, entry *node.NodeEntry) {
	if m.fetcher == nil {
		return
	}

	if entry.Outbound.Load() == nil {
		return
	}

	// Always record the probe attempt (success or failure).
	if m.onProbeEvent != nil {
		m.onProbeEvent("egress")
	}

	_, stage, err := m.performEgressProbe(m.probeCtx, hash, entry)
	if err != nil {
		if stage == egressProbeParseError {
			log.Printf("[probe] parse egress IP for %s: %v", hash.Hex(), err)
			return
		}
		log.Printf("[probe] egress probe failed for %s: %v", hash.Hex(), err)
		return
	}
}

// probeLatency performs a latency probe against a node using the configured test URL.
// Writes back: RecordResult, RecordLatency.
func (m *ProbeManager) probeLatency(hash node.Hash, entry *node.NodeEntry, testURL string) {
	if m.fetcher == nil {
		return
	}

	if entry.Outbound.Load() == nil {
		return
	}

	// Always record the probe attempt (success or failure).
	if m.onProbeEvent != nil {
		m.onProbeEvent("latency")
	}

	if err := m.performLatencyProbe(m.probeCtx, hash, entry, testURL); err != nil {
		log.Printf("[probe] latency probe failed for %s: %v", hash.Hex(), err)
		return
	}
}

func (m *ProbeManager) performEgressProbe(ctx context.Context, hash node.Hash, entry *node.NodeEntry) (netip.Addr, egressProbeErrorStage, error) {
	body, latency, err := m.fetcher(ctx, entry, egressTraceURL)
	if err != nil {
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return netip.Addr{}, egressProbeFetchError, ctxErr
			}
		}
		m.pool.RecordResultForEntry(hash, entry, false)
		m.pool.UpdateNodeEgressIPForEntry(hash, entry, nil, nil)
		return netip.Addr{}, egressProbeFetchError, err
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return netip.Addr{}, egressProbeFetchError, ctxErr
		}
	}

	if !m.pool.RecordResultForEntry(hash, entry, true) {
		return netip.Addr{}, egressProbeFetchError, errProbeEntryNotLive
	}
	if hook := m.afterProbeResultRecordHook; hook != nil {
		hook()
	}

	ip, loc, err := ParseCloudflareTrace(body)
	postErr := m.withProbePostProcessing(ctx, func() {
		if latency > 0 {
			m.pool.RecordLatencyForEntry(hash, entry, egressTraceDomain, &latency)
		}
		if err != nil {
			m.pool.UpdateNodeEgressIPForEntry(hash, entry, nil, nil)
			return
		}
		m.pool.UpdateNodeEgressIPForEntry(hash, entry, &ip, loc)
	})
	if postErr != nil {
		return netip.Addr{}, egressProbeFetchError, postErr
	}
	if err != nil {
		return netip.Addr{}, egressProbeParseError, err
	}
	return ip, egressProbeNoError, nil
}

func (m *ProbeManager) performLatencyProbe(ctx context.Context, hash node.Hash, entry *node.NodeEntry, testURL string) error {
	domain := netutil.ExtractDomain(testURL)
	_, latency, err := m.fetcher(ctx, entry, testURL)
	if err != nil {
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
		}
		m.pool.RecordResultForEntry(hash, entry, false)
		m.pool.RecordLatencyForEntry(hash, entry, domain, nil)
		return err
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}

	if !m.pool.RecordResultForEntry(hash, entry, true) {
		return errProbeEntryNotLive
	}
	if hook := m.afterProbeResultRecordHook; hook != nil {
		hook()
	}
	if err := m.withProbePostProcessing(ctx, func() {
		m.pool.RecordLatencyForEntry(hash, entry, domain, &latency)
	}); err != nil {
		return err
	}
	return nil
}

// withProbePostProcessing owns the final health metadata write after the
// result record. Stop uses the same lifecycle lock to close this admission, so
// cancellation cannot land between the context check and the writeback.
func (m *ProbeManager) withProbePostProcessing(ctx context.Context, fn func()) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.stopped {
		return context.Canceled
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if fn != nil {
		fn()
	}
	return nil
}

func (m *ProbeManager) currentLatencyTestURL() string {
	testURL := defaultLatencyTestURL
	if m.latencyTestURL != nil {
		testURL = m.latencyTestURL()
	}
	return testURL
}
