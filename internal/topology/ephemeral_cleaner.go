package topology

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/Resinat/Resin/internal/subscription"
)

// EphemeralCleaner periodically removes unhealthy nodes from ephemeral subscriptions.
type EphemeralCleaner struct {
	subManager    *SubscriptionManager
	pool          *GlobalNodePool
	onNodeEvicted func(subID string, hash node.Hash)
	// runPersistenceMutation admits the whole cleanup and its pool callbacks
	// as one dirty-write transaction.  A shutdown may close dirty-write
	// admission while a sweep is still unwinding; an already-admitted
	// mutation must finish, while a late mutation must fail before changing
	// runtime state.
	runPersistenceMutation func(func(PersistenceAdmission)) bool

	// beforeSubscriptionLockHook is test-only coordination for the lifecycle
	// boundary between taking the sweep snapshot and acquiring a sub's op lock.
	// It is intentionally private so production callers cannot bypass the
	// subscription mutation protocol.
	beforeSubscriptionLockHook func(id string, sub *subscription.Subscription)
	// afterSubscriptionMutationHook is a package-test seam after the per-sub
	// operation lock is released. Production leaves it nil.
	afterSubscriptionMutationHook func()

	stopCh      chan struct{}
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	stopDone    chan struct{}
	minInterval time.Duration
	jitterRange time.Duration
	// beforeStartAdmissionHook is package-test coordination for the
	// Start/Stop admission boundary. Production leaves it nil.
	beforeStartAdmissionHook func()
}

// NewEphemeralCleaner creates an EphemeralCleaner that reads per-subscription
// eviction delay values during each sweep.
func NewEphemeralCleaner(
	subManager *SubscriptionManager,
	pool *GlobalNodePool,
) *EphemeralCleaner {
	return NewEphemeralCleanerWithIntervals(
		subManager,
		pool,
		scanloop.DefaultMinInterval,
		scanloop.DefaultJitterRange,
	)
}

// NewEphemeralCleanerWithIntervals creates a cleaner with an explicit scan
// cadence. The normal application uses NewEphemeralCleaner; this constructor
// is also useful for embedding callers that own their scheduling budget.
func NewEphemeralCleanerWithIntervals(
	subManager *SubscriptionManager,
	pool *GlobalNodePool,
	minInterval time.Duration,
	jitterRange time.Duration,
) *EphemeralCleaner {
	if minInterval <= 0 {
		minInterval = scanloop.DefaultMinInterval
	}
	if jitterRange < 0 {
		jitterRange = 0
	}
	return &EphemeralCleaner{
		subManager:  subManager,
		pool:        pool,
		stopCh:      make(chan struct{}),
		minInterval: minInterval,
		jitterRange: jitterRange,
	}
}

// SetOnNodeEvicted sets the synchronous persistence callback for each newly
// evicted node. It runs while the subscription operation lock is held and
// must not re-enter a mutation on that subscription.
func (c *EphemeralCleaner) SetOnNodeEvicted(fn func(subID string, hash node.Hash)) {
	c.onNodeEvicted = fn
}

// SetPersistenceMutationRunner supplies the owner for a cleanup mutation.
// The runner must reject the callback before it changes runtime state when
// shutdown has closed the persistence admission.
func (c *EphemeralCleaner) SetPersistenceMutationRunner(
	run func(func(PersistenceAdmission)) bool,
) {
	c.runPersistenceMutation = run
}

// Start launches the background cleaner goroutine.
func (c *EphemeralCleaner) Start() {
	c.lifecycleMu.Lock()
	if c.started || c.stopped {
		c.lifecycleMu.Unlock()
		return
	}
	c.started = true
	if hook := c.beforeStartAdmissionHook; hook != nil {
		hook()
	}
	c.wg.Add(1)
	c.lifecycleMu.Unlock()
	go func() {
		defer c.wg.Done()
		scanloop.Run(c.stopCh, c.minInterval, c.jitterRange, c.sweep)
	}()
}

// Stop signals the cleaner to stop and waits for it to finish.
func (c *EphemeralCleaner) Stop() {
	_ = c.StopContext(context.Background())
}

// StopContext closes cleaner admission once and waits for the single stop
// owner. The caller's context only bounds this wait; it never cancels a
// sweep that was already admitted. A later Background call joins the same
// owner before process-level persistence is closed.
func (c *EphemeralCleaner) StopContext(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.lifecycleMu.Lock()
	var done chan struct{}
	if !c.stopped {
		c.stopped = true
		close(c.stopCh)
		c.stopDone = make(chan struct{})
		done = c.stopDone
		c.lifecycleMu.Unlock()

		go func() {
			c.wg.Wait()
			close(done)
		}()
	} else {
		done = c.stopDone
		c.lifecycleMu.Unlock()
		if done == nil {
			return nil
		}
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *EphemeralCleaner) sweep() {
	c.sweepWithHook(nil)
}

// sweepWithHook runs the sweep. If betweenScans is non-nil, it is called
// after the candidate set (evictSet) is built but before the second
// verification check. This allows tests to inject state changes at the
// exact TOCTOU window.
func (c *EphemeralCleaner) sweepWithHook(betweenScans func()) {
	now := time.Now().UnixNano()

	type ephemeralSub struct {
		id  string
		sub *subscription.Subscription
	}
	ephemeralSubs := make([]ephemeralSub, 0, c.subManager.Size())

	c.subManager.Range(func(id string, sub *subscription.Subscription) bool {
		if sub.Ephemeral() {
			ephemeralSubs = append(ephemeralSubs, ephemeralSub{id: id, sub: sub})
		}
		return true
	})

	if len(ephemeralSubs) == 0 {
		return
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(ephemeralSubs) {
		workers = len(ephemeralSubs)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, item := range ephemeralSubs {
		sem <- struct{}{}
		wg.Add(1)
		go func(id string, sub *subscription.Subscription) {
			defer wg.Done()
			defer func() { <-sem }()
			c.sweepOneSubscription(id, sub, now, betweenScans)
		}(item.id, item.sub)
	}
	wg.Wait()
}

func (c *EphemeralCleaner) sweepOneSubscription(
	id string,
	sub *subscription.Subscription,
	now int64,
	betweenScans func(),
) {
	var (
		evictCount    int
		evictedHashes []node.Hash
	)
	if c.beforeSubscriptionLockHook != nil {
		c.beforeSubscriptionLockHook(id, sub)
	}
	sub.WithOpLock(func() {
		// Stop closes admission before waiting for the admitted worker. A
		// sweep that had not entered its mutation owner yet must not mutate
		// runtime state after shutdown has begun.
		c.lifecycleMu.Lock()
		stopped := c.stopped
		c.lifecycleMu.Unlock()
		if stopped {
			return
		}

		// The sweep list is only a snapshot. DeleteSubscription serializes its
		// runtime teardown with this same op lock, so membership must be checked
		// again before mutating managed nodes.
		if c.subManager.Lookup(id) != sub {
			return
		}
		evictDelayNs := sub.EphemeralNodeEvictDelayNs()
		mutate := func(admission PersistenceAdmission) {
			evictCount, evictedHashes = CleanupSubscriptionNodesWithConfirmNoLock(
				sub,
				c.pool,
				func(entry *node.NodeEntry) bool {
					return c.shouldEvictEntry(entry, now, evictDelayNs)
				},
				betweenScans,
				nil,
				admission,
			)
			// The persistence callback is part of the same subscription
			// mutation transaction. DeleteSubscription uses this op lock too;
			// running the callback after unlock lets its delete mark overtake
			// this upsert mark.
			if admission != nil {
				for _, h := range evictedHashes {
					admission.MarkSubscriptionNode(id, h.Hex())
				}
			} else if c.onNodeEvicted != nil {
				for _, h := range evictedHashes {
					c.onNodeEvicted(id, h)
				}
			}
		}
		if c.runPersistenceMutation != nil {
			c.runPersistenceMutation(mutate)
		} else {
			mutate(nil)
		}
	})
	if c.afterSubscriptionMutationHook != nil {
		c.afterSubscriptionMutationHook()
	}

	if evictCount > 0 {
		log.Printf("[ephemeral] evicted %d nodes from sub %s", evictCount, id)
	}
}

func (c *EphemeralCleaner) shouldEvictEntry(entry *node.NodeEntry, now int64, evictDelayNs int64) bool {
	if entry == nil {
		return false
	}

	// Outbound build failed and node is still without outbound.
	// For ephemeral subscriptions, this node should be dropped quickly.
	if !entry.HasOutbound() && entry.GetLastError() != "" {
		return true
	}

	// Circuit remains open beyond configured eviction delay.
	circuitSince := entry.CircuitOpenSince.Load()
	return circuitSince > 0 && (now-circuitSince) > evictDelayNs
}
