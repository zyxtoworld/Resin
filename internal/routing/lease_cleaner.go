package routing

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/puzpuzpuz/xsync/v4"
)

// LeaseCleaner periodically sweeps for expired leases.
type LeaseCleaner struct {
	router      *Router
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

	// test hook: called at the beginning of each sweep.
	sweepHook func()
	// test hook: called after the state snapshot is complete and before any
	// platform worker starts. lifecycleMu's read lock is held at this point.
	afterStateSnapshotHook func()
	// test hook: called once per platform worker before it scans that platform.
	sweepPlatformStateHook func()
}

func NewLeaseCleaner(router *Router) *LeaseCleaner {
	return newLeaseCleanerWithIntervals(router, 13*time.Second, 4*time.Second)
}

// NewLeaseCleanerWithIntervals creates a lease cleaner with an explicit scan
// cadence. Production uses NewLeaseCleaner; the explicit form is also useful
// to callers that own a different scheduling budget.
func NewLeaseCleanerWithIntervals(router *Router, minInterval, jitterRange time.Duration) *LeaseCleaner {
	return newLeaseCleanerWithIntervals(router, minInterval, jitterRange)
}

func newLeaseCleanerWithIntervals(router *Router, minInterval, jitterRange time.Duration) *LeaseCleaner {
	return &LeaseCleaner{
		router:      router,
		stopCh:      make(chan struct{}),
		minInterval: minInterval,
		jitterRange: jitterRange,
	}
}

func (c *LeaseCleaner) Start() {
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

func (c *LeaseCleaner) Stop() {
	_ = c.StopContext(context.Background())
}

// StopContext closes cleaner admission once and waits for the single stop
// owner. The caller's context only bounds this wait; it does not cancel an
// already-admitted lease sweep. A later Background call joins the same owner
// before the router or persistence owners are closed.
func (c *LeaseCleaner) StopContext(ctx context.Context) error {
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

func (c *LeaseCleaner) sweep() {
	events := c.router.newLeaseEventBatch()
	c.router.lifecycleMu.RLock()
	if c.router.stopped {
		c.router.lifecycleMu.RUnlock()
		events.finish()
		return
	}

	if c.sweepHook != nil {
		c.sweepHook()
	}

	now := time.Now()
	nowNs := now.UnixNano()

	type platformState struct {
		platID string
		state  *PlatformRoutingState
	}
	states := make([]platformState, 0, c.router.states.Size())
	c.router.states.Range(func(platID string, state *PlatformRoutingState) bool {
		select {
		case <-c.stopCh:
			return false
		default:
		}
		states = append(states, platformState{platID: platID, state: state})
		return true
	})
	if c.afterStateSnapshotHook != nil {
		c.afterStateSnapshotHook()
	}
	if len(states) == 0 {
		c.router.lifecycleMu.RUnlock()
		events.finish()
		return
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(states) {
		workers = len(states)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, item := range states {
		select {
		case <-c.stopCh:
			wg.Wait()
			c.router.lifecycleMu.RUnlock()
			events.finish()
			return
		default:
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(platID string, state *PlatformRoutingState) {
			defer wg.Done()
			defer func() { <-sem }()
			c.sweepPlatformStateWithEvents(platID, state, nowNs, events)
		}(item.platID, item.state)
	}
	wg.Wait()
	c.router.lifecycleMu.RUnlock()
	events.finish()
}

func (c *LeaseCleaner) sweepPlatformState(platID string, state *PlatformRoutingState, nowNs int64) {
	events := c.router.newLeaseEventBatch()
	c.router.lifecycleMu.RLock()
	c.sweepPlatformStateWithEvents(platID, state, nowNs, events)
	c.router.lifecycleMu.RUnlock()
	events.finish()
}

func (c *LeaseCleaner) sweepPlatformStateWithEvents(
	platID string,
	state *PlatformRoutingState,
	nowNs int64,
	events *leaseEventBatch,
) {
	if c.sweepPlatformStateHook != nil {
		c.sweepPlatformStateHook()
	}
	// Iterate over all leases for this platform
	state.Leases.Range(func(account string, lease Lease) bool {
		// Check against stop signal
		select {
		case <-c.stopCh:
			return false
		default:
		}

		if lease.ExpiryNs <= nowNs {
			// Expired. Use Compute to verify and delete atomically.
			state.Leases.leases.Compute(account, func(current Lease, loaded bool) (Lease, xsync.ComputeOp) {
				if !loaded {
					return current, xsync.CancelOp
				}
				// Double-check expiry inside lock
				if current.ExpiryNs <= nowNs {
					mutation := events.newMutation()
					state.Leases.stats.Dec(current.EgressIP)
					mutation.add(LeaseEvent{
						Type:        LeaseExpire,
						PlatformID:  platID,
						Account:     account,
						NodeHash:    current.NodeHash,
						EgressIP:    current.EgressIP,
						CreatedAtNs: current.CreatedAtNs,
					})
					mutation.commit()
					if hook := c.router.afterLeaseDeleteLinearizedHook; hook != nil {
						hook()
					}

					return current, xsync.DeleteOp
				}
				return current, xsync.CancelOp // Renewed concurrently, don't delete
			})
		}
		return true
	})
}
