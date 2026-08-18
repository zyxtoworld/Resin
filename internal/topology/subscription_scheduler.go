package topology

import (
	"context"
	"errors"
	"log"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/Resinat/Resin/internal/subscription"
)

const schedulerLookahead = 15 * time.Second

type nodeRuntimePreparation struct {
	hash  node.Hash
	entry *node.NodeEntry
}

// SubscriptionScheduler manages periodic subscription updates.
type SubscriptionScheduler struct {
	subManager     *SubscriptionManager
	pool           *GlobalNodePool
	downloader     netutil.Downloader
	downloadCtx    context.Context
	cancelDownload context.CancelFunc

	// Fetcher fetches subscription data from a URL.
	// Defaults to downloader.Download; injectable for testing.
	Fetcher func(context.Context, string) ([]byte, error)

	// For persistence.
	onSubUpdated func(sub *subscription.Subscription)
	// runRefreshMutation owns the complete ManagedNodes -> pool mutation. A
	// caller may use it to keep the corresponding persistence admission open
	// until every node callback has been emitted.
	runRefreshMutation func(func(PersistenceAdmission)) bool
	// onSubReenabledNode is called for each non-evicted node entry when a
	// subscription transitions from disabled to enabled. The entry token keeps
	// a delayed resource callback from acting on a same-hash replacement.
	onSubReenabledNode func(hash node.Hash, expected *node.NodeEntry)

	stopCh      chan struct{}
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	stopDone    chan struct{}

	// Package-private test seam for the deterministic boundary after the
	// worker's stop check and immediately before refresh source admission.
	beforeRefreshUpdateHook func()
	// Package-private test seam after the refresh input snapshot and before
	// source admission. With the pre-snapshot implementation this same seam
	// sits between the individual input reads and the version read.
	beforeRefreshConfigVersionHook func()
	// Package-private test seam immediately before the context-aware input
	// snapshot. Production leaves it nil.
	beforeRefreshConfigSnapshotHook func()
	// Package-private test seam used to cancel a synchronous refresh after its
	// input has been read but before parsing begins.
	beforeRefreshParseHook func()
	// Package-private test seam immediately before a refresh tries to acquire
	// the subscription operation lock for its apply phase.
	beforeRefreshApplyLockHook func()
	// Package-private test seam immediately before a refresh failure tries to
	// acquire the subscription operation lock for its failure record.
	beforeRefreshFailureLockHook func()
	// Package-private test seam after the re-enable runtime mutation has
	// published and immediately before deferred node-runtime callbacks run.
	beforeReenabledRuntimeHook func()
}

var ErrSchedulerStopped = errors.New("subscription scheduler is stopped")

// SchedulerConfig configures the SubscriptionScheduler.
type SchedulerConfig struct {
	SubManager   *SubscriptionManager
	Pool         *GlobalNodePool
	Downloader   netutil.Downloader                            // shared downloader
	Fetcher      func(context.Context, string) ([]byte, error) // optional, defaults to Downloader.Download
	OnSubUpdated func(sub *subscription.Subscription)
	// RunRefreshMutation wraps the complete runtime mutation and its dirty
	// callbacks. Returning false rejects the mutation without changing state.
	RunRefreshMutation func(func(PersistenceAdmission)) bool
	// OnSubReenabledNode is fired after false->true enabled transition for the
	// exact entry that was observed while the runtime mutation was committed.
	OnSubReenabledNode func(hash node.Hash, expected *node.NodeEntry)
}

// NewSubscriptionScheduler creates a new scheduler.
func NewSubscriptionScheduler(cfg SchedulerConfig) *SubscriptionScheduler {
	downloadCtx, cancelDownload := context.WithCancel(context.Background())
	sched := &SubscriptionScheduler{
		subManager:         cfg.SubManager,
		pool:               cfg.Pool,
		downloader:         cfg.Downloader,
		downloadCtx:        downloadCtx,
		cancelDownload:     cancelDownload,
		onSubUpdated:       cfg.OnSubUpdated,
		runRefreshMutation: cfg.RunRefreshMutation,
		onSubReenabledNode: cfg.OnSubReenabledNode,
		stopCh:             make(chan struct{}),
	}
	if cfg.Fetcher != nil {
		sched.Fetcher = cfg.Fetcher
	} else {
		sched.Fetcher = sched.fetchViaDownloader
	}
	return sched
}

// Start launches the background scheduler goroutine.
func (s *SubscriptionScheduler) Start() {
	s.lifecycleMu.Lock()
	if s.started || s.stopped {
		s.lifecycleMu.Unlock()
		return
	}
	s.started = true
	s.wg.Add(1)
	s.lifecycleMu.Unlock()
	go func() {
		defer s.wg.Done()
		scanloop.Run(s.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, s.tick)
	}()
}

// Stop signals the scheduler to stop and waits for it to finish.
func (s *SubscriptionScheduler) Stop() {
	_ = s.StopContext(context.Background())
}

// StopContext closes scheduler admission once and waits for the single stop
// owner. The caller's context only bounds this wait; it never cancels the
// admitted refreshes. A later Background call can join the same owner before
// process-level persistence is closed.
func (s *SubscriptionScheduler) StopContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.lifecycleMu.Lock()
	var done chan struct{}
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
		s.cancelDownload()
		s.stopDone = make(chan struct{})
		done = s.stopDone
		s.lifecycleMu.Unlock()

		go func() {
			s.wg.Wait()
			close(done)
		}()
	} else {
		done = s.stopDone
		s.lifecycleMu.Unlock()
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

// ForceRefreshAll unconditionally updates ALL enabled subscriptions, regardless
// of their next-check timestamps. Called once at startup to compensate for
// lost data from weak persistence (DESIGN.md step 8 batch 3).
// Updates run in parallel, and this method waits until all started updates exit.
func (s *SubscriptionScheduler) ForceRefreshAll() {
	s.runTracked(s.forceRefreshAll)
}

func (s *SubscriptionScheduler) forceRefreshAll() {
	select {
	case <-s.stopCh:
		return
	default:
	}

	subsToRefresh := make([]*subscription.Subscription, 0, s.subManager.Size())
	s.subManager.Range(func(id string, sub *subscription.Subscription) bool {
		select {
		case <-s.stopCh:
			return false
		default:
		}
		if sub.Enabled() {
			subsToRefresh = append(subsToRefresh, sub)
		}
		return true
	})
	s.runUpdatesWithWorkerLimit(subsToRefresh)
}

// ForceRefreshAllAsync triggers ForceRefreshAll in a background goroutine.
// The goroutine is tracked by scheduler waitgroup so Stop() waits for exit.
func (s *SubscriptionScheduler) ForceRefreshAllAsync() {
	s.startTracked(s.forceRefreshAll)
}

// UpdateSubscriptionAsync starts a refresh tracked by Stop. It is used by
// control-plane mutations that need to refresh new URL/content asynchronously
// after the persisted runtime mutation has committed.
func (s *SubscriptionScheduler) UpdateSubscriptionAsync(sub *subscription.Subscription) {
	if sub == nil {
		return
	}
	s.startTracked(func() {
		select {
		case <-s.stopCh:
			return
		default:
		}
		s.updateSubscription(context.Background(), sub)
	})
}

func (s *SubscriptionScheduler) beginTracked() bool {
	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return false
	}
	s.wg.Add(1)
	s.lifecycleMu.Unlock()
	return true
}

// RunMutation admits one synchronous control-plane mutation into the
// scheduler lifecycle. The caller may perform persistence and the matching
// locked runtime side effect in fn; Stop waits for the whole function before
// returning. If Stop won the admission race, fn is not run.
func (s *SubscriptionScheduler) RunMutation(fn func() error) error {
	if s == nil || fn == nil {
		return ErrSchedulerStopped
	}
	if !s.beginTracked() {
		return ErrSchedulerStopped
	}
	defer s.wg.Done()
	return fn()
}

func (s *SubscriptionScheduler) runTracked(fn func()) bool {
	if !s.beginTracked() {
		return false
	}
	defer s.wg.Done()
	fn()
	return true
}

func (s *SubscriptionScheduler) startTracked(fn func()) bool {
	if !s.beginTracked() {
		return false
	}
	go func() {
		defer s.wg.Done()
		fn()
	}()
	return true
}

func (s *SubscriptionScheduler) tick() {
	select {
	case <-s.stopCh:
		return
	default:
	}

	now := time.Now().UnixNano()
	dueSubs := make([]*subscription.Subscription, 0, s.subManager.Size())
	s.subManager.Range(func(id string, sub *subscription.Subscription) bool {
		select {
		case <-s.stopCh:
			return false
		default:
		}
		if !sub.Enabled() {
			return true
		}
		if subscriptionDueAt(sub.LastCheckedNs.Load(), sub.UpdateIntervalNs(), now) {
			dueSubs = append(dueSubs, sub)
		}
		return true
	})
	s.runUpdatesWithWorkerLimit(dueSubs)
}

func subscriptionDueAt(lastCheckedNs, intervalNs, nowNs int64) bool {
	lookaheadNs := int64(schedulerLookahead)
	delayNs := int64(0)
	if intervalNs > lookaheadNs {
		delayNs = intervalNs - lookaheadNs
	}

	dueAtNs := lastCheckedNs
	if delayNs > 0 && lastCheckedNs > math.MaxInt64-delayNs {
		dueAtNs = math.MaxInt64
	} else {
		dueAtNs += delayNs
	}
	return dueAtNs <= nowNs
}

func (s *SubscriptionScheduler) runUpdatesWithWorkerLimit(subs []*subscription.Subscription) {
	if len(subs) == 0 {
		return
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(subs) {
		workers = len(subs)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, sub := range subs {
		select {
		case <-s.stopCh:
			wg.Wait()
			return
		default:
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(sub *subscription.Subscription) {
			defer wg.Done()
			defer func() { <-sem }()
			select {
			case <-s.stopCh:
				return
			default:
			}
			s.updateSubscription(context.Background(), sub)
		}(sub)
	}
	wg.Wait()
}

// UpdateSubscription fetches and parses outside the lock, then diffs and
// applies the result under WithSubLock. This keeps the lock scope narrow
// (no I/O under lock) while still preventing concurrent diff/apply races.
// It reports whether scheduler lifecycle admission accepted the refresh.
func (s *SubscriptionScheduler) UpdateSubscription(sub *subscription.Subscription) bool {
	return s.UpdateSubscriptionContext(context.Background(), sub)
}

// UpdateSubscriptionContext refreshes one subscription while honoring the
// caller's cancellation. Scheduler Stop remains an independent cancellation
// source and still waits for the admitted refresh to exit.
func (s *SubscriptionScheduler) UpdateSubscriptionContext(ctx context.Context, sub *subscription.Subscription) bool {
	if s == nil || sub == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false
	}
	return s.runTracked(func() {
		s.updateSubscription(ctx, sub)
	})
}

func (s *SubscriptionScheduler) updateSubscription(ctx context.Context, sub *subscription.Subscription) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return
	}
	attemptSeq := sub.NextAttemptSeq()
	if s.beforeRefreshConfigSnapshotHook != nil {
		s.beforeRefreshConfigSnapshotHook()
	}
	attemptConfig, snapshotErr := sub.SnapshotConfigContext(ctx)
	if snapshotErr != nil {
		return
	}
	attemptURL := attemptConfig.URL
	attemptSourceType := attemptConfig.SourceType
	attemptContent := attemptConfig.Content
	if s.beforeRefreshConfigVersionHook != nil {
		s.beforeRefreshConfigVersionHook()
	}
	attemptConfigVersion := attemptConfig.ConfigVersion

	// 1. Fetch/read content (lock-free).
	var (
		body []byte
		err  error
	)
	if s.beforeRefreshUpdateHook != nil {
		s.beforeRefreshUpdateHook()
	}
	if attemptSourceType == subscription.SourceTypeLocal {
		body = []byte(attemptContent)
	} else {
		body, err = s.fetchSubscription(ctx, attemptURL)
		if err != nil {
			// A request cancellation/deadline is not a subscription failure.
			// Keep counting a timeout produced by the Fetcher itself while the
			// caller is still alive (for example, the configured resource-fetch
			// timeout), but never publish a result for a finished caller.
			if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				s.handleUpdateFailure(ctx, sub, attemptSeq, attemptConfigVersion, "fetch", err)
			}
			return
		}
	}
	if err := ctx.Err(); err != nil {
		return
	}

	// 2. Parse (lock-free).
	if s.beforeRefreshParseHook != nil {
		s.beforeRefreshParseHook()
	}
	parsed, err := subscription.ParseGeneralSubscription(body)
	if err != nil {
		s.handleUpdateFailure(ctx, sub, attemptSeq, attemptConfigVersion, "parse", err)
		return
	}
	if len(parsed) == 0 {
		// A recognized payload with no supported nodes is not a trustworthy
		// replacement. Keep the last known-good runtime view instead of treating
		// an all-invalid/unsupported response as an intentional empty refresh.
		s.handleUpdateFailure(
			ctx,
			sub,
			attemptSeq,
			attemptConfigVersion,
			"parse",
			errors.New("subscription: no supported nodes found"),
		)
		return
	}

	// 3. Build refreshed managed nodes map (lock-free, pure computation).
	refreshedManagedNodes := subscription.NewManagedNodes()
	rawByHash := make(map[node.Hash][]byte)
	for _, p := range parsed {
		h := node.HashFromRawOptions(p.RawOptions)
		existing, _ := refreshedManagedNodes.LoadNode(h)
		existing.Tags = append(existing.Tags, p.Tag)
		refreshedManagedNodes.StoreNode(h, existing)
		if _, ok := rawByHash[h]; !ok {
			rawByHash[h] = p.RawOptions
		}
	}

	// 4. Diff, swap, add/remove — under lock.
	applied := false
	var runtimePreparation []nodeRuntimePreparation
	if hook := s.beforeRefreshApplyLockHook; hook != nil {
		hook()
	}
	if err := sub.WithOpLockContext(ctx, func() {
		mutate := func(admission PersistenceAdmission) {
			if err := ctx.Err(); err != nil {
				return
			}
			// A refresh may have started while the subscription was still
			// registered, then waited on the operation lock while DeleteSubscription
			// removed it.  The manager membership check is the lifecycle boundary:
			// once delete has linearized, an in-flight refresh must not publish
			// runtime state for that subscription again.
			if s.subManager != nil && s.subManager.Lookup(sub.ID) != sub {
				return
			}
			// If refresh-input config changed while this attempt was in-flight, discard.
			if sub.ConfigVersion() != attemptConfigVersion {
				return
			}
			// Stale result guard: if a newer refresh result has already landed,
			// discard this older attempt to avoid rolling state backward.
			if sub.LastAppliedSeq() > attemptSeq {
				return
			}

			old := sub.ManagedNodes()
			mergedManagedNodes := refreshedManagedNodes
			if sub.IncrementalAliveNodes() {
				mergedManagedNodes = subscription.NewManagedNodes()
				old.RangeNodes(func(h node.Hash, oldNode subscription.ManagedNode) bool {
					if oldNode.Evicted {
						mergedManagedNodes.StoreNode(h, oldNode)
						return true
					}
					if entry, ok := s.pool.GetEntry(h); ok && !shouldRemoveUnhealthyNodeForIncrementalMode(entry) {
						mergedManagedNodes.StoreNode(h, oldNode)
					}
					return true
				})
				refreshedManagedNodes.RangeNodes(func(h node.Hash, refreshedNode subscription.ManagedNode) bool {
					mergedManagedNodes.StoreNode(h, refreshedNode)
					return true
				})
			}

			// Keep hashes inherit historical eviction state so refresh will not
			// re-add previously evicted nodes back into pool.
			old.RangeNodes(func(h node.Hash, oldNode subscription.ManagedNode) bool {
				if !oldNode.Evicted {
					return true
				}
				nextNode, ok := mergedManagedNodes.LoadNode(h)
				if !ok {
					return true
				}
				nextNode.Evicted = true
				mergedManagedNodes.StoreNode(h, nextNode)
				return true
			})
			added, kept, removed := subscription.DiffHashes(old, mergedManagedNodes)

			sub.SwapManagedNodes(mergedManagedNodes)

			for _, h := range added {
				managed, ok := mergedManagedNodes.LoadNode(h)
				if ok && managed.Evicted {
					continue
				}
				raw := rawByHash[h]
				if len(raw) == 0 {
					if entry, ok := s.pool.GetEntry(h); ok {
						raw = entry.RawOptions
					}
				}
				if len(raw) == 0 {
					continue
				}
				if entry := s.pool.AddNodeFromSubWithPersistenceForRuntimeBatch(h, raw, sub.ID, admission); entry != nil {
					runtimePreparation = append(runtimePreparation, nodeRuntimePreparation{hash: h, entry: entry})
				}
			}
			for _, h := range kept {
				managed, ok := mergedManagedNodes.LoadNode(h)
				if ok && managed.Evicted {
					continue
				}
				raw := rawByHash[h]
				if len(raw) == 0 {
					if entry, ok := s.pool.GetEntry(h); ok {
						raw = entry.RawOptions
					}
				}
				if len(raw) == 0 {
					continue
				}
				if entry := s.pool.AddNodeFromSubWithPersistenceForRuntimeBatch(h, raw, sub.ID, admission); entry != nil {
					runtimePreparation = append(runtimePreparation, nodeRuntimePreparation{hash: h, entry: entry})
				}
			}
			for _, h := range removed {
				s.pool.RemoveNodeFromSubWithPersistence(h, sub.ID, admission)
			}
			// 5. Update timestamps (inside lock, using current time).
			now := time.Now().UnixNano()
			sub.LastCheckedNs.Store(now)
			sub.LastUpdatedNs.Store(now)
			sub.MarkAppliedAttempt(attemptSeq)
			sub.SetLastError("")
			applied = true
		}
		applyMutation := func(admission PersistenceAdmission) {
			if s.pool != nil {
				s.pool.WithRuntimeMutation(func() {
					mutate(admission)
				})
				return
			}
			mutate(admission)
		}
		if s.runRefreshMutation != nil {
			if !s.runRefreshMutation(applyMutation) {
				return
			}
		} else {
			applyMutation(nil)
		}
	}); err != nil {
		return
	}
	if !applied {
		log.Printf("[scheduler] stale success ignored for %s", sub.ID)
		return
	}
	for _, prep := range runtimePreparation {
		s.pool.RunNodeAddedRuntime(prep.hash, prep.entry)
	}

	if s.onSubUpdated != nil {
		s.onSubUpdated(sub)
	}
}

func shouldRemoveUnhealthyNodeForIncrementalMode(entry *node.NodeEntry) bool {
	if entry == nil {
		return false
	}
	return entry.IsCircuitOpen() || (!entry.HasOutbound() && entry.GetLastError() != "")
}

// handleUpdateFailure applies a fetch/parse failure to subscription state.
// It ignores stale results using the same sequence guard as successful updates.
func (s *SubscriptionScheduler) handleUpdateFailure(
	ctx context.Context,
	sub *subscription.Subscription,
	attemptSeq int64,
	attemptConfigVersion int64,
	stage string,
	err error,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	applied := false
	if hook := s.beforeRefreshFailureLockHook; hook != nil {
		hook()
	}
	if err := sub.WithOpLockContext(ctx, func() {
		if ctx.Err() != nil {
			return
		}
		if s.subManager != nil && s.subManager.Lookup(sub.ID) != sub {
			return
		}
		// If refresh-input config changed while this attempt was in-flight, discard.
		if sub.ConfigVersion() != attemptConfigVersion {
			return
		}
		if sub.LastAppliedSeq() > attemptSeq {
			return
		}
		now := time.Now().UnixNano()
		sub.LastCheckedNs.Store(now)
		// A failure is a committed refresh result too. Use the same monotonic
		// sequence as success so stale failures and stale successes cannot
		// overwrite a newer outcome.
		sub.MarkAppliedAttempt(attemptSeq)
		sub.SetLastError(err.Error())
		applied = true
	}); err != nil {
		return
	}
	if !applied {
		log.Printf("[scheduler] stale %s failure ignored for %s: %v", stage, sub.ID, err)
		return
	}

	log.Printf("[scheduler] %s %s failed: %v", stage, sub.ID, err)
	if s.onSubUpdated != nil {
		s.onSubUpdated(sub)
	}
}

// SetSubscriptionEnabled updates the enabled flag and rebuilds all platform
// routable views. Disabling a subscription makes its nodes invisible to
// platform tag-regex matching; enabling makes them visible again.
func (s *SubscriptionScheduler) SetSubscriptionEnabled(sub *subscription.Subscription, enabled bool) {
	sub.WithOpLock(func() {
		s.SetSubscriptionEnabledLocked(sub, enabled)
	})
	if s.onSubUpdated != nil {
		s.onSubUpdated(sub)
	}
}

// SetSubscriptionEnabledLocked applies an enabled mutation while the caller
// already holds sub's operation lock. It keeps the runtime side-effects in
// the same critical section as the subscription mutation. Callers must use
// sub.WithOpLock before calling this method.
func (s *SubscriptionScheduler) SetSubscriptionEnabledLocked(sub *subscription.Subscription, enabled bool) {
	var reenabled []nodeRuntimePreparation
	if s.pool != nil {
		s.pool.WithRuntimeMutation(func() {
			reenabled = s.setSubscriptionEnabledLocked(sub, enabled)
		})
	} else {
		reenabled = s.setSubscriptionEnabledLocked(sub, enabled)
	}
	if hook := s.beforeReenabledRuntimeHook; hook != nil {
		hook()
	}
	for _, prep := range reenabled {
		if s.pool != nil {
			current, ok := s.pool.GetEntry(prep.hash)
			if !ok || current != prep.entry {
				continue
			}
		}
		if s.onSubReenabledNode != nil {
			s.onSubReenabledNode(prep.hash, prep.entry)
		}
		if s.pool != nil {
			// Resource preparation is deliberately outside runtimeBatchMu. The
			// callback may build an outbound and start a probe synchronously.
			s.pool.NotifyNodeDirty(prep.hash)
		}
	}
}

func (s *SubscriptionScheduler) setSubscriptionEnabledLocked(sub *subscription.Subscription, enabled bool) []nodeRuntimePreparation {
	wasEnabled := false
	var candidateHashes []node.Hash
	wasDisabled := make(map[node.Hash]struct{})
	candidateEntries := make(map[node.Hash]*node.NodeEntry)
	wasEnabled = sub.Enabled()

	if !wasEnabled && enabled {
		sub.ManagedNodes().RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
			if managed.Evicted {
				return true
			}
			candidateHashes = append(candidateHashes, h)
			if s.pool != nil && s.pool.IsNodeDisabled(h) {
				wasDisabled[h] = struct{}{}
				if entry, ok := s.pool.GetEntry(h); ok && entry != nil {
					candidateEntries[h] = entry
				}
			}
			return true
		})
	}

	sub.SetEnabled(enabled)

	// Rebuild all platform views so they pick up the visibility change.
	if s.pool != nil {
		s.pool.RebuildAllPlatforms()
	}

	var reenabled []nodeRuntimePreparation
	// On re-enable, immediately re-check node outbound/probe state for nodes
	// that actually transitioned from disabled -> enabled.
	if !wasEnabled && enabled && s.onSubReenabledNode != nil {
		for _, h := range candidateHashes {
			if _, ok := wasDisabled[h]; !ok {
				continue
			}
			if s.pool.IsNodeDisabled(h) {
				continue
			}
			// The caller performs resource preparation and the follow-up
			// notification after runtimeBatchMu is released.
			if entry := candidateEntries[h]; entry != nil {
				reenabled = append(reenabled, nodeRuntimePreparation{hash: h, entry: entry})
			}
		}
	}
	return reenabled
}

// RenameSubscription updates the subscription name and re-triggers platform
// re-evaluation for all managed nodes (since tags include the sub name).
func (s *SubscriptionScheduler) RenameSubscription(sub *subscription.Subscription, newName string) {
	sub.WithOpLock(func() {
		s.RenameSubscriptionLocked(sub, newName)
	})
}

// RenameSubscriptionLocked applies a rename while the caller already holds
// sub's operation lock. Callers must use sub.WithOpLock before calling this
// method.
func (s *SubscriptionScheduler) RenameSubscriptionLocked(sub *subscription.Subscription, newName string) {
	var runtimePreparation []nodeRuntimePreparation
	if s.pool != nil {
		s.pool.WithRuntimeMutation(func() {
			runtimePreparation = s.renameSubscriptionLocked(sub, newName)
		})
	} else {
		runtimePreparation = s.renameSubscriptionLocked(sub, newName)
	}
	for _, prep := range runtimePreparation {
		s.pool.RunNodeAddedRuntime(prep.hash, prep.entry)
	}
}

func (s *SubscriptionScheduler) renameSubscriptionLocked(sub *subscription.Subscription, newName string) []nodeRuntimePreparation {
	sub.SetName(newName)
	var runtimePreparation []nodeRuntimePreparation
	// Re-add all managed hashes to trigger platform re-filter.
	sub.ManagedNodes().RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
		if managed.Evicted {
			return true
		}
		if s.pool == nil {
			return true
		}
		entry, ok := s.pool.GetEntry(h)
		if ok {
			if prepared := s.pool.AddNodeFromSubWithPersistenceForRuntimeBatch(h, entry.RawOptions, sub.ID, nil); prepared != nil {
				runtimePreparation = append(runtimePreparation, nodeRuntimePreparation{hash: h, entry: prepared})
			}
		}
		return true
	})
	return runtimePreparation
}

func (s *SubscriptionScheduler) fetchSubscription(parent context.Context, url string) ([]byte, error) {
	if parent == nil {
		parent = context.Background()
	}
	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return nil, context.Canceled
	}
	lifetimeCtx := s.downloadCtx
	fetcher := s.Fetcher
	s.lifecycleMu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stopAfterFunc := context.AfterFunc(lifetimeCtx, cancel)
	defer func() {
		stopAfterFunc()
		cancel()
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := fetcher(ctx, url)
	if err == nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}
	return body, err
}

func (s *SubscriptionScheduler) fetchViaDownloader(ctx context.Context, url string) ([]byte, error) {
	return s.downloader.Download(ctx, url)
}
