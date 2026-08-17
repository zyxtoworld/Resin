package routing

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/puzpuzpuz/xsync/v4"
)

var (
	ErrPlatformNotFound = errors.New("platform not found")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrRouterStopped    = errors.New("router stopped")
	// ErrRuntimeGenerationBusy means a complete subscription/node/platform
	// generation is being published. Data-plane callers must fail closed
	// rather than observe a mixture of the old and new generation.
	ErrRuntimeGenerationBusy = errors.New("runtime generation busy")
)

type PoolAccessor interface {
	GetEntry(hash node.Hash) (*node.NodeEntry, bool)
	// IsNodeDisabled is checked again at the traffic-selection boundary; a
	// published platform view can lag a subscription enable/disable rebuild.
	IsNodeDisabled(hash node.Hash) bool
	GetPlatform(id string) (*platform.Platform, bool)
	GetPlatformByName(name string) (*platform.Platform, bool)
	RangePlatforms(fn func(*platform.Platform) bool)
}

// RuntimeGenerationReader is implemented by the production node pool. Route
// selection reads platform views, pool entries, and subscription-derived
// state as one generation; the optional interface keeps lightweight test pool
// fakes source-compatible while production takes the read side.
type RuntimeGenerationReader interface {
	WithRuntimeRead(func())
}

// RuntimeGenerationTryReader is the non-blocking data-plane variant. It is
// optional so lightweight pool fakes keep the historical blocking contract.
type RuntimeGenerationTryReader interface {
	TryWithRuntimeRead(func()) bool
}

// ExactEntryExecutor provides the node-lifecycle linearization point for a
// side effect tied to one captured entry generation. Implementations must
// recheck the current pointer while holding their node replacement owner and
// invoke fn only when it is still exact.
type ExactEntryExecutor interface {
	WithCurrentEntry(hash node.Hash, expected *node.NodeEntry, fn func()) bool
}

// Router handles route selection and lease management.
type Router struct {
	pool   PoolAccessor
	states *xsync.Map[string, *PlatformRoutingState]
	// lifecycleMu is the platform-routing lifetime owner. Routes, lease writes,
	// reads, and cleaner sweeps hold the read side. Platform removal first
	// unregisters the platform from the pool, then takes the write side to drain
	// old operations before deleting the complete routing state.
	lifecycleMu            sync.RWMutex
	stopped                bool
	leaseEventsMu          sync.Mutex
	leaseEventsCond        *sync.Cond
	nextLeaseEventSeq      uint64
	deliveredLeaseEventSeq uint64
	authorities            func() []string
	p2cWindow              func() time.Duration
	onLeaseEvent           LeaseEventFunc
	nodeTagResolver        func(node.Hash, *node.NodeEntry) string
	clock                  func() time.Time

	// Package-private seam for deterministic lifecycle tests. It runs after
	// RouteRequest has entered lifecycleMu's read section and resolved a
	// platform, but before it mutates or reads routing state.
	afterPlatformResolveHook func(*platform.Platform)
	// Package-private seam for proving that removal waits at the exclusive
	// lifecycle boundary after the caller has started deletion.
	beforePlatformStateRemovalLockHook func()
	// Package-private seam called after the exclusive lifecycle lock is
	// acquired, while it is still held.
	afterPlatformStateRemovalLockHook func()
	// Package-private seam used to stop immediately after a lease-delete ticket
	// is assigned, while the per-account Compute is still in progress.
	afterLeaseDeleteLinearizedHook func()
	// Package-private seam used to stop immediately after a lease-create or
	// replace ticket is assigned, while the per-account Compute is in progress.
	afterLeaseUpsertLinearizedHook func()
	// Package-private seam called after one Compute's contiguous event ticket
	// range has been assigned, before that Compute callback returns.
	afterLeaseEventBatchTicketHook func()
	// Package-private seam called inside an atomic platform read after the pool
	// existence check, while lifecycleMu.RLock is held.
	beforeAtomicPlatformReadHook func()
	// Package-private seam called after QuarantineRoute validates the exact
	// selected entry and immediately before it records the cooldown.
	beforeResponseCooldownMarkHook func()
	// Package-private seam called after Stop closes route admission and before
	// it waits for lease-event delivery.
	afterStopAdmissionHook func()
}

type RouterConfig struct {
	Pool        PoolAccessor
	Authorities func() []string
	P2CWindow   func() time.Duration
	// OnLeaseEvent is called synchronously after the lifecycle lock is released.
	// It may re-enter Router read APIs, but must not call a Router mutation that
	// can emit another lease event.
	OnLeaseEvent LeaseEventFunc
	// NodeTagResolver resolves the selected node entry to its display tag
	// ("<Sub>/<Tag>"). The entry is an identity token: implementations must
	// fail closed when it is no longer the current generation for the hash.
	// If nil, NodeTag will be empty.
	NodeTagResolver func(node.Hash, *node.NodeEntry) string
}

func NewRouter(cfg RouterConfig) *Router {
	r := &Router{
		pool:            cfg.Pool,
		states:          xsync.NewMap[string, *PlatformRoutingState](),
		authorities:     cfg.Authorities,
		p2cWindow:       cfg.P2CWindow,
		onLeaseEvent:    cfg.OnLeaseEvent,
		nodeTagResolver: cfg.NodeTagResolver,
	}
	r.leaseEventsCond = sync.NewCond(&r.leaseEventsMu)
	return r
}

func (r *Router) now() time.Time {
	if r != nil && r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

// Stop closes route and lease mutation admission. It waits for any operation
// already inside lifecycleMu, while later callers fail closed under the same
// lock. It is idempotent.
func (r *Router) Stop() {
	if r == nil {
		return
	}
	r.lifecycleMu.Lock()
	r.stopped = true
	r.lifecycleMu.Unlock()
	if hook := r.afterStopAdmissionHook; hook != nil {
		hook()
	}
	// Route/lease mutations assign their event tickets before releasing
	// lifecycleMu, but deliver callbacks afterward. Drain those already
	// admitted callbacks before reporting the router stopped; otherwise the
	// application could close dirty-write admission while a lease event is
	// still waiting to persist.
	r.WaitForLeaseEvents()
}

// RemovePlatformState linearly removes all routing state for a platform.
// Callers must unregister the platform from the pool first so new routes stop
// resolving it before this exclusive boundary drains old operations.
func (r *Router) RemovePlatformState(platformID string) {
	if r == nil || platformID == "" {
		return
	}
	if hook := r.beforePlatformStateRemovalLockHook; hook != nil {
		hook()
	}
	events := r.newLeaseEventBatch()
	r.lifecycleMu.Lock()
	if hook := r.afterPlatformStateRemovalLockHook; hook != nil {
		hook()
	}
	r.removePlatformStateUnlocked(platformID, events)
	r.lifecycleMu.Unlock()
	events.finish()
}

// removePlatformStateUnlocked removes a platform's complete routing state.
// The caller must hold lifecycleMu exclusively.
func (r *Router) removePlatformStateUnlocked(platformID string, events *leaseEventBatch) {
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		return
	}

	var accounts []string
	state.Leases.Range(func(account string, _ Lease) bool {
		accounts = append(accounts, account)
		return true
	})
	for _, account := range accounts {
		// Delete through the table so the per-IP counter stays in sync.
		r.deleteLeaseWithEvent(state, platformID, account, events)
	}
	r.states.Delete(platformID)
}

// deleteLeaseWithEvent is the common local mutation path for all removals.
// The event ticket is assigned from inside LeaseTable's Compute callback, so
// a same-account replacement cannot overtake the removal after the table has
// already committed its delete.
func (r *Router) deleteLeaseWithEvent(
	state *PlatformRoutingState,
	platformID string,
	account string,
	events *leaseEventBatch,
) bool {
	return state.Leases.deleteLeaseWith(account, func(lease Lease) {
		mutation := events.newMutation()
		mutation.add(LeaseEvent{
			Type:        LeaseRemove,
			PlatformID:  platformID,
			Account:     account,
			NodeHash:    lease.NodeHash,
			EgressIP:    lease.EgressIP,
			CreatedAtNs: lease.CreatedAtNs,
		})
		mutation.commit()
		if hook := r.afterLeaseDeleteLinearizedHook; hook != nil {
			hook()
		}
	})
}

type RouteResult struct {
	PlatformID   string
	PlatformName string
	NodeHash     node.Hash
	EgressIP     netip.Addr
	// RetryBudget is the upper bound of routable candidates captured from the
	// platform view for this route generation. Callers may retry only within
	// this fixed budget; later platform changes must not enlarge one request's
	// attempt set.
	RetryBudget                   int
	NodeTag                       string // display tag: "<Subscription>/<Tag>" (DESIGN.md §601)
	LeaseCreated                  bool
	PassiveCircuitBreakerDisabled bool
	ResponseRules                 platform.ResponseRules
	// selectedEntry is the exact pool entry that passed the platform view at
	// route selection time. Consumers must re-read the pool and compare pointer
	// identity before using the current entry; this is not a resource lease.
	selectedEntry *node.NodeEntry
	platform      *platform.Platform
	retrySnapshot *RouteRetrySnapshot
}

// SelectedEntry returns the entry identity captured during route selection.
// Callers must validate it against a fresh pool lookup before using any
// mutable or closable resource owned by the entry.
func (r RouteResult) SelectedEntry() *node.NodeEntry {
	return r.selectedEntry
}

// RouteRetryExclusions is the per-request attempted set for retry selection.
// RouteRequestNext copies these slices before using them.
type RouteRetryExclusions struct {
	Entries   []*node.NodeEntry
	EgressIPs []netip.Addr
}

type routeRetryCandidate struct {
	hash  node.Hash
	entry *node.NodeEntry
	ip    netip.Addr
}

// RouteRetrySnapshot is captured by the initial route generation. It cannot
// grow when the platform is hot-reloaded.
type RouteRetrySnapshot struct {
	platformID string
	platform   *platform.Platform
	candidates []routeRetryCandidate
	budget     int
}

// RetrySnapshot returns the immutable retry candidate generation captured by
// this route result.
func (r RouteResult) RetrySnapshot() *RouteRetrySnapshot {
	return r.retrySnapshot
}

const livePickAttempts = 2 // first pick + one retry

type leaseInvalidationReason int

const (
	leaseInvalidationNone leaseInvalidationReason = iota
	leaseInvalidationExpire
	leaseInvalidationRemove
)

func (r *Router) RouteRequest(platName, account, target string) (RouteResult, error) {
	if tryReader, ok := r.pool.(RuntimeGenerationTryReader); ok {
		var result RouteResult
		var err error
		if !tryReader.TryWithRuntimeRead(func() {
			result, err = r.routeRequest(platName, account, target, true)
		}) {
			return RouteResult{}, ErrRuntimeGenerationBusy
		}
		return result, err
	}
	if reader, ok := r.pool.(RuntimeGenerationReader); ok {
		var result RouteResult
		var err error
		reader.WithRuntimeRead(func() {
			result, err = r.routeRequest(platName, account, target, true)
		})
		return result, err
	}
	return r.routeRequest(platName, account, target, true)
}

// RouteRequestForProxy selects a route without mutating the account's sticky
// lease. Proxy response policy commits exactly one route only after the final
// response is accepted, so concurrent failed requests cannot create a
// provisional lease chain that later rollback attempts can corrupt.
func (r *Router) RouteRequestForProxy(platName, account, target string) (RouteResult, error) {
	if tryReader, ok := r.pool.(RuntimeGenerationTryReader); ok {
		var result RouteResult
		var err error
		if !tryReader.TryWithRuntimeRead(func() {
			result, err = r.routeRequest(platName, account, target, false)
		}) {
			return RouteResult{}, ErrRuntimeGenerationBusy
		}
		return result, err
	}
	if reader, ok := r.pool.(RuntimeGenerationReader); ok {
		var result RouteResult
		var err error
		reader.WithRuntimeRead(func() {
			result, err = r.routeRequest(platName, account, target, false)
		})
		return result, err
	}
	return r.routeRequest(platName, account, target, false)
}

// RouteRequestNext selects one never-attempted candidate from the immutable
// snapshot carried by previous. It does not mutate the sticky lease; callers
// promote only the route that ultimately succeeds.
func (r *Router) RouteRequestNext(previous RouteResult, exclusions RouteRetryExclusions) (RouteResult, error) {
	snapshot := previous.retrySnapshot
	if r == nil || snapshot == nil || snapshot.platform == nil {
		return RouteResult{}, ErrNoAvailableNodes
	}
	entrySet := make(map[*node.NodeEntry]struct{}, len(exclusions.Entries))
	for _, entry := range exclusions.Entries {
		if entry != nil {
			entrySet[entry] = struct{}{}
		}
	}
	ipSet := make(map[netip.Addr]struct{}, len(exclusions.EgressIPs))
	for _, ip := range exclusions.EgressIPs {
		if ip.IsValid() {
			ipSet[ip] = struct{}{}
		}
	}

	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped || r.pool == nil {
		return RouteResult{}, ErrRouterStopped
	}
	currentPlatform, ok := r.pool.GetPlatform(snapshot.platformID)
	if !ok || currentPlatform != snapshot.platform {
		return RouteResult{}, ErrNoAvailableNodes
	}
	cooldowns := r.responseCooldowns(snapshot.platformID)
	now := r.now()
	for _, candidate := range snapshot.candidates {
		if _, ok := entrySet[candidate.entry]; ok {
			continue
		}
		if _, ok := ipSet[candidate.ip]; ok {
			continue
		}
		current, ok := r.pool.GetEntry(candidate.hash)
		if !ok || current != candidate.entry || current.GetEgressIP() != candidate.ip || !current.IsHealthy() || r.pool.IsNodeDisabled(candidate.hash) || !snapshot.platform.ContainsViewEntry(candidate.hash, candidate.entry) {
			continue
		}
		if cooldowns.IsCoolingForEntry(candidate.hash, candidate.entry, candidate.ip, now) {
			continue
		}
		result := RouteResult{
			PlatformID: snapshot.platform.ID, PlatformName: snapshot.platform.Name,
			NodeHash: candidate.hash, EgressIP: candidate.ip,
			RetryBudget: snapshot.budget, ResponseRules: snapshot.platform.ResponseRules,
			selectedEntry: candidate.entry, platform: snapshot.platform, retrySnapshot: snapshot,
		}
		result = withPlatformContext(snapshot.platform, result)
		if r.nodeTagResolver != nil {
			result.NodeTag = r.nodeTagResolver(result.NodeHash, result.selectedEntry)
		}
		return result, nil
	}
	return RouteResult{}, ErrNoAvailableNodes
}

// CommitRouteForAccount publishes an accepted proxy route as the sticky owner.
// It is exact-entry guarded so a late response cannot write a new generation's
// lease.
func (r *Router) CommitRouteForAccount(route RouteResult, account string) bool {
	if r == nil || account == "" || route.platform == nil || route.selectedEntry == nil {
		return false
	}
	now := r.now()
	expiry, err := platform.StickyLeaseExpiryUnixNano(now, route.platform.StickyTTLNs)
	if err != nil {
		return false
	}
	events := r.newLeaseEventBatch()
	r.lifecycleMu.RLock()
	if r.stopped || r.pool == nil {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false
	}
	currentPlatform, platformOK := r.pool.GetPlatform(route.platform.ID)
	currentEntry, entryOK := r.pool.GetEntry(route.NodeHash)
	if !platformOK || currentPlatform != route.platform || !entryOK || currentEntry != route.selectedEntry || !currentEntry.IsHealthy() || r.pool.IsNodeDisabled(route.NodeHash) || !route.platform.ContainsViewEntry(route.NodeHash, route.selectedEntry) {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false
	}
	r.upsertLeaseUnlocked(route.platform.ID, account, Lease{
		NodeHash: route.NodeHash, EgressIP: route.EgressIP,
		CreatedAtNs: now.UnixNano(), LastAccessedNs: now.UnixNano(), ExpiryNs: expiry,
	}, events)
	r.lifecycleMu.RUnlock()
	events.finish()
	return true
}

// RouteRequestForPlatform routes with an already captured platform generation.
// The caller must hold the platform publication read owner while this method
// runs; that owner keeps account-policy extraction and route selection on one
// complete platform object without re-entering the platform lookup lock.
func (r *Router) RouteRequestForPlatform(
	plat *platform.Platform,
	account string,
	target string,
) (RouteResult, error) {
	if r == nil || plat == nil {
		return RouteResult{}, ErrPlatformNotFound
	}
	if tryReader, ok := r.pool.(RuntimeGenerationTryReader); ok {
		var result RouteResult
		var err error
		if !tryReader.TryWithRuntimeRead(func() {
			result, err = r.routeRequestForPlatform(plat, account, target, true)
		}) {
			return RouteResult{}, ErrRuntimeGenerationBusy
		}
		return result, err
	}
	if reader, ok := r.pool.(RuntimeGenerationReader); ok {
		var result RouteResult
		var err error
		reader.WithRuntimeRead(func() {
			result, err = r.routeRequestForPlatform(plat, account, target, true)
		})
		return result, err
	}
	return r.routeRequestForPlatform(plat, account, target, true)
}

// RouteRequestForProxyForPlatform is the no-side-effect counterpart used by
// reverse proxy requests after they captured a platform generation.
func (r *Router) RouteRequestForProxyForPlatform(plat *platform.Platform, account, target string) (RouteResult, error) {
	if r == nil || plat == nil {
		return RouteResult{}, ErrPlatformNotFound
	}
	if tryReader, ok := r.pool.(RuntimeGenerationTryReader); ok {
		var result RouteResult
		var err error
		if !tryReader.TryWithRuntimeRead(func() {
			result, err = r.routeRequestForPlatform(plat, account, target, false)
		}) {
			return RouteResult{}, ErrRuntimeGenerationBusy
		}
		return result, err
	}
	if reader, ok := r.pool.(RuntimeGenerationReader); ok {
		var result RouteResult
		var err error
		reader.WithRuntimeRead(func() {
			result, err = r.routeRequestForPlatform(plat, account, target, false)
		})
		return result, err
	}
	return r.routeRequestForPlatform(plat, account, target, false)
}

func (r *Router) routeRequest(platName, account, target string, commitLease bool) (RouteResult, error) {
	var events *leaseEventBatch
	if commitLease && account != "" {
		events = r.newLeaseEventBatch()
	}
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return RouteResult{}, ErrRouterStopped
	}

	plat, err := r.resolvePlatform(platName)
	if err != nil {
		r.lifecycleMu.RUnlock()
		events.finish()
		return RouteResult{}, err
	}
	return r.routeRequestLocked(plat, account, target, events, commitLease)
}

func (r *Router) routeRequestForPlatform(plat *platform.Platform, account, target string, commitLease bool) (RouteResult, error) {
	var events *leaseEventBatch
	if commitLease && account != "" {
		events = r.newLeaseEventBatch()
	}
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return RouteResult{}, ErrRouterStopped
	}
	return r.routeRequestLocked(plat, account, target, events, commitLease)
}

func (r *Router) routeRequestLocked(
	plat *platform.Platform,
	account string,
	target string,
	events *leaseEventBatch,
	commitLease bool,
) (RouteResult, error) {
	if hook := r.afterPlatformResolveHook; hook != nil {
		hook(plat)
	}

	targetDomain := netutil.ExtractDomain(target)
	state := r.ensurePlatformState(plat.ID)
	snapshot := r.captureRetrySnapshot(plat, state.ResponseCooldowns, r.now())
	retryBudget := snapshot.budget
	var result RouteResult
	var err error
	if account == "" {
		result, err = r.routeRandom(plat, state, targetDomain)
	} else if commitLease {
		result, err = r.routeSticky(plat, state, account, targetDomain, r.now(), events)
	} else {
		result, err = r.routeStickyReadOnly(plat, state, account, targetDomain, r.now())
	}
	r.lifecycleMu.RUnlock()
	events.finish()
	if err != nil {
		return RouteResult{}, err
	}
	result.RetryBudget = retryBudget
	result.platform = plat
	result.retrySnapshot = snapshot
	result = withPlatformContext(plat, result)
	if r.nodeTagResolver != nil {
		result.NodeTag = r.nodeTagResolver(result.NodeHash, result.selectedEntry)
	}
	return result, nil
}

func (r *Router) captureRetrySnapshot(plat *platform.Platform, cooldowns *ResponseCooldowns, now time.Time) *RouteRetrySnapshot {
	snapshot := &RouteRetrySnapshot{platform: plat}
	if plat == nil || r.pool == nil {
		snapshot.budget = 1
		return snapshot
	}
	snapshot.platformID = plat.ID
	plat.RangeViewEntries(func(hash node.Hash, published *node.NodeEntry) bool {
		if published == nil || !published.IsHealthy() || r.pool.IsNodeDisabled(hash) {
			return true
		}
		ip := published.GetEgressIP()
		if !ip.IsValid() || (cooldowns != nil && cooldowns.IsCoolingForEntry(hash, published, ip, now)) {
			return true
		}
		snapshot.candidates = append(snapshot.candidates, routeRetryCandidate{hash: hash, entry: published, ip: ip})
		return true
	})
	snapshot.budget = len(snapshot.candidates)
	if snapshot.budget < 1 {
		snapshot.budget = 1
	}
	return snapshot
}

func withPlatformContext(plat *platform.Platform, res RouteResult) RouteResult {
	res.PlatformID = plat.ID
	res.PlatformName = plat.Name
	res.PassiveCircuitBreakerDisabled = plat.PassiveCircuitBreakerDisabled
	res.ResponseRules = plat.ResponseRules
	return res
}

func (r *Router) resolvePlatform(platName string) (*platform.Platform, error) {
	if platName == "" {
		if p, ok := r.pool.GetPlatform(platform.DefaultPlatformID); ok {
			return p, nil
		}
		return nil, ErrPlatformNotFound
	}
	p, ok := r.pool.GetPlatformByName(platName)
	if !ok {
		return nil, ErrPlatformNotFound
	}
	return p, nil
}

func (r *Router) ensurePlatformState(platformID string) *PlatformRoutingState {
	state, _ := r.states.LoadOrCompute(platformID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})
	return state
}

// platformExistsLocked reports whether the platform is still registered in the
// pool. The caller must hold lifecycleMu for reading or writing.
func (r *Router) platformExistsLocked(platformID string) bool {
	if r == nil || r.pool == nil || platformID == "" {
		return false
	}
	_, ok := r.pool.GetPlatform(platformID)
	return ok
}

func (r *Router) routeRandom(
	plat *platform.Platform,
	state *PlatformRoutingState,
	targetDomain string,
) (RouteResult, error) {
	h, entry, err := r.selectLiveRandomRoute(plat, state.IPLoadStats, targetDomain)
	if err != nil {
		return RouteResult{}, err
	}
	return RouteResult{
		NodeHash:      h,
		EgressIP:      entry.GetEgressIP(),
		LeaseCreated:  false,
		selectedEntry: entry,
	}, nil
}

func (r *Router) routeSticky(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
	events *leaseEventBatch,
) (RouteResult, error) {
	nowNs := now.UnixNano()
	var result RouteResult
	var routeErr error

	_, _ = state.Leases.leases.Compute(account, func(current Lease, loaded bool) (Lease, xsync.ComputeOp) {
		mutation := events.newMutation()
		newLease, op, routeResult, err := r.decideStickyLease(
			plat,
			state,
			account,
			targetDomain,
			now,
			nowNs,
			current,
			loaded,
			mutation,
		)
		if err != nil {
			routeErr = err
			mutation.commit()
			return newLease, op
		}
		result = routeResult
		mutation.commit()
		return newLease, op
	})

	return result, routeErr
}

func (r *Router) routeStickyReadOnly(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
) (RouteResult, error) {
	current, loaded := state.Leases.GetLease(account)
	if loaded && !current.IsExpired(now) {
		if result, ok := r.tryLeaseHitReadOnly(plat, state, current, now); ok {
			return result, nil
		}
		if result, ok := r.tryLeaseSameIPRotationReadOnly(plat, state, current, targetDomain, now); ok {
			return result, nil
		}
	}
	return r.routeRandom(plat, state, targetDomain)
}

func (r *Router) tryLeaseHitReadOnly(
	plat *platform.Platform,
	state *PlatformRoutingState,
	current Lease,
	now time.Time,
) (RouteResult, bool) {
	entry, ok := r.pool.GetEntry(current.NodeHash)
	if !ok || entry == nil || !entry.IsHealthy() || r.pool.IsNodeDisabled(current.NodeHash) || !plat.ContainsViewEntry(current.NodeHash, entry) || entry.GetEgressIP() != current.EgressIP || state.ResponseCooldowns.IsCoolingForEntry(current.NodeHash, entry, current.EgressIP, now) {
		return RouteResult{}, false
	}
	return RouteResult{
		NodeHash:      current.NodeHash,
		EgressIP:      current.EgressIP,
		LeaseCreated:  false,
		selectedEntry: entry,
	}, true
}

func (r *Router) tryLeaseSameIPRotationReadOnly(
	plat *platform.Platform,
	state *PlatformRoutingState,
	current Lease,
	targetDomain string,
	now time.Time,
) (RouteResult, bool) {
	bestHash, bestEntry, ok := chooseSameIPRotationCandidateWithCooldownEntryAt(
		plat,
		r.pool,
		current.EgressIP,
		targetDomain,
		r.authorities(),
		r.p2cWindow(),
		state.ResponseCooldowns,
		now,
	)
	if !ok {
		return RouteResult{}, false
	}
	return RouteResult{
		NodeHash:      bestHash,
		EgressIP:      current.EgressIP,
		LeaseCreated:  false,
		selectedEntry: bestEntry,
	}, true
}

func (r *Router) decideStickyLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
	nowNs int64,
	current Lease,
	loaded bool,
	events leaseEventSink,
) (Lease, xsync.ComputeOp, RouteResult, error) {
	hadPreviousLease := loaded
	invalidation := leaseInvalidationNone

	if loaded && current.IsExpired(now) {
		invalidation = leaseInvalidationExpire
		loaded = false
	}

	if loaded {
		if newLease, hitResult, ok := r.tryLeaseHit(plat, state, account, current, nowNs, events); ok {
			return newLease, xsync.UpdateOp, hitResult, nil
		}
		if newLease, rotatedResult, ok := r.tryLeaseSameIPRotation(plat, state, account, current, targetDomain, nowNs, events); ok {
			return newLease, xsync.UpdateOp, rotatedResult, nil
		}
		invalidation = leaseInvalidationRemove
	}

	return r.createOrAbortStickyLease(
		plat,
		state,
		account,
		targetDomain,
		now,
		nowNs,
		current,
		hadPreviousLease,
		invalidation,
		events,
	)
}

func (r *Router) createOrAbortStickyLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
	nowNs int64,
	previous Lease,
	hadPreviousLease bool,
	invalidation leaseInvalidationReason,
	events leaseEventSink,
) (Lease, xsync.ComputeOp, RouteResult, error) {
	newLease, createdResult, err := r.createLease(plat, state, targetDomain, now, nowNs)
	if err != nil {
		r.cleanupPreviousLease(state, previous, hadPreviousLease, invalidation, plat.ID, account, events)
		lease, op := abortLeaseCreate(previous, hadPreviousLease)
		return lease, op, RouteResult{}, err
	}
	// Windows clocks can expose coarse Unix-nanosecond values. A recreated
	// lease must still carry a new expiry even when the old and new calculations
	// land in the same clock tick.
	if hadPreviousLease && newLease.ExpiryNs == previous.ExpiryNs {
		if newLease.ExpiryNs < math.MaxInt64 {
			newLease.ExpiryNs++
		} else if newLease.ExpiryNs-1 > nowNs {
			// MaxInt64 cannot be incremented. Keep the replacement distinct
			// while preserving the expiry-after-now contract.
			newLease.ExpiryNs--
		} else {
			return Lease{}, xsync.CancelOp, RouteResult{}, fmt.Errorf("cannot produce a distinct non-expired lease expiry")
		}
		createdResult.LeaseCreated = true
	}

	r.cleanupPreviousLease(state, previous, hadPreviousLease, invalidation, plat.ID, account, events)
	state.IPLoadStats.Inc(newLease.EgressIP)
	if events != nil {
		events.add(LeaseEvent{
			Type:       LeaseCreate,
			PlatformID: plat.ID,
			Account:    account,
			NodeHash:   newLease.NodeHash,
			EgressIP:   newLease.EgressIP,
		})
	}
	return newLease, xsync.UpdateOp, createdResult, nil
}

func (r *Router) tryLeaseHit(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	current Lease,
	nowNs int64,
	events leaseEventSink,
) (Lease, RouteResult, bool) {
	entry, ok := r.pool.GetEntry(current.NodeHash)
	if !ok || entry == nil || !entry.IsHealthy() || r.pool.IsNodeDisabled(current.NodeHash) || !plat.ContainsViewEntry(current.NodeHash, entry) || entry.GetEgressIP() != current.EgressIP ||
		state.ResponseCooldowns.IsCoolingForEntry(current.NodeHash, entry, current.EgressIP, r.now()) {
		return Lease{}, RouteResult{}, false
	}

	newLease := current
	newLease.LastAccessedNs = nowNs
	if events != nil {
		events.add(LeaseEvent{
			Type:       LeaseTouch,
			PlatformID: plat.ID,
			Account:    account,
			NodeHash:   current.NodeHash,
			EgressIP:   current.EgressIP,
		})
	}
	return newLease, RouteResult{
		NodeHash:      current.NodeHash,
		EgressIP:      current.EgressIP,
		LeaseCreated:  false,
		selectedEntry: entry,
	}, true
}

func (r *Router) tryLeaseSameIPRotation(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	current Lease,
	targetDomain string,
	nowNs int64,
	events leaseEventSink,
) (Lease, RouteResult, bool) {
	bestHash, bestEntry, ok := chooseSameIPRotationCandidateWithCooldownEntryAt(
		plat,
		r.pool,
		current.EgressIP,
		targetDomain,
		r.authorities(),
		r.p2cWindow(),
		state.ResponseCooldowns,
		r.now(),
	)
	if !ok {
		return Lease{}, RouteResult{}, false
	}

	newLease := current
	newLease.NodeHash = bestHash
	newLease.LastAccessedNs = nowNs
	if events != nil {
		events.add(LeaseEvent{
			Type:       LeaseReplace,
			PlatformID: plat.ID,
			Account:    account,
			NodeHash:   bestHash,
			EgressIP:   current.EgressIP,
		})
	}
	return newLease, RouteResult{
		NodeHash:      bestHash,
		EgressIP:      current.EgressIP,
		LeaseCreated:  false,
		selectedEntry: bestEntry,
	}, true
}

func (r *Router) createLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	targetDomain string,
	now time.Time,
	nowNs int64,
) (Lease, RouteResult, error) {
	h, entry, err := r.selectLiveRandomRoute(plat, state.IPLoadStats, targetDomain)
	if err != nil {
		return Lease{}, RouteResult{}, err
	}
	ttl := plat.StickyTTLNs
	if ttl <= 0 {
		ttl = int64(24 * time.Hour) // Default safeguard
	}
	expiryNs, err := platform.StickyLeaseExpiryUnixNano(now, ttl)
	if err != nil {
		return Lease{}, RouteResult{}, fmt.Errorf("invalid sticky ttl: %w", err)
	}

	lease := Lease{
		NodeHash:       h,
		EgressIP:       entry.GetEgressIP(),
		CreatedAtNs:    nowNs,
		ExpiryNs:       expiryNs,
		LastAccessedNs: nowNs,
	}
	return lease, RouteResult{
		NodeHash:      lease.NodeHash,
		EgressIP:      lease.EgressIP,
		LeaseCreated:  true,
		selectedEntry: entry,
	}, nil
}

func (r *Router) cleanupPreviousLease(
	state *PlatformRoutingState,
	lease Lease,
	hadPreviousLease bool,
	invalidation leaseInvalidationReason,
	platformID string,
	account string,
	events leaseEventSink,
) {
	if !hadPreviousLease {
		return
	}
	state.Leases.stats.Dec(lease.EgressIP)
	if events == nil {
		return
	}
	switch invalidation {
	case leaseInvalidationExpire:
		events.add(LeaseEvent{
			Type:        LeaseExpire,
			PlatformID:  platformID,
			Account:     account,
			NodeHash:    lease.NodeHash,
			EgressIP:    lease.EgressIP,
			CreatedAtNs: lease.CreatedAtNs,
		})
	case leaseInvalidationRemove:
		events.add(LeaseEvent{
			Type:        LeaseRemove,
			PlatformID:  platformID,
			Account:     account,
			NodeHash:    lease.NodeHash,
			EgressIP:    lease.EgressIP,
			CreatedAtNs: lease.CreatedAtNs,
		})
	}
}

func abortLeaseCreate(current Lease, hadPreviousLease bool) (Lease, xsync.ComputeOp) {
	if hadPreviousLease {
		return current, xsync.DeleteOp
	}
	return current, xsync.CancelOp
}

func (r *Router) selectLiveRandomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	targetDomain string,
) (node.Hash, *node.NodeEntry, error) {
	var lastMissing node.Hash
	for i := 0; i < livePickAttempts; i++ {
		h, err := randomRouteFiltered(plat, stats, r.pool, targetDomain, r.authorities(), r.p2cWindow(), func(hash node.Hash) bool {
			entry, ok := r.pool.GetEntry(hash)
			if !ok || entry == nil || !entry.IsHealthy() || r.pool.IsNodeDisabled(hash) || !plat.ContainsViewEntry(hash, entry) {
				return false
			}
			return !r.responseCooldowns(plat.ID).IsCoolingForEntry(hash, entry, entry.GetEgressIP(), r.now())
		})
		if err != nil {
			return node.Zero, nil, err
		}
		entry, ok := r.pool.GetEntry(h)
		if ok && entry != nil && entry.IsHealthy() && !r.pool.IsNodeDisabled(h) && plat.ContainsViewEntry(h, entry) {
			return h, entry, nil
		}
		lastMissing = h
	}
	if lastMissing != node.Zero {
		return node.Zero, nil, fmt.Errorf("%w: selected node %s no longer in pool", ErrNoAvailableNodes, lastMissing.Hex())
	}
	return node.Zero, nil, ErrNoAvailableNodes
}

func chooseSameIPRotationCandidate(
	plat *platform.Platform,
	pool PoolAccessor,
	targetIP netip.Addr,
	targetDomain string,
	authorities []string,
	window time.Duration,
) (node.Hash, bool) {
	h, _, ok := chooseSameIPRotationCandidateWithCooldownEntry(plat, pool, targetIP, targetDomain, authorities, window, nil)
	return h, ok
}

func chooseSameIPRotationCandidateWithCooldown(
	plat *platform.Platform,
	pool PoolAccessor,
	targetIP netip.Addr,
	targetDomain string,
	authorities []string,
	window time.Duration,
	cooldowns *ResponseCooldowns,
) (node.Hash, bool) {
	h, _, ok := chooseSameIPRotationCandidateWithCooldownEntry(plat, pool, targetIP, targetDomain, authorities, window, cooldowns)
	return h, ok
}

func chooseSameIPRotationCandidateWithCooldownEntry(
	plat *platform.Platform,
	pool PoolAccessor,
	targetIP netip.Addr,
	targetDomain string,
	authorities []string,
	window time.Duration,
	cooldowns *ResponseCooldowns,
) (node.Hash, *node.NodeEntry, bool) {
	return chooseSameIPRotationCandidateWithCooldownEntryAt(
		plat,
		pool,
		targetIP,
		targetDomain,
		authorities,
		window,
		cooldowns,
		time.Now(),
	)
}

func chooseSameIPRotationCandidateWithCooldownEntryAt(
	plat *platform.Platform,
	pool PoolAccessor,
	targetIP netip.Addr,
	targetDomain string,
	authorities []string,
	window time.Duration,
	cooldowns *ResponseCooldowns,
	now time.Time,
) (node.Hash, *node.NodeEntry, bool) {
	bestKnownHash := node.Zero
	var bestKnownEntry *node.NodeEntry
	bestKnownLatency := time.Duration(math.MaxInt64)
	fallbackHash := node.Zero
	var fallbackEntry *node.NodeEntry

	plat.RangeViewEntries(func(h node.Hash, publishedEntry *node.NodeEntry) bool {
		entry, ok := pool.GetEntry(h)
		if !ok || entry == nil || entry != publishedEntry || !entry.IsHealthy() || pool.IsNodeDisabled(h) || entry.GetEgressIP() != targetIP || (cooldowns != nil && cooldowns.IsCoolingForEntry(h, entry, targetIP, now)) {
			return true
		}
		if fallbackHash == node.Zero {
			fallbackHash = h
			fallbackEntry = entry
		}

		latency, hasLatency := sameIPCandidateLatency(entry, targetDomain, authorities, window)
		if hasLatency && latency < bestKnownLatency {
			bestKnownLatency = latency
			bestKnownHash = h
			bestKnownEntry = entry
		}
		return true
	})

	if bestKnownHash != node.Zero {
		return bestKnownHash, bestKnownEntry, true
	}
	if fallbackHash != node.Zero {
		return fallbackHash, fallbackEntry, true
	}
	return node.Zero, nil, false
}

func (r *Router) responseCooldowns(platformID string) *ResponseCooldowns {
	return r.ensurePlatformState(platformID).ResponseCooldowns
}

// QuarantineRoute records a response-driven cooldown for a route. Cooldowns
// are in-memory and expire automatically when routing checks the deadline.
func (r *Router) QuarantineRoute(route RouteResult, scope platform.ResponseRuleScope, until time.Time) {
	if r == nil || route.PlatformID == "" {
		return
	}
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped || r.pool == nil {
		return
	}
	if route.platform == nil {
		return
	}
	currentPlatform, ok := r.pool.GetPlatform(route.PlatformID)
	if !ok || currentPlatform != route.platform {
		return
	}
	// A response can arrive after the node hash has been removed and rebuilt.
	// The route carries the exact entry selected by the router; do not let a
	// late response from that retired generation cool its replacement.
	if route.selectedEntry == nil {
		return
	}
	current, ok := r.pool.GetEntry(route.NodeHash)
	if !ok || current != route.selectedEntry {
		return
	}
	if hook := r.beforeResponseCooldownMarkHook; hook != nil {
		hook()
	}
	mark := func() {
		r.responseCooldowns(route.PlatformID).markForEntry(scope, route.NodeHash, route.selectedEntry, route.EgressIP, until, r.now())
	}
	if executor, ok := r.pool.(ExactEntryExecutor); ok {
		executor.WithCurrentEntry(route.NodeHash, route.selectedEntry, mark)
		return
	}
	// Test and narrow custom PoolAccessor implementations may not expose the
	// lifecycle owner. Preserve the exact-pointer fail-closed behavior for
	// those accessors; production GlobalNodePool implements the atomic path.
	if current, ok := r.pool.GetEntry(route.NodeHash); !ok || current != route.selectedEntry {
		return
	}
	mark()
}

func sameIPCandidateLatency(
	entry *node.NodeEntry,
	targetDomain string,
	authorities []string,
	window time.Duration,
) (time.Duration, bool) {
	now := time.Now()
	if latency, ok := lookupRecentDomainLatency(entry, targetDomain, now, window); ok {
		return latency, true
	}

	if latency, ok := averageRecentAuthorityLatency(entry, authorities, now, window); ok {
		return latency, true
	}
	return 0, false
}

// readLeaseUnlocked reads a lease from routing state. The caller must hold
// lifecycleMu for reading or writing.
func (r *Router) readLeaseUnlocked(key model.LeaseKey) *model.Lease {
	state, ok := r.states.Load(key.PlatformID)
	if !ok {
		return nil
	}
	lease, ok := state.Leases.GetLease(key.Account)
	if !ok {
		return nil
	}
	return &model.Lease{
		PlatformID:     key.PlatformID,
		Account:        key.Account,
		NodeHash:       lease.NodeHash.Hex(),
		EgressIP:       lease.EgressIP.String(),
		CreatedAtNs:    lease.CreatedAtNs,
		ExpiryNs:       lease.ExpiryNs,
		LastAccessedNs: lease.LastAccessedNs,
	}
}

// listLeasesUnlocked snapshots all leases for a platform. The caller must
// hold lifecycleMu for reading or writing.
func (r *Router) listLeasesUnlocked(platformID string) []model.Lease {
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		return []model.Lease{}
	}
	result := make([]model.Lease, 0, state.Leases.Size())
	state.Leases.Range(func(account string, lease Lease) bool {
		result = append(result, model.Lease{
			PlatformID:     platformID,
			Account:        account,
			NodeHash:       lease.NodeHash.Hex(),
			EgressIP:       lease.EgressIP.String(),
			CreatedAtNs:    lease.CreatedAtNs,
			ExpiryNs:       lease.ExpiryNs,
			LastAccessedNs: lease.LastAccessedNs,
		})
		return true
	})
	return result
}

// snapshotIPLoadUnlocked snapshots a platform's IP load. The caller must
// hold lifecycleMu for reading or writing.
func (r *Router) snapshotIPLoadUnlocked(platformID string) map[netip.Addr]int64 {
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		return map[netip.Addr]int64{}
	}
	return state.IPLoadStats.Snapshot()
}

// ReadLease implements weak persistence read.
func (r *Router) ReadLease(key model.LeaseKey) *model.Lease {
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped {
		return nil
	}
	return r.readLeaseUnlocked(key)
}

// ReadLeaseForPersistence reads the last live routing state for cache flushes.
// Unlike the runtime API, it remains available after Router.Stop so the final
// cache flush can persist active leases. Platform removal still wins because
// RemovePlatformState deletes the state under the same lifecycle lock.
func (r *Router) ReadLeaseForPersistence(key model.LeaseKey) *model.Lease {
	if r == nil {
		return nil
	}
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.pool != nil {
		if _, ok := r.pool.GetPlatform(key.PlatformID); !ok {
			return nil
		}
	}
	return r.readLeaseUnlocked(key)
}

// ListLeasesForPlatform atomically checks platform lifetime and snapshots its
// leases. The boolean is false when the platform is not registered or Router
// mutation admission has been stopped; a true value with an empty slice means
// the platform has no routing state or leases.
func (r *Router) ListLeasesForPlatform(platformID string) ([]model.Lease, bool) {
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		return nil, false
	}
	if !r.platformExistsLocked(platformID) {
		r.lifecycleMu.RUnlock()
		return nil, false
	}
	if hook := r.beforeAtomicPlatformReadHook; hook != nil {
		hook()
	}
	result := r.listLeasesUnlocked(platformID)
	r.lifecycleMu.RUnlock()
	return result, true
}

// ReadLeaseForPlatform atomically checks platform lifetime and reads one
// lease. The boolean is false when the platform is not registered or Router
// mutation admission has been stopped; a true value with a nil lease means the
// platform has no such lease.
func (r *Router) ReadLeaseForPlatform(key model.LeaseKey) (*model.Lease, bool) {
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		return nil, false
	}
	if !r.platformExistsLocked(key.PlatformID) {
		r.lifecycleMu.RUnlock()
		return nil, false
	}
	lease := r.readLeaseUnlocked(key)
	r.lifecycleMu.RUnlock()
	return lease, true
}

// UpsertLease writes or replaces a lease for (platform_id, account).
// It updates per-IP lease counters and emits LeaseCreate/LeaseReplace events.
func (r *Router) UpsertLease(ml model.Lease) error {
	platformID := strings.TrimSpace(ml.PlatformID)
	if platformID == "" {
		return errors.New("platform_id is required")
	}
	account := strings.TrimSpace(ml.Account)
	if account == "" {
		return errors.New("account is required")
	}

	h, err := node.ParseHex(ml.NodeHash)
	if err != nil {
		return fmt.Errorf("parse node_hash: %w", err)
	}
	ip, err := netip.ParseAddr(ml.EgressIP)
	if err != nil {
		return fmt.Errorf("parse egress_ip: %w", err)
	}

	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		return ErrRouterStopped
	}
	if !r.platformExistsLocked(platformID) {
		r.lifecycleMu.RUnlock()
		return ErrPlatformNotFound
	}

	lease := Lease{
		NodeHash:       h,
		EgressIP:       ip,
		CreatedAtNs:    ml.CreatedAtNs,
		ExpiryNs:       ml.ExpiryNs,
		LastAccessedNs: ml.LastAccessedNs,
	}

	events := r.newLeaseEventBatch()
	r.upsertLeaseUnlocked(platformID, account, lease, events)
	r.lifecycleMu.RUnlock()
	events.finish()
	return nil
}

// upsertLeaseUnlocked replaces one lease and appends its event ticket. The
// caller must hold lifecycleMu for reading or writing.
func (r *Router) upsertLeaseUnlocked(
	platformID string,
	account string,
	lease Lease,
	events *leaseEventBatch,
) {
	state := r.ensurePlatformState(platformID)
	eventType := LeaseCreate
	_, _ = state.Leases.leases.Compute(account, func(current Lease, loaded bool) (Lease, xsync.ComputeOp) {
		mutation := events.newMutation()
		if loaded {
			state.Leases.stats.Dec(current.EgressIP)
			eventType = LeaseReplace
		}
		state.Leases.stats.Inc(lease.EgressIP)
		mutation.add(LeaseEvent{
			Type:       eventType,
			PlatformID: platformID,
			Account:    account,
			NodeHash:   lease.NodeHash,
			EgressIP:   lease.EgressIP,
		})
		mutation.commit()
		if hook := r.afterLeaseUpsertLinearizedHook; hook != nil {
			hook()
		}
		return lease, xsync.UpdateOp
	})
}

// SnapshotIPLoad returns a best-effort point-in-time IP load snapshot for a platform.
// If the platform has no routing state yet, it returns an empty snapshot.
func (r *Router) SnapshotIPLoad(platformID string) map[netip.Addr]int64 {
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped {
		return map[netip.Addr]int64{}
	}
	return r.snapshotIPLoadUnlocked(platformID)
}

// SnapshotIPLoadForPlatform atomically checks platform lifetime and snapshots
// its IP load. The boolean is false when the platform is not registered or
// Router mutation admission has been stopped; a true value with an empty map
// means no routing state or active leases.
func (r *Router) SnapshotIPLoadForPlatform(platformID string) (map[netip.Addr]int64, bool) {
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		return nil, false
	}
	if !r.platformExistsLocked(platformID) {
		r.lifecycleMu.RUnlock()
		return nil, false
	}
	snapshot := r.snapshotIPLoadUnlocked(platformID)
	r.lifecycleMu.RUnlock()
	return snapshot, true
}

// InheritLeaseForPlatform validates the parent and creates/replaces the child
// inside one lifecycle read section. The platform check and the lease mutation
// therefore cannot be split by platform removal.
func (r *Router) InheritLeaseForPlatform(
	platformID string,
	parentAccount string,
	newAccount string,
	now time.Time,
) error {
	return r.inheritLeaseForPlatform(nil, platformID, parentAccount, newAccount, now)
}

// InheritLeaseForPlatformExact performs the same mutation while requiring the
// platform object captured by the caller to still be the object published in
// the pool. The identity check is made under the routing lifecycle read lock,
// before the lease state is read or changed.
func (r *Router) InheritLeaseForPlatformExact(
	expected *platform.Platform,
	parentAccount string,
	newAccount string,
	now time.Time,
) error {
	if expected == nil {
		return ErrPlatformNotFound
	}
	return r.inheritLeaseForPlatform(expected, expected.ID, parentAccount, newAccount, now)
}

func (r *Router) inheritLeaseForPlatform(
	expected *platform.Platform,
	platformID string,
	parentAccount string,
	newAccount string,
	now time.Time,
) error {
	events := r.newLeaseEventBatch()
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return ErrRouterStopped
	}
	if expected == nil {
		if !r.platformExistsLocked(platformID) {
			r.lifecycleMu.RUnlock()
			events.finish()
			return ErrPlatformNotFound
		}
	} else {
		if r.pool == nil {
			r.lifecycleMu.RUnlock()
			events.finish()
			return ErrPlatformNotFound
		}
		current, ok := r.pool.GetPlatform(platformID)
		if !ok || current != expected {
			r.lifecycleMu.RUnlock()
			events.finish()
			return ErrPlatformNotFound
		}
	}
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		r.lifecycleMu.RUnlock()
		events.finish()
		return ErrLeaseNotFound
	}
	parent, ok := state.Leases.GetLease(parentAccount)
	if !ok || parent.ExpiryNs <= now.UnixNano() {
		r.lifecycleMu.RUnlock()
		events.finish()
		return ErrLeaseNotFound
	}
	r.upsertLeaseUnlocked(platformID, newAccount, parent, events)
	r.lifecycleMu.RUnlock()
	events.finish()
	return nil
}

// RestoreLeases restores leases from persistence during bootstrap.
func (r *Router) RestoreLeases(leases []model.Lease) {
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped || r.pool == nil {
		return
	}

	for _, ml := range leases {
		h, err := node.ParseHex(ml.NodeHash)
		if err != nil {
			continue
		}
		ip, err := netip.ParseAddr(ml.EgressIP)
		if err != nil {
			continue
		}

		if _, ok := r.pool.GetPlatform(ml.PlatformID); !ok {
			continue
		}
		state, _ := r.states.LoadOrCompute(ml.PlatformID, func() (*PlatformRoutingState, bool) {
			return NewPlatformRoutingState(), false
		})

		l := Lease{
			NodeHash:       h,
			EgressIP:       ip,
			CreatedAtNs:    ml.CreatedAtNs,
			ExpiryNs:       ml.ExpiryNs,
			LastAccessedNs: ml.LastAccessedNs,
		}
		// Directly insert into table and stats.
		state.Leases.CreateLease(ml.Account, l)
	}
}

// RangeLeases iterates over all leases for a registered platform.
// Returns false if the platform is not registered or has no routing state.
func (r *Router) RangeLeases(platformID string, fn func(account string, lease Lease) bool) bool {
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped {
		return false
	}
	if !r.platformExistsLocked(platformID) {
		return false
	}

	state, ok := r.states.Load(platformID)
	if !ok {
		return false
	}
	state.Leases.Range(fn)
	return true
}

// DeleteLease removes a single lease by platform and account.
// Returns true if a lease was deleted. Emits a LeaseRemove event.
func (r *Router) DeleteLease(platformID, account string) bool {
	events := r.newLeaseEventBatch()
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false
	}

	state, ok := r.states.Load(platformID)
	if !ok {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false
	}
	deleted := r.deleteLeaseWithEvent(state, platformID, account, events)
	if !deleted {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false
	}
	r.lifecycleMu.RUnlock()
	events.finish()
	return true
}

// DeleteLeaseForPlatform atomically checks platform lifetime and removes one
// lease. The second result is false only when the platform is not registered;
// a true value with deleted=false means the platform exists but the lease does
// not.
func (r *Router) DeleteLeaseForPlatform(platformID, account string) (deleted bool, platformExists bool) {
	events := r.newLeaseEventBatch()
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false, false
	}
	if !r.platformExistsLocked(platformID) {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false, false
	}
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		r.lifecycleMu.RUnlock()
		events.finish()
		return false, true
	}
	deleted = r.deleteLeaseWithEvent(state, platformID, account, events)
	r.lifecycleMu.RUnlock()
	events.finish()
	return deleted, true
}

// DeleteAllLeases removes all leases for a platform.
// Returns the number of leases deleted. Emits a LeaseRemove event for each.
func (r *Router) DeleteAllLeases(platformID string) int {
	events := r.newLeaseEventBatch()
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return 0
	}

	state, ok := r.states.Load(platformID)
	if !ok {
		r.lifecycleMu.RUnlock()
		events.finish()
		return 0
	}
	count := 0
	state.Leases.Range(func(account string, _ Lease) bool {
		if r.deleteLeaseWithEvent(state, platformID, account, events) {
			count++
		}
		return true
	})
	r.lifecycleMu.RUnlock()
	events.finish()
	return count
}

// DeleteAllLeasesForPlatform atomically checks platform lifetime and removes
// every lease in its routing state. A zero count with platformExists=true is a
// valid empty-platform result.
func (r *Router) DeleteAllLeasesForPlatform(platformID string) (count int, platformExists bool) {
	events := r.newLeaseEventBatch()
	r.lifecycleMu.RLock()
	if r.stopped {
		r.lifecycleMu.RUnlock()
		events.finish()
		return 0, false
	}
	if !r.platformExistsLocked(platformID) {
		r.lifecycleMu.RUnlock()
		events.finish()
		return 0, false
	}
	state, ok := r.states.Load(platformID)
	if ok && state != nil {
		state.Leases.Range(func(account string, _ Lease) bool {
			if r.deleteLeaseWithEvent(state, platformID, account, events) {
				count++
			}
			return true
		})
	}
	r.lifecycleMu.RUnlock()
	events.finish()
	return count, true
}
