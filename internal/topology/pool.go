// Package topology coordinates the subscription → node pool → platform view pipeline.
// It owns the GlobalNodePool, PlatformManager, and SubscriptionManager,
// breaking import cycles between the leaf packages (node, subscription, platform).
package topology

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/puzpuzpuz/xsync/v4"
)

// GlobalNodePool is the system's single source of truth for nodes.
// It uses xsync.Map for concurrent access and xsync.Compute for atomic
// AddNodeFromSub / RemoveNodeFromSub operations.
type GlobalNodePool struct {
	nodes *xsync.Map[node.Hash, *node.NodeEntry]
	// runtimeBatchMu is the read/write owner for cross-object runtime
	// generations. A subscription refresh swaps ManagedNodes and publishes
	// platform views through several pool operations; readers that combine
	// those objects must hold the read side for the whole observation.
	runtimeBatchMu runtimeBatchGate
	// nodeLifecycleMu orders pool entry replacement with the callbacks that
	// release resources owned by the removed entry. Without this owner, a new
	// entry for the same hash can be created while the old callback is still
	// closing/evicting resources, so cleanup can hit the replacement.
	nodeLifecycleMu sync.Mutex

	// Platform references for dirty-notify.
	// platformBatchMu is the cancellation-aware admission owner for platform
	// map writers and context-aware rebuild readers. The ordinary platMu still
	// protects the indexes; this owner makes a rebuild cancelable before it
	// waits behind a long platform replacement rebuild.
	platformBatchMu runtimeBatchGate
	platMu          sync.RWMutex
	platformByID    map[string]*platform.Platform // id -> platform
	platformByName  map[string]*platform.Platform // name -> platform
	platformGen     uint64                        // protected by platMu; changes invalidate notify snapshots
	// defaultPlatform is published only after a complete Default platform
	// object has been registered/replaced. Its view is immutable to readers;
	// later replacements publish a different pointer after rebuilding it.
	defaultPlatform atomic.Pointer[platform.Platform]
	// Package-private test seam immediately before the runtime read owner is
	// acquired. Production leaves it nil.
	beforeRuntimeReadLockHook func()

	// Package-private test seam for the deterministic replacement boundary.
	afterPlatformRebuildHook func()
	// Package-private test seam immediately before ReplacePlatform attempts
	// the platform publication write lock. Production leaves it nil.
	beforePlatformReplaceLockHook func()
	// Package-private test seam called immediately before platformSnapshot
	// attempts its read lock. It must not change the snapshot result.
	beforePlatformSnapshotLockHook func()
	// Package-private test seam called after platformSnapshot has copied the
	// platform pointers while still holding platMu. Production leaves it nil.
	afterPlatformSnapshotHook func()
	// Package-private test seam called immediately before a captured platform
	// receives a dirty notification. Production leaves it nil.
	beforePlatformNotifyHook func(*platform.Platform)
	// Package-private test seam called before a dirty notification waits for the
	// platform publication owner. Production leaves this nil.
	beforePlatformNotifyLockHook func()
	// Package-private test seam called after a node DeleteOp has been published
	// by nodes.Compute, but before RemoveNodeFromSub runs external cleanup.
	// Production leaves it nil. The exact entry must already reject new
	// outbound leases at this point.
	afterNodeDeleteComputeHook func(node.Hash, *node.NodeEntry)
	// Package-private test seam called after RebuildAllPlatforms captures its
	// platform snapshot and before it starts rebuilding any captured object.
	// Production leaves it nil.
	beforePlatformRebuildAllHook func()
	// Package-private test seam immediately before an out-of-band node runtime
	// preparation callback. Production leaves it nil.
	beforeNodeAddedRuntimeHook func(node.Hash, *node.NodeEntry)
	// Package-private test seam immediately before a context-aware platform
	// rebuild attempts the platform read lock. Production leaves it nil.
	beforePlatformReadLockHook func()

	// Subscription lookup — injected by SubscriptionManager.
	subLookup func(subID string) *subscription.Subscription

	// GeoIP lookup — injected at construction.
	geoLookup platform.GeoLookupFunc

	// Persistence callbacks (optional, nil in tests without persistence).
	onNodeAdded                       func(hash node.Hash) // called after a new node is created
	onNodeAddedWithPersistence        func(hash node.Hash, admission PersistenceAdmission)
	onNodeAddedRuntime                func(hash node.Hash, expected *node.NodeEntry) // runtime preparation; runs outside nodeLifecycleMu
	onNodeRemoved                     func(hash node.Hash, entry *node.NodeEntry)    // called after a node is deleted from pool
	onSubNodeChanged                  func(subID string, hash node.Hash, added bool)
	onSubNodeChangedWithPersistence   func(subID string, hash node.Hash, added bool, admission PersistenceAdmission)
	onFinalNodeRemoved                func(subID string, hash node.Hash, entry *node.NodeEntry) // called for the last subscription reference removal; replaces the ordinary remove callback
	onFinalNodeRemovedWithPersistence func(subID string, hash node.Hash, entry *node.NodeEntry, admission PersistenceAdmission)

	// Health callbacks (optional).
	onNodeDynamicChanged func(hash node.Hash)                // fired on circuit/failure/egress changes
	onNodeLatencyChanged func(hash node.Hash, domain string) // fired on latency upserts and evictions

	// Health config
	maxLatencyTableEntries int
	maxConsecutiveFailures func() int
	latencyDecayWindow     func() time.Duration
	latencyAuthorities     func() []string
}

// PersistenceAdmission is the already-admitted writer for one compound
// topology mutation. Pool callbacks must use it for every dirty mark emitted
// by that mutation instead of opening a second global admission.
type PersistenceAdmission interface {
	MarkNodeStatic(hash string) bool
	MarkNodeStaticDelete(hash string) bool
	MarkNodeDynamic(hash string) bool
	MarkNodeDynamicDelete(hash string) bool
	MarkNodeLatency(nodeHash, domain string) bool
	MarkNodeLatencyDelete(nodeHash, domain string) bool
	MarkSubscriptionNode(subID, nodeHash string) bool
	MarkSubscriptionNodeDelete(subID, nodeHash string) bool
}

// PlatformMutation is the exclusive platform publication capability supplied
// by WithPlatformMutationContext. Its methods may only be used inside that
// callback; the capability keeps the pool writer owner across the state
// persistence and runtime publication halves of one control-plane mutation.
type PlatformMutation interface {
	ValidatePlatformRegistration(string, string) error
	ValidatePlatformReplacement(string, string) error
	RegisterPlatform(*platform.Platform) error
	ReplacePlatform(*platform.Platform) error
	UnregisterPlatform(string) error
}

type platformMutation struct {
	mu   sync.Mutex
	pool *GlobalNodePool
}

// PoolConfig configures the GlobalNodePool.
type PoolConfig struct {
	SubLookup                  func(subID string) *subscription.Subscription
	GeoLookup                  platform.GeoLookupFunc
	OnNodeAdded                func(hash node.Hash)
	OnNodeAddedWithPersistence func(hash node.Hash, admission PersistenceAdmission)
	// OnNodeAddedRuntime prepares external runtime resources for a newly
	// created node. It runs after the pool lifecycle mutation is unlocked, so
	// arbitrary builder/runtime work cannot block unrelated node mutations.
	OnNodeAddedRuntime              func(hash node.Hash, expected *node.NodeEntry)
	OnNodeRemoved                   func(hash node.Hash, entry *node.NodeEntry)
	OnSubNodeChanged                func(subID string, hash node.Hash, added bool)
	OnSubNodeChangedWithPersistence func(subID string, hash node.Hash, added bool, admission PersistenceAdmission)
	// OnFinalNodeRemoved is the single persistence callback for removing the
	// last subscription reference from a node. It replaces
	// OnSubNodeChanged(..., false) for that operation so callers can keep the
	// subscription-node and node-level deletes in one admission transaction.
	OnFinalNodeRemoved                func(subID string, hash node.Hash, entry *node.NodeEntry)
	OnFinalNodeRemovedWithPersistence func(subID string, hash node.Hash, entry *node.NodeEntry, admission PersistenceAdmission)
	OnNodeDynamicChanged              func(hash node.Hash)
	OnNodeLatencyChanged              func(hash node.Hash, domain string)
	MaxLatencyTableEntries            int
	MaxConsecutiveFailures            func() int
	LatencyDecayWindow                func() time.Duration
	LatencyAuthorities                func() []string
}

var (
	// ErrInvalidPlatform indicates that a platform cannot be registered in the
	// runtime pool because it has no usable identity.
	ErrInvalidPlatform = errors.New("invalid platform")
	// ErrPlatformAlreadyRegistered indicates that the platform ID is already
	// owned by another runtime object.
	ErrPlatformAlreadyRegistered = errors.New("platform already registered")
	// ErrPlatformNotRegistered indicates the target platform ID is not registered.
	ErrPlatformNotRegistered = errors.New("platform not registered")
	// ErrPlatformNameConflict indicates another platform already uses the target name.
	ErrPlatformNameConflict = errors.New("platform name conflict")
	// ErrNoAvailableOutbound means that no enabled, healthy node in the
	// requested platform view currently has an outbound ready for use.
	ErrNoAvailableOutbound = errors.New("no available outbound")
	// ErrRuntimeGenerationBusy means a complete runtime generation is being
	// published and a non-blocking data-plane read was rejected.
	ErrRuntimeGenerationBusy = errors.New("runtime generation busy")
)

// NewGlobalNodePool creates a new GlobalNodePool.
func NewGlobalNodePool(cfg PoolConfig) *GlobalNodePool {
	maxConsecutiveFailuresFn := cfg.MaxConsecutiveFailures
	if maxConsecutiveFailuresFn == nil {
		panic("topology: NewGlobalNodePool requires non-nil MaxConsecutiveFailures")
	}

	return &GlobalNodePool{
		nodes:                             xsync.NewMap[node.Hash, *node.NodeEntry](),
		subLookup:                         cfg.SubLookup,
		geoLookup:                         cfg.GeoLookup,
		onNodeAdded:                       cfg.OnNodeAdded,
		onNodeAddedWithPersistence:        cfg.OnNodeAddedWithPersistence,
		onNodeAddedRuntime:                cfg.OnNodeAddedRuntime,
		onNodeRemoved:                     cfg.OnNodeRemoved,
		onSubNodeChanged:                  cfg.OnSubNodeChanged,
		onSubNodeChangedWithPersistence:   cfg.OnSubNodeChangedWithPersistence,
		onFinalNodeRemoved:                cfg.OnFinalNodeRemoved,
		onFinalNodeRemovedWithPersistence: cfg.OnFinalNodeRemovedWithPersistence,
		onNodeDynamicChanged:              cfg.OnNodeDynamicChanged,
		onNodeLatencyChanged:              cfg.OnNodeLatencyChanged,
		maxLatencyTableEntries:            cfg.MaxLatencyTableEntries,
		maxConsecutiveFailures:            maxConsecutiveFailuresFn,
		latencyDecayWindow:                cfg.LatencyDecayWindow,
		latencyAuthorities:                cfg.LatencyAuthorities,
		platformByID:                      make(map[string]*platform.Platform),
		platformByName:                    make(map[string]*platform.Platform),
	}
}

// AddNodeFromSub adds a node to the pool with the given subscription reference.
// Uses xsync.Compute for atomic load-or-create + ref-update.
// Idempotent: adding the same (hash, subID) pair multiple times is safe.
// After mutation, notifies all platforms to re-evaluate the node.
func (p *GlobalNodePool) AddNodeFromSub(hash node.Hash, rawOpts json.RawMessage, subID string) {
	p.addNodeFromSub(hash, rawOpts, subID, nil, true)
}

// AddNodeFromSubWithPersistence applies one subscription mutation while
// passing its already-admitted persistence writer to every configured
// writer-aware synchronous dirty callback. Legacy callbacks are used only
// when their writer-aware counterpart is not configured.
func (p *GlobalNodePool) AddNodeFromSubWithPersistence(
	hash node.Hash,
	rawOpts json.RawMessage,
	subID string,
	admission PersistenceAdmission,
) {
	p.addNodeFromSub(hash, rawOpts, subID, admission, true)
}

// AddNodeFromSubWithPersistenceForRuntimeBatch applies the pool membership
// mutation without running the external node-runtime preparation callback.
// The scheduler calls RunNodeAddedRuntime after its runtime publication lock
// is released, so builders/probes cannot block the global generation owner.
func (p *GlobalNodePool) AddNodeFromSubWithPersistenceForRuntimeBatch(
	hash node.Hash,
	rawOpts json.RawMessage,
	subID string,
	admission PersistenceAdmission,
) *node.NodeEntry {
	return p.addNodeFromSub(hash, rawOpts, subID, admission, false)
}

func (p *GlobalNodePool) addNodeFromSub(
	hash node.Hash,
	rawOpts json.RawMessage,
	subID string,
	admission PersistenceAdmission,
	runRuntime bool,
) *node.NodeEntry {
	p.nodeLifecycleMu.Lock()
	isNew := false
	var runtimeEntry *node.NodeEntry
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded {
			createdAt := time.Now()
			entry = node.NewNodeEntry(hash, rawOpts, createdAt, p.maxLatencyTableEntries)
			// New subscription nodes start as circuit-open and must be proven healthy by probes.
			entry.CircuitOpenSince.Store(createdAt.UnixNano())
			isNew = true
			runtimeEntry = entry
		}
		entry.AddSubscriptionID(subID)
		return entry, xsync.UpdateOp
	})

	if isNew {
		if admission != nil && p.onNodeAddedWithPersistence != nil {
			p.onNodeAddedWithPersistence(hash, admission)
		} else if p.onNodeAdded != nil {
			p.onNodeAdded(hash)
		}
	}
	if admission != nil && p.onSubNodeChangedWithPersistence != nil {
		p.onSubNodeChangedWithPersistence(subID, hash, true, admission)
	} else if p.onSubNodeChanged != nil {
		p.onSubNodeChanged(subID, hash, true)
	}
	p.nodeLifecycleMu.Unlock()
	if isNew && runRuntime {
		p.runNodeAddedRuntime(hash, runtimeEntry)
	}

	p.notifyAllPlatformsDirty(hash)
	return runtimeEntry
}

// RemoveNodeFromSub removes a subscription reference from a node.
// If the node has no remaining references, it is deleted from the pool.
// Uses xsync.Compute for atomic ref-update + conditional delete.
// Idempotent: removing a nonexistent (hash, subID) pair is safe.
func (p *GlobalNodePool) RemoveNodeFromSub(hash node.Hash, subID string) {
	p.removeNodeFromSub(hash, subID, nil, nil)
}

// RemoveNodeFromSubWithPersistence applies one subscription mutation while
// passing its already-admitted persistence writer to every configured
// writer-aware synchronous dirty callback. Legacy callbacks are used only
// when their writer-aware counterpart is not configured.
func (p *GlobalNodePool) RemoveNodeFromSubWithPersistence(
	hash node.Hash,
	subID string,
	admission PersistenceAdmission,
) {
	p.removeNodeFromSub(hash, subID, nil, admission)
}

// removeNodeFromSub performs removal with an optional per-mutation persistence
// callback. A non-nil callback runs before the pool-wide callback for this one
// synchronous operation; all other removal side effects keep their normal
// ownership.
func (p *GlobalNodePool) removeNodeFromSub(
	hash node.Hash,
	subID string,
	onSubNodeChanged func(string, node.Hash, bool),
	admission PersistenceAdmission,
) {
	p.nodeLifecycleMu.Lock()
	lifecycleLocked := true
	defer func() {
		if lifecycleLocked {
			p.nodeLifecycleMu.Unlock()
		}
	}()
	entry, loaded := p.nodes.Load(hash)
	healthEventLocked := loaded && entry != nil
	if healthEventLocked {
		entry.LockHealthEvent()
		defer func() {
			if healthEventLocked {
				entry.UnlockHealthEvent()
			}
		}()
	}
	wasDeleted := false
	var deletedEntry *node.NodeEntry // capture entry before map deletion
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded {
			return entry, xsync.CancelOp // idempotent no-op
		}
		empty := entry.RemoveSubscriptionID(subID)
		if empty {
			wasDeleted = true
			deletedEntry = entry
			// Retire before returning DeleteOp. xsync publishes the deletion
			// after this callback returns; the map-removal linearization point
			// must not expose an entry that can still admit new outbound use.
			entry.RetireOutbound()
			return nil, xsync.DeleteOp
		}
		return entry, xsync.UpdateOp
	})

	if wasDeleted && deletedEntry != nil {
		if hook := p.afterNodeDeleteComputeHook; hook != nil {
			hook(hash, deletedEntry)
		}
	}
	if wasDeleted && admission != nil && p.onFinalNodeRemovedWithPersistence != nil {
		p.onFinalNodeRemovedWithPersistence(subID, hash, deletedEntry, admission)
	} else if wasDeleted && p.onFinalNodeRemoved != nil {
		p.onFinalNodeRemoved(subID, hash, deletedEntry)
	} else {
		if onSubNodeChanged != nil {
			onSubNodeChanged(subID, hash, false)
		}
		if admission != nil && p.onSubNodeChangedWithPersistence != nil {
			p.onSubNodeChangedWithPersistence(subID, hash, false, admission)
		} else if p.onSubNodeChanged != nil {
			p.onSubNodeChanged(subID, hash, false)
		}
	}
	if wasDeleted && p.onNodeRemoved != nil {
		p.onNodeRemoved(hash, deletedEntry)
	}
	if healthEventLocked {
		entry.UnlockHealthEvent()
		healthEventLocked = false
	}
	p.nodeLifecycleMu.Unlock()
	lifecycleLocked = false

	p.notifyAllPlatformsDirty(hash)
}

// GetEntry retrieves a node entry by hash.
func (p *GlobalNodePool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	return p.nodes.Load(hash)
}

// WithNodeLifecycle runs fn while pool add/remove mutations and their external
// cleanup callbacks are excluded. Shutdown owners use this boundary to avoid
// taking a snapshot between a node DeleteOp and its onNodeRemoved callback.
func (p *GlobalNodePool) WithNodeLifecycle(fn func()) {
	if p == nil || fn == nil {
		return
	}
	p.nodeLifecycleMu.Lock()
	defer p.nodeLifecycleMu.Unlock()
	fn()
}

// WithCurrentEntry runs fn only while the captured entry is still the live
// generation for hash. The node lifecycle owner serializes this check with
// AddNodeFromSub/RemoveNodeFromSub, so a stale response cannot publish a
// side effect after replacement.
func (p *GlobalNodePool) WithCurrentEntry(hash node.Hash, expected *node.NodeEntry, fn func()) bool {
	if p == nil || expected == nil || fn == nil {
		return false
	}
	p.nodeLifecycleMu.Lock()
	defer p.nodeLifecycleMu.Unlock()
	current, ok := p.nodes.Load(hash)
	if !ok || current != expected {
		return false
	}
	fn()
	return true
}

// Range iterates all nodes in the pool.
func (p *GlobalNodePool) Range(fn func(node.Hash, *node.NodeEntry) bool) {
	p.nodes.Range(fn)
}

// Size returns the number of nodes in the pool.
func (p *GlobalNodePool) Size() int {
	return p.nodes.Size()
}

// WithRuntimeMutation publishes one complete subscription/node/platform
// generation. The callback must not perform a runtime read that waits for the
// same owner.
func (p *GlobalNodePool) WithRuntimeMutation(fn func()) {
	_ = p.WithRuntimeMutationContext(context.Background(), fn)
}

// WithRuntimeMutationContext publishes one complete runtime generation unless
// ctx is canceled while waiting for readers or another writer. Cancellation is
// observed before the callback, so a canceled caller cannot publish a batch.
func (p *GlobalNodePool) WithRuntimeMutationContext(ctx context.Context, fn func()) error {
	if p == nil || fn == nil {
		return nil
	}
	if err := p.runtimeBatchMu.writeLockContext(ctx); err != nil {
		return err
	}
	defer p.runtimeBatchMu.writeUnlock()
	fn()
	return nil
}

// WithRuntimeRead observes one complete subscription/node/platform
// generation. It is the read-side counterpart of WithRuntimeMutation.
func (p *GlobalNodePool) WithRuntimeRead(fn func()) {
	_ = p.WithRuntimeReadContext(context.Background(), fn)
}

// WithRuntimeReadContext observes one complete runtime generation while
// allowing a request-bound caller to abandon admission if a refresh owns the
// write side. Cancellation is only effective before the read owner is
// admitted; an admitted callback still runs to completion.
func (p *GlobalNodePool) WithRuntimeReadContext(ctx context.Context, fn func()) error {
	if p == nil || fn == nil {
		return nil
	}
	if hook := p.beforeRuntimeReadLockHook; hook != nil {
		hook()
	}
	if err := p.runtimeBatchMu.readLockContext(ctx); err != nil {
		return err
	}
	defer p.runtimeBatchMu.readUnlock()
	fn()
	return nil
}

// TryWithRuntimeRead runs fn only when a complete runtime generation can be
// admitted immediately. Data-plane callers use this to fail closed during a
// long refresh notification instead of reading a half-published generation.
func (p *GlobalNodePool) TryWithRuntimeRead(fn func()) bool {
	if p == nil || fn == nil || !p.runtimeBatchMu.tryReadLock() {
		return false
	}
	defer p.runtimeBatchMu.readUnlock()
	fn()
	return true
}

// LoadNodeFromBootstrap inserts a node during bootstrap recovery.
// No dirty-marks, no platform notifications.
func (p *GlobalNodePool) LoadNodeFromBootstrap(entry *node.NodeEntry) {
	p.nodes.Store(entry.Hash, entry)
}

// WithPlatformMutationContext admits one exclusive platform publication
// owner. The callback must keep all state persistence and runtime publication
// for the same control-plane mutation inside this owner. Cancellation is
// observed while waiting for the owner; once admitted, the callback runs to
// completion under its caller-owned commit context.
func (p *GlobalNodePool) WithPlatformMutationContext(ctx context.Context, fn func(PlatformMutation) error) error {
	if p == nil || fn == nil {
		return nil
	}
	if err := p.platformBatchMu.writeLockContext(ctx); err != nil {
		return err
	}
	defer p.platformBatchMu.writeUnlock()
	owner := &platformMutation{pool: p}
	defer func() {
		owner.mu.Lock()
		owner.pool = nil
		owner.mu.Unlock()
	}()
	return fn(owner)
}

var ErrPlatformMutationDone = errors.New("platform mutation owner is done")

func (m *platformMutation) withPool(fn func(*GlobalNodePool) error) error {
	if m == nil {
		return ErrPlatformMutationDone
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pool == nil {
		return ErrPlatformMutationDone
	}
	return fn(m.pool)
}

func (m *platformMutation) ValidatePlatformRegistration(id, name string) error {
	return m.withPool(func(p *GlobalNodePool) error {
		p.platMu.RLock()
		defer p.platMu.RUnlock()
		if _, exists := p.platformByID[id]; exists {
			return ErrPlatformAlreadyRegistered
		}
		if name != "" {
			if _, exists := p.platformByName[name]; exists {
				return ErrPlatformNameConflict
			}
		}
		return nil
	})
}

func (m *platformMutation) ValidatePlatformReplacement(id, name string) error {
	return m.withPool(func(p *GlobalNodePool) error {
		p.platMu.RLock()
		defer p.platMu.RUnlock()
		if _, exists := p.platformByID[id]; !exists {
			return ErrPlatformNotRegistered
		}
		if name != "" {
			if existing, exists := p.platformByName[name]; exists && existing != nil && existing.ID != id {
				return ErrPlatformNameConflict
			}
		}
		return nil
	})
}

func (m *platformMutation) RegisterPlatform(plat *platform.Platform) error {
	return m.withPool(func(p *GlobalNodePool) error {
		if plat == nil || plat.ID == "" {
			return ErrInvalidPlatform
		}

		p.platMu.RLock()
		if _, exists := p.platformByID[plat.ID]; exists {
			p.platMu.RUnlock()
			return ErrPlatformAlreadyRegistered
		}
		if plat.Name != "" {
			if _, exists := p.platformByName[plat.Name]; exists {
				p.platMu.RUnlock()
				return ErrPlatformNameConflict
			}
		}
		p.platMu.RUnlock()

		// Build the complete view before publishing the platform into either
		// index. The platform mutation owner prevents another map writer from
		// overtaking this candidate, while readers continue to see the existing
		// platform indexes during the slow scan.
		p.RebuildPlatform(plat)

		p.platMu.Lock()
		defer p.platMu.Unlock()
		// Recheck after the candidate build. The owner normally makes this
		// redundant, but keeping the publication boundary self-contained makes
		// the invariant explicit and protects future callers.
		if _, exists := p.platformByID[plat.ID]; exists {
			return ErrPlatformAlreadyRegistered
		}
		if plat.Name != "" {
			if _, exists := p.platformByName[plat.Name]; exists {
				return ErrPlatformNameConflict
			}
		}
		p.platformByID[plat.ID] = plat
		if plat.Name != "" {
			p.platformByName[plat.Name] = plat
		}
		if plat.ID == platform.DefaultPlatformID {
			p.defaultPlatform.Store(plat)
		}
		p.platformGen++
		return nil
	})
}

// RegisterPlatform adds a platform to receive dirty notifications.
//
// Registration is strict: it never replaces an existing ID and never
// overwrites an existing name index. Callers must handle the returned error.
func (p *GlobalNodePool) RegisterPlatform(plat *platform.Platform) error {
	return p.WithPlatformMutationContext(context.Background(), func(m PlatformMutation) error {
		return m.RegisterPlatform(plat)
	})
}

func (m *platformMutation) UnregisterPlatform(id string) error {
	return m.withPool(func(p *GlobalNodePool) error {
		if id == platform.DefaultPlatformID {
			return nil
		}
		p.platMu.Lock()
		defer p.platMu.Unlock()
		plat, ok := p.platformByID[id]
		if !ok {
			return nil
		}
		delete(p.platformByID, id)
		if plat.Name != "" {
			// Only delete when name still points at this platform.
			if current, ok := p.platformByName[plat.Name]; ok && current == plat {
				delete(p.platformByName, plat.Name)
			}
		}
		p.platformGen++
		return nil
	})
}

// UnregisterPlatform removes a platform from dirty notifications.
func (p *GlobalNodePool) UnregisterPlatform(id string) {
	_ = p.WithPlatformMutationContext(context.Background(), func(m PlatformMutation) error {
		return m.UnregisterPlatform(id)
	})
}

func (m *platformMutation) ReplacePlatform(next *platform.Platform) error {
	return m.withPool(func(p *GlobalNodePool) error {
		if next == nil || next.ID == "" {
			return ErrPlatformNotRegistered
		}

		p.platMu.RLock()
		current, ok := p.platformByID[next.ID]
		if !ok {
			p.platMu.RUnlock()
			return ErrPlatformNotRegistered
		}

		if next.Name != "" {
			if existingByName, exists := p.platformByName[next.Name]; exists && existingByName != current {
				p.platMu.RUnlock()
				return ErrPlatformNameConflict
			}
		}
		p.platMu.RUnlock()

		// Construct the complete replacement view while the old platform stays
		// published. Route readers therefore continue using the old complete
		// generation instead of waiting for slow GeoIP/health evaluation.
		p.RebuildPlatform(next)
		if hook := p.afterPlatformRebuildHook; hook != nil {
			hook()
		}

		if hook := p.beforePlatformReplaceLockHook; hook != nil {
			hook()
		}
		p.platMu.Lock()
		defer p.platMu.Unlock()

		current, ok = p.platformByID[next.ID]
		if !ok {
			return ErrPlatformNotRegistered
		}
		if next.Name != "" {
			if existingByName, exists := p.platformByName[next.Name]; exists && existingByName != current {
				return ErrPlatformNameConflict
			}
		}

		p.platformByID[next.ID] = next

		if current.Name != "" {
			if mapped, exists := p.platformByName[current.Name]; exists && mapped == current {
				delete(p.platformByName, current.Name)
			}
		}
		if next.Name != "" {
			p.platformByName[next.Name] = next
		}
		if next.ID == platform.DefaultPlatformID {
			p.defaultPlatform.Store(next)
		}
		p.platformGen++

		return nil
	})
}

// ReplacePlatform atomically replaces an existing platform object by ID.
// It follows a copy-on-write update path: the caller builds a new Platform
// instance, and the platform mutation owner serializes candidate rebuilds with
// dirty notifications. The platMu critical section is limited to the final
// map-pointer swap, so route readers keep seeing the old complete generation
// while the candidate is built.
func (p *GlobalNodePool) ReplacePlatform(next *platform.Platform) error {
	return p.WithPlatformMutationContext(context.Background(), func(m PlatformMutation) error {
		return m.ReplacePlatform(next)
	})
}

// GetPlatform retrieves a platform by ID.
func (p *GlobalNodePool) GetPlatform(id string) (*platform.Platform, bool) {
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByID[id]
	return plat, ok
}

// GetPlatformByName retrieves a platform by Name.
func (p *GlobalNodePool) GetPlatformByName(name string) (*platform.Platform, bool) {
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByName[name]
	return plat, ok
}

// WithPlatformReadByName runs fn while the named platform generation is held
// under the platform publication read lock. The callback must not re-enter a
// platform lookup; it is for an atomic account-policy plus route observation.
// A blank name selects the built-in Default platform.
func (p *GlobalNodePool) WithPlatformReadByName(name string, fn func(*platform.Platform)) bool {
	if p == nil || fn == nil {
		return false
	}
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	var plat *platform.Platform
	var ok bool
	if name == "" {
		plat, ok = p.platformByID[platform.DefaultPlatformID]
	} else {
		plat, ok = p.platformByName[name]
	}
	if !ok || plat == nil {
		return false
	}
	fn(plat)
	return true
}

// SnapshotPlatformViewEntries returns one published platform view together
// with the exact NodeEntry identity that satisfied each view member. The
// platform publication lock covers the platform lookup and view copy, so a
// replacement cannot split the platform pointer from its view generation.
// Callers must still re-check the pool entry identity before using a member,
// because nodes may be removed or recreated after this method returns.
func (p *GlobalNodePool) SnapshotPlatformViewEntries(
	id string,
) ([]platform.RoutableViewEntry, bool) {
	if p == nil {
		return nil, false
	}
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByID[id]
	if !ok || plat == nil {
		return nil, false
	}
	return plat.SnapshotViewEntries(), true
}

// RangePlatforms iterates all registered platforms.
func (p *GlobalNodePool) RangePlatforms(fn func(*platform.Platform) bool) {
	for _, plat := range p.platformSnapshot() {
		if !fn(plat) {
			return
		}
	}
}

func (p *GlobalNodePool) platformSnapshot() []*platform.Platform {
	platforms, _ := p.platformSnapshotWithGeneration()
	return platforms
}

func (p *GlobalNodePool) platformSnapshotWithGeneration() ([]*platform.Platform, uint64) {
	if hook := p.beforePlatformSnapshotLockHook; hook != nil {
		hook()
	}
	p.platMu.RLock()
	defer p.platMu.RUnlock()

	platforms := make([]*platform.Platform, 0, len(p.platformByID))
	for _, plat := range p.platformByID {
		platforms = append(platforms, plat)
	}
	if hook := p.afterPlatformSnapshotHook; hook != nil {
		hook()
	}
	return platforms, p.platformGen
}

// MakeSubLookup builds the SubLookupFunc closure for MatchRegexs / tag resolution.
func (p *GlobalNodePool) MakeSubLookup() node.SubLookupFunc {
	return func(subID string, hash node.Hash) (string, bool, []string, bool) {
		// Compatibility fallback for test wiring that omits SubLookup.
		// We cannot resolve subscription metadata, so treat the reference as
		// "present+enabled" without tags.
		if p.subLookup == nil {
			return "", true, nil, true
		}

		sub := p.subLookup(subID)
		if sub == nil {
			return "", false, nil, false
		}

		managed, ok := sub.ManagedNodes().LoadNode(hash)
		if !ok || managed.Evicted {
			return "", false, nil, false
		}
		tags := managed.Tags
		return sub.Name(), sub.Enabled(), tags, true
	}
}

// ResolveNodeDisplayTag resolves a current node hash to its display tag for
// request logs. The node lifecycle lock keeps the entry and its subscription
// references on one node generation while the tag is copied.
// Rule:
//  1. Prefer enabled subscriptions: among enabled holders, choose earliest-created.
//  2. Within that subscription, choose lexicographically smallest tag.
//  3. If no enabled holder exists, fallback to all holders with the same rule.
//  4. Return "<SubscriptionName>/<Tag>".
//
// Returns empty string when resolution is not possible.
func (p *GlobalNodePool) ResolveNodeDisplayTag(hash node.Hash) string {
	p.nodeLifecycleMu.Lock()
	defer p.nodeLifecycleMu.Unlock()

	entry, ok := p.nodes.Load(hash)
	if !ok || entry == nil {
		return ""
	}
	return p.resolveNodeDisplayTagLocked(hash, entry)
}

// ResolveNodeDisplayTagForEntry resolves a tag only for the exact entry
// selected by a route. A removed or recreated hash is intentionally reported
// as empty rather than attributing the request to a different generation.
func (p *GlobalNodePool) ResolveNodeDisplayTagForEntry(hash node.Hash, expected *node.NodeEntry) string {
	if p == nil || expected == nil {
		return ""
	}
	p.nodeLifecycleMu.Lock()
	defer p.nodeLifecycleMu.Unlock()

	current, ok := p.nodes.Load(hash)
	if !ok || current != expected {
		return ""
	}
	return p.resolveNodeDisplayTagLocked(hash, current)
}

func (p *GlobalNodePool) resolveNodeDisplayTagLocked(hash node.Hash, entry *node.NodeEntry) string {
	if p.subLookup == nil {
		return ""
	}

	subIDs := entry.SubscriptionIDs()
	if len(subIDs) == 0 {
		return ""
	}

	pick := func(enabledOnly bool) (string, bool) {
		bestFound := false
		var bestCreatedAtNs int64
		var bestSubID string
		var bestSubName string
		var bestTag string

		for _, subID := range subIDs {
			sub := p.subLookup(subID)
			if sub == nil {
				continue
			}
			if enabledOnly && !sub.Enabled() {
				continue
			}

			managed, ok := sub.ManagedNodes().LoadNode(hash)
			if !ok || managed.Evicted {
				continue
			}
			tags := managed.Tags
			if len(tags) == 0 {
				continue
			}

			smallestTag := tags[0]
			for _, tag := range tags[1:] {
				if tag < smallestTag {
					smallestTag = tag
				}
			}

			createdAtNs := sub.CreatedAtNs
			if !bestFound ||
				createdAtNs < bestCreatedAtNs ||
				(createdAtNs == bestCreatedAtNs && subID < bestSubID) {
				bestFound = true
				bestCreatedAtNs = createdAtNs
				bestSubID = subID
				bestSubName = sub.Name()
				bestTag = smallestTag
			}
		}

		if !bestFound || bestSubName == "" || bestTag == "" {
			return "", false
		}
		return bestSubName + "/" + bestTag, true
	}

	if tag, ok := pick(true); ok {
		return tag
	}
	if tag, ok := pick(false); ok {
		return tag
	}
	return ""
}

// IsNodeDisabled reports whether a node is disabled by subscription state:
// all referencing subscriptions are disabled (or missing / not applicable).
func (p *GlobalNodePool) IsNodeDisabled(hash node.Hash) bool {
	entry, ok := p.GetEntry(hash)
	if !ok || entry == nil {
		return true
	}
	return entry.IsDisabledBySubscriptions(p.MakeSubLookup())
}

// MakeHealthyAndEnabledEvaluator builds a predicate for pool-context health
// aggregates: the node must not be disabled by subscription state and must
// satisfy the entry-local health checks.
func (p *GlobalNodePool) MakeHealthyAndEnabledEvaluator() func(entry *node.NodeEntry) bool {
	subLookup := p.MakeSubLookup()
	return func(entry *node.NodeEntry) bool {
		if entry == nil || entry.IsDisabledBySubscriptions(subLookup) {
			return false
		}
		return entry.IsHealthy()
	}
}

// notifyAllPlatformsDirty tells every registered platform to re-evaluate a node.
func (p *GlobalNodePool) notifyAllPlatformsDirty(hash node.Hash) {
	// Platform replacement builds its candidate outside platMu so route readers
	// stay available. Keep dirty notification in the platform publication owner
	// while it snapshots and rebuilds, otherwise it can finish against the old
	// generation just before the replacement publishes and lose the update.
	if hook := p.beforePlatformNotifyLockHook; hook != nil {
		hook()
	}
	if err := p.platformBatchMu.readLockContext(context.Background()); err != nil {
		return
	}
	defer p.platformBatchMu.readUnlock()

	for {
		platforms, generation := p.platformSnapshotWithGeneration()
		if len(platforms) == 0 {
			return
		}
		p.notifyPlatformSnapshot(hash, platforms)

		p.platMu.RLock()
		unchanged := p.platformGen == generation
		p.platMu.RUnlock()
		if unchanged {
			return
		}
	}
}

func (p *GlobalNodePool) notifyPlatformSnapshot(hash node.Hash, platforms []*platform.Platform) {
	subLookup := p.MakeSubLookup()
	getEntry := func(h node.Hash) (*node.NodeEntry, bool) {
		return p.nodes.Load(h)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(platforms) {
		workers = len(platforms)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, plat := range platforms {
		sem <- struct{}{}
		wg.Add(1)
		go func(plat *platform.Platform) {
			defer wg.Done()
			defer func() { <-sem }()
			if hook := p.beforePlatformNotifyHook; hook != nil {
				hook(plat)
			}
			plat.NotifyDirty(hash, getEntry, subLookup, p.geoLookup)
		}(plat)
	}
	wg.Wait()
}

// RebuildAllPlatforms triggers a full rebuild on all registered platforms.
func (p *GlobalNodePool) RebuildAllPlatforms() {
	platforms := p.platformSnapshot()
	if len(platforms) == 0 {
		return
	}
	if hook := p.beforePlatformRebuildAllHook; hook != nil {
		hook()
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(platforms) {
		workers = len(platforms)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, plat := range platforms {
		sem <- struct{}{}
		wg.Add(1)
		go func(plat *platform.Platform) {
			defer wg.Done()
			defer func() { <-sem }()
			p.RebuildPlatformIfCurrent(plat.ID, plat)
		}(plat)
	}
	wg.Wait()
}

// RebuildPlatform triggers a full rebuild on a specific platform.
func (p *GlobalNodePool) RebuildPlatform(plat *platform.Platform) {
	_ = p.rebuildPlatformContext(context.Background(), plat)
}

func (p *GlobalNodePool) rebuildPlatformContext(ctx context.Context, plat *platform.Platform) error {
	subLookup := p.MakeSubLookup()
	poolRange := func(fn func(node.Hash, *node.NodeEntry) bool) {
		p.nodes.Range(fn)
	}
	return plat.FullRebuildContext(ctx, poolRange, subLookup, p.geoLookup)
}

// RebuildPlatformIfCurrent rebuilds the platform only while its exact object
// is still published under id. The index check and rebuild share platMu, so a
// concurrent unregister or replacement either happens before this operation
// or after it; a stale caller cannot rebuild a retired platform object.
func (p *GlobalNodePool) RebuildPlatformIfCurrent(id string, expected *platform.Platform) bool {
	ok, err := p.RebuildPlatformIfCurrentContext(context.Background(), id, expected)
	return ok && err == nil
}

// RebuildPlatformIfCurrentContext keeps the exact platform identity check and
// makes candidate construction cancellation-aware. The caller still owns the
// runtime generation read boundary; a canceled build never publishes a new
// routable view.
func (p *GlobalNodePool) RebuildPlatformIfCurrentContext(
	ctx context.Context,
	id string,
	expected *platform.Platform,
) (bool, error) {
	if p == nil || expected == nil {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if hook := p.beforePlatformReadLockHook; hook != nil {
		hook()
	}
	if err := p.platformBatchMu.readLockContext(ctx); err != nil {
		return false, err
	}
	defer p.platformBatchMu.readUnlock()
	p.platMu.RLock()
	defer p.platMu.RUnlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}
	current, ok := p.platformByID[id]
	if !ok || current != expected {
		return false, nil
	}
	if err := p.rebuildPlatformContext(ctx, expected); err != nil {
		return false, err
	}
	return true, nil
}

// --- Health Management ---

// SetOnNodeAdded sets the callback fired when a new node is added.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeAdded(fn func(hash node.Hash)) {
	p.onNodeAdded = fn
}

// SetOnNodeAddedRuntime sets the callback that prepares external runtime
// resources for a newly created node. The callback runs after the pool
// lifecycle mutation is unlocked and may perform arbitrary builder work.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeAddedRuntime(fn func(hash node.Hash, expected *node.NodeEntry)) {
	p.onNodeAddedRuntime = fn
}

// RunNodeAddedRuntime runs the configured external preparation callback for a
// node after the caller has completed its runtime publication critical
// section. It is intentionally separate from AddNodeFromSub's membership
// mutation so expensive builders and probes do not run under runtimeBatchMu.
func (p *GlobalNodePool) RunNodeAddedRuntime(hash node.Hash, expected *node.NodeEntry) {
	p.runNodeAddedRuntime(hash, expected)
}

func (p *GlobalNodePool) runNodeAddedRuntime(hash node.Hash, expected *node.NodeEntry) {
	if p == nil || expected == nil || p.onNodeAddedRuntime == nil {
		return
	}
	current, ok := p.nodes.Load(hash)
	if !ok || current != expected {
		return
	}
	if hook := p.beforeNodeAddedRuntimeHook; hook != nil {
		hook(hash, expected)
	}
	// The callback may remove and recreate the hash while the test seam (or a
	// future scheduler boundary) is active. The entry token must be revalidated
	// immediately before any external builder/probe is allowed to run.
	current, ok = p.nodes.Load(hash)
	if !ok || current != expected {
		return
	}
	p.onNodeAddedRuntime(hash, expected)
}

// SetOnNodeRemoved sets the callback fired when a node is removed from the pool.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeRemoved(fn func(hash node.Hash, entry *node.NodeEntry)) {
	p.onNodeRemoved = fn
}

// NotifyNodeDirty triggers platform re-evaluation for a single node.
// Used by OutboundManager after outbound creation to update routable views.
func (p *GlobalNodePool) NotifyNodeDirty(hash node.Hash) {
	p.notifyAllPlatformsDirty(hash)
}

// RangeNodes iterates over all nodes in the pool.
// The callback receives each node's hash and entry. Return false to stop.
func (p *GlobalNodePool) RangeNodes(fn func(node.Hash, *node.NodeEntry) bool) {
	p.nodes.Range(fn)
}

// PickDefaultPlatformOutbound selects an enabled, healthy node with a ready
// outbound from the published Default platform view.
// It returns the exact entry that passed the published Default platform view
// as an identity token; callers must compare it with a fresh pool lookup before
// using its outbound.
//
// This is intentionally a pool-only read path for background resource
// downloads (GeoIP and subscription data). Such downloads have no account or
// sticky-lease semantics, so they must not enter Router's platform lifecycle
// or create routing state merely to choose an outbound. The Default pointer
// is read lock-free, so a platform replacement cannot make this picker wait;
// it sees either the old complete view or the newly published complete view.
// The platform's regex/region/health/latency filters remain authoritative.
func (p *GlobalNodePool) PickDefaultPlatformOutbound(ctx context.Context) (node.Hash, *node.NodeEntry, error) {
	return p.PickDefaultPlatformOutboundExcluding(ctx, nil)
}

// PickDefaultPlatformOutboundExcluding selects a ready outbound from the
// published Default platform view while excluding hashes already attempted by
// one bounded resource download. The exclusion set is per request; it does
// not mutate pool health or sticky state.
func (p *GlobalNodePool) PickDefaultPlatformOutboundExcluding(ctx context.Context, excluded []node.Hash) (node.Hash, *node.NodeEntry, error) {
	var selected node.Hash
	var selectedEntry *node.NodeEntry
	var err error
	if !p.TryWithRuntimeRead(func() {
		selected, selectedEntry, err = p.pickDefaultPlatformOutbound(ctx, excluded)
	}) {
		return node.Zero, nil, ErrRuntimeGenerationBusy
	}
	return selected, selectedEntry, err
}

func (p *GlobalNodePool) pickDefaultPlatformOutbound(ctx context.Context, excluded []node.Hash) (node.Hash, *node.NodeEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return node.Zero, nil, err
	}
	if p == nil || p.nodes == nil {
		return node.Zero, nil, ErrNoAvailableOutbound
	}

	plat := p.defaultPlatform.Load()
	if plat == nil {
		return node.Zero, nil, ErrPlatformNotRegistered
	}

	subLookup := p.MakeSubLookup()
	excludedHashes := make(map[node.Hash]struct{}, len(excluded))
	for _, hash := range excluded {
		excludedHashes[hash] = struct{}{}
	}
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	var selected node.Hash
	var selectedEntry *node.NodeEntry
	selectedCount := 0
	canceled := false
	for _, candidate := range plat.SnapshotViewEntries() {
		if err := ctx.Err(); err != nil {
			canceled = true
			break
		}
		if _, excluded := excludedHashes[candidate.Hash]; excluded {
			continue
		}
		entry, ok := p.nodes.Load(candidate.Hash)
		if !ok || entry == nil || entry != candidate.Entry {
			continue
		}
		if !entry.IsHealthy() || entry.IsDisabledBySubscriptions(subLookup) {
			continue
		}

		selectedCount++
		if rng.IntN(selectedCount) == 0 {
			selected = candidate.Hash
			selectedEntry = entry
		}
	}
	if canceled {
		return node.Zero, nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return node.Zero, nil, err
	}
	if selectedCount == 0 {
		return node.Zero, nil, ErrNoAvailableOutbound
	}
	return selected, selectedEntry, nil
}

// RecordResult records a probe or passive health-check result.
// On success, resets FailureCount and clears circuit-breaker.
// On failure, increments FailureCount and opens circuit-breaker if threshold is reached.
// Notifies platforms only when circuit state changes (open/recover).
// Fires OnNodeDynamicChanged only when dynamic fields actually change.
func (p *GlobalNodePool) RecordResult(hash node.Hash, success bool) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}
	p.RecordResultForEntry(hash, entry, success)
}

// RecordResultForEntry applies a health result only if expected is still the
// live entry for hash. Probes can retain an entry across network I/O; the
// identity check is performed inside the same per-key Compute operation as
// node replacement, so a stale probe cannot update a recreated entry.
func (p *GlobalNodePool) RecordResultForEntry(hash node.Hash, expected *node.NodeEntry, success bool) bool {
	if expected == nil {
		return false
	}
	expected.LockHealthEvent()
	defer expected.UnlockHealthEvent()

	dynamicChanged := false
	circuitStateChanged := false
	applied := false
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded || entry != expected {
			return entry, xsync.CancelOp
		}
		applied = true
		if success {
			if entry.FailureCount.Swap(0) != 0 {
				dynamicChanged = true
			}
			if entry.CircuitOpenSince.Swap(0) != 0 {
				dynamicChanged = true
				circuitStateChanged = true
			}
		} else {
			newCount := entry.FailureCount.Add(1)
			dynamicChanged = true
			maxConsecutiveFailures := p.currentMaxConsecutiveFailures()
			if maxConsecutiveFailures > 0 && int(newCount) >= maxConsecutiveFailures {
				// Open circuit if not already open.
				if entry.CircuitOpenSince.CompareAndSwap(0, time.Now().UnixNano()) {
					circuitStateChanged = true
				}
			}
		}
		return entry, xsync.UpdateOp
	})
	if !applied {
		return false
	}

	if circuitStateChanged {
		p.notifyAllPlatformsDirty(hash)
	}
	if dynamicChanged && p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
	return true
}

// RecordPassiveResult records health feedback from user proxy traffic.
// Failed passive traffic is ignored when the originating platform disables
// passive circuit breaking; successes still count as positive health feedback.
func (p *GlobalNodePool) RecordPassiveResult(platformID string, hash node.Hash, success bool) {
	if success || !p.passiveCircuitBreakerDisabled(platformID) {
		p.RecordResult(hash, success)
	}
}

// RecordPassiveResultForEntry applies passive health feedback only when the
// entry captured by the request is still the live entry for hash.
func (p *GlobalNodePool) RecordPassiveResultForEntry(
	platformID string,
	hash node.Hash,
	expected *node.NodeEntry,
	success bool,
) bool {
	if success || !p.passiveCircuitBreakerDisabled(platformID) {
		return p.RecordResultForEntry(hash, expected, success)
	}
	return false
}

// RecordPassiveResultForRoute applies feedback using the policy captured when
// the request was routed. A response can outlive a platform replacement that
// reuses the same ID; consulting that ID again would apply the replacement's
// policy to the old request.
func (p *GlobalNodePool) RecordPassiveResultForRoute(
	platformID string,
	passiveCircuitBreakerDisabled bool,
	hash node.Hash,
	expected *node.NodeEntry,
	success bool,
) bool {
	if platformID == "" || (!success && passiveCircuitBreakerDisabled) {
		return false
	}
	return p.RecordResultForEntry(hash, expected, success)
}

func (p *GlobalNodePool) passiveCircuitBreakerDisabled(platformID string) bool {
	if platformID == "" {
		return false
	}

	p.platMu.RLock()
	defer p.platMu.RUnlock()

	plat, ok := p.platformByID[platformID]
	if !ok || plat == nil {
		return false
	}
	return plat.PassiveCircuitBreakerDisabled
}

func (p *GlobalNodePool) currentMaxConsecutiveFailures() int {
	return p.maxConsecutiveFailures()
}

// RecordLatency records a latency probe attempt for the given node and raw target.
// rawTarget is normalized through ExtractDomain (eTLD+1). latency may be nil,
// which means "attempt only" without latency sample writeback.
func (p *GlobalNodePool) RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}
	p.RecordLatencyForEntry(hash, entry, rawTarget, latency)
}

// RecordLatencyForEntry applies a probe attempt only if expected is still the
// live entry for hash. It preserves entry identity across node recreation.
func (p *GlobalNodePool) RecordLatencyForEntry(
	hash node.Hash,
	expected *node.NodeEntry,
	rawTarget string,
	latency *time.Duration,
) bool {
	if expected == nil {
		return false
	}
	expected.LockHealthEvent()
	defer expected.UnlockHealthEvent()

	domain := netutil.ExtractDomain(rawTarget)
	isAuthority := p.isAuthorityDomain(domain)

	var decayWindow time.Duration
	if p.latencyDecayWindow != nil {
		decayWindow = p.latencyDecayWindow()
	}
	if decayWindow <= 0 {
		decayWindow = 30 * time.Second // default
	}

	var wasEmpty, evicted bool
	var evictedDomain string
	latencyRecorded := false
	applied := false
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded || entry != expected {
			return entry, xsync.CancelOp
		}
		applied = true
		nowNs := time.Now().UnixNano()
		entry.LastLatencyProbeAttempt.Store(nowNs)
		if isAuthority {
			entry.LastAuthorityLatencyProbeAttempt.Store(nowNs)
		}
		if latency == nil || *latency <= 0 || entry.LatencyTable == nil {
			return entry, xsync.UpdateOp
		}
		wasEmpty, evictedDomain, evicted = entry.LatencyTable.UpdateClassified(domain, *latency, decayWindow, isAuthority)
		latencyRecorded = true
		return entry, xsync.UpdateOp
	})
	if !applied {
		return false
	}
	if p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
	if !latencyRecorded {
		return true
	}

	// If the table transitioned from empty to non-empty, the node might
	// now satisfy the HasLatency filter — notify platforms.
	if wasEmpty {
		p.notifyAllPlatformsDirty(hash)
	}

	if p.onNodeLatencyChanged != nil {
		p.onNodeLatencyChanged(hash, domain)
		if evicted {
			p.onNodeLatencyChanged(hash, evictedDomain)
		}
	}
	return true
}

// UpdateNodeEgressIP records an egress probe attempt and optionally updates
// the node's egress IP and explicit region metadata.
// Region update rules:
//   - ip=nil,  loc=nil: keep both IP and region unchanged.
//   - ip!=nil, loc=nil: keep region if IP unchanged; clear region if IP changed.
//   - loc!=nil: set region to loc (normalized).
func (p *GlobalNodePool) UpdateNodeEgressIP(hash node.Hash, ip *netip.Addr, loc *string) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}
	p.UpdateNodeEgressIPForEntry(hash, entry, ip, loc)
}

// UpdateNodeEgressIPForEntry applies an egress result only if expected is
// still the live entry for hash. It returns false when the node was removed or
// recreated while the probe was in flight.
func (p *GlobalNodePool) UpdateNodeEgressIPForEntry(
	hash node.Hash,
	expected *node.NodeEntry,
	ip *netip.Addr,
	loc *string,
) bool {
	if expected == nil {
		return false
	}
	expected.LockHealthEvent()
	defer expected.UnlockHealthEvent()

	ipChanged := false
	regionChanged := false
	applied := false
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded || entry != expected {
			return entry, xsync.CancelOp
		}
		applied = true
		nowNs := time.Now().UnixNano()
		entry.LastEgressUpdateAttempt.Store(nowNs)

		oldIP := entry.GetEgressIP()
		oldRegion := entry.GetEgressRegion()
		if ip != nil {
			// Record successful egress-IP sample timestamp.
			entry.LastEgressUpdate.Store(nowNs)
			if oldIP != *ip {
				entry.SetEgressIP(*ip)
				ipChanged = true
			}
		}

		switch {
		case loc != nil:
			entry.SetEgressRegion(*loc)
			regionChanged = oldRegion != entry.GetEgressRegion()
		case ip == nil:
			// Attempt-only update: keep region as-is.
		case !ipChanged:
			// IP unchanged and no explicit region: keep existing region.
		default:
			// IP changed without explicit region: clear stale region metadata.
			if oldRegion != "" {
				entry.SetEgressRegion("")
				regionChanged = true
			}
		}
		return entry, xsync.UpdateOp
	})
	if !applied {
		return false
	}

	if ipChanged || regionChanged {
		p.notifyAllPlatformsDirty(hash)
	}
	if p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
	return true
}

func (p *GlobalNodePool) isAuthorityDomain(domain string) bool {
	if domain == "" || p.latencyAuthorities == nil {
		return false
	}
	authorities := p.latencyAuthorities()
	for _, authority := range authorities {
		if strings.EqualFold(strings.TrimSpace(authority), domain) {
			return true
		}
	}
	return false
}
