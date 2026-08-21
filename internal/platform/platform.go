package platform

import (
	"context"
	"net/netip"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/Resinat/Resin/internal/node"
)

// DefaultPlatformID is the well-known UUID of the built-in Default platform.
const DefaultPlatformID = "00000000-0000-0000-0000-000000000000"

// DefaultPlatformName is the built-in platform name.
const DefaultPlatformName = "Default"

// GeoLookupFunc resolves an IP address to a lowercase ISO country code.
type GeoLookupFunc func(netip.Addr) string

// PoolRangeFunc iterates all nodes in the global pool.
type PoolRangeFunc func(fn func(node.Hash, *node.NodeEntry) bool)

// RebuildPublishGuard owns the final publication of a rebuild candidate.
// The guard may validate the candidate against the source pool and must call
// publish while that validation remains linearized.
type RebuildPublishGuard func([]RoutableViewEntry, func() error) error

// GetEntryFunc retrieves a node entry from the global pool by hash.
type GetEntryFunc func(node.Hash) (*node.NodeEntry, bool)

// Platform represents a routing platform with its filtered routable view.
type Platform struct {
	ID   string
	Name string

	// Filter configuration.
	RegexFilters  node.TagFilter
	RegionFilters []string // lowercase ISO codes, supports negation "!xx"

	// Other config fields.
	StickyTTLNs                      int64
	ReverseProxyMissAction           string
	ReverseProxyEmptyAccountBehavior string
	ReverseProxyFixedAccountHeader   string
	ReverseProxyFixedAccountHeaders  []string
	AllocationPolicy                 AllocationPolicy
	PassiveCircuitBreakerDisabled    bool
	// ProxyRequestTotalTimeoutNs is copied into each route generation. Zero is
	// deliberate fail-closed: a global cap never enables platform retries.
	ProxyRequestTotalTimeoutNs int64
	// ProxyRequestAttemptTimeoutNs optionally bounds one pre-response attempt.
	// Zero means that attempt uses the remaining total deadline.
	ProxyRequestAttemptTimeoutNs int64
	// ProxyRequestMaxAttempts is an explicit platform limit. Zero means all
	// candidates captured by the immutable route generation may be tried.
	ProxyRequestMaxAttempts int
	ResponseRules           ResponseRules

	// Routable view & its publication lock.
	// viewMu serializes publishers while allowing concurrent identity reads.
	view        atomic.Pointer[RoutableView]
	viewEntries map[node.Hash]*node.NodeEntry
	viewMu      sync.RWMutex
	// viewWriterMu is the cancellation-aware admission owner for the two
	// operations that publish the view. It prevents a canceled full rebuild
	// from waiting indefinitely behind a dirty notification that is doing a
	// slow GeoIP lookup while holding viewMu.
	viewWriterMu contextMutex
	// Package-private test seam immediately before a context-aware full
	// rebuild waits for the view writer admission. Production leaves it nil.
	beforeViewWriterLockHook func()
}

// RoutableViewEntry pairs a view hash with the exact NodeEntry that satisfied
// the platform filters when the hash was published. Consumers that need to
// survive node replacement can reject a stale hash by comparing pointers.
type RoutableViewEntry struct {
	Hash  node.Hash
	Entry *node.NodeEntry
}

// NewPlatform creates a Platform with an empty routable view.
// The regex slice is treated as MUST rules for compatibility with internal callers.
func NewPlatform(id, name string, regexFilters []*regexp.Regexp, regionFilters []string) *Platform {
	return NewPlatformWithTagFilter(id, name, node.TagFilter{Must: regexFilters}, regionFilters)
}

// NewPlatformWithTagFilter creates a Platform with compiled line-oriented tag rules.
func NewPlatformWithTagFilter(id, name string, regexFilters node.TagFilter, regionFilters []string) *Platform {
	p := &Platform{
		ID:            id,
		Name:          name,
		RegexFilters:  regexFilters,
		RegionFilters: regionFilters,
		viewEntries:   make(map[node.Hash]*node.NodeEntry),
	}
	p.view.Store(NewRoutableView())
	return p
}

// View returns the platform's routable view as a read-only interface.
// External callers cannot Add/Remove/Clear — only FullRebuild and NotifyDirty can mutate.
func (p *Platform) View() ReadOnlyView {
	if p == nil {
		return nil
	}
	return p.view.Load()
}

// SnapshotView returns a consistent copy of the current routable hashes.
// FullRebuild and NotifyDirty publish/mutate the view under viewMu; taking the
// same lock here keeps the hash snapshot aligned with viewEntries.
func (p *Platform) SnapshotView() []node.Hash {
	entries := p.SnapshotViewEntries()
	hashes := make([]node.Hash, 0, len(entries))
	for _, entry := range entries {
		hashes = append(hashes, entry.Hash)
	}
	return hashes
}

// SnapshotViewEntries returns a consistent copy of the routable hashes and
// the exact node entries that were published with them.
func (p *Platform) SnapshotViewEntries() []RoutableViewEntry {
	if p == nil {
		return nil
	}
	p.viewMu.RLock()
	defer p.viewMu.RUnlock()
	view := p.view.Load()
	if view == nil {
		return nil
	}

	entries := make([]RoutableViewEntry, 0, view.Size())
	view.Range(func(hash node.Hash) bool {
		if entry, ok := p.viewEntries[hash]; ok && entry != nil {
			entries = append(entries, RoutableViewEntry{Hash: hash, Entry: entry})
		}
		return true
	})
	return entries
}

// ContainsViewEntry reports whether entry is still the exact NodeEntry that
// was published for hash. The identity check is the routing contract: a hash
// can be reused by the pool while the old platform view is still pending its
// dirty notification.
func (p *Platform) ContainsViewEntry(hash node.Hash, entry *node.NodeEntry) bool {
	if p == nil || entry == nil {
		return false
	}
	p.viewMu.RLock()
	defer p.viewMu.RUnlock()
	if p.view.Load() == nil {
		return false
	}
	return p.viewEntries[hash] == entry
}

// RangeViewEntries visits the currently published hash/entry pairs while the
// view publication lock is held. The callback must not call back into this
// Platform's view methods; it is intended for short pool/node reads.
func (p *Platform) RangeViewEntries(fn func(node.Hash, *node.NodeEntry) bool) {
	if p == nil || fn == nil {
		return
	}
	p.viewMu.RLock()
	defer p.viewMu.RUnlock()
	view := p.view.Load()
	if view == nil {
		return
	}

	view.Range(func(hash node.Hash) bool {
		entry := p.viewEntries[hash]
		if entry == nil {
			return true
		}
		return fn(hash, entry)
	})
}

// FullRebuild builds a complete candidate and publishes it atomically after
// every node has been evaluated. Readers keep seeing the previous complete
// view until that publication point.
func (p *Platform) FullRebuild(
	poolRange PoolRangeFunc,
	subLookup node.SubLookupFunc,
	geoLookup GeoLookupFunc,
) {
	_ = p.FullRebuildContext(context.Background(), poolRange, subLookup, geoLookup)
}

// FullRebuildContext builds a complete candidate and publishes it only when
// ctx is still live. A node evaluation or GeoIP lookup already in progress is
// not forcibly interrupted; cancellation is checked before each evaluation
// and again immediately before publication, so a canceled request cannot
// publish a partially accepted generation.
func (p *Platform) FullRebuildContext(
	ctx context.Context,
	poolRange PoolRangeFunc,
	subLookup node.SubLookupFunc,
	geoLookup GeoLookupFunc,
) error {
	return p.fullRebuildContext(ctx, poolRange, subLookup, geoLookup, nil)
}

// FullRebuildContextWithPublishGuard builds one candidate and lets the pool
// owner linearize its publication against concurrent source mutations.
func (p *Platform) FullRebuildContextWithPublishGuard(
	ctx context.Context,
	poolRange PoolRangeFunc,
	subLookup node.SubLookupFunc,
	geoLookup GeoLookupFunc,
	guard RebuildPublishGuard,
) error {
	return p.fullRebuildContext(ctx, poolRange, subLookup, geoLookup, guard)
}

func (p *Platform) fullRebuildContext(
	ctx context.Context,
	poolRange PoolRangeFunc,
	subLookup node.SubLookupFunc,
	geoLookup GeoLookupFunc,
	guard RebuildPublishGuard,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if hook := p.beforeViewWriterLockHook; hook != nil {
		hook()
	}
	if err := p.viewWriterMu.lockContext(ctx); err != nil {
		return err
	}
	defer p.viewWriterMu.unlock()

	nextView := NewRoutableView()
	nextEntries := make(map[node.Hash]*node.NodeEntry)
	var buildErr error
	poolRange(func(h node.Hash, entry *node.NodeEntry) bool {
		if err := ctx.Err(); err != nil {
			buildErr = err
			return false
		}
		if p.evaluateNode(entry, subLookup, geoLookup) {
			nextView.Add(h)
			nextEntries[h] = entry
		}
		return true
	})
	if buildErr != nil {
		return buildErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	publish := func() error {
		p.viewMu.Lock()
		defer p.viewMu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		p.viewEntries = nextEntries
		p.view.Store(nextView)
		return nil
	}
	if guard == nil {
		return publish()
	}
	candidates := make([]RoutableViewEntry, 0, len(nextEntries))
	for hash, entry := range nextEntries {
		candidates = append(candidates, RoutableViewEntry{Hash: hash, Entry: entry})
	}
	return guard(candidates, publish)
}

// NotifyDirty re-evaluates a single node and adds/removes it from the view.
// Acquires viewMu — serialized with FullRebuild.
func (p *Platform) NotifyDirty(
	h node.Hash,
	getEntry GetEntryFunc,
	subLookup node.SubLookupFunc,
	geoLookup GeoLookupFunc,
) {
	if err := p.viewWriterMu.lockContext(context.Background()); err != nil {
		return
	}
	defer p.viewWriterMu.unlock()

	entry, ok := getEntry(h)
	if !ok {
		p.viewMu.Lock()
		defer p.viewMu.Unlock()
		view := p.view.Load()
		if view == nil {
			view = NewRoutableView()
			p.view.Store(view)
		}
		// Node was deleted from pool.
		view.Remove(h)
		delete(p.viewEntries, h)
		return
	}

	routable := p.evaluateNode(entry, subLookup, geoLookup)
	p.viewMu.Lock()
	defer p.viewMu.Unlock()
	view := p.view.Load()
	if view == nil {
		view = NewRoutableView()
		p.view.Store(view)
	}
	if routable {
		view.Add(h)
		if p.viewEntries == nil {
			p.viewEntries = make(map[node.Hash]*node.NodeEntry)
		}
		p.viewEntries[h] = entry
	} else {
		view.Remove(h)
		delete(p.viewEntries, h)
	}
}

// evaluateNode checks all filter conditions for platform routability.
func (p *Platform) evaluateNode(
	entry *node.NodeEntry,
	subLookup node.SubLookupFunc,
	geoLookup GeoLookupFunc,
) bool {
	// 0. Disabled nodes are never routable.
	if entry.IsDisabledBySubscriptions(subLookup) {
		return false
	}

	// 1. Healthy for routing (outbound ready + circuit not open).
	if !entry.IsHealthy() {
		return false
	}

	// 2. Tag regex match.
	if !entry.MatchTagFilter(p.RegexFilters, subLookup) {
		return false
	}

	// 3. Egress IP must be known.
	egressIP := entry.GetEgressIP()
	if !egressIP.IsValid() {
		return false
	}

	// 4. Region filter (when configured).
	if len(p.RegionFilters) > 0 {
		region := entry.GetRegion(geoLookup)
		if !MatchRegionFilter(region, p.RegionFilters) {
			return false
		}
	}

	// 5. Has at least one latency record.
	if !entry.HasLatency() {
		return false
	}

	return true
}

// MatchRegionFilter applies include/exclude region filters.
// Positive entries (xx) build an include set; negative entries (!xx) build an exclude set.
// Unknown regions never match when region filters are configured.
// Final result is: region known AND (include empty OR region in include) AND (region not in exclude).
func MatchRegionFilter(region string, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	if region == "" {
		return false
	}

	included := false
	hasInclude := false

	for _, filter := range filters {
		if len(filter) > 0 && filter[0] == '!' {
			if region == filter[1:] {
				return false
			}
			continue
		}
		hasInclude = true
		if region == filter {
			included = true
		}
	}

	if hasInclude && !included {
		return false
	}
	return true
}
