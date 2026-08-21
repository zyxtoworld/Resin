package routing

import (
	"container/heap"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

// ResponseCooldowns is the in-memory quarantine table for a platform. It is
// deliberately not persisted: an upstream quota window is external state and
// stale cooldowns must not survive a restart.
type ResponseCooldowns struct {
	mu           sync.Mutex
	byNode       map[node.Hash]time.Time
	nodeEntry    map[node.Hash]*node.NodeEntry
	byEgress     map[netip.Addr]time.Time
	nodeExpiry   nodeCooldownExpiryHeap
	egressExpiry egressCooldownExpiryHeap
}

// ResponseCooldownSnapshot is the read-only representation of one active
// response cooldown. The owning Router supplies the platform boundary; this
// type intentionally contains no persistence or mutation behavior.
type ResponseCooldownSnapshot struct {
	Scope    platform.ResponseRuleScope
	NodeHash node.Hash
	Entry    *node.NodeEntry
	EgressIP netip.Addr
	Until    time.Time
}

// ResponseCooldownReadSnapshot is an immutable, lock-free view produced by
// one cooldown-owner read. Callers may classify many nodes against it without
// re-entering the cooldown mutex for every node.
type ResponseCooldownReadSnapshot struct {
	byNode   map[node.Hash]ResponseCooldownSnapshot
	byEgress map[netip.Addr]ResponseCooldownSnapshot
}

type nodeCooldownExpiry struct {
	hash  node.Hash
	until time.Time
	entry *node.NodeEntry
}

type nodeCooldownExpiryHeap []nodeCooldownExpiry

func (h nodeCooldownExpiryHeap) Len() int           { return len(h) }
func (h nodeCooldownExpiryHeap) Less(i, j int) bool { return h[i].until.Before(h[j].until) }
func (h nodeCooldownExpiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *nodeCooldownExpiryHeap) Push(x any)        { *h = append(*h, x.(nodeCooldownExpiry)) }
func (h *nodeCooldownExpiryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type egressCooldownExpiry struct {
	ip    netip.Addr
	until time.Time
}

type egressCooldownExpiryHeap []egressCooldownExpiry

func (h egressCooldownExpiryHeap) Len() int           { return len(h) }
func (h egressCooldownExpiryHeap) Less(i, j int) bool { return h[i].until.Before(h[j].until) }
func (h egressCooldownExpiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *egressCooldownExpiryHeap) Push(x any)        { *h = append(*h, x.(egressCooldownExpiry)) }
func (h *egressCooldownExpiryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

const cooldownExpiryHeapSlack = 64

func NewResponseCooldowns() *ResponseCooldowns {
	return &ResponseCooldowns{
		byNode:    make(map[node.Hash]time.Time),
		nodeEntry: make(map[node.Hash]*node.NodeEntry),
		byEgress:  make(map[netip.Addr]time.Time),
	}
}

func (c *ResponseCooldowns) Mark(scope platform.ResponseRuleScope, hash node.Hash, egressIP netip.Addr, until time.Time) {
	c.markAtEntry(scope, hash, nil, egressIP, until, time.Now())
}

func (c *ResponseCooldowns) markAt(
	scope platform.ResponseRuleScope,
	hash node.Hash,
	egressIP netip.Addr,
	until time.Time,
	now time.Time,
) {
	c.markAtEntry(scope, hash, nil, egressIP, until, now)
}

// markForEntry records a cooldown for the response's exact route generation.
// The entry is validated by Router/ExactEntryExecutor before this method is
// called. Once published, an egress-IP cooldown is intentionally keyed by the
// stable public IP and survives an entry rebuild with that same IP.
func (c *ResponseCooldowns) markForEntry(
	scope platform.ResponseRuleScope,
	hash node.Hash,
	entry *node.NodeEntry,
	egressIP netip.Addr,
	until time.Time,
	now time.Time,
) {
	c.markAtEntry(scope, hash, entry, egressIP, until, now)
}

func (c *ResponseCooldowns) markAtEntry(
	scope platform.ResponseRuleScope,
	hash node.Hash,
	entry *node.NodeEntry,
	egressIP netip.Addr,
	until time.Time,
	now time.Time,
) {
	if c == nil || !until.After(now) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked(now)

	if scope == platform.ResponseRuleScopeEgressIP && egressIP.IsValid() {
		if current := c.byEgress[egressIP]; until.After(current) {
			c.byEgress[egressIP] = until
			heap.Push(&c.egressExpiry, egressCooldownExpiry{ip: egressIP, until: until})
			c.compactExpiryHeapsLocked()
		}
		return
	}
	if hash != node.Zero {
		if current := c.byNode[hash]; until.After(current) {
			c.byNode[hash] = until
			c.nodeEntry[hash] = entry
			heap.Push(&c.nodeExpiry, nodeCooldownExpiry{hash: hash, until: until, entry: entry})
			c.compactExpiryHeapsLocked()
		} else if until.Equal(current) && c.nodeEntry[hash] != entry {
			c.nodeEntry[hash] = entry
			heap.Push(&c.nodeExpiry, nodeCooldownExpiry{hash: hash, until: until, entry: entry})
			c.compactExpiryHeapsLocked()
		}
	}
}

func (c *ResponseCooldowns) IsCooling(hash node.Hash, egressIP netip.Addr, now time.Time) bool {
	return c.isCooling(hash, nil, egressIP, now, false)
}

// IsCoolingForEntry applies node-scope cooldowns only to the exact entry that
// was current when that cooldown was recorded. Generic Mark calls have a nil
// generation and therefore remain visible to every entry with the hash.
func (c *ResponseCooldowns) IsCoolingForEntry(hash node.Hash, entry *node.NodeEntry, egressIP netip.Addr, now time.Time) bool {
	return c.isCooling(hash, entry, egressIP, now, true)
}

// Snapshot returns active cooldowns and removes expired entries as part of the
// same read. Callers should pass the timestamp of the surrounding runtime
// snapshot so the returned remaining durations are generation-consistent.
func (c *ResponseCooldowns) Snapshot(now time.Time) []ResponseCooldownSnapshot {
	return c.ReadSnapshot(now).Items()
}

// ReadSnapshot prunes and copies the active cooldowns under one owner lock.
// The returned value is immutable and safe for repeated node classification.
func (c *ResponseCooldowns) ReadSnapshot(now time.Time) ResponseCooldownReadSnapshot {
	result := ResponseCooldownReadSnapshot{
		byNode:   make(map[node.Hash]ResponseCooldownSnapshot),
		byEgress: make(map[netip.Addr]ResponseCooldownSnapshot),
	}
	if c == nil {
		return result
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked(now)
	for hash, until := range c.byNode {
		if until.After(now) {
			item := ResponseCooldownSnapshot{
				Scope:    platform.ResponseRuleScopeNode,
				NodeHash: hash,
				Entry:    c.nodeEntry[hash],
				Until:    until,
			}
			result.byNode[hash] = item
		}
	}
	for ip, until := range c.byEgress {
		if until.After(now) {
			item := ResponseCooldownSnapshot{
				Scope:    platform.ResponseRuleScopeEgressIP,
				EgressIP: ip,
				Until:    until,
			}
			result.byEgress[ip] = item
		}
	}
	return result
}

// Range visits the immutable cooldown indexes without sorting or materializing
// a second ordered slice. It is intended for classification and bounded page
// selection; callers must not mutate the supplied snapshots.
func (s ResponseCooldownReadSnapshot) Range(visit func(ResponseCooldownSnapshot) bool) {
	if visit == nil {
		return
	}
	for _, item := range s.byNode {
		if !visit(item) {
			return
		}
	}
	for _, item := range s.byEgress {
		if !visit(item) {
			return
		}
	}
}

// Items returns a detached, deterministically ordered list from the read view.
// Sorting is deliberately deferred to this explicit full-list API; route-state
// classification and bounded pages use Range instead.
func (s ResponseCooldownReadSnapshot) Items() []ResponseCooldownSnapshot {
	items := make([]ResponseCooldownSnapshot, 0, len(s.byNode)+len(s.byEgress))
	s.Range(func(item ResponseCooldownSnapshot) bool {
		items = append(items, item)
		return true
	})
	sort.Slice(items, func(i, j int) bool {
		return compareResponseCooldownSnapshots(items[i], items[j]) < 0
	})
	return items
}

// Filter evaluates include exactly once per item and returns another detached
// immutable view. Callers that need both an exact total and a bounded page can
// filter generation validity once, then page the filtered view without
// repeating expensive entry/view-owner checks.
func (s ResponseCooldownReadSnapshot) Filter(include func(ResponseCooldownSnapshot) bool) ResponseCooldownReadSnapshot {
	if include == nil {
		return s
	}
	filtered := ResponseCooldownReadSnapshot{
		byNode:   make(map[node.Hash]ResponseCooldownSnapshot),
		byEgress: make(map[netip.Addr]ResponseCooldownSnapshot),
	}
	s.Range(func(item ResponseCooldownSnapshot) bool {
		if !include(item) {
			return true
		}
		if item.Scope == platform.ResponseRuleScopeNode {
			filtered.byNode[item.NodeHash] = item
		} else if item.EgressIP.IsValid() {
			filtered.byEgress[item.EgressIP] = item
		}
		return true
	})
	return filtered
}

// IsCoolingForEntry classifies a node without taking a lock. Node-scope
// cooldowns with a nil Entry are legacy hash-scoped values; generation-bound
// values require exact entry identity.
func (s ResponseCooldownReadSnapshot) IsCoolingForEntry(hash node.Hash, entry *node.NodeEntry, egressIP netip.Addr) bool {
	if item, ok := s.byNode[hash]; ok && (item.Entry == nil || item.Entry == entry) {
		return true
	}
	if _, ok := s.byEgress[egressIP]; ok {
		return true
	}
	return false
}

type responseCooldownPageHeap struct {
	items []ResponseCooldownSnapshot
}

func (h responseCooldownPageHeap) Len() int { return len(h.items) }

// Less keeps the worst retained item at the root, so a page scan retains only
// offset+limit+1 candidates instead of materializing the whole cooldown table.
func (h responseCooldownPageHeap) Less(i, j int) bool {
	return compareResponseCooldownSnapshots(h.items[i], h.items[j]) > 0
}

func (h responseCooldownPageHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *responseCooldownPageHeap) Push(value any) {
	h.items = append(h.items, value.(ResponseCooldownSnapshot))
}

func (h *responseCooldownPageHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

func compareResponseCooldownSnapshots(left, right ResponseCooldownSnapshot) int {
	if left.Scope != right.Scope {
		if left.Scope < right.Scope {
			return -1
		}
		return 1
	}
	if left.NodeHash != right.NodeHash {
		if left.NodeHash.Hex() < right.NodeHash.Hex() {
			return -1
		}
		return 1
	}
	if left.EgressIP != right.EgressIP {
		if left.EgressIP.String() < right.EgressIP.String() {
			return -1
		}
		return 1
	}
	if left.Until.Before(right.Until) {
		return -1
	}
	if left.Until.After(right.Until) {
		return 1
	}
	return 0
}

// SnapshotPage returns a stable, bounded page of active entries accepted by
// include. It first takes one immutable read snapshot, then performs exact
// counting while retaining only the requested top-k candidates. include runs
// after the cooldown owner is released and must remain read-only.
func (c *ResponseCooldowns) SnapshotPage(now time.Time, offset, limit int, include func(ResponseCooldownSnapshot) bool) ([]ResponseCooldownSnapshot, int, bool) {
	return c.ReadSnapshot(now).SnapshotPageWithCount(offset, limit, include, include)
}

// SnapshotPageWithCount scans the immutable cooldown read once. countInclude
// defines the exact total, while pageInclude defines the bounded page
// contents. This is useful when a page shows only the currently visible nodes
// but its total describes the whole platform.
func (s ResponseCooldownReadSnapshot) SnapshotPageWithCount(
	offset, limit int,
	countInclude func(ResponseCooldownSnapshot) bool,
	pageInclude func(ResponseCooldownSnapshot) bool,
) ([]ResponseCooldownSnapshot, int, bool) {
	if limit <= 0 || offset < 0 {
		return []ResponseCooldownSnapshot{}, 0, false
	}
	window := offset + limit + 1
	selected := &responseCooldownPageHeap{}
	heap.Init(selected)
	total := 0
	pageTotal := 0
	visit := func(item ResponseCooldownSnapshot) {
		if countInclude == nil || countInclude(item) {
			total++
		}
		if pageInclude != nil && !pageInclude(item) {
			return
		}
		pageTotal++
		if selected.Len() < window {
			heap.Push(selected, item)
			return
		}
		if compareResponseCooldownSnapshots(item, selected.items[0]) < 0 {
			heap.Pop(selected)
			heap.Push(selected, item)
		}
	}
	s.Range(func(item ResponseCooldownSnapshot) bool {
		visit(item)
		return true
	})
	items := make([]ResponseCooldownSnapshot, 0, selected.Len())
	for selected.Len() > 0 {
		items = append(items, heap.Pop(selected).(ResponseCooldownSnapshot))
	}
	sort.Slice(items, func(i, j int) bool {
		return compareResponseCooldownSnapshots(items[i], items[j]) < 0
	})
	if offset >= len(items) {
		return []ResponseCooldownSnapshot{}, total, false
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	page := items[offset:end]
	return page, total, end < pageTotal
}

func (c *ResponseCooldowns) isCooling(
	hash node.Hash,
	entry *node.NodeEntry,
	egressIP netip.Addr,
	now time.Time,
	checkEntry bool,
) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked(now)

	cooling := false
	if until, ok := c.byNode[hash]; ok {
		if until.After(now) && (!checkEntry || c.nodeEntry[hash] == nil || c.nodeEntry[hash] == entry) {
			cooling = true
		}
	}
	if egressIP.IsValid() {
		if until, ok := c.byEgress[egressIP]; ok && until.After(now) {
			cooling = true
		}
	}
	return cooling
}

// pruneExpiredLocked removes every deadline that has passed. The heaps make
// this proportional to the number of newly expired entries, not the total
// number of historical node/IP keys.
func (c *ResponseCooldowns) pruneExpiredLocked(now time.Time) {
	for len(c.nodeExpiry) > 0 && !c.nodeExpiry[0].until.After(now) {
		item := heap.Pop(&c.nodeExpiry).(nodeCooldownExpiry)
		if current, ok := c.byNode[item.hash]; ok && current.Equal(item.until) && c.nodeEntry[item.hash] == item.entry {
			delete(c.byNode, item.hash)
			delete(c.nodeEntry, item.hash)
		}
	}
	for len(c.egressExpiry) > 0 && !c.egressExpiry[0].until.After(now) {
		item := heap.Pop(&c.egressExpiry).(egressCooldownExpiry)
		if current, ok := c.byEgress[item.ip]; ok && current.Equal(item.until) {
			delete(c.byEgress, item.ip)
		}
	}
}

// compactExpiryHeapsLocked bounds stale heap entries created when one active
// node/IP receives a later deadline repeatedly. The maps remain the source of
// truth; heap entries are only expiry work items.
func (c *ResponseCooldowns) compactExpiryHeapsLocked() {
	if len(c.nodeExpiry) > 2*len(c.byNode)+cooldownExpiryHeapSlack {
		next := make(nodeCooldownExpiryHeap, 0, len(c.byNode))
		for hash, until := range c.byNode {
			next = append(next, nodeCooldownExpiry{hash: hash, until: until, entry: c.nodeEntry[hash]})
		}
		heap.Init(&next)
		c.nodeExpiry = next
	}
	if len(c.egressExpiry) > 2*len(c.byEgress)+cooldownExpiryHeapSlack {
		next := make(egressCooldownExpiryHeap, 0, len(c.byEgress))
		for ip, until := range c.byEgress {
			next = append(next, egressCooldownExpiry{ip: ip, until: until})
		}
		heap.Init(&next)
		c.egressExpiry = next
	}
}
