package routing

import (
	"container/heap"
	"net/netip"
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
