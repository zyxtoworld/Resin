package routing

import (
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
	mu       sync.RWMutex
	byNode   map[node.Hash]time.Time
	byEgress map[netip.Addr]time.Time
}

func NewResponseCooldowns() *ResponseCooldowns {
	return &ResponseCooldowns{
		byNode:   make(map[node.Hash]time.Time),
		byEgress: make(map[netip.Addr]time.Time),
	}
}

func (c *ResponseCooldowns) Mark(scope platform.ResponseRuleScope, hash node.Hash, egressIP netip.Addr, until time.Time) {
	if c == nil || !until.After(time.Now()) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if scope == platform.ResponseRuleScopeEgressIP && egressIP.IsValid() {
		if current := c.byEgress[egressIP]; until.After(current) {
			c.byEgress[egressIP] = until
		}
		return
	}
	if hash != node.Zero {
		if current := c.byNode[hash]; until.After(current) {
			c.byNode[hash] = until
		}
	}
}

func (c *ResponseCooldowns) IsCooling(hash node.Hash, egressIP netip.Addr, now time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	if until, ok := c.byNode[hash]; ok && until.After(now) {
		return true
	}
	if egressIP.IsValid() {
		if until, ok := c.byEgress[egressIP]; ok && until.After(now) {
			return true
		}
	}
	return false
}
