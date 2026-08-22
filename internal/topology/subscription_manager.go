package topology

import (
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/puzpuzpuz/xsync/v4"
)

// SubscriptionManager holds all subscription instances and provides
// lifecycle-safe lookup/register/unregister for subscription instances.
type SubscriptionManager struct {
	subs *xsync.Map[string, *subscription.Subscription]
}

// NewSubscriptionManager creates a new SubscriptionManager.
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		subs: xsync.NewMap[string, *subscription.Subscription](),
	}
}

// Get retrieves a subscription by ID.
func (m *SubscriptionManager) Get(id string) (*subscription.Subscription, bool) {
	return m.subs.Load(id)
}

// Register adds a subscription to the manager.
func (m *SubscriptionManager) Register(sub *subscription.Subscription) {
	if sub == nil {
		return
	}
	m.subs.Compute(sub.ID, func(previous *subscription.Subscription, loaded bool) (*subscription.Subscription, xsync.ComputeOp) {
		if loaded && previous != nil && previous != sub {
			previous.InvalidateRuntimePreparation()
		}
		return sub, xsync.UpdateOp
	})
}

// Unregister removes the instance captured from the current ID lookup. It is
// safe against a replacement racing after that lookup, but lifecycle callers
// holding an explicit instance must use UnregisterExact.
func (m *SubscriptionManager) Unregister(id string) {
	expected, _ := m.Get(id)
	m.UnregisterExact(id, expected)
}

// UnregisterExact removes only the exact subscription instance captured by the
// caller. This prevents a stale delete by ID from removing a same-ID
// replacement that was registered after the caller's lookup.
func (m *SubscriptionManager) UnregisterExact(id string, expected *subscription.Subscription) bool {
	if expected == nil {
		return false
	}
	removed := false
	m.subs.Compute(id, func(sub *subscription.Subscription, loaded bool) (*subscription.Subscription, xsync.ComputeOp) {
		if !loaded || sub != expected {
			return sub, xsync.CancelOp
		}
		// The map-shard mutation is the identity linearization point: invalidate
		// the exact instance and remove that same pointer in one exclusive map
		// operation, so a same-ID replacement cannot be deleted by a stale call.
		sub.InvalidateRuntimePreparation()
		removed = true
		return nil, xsync.DeleteOp
	})
	return removed
}

// UnregisterIf is retained as a readable alias for callers already using the
// exact-instance contract.
func (m *SubscriptionManager) UnregisterIf(id string, expected *subscription.Subscription) bool {
	return m.UnregisterExact(id, expected)
}

// Lookup returns a subscription by ID (convenience for pool's subLookup).
func (m *SubscriptionManager) Lookup(subID string) *subscription.Subscription {
	sub, _ := m.subs.Load(subID)
	return sub
}

// Range iterates all subscriptions.
func (m *SubscriptionManager) Range(fn func(id string, sub *subscription.Subscription) bool) {
	m.subs.Range(fn)
}

// Size returns the number of subscriptions.
func (m *SubscriptionManager) Size() int {
	return m.subs.Size()
}
