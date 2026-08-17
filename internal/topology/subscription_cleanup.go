package topology

import (
	"context"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
)

// CleanupSubscriptionNodesWithConfirmNoLock marks managed nodes as evicted when
// they match shouldRemove, using two-pass confirmation to reduce TOCTOU issues.
//
// It keeps hashes in managed view, removes pool subscription references, and
// returns newly-evicted hashes for persistence upsert compensation.
// Caller must hold sub.WithOpLock while invoking this function.
func CleanupSubscriptionNodesWithConfirmNoLock(
	sub *subscription.Subscription,
	pool *GlobalNodePool,
	shouldRemove func(entry *node.NodeEntry) bool,
	betweenScans func(),
	onSubNodeChanged func(string, node.Hash, bool),
	admission PersistenceAdmission,
) (int, []node.Hash) {
	cleaned, evicted, _ := CleanupSubscriptionNodesWithConfirmContextNoLock(
		context.Background(),
		sub,
		pool,
		shouldRemove,
		betweenScans,
		onSubNodeChanged,
		admission,
	)
	return cleaned, evicted
}

// CleanupSubscriptionNodesWithConfirmContextNoLock is the context-aware form
// used by request-bound control-plane cleanup. It prepares the next managed
// view before waiting for the runtime owner, so cancellation cannot publish a
// partial generation.
func CleanupSubscriptionNodesWithConfirmContextNoLock(
	ctx context.Context,
	sub *subscription.Subscription,
	pool *GlobalNodePool,
	shouldRemove func(entry *node.NodeEntry) bool,
	betweenScans func(),
	onSubNodeChanged func(string, node.Hash, bool),
	admission PersistenceAdmission,
) (int, []node.Hash, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if sub == nil || pool == nil || shouldRemove == nil {
		return 0, nil, nil
	}

	currentManaged := sub.ManagedNodes()
	removeCandidates := make(map[node.Hash]*node.NodeEntry)
	currentManaged.RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
		if managed.Evicted {
			return true
		}
		entry, ok := pool.GetEntry(h)
		if !ok {
			return true
		}
		if shouldRemove(entry) {
			removeCandidates[h] = entry
		}
		return true
	})
	if len(removeCandidates) == 0 {
		return 0, nil, nil
	}

	if betweenScans != nil {
		betweenScans()
	}

	confirmedRemove := make(map[node.Hash]*node.NodeEntry)
	for h, expected := range removeCandidates {
		managed, ok := currentManaged.LoadNode(h)
		if !ok || managed.Evicted {
			continue
		}
		entry, ok := pool.GetEntry(h)
		if !ok || entry != expected {
			continue
		}
		if shouldRemove(entry) {
			confirmedRemove[h] = expected
		}
	}
	if len(confirmedRemove) == 0 {
		return 0, nil, nil
	}

	nextManaged := subscription.NewManagedNodes()
	newlyEvicted := make([]node.Hash, 0, len(confirmedRemove))
	currentManaged.RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
		if _, remove := confirmedRemove[h]; remove {
			if !managed.Evicted {
				newlyEvicted = append(newlyEvicted, h)
			}
			managed.Evicted = true
		}
		nextManaged.StoreNode(h, managed)
		return true
	})
	if err := pool.WithRuntimeMutationContext(ctx, func() {
		sub.SwapManagedNodes(nextManaged)

		for _, h := range newlyEvicted {
			pool.removeNodeFromSub(h, sub.ID, onSubNodeChanged, admission)
		}
	}); err != nil {
		return 0, nil, err
	}

	return len(newlyEvicted), newlyEvicted, nil
}
