// Package subscription provides subscription types and parsing logic.
package subscription

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/puzpuzpuz/xsync/v4"
)

const defaultEphemeralNodeEvictDelayNs = int64(72 * time.Hour)

const (
	// SourceTypeRemote pulls subscription content over HTTP(S) from URL.
	SourceTypeRemote = "remote"
	// SourceTypeLocal reads subscription content from local text content.
	SourceTypeLocal = "local"
)

const (
	// UpdateModeReplace replaces current subscription nodes with refreshed content.
	UpdateModeReplace = false
	// UpdateModeIncrementalAlive keeps existing non-evicted nodes and merges refreshed content.
	UpdateModeIncrementalAlive = true
)

// ManagedNode represents one hash entry in subscription managed nodes.
type ManagedNode struct {
	Tags    []string
	Evicted bool
}

// ManagedNodes wraps hash->ManagedNode map.
//
// Maintenance rule:
//   - StoreNode makes a defensive copy of input Tags.
//   - LoadNode/RangeNodes return direct references to stored tag slices.
//   - Callers must treat returned Tags as read-only and must not mutate them.
//   - If mutation is needed, make an explicit copy first.
type ManagedNodes struct {
	m *xsync.Map[node.Hash, ManagedNode]
}

// cancellableOpLock serializes high-level subscription mutations while still
// allowing an HTTP caller to stop waiting when its context is canceled.
// The zero value is ready for use.
type cancellableOpLock struct {
	once  sync.Once
	token chan struct{}
}

func (m *cancellableOpLock) init() {
	m.token = make(chan struct{}, 1)
	m.token <- struct{}{}
}

func (m *cancellableOpLock) lockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.once.Do(m.init)
	select {
	case <-m.token:
		if err := ctx.Err(); err != nil {
			m.unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *cancellableOpLock) unlock() {
	m.once.Do(m.init)
	m.token <- struct{}{}
}

// NewManagedNodes creates an empty managed-node view.
func NewManagedNodes() *ManagedNodes {
	return &ManagedNodes{m: xsync.NewMap[node.Hash, ManagedNode]()}
}

// Size returns the count of hash entries (including evicted entries).
func (mn *ManagedNodes) Size() int {
	if mn == nil || mn.m == nil {
		return 0
	}
	return mn.m.Size()
}

// LoadNode loads the full managed-node state for a hash.
// Tags are returned as-is (no copy); treat them as read-only.
func (mn *ManagedNodes) LoadNode(h node.Hash) (ManagedNode, bool) {
	if mn == nil || mn.m == nil {
		return ManagedNode{}, false
	}
	n, ok := mn.m.Load(h)
	if !ok {
		return ManagedNode{}, false
	}
	return n, true
}

// StoreNode stores the full managed-node state for a hash.
// Tags are defensively copied on store.
func (mn *ManagedNodes) StoreNode(h node.Hash, n ManagedNode) {
	if mn == nil || mn.m == nil {
		return
	}
	mn.m.Store(h, ManagedNode{
		Tags:    cloneTags(n.Tags),
		Evicted: n.Evicted,
	})
}

// Delete deletes a hash entry.
func (mn *ManagedNodes) Delete(h node.Hash) {
	if mn == nil || mn.m == nil {
		return
	}
	mn.m.Delete(h)
}

// RangeNodes iterates hash->ManagedNode entries.
// ManagedNode.Tags is provided as-is (no copy); treat it as read-only.
func (mn *ManagedNodes) RangeNodes(fn func(node.Hash, ManagedNode) bool) {
	if mn == nil || mn.m == nil || fn == nil {
		return
	}
	mn.m.Range(fn)
}

// Subscription represents a subscription's runtime state.
// It has two synchronization layers:
//   - mu protects mutable config fields
//     (url/updateInterval/name/enabled/ephemeral/ephemeralNodeEvictDelayNs).
//   - opMu serializes high-level operations (update/rename/eviction/delete)
//     on the same subscription instance.
//
// Lock-order rule (must be preserved to avoid deadlocks):
//   - If both locks are needed in one flow, always acquire opMu before mu.
//   - Never call WithOpLock from code that already holds mu.
type Subscription struct {
	// Immutable after creation.
	ID string

	// Operation-level lock for serializing multi-step workflows.
	opMu cancellableOpLock

	// Mutable fields guarded by mu.
	mu         sync.RWMutex
	url        string
	sourceType string
	content    string
	// updateIntervalNs is the configured subscription refresh interval.
	updateIntervalNs      int64
	name                  string
	enabled               bool
	ephemeral             bool
	incrementalAliveNodes bool
	// ephemeralNodeEvictDelayNs is the per-subscription eviction delay for
	// circuit-broken nodes when Ephemeral is enabled.
	ephemeralNodeEvictDelayNs int64

	// Persistence timestamps (written under mu or single-writer context).
	CreatedAtNs int64
	UpdatedAtNs int64

	// Runtime-only fields (NOT persisted). Atomic for lock-free reads
	// from the scheduler's due-check loop.
	LastCheckedNs atomic.Int64
	LastUpdatedNs atomic.Int64
	LastError     atomic.Pointer[string]

	// Scheduler sequencing for stale-attempt guards.
	attemptSeq     atomic.Int64
	lastAppliedSeq atomic.Int64

	// managedNodes is the subscription's node view: Hash → ManagedNode.
	// Swapped atomically on subscription update.
	managedNodes atomic.Pointer[ManagedNodes]

	// configVersion is incremented whenever refresh-input-related config changes
	// (URL/source/content/update-interval). Scheduler uses it for stale-guard.
	configVersion atomic.Int64
}

// ConfigSnapshot is an immutable copy of the persisted subscription
// configuration and timestamps. Callers may use it after the subscription
// operation lock has been released while building a response.
type ConfigSnapshot struct {
	URL                       string
	SourceType                string
	Content                   string
	ConfigVersion             int64
	UpdateIntervalNs          int64
	Name                      string
	Enabled                   bool
	Ephemeral                 bool
	IncrementalAliveNodes     bool
	EphemeralNodeEvictDelayNs int64
	CreatedAtNs               int64
	UpdatedAtNs               int64
}

// NewSubscription creates a Subscription with an empty ManagedNodes map.
func NewSubscription(id, name, url string, enabled, ephemeral bool) *Subscription {
	s := &Subscription{
		ID:                        id,
		url:                       url,
		sourceType:                SourceTypeRemote,
		name:                      name,
		enabled:                   enabled,
		ephemeral:                 ephemeral,
		incrementalAliveNodes:     UpdateModeReplace,
		ephemeralNodeEvictDelayNs: defaultEphemeralNodeEvictDelayNs,
	}
	empty := NewManagedNodes()
	s.managedNodes.Store(empty)
	emptyErr := ""
	s.LastError.Store(&emptyErr)
	s.configVersion.Store(1)
	return s
}

// SetLastError atomically sets the last error string.
func (s *Subscription) SetLastError(err string) { s.LastError.Store(&err) }

// GetLastError atomically loads the last error string.
func (s *Subscription) GetLastError() string { return *s.LastError.Load() }

// NextAttemptSeq returns a strictly increasing sequence for refresh attempts.
func (s *Subscription) NextAttemptSeq() int64 { return s.attemptSeq.Add(1) }

// LastAppliedSeq returns the latest committed refresh result sequence.
func (s *Subscription) LastAppliedSeq() int64 { return s.lastAppliedSeq.Load() }

// MarkAppliedAttempt records the latest committed refresh result sequence.
func (s *Subscription) MarkAppliedAttempt(seq int64) { s.lastAppliedSeq.Store(seq) }

// WithOpLock runs fn under the subscription operation lock.
func (s *Subscription) WithOpLock(fn func()) {
	if s == nil || fn == nil {
		return
	}
	_ = s.opMu.lockContext(context.Background())
	defer s.opMu.unlock()
	fn()
}

// WithOpLockContext runs fn under the subscription operation lock, returning
// the caller context error if the lock cannot be acquired in time.
func (s *Subscription) WithOpLockContext(ctx context.Context, fn func()) error {
	if s == nil || fn == nil {
		return nil
	}
	if err := s.opMu.lockContext(ctx); err != nil {
		return err
	}
	defer s.opMu.unlock()
	fn()
	return nil
}

// SnapshotConfig copies all response configuration under the established
// opMu -> mu lock order. The copy is intentionally short-lived; callers must
// perform potentially slow managed-node and pool reads after it returns.
func (s *Subscription) SnapshotConfig() ConfigSnapshot {
	snapshot, _ := s.SnapshotConfigContext(context.Background())
	return snapshot
}

// SnapshotConfigContext copies all response configuration under the
// established opMu -> mu lock order while allowing a request-scoped caller to
// stop waiting for the operation owner.
func (s *Subscription) SnapshotConfigContext(ctx context.Context) (ConfigSnapshot, error) {
	if s == nil {
		return ConfigSnapshot{}, nil
	}
	var snapshot ConfigSnapshot
	if err := s.WithOpLockContext(ctx, func() {
		s.mu.RLock()
		snapshot = ConfigSnapshot{
			URL:                       s.url,
			SourceType:                normalizeSourceType(s.sourceType),
			Content:                   s.content,
			ConfigVersion:             s.configVersion.Load(),
			UpdateIntervalNs:          s.updateIntervalNs,
			Name:                      s.name,
			Enabled:                   s.enabled,
			Ephemeral:                 s.ephemeral,
			IncrementalAliveNodes:     s.incrementalAliveNodes,
			EphemeralNodeEvictDelayNs: s.ephemeralNodeEvictDelayNs,
		}
		s.mu.RUnlock()

		// These timestamps are immutable/operation-owned respectively. Holding
		// opMu makes their copy part of the same configuration generation.
		snapshot.CreatedAtNs = s.CreatedAtNs
		snapshot.UpdatedAtNs = s.UpdatedAtNs
	}); err != nil {
		return ConfigSnapshot{}, err
	}
	return snapshot, nil
}

// URL returns the subscription source URL (thread-safe).
func (s *Subscription) URL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.url
}

// SourceType returns the subscription source type (thread-safe).
func (s *Subscription) SourceType() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return normalizeSourceType(s.sourceType)
}

// Content returns the local subscription content (thread-safe).
func (s *Subscription) Content() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.content
}

// ConfigVersion returns the scheduler input config version.
func (s *Subscription) ConfigVersion() int64 {
	return s.configVersion.Load()
}

// UpdateIntervalNs returns the configured update interval in nanoseconds (thread-safe).
func (s *Subscription) UpdateIntervalNs() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.updateIntervalNs
}

// SetFetchConfig updates URL and update interval together atomically under lock.
func (s *Subscription) SetFetchConfig(url string, updateIntervalNs int64) {
	s.mu.Lock()
	changed := s.url != url || s.updateIntervalNs != updateIntervalNs
	s.url = url
	s.updateIntervalNs = updateIntervalNs
	if changed {
		s.configVersion.Add(1)
	}
	s.mu.Unlock()
}

// SetSourceType updates subscription source type (thread-safe).
func (s *Subscription) SetSourceType(sourceType string) {
	sourceType = normalizeSourceType(sourceType)
	s.mu.Lock()
	if s.sourceType != sourceType {
		s.sourceType = sourceType
		s.configVersion.Add(1)
	}
	s.mu.Unlock()
}

// SetContent updates local subscription content (thread-safe).
func (s *Subscription) SetContent(content string) {
	s.mu.Lock()
	if s.content != content {
		s.content = content
		s.configVersion.Add(1)
	}
	s.mu.Unlock()
}

// Name returns the subscription name (thread-safe).
func (s *Subscription) Name() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.name
}

// SetName updates the subscription name (thread-safe).
func (s *Subscription) SetName(name string) {
	s.mu.Lock()
	s.name = name
	s.mu.Unlock()
}

// Enabled returns whether the subscription is enabled (thread-safe).
func (s *Subscription) Enabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled
}

// SetEnabled updates the enabled flag (thread-safe).
func (s *Subscription) SetEnabled(v bool) {
	s.mu.Lock()
	s.enabled = v
	s.mu.Unlock()
}

// Ephemeral returns whether the subscription is ephemeral (thread-safe).
func (s *Subscription) Ephemeral() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ephemeral
}

// SetEphemeral updates the ephemeral flag (thread-safe).
func (s *Subscription) SetEphemeral(v bool) {
	s.mu.Lock()
	s.ephemeral = v
	s.mu.Unlock()
}

// IncrementalAliveNodes returns whether refresh keeps existing non-evicted nodes (thread-safe).
func (s *Subscription) IncrementalAliveNodes() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.incrementalAliveNodes
}

// SetIncrementalAliveNodes updates the refresh merge mode (thread-safe).
func (s *Subscription) SetIncrementalAliveNodes(v bool) {
	s.mu.Lock()
	s.incrementalAliveNodes = v
	s.mu.Unlock()
}

// EphemeralNodeEvictDelayNs returns the per-subscription eviction delay in nanoseconds.
func (s *Subscription) EphemeralNodeEvictDelayNs() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ephemeralNodeEvictDelayNs
}

// SetEphemeralNodeEvictDelayNs updates the per-subscription eviction delay.
func (s *Subscription) SetEphemeralNodeEvictDelayNs(v int64) {
	s.mu.Lock()
	s.ephemeralNodeEvictDelayNs = v
	s.mu.Unlock()
}

// ManagedNodes returns the current node view via atomic load.
func (s *Subscription) ManagedNodes() *ManagedNodes {
	return s.managedNodes.Load()
}

// SwapManagedNodes atomically replaces the managed nodes view.
func (s *Subscription) SwapManagedNodes(m *ManagedNodes) {
	s.managedNodes.Store(m)
}

// DiffHashes computes the hash diff between old and new managed-nodes maps.
// Returns slices of added, kept, and removed hashes.
func DiffHashes(
	oldMap, newMap *ManagedNodes,
) (added, kept, removed []node.Hash) {
	// Hashes only in new → added. Hashes in both → kept.
	newMap.RangeNodes(func(h node.Hash, _ ManagedNode) bool {
		if _, ok := oldMap.LoadNode(h); ok {
			kept = append(kept, h)
		} else {
			added = append(added, h)
		}
		return true
	})

	// Hashes only in old → removed.
	oldMap.RangeNodes(func(h node.Hash, _ ManagedNode) bool {
		if _, ok := newMap.LoadNode(h); !ok {
			removed = append(removed, h)
		}
		return true
	})

	return added, kept, removed
}

func normalizeSourceType(sourceType string) string {
	switch sourceType {
	case SourceTypeLocal:
		return SourceTypeLocal
	default:
		return SourceTypeRemote
	}
}

func cloneTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	cp := make([]string, len(tags))
	copy(cp, tags)
	return cp
}
