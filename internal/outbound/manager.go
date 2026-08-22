package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/runtimeguard"
	"github.com/sagernet/sing-box/adapter"
)

var ErrOutboundNotReady = errors.New("outbound not ready")

// PoolAccessor provides read-only access to the node pool.
type PoolAccessor interface {
	GetEntry(hash node.Hash) (*node.NodeEntry, bool)
	// IsNodeDisabled is the pool's current subscription-admission view. Traffic
	// callers must fail closed when it reports true, even if a published view
	// still contains the hash during a rebuild.
	IsNodeDisabled(hash node.Hash) bool
	RangeNodes(fn func(node.Hash, *node.NodeEntry) bool)
}

// poolLifecycleAccessor lets shutdown share the pool's mutation boundary with
// node-removal callbacks. A callback is invoked after the pool entry is
// deleted, so a plain RangeNodes snapshot can otherwise miss its outbound
// retirement entirely.
type poolLifecycleAccessor interface {
	WithNodeLifecycle(func())
}

// closeOutbound closes an outbound if it implements io.Closer.
func closeOutbound(ob adapter.Outbound) {
	if c, ok := ob.(io.Closer); ok {
		_ = c.Close()
	}
}

func buildOutboundSafely(builder OutboundBuilder, rawOptions json.RawMessage) (ob adapter.Outbound, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ob != nil {
				closeOutbound(ob)
			}
			ob = nil
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return builder.Build(rawOptions)
}

// OutboundManager manages outbound lifecycle and provides unified HTTP execution.
type OutboundManager struct {
	pool    PoolAccessor
	builder OutboundBuilder

	lifecycleMu  sync.Mutex
	closed       bool
	activeBuilds int
	buildsDone   chan struct{}
	buildsClosed bool
	shutdownDone chan struct{}

	// retirementMu protects outbounds that have left the pool but whose
	// adapter close is still owned by an entry lease. Shutdown must include
	// these exact entries in its retirement snapshot.
	retirementMu sync.Mutex
	retirements  map[*node.NodeEntry]<-chan struct{}

	// retirementAdmissionMu orders shutdown's retirement snapshot with
	// onNodeRemoved callbacks that have already entered RemoveNodeOutbound.
	// Such a callback may be blocked briefly on retirementMu; shutdown must
	// wait for it to register before taking its final pending snapshot.
	retirementAdmissionMu     sync.Mutex
	retirementAdmissionCond   *sync.Cond
	retirementAdmissionClosed bool
	activeRetirementCalls     int

	// beforeRetirementWaitHook is a package-test seam. Production leaves it
	// nil; tests use it to observe the exact retirement set before Shutdown
	// waits for adapter close completion.
	beforeRetirementWaitHook func(int)
	// beforeRetirementTrackHook is a package-test seam after a removal callback
	// has entered retirement admission and before it registers the entry.
	// Production leaves it nil.
	beforeRetirementTrackHook func(*node.NodeEntry)
	// beforeRetirementLifecycleHook is a package-test seam immediately before
	// shutdown enters the pool mutation boundary.
	beforeRetirementLifecycleHook func()
	// beforeGuardCommitHook is a package-test seam after external build work and
	// before the guarded final commit. Production leaves it nil.
	beforeGuardCommitHook func()
}

func NewOutboundManager(pool PoolAccessor, builder OutboundBuilder) *OutboundManager {
	m := &OutboundManager{
		pool:        pool,
		builder:     builder,
		buildsDone:  make(chan struct{}),
		retirements: make(map[*node.NodeEntry]<-chan struct{}),
	}
	m.retirementAdmissionCond = sync.NewCond(&m.retirementAdmissionMu)
	return m
}

func (m *OutboundManager) isLiveEntry(hash node.Hash, entry *node.NodeEntry) bool {
	current, ok := m.pool.GetEntry(hash)
	return ok && current == entry
}

// EnsureNodeOutbound idempotently creates and stores an outbound for a node.
// The NodeEntry owner serializes publication with retirement; losing builds
// are discarded and closed.
func (m *OutboundManager) EnsureNodeOutbound(hash node.Hash) {
	m.ensureNodeOutbound(hash, nil, nil)
}

// EnsureNodeOutboundForEntry prepares an outbound only for the exact node
// generation captured by the caller. A same-hash replacement is a different
// owner and must be prepared by its own creation callback.
func (m *OutboundManager) EnsureNodeOutboundForEntry(hash node.Hash, expected *node.NodeEntry) {
	m.ensureNodeOutbound(hash, expected, nil)
}

// EnsureNodeOutboundForEntryGuarded performs the same build, but refuses to
// publish a result after the owning subscription generation is invalidated.
// The guard is checked again while holding the short publication lock; no
// lock spans the external builder.
func (m *OutboundManager) EnsureNodeOutboundForEntryGuarded(
	hash node.Hash,
	expected *node.NodeEntry,
	guard *runtimeguard.Guard,
) {
	m.ensureNodeOutbound(hash, expected, guard)
}

func (m *OutboundManager) ensureNodeOutbound(
	hash node.Hash,
	expected *node.NodeEntry,
	guard *runtimeguard.Guard,
) {
	if m == nil || m.pool == nil {
		return
	}
	if guard != nil && !guard.Allowed() {
		return
	}
	m.lifecycleMu.Lock()
	if m.closed {
		m.lifecycleMu.Unlock()
		return
	}
	m.activeBuilds++
	m.lifecycleMu.Unlock()
	defer m.finishBuild()

	entry, ok := m.pool.GetEntry(hash)
	if !ok || (expected != nil && entry != expected) || (guard != nil && !guard.Allowed()) {
		return
	}
	// Fast path: already has outbound.
	if entry.Outbound.Load() != nil {
		return
	}

	ob, err := buildOutboundSafely(m.builder, entry.RawOptions)
	if err != nil {
		// Builders are allowed to return a partially-created adapter alongside
		// an error. It is still owned by this failed attempt and must not leak.
		if ob != nil {
			closeOutbound(ob)
		}
		// Serialize the status write with successful publication. A concurrent
		// build may already have installed a usable outbound; that result owns
		// the recovery state and a late failed attempt must not make the live
		// entry look broken again.
		commit := func() {
			m.lifecycleMu.Lock()
			defer m.lifecycleMu.Unlock()
			if !m.closed && m.isLiveEntry(hash, entry) &&
				(expected == nil || entry == expected) && entry.Outbound.Load() == nil {
				entry.SetLastError("outbound build: " + err.Error())
			}
		}
		if guard == nil {
			commit()
		} else {
			guard.Commit(commit)
		}
		return
	}

	if guard != nil {
		if hook := m.beforeGuardCommitHook; hook != nil {
			hook()
		}
	}
	// Serialize the final live-entry check and publication with Shutdown. A
	// build may run for an arbitrary duration, so the lifecycle lock covers only
	// this short commit point; a result built after admission closes is closed
	// instead of escaping the shutdown retirement snapshot. A guarded commit
	// also shares the subscription gate with invalidation, so invalidation
	// cannot return between the token check and this publication.
	installed := false
	commit := func() {
		m.lifecycleMu.Lock()
		defer m.lifecycleMu.Unlock()
		if m.closed || !m.isLiveEntry(hash, entry) || (expected != nil && entry != expected) {
			return
		}
		installed = entry.InstallOutboundIfAbsent(ob)
		if installed {
			// Keep the recovery state update in the same lifecycle commit as the
			// outbound publication, so a concurrent failed build cannot overwrite
			// it after this clear.
			entry.SetLastError("")
		}
	}
	committed := true
	if guard == nil {
		commit()
	} else {
		committed = guard.Commit(commit)
	}
	if !committed || !installed {
		// Another goroutine won the race. Close the losing build result.
		closeOutbound(ob)
		return
	}
	// Retire-and-close if the node disappeared/replaced right after install.
	if !m.isLiveEntry(hash, entry) {
		m.trackRetirement(entry)
	}
}

func (m *OutboundManager) finishBuild() {
	m.lifecycleMu.Lock()
	m.activeBuilds--
	if m.closed && m.activeBuilds == 0 && !m.buildsClosed {
		close(m.buildsDone)
		m.buildsClosed = true
	}
	m.lifecycleMu.Unlock()
}

func (m *OutboundManager) closeBuildAdmissionLocked() <-chan struct{} {
	if m.activeBuilds == 0 && !m.buildsClosed {
		close(m.buildsDone)
		m.buildsClosed = true
	}
	return m.buildsDone
}

// RemoveNodeOutbound clears a node's outbound reference.
// Accepts the entry directly because the node may already be deleted from the pool
// (RemoveNodeFromSub deletes before firing onNodeRemoved callback).
func (m *OutboundManager) RemoveNodeOutbound(entry *node.NodeEntry) {
	if entry == nil {
		return
	}
	if m == nil {
		entry.RetireOutbound()
		return
	}
	if !m.beginRetirementCall() {
		// Shutdown has already taken its final retirement snapshot. This late
		// callback is outside that owner, but it must still synchronously stop
		// new leases on the exact retired entry rather than silently escaping.
		entry.RetireOutbound()
		return
	}
	defer m.endRetirementCall()
	if hook := m.beforeRetirementTrackHook; hook != nil {
		hook(entry)
	}
	m.trackRetirement(entry)
}

func (m *OutboundManager) beginRetirementCall() bool {
	m.retirementAdmissionMu.Lock()
	defer m.retirementAdmissionMu.Unlock()
	if m.retirementAdmissionClosed {
		return false
	}
	m.activeRetirementCalls++
	return true
}

func (m *OutboundManager) endRetirementCall() {
	m.retirementAdmissionMu.Lock()
	m.activeRetirementCalls--
	if m.activeRetirementCalls == 0 {
		m.retirementAdmissionCond.Broadcast()
	}
	m.retirementAdmissionMu.Unlock()
}

func (m *OutboundManager) closeRetirementAdmissionAndWait() {
	m.retirementAdmissionMu.Lock()
	m.retirementAdmissionClosed = true
	for m.activeRetirementCalls != 0 {
		m.retirementAdmissionCond.Wait()
	}
	m.retirementAdmissionMu.Unlock()
}

// WarmupAll iterates all nodes in the pool and ensures each has an outbound.
// Called once after bootstrap to avoid ErrOutboundNotReady on restart.
func (m *OutboundManager) WarmupAll() {
	m.pool.RangeNodes(func(h node.Hash, _ *node.NodeEntry) bool {
		m.EnsureNodeOutbound(h)
		return true
	})
}

// RetireAllOutbounds releases every outbound currently published by the pool.
// It is used when a topology is discarded before its background workers start;
// each entry owns the exact close/lease state, so retirement remains
// non-blocking and closes adapters only after any admitted use releases.
func (m *OutboundManager) RetireAllOutbounds() {
	m.retireAllOutbounds(false)
}

// RetireAllOutboundsAndWait retires every outbound and waits until each
// adapter's close/lease ownership has completed. Rollback callers use this
// before returning so they do not leave cleanup running in the background.
func (m *OutboundManager) RetireAllOutboundsAndWait() {
	m.retireAllOutbounds(true)
}

func (m *OutboundManager) retireAllOutbounds(wait bool) {
	if m == nil || m.pool == nil {
		return
	}
	m.retirementMu.Lock()
	m.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
		if entry != nil {
			m.trackRetirementLocked(entry)
		}
		return true
	})
	retirements := make([]<-chan struct{}, 0, len(m.retirements))
	for _, done := range m.retirements {
		retirements = append(retirements, done)
	}
	m.retirementMu.Unlock()
	if wait {
		if hook := m.beforeRetirementWaitHook; hook != nil {
			hook(len(retirements))
		}
	}
	for _, done := range retirements {
		<-done
	}
}

// retireAllOutboundsForShutdown takes the pool snapshot first, then closes
// retirement admission and waits for callbacks that entered before that
// boundary to finish registering. The second snapshot is therefore the exact
// owner set for shutdown; it is not just a best-effort copy of the first one.
func (m *OutboundManager) retireAllOutboundsForShutdown() {
	if m == nil || m.pool == nil {
		return
	}

	if hook := m.beforeRetirementLifecycleHook; hook != nil {
		hook()
	}
	retireAndCloseAdmission := func() {
		m.retirementMu.Lock()
		m.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
			if entry != nil {
				m.trackRetirementLocked(entry)
			}
			return true
		})
		m.retirementMu.Unlock()

		m.closeRetirementAdmissionAndWait()
	}
	if lifecycle, ok := m.pool.(poolLifecycleAccessor); ok {
		// GlobalNodePool holds this boundary through the external removal
		// callback. Taking the same lock makes the initial snapshot and callback
		// retirement admission one linearization point.
		lifecycle.WithNodeLifecycle(retireAndCloseAdmission)
	} else {
		// Lightweight test pools and other callers without a lifecycle owner
		// retain the historical snapshot behavior.
		retireAndCloseAdmission()
	}

	m.retirementMu.Lock()
	retirements := make([]<-chan struct{}, 0, len(m.retirements))
	for _, done := range m.retirements {
		retirements = append(retirements, done)
	}
	m.retirementMu.Unlock()

	if hook := m.beforeRetirementWaitHook; hook != nil {
		hook(len(retirements))
	}
	for _, done := range retirements {
		<-done
	}
}

func (m *OutboundManager) trackRetirement(entry *node.NodeEntry) {
	if m == nil || entry == nil {
		return
	}
	m.retirementMu.Lock()
	m.trackRetirementLocked(entry)
	m.retirementMu.Unlock()
}

func (m *OutboundManager) trackRetirementLocked(entry *node.NodeEntry) {
	entry.RetireOutbound()
	done := entry.OutboundRetirementDone()
	select {
	case <-done:
		return
	default:
	}
	if m.retirements == nil {
		m.retirements = make(map[*node.NodeEntry]<-chan struct{})
	}
	if current, ok := m.retirements[entry]; ok && current == done {
		return
	}
	m.retirements[entry] = done
	go m.reapRetirement(entry, done)
}

func (m *OutboundManager) reapRetirement(entry *node.NodeEntry, done <-chan struct{}) {
	<-done
	m.retirementMu.Lock()
	if current, ok := m.retirements[entry]; ok && current == done {
		delete(m.retirements, entry)
	}
	m.retirementMu.Unlock()
}

// Shutdown closes outbound construction admission and retires every currently
// published outbound. Existing leased operations retain their adapter until
// release; no later EnsureNodeOutbound can publish a new one.
func (m *OutboundManager) Shutdown() {
	_ = m.ShutdownContext(context.Background())
}

// ShutdownContext closes outbound construction admission and waits for the
// single retirement owner with the caller's context. A caller that times out
// does not cancel retirement; a later Background call can join the same owner.
func (m *OutboundManager) ShutdownContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	if m.closed {
		done := m.shutdownDone
		m.lifecycleMu.Unlock()
		if done == nil {
			return nil
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.closed = true
	buildsDone := m.closeBuildAdmissionLocked()
	done := make(chan struct{})
	m.shutdownDone = done
	m.lifecycleMu.Unlock()

	go func() {
		<-buildsDone
		m.retireAllOutboundsForShutdown()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Fetch executes HTTP request using the node's outbound.
// Returns ErrOutboundNotReady if the node's outbound is not yet initialized.
// ctx controls timeout/cancellation.
func (m *OutboundManager) Fetch(ctx context.Context, hash node.Hash, url string) ([]byte, time.Duration, error) {
	return m.FetchWithUserAgent(ctx, hash, url, "")
}

// FetchWithUserAgent executes HTTP request using the node's outbound and
// applies the given User-Agent if non-empty.
func (m *OutboundManager) FetchWithUserAgent(
	ctx context.Context,
	hash node.Hash,
	url string,
	userAgent string,
) ([]byte, time.Duration, error) {
	return m.fetchWithExpectedEntry(ctx, hash, nil, url, userAgent)
}

// FetchWithExpectedEntry binds a fetch to the exact entry selected before the
// network operation. A same-hash replacement is rejected instead of silently
// using a node that did not pass the original platform view.
func (m *OutboundManager) FetchWithExpectedEntry(
	ctx context.Context,
	hash node.Hash,
	expected *node.NodeEntry,
	url string,
	userAgent string,
) ([]byte, time.Duration, error) {
	if expected == nil {
		return nil, 0, errors.New("expected node entry is nil")
	}
	return m.fetchWithExpectedEntry(ctx, hash, expected, url, userAgent)
}

func (m *OutboundManager) fetchWithExpectedEntry(
	ctx context.Context,
	hash node.Hash,
	expected *node.NodeEntry,
	url string,
	userAgent string,
) ([]byte, time.Duration, error) {
	// Admission and lease acquisition share the lifecycle lock with Shutdown.
	// This keeps a fetch that is admitted before shutdown in the retirement
	// owner, while rejecting every fetch that starts after closed is published.
	m.lifecycleMu.Lock()
	if m.closed {
		m.lifecycleMu.Unlock()
		return nil, 0, ErrOutboundNotReady
	}
	entry, ok := m.pool.GetEntry(hash)
	if !ok {
		m.lifecycleMu.Unlock()
		return nil, 0, errors.New("node not found")
	}
	if expected != nil && entry != expected {
		m.lifecycleMu.Unlock()
		return nil, 0, errors.New("node entry replaced")
	}
	if entry == nil || !entry.IsHealthy() || m.pool.IsNodeDisabled(hash) {
		m.lifecycleMu.Unlock()
		return nil, 0, ErrOutboundNotReady
	}
	outbound, release, ok := entry.AcquireOutbound()
	m.lifecycleMu.Unlock()
	if !ok {
		return nil, 0, ErrOutboundNotReady
	}
	defer release()
	return netutil.HTTPGetViaOutbound(ctx, outbound, url, netutil.OutboundHTTPOptions{
		RequireStatusOK: true,
		UserAgent:       userAgent,
	})
}
