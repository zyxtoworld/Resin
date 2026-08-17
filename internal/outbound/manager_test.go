package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// --- Test helpers ---

// countingBuilder counts how many times Build is called.
type countingBuilder struct {
	mu    sync.Mutex
	count int
}

func (b *countingBuilder) Build(_ json.RawMessage) (adapter.Outbound, error) {
	b.mu.Lock()
	b.count++
	b.mu.Unlock()
	// Simulate some work to increase chance of concurrent calls overlapping.
	time.Sleep(time.Millisecond)
	return testutil.NewNoopOutbound(), nil
}

func (b *countingBuilder) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// failBuilder always fails.
type failBuilder struct{}

func (b *failBuilder) Build(_ json.RawMessage) (adapter.Outbound, error) {
	return nil, errors.New("simulated build failure")
}

type panicBuilder struct{}

func (b *panicBuilder) Build(_ json.RawMessage) (adapter.Outbound, error) {
	panic("simulated build panic")
}

type closableOnly struct {
	closed       atomic.Bool
	closeEntered chan struct{}
	allowClose   chan struct{}
	closeOnce    sync.Once
}

type fetchTrackingOutbound struct {
	dialEntered chan struct{}
	dialOnce    sync.Once
	allowDial   <-chan struct{}
}

func (o *fetchTrackingOutbound) Type() string           { return "fetch-tracking" }
func (o *fetchTrackingOutbound) Tag() string            { return "fetch-tracking" }
func (o *fetchTrackingOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *fetchTrackingOutbound) Dependencies() []string { return nil }
func (o *fetchTrackingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dialOnce.Do(func() { close(o.dialEntered) })
	if o.allowDial != nil {
		<-o.allowDial
	}
	return nil, errors.New("fetch-tracking: dial called")
}
func (o *fetchTrackingOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("fetch-tracking: listen packet not supported")
}

func (c *closableOnly) Close() error {
	c.closed.Store(true)
	if c.closeEntered != nil {
		c.closeOnce.Do(func() { close(c.closeEntered) })
	}
	if c.allowClose != nil {
		<-c.allowClose
	}
	return nil
}

func (c *closableOnly) Type() string {
	return "closable-only"
}

func (c *closableOnly) Tag() string {
	return "closable-only"
}

func (c *closableOnly) Network() []string {
	return []string{"tcp", "udp"}
}

func (c *closableOnly) Dependencies() []string {
	return nil
}

func (c *closableOnly) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("closable-only: dial not supported")
}

func (c *closableOnly) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("closable-only: listen packet not supported")
}

type blockingClosableBuilder struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	built   []*closableOnly
}

func newBlockingClosableBuilder() *blockingClosableBuilder {
	return &blockingClosableBuilder{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingClosableBuilder) Build(_ json.RawMessage) (adapter.Outbound, error) {
	select {
	case <-b.started:
	default:
		close(b.started)
	}
	<-b.release
	ob := &closableOnly{}
	b.mu.Lock()
	b.built = append(b.built, ob)
	b.mu.Unlock()
	return ob, nil
}

func (b *blockingClosableBuilder) firstBuilt(t *testing.T) *closableOnly {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.built) == 0 {
		t.Fatal("expected at least one built outbound")
	}
	return b.built[0]
}

// mockPool implements PoolAccessor for tests.
type mockPool struct {
	entries sync.Map // node.Hash -> *node.NodeEntry

	rangeEntered chan struct{}
	allowRange   chan struct{}
	rangeOnce    sync.Once
}

func (p *mockPool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	v, ok := p.entries.Load(hash)
	if !ok {
		return nil, false
	}
	return v.(*node.NodeEntry), true
}

func (p *mockPool) IsNodeDisabled(node.Hash) bool { return false }

func (p *mockPool) RangeNodes(fn func(node.Hash, *node.NodeEntry) bool) {
	if p.rangeEntered != nil {
		p.rangeOnce.Do(func() {
			close(p.rangeEntered)
			<-p.allowRange
		})
	}
	p.entries.Range(func(key, value any) bool {
		return fn(key.(node.Hash), value.(*node.NodeEntry))
	})
}

func (p *mockPool) addEntry(entry *node.NodeEntry) {
	p.entries.Store(entry.Hash, entry)
}

func (p *mockPool) removeEntry(hash node.Hash) {
	p.entries.Delete(hash)
}

func makeHash(seed string) node.Hash {
	return node.HashFromRawOptions([]byte(seed))
}

func newTestEntry(rawOpts string) *node.NodeEntry {
	h := makeHash(rawOpts)
	return node.NewNodeEntry(h, json.RawMessage(rawOpts), time.Now(), 0)
}

// --- Tests ---

func TestEnsureNodeOutbound_Success(t *testing.T) {
	entry := newTestEntry(`{"type":"test"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)

	if !entry.HasOutbound() {
		t.Fatal("expected HasOutbound() == true after EnsureNodeOutbound")
	}
}

func TestEnsureNodeOutbound_BuildFailure(t *testing.T) {
	entry := newTestEntry(`{"type":"fail"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &failBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)

	if entry.HasOutbound() {
		t.Fatal("expected HasOutbound() == false after build failure")
	}
	if entry.GetLastError() == "" {
		t.Fatal("expected GetLastError() non-empty after build failure")
	}
}

func TestEnsureNodeOutbound_BuildPanicCaptured(t *testing.T) {
	entry := newTestEntry(`{"type":"panic"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &panicBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)

	if entry.HasOutbound() {
		t.Fatal("expected HasOutbound() == false after build panic")
	}
	if got := entry.GetLastError(); !strings.Contains(got, "outbound build: panic: simulated build panic") {
		t.Fatalf("unexpected last error after build panic: %q", got)
	}
}

func TestEnsureNodeOutboundForEntryRejectsStaleGeneration(t *testing.T) {
	entryA := newTestEntry(`{"type":"generation-a"}`)
	entryB := node.NewNodeEntry(entryA.Hash, json.RawMessage(`{"type":"generation-b"}`), time.Now(), 0)
	pool := &mockPool{}
	pool.addEntry(entryA)
	pool.entries.Store(entryA.Hash, entryB)

	builder := &countingBuilder{}
	mgr := NewOutboundManager(pool, builder)
	mgr.EnsureNodeOutboundForEntry(entryA.Hash, entryA)
	if got := builder.Count(); got != 0 {
		t.Fatalf("stale generation started an outbound build: %d", got)
	}
	if entryA.HasOutbound() || entryB.HasOutbound() {
		t.Fatal("stale generation changed an outbound")
	}

	mgr.EnsureNodeOutboundForEntry(entryB.Hash, entryB)
	if got := builder.Count(); got != 1 {
		t.Fatalf("current generation build count = %d, want 1", got)
	}
	if !entryB.HasOutbound() {
		t.Fatal("current generation did not receive its outbound")
	}
}

func TestEnsureNodeOutbound_Idempotent(t *testing.T) {
	entry := newTestEntry(`{"type":"idem"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	builder := &countingBuilder{}
	mgr := NewOutboundManager(pool, builder)

	// Call twice sequentially.
	mgr.EnsureNodeOutbound(entry.Hash)
	mgr.EnsureNodeOutbound(entry.Hash)

	if !entry.HasOutbound() {
		t.Fatal("expected HasOutbound() == true")
	}
	// Second call should skip Build because Outbound is already non-nil.
	if builder.Count() != 1 {
		t.Fatalf("expected Build called 1 time, got %d", builder.Count())
	}
}

func TestEnsureNodeOutbound_ConcurrentIdempotent(t *testing.T) {
	entry := newTestEntry(`{"type":"conc"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	builder := &countingBuilder{}
	mgr := NewOutboundManager(pool, builder)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	// Reset outbound to nil to force all goroutines to race.
	entry.Outbound.Store(nil)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			mgr.EnsureNodeOutbound(entry.Hash)
		}()
	}
	wg.Wait()

	if !entry.HasOutbound() {
		t.Fatal("expected HasOutbound() == true after concurrent ensure")
	}
	// Due to the fast-path check (Load != nil returns early) and CAS,
	// Build may be called more than once if multiple goroutines pass
	// the fast-path check simultaneously, but the final stored value
	// is set by exactly one CAS winner.
	t.Logf("Build called %d times (expected 1-N due to race window)", builder.Count())
}

func TestEnsureNodeOutbound_NodeRemovedDuringBuild_DropsAndCloses(t *testing.T) {
	entry := newTestEntry(`{"type":"slow-build-remove"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	builder := newBlockingClosableBuilder()
	mgr := NewOutboundManager(pool, builder)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.EnsureNodeOutbound(entry.Hash)
	}()

	<-builder.started

	// Simulate removal callback order: delete from pool first, then cleanup.
	pool.removeEntry(entry.Hash)
	mgr.RemoveNodeOutbound(entry)

	close(builder.release)
	<-done

	if entry.Outbound.Load() != nil {
		t.Fatal("expected removed entry to keep outbound nil")
	}
	ob := builder.firstBuilt(t)
	if !ob.closed.Load() {
		t.Fatal("expected built outbound to be closed when node is removed during build")
	}
}

func TestShutdown_ClosesOutboundBuiltAfterAdmission(t *testing.T) {
	entry := newTestEntry(`{"type":"slow-build-shutdown"}`)
	p := &mockPool{}
	p.addEntry(entry)
	builder := newBlockingClosableBuilder()
	mgr := NewOutboundManager(p, builder)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.EnsureNodeOutbound(entry.Hash)
	}()
	<-builder.started

	shutdownDone := make(chan struct{})
	go func() {
		mgr.Shutdown()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before the admitted Build completed")
	case <-time.After(100 * time.Millisecond):
	}
	close(builder.release)
	<-done
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after the admitted Build completed")
	}

	if entry.Outbound.Load() != nil {
		t.Fatal("shutdown published an outbound after admission closed")
	}
	ob := builder.firstBuilt(t)
	if !ob.closed.Load() {
		t.Fatal("shutdown did not close the in-flight build result")
	}
}

func TestShutdownWaitsForAdmittedBuildBeforeCompleting(t *testing.T) {
	entry := newTestEntry(`{"type":"shutdown-waits-for-build"}`)
	p := &mockPool{}
	p.addEntry(entry)
	builder := newBlockingClosableBuilder()
	mgr := NewOutboundManager(p, builder)

	buildDone := make(chan struct{})
	go func() {
		defer close(buildDone)
		mgr.EnsureNodeOutbound(entry.Hash)
	}()
	select {
	case <-builder.started:
	case <-time.After(time.Second):
		t.Fatal("EnsureNodeOutbound did not enter Build")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- mgr.ShutdownContext(context.Background())
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("ShutdownContext returned before the admitted Build completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(builder.release)
	select {
	case <-buildDone:
	case <-time.After(time.Second):
		t.Fatal("admitted Build did not finish after release")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not finish after the admitted Build completed")
	}
}

func TestShutdown_WaitsForRetiredOutboundClose(t *testing.T) {
	entry := newTestEntry(`{"type":"blocking-retire-close"}`)
	pool := &mockPool{}
	pool.addEntry(entry)
	ob := &closableOnly{
		closeEntered: make(chan struct{}),
		allowClose:   make(chan struct{}),
	}
	var raw adapter.Outbound = ob
	entry.Outbound.Store(&raw)
	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})

	shutdownDone := make(chan struct{})
	go func() {
		mgr.Shutdown()
		close(shutdownDone)
	}()
	select {
	case <-ob.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not retire the published outbound")
	}
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before retired outbound Close completed")
	default:
	}

	close(ob.allowClose)
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after retired outbound Close completed")
	}
}

func TestShutdownWaitsForRemovedNodeOutboundRetirement(t *testing.T) {
	raw := json.RawMessage(`{"type":"shutdown-removed-outbound"}`)
	hash := node.HashFromRawOptions(raw)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	pool.AddNodeFromSub(hash, raw, "shutdown-sub")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("setup: node entry not found")
	}
	ob := &closableOnly{
		closeEntered: make(chan struct{}),
		allowClose:   make(chan struct{}),
	}
	var rawOutbound adapter.Outbound = ob
	entry.Outbound.Store(&rawOutbound)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	pool.SetOnNodeRemoved(func(_ node.Hash, removed *node.NodeEntry) {
		mgr.RemoveNodeOutbound(removed)
	})

	removeDone := make(chan struct{})
	go func() {
		pool.RemoveNodeFromSub(hash, "shutdown-sub")
		close(removeDone)
	}()
	select {
	case <-ob.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("node removal did not start outbound retirement")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("node removal waited for outbound Close")
	}

	retirementSet := make(chan int, 1)
	mgr.beforeRetirementWaitHook = func(count int) {
		retirementSet <- count
	}
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- mgr.ShutdownContext(context.Background())
	}()

	select {
	case count := <-retirementSet:
		if count != 1 {
			t.Fatalf("Shutdown retirement set = %d, want removed outbound included", count)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not reach retirement wait")
	}

	close(ob.allowClose)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after removed outbound Close")
	}
}

func TestShutdownDoesNotMissPoolRemovalCallbackOutsideRetirementAdmission(t *testing.T) {
	raw := json.RawMessage(`{"type":"shutdown-callback-gap"}`)
	hash := node.HashFromRawOptions(raw)
	p := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
	p.AddNodeFromSub(hash, raw, "shutdown-callback-gap-sub")
	entry, ok := p.GetEntry(hash)
	if !ok {
		t.Fatal("setup: node entry not found")
	}

	ob := &closableOnly{
		closeEntered: make(chan struct{}),
		allowClose:   make(chan struct{}),
	}
	var rawOutbound adapter.Outbound = ob
	entry.Outbound.Store(&rawOutbound)
	_, releaseLease, acquired := entry.AcquireOutbound()
	if !acquired {
		t.Fatal("setup: expected outbound lease")
	}
	var releaseLeaseOnce sync.Once

	mgr := NewOutboundManager(p, &testutil.StubOutboundBuilder{})
	callbackEntered := make(chan struct{})
	allowCallback := make(chan struct{})
	var callbackOnce sync.Once
	var allowCallbackOnce sync.Once
	var allowCloseOnce sync.Once
	p.SetOnNodeRemoved(func(_ node.Hash, removed *node.NodeEntry) {
		if removed != entry {
			t.Errorf("removed callback entry = %p, want %p", removed, entry)
		}
		callbackOnce.Do(func() { close(callbackEntered) })
		<-allowCallback
		mgr.RemoveNodeOutbound(removed)
	})

	removeDone := make(chan struct{})
	go func() {
		p.RemoveNodeFromSub(hash, "shutdown-callback-gap-sub")
		close(removeDone)
	}()
	t.Cleanup(func() {
		allowCallbackOnce.Do(func() { close(allowCallback) })
		allowCloseOnce.Do(func() { close(ob.allowClose) })
		releaseLeaseOnce.Do(releaseLease)
		select {
		case <-removeDone:
		case <-time.After(time.Second):
			t.Error("pool removal cleanup did not finish")
		}
	})
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("pool removal did not enter the production removal callback")
	}
	if _, present := p.GetEntry(hash); present {
		t.Fatal("pool entry remained visible after callback entry")
	}

	shutdownDone := make(chan error, 1)
	retirementSnapshot := make(chan int, 1)
	shutdownEntered := make(chan struct{})
	var shutdownEnteredOnce sync.Once
	mgr.beforeRetirementLifecycleHook = func() {
		shutdownEnteredOnce.Do(func() { close(shutdownEntered) })
	}
	mgr.beforeRetirementWaitHook = func(count int) {
		retirementSnapshot <- count
	}
	go func() { shutdownDone <- mgr.ShutdownContext(context.Background()) }()
	select {
	case <-shutdownEntered:
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not enter the pool lifecycle boundary")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("ShutdownContext returned before the already-started pool removal callback registered retirement: %v", err)
	default:
	}

	allowCallbackOnce.Do(func() { close(allowCallback) })
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("pool removal did not finish after callback release")
	}
	select {
	case count := <-retirementSnapshot:
		if count != 1 {
			t.Fatalf("retirement snapshot = %d, want callback retirement included", count)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not reach the final retirement snapshot")
	}
	allowCloseOnce.Do(func() { close(ob.allowClose) })
	releaseLeaseOnce.Do(releaseLease)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not finish after callback and lease release")
	}
}

func TestShutdownTracksRetirementAddedAfterPoolSnapshot(t *testing.T) {
	entry := newTestEntry(`{"type":"late-retirement"}`)
	ob := &closableOnly{
		closeEntered: make(chan struct{}),
		allowClose:   make(chan struct{}),
	}
	var raw adapter.Outbound = ob
	entry.Outbound.Store(&raw)

	p := &mockPool{
		rangeEntered: make(chan struct{}),
		allowRange:   make(chan struct{}),
	}
	p.addEntry(entry)
	mgr := NewOutboundManager(p, &testutil.StubOutboundBuilder{})

	retirementCallEntered := make(chan struct{})
	mgr.beforeRetirementTrackHook = func(*node.NodeEntry) {
		close(retirementCallEntered)
	}
	retirementSnapshot := make(chan int, 1)
	allowRetirementWait := make(chan struct{})
	mgr.beforeRetirementWaitHook = func(count int) {
		retirementSnapshot <- count
		<-allowRetirementWait
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- mgr.ShutdownContext(context.Background()) }()
	select {
	case <-p.rangeEntered:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not begin the pool retirement snapshot")
	}

	// This is the production callback order: the pool deletion has already
	// removed the entry, and its onNodeRemoved callback arrives while the
	// outbound shutdown owner is between its snapshot and wait.
	p.removeEntry(entry.Hash)
	lateRemovalDone := make(chan struct{})
	go func() {
		mgr.RemoveNodeOutbound(entry)
		close(lateRemovalDone)
	}()
	select {
	case <-retirementCallEntered:
	case <-time.After(time.Second):
		t.Fatal("late node-removal callback did not enter retirement admission")
	}

	close(p.allowRange)
	select {
	case count := <-retirementSnapshot:
		if count != 1 {
			t.Fatalf("final retirement snapshot = %d, want late callback included", count)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not reach the retirement wait boundary")
	}
	select {
	case <-lateRemovalDone:
	case <-time.After(time.Second):
		t.Fatal("late node-removal callback did not register retirement")
	}
	select {
	case <-ob.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("late retirement did not start outbound Close")
	}

	close(allowRetirementWait)
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before the late retirement Close completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(ob.allowClose)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after the late retirement Close completed")
	}
}

func TestRemoveNodeOutbound(t *testing.T) {
	entry := newTestEntry(`{"type":"rm"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)
	if !entry.HasOutbound() {
		t.Fatal("setup: expected HasOutbound() == true")
	}

	mgr.RemoveNodeOutbound(entry)
	if entry.HasOutbound() {
		t.Fatal("expected HasOutbound() == false after RemoveNodeOutbound")
	}
}

func TestRemoveNodeOutbound_NilEntry(t *testing.T) {
	pool := &mockPool{}
	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	// Should not panic.
	mgr.RemoveNodeOutbound(nil)
}

func TestFetch_OutboundNotReady(t *testing.T) {
	entry := newTestEntry(`{"type":"notready"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	// Don't call EnsureNodeOutbound — outbound remains nil.

	ctx := context.Background()
	_, _, err := mgr.Fetch(ctx, entry.Hash, "http://example.com")
	if !errors.Is(err, ErrOutboundNotReady) {
		t.Fatalf("expected ErrOutboundNotReady, got: %v", err)
	}
}

func TestFetchWithExpectedEntry_RejectsCircuitOpenEntry(t *testing.T) {
	entry := newTestEntry(`{"type":"circuit-open"}`)
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.CircuitOpenSince.Store(time.Now().UnixNano())

	pool := &mockPool{}
	pool.addEntry(entry)
	mgr := NewOutboundManager(pool, nil)

	_, _, err := mgr.FetchWithExpectedEntry(
		context.Background(),
		entry.Hash,
		entry,
		"http://example.com",
		"",
	)
	if !errors.Is(err, ErrOutboundNotReady) {
		t.Fatalf("expected ErrOutboundNotReady for circuit-open entry, got %v", err)
	}
}

func TestFetchWithExpectedEntry_RejectsAfterShutdownAdmissionClosesBeforeRetirement(t *testing.T) {
	entry := newTestEntry(`{"type":"fetch-after-shutdown"}`)
	pool := &mockPool{}
	pool.addEntry(entry)
	tracking := &fetchTrackingOutbound{dialEntered: make(chan struct{})}
	var raw adapter.Outbound = tracking
	entry.Outbound.Store(&raw)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	retirementStarted := make(chan struct{})
	allowRetirement := make(chan struct{})
	var allowRetirementOnce sync.Once
	mgr.beforeRetirementLifecycleHook = func() {
		close(retirementStarted)
		<-allowRetirement
	}
	t.Cleanup(func() { allowRetirementOnce.Do(func() { close(allowRetirement) }) })

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- mgr.ShutdownContext(context.Background()) }()
	select {
	case <-retirementStarted:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not close admission before retirement")
	}

	_, _, err := mgr.FetchWithExpectedEntry(
		context.Background(),
		entry.Hash,
		entry,
		"http://example.test/",
		"",
	)
	if !errors.Is(err, ErrOutboundNotReady) {
		t.Fatalf("fetch after shutdown admission close = %v, want ErrOutboundNotReady", err)
	}
	select {
	case <-tracking.dialEntered:
		t.Fatal("fetch after shutdown admission close reached the outbound")
	default:
	}

	allowRetirementOnce.Do(func() { close(allowRetirement) })
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not finish after retirement was released")
	}
}

func TestFetchAdmittedBeforeShutdownKeepsLeaseUntilDialReturns(t *testing.T) {
	entry := newTestEntry(`{"type":"fetch-before-shutdown"}`)
	pool := &mockPool{}
	pool.addEntry(entry)
	allowDial := make(chan struct{})
	tracking := &fetchTrackingOutbound{
		dialEntered: make(chan struct{}),
		allowDial:   allowDial,
	}
	var raw adapter.Outbound = tracking
	entry.Outbound.Store(&raw)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	fetchDone := make(chan error, 1)
	go func() {
		_, _, err := mgr.FetchWithExpectedEntry(
			context.Background(),
			entry.Hash,
			entry,
			"http://example.test/",
			"",
		)
		fetchDone <- err
	}()
	select {
	case <-tracking.dialEntered:
	case <-time.After(time.Second):
		t.Fatal("fetch did not acquire the outbound lease")
	}

	retirementWaiting := make(chan struct{})
	var retirementWaitingOnce sync.Once
	mgr.beforeRetirementWaitHook = func(count int) {
		if count != 1 {
			t.Errorf("retirement count = %d, want 1", count)
		}
		retirementWaitingOnce.Do(func() { close(retirementWaiting) })
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- mgr.ShutdownContext(context.Background()) }()
	select {
	case <-retirementWaiting:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not reach the admitted lease retirement")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before admitted fetch completed: %v", err)
	default:
	}

	close(allowDial)
	select {
	case err := <-fetchDone:
		if err == nil {
			t.Fatal("expected the gated test dial to return an error")
		}
		if errors.Is(err, ErrOutboundNotReady) {
			t.Fatalf("admitted fetch was rejected as not ready: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted fetch did not finish after DialContext was released")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not finish after the admitted fetch released its lease")
	}
}

func TestFetch_NodeNotFound(t *testing.T) {
	pool := &mockPool{}
	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})

	ctx := context.Background()
	_, _, err := mgr.Fetch(ctx, makeHash("nonexistent"), "http://example.com")
	if err == nil {
		t.Fatal("expected error for non-existent node")
	}
}

func TestWarmupAll(t *testing.T) {
	pool := &mockPool{}
	entries := make([]*node.NodeEntry, 5)
	for i := range entries {
		entries[i] = node.NewNodeEntry(
			makeHash("warmup"+string(rune('0'+i))),
			json.RawMessage(`{}`),
			time.Now(), 0,
		)
		pool.addEntry(entries[i])
	}

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	mgr.WarmupAll()

	var count atomic.Int32
	pool.RangeNodes(func(_ node.Hash, e *node.NodeEntry) bool {
		if e.HasOutbound() {
			count.Add(1)
		}
		return true
	})
	if int(count.Load()) != len(entries) {
		t.Fatalf("expected all %d entries to have outbound, got %d", len(entries), count.Load())
	}
}

func TestFetch_HTTPStatusNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	entry := newTestEntry(`{"type":"status"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)
	_, _, err := mgr.Fetch(context.Background(), entry.Hash, srv.URL)
	if err == nil {
		t.Fatal("expected non-200 status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 404") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestFetch_HTTPSCertValidationEnabled(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	entry := newTestEntry(`{"type":"https-cert"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)

	_, _, err := mgr.Fetch(context.Background(), entry.Hash, srv.URL)
	if err == nil {
		t.Fatal("expected TLS verification error for self-signed cert")
	}
	lower := strings.ToLower(err.Error())
	if !strings.Contains(lower, "x509") && !strings.Contains(lower, "certificate") {
		t.Fatalf("expected certificate verification error, got: %v", err)
	}
}

func TestFetchWithUserAgent_UsesCustomHeader(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	entry := newTestEntry(`{"type":"ua-custom"}`)
	pool := &mockPool{}
	pool.addEntry(entry)

	mgr := NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	mgr.EnsureNodeOutbound(entry.Hash)

	const customUA = "Resin-Test-UA/42"
	_, _, err := mgr.FetchWithUserAgent(context.Background(), entry.Hash, srv.URL, customUA)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if gotUA != customUA {
		t.Fatalf("unexpected user-agent: got %q want %q", gotUA, customUA)
	}
}
