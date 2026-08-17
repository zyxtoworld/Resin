package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type noopOutbound struct {
	adapter.Outbound
}

func (n *noopOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("not used in transport-pool tests")
}

func (n *noopOutbound) Tag() string  { return "noop" }
func (n *noopOutbound) Type() string { return "noop" }

type gatedRoundTripOutbound struct {
	closed       atomic.Bool
	closeCount   atomic.Int32
	dialEntered  chan struct{}
	allowDial    chan struct{}
	closeEntered chan struct{}
	dialOnce     sync.Once
	closeOnce    sync.Once
}

func (o *gatedRoundTripOutbound) Type() string           { return "gated-roundtrip" }
func (o *gatedRoundTripOutbound) Tag() string            { return "gated-roundtrip" }
func (o *gatedRoundTripOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *gatedRoundTripOutbound) Dependencies() []string { return nil }
func (o *gatedRoundTripOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("listen packet unsupported")
}
func (o *gatedRoundTripOutbound) Close() error {
	o.closeCount.Add(1)
	o.closed.Store(true)
	o.closeOnce.Do(func() { close(o.closeEntered) })
	return nil
}
func (o *gatedRoundTripOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	o.dialOnce.Do(func() { close(o.dialEntered) })
	select {
	case <-o.allowDial:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if o.closed.Load() {
		return nil, errors.New("dial used closed outbound")
	}
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		reader := bufio.NewReader(server)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" {
				_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
				return
			}
		}
	}()
	return client, nil
}

var _ adapter.Outbound = (*gatedRoundTripOutbound)(nil)

func TestOutboundTransportPool_ReusesByNodeHash(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{1}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)

	t1 := pool.Get(hash, entry, &noopOutbound{}, nil)
	t2 := pool.Get(hash, entry, &noopOutbound{}, nil)

	if t1 != t2 {
		t.Fatal("expected same transport instance for identical node hash")
	}
}

func TestOutboundTransportPool_ReplacesSameHashEntry(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{3}
	oldEntry := node.NewNodeEntry(hash, nil, time.Now(), 0)
	newEntry := node.NewNodeEntry(hash, nil, time.Now(), 0)

	oldTransport := pool.Get(hash, oldEntry, &noopOutbound{}, nil)
	newTransport := pool.Get(hash, newEntry, &noopOutbound{}, nil)

	if oldTransport == newTransport {
		t.Fatal("same-hash replacement reused the retired entry transport")
	}
}

func TestOutboundTransportPool_SplitsByNodeHash(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash1 := node.Hash{1}
	hash2 := node.Hash{2}
	entry1 := node.NewNodeEntry(hash1, nil, time.Now(), 0)
	entry2 := node.NewNodeEntry(hash2, nil, time.Now(), 0)

	base := pool.Get(hash1, entry1, ob, nil)
	byNodeHash := pool.Get(hash2, entry2, ob, nil)
	if base == byNodeHash {
		t.Fatal("expected different transport for different node hash")
	}
}

func TestOutboundTransportPool_UsesKeepAliveTransport(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash := node.Hash{1}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)

	transport := pool.Get(hash, entry, ob, nil)
	if transport.DisableKeepAlives {
		t.Fatal("expected keep-alive enabled transport")
	}
}

func TestOutboundTransportPool_EvictRemovesNodeTransport(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{1}
	ob := &noopOutbound{}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)

	t1 := pool.Get(hash, entry, ob, nil)
	pool.Evict(hash)
	t2 := pool.Get(hash, entry, ob, nil)

	if t1 == t2 {
		t.Fatal("expected a new transport after evict")
	}
}

func TestOutboundTransportPool_EvictRejectsLateRetiredEntryGet(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{13}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)
	raw := testutil.NewNoopOutbound()
	if !entry.InstallOutboundIfAbsent(raw) {
		t.Fatal("failed to install test outbound")
	}
	leased, ok := entry.NewLeasedOutbound()
	if !ok {
		t.Fatal("failed to create leased outbound")
	}
	pool.Get(hash, entry, leased, nil)

	// This is the production removal order: the exact entry is retired before
	// the topology callback evicts its reusable transport.
	entry.RetireOutbound()

	getEntered := make(chan struct{})
	releaseGet := make(chan struct{})
	pool.beforeTransportGetHook = func() {
		close(getEntered)
		<-releaseGet
	}
	defer func() {
		select {
		case <-releaseGet:
		default:
			close(releaseGet)
		}
		pool.beforeTransportGetHook = nil
	}()

	getDone := make(chan *http.Transport, 1)
	go func() {
		getDone <- pool.Get(hash, entry, leased, nil)
	}()
	select {
	case <-getEntered:
	case <-time.After(time.Second):
		t.Fatal("late transport Get did not enter")
	}

	pool.Evict(hash)
	if _, ok := pool.transports.Load(hash); ok {
		t.Fatal("Evict did not remove the current transport before late Get")
	}

	close(releaseGet)
	select {
	case <-getDone:
	case <-time.After(time.Second):
		t.Fatal("late transport Get did not finish")
	}
	if _, ok := pool.transports.Load(hash); ok {
		t.Fatal("late Get reinserted a transport for a retired entry")
	}
}

func TestOutboundTransportPool_AppliesConfiguredLimits(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		MaxIdleConns:        9,
		MaxIdleConnsPerHost: 3,
		IdleConnTimeout:     12 * time.Second,
	})
	ob := &noopOutbound{}
	hash := node.Hash{1}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)

	transport := pool.Get(hash, entry, ob, nil)
	if transport.MaxIdleConns != 9 {
		t.Fatalf("MaxIdleConns: got %d, want %d", transport.MaxIdleConns, 9)
	}
	if transport.MaxIdleConnsPerHost != 3 {
		t.Fatalf("MaxIdleConnsPerHost: got %d, want %d", transport.MaxIdleConnsPerHost, 3)
	}
	if transport.IdleConnTimeout != 12*time.Second {
		t.Fatalf("IdleConnTimeout: got %s, want %s", transport.IdleConnTimeout, 12*time.Second)
	}
}

func TestOutboundTransportPool_CloseAllClearsEntries(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}

	hashA := node.Hash{1}
	hashB := node.Hash{2}
	entryA := node.NewNodeEntry(hashA, nil, time.Now(), 0)
	entryB := node.NewNodeEntry(hashB, nil, time.Now(), 0)
	t1 := pool.Get(hashA, entryA, ob, nil)
	_ = pool.Get(hashB, entryB, ob, nil)

	pool.CloseAll()

	t2 := pool.Get(hashA, entryA, ob, nil)
	if t1 == t2 {
		t.Fatal("expected a new transport after CloseAll")
	}
}

func TestOutboundTransportPool_CloseAllLinearizesWithGet(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash := node.Hash{11}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)
	pool.Get(hash, entry, ob, nil)

	closeAllEntered := make(chan struct{})
	allowCloseAll := make(chan struct{})
	pool.beforeCloseAllClearHook = func() {
		close(closeAllEntered)
		<-allowCloseAll
	}
	closeAllDone := make(chan struct{})
	go func() {
		pool.CloseAll()
		close(closeAllDone)
	}()
	select {
	case <-closeAllEntered:
	case <-time.After(time.Second):
		t.Fatal("CloseAll did not reach its clear boundary")
	}

	getEntered := make(chan struct{})
	allowGet := make(chan struct{})
	pool.beforeTransportGetHook = func() {
		close(getEntered)
		<-allowGet
	}
	getDone := make(chan *http.Transport, 1)
	go func() {
		getDone <- pool.Get(hash, entry, ob, nil)
	}()
	select {
	case <-getEntered:
	case <-time.After(time.Second):
		t.Fatal("concurrent Get did not enter")
	}
	close(allowGet)
	var got *http.Transport
	if pool.transportMu.TryLock() {
		// Old code does not hold transportMu across CloseAll's snapshot and
		// clear. Let its concurrent Get finish before releasing the clear gate;
		// the old Clear then deterministically drops that newly inserted entry.
		pool.transportMu.Unlock()
		select {
		case got = <-getDone:
		case <-time.After(time.Second):
			t.Fatal("old concurrent Get did not finish while CloseAll was gated")
		}
	}
	close(allowCloseAll)

	select {
	case <-closeAllDone:
	case <-time.After(time.Second):
		t.Fatal("CloseAll did not finish")
	}
	if got == nil {
		select {
		case got = <-getDone:
		case <-time.After(time.Second):
			t.Fatal("concurrent Get did not finish")
		}
	}
	pool.beforeTransportGetHook = nil
	pool.beforeCloseAllClearHook = nil

	current, ok := pool.transports.Load(hash)
	if !ok || current == nil || current.transport != got {
		t.Fatal("CloseAll lost a transport inserted by a concurrent Get")
	}
}

func TestOutboundTransportPool_ShutdownRejectsLateForwardTransportGet(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{12}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)
	ob := &noopOutbound{}
	pool.Get(hash, entry, ob, nil)

	pool.Shutdown()

	forward := &ForwardProxy{transportPool: pool}
	late := forward.outboundHTTPTransport(routedOutbound{
		Route:    routing.RouteResult{NodeHash: hash},
		Entry:    entry,
		Outbound: ob,
	})
	if late == nil {
		t.Fatal("late transport acquisition returned nil")
	}
	if got := pool.transports.Size(); got != 0 {
		t.Fatalf("late forward handler recreated %d pooled transports after shutdown", got)
	}
}

func TestOutboundTransportRoundTripRetainsLeaseUntilConnectionClose(t *testing.T) {
	hash := node.Hash{7}
	entry := node.NewNodeEntry(hash, nil, time.Now(), 0)
	ob := &gatedRoundTripOutbound{
		dialEntered:  make(chan struct{}),
		allowDial:    make(chan struct{}),
		closeEntered: make(chan struct{}),
	}
	var raw adapter.Outbound = ob
	entry.Outbound.Store(&raw)
	leased, ok := entry.NewLeasedOutbound()
	if !ok {
		t.Fatal("expected a leased outbound")
	}

	transportPool := newOutboundTransportPool()
	transport := transportPool.Get(hash, entry, leased, nil)
	request, err := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	responseDone := make(chan error, 1)
	go func() {
		response, err := transport.RoundTrip(request)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil {
				err = readErr
			} else if closeErr != nil {
				err = closeErr
			} else if string(body) != "ok" {
				err = errors.New("unexpected response body")
			}
		}
		responseDone <- err
	}()

	select {
	case <-ob.dialEntered:
	case <-time.After(time.Second):
		t.Fatal("RoundTrip did not enter outbound DialContext")
	}

	manager := outbound.NewOutboundManager(nil, nil)
	removeDone := make(chan struct{})
	go func() {
		manager.RemoveNodeOutbound(entry)
		close(removeDone)
	}()
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("RemoveNodeOutbound blocked on an active RoundTrip")
	}
	select {
	case <-ob.closeEntered:
		t.Fatal("retiring outbound closed before the active RoundTrip released it")
	default:
	}

	close(ob.allowDial)
	select {
	case err := <-responseDone:
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RoundTrip did not finish")
	}
	select {
	case <-ob.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("last RoundTrip release did not close the retired outbound")
	}
	if got := ob.closeCount.Load(); got != 1 {
		t.Fatalf("retired outbound close count = %d, want 1", got)
	}
}
