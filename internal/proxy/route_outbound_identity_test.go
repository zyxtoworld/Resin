package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// replacementRoutePool exposes the real boundary under test: Router and the
// proxy resolver share GetEntry, but a same-hash replacement can land between
// Router's final identity check and the resolver's lookup.
type replacementRoutePool struct {
	mu           sync.RWMutex
	platform     *platform.Platform
	oldEntry     *node.NodeEntry
	newEntry     *node.NodeEntry
	getCalls     atomic.Int32
	thirdCall    sync.Once
	thirdReached chan struct{}
	allowThird   chan struct{}
}

func (p *replacementRoutePool) GetEntry(_ node.Hash) (*node.NodeEntry, bool) {
	call := p.getCalls.Add(1)
	if call == 3 {
		p.thirdCall.Do(func() { close(p.thirdReached) })
		<-p.allowThird
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.newEntry, true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.oldEntry, true
}

func (p *replacementRoutePool) IsNodeDisabled(node.Hash) bool { return false }

func (p *replacementRoutePool) RangeNodes(func(node.Hash, *node.NodeEntry) bool) {}

func (p *replacementRoutePool) GetPlatform(id string) (*platform.Platform, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.platform == nil || p.platform.ID != id {
		return nil, false
	}
	return p.platform, true
}

func (p *replacementRoutePool) GetPlatformByName(name string) (*platform.Platform, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.platform == nil || p.platform.Name != name {
		return nil, false
	}
	return p.platform, true
}

func (p *replacementRoutePool) RangePlatforms(func(*platform.Platform) bool) {}

var _ routing.PoolAccessor = (*replacementRoutePool)(nil)
var _ outbound.PoolAccessor = (*replacementRoutePool)(nil)

func newProxyHealthyEntry(t *testing.T, hash node.Hash, ip string) *node.NodeEntry {
	t.Helper()
	entry := node.NewNodeEntry(hash, json.RawMessage(`{"id":"proxy-same-hash"}`), time.Now(), 4)
	entry.AddSubscriptionID("sub-test")
	entry.SetEgressIP(netip.MustParseAddr(ip))
	entry.LatencyTable.Update("cloudflare.com", 10*time.Millisecond, time.Minute)
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	return entry
}

func TestResolveRoutedOutboundRejectsSameHashReplacementAfterRoute(t *testing.T) {
	payload := json.RawMessage(`{"id":"proxy-same-hash"}`)
	hash := node.HashFromRawOptions(payload)
	oldEntry := newProxyHealthyEntry(t, hash, "203.0.113.231")
	newEntry := newProxyHealthyEntry(t, hash, "203.0.113.232")
	plat := platform.NewPlatform("proxy-stale-identity", "Proxy-Stale-Identity", nil, nil)
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) { fn(hash, oldEntry) },
		func(_ string, _ node.Hash) (string, bool, []string, bool) {
			return "sub-test", true, nil, true
		},
		func(_ netip.Addr) string { return "" },
	)

	pool := &replacementRoutePool{
		platform:     plat,
		oldEntry:     oldEntry,
		newEntry:     newEntry,
		thirdReached: make(chan struct{}),
		allowThird:   make(chan struct{}),
	}
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})

	type result struct {
		routed routedOutbound
		err    *ProxyError
	}
	resultCh := make(chan result, 1)
	go func() {
		routed, err := resolveRoutedOutbound(router, pool, plat.Name, "", "cloudflare.com:443")
		resultCh <- result{routed: routed, err: err}
	}()

	select {
	case <-pool.thirdReached:
	case <-time.After(time.Second):
		t.Fatal("resolver did not reach the post-route pool lookup")
	}
	close(pool.allowThird)

	select {
	case got := <-resultCh:
		if got.err != ErrNoAvailableNodes {
			t.Fatalf("same-hash replacement escaped the platform identity: routed=%#v err=%v", got.routed, got.err)
		}
		if got.routed.Entry != nil || got.routed.Outbound != nil {
			t.Fatal("failed identity binding must not expose either retired or replacement outbound")
		}
	case <-time.After(time.Second):
		t.Fatal("resolveRoutedOutbound did not return")
	}
}

type staticRoutePool struct {
	entry    *node.NodeEntry
	platform *platform.Platform
}

func (p *staticRoutePool) GetEntry(node.Hash) (*node.NodeEntry, bool) {
	return p.entry, p.entry != nil
}

func (p *staticRoutePool) IsNodeDisabled(node.Hash) bool { return false }

func (p *staticRoutePool) GetPlatform(id string) (*platform.Platform, bool) {
	if p.platform == nil || p.platform.ID != id {
		return nil, false
	}
	return p.platform, true
}

func (p *staticRoutePool) GetPlatformByName(name string) (*platform.Platform, bool) {
	if p.platform == nil || p.platform.Name != name {
		return nil, false
	}
	return p.platform, true
}

func (p *staticRoutePool) RangePlatforms(func(*platform.Platform) bool) {}

func (p *staticRoutePool) RangeNodes(func(node.Hash, *node.NodeEntry) bool) {}

var _ routing.PoolAccessor = (*staticRoutePool)(nil)
var _ outbound.PoolAccessor = (*staticRoutePool)(nil)

type gatedBindingPool struct {
	*staticRoutePool
	getCalls        atomic.Int32
	bindingEntered  chan struct{}
	allowBinding    chan struct{}
	bindingCallOnce sync.Once
}

func (p *gatedBindingPool) GetEntry(node.Hash) (*node.NodeEntry, bool) {
	if p.getCalls.Add(1) == 3 {
		p.bindingCallOnce.Do(func() { close(p.bindingEntered) })
		<-p.allowBinding
	}
	return p.staticRoutePool.GetEntry(node.Zero)
}

var _ routing.PoolAccessor = (*gatedBindingPool)(nil)

func TestResolveRoutedOutboundRejectsHealthChangeBeforeBinding(t *testing.T) {
	payload := json.RawMessage(`{"id":"proxy-health-binding"}`)
	hash := node.HashFromRawOptions(payload)
	entry := newProxyHealthyEntry(t, hash, "203.0.113.242")
	plat := platform.NewPlatform("plat-health-binding", "Plat-Health-Binding", nil, nil)
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) { fn(hash, entry) },
		func(_ string, _ node.Hash) (string, bool, []string, bool) {
			return "sub-test", true, nil, true
		},
		func(_ netip.Addr) string { return "" },
	)
	pool := &gatedBindingPool{
		staticRoutePool: &staticRoutePool{entry: entry, platform: plat},
		bindingEntered:  make(chan struct{}),
		allowBinding:    make(chan struct{}),
	}
	defer func() {
		select {
		case <-pool.allowBinding:
		default:
			close(pool.allowBinding)
		}
	}()
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})

	resultCh := make(chan *ProxyError, 1)
	go func() {
		_, err := resolveRoutedOutbound(router, pool, plat.Name, "", "cloudflare.com:443")
		resultCh <- err
	}()
	select {
	case <-pool.bindingEntered:
	case <-time.After(time.Second):
		t.Fatal("resolver did not reach the binding lookup")
	}
	entry.CircuitOpenSince.Store(time.Now().UnixNano())
	close(pool.allowBinding)

	select {
	case err := <-resultCh:
		if err != ErrNoAvailableNodes {
			t.Fatalf("health change escaped binding gate: got %v, want %v", err, ErrNoAvailableNodes)
		}
	case <-time.After(time.Second):
		t.Fatal("resolver did not finish after releasing binding lookup")
	}
}

var errOutboundClosedDuringDial = errors.New("outbound closed during dial")

type closeAwareOutbound struct {
	closed       atomic.Bool
	closeCount   atomic.Int32
	dialEntered  chan struct{}
	allowDial    chan struct{}
	closeEntered chan struct{}
	closeOnce    sync.Once
}

func (o *closeAwareOutbound) Type() string           { return "close-aware" }
func (o *closeAwareOutbound) Tag() string            { return "close-aware" }
func (o *closeAwareOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *closeAwareOutbound) Dependencies() []string { return nil }
func (o *closeAwareOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("listen packet unsupported")
}
func (o *closeAwareOutbound) Close() error {
	o.closeCount.Add(1)
	o.closed.Store(true)
	o.closeOnce.Do(func() { close(o.closeEntered) })
	return nil
}
func (o *closeAwareOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	close(o.dialEntered)
	select {
	case <-o.allowDial:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if o.closed.Load() {
		return nil, errOutboundClosedDuringDial
	}
	conn, peer := net.Pipe()
	_ = peer.Close()
	return conn, nil
}

var _ adapter.Outbound = (*closeAwareOutbound)(nil)

func TestResolvedOutboundUseIsNotClosedDuringDial(t *testing.T) {
	payload := json.RawMessage(`{"id":"dial-close-race"}`)
	hash := node.HashFromRawOptions(payload)
	entry := newProxyHealthyEntry(t, hash, "203.0.113.241")
	ob := &closeAwareOutbound{
		dialEntered:  make(chan struct{}),
		allowDial:    make(chan struct{}),
		closeEntered: make(chan struct{}),
	}
	var adapterOutbound adapter.Outbound = ob
	entry.Outbound.Store(&adapterOutbound)
	plat := platform.NewPlatform("plat-dial-close", "Plat-Dial-Close", nil, nil)
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) { fn(hash, entry) },
		func(_ string, _ node.Hash) (string, bool, []string, bool) {
			return "sub-test", true, nil, true
		},
		func(_ netip.Addr) string { return "" },
	)
	pool := &staticRoutePool{entry: entry, platform: plat}
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})
	routed, proxyErr := resolveRoutedOutbound(router, pool, plat.Name, "", "cloudflare.com:443")
	if proxyErr != nil {
		t.Fatalf("resolve route: %v", proxyErr)
	}

	dialDone := make(chan error, 1)
	go func() {
		conn, err := routed.Outbound.DialContext(context.Background(), "tcp", M.ParseSocksaddr("example.com:443"))
		if conn != nil {
			_ = conn.Close()
		}
		dialDone <- err
	}()
	select {
	case <-ob.dialEntered:
	case <-time.After(time.Second):
		t.Fatal("DialContext did not start")
	}

	manager := outbound.NewOutboundManager(pool, nil)
	removeDone := make(chan struct{})
	go func() {
		manager.RemoveNodeOutbound(entry)
		close(removeDone)
	}()
	select {
	case <-ob.closeEntered:
		t.Fatal("RemoveNodeOutbound closed an outbound while DialContext was using it")
	case <-removeDone:
	}
	if entry.Outbound.Load() != nil {
		t.Fatal("retired entry still published an outbound")
	}
	if _, release, ok := entry.AcquireOutbound(); ok {
		release()
		t.Fatal("retired entry accepted a new outbound lease")
	}
	close(ob.allowDial)

	select {
	case err := <-dialDone:
		if err != nil {
			t.Fatalf("DialContext observed a closed outbound: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DialContext did not finish")
	}
	select {
	case <-ob.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("last outbound use did not close the retired outbound")
	}
	if got := ob.closeCount.Load(); got != 1 {
		t.Fatalf("retired outbound close count = %d, want 1", got)
	}
}
