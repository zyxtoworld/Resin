package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
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
	beforeBinding   func()
}

func (p *gatedBindingPool) GetEntry(node.Hash) (*node.NodeEntry, bool) {
	if p.getCalls.Add(1) == 3 {
		p.bindingCallOnce.Do(func() { close(p.bindingEntered) })
		if p.beforeBinding != nil {
			p.beforeBinding()
		}
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

func TestForwardProxyRejectsEgressChangeBeforeBinding(t *testing.T) {
	payload := json.RawMessage(`{"id":"proxy-egress-binding"}`)
	hash := node.HashFromRawOptions(payload)
	entry := newProxyHealthyEntry(t, hash, "203.0.113.243")
	plat := platform.NewPlatform("plat-egress-binding", "Plat-Egress-Binding", nil, nil)
	plat.FullRebuild(
		func(fn func(node.Hash, *node.NodeEntry) bool) { fn(hash, entry) },
		func(_ string, _ node.Hash) (string, bool, []string, bool) {
			return "sub-test", true, nil, true
		},
		func(_ netip.Addr) string { return "" },
	)
	rules, err := platform.CompileResponseRules("plat-egress-binding", []model.PlatformResponseRule{{
		ID: "cooldown", Enabled: true,
		Match: model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusTooManyRequests}},
		Action: model.PlatformResponseRuleAction{
			Type:          "cooldown",
			CooldownScope: "egress_ip",
			Fallback:      "next_utc_midnight",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	plat.ResponseRules = rules

	var upstreamRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, "quota-limited")
	}))
	defer upstream.Close()
	var wrapped adapter.Outbound = &mockOutbound{dialFunc: func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	}}
	entry.Outbound.Store(&wrapped)

	pool := &gatedBindingPool{
		staticRoutePool: &staticRoutePool{entry: entry, platform: plat},
		bindingEntered:  make(chan struct{}),
		allowBinding:    make(chan struct{}),
		beforeBinding: func() {
			entry.SetEgressIP(netip.MustParseAddr("203.0.113.244"))
		},
	}
	router := routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return nil },
		P2CWindow:   func() time.Duration { return time.Minute },
	})
	proxy := NewForwardProxy(ForwardProxyConfig{
		ProxyToken: "tok",
		Router:     router,
		Pool:       pool,
	})

	requestDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		req.Header.Set("Proxy-Authorization", basicAuth(plat.Name, "tok"))
		writer := httptest.NewRecorder()
		proxy.ServeHTTP(writer, req)
		requestDone <- writer
	}()
	select {
	case <-pool.bindingEntered:
	case <-time.After(time.Second):
		select {
		case writer := <-requestDone:
			t.Fatalf("forward proxy returned before binding lookup: status=%d body=%q; GetEntry calls=%d", writer.Code, writer.Body.String(), pool.getCalls.Load())
		default:
			t.Fatalf("forward proxy did not reach the binding lookup; GetEntry calls=%d", pool.getCalls.Load())
		}
	}
	close(pool.allowBinding)

	select {
	case writer := <-requestDone:
		if writer.Code != http.StatusServiceUnavailable {
			t.Fatalf("stale egress binding status: got %d body=%q, want %d", writer.Code, writer.Body.String(), http.StatusServiceUnavailable)
		}
	case <-time.After(time.Second):
		t.Fatal("forward proxy did not finish after releasing binding lookup")
	}
	if got := upstreamRequests.Load(); got != 0 {
		t.Fatalf("stale egress binding reached upstream %d time(s)", got)
	}
	cooldowns, ok := router.SnapshotResponseCooldownsForPlatform(plat.ID, time.Now())
	if !ok {
		t.Fatal("platform cooldown snapshot unavailable")
	}
	if len(cooldowns) != 0 {
		t.Fatalf("stale egress response created cooldowns: %#v", cooldowns)
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
