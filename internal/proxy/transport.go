package proxy

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type OutboundTransportConfig struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
}

const (
	defaultTransportMaxIdleConns        = 1024
	defaultTransportMaxIdleConnsPerHost = 64
	defaultTransportIdleConnTimeout     = 90 * time.Second
)

func normalizeOutboundTransportConfig(cfg OutboundTransportConfig) OutboundTransportConfig {
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = defaultTransportMaxIdleConns
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = defaultTransportMaxIdleConnsPerHost
	}
	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = defaultTransportIdleConnTimeout
	}
	return cfg
}

// OutboundTransportPool manages reusable outbound HTTP transports keyed by node hash.
// A single instance should be shared by forward/reverse proxies so keep-alive pools
// are reused and can be evicted on node removal.
type OutboundTransportPool struct {
	config     OutboundTransportConfig
	transports *xsync.Map[node.Hash, *pooledOutboundTransport]
	// transportMu linearizes map lifecycle operations with CloseAll. The
	// underlying xsync map still provides per-key operations, but a close
	// snapshot must not race a late Get that would otherwise be cleared without
	// ever being retired.
	transportMu sync.RWMutex

	// Package-private seams for deterministic lifecycle tests.
	beforeTransportGetHook  func()
	beforeCloseAllClearHook func()
	closed                  bool
}

type pooledOutboundTransport struct {
	entry     *node.NodeEntry
	transport *http.Transport
}

func newOutboundTransportPool() *OutboundTransportPool {
	return NewOutboundTransportPool(OutboundTransportConfig{})
}

func newOutboundTransportPoolWithConfig(cfg OutboundTransportConfig) *OutboundTransportPool {
	return NewOutboundTransportPool(cfg)
}

// NewOutboundTransportPool creates a transport pool with normalized settings.
func NewOutboundTransportPool(cfg OutboundTransportConfig) *OutboundTransportPool {
	return &OutboundTransportPool{
		config:     normalizeOutboundTransportConfig(cfg),
		transports: xsync.NewMap[node.Hash, *pooledOutboundTransport](),
	}
}

// Get returns a reusable transport for the given exact node entry. A same-hash
// replacement gets a new transport so no cached DialContext can retain the
// retired entry's outbound wrapper.
func (p *OutboundTransportPool) Get(
	hash node.Hash,
	entry *node.NodeEntry,
	ob adapter.Outbound,
	sink MetricsEventSink,
) *http.Transport {
	if hook := p.beforeTransportGetHook; hook != nil {
		hook()
	}
	var selected *pooledOutboundTransport
	var retired *pooledOutboundTransport
	p.transportMu.RLock()
	if p.closed {
		p.transportMu.RUnlock()
		return closedOutboundTransport()
	}
	p.transports.Compute(hash, func(current *pooledOutboundTransport, loaded bool) (*pooledOutboundTransport, xsync.ComputeOp) {
		if loaded && current != nil && current.entry == entry {
			selected = current
			return current, xsync.UpdateOp
		}
		selected = &pooledOutboundTransport{
			entry:     entry,
			transport: p.newReusableOutboundTransport(ob, sink),
		}
		if loaded {
			retired = current
		}
		return selected, xsync.UpdateOp
	})
	p.transportMu.RUnlock()
	if retired != nil && retired.transport != nil {
		retired.transport.CloseIdleConnections()
	}
	if selected == nil || entry == nil || entry.IsOutboundRetiring() {
		p.removeTransportIfCurrent(hash, selected)
		return closedOutboundTransport()
	}
	return selected.transport
}

func (p *OutboundTransportPool) removeTransportIfCurrent(hash node.Hash, selected *pooledOutboundTransport) {
	if selected == nil {
		return
	}

	var removed *pooledOutboundTransport
	p.transportMu.RLock()
	p.transports.Compute(hash, func(current *pooledOutboundTransport, loaded bool) (*pooledOutboundTransport, xsync.ComputeOp) {
		if loaded && current == selected {
			removed = current
			return nil, xsync.DeleteOp
		}
		return current, xsync.CancelOp
	})
	p.transportMu.RUnlock()
	if removed != nil && removed.transport != nil {
		removed.transport.CloseIdleConnections()
	}
}

// Evict closes idle connections for one node transport and removes it from pool.
func (p *OutboundTransportPool) Evict(hash node.Hash) {
	p.transportMu.RLock()
	pooled, ok := p.transports.LoadAndDelete(hash)
	p.transportMu.RUnlock()
	if !ok || pooled == nil || pooled.transport == nil {
		return
	}
	pooled.transport.CloseIdleConnections()
}

// CloseAll closes idle connections and clears all pooled transports.
func (p *OutboundTransportPool) CloseAll() {
	p.transportMu.Lock()
	transports := p.clearLocked()
	p.transportMu.Unlock()
	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

// Shutdown closes the pool for new reusable transport acquisition and clears
// all currently pooled transports. The application uses this at shutdown;
// CloseAll remains a reusable clear operation for ordinary lifecycle changes.
func (p *OutboundTransportPool) Shutdown() {
	p.transportMu.Lock()
	p.closed = true
	transports := p.clearLocked()
	p.transportMu.Unlock()
	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

func (p *OutboundTransportPool) clearLocked() []*http.Transport {
	transports := make([]*http.Transport, 0, p.transports.Size())
	p.transports.Range(func(_ node.Hash, pooled *pooledOutboundTransport) bool {
		if pooled != nil && pooled.transport != nil {
			transports = append(transports, pooled.transport)
		}
		return true
	})
	if hook := p.beforeCloseAllClearHook; hook != nil {
		hook()
	}
	p.transports.Clear()
	return transports
}

func closedOutboundTransport() *http.Transport {
	return &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, net.ErrClosed
		},
	}
}

func (p *OutboundTransportPool) newReusableOutboundTransport(ob adapter.Outbound, sink MetricsEventSink) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := ob.DialContext(ctx, network, M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			if sink != nil {
				sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
				conn = newCountingConn(conn, sink)
			}
			return conn, nil
		},
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        p.config.MaxIdleConns,
		MaxIdleConnsPerHost: p.config.MaxIdleConnsPerHost,
		IdleConnTimeout:     p.config.IdleConnTimeout,
	}
}

func newDirectHTTPTransport(cfg OutboundTransportConfig, sink MetricsEventSink) *http.Transport {
	cfg = normalizeOutboundTransportConfig(cfg)
	dialer := &net.Dialer{}
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if sink != nil {
				sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
				conn = newCountingConn(conn, sink)
			}
			return conn, nil
		},
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:     cfg.IdleConnTimeout,
	}
}
