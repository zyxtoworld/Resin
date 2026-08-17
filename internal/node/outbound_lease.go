package node

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// ErrOutboundUnavailable means that an entry is retiring or has no published
// outbound. Callers must fail closed instead of falling back to a same-hash
// replacement.
var ErrOutboundUnavailable = errors.New("outbound unavailable")

// InstallOutboundIfAbsent publishes an outbound only while this entry is
// live. It is the single production write path for a newly built outbound.
func (e *NodeEntry) InstallOutboundIfAbsent(ob adapter.Outbound) bool {
	if e == nil || ob == nil {
		return false
	}

	e.outboundMu.Lock()
	defer e.outboundMu.Unlock()
	if e.outboundRetiring || e.outboundRetired || e.Outbound.Load() != nil {
		return false
	}
	e.Outbound.Store(&ob)
	return true
}

// AcquireOutbound obtains a bounded use lease for the entry's current
// outbound. Retiring entries reject new leases. The release function is
// idempotent; the last release after retirement closes the adapter.
func (e *NodeEntry) AcquireOutbound() (adapter.Outbound, func(), bool) {
	return e.acquireOutbound()
}

// IsOutboundRetiring reports whether this exact entry has stopped admitting
// new outbound use. Transport caches use it to reject a stale route that
// arrives after node removal without changing the behavior of callers that
// provide an outbound before the entry has been initialized.
func (e *NodeEntry) IsOutboundRetiring() bool {
	if e == nil {
		return true
	}
	e.outboundMu.Lock()
	retiring := e.outboundRetiring || e.outboundRetired
	e.outboundMu.Unlock()
	return retiring
}

func (e *NodeEntry) acquireOutbound() (adapter.Outbound, func(), bool) {
	if e == nil {
		return nil, nil, false
	}

	e.outboundMu.Lock()
	if e.outboundRetiring || e.outboundRetired {
		e.outboundMu.Unlock()
		return nil, nil, false
	}
	ptr := e.Outbound.Load()
	if ptr == nil {
		e.outboundMu.Unlock()
		return nil, nil, false
	}
	outbound := *ptr
	e.outboundUsers++
	e.outboundMu.Unlock()

	var once sync.Once
	return outbound, func() {
		once.Do(e.releaseOutbound)
	}, true
}

func (e *NodeEntry) releaseOutbound() {
	e.outboundMu.Lock()
	if e.outboundUsers == 0 {
		e.outboundMu.Unlock()
		return
	}
	e.outboundUsers--
	var closeOutbound adapter.Outbound
	var closeDone chan struct{}
	if e.outboundRetiring && e.outboundUsers == 0 && !e.outboundRetired {
		e.outboundRetired = true
		closeOutbound = e.retiringOutbound
		e.retiringOutbound = nil
		closeDone = e.outboundCloseDone
	}
	e.outboundMu.Unlock()

	if closeDone != nil {
		defer close(closeDone)
	}
	closeNodeOutbound(closeOutbound)
}

// RetireOutbound stops new use and removes the published reference. It is
// deliberately non-blocking: pool lifecycle callbacks must not wait for an
// arbitrary DialContext or RoundTrip. Existing leases keep the adapter alive;
// the final release performs the one close.
func (e *NodeEntry) RetireOutbound() {
	if e == nil {
		return
	}

	e.outboundMu.Lock()
	if e.outboundRetiring || e.outboundRetired {
		e.outboundMu.Unlock()
		return
	}
	e.outboundRetiring = true
	e.outboundCloseDone = make(chan struct{})
	if ptr := e.Outbound.Swap(nil); ptr != nil {
		e.retiringOutbound = *ptr
	}
	var closeOutbound adapter.Outbound
	var closeDone chan struct{}
	if e.outboundUsers == 0 {
		e.outboundRetired = true
		closeOutbound = e.retiringOutbound
		e.retiringOutbound = nil
		closeDone = e.outboundCloseDone
	}
	e.outboundMu.Unlock()

	if closeDone != nil {
		go func() {
			defer close(closeDone)
			closeNodeOutbound(closeOutbound)
		}()
	}
}

// OutboundRetirementDone returns a completion signal for the exact entry's
// retirement. It is closed after the adapter's one final Close completes. A
// live, non-retiring entry returns an already-closed signal.
func (e *NodeEntry) OutboundRetirementDone() <-chan struct{} {
	if e == nil {
		return closedSignal()
	}
	e.outboundMu.Lock()
	done := e.outboundCloseDone
	retiring := e.outboundRetiring || e.outboundRetired
	e.outboundMu.Unlock()
	if !retiring || done == nil {
		return closedSignal()
	}
	return done
}

func closedSignal() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func closeNodeOutbound(ob adapter.Outbound) {
	if c, ok := ob.(io.Closer); ok {
		_ = c.Close()
	}
}

// NewLeasedOutbound returns an adapter bound to this exact NodeEntry. Each
// DialContext/ListenPacket acquires a short-lived entry lease and transfers it
// to the returned connection. The wrapper itself is not the resource owner and
// therefore Close is intentionally a no-op.
func (e *NodeEntry) NewLeasedOutbound() (adapter.Outbound, bool) {
	outbound, release, ok := e.AcquireOutbound()
	if !ok {
		return nil, false
	}
	defer release()
	wrapped := &leasedOutbound{
		entry:        e,
		typeName:     outbound.Type(),
		tag:          outbound.Tag(),
		networks:     append([]string(nil), outbound.Network()...),
		dependencies: append([]string(nil), outbound.Dependencies()...),
	}
	return wrapped, true
}

type leasedOutbound struct {
	entry        *NodeEntry
	typeName     string
	tag          string
	networks     []string
	dependencies []string
}

func (o *leasedOutbound) Type() string { return o.typeName }
func (o *leasedOutbound) Tag() string  { return o.tag }
func (o *leasedOutbound) Network() []string {
	return append([]string(nil), o.networks...)
}
func (o *leasedOutbound) Dependencies() []string {
	return append([]string(nil), o.dependencies...)
}

func (o *leasedOutbound) DialContext(
	ctx context.Context,
	network string,
	destination M.Socksaddr,
) (net.Conn, error) {
	outbound, release, ok := o.entry.acquireOutbound()
	if !ok {
		return nil, ErrOutboundUnavailable
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	conn, err := outbound.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("outbound returned nil connection")
	}
	transferred = true
	return &leasedConn{Conn: conn, release: release}, nil
}

func (o *leasedOutbound) ListenPacket(
	ctx context.Context,
	destination M.Socksaddr,
) (net.PacketConn, error) {
	outbound, release, ok := o.entry.acquireOutbound()
	if !ok {
		return nil, ErrOutboundUnavailable
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	packetConn, err := outbound.ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	if packetConn == nil {
		return nil, errors.New("outbound returned nil packet connection")
	}
	transferred = true
	return &leasedPacketConn{PacketConn: packetConn, release: release}, nil
}

func (o *leasedOutbound) Close() error { return nil }

type leasedConn struct {
	net.Conn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func (c *leasedConn) Close() error {
	c.closeOnce.Do(func() {
		defer c.release()
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

func (c *leasedConn) CloseWrite() error {
	closeWriter, ok := c.Conn.(interface{ CloseWrite() error })
	if !ok {
		return errors.New("half-close write unsupported")
	}
	return closeWriter.CloseWrite()
}

func (c *leasedConn) CloseRead() error {
	closeReader, ok := c.Conn.(interface{ CloseRead() error })
	if !ok {
		return errors.New("half-close read unsupported")
	}
	return closeReader.CloseRead()
}

type leasedPacketConn struct {
	net.PacketConn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func (c *leasedPacketConn) Close() error {
	c.closeOnce.Do(func() {
		defer c.release()
		c.closeErr = c.PacketConn.Close()
	})
	return c.closeErr
}

var _ adapter.Outbound = (*leasedOutbound)(nil)
