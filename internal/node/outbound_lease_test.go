package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type panicOutbound struct {
	closeCount   atomic.Int32
	closeEntered chan struct{}
	closeOnce    sync.Once
}

func (p *panicOutbound) Type() string           { return "panic" }
func (p *panicOutbound) Tag() string            { return "panic" }
func (p *panicOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (p *panicOutbound) Dependencies() []string { return nil }
func (p *panicOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	panic("dial panic")
}
func (p *panicOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	panic("listen packet panic")
}
func (p *panicOutbound) Close() error {
	p.closeCount.Add(1)
	if p.closeEntered != nil {
		p.closeOnce.Do(func() { close(p.closeEntered) })
	}
	return nil
}

var _ adapter.Outbound = (*panicOutbound)(nil)

type blockingCloseOutbound struct {
	closeEntered chan struct{}
	allowClose   chan struct{}
	closeOnce    sync.Once
	closeCount   atomic.Int32
}

func (o *blockingCloseOutbound) Type() string           { return "blocking-close" }
func (o *blockingCloseOutbound) Tag() string            { return "blocking-close" }
func (o *blockingCloseOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *blockingCloseOutbound) Dependencies() []string { return nil }
func (o *blockingCloseOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("dial not used")
}
func (o *blockingCloseOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("listen packet not used")
}
func (o *blockingCloseOutbound) Close() error {
	o.closeCount.Add(1)
	o.closeOnce.Do(func() { close(o.closeEntered) })
	<-o.allowClose
	return nil
}

func TestRetireOutboundDoesNotWaitForAdapterClose(t *testing.T) {
	ob := &blockingCloseOutbound{
		closeEntered: make(chan struct{}),
		allowClose:   make(chan struct{}),
	}
	entry := NewNodeEntry(Hash{9}, nil, time.Now(), 0)
	var raw adapter.Outbound = ob
	entry.Outbound.Store(&raw)

	retireDone := make(chan struct{})
	go func() {
		entry.RetireOutbound()
		close(retireDone)
	}()
	select {
	case <-ob.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("RetireOutbound did not start adapter close")
	}

	if entry.Outbound.Load() != nil {
		t.Fatal("retired entry still published an outbound")
	}
	if _, release, ok := entry.AcquireOutbound(); ok {
		release()
		t.Fatal("retired entry accepted a new outbound lease")
	}
	select {
	case <-retireDone:
	case <-time.After(time.Second):
		t.Fatal("RetireOutbound waited for adapter Close")
	}

	close(ob.allowClose)
	if got := ob.closeCount.Load(); got != 1 {
		t.Fatalf("adapter close count = %d, want 1", got)
	}
}

func TestLeasedOutbound_PanicReleasesLeaseBeforeRetire(t *testing.T) {
	tests := []struct {
		name string
		call func(adapter.Outbound)
	}{
		{
			name: "dial",
			call: func(outbound adapter.Outbound) {
				_, _ = outbound.DialContext(context.Background(), "tcp", M.ParseSocksaddr("example.com:443"))
			},
		},
		{
			name: "listen packet",
			call: func(outbound adapter.Outbound) {
				_, _ = outbound.ListenPacket(context.Background(), M.ParseSocksaddr("example.com:443"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := NewNodeEntry(Hash{byte(len(test.name))}, nil, time.Now(), 0)
			panicAdapter := &panicOutbound{closeEntered: make(chan struct{})}
			var raw adapter.Outbound = panicAdapter
			entry.Outbound.Store(&raw)
			leased, ok := entry.NewLeasedOutbound()
			if !ok {
				t.Fatal("expected leased outbound")
			}

			panicked := recoverCall(func() { test.call(leased) })
			if panicked != "dial panic" && panicked != "listen packet panic" {
				t.Fatalf("panic = %#v, want underlying panic", panicked)
			}

			entry.RetireOutbound()
			entry.RetireOutbound()
			select {
			case <-panicAdapter.closeEntered:
			case <-time.After(time.Second):
				t.Fatal("retired panic adapter was not closed")
			}
			if got := panicAdapter.closeCount.Load(); got != 1 {
				t.Fatalf("close count = %d, want 1", got)
			}
		})
	}
}

func recoverCall(fn func()) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}
