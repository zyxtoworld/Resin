package proxy

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type signalWriteConn struct {
	net.Conn
	successReplyEntered chan struct{}
	completedWrites     atomic.Int32
	once                sync.Once
}

func (c *signalWriteConn) Write(p []byte) (int, error) {
	if len(p) >= 2 && p[0] == socks5Version && p[1] == socks5ReplySucceeded {
		c.once.Do(func() { close(c.successReplyEntered) })
	}
	n, err := c.Conn.Write(p)
	if err == nil {
		c.completedWrites.Add(1)
	}
	return n, err
}

type closeSignalConn struct {
	net.Conn
	established     chan struct{}
	closed          chan struct{}
	establishedOnce sync.Once
	closeOnce       sync.Once
}

func (c *closeSignalConn) markEstablished() {
	c.establishedOnce.Do(func() { close(c.established) })
}

func (c *closeSignalConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestSocks5Inbound_BaseContextCancelInterruptsSuccessReplyWrite(t *testing.T) {
	env := newProxyE2EEnv(t)

	upstreamConn, upstreamPeer := net.Pipe()
	defer upstreamPeer.Close()
	trackedUpstream := &closeSignalConn{
		Conn:        upstreamConn,
		established: make(chan struct{}),
		closed:      make(chan struct{}),
	}
	setProxyE2EOutboundDialFunc(t, env, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		trackedUpstream.markEstablished()
		return trackedUpstream, nil
	})

	inbound := NewSocks5Inbound(Socks5InboundConfig{
		ProxyToken: "tok",
		Router:     env.router,
		Pool:       env.pool,
		Health:     &mockHealthRecorder{},
		Events:     NoOpEventEmitter{},
	})

	clientConn, rawServerConn := net.Pipe()
	defer clientConn.Close()
	defer rawServerConn.Close()
	serverConn := &signalWriteConn{
		Conn:                rawServerConn,
		successReplyEntered: make(chan struct{}),
	}

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	done := make(chan struct{})
	go func() {
		defer close(done)
		inbound.ServeConnContext(baseCtx, serverConn)
	}()

	reader := bufio.NewReader(clientConn)
	writeAll(t, clientConn, []byte{socks5Version, 1, socks5MethodUserPass})
	if got := readExactly(t, reader, 2); got[1] != socks5MethodUserPass {
		t.Fatalf("selected method: got %d, want %d", got[1], socks5MethodUserPass)
	}
	writeAll(t, clientConn, socks5UserPassPacket("plat.acct", "tok"))
	if got := readExactly(t, reader, 2); got[1] != socks5UserPassStatusSuccess {
		t.Fatalf("auth status: got %d, want %d", got[1], socks5UserPassStatusSuccess)
	}
	writeAll(t, clientConn, socks5ConnectIPv4Packet("127.0.0.1:443"))

	select {
	case <-trackedUpstream.established:
	case <-time.After(time.Second):
		t.Fatal("upstream connection was not established")
	}
	select {
	case <-serverConn.successReplyEntered:
	case <-time.After(time.Second):
		t.Fatal("SOCKS5 success reply write was not entered")
	}
	if got := serverConn.completedWrites.Load(); got != 2 {
		t.Fatalf("completed SOCKS5 handshake writes = %d, want method and auth replies", got)
	}

	cancelBase()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		// Release the old implementation's blocked net.Pipe write so this
		// regression test cannot leak a handler goroutine when it is red.
		_ = clientConn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("SOCKS5 handler remained blocked after test cleanup")
		}
		t.Fatal("base context cancellation did not interrupt success reply write")
	}

	select {
	case <-trackedUpstream.closed:
	case <-time.After(time.Second):
		t.Fatal("upstream connection was not closed after canceled success reply")
	}
}
