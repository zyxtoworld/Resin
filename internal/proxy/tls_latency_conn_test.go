package proxy

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type tlsLatencyReadWithErrorConn struct {
	readErr error
}

func (c *tlsLatencyReadWithErrorConn) Read(p []byte) (int, error) {
	p[0] = 's'
	return 1, c.readErr
}

func (c *tlsLatencyReadWithErrorConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *tlsLatencyReadWithErrorConn) Close() error                     { return nil }
func (c *tlsLatencyReadWithErrorConn) LocalAddr() net.Addr              { return stubAddr("local") }
func (c *tlsLatencyReadWithErrorConn) RemoteAddr() net.Addr             { return stubAddr("remote") }
func (c *tlsLatencyReadWithErrorConn) SetDeadline(time.Time) error      { return nil }
func (c *tlsLatencyReadWithErrorConn) SetReadDeadline(time.Time) error  { return nil }
func (c *tlsLatencyReadWithErrorConn) SetWriteDeadline(time.Time) error { return nil }

func TestTLSLatencyConn_RecordsBytesReturnedWithReadError(t *testing.T) {
	var callbacks atomic.Int32
	conn := newTLSLatencyConn(&tlsLatencyReadWithErrorConn{readErr: io.EOF}, func(time.Duration) {
		callbacks.Add(1)
	})

	if _, err := conn.Write([]byte("client hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	buf := make([]byte, 1)
	if n, err := conn.Read(buf); n != 1 || err != io.EOF {
		t.Fatalf("Read = (%d, %v), want (1, EOF)", n, err)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("latency callback count = %d, want 1", got)
	}
}
