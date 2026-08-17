package proxy

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// trafficFlushThreshold is the byte count at which a countingConn emits
// a traffic delta mid-stream. This ensures realtime throughput sampling
// and bucket aggregation see traffic during long-lived connections, not only
// at close. Fixed constant — not configurable.
const trafficFlushThreshold int64 = 32768 // 32 KB

// trafficFlushInterval bounds how long sub-threshold pending bytes can remain
// unreported on keep-alive connections. Exposed as var for unit tests.
var trafficFlushInterval = time.Second

// MetricsEventSink receives traffic and connection lifecycle events from the
// proxy layer. Implemented by metrics.Manager (wired in main.go).
// This interface is defined here (in the proxy package) to avoid an import
// cycle between proxy and metrics.
type MetricsEventSink interface {
	// OnTrafficDelta reports a global traffic byte count delta.
	OnTrafficDelta(ingressBytes, egressBytes int64)
	// OnConnectionLifecycle reports a connection open/close event.
	OnConnectionLifecycle(direction ConnectionDirection, op ConnectionOp)
}

// countingConn wraps a net.Conn, counting bytes read/written.
// Flushes a traffic delta every trafficFlushThreshold bytes
// and on Close (for the remainder).
type countingConn struct {
	net.Conn
	sink MetricsEventSink

	pendingRead  atomic.Int64
	pendingWrite atomic.Int64
	closed       atomic.Bool
	flushArmed   atomic.Bool
	opMu         sync.Mutex
	opCond       *sync.Cond
	activeOps    int
	closeDone    chan struct{}
	closeErr     error
}

func newCountingConn(conn net.Conn, sink MetricsEventSink) *countingConn {
	c := &countingConn{
		Conn:      conn,
		sink:      sink,
		closeDone: make(chan struct{}),
	}
	c.opCond = sync.NewCond(&c.opMu)
	return c
}

func (c *countingConn) Read(b []byte) (int, error) {
	if !c.beginOp() {
		return 0, net.ErrClosed
	}
	defer c.endOp()

	n, err := c.Conn.Read(b)
	if n > 0 {
		total := c.pendingRead.Add(int64(n))
		if total >= trafficFlushThreshold {
			c.flushPendingTraffic()
		} else {
			c.armDeferredFlush()
		}
	}
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	if !c.beginOp() {
		return 0, net.ErrClosed
	}
	defer c.endOp()

	n, err := c.Conn.Write(b)
	if n > 0 {
		total := c.pendingWrite.Add(int64(n))
		if total >= trafficFlushThreshold {
			c.flushPendingTraffic()
		} else {
			c.armDeferredFlush()
		}
	}
	return n, err
}

func (c *countingConn) armDeferredFlush() {
	c.opMu.Lock()
	if c.closed.Load() || c.flushArmed.Load() {
		c.opMu.Unlock()
		return
	}
	c.flushArmed.Store(true)
	c.opMu.Unlock()
	time.AfterFunc(trafficFlushInterval, func() {
		if !c.beginDeferredFlush() {
			return
		}
		defer c.endOp()
		c.flushArmed.Store(false)
		c.flushPendingTraffic()
	})
}

func (c *countingConn) beginDeferredFlush() bool {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.flushArmed.Store(false)
	if c.closed.Load() {
		return false
	}
	c.activeOps++
	return true
}

func (c *countingConn) beginOp() bool {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	if c.closed.Load() {
		return false
	}
	c.activeOps++
	return true
}

func (c *countingConn) endOp() {
	c.opMu.Lock()
	c.activeOps--
	if c.activeOps == 0 {
		c.opCond.Broadcast()
	}
	c.opMu.Unlock()
}

func (c *countingConn) waitForOps() {
	c.opMu.Lock()
	for c.activeOps != 0 {
		c.opCond.Wait()
	}
	c.opMu.Unlock()
}

func (c *countingConn) flushPendingTraffic() {
	pendR := c.pendingRead.Swap(0)
	pendW := c.pendingWrite.Swap(0)
	if pendR > 0 || pendW > 0 {
		c.sink.OnTrafficDelta(pendR, pendW)
	}
}

func (c *countingConn) Close() error {
	c.opMu.Lock()
	if c.closed.Load() {
		done := c.closeDone
		c.opMu.Unlock()
		<-done
		return c.closeErr
	}
	c.closed.Store(true)
	c.opMu.Unlock()
	defer func() {
		c.opMu.Lock()
		close(c.closeDone)
		c.opMu.Unlock()
	}()

	// Close the underlying connection first so admitted Read/Write calls can
	// return. Their operation leases keep the final flush from racing them.
	err := c.Conn.Close()
	c.waitForOps()
	// Flush remaining bytes.
	c.flushPendingTraffic()
	c.sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionClose)
	c.opMu.Lock()
	c.closeErr = err
	c.opMu.Unlock()
	return err
}

func (c *countingConn) CloseWrite() error {
	if !c.beginOp() {
		return net.ErrClosed
	}
	defer c.endOp()
	return closeWriteErr(c.Conn)
}

func (c *countingConn) CloseRead() error {
	if !c.beginOp() {
		return net.ErrClosed
	}
	defer c.endOp()
	return closeReadErr(c.Conn)
}

// countingListener wraps a net.Listener, emitting connection lifecycle events
// on Accept (open) and on each connection's Close.
type countingListener struct {
	net.Listener
	sink MetricsEventSink
}

// NewCountingListener wraps a listener with connection lifecycle tracking.
func NewCountingListener(ln net.Listener, sink MetricsEventSink) net.Listener {
	if sink == nil {
		return ln
	}
	return &countingListener{Listener: ln, sink: sink}
}

func (cl *countingListener) Accept() (net.Conn, error) {
	conn, err := cl.Listener.Accept()
	if err != nil {
		return nil, err
	}
	cl.sink.OnConnectionLifecycle(ConnectionInbound, ConnectionOpen)
	return &connCloseNotifier{Conn: conn, sink: cl.sink}, nil
}

// connCloseNotifier emits a connection close event on Close.
type connCloseNotifier struct {
	net.Conn
	sink MetricsEventSink

	closeMu   sync.Mutex
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func (c *connCloseNotifier) Close() error {
	c.closeMu.Lock()
	if c.closed {
		done := c.closeDone
		c.closeMu.Unlock()
		<-done
		c.closeMu.Lock()
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
	c.closed = true
	if c.closeDone == nil {
		c.closeDone = make(chan struct{})
	}
	done := c.closeDone
	c.closeMu.Unlock()
	defer func() {
		c.closeMu.Lock()
		close(done)
		c.closeMu.Unlock()
	}()

	err := c.Conn.Close()
	c.sink.OnConnectionLifecycle(ConnectionInbound, ConnectionClose)
	c.closeMu.Lock()
	c.closeErr = err
	c.closeMu.Unlock()
	return err
}

func (c *connCloseNotifier) CloseWrite() error {
	return closeWriteErr(c.Conn)
}

func (c *connCloseNotifier) CloseRead() error {
	return closeReadErr(c.Conn)
}
