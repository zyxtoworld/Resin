package proxy

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type trafficDelta struct {
	ingress int64
	egress  int64
}

type countingConnTestSink struct {
	traffic chan trafficDelta
	connOps chan ConnectionOp
}

type blockingTrafficSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingTrafficSink) OnTrafficDelta(int64, int64) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
}

func (s *blockingTrafficSink) OnConnectionLifecycle(ConnectionDirection, ConnectionOp) {}

type blockingConnectionCloseSink struct {
	entered    chan struct{}
	release    chan struct{}
	closeCalls atomic.Int32
	once       sync.Once
}

func (s *blockingConnectionCloseSink) OnTrafficDelta(int64, int64) {}

func (s *blockingConnectionCloseSink) OnConnectionLifecycle(_ ConnectionDirection, op ConnectionOp) {
	if op != ConnectionClose {
		return
	}
	s.closeCalls.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-s.release
}

func newCountingConnTestSink() *countingConnTestSink {
	return &countingConnTestSink{
		traffic: make(chan trafficDelta, 16),
		connOps: make(chan ConnectionOp, 16),
	}
}

func (s *countingConnTestSink) OnTrafficDelta(ingressBytes, egressBytes int64) {
	s.traffic <- trafficDelta{ingress: ingressBytes, egress: egressBytes}
}

func (s *countingConnTestSink) OnConnectionLifecycle(direction ConnectionDirection, op ConnectionOp) {
	if direction == ConnectionOutbound {
		s.connOps <- op
	}
}

type stubConn struct {
	closed       atomic.Bool
	closeCalls   atomic.Int32
	closeEntered chan struct{}
	closeOnce    sync.Once
}

func (c *stubConn) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (c *stubConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *stubConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *stubConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *stubConn) SetDeadline(_ time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(_ time.Time) error { return nil }
func (c *stubConn) Close() error {
	c.closeCalls.Add(1)
	if c.closeEntered != nil {
		c.closeOnce.Do(func() { close(c.closeEntered) })
	}
	c.closed.Store(true)
	return nil
}

type stubHalfCloseConn struct {
	stubConn
	closeWriteCalls atomic.Int32
	closeReadCalls  atomic.Int32
}

type gatedReadConn struct {
	stubConn
	readEntered chan struct{}
	allowRead   chan struct{}
	readOnce    sync.Once
}

func (c *gatedReadConn) Read(p []byte) (int, error) {
	c.readOnce.Do(func() { close(c.readEntered) })
	<-c.allowRead
	p[0] = 'x'
	return 1, nil
}

func (c *stubHalfCloseConn) CloseWrite() error {
	c.closeWriteCalls.Add(1)
	return nil
}

func (c *stubHalfCloseConn) CloseRead() error {
	c.closeReadCalls.Add(1)
	return nil
}

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

func waitTrafficDelta(t *testing.T, ch <-chan trafficDelta, timeout time.Duration) trafficDelta {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(timeout):
		t.Fatalf("expected traffic delta within %s", timeout)
		return trafficDelta{}
	}
}

func expectNoTrafficDelta(t *testing.T, ch <-chan trafficDelta, timeout time.Duration) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("unexpected extra traffic delta: %+v", d)
	case <-time.After(timeout):
	}
}

func TestCountingConn_DeferredFlushReportsSmallTraffic(t *testing.T) {
	prev := trafficFlushInterval
	trafficFlushInterval = 20 * time.Millisecond
	t.Cleanup(func() { trafficFlushInterval = prev })

	sink := newCountingConnTestSink()
	conn := newCountingConn(&stubConn{}, sink)
	defer conn.Close()

	if _, err := conn.Write(make([]byte, 128)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := waitTrafficDelta(t, sink.traffic, 300*time.Millisecond)
	if got.ingress != 0 || got.egress != 128 {
		t.Fatalf("traffic delta mismatch: got %+v, want ingress=0 egress=128", got)
	}
	expectNoTrafficDelta(t, sink.traffic, 60*time.Millisecond)
}

func TestCountingConn_ThresholdFlushIsImmediate(t *testing.T) {
	prev := trafficFlushInterval
	trafficFlushInterval = 2 * time.Second
	t.Cleanup(func() { trafficFlushInterval = prev })

	sink := newCountingConnTestSink()
	conn := newCountingConn(&stubConn{}, sink)
	defer conn.Close()

	start := time.Now()
	if _, err := conn.Write(make([]byte, trafficFlushThreshold)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := waitTrafficDelta(t, sink.traffic, 120*time.Millisecond)
	if got.ingress != 0 || got.egress != trafficFlushThreshold {
		t.Fatalf(
			"traffic delta mismatch: got %+v, want ingress=0 egress=%d",
			got,
			trafficFlushThreshold,
		)
	}
	if elapsed := time.Since(start); elapsed >= trafficFlushInterval {
		t.Fatalf("threshold flush waited for deferred interval: elapsed=%s interval=%s", elapsed, trafficFlushInterval)
	}
	expectNoTrafficDelta(t, sink.traffic, 50*time.Millisecond)
}

func TestCountingConn_CloseFlushesPendingOnce(t *testing.T) {
	prev := trafficFlushInterval
	trafficFlushInterval = 50 * time.Millisecond
	t.Cleanup(func() { trafficFlushInterval = prev })

	sink := newCountingConnTestSink()
	conn := newCountingConn(&stubConn{}, sink)

	if _, err := conn.Write(make([]byte, 77)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := waitTrafficDelta(t, sink.traffic, 100*time.Millisecond)
	if got.ingress != 0 || got.egress != 77 {
		t.Fatalf("traffic delta mismatch: got %+v, want ingress=0 egress=77", got)
	}
	select {
	case op := <-sink.connOps:
		if op != ConnectionClose {
			t.Fatalf("unexpected connection op: got %v, want %v", op, ConnectionClose)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected outbound close event")
	}
	expectNoTrafficDelta(t, sink.traffic, 90*time.Millisecond)
}

func TestCountingConn_CloseWaitsForAdmittedReadAndFlushesResult(t *testing.T) {
	prev := trafficFlushInterval
	trafficFlushInterval = time.Hour
	t.Cleanup(func() { trafficFlushInterval = prev })

	sink := newCountingConnTestSink()
	base := &gatedReadConn{
		stubConn:    stubConn{closeEntered: make(chan struct{})},
		readEntered: make(chan struct{}),
		allowRead:   make(chan struct{}),
	}
	conn := newCountingConn(base, sink)

	readDone := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		var buf [1]byte
		n, err := conn.Read(buf[:])
		readDone <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()
	select {
	case <-base.readEntered:
	case <-time.After(time.Second):
		t.Fatal("read did not enter the underlying connection")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- conn.Close() }()
	select {
	case <-base.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("Close did not reach the underlying connection")
	}
	secondCloseDone := make(chan error, 1)
	go func() { secondCloseDone <- conn.Close() }()
	select {
	case err := <-secondCloseDone:
		close(base.allowRead)
		<-readDone
		t.Fatalf("second Close returned before the first close completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(base.allowRead)
	result := <-readDone
	if result.n != 1 || result.err != nil {
		t.Fatalf("Read result = (%d, %v), want (1, nil)", result.n, result.err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-secondCloseDone; err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := base.closeCalls.Load(); got != 1 {
		t.Fatalf("underlying Close calls: got %d, want 1", got)
	}
	select {
	case op := <-sink.connOps:
		if op != ConnectionClose {
			t.Fatalf("unexpected connection op: got %v, want %v", op, ConnectionClose)
		}
	case <-time.After(time.Second):
		t.Fatal("expected outbound close event")
	}
	select {
	case op := <-sink.connOps:
		t.Fatalf("unexpected duplicate connection lifecycle event: %v", op)
	default:
	}

	got := waitTrafficDelta(t, sink.traffic, time.Second)
	if got.ingress != 1 || got.egress != 0 {
		t.Fatalf("traffic delta mismatch: got %+v, want ingress=1 egress=0", got)
	}
	expectNoTrafficDelta(t, sink.traffic, 100*time.Millisecond)
}

func TestCountingConn_CloseWaitsForAdmittedDeferredFlush(t *testing.T) {
	prev := trafficFlushInterval
	trafficFlushInterval = time.Millisecond
	t.Cleanup(func() { trafficFlushInterval = prev })

	sink := &blockingTrafficSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	conn := newCountingConn(&stubConn{}, sink)

	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("deferred traffic flush did not enter sink")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- conn.Close() }()
	select {
	case err := <-closeDone:
		close(sink.release)
		t.Fatalf("Close returned while deferred flush callback was active: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(sink.release)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConnCloseNotifier_ForwardsHalfClose(t *testing.T) {
	base := &stubHalfCloseConn{}
	conn := &connCloseNotifier{
		Conn: base,
		sink: newCountingConnTestSink(),
	}

	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := conn.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}
	if got := base.closeWriteCalls.Load(); got != 1 {
		t.Fatalf("CloseWrite calls: got %d, want 1", got)
	}
	if got := base.closeReadCalls.Load(); got != 1 {
		t.Fatalf("CloseRead calls: got %d, want 1", got)
	}
}

type connectionCloseOrderSink struct {
	underlyingClosed *atomic.Bool
	observed         chan bool
}

func (s *connectionCloseOrderSink) OnTrafficDelta(int64, int64) {}

func (s *connectionCloseOrderSink) OnConnectionLifecycle(_ ConnectionDirection, op ConnectionOp) {
	if op == ConnectionClose {
		s.observed <- s.underlyingClosed.Load()
	}
}

func TestConnCloseNotifier_ClosesUnderlyingBeforeCloseEvent(t *testing.T) {
	base := &stubConn{}
	sink := &connectionCloseOrderSink{
		underlyingClosed: &base.closed,
		observed:         make(chan bool, 1),
	}
	conn := &connCloseNotifier{
		Conn: base,
		sink: sink,
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case closed := <-sink.observed:
		if !closed {
			t.Fatal("close event was emitted before the underlying connection closed")
		}
	case <-time.After(time.Second):
		t.Fatal("close event was not emitted")
	}
}

func TestConnCloseNotifier_ConcurrentCloseWaitsForCompletion(t *testing.T) {
	sink := &blockingConnectionCloseSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	base := &stubConn{}
	conn := &connCloseNotifier{
		Conn: base,
		sink: sink,
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- conn.Close() }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("first Close did not enter the lifecycle sink")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- conn.Close() }()
	select {
	case err := <-secondDone:
		t.Fatalf("second Close returned before the first close completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(sink.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := base.closeCalls.Load(); got != 1 {
		t.Fatalf("underlying Close calls: got %d, want 1", got)
	}
	if got := sink.closeCalls.Load(); got != 1 {
		t.Fatalf("lifecycle close events: got %d, want 1", got)
	}
}
