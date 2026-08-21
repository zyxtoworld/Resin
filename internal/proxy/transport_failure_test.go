package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type classifiedTimeoutError struct{}

func (classifiedTimeoutError) Error() string   { return "timeout" }
func (classifiedTimeoutError) Timeout() bool   { return true }
func (classifiedTimeoutError) Temporary() bool { return true }

func TestClassifyTransportFailureUsesAttemptMilestones(t *testing.T) {
	withWrittenRequest := func() *upstreamRequestAttemptTrace {
		trace := &upstreamRequestAttemptTrace{}
		trace.gotConn.Store(true)
		trace.wroteRequest.Store(true)
		return trace
	}
	withConnOnly := &upstreamRequestAttemptTrace{}
	withConnOnly.gotConn.Store(true)
	withFirstByte := withWrittenRequest()
	withFirstByte.firstResponseByteSeen.Store(true)

	for _, tc := range []struct {
		name  string
		err   error
		trace *upstreamRequestAttemptTrace
		want  string
	}{
		{name: "ordinary transport error", err: errors.New("dial failed"), want: "transport_error"},
		{name: "deadline before connection", err: context.DeadlineExceeded, want: "connect_timeout"},
		{name: "timeout before request write", err: classifiedTimeoutError{}, trace: withConnOnly, want: "transport_timeout"},
		{name: "response header timeout", err: classifiedTimeoutError{}, trace: withWrittenRequest(), want: "response_header_timeout"},
		{name: "idle after first byte", err: classifiedTimeoutError{}, trace: withFirstByte, want: "idle_timeout"},
		{name: "caller cancellation", err: context.Canceled, trace: withWrittenRequest(), want: "canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyTransportFailure(tc.err, tc.trace); got != tc.want {
				t.Fatalf("classifyTransportFailure: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClassifyTransportFailureRecognizesWrappedTimeout(t *testing.T) {
	wrapped := &net.OpError{Op: "read", Net: "tcp", Err: classifiedTimeoutError{}}
	trace := &upstreamRequestAttemptTrace{}
	trace.gotConn.Store(true)
	trace.wroteRequest.Store(true)
	if got := classifyTransportFailure(wrapped, trace); got != "response_header_timeout" {
		t.Fatalf("wrapped timeout classification: got %q, want response_header_timeout", got)
	}
}

func TestAttemptContextForRequestReservesOverallDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	ctx, attemptCancel, releaseAttempt, bounded := attemptContextForRequest(parent, 0, 3)
	if !bounded || attemptCancel == nil || ctx == nil {
		t.Fatal("deadline request did not receive a bounded attempt context")
	}
	deadline, ok := ctx.Deadline()
	parentDeadline, parentHasDeadline := parent.Deadline()
	if !ok || !parentHasDeadline || deadline.After(parentDeadline) {
		t.Fatal("attempt context escaped caller deadline")
	}
	attemptCancel()
	releaseAttempt()

	unbounded, noCancel, _, bounded := attemptContextForRequest(context.Background(), 0, 3)
	if bounded || noCancel != nil || unbounded == nil {
		t.Fatal("request without an overall deadline must not claim a bounded retry budget")
	}
}

func TestStoppableDeadlineContextAlreadyCanceledCleansHandles(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	ctx, cancel, release := newStoppableDeadlineContext(parent, 5*time.Minute)

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("already-canceled parent did not cancel derived context")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("derived context error = %v, want context.Canceled", ctx.Err())
	}
	c := ctx.(*stoppableDeadlineContext)
	c.mu.Lock()
	closed, stopHook, timer := c.closed, c.stopHook, c.timer
	c.mu.Unlock()
	if !closed || stopHook != nil || timer != nil {
		t.Fatalf("closed context retained handles: closed=%v stopHook=%v timer=%v", closed, stopHook != nil, timer != nil)
	}
	cancel()
	cancel()
	release()
	release()
}

func TestStoppableDeadlineContextConcurrentCancelRelease(t *testing.T) {
	for i := 0; i < 200; i++ {
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, cancel, release := newStoppableDeadlineContext(parent, time.Nanosecond)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			cancel()
			cancel()
		}()
		go func() {
			defer wg.Done()
			release()
			release()
		}()
		go func() {
			defer wg.Done()
			cancelParent()
		}()
		wg.Wait()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("concurrent cancellation did not close derived context")
		}
		cancel()
		release()
	}
}

type testReadWriteCloser struct {
	closeCount atomic.Int32
	writes     []byte
}

func (b *testReadWriteCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (b *testReadWriteCloser) Write(p []byte) (int, error) {
	b.writes = append(b.writes, p...)
	return len(p), nil
}

func (b *testReadWriteCloser) Close() error {
	b.closeCount.Add(1)
	return nil
}

func TestCancelOnCloseReadWriteCloserPreservesHalfClose(t *testing.T) {
	backend := &testReadWriteCloser{}
	var cancelCount atomic.Int32
	wrapped := &cancelOnCloseReadWriteCloser{
		ReadWriteCloser: backend,
		cancel:          func() { cancelCount.Add(1) },
	}
	var _ io.ReadWriteCloser = wrapped

	if _, err := wrapped.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read error = %v, want EOF", err)
	}
	if got := cancelCount.Load(); got != 0 {
		t.Fatalf("half-close canceled budget: got %d, want 0", got)
	}
	if n, err := wrapped.Write([]byte("client-data")); err != nil || n != len("client-data") {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if string(backend.writes) != "client-data" {
		t.Fatalf("write payload = %q, want client-data", backend.writes)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if got := backend.closeCount.Load(); got != 1 {
		t.Fatalf("backend close count = %d, want 1", got)
	}
	if got := cancelCount.Load(); got != 1 {
		t.Fatalf("cancel count = %d, want 1", got)
	}
}
