package proxy

import (
	"context"
	"errors"
	"net"
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
	ctx, attemptCancel, bounded := attemptContextForRequest(parent, 0, 3)
	if !bounded || attemptCancel == nil || ctx == nil {
		t.Fatal("deadline request did not receive a bounded attempt context")
	}
	deadline, ok := ctx.Deadline()
	parentDeadline, parentHasDeadline := parent.Deadline()
	if !ok || !parentHasDeadline || deadline.After(parentDeadline) {
		t.Fatal("attempt context escaped caller deadline")
	}
	attemptCancel()

	unbounded, noCancel, bounded := attemptContextForRequest(context.Background(), 0, 3)
	if bounded || noCancel != nil || unbounded == nil {
		t.Fatal("request without an overall deadline must not claim a bounded retry budget")
	}
}
