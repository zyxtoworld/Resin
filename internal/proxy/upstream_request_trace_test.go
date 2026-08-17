package proxy

import (
	"errors"
	"net/http/httptrace"
	"testing"
)

func TestUpstreamRequestTrace_NoConnNoWrite(t *testing.T) {
	trace := newUpstreamRequestTrace()
	if trace.newAttempt().shouldCommitEgress() {
		t.Fatal("shouldCommitEgress: got true, want false")
	}
}

func TestUpstreamRequestTrace_GotConnAndSuccessfulWrite(t *testing.T) {
	trace := newUpstreamRequestTrace()
	attempt := trace.newAttempt()
	clientTrace := attempt.clientTrace()
	clientTrace.GotConn(httptrace.GotConnInfo{})
	clientTrace.WroteRequest(httptrace.WroteRequestInfo{Err: nil})

	if !attempt.shouldCommitEgress() {
		t.Fatal("shouldCommitEgress: got false, want true")
	}
}

func TestUpstreamRequestTrace_GotConnButWriteFailed(t *testing.T) {
	trace := newUpstreamRequestTrace()
	attempt := trace.newAttempt()
	clientTrace := attempt.clientTrace()
	clientTrace.GotConn(httptrace.GotConnInfo{})
	clientTrace.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("write failed")})

	if attempt.shouldCommitEgress() {
		t.Fatal("shouldCommitEgress: got true, want false")
	}
}

func TestUpstreamRequestTrace_RetryAfterWriteFailure(t *testing.T) {
	trace := newUpstreamRequestTrace()
	attempt := trace.newAttempt()
	clientTrace := attempt.clientTrace()
	clientTrace.GotConn(httptrace.GotConnInfo{})
	clientTrace.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("first attempt failed")})
	clientTrace.WroteRequest(httptrace.WroteRequestInfo{Err: nil})

	if !attempt.shouldCommitEgress() {
		t.Fatal("shouldCommitEgress: got false, want true")
	}
}

func TestUpstreamRequestTrace_NewAttemptDoesNotInheritWriteState(t *testing.T) {
	trace := newUpstreamRequestTrace()
	first := trace.newAttempt()
	firstTrace := first.clientTrace()
	firstTrace.GotConn(httptrace.GotConnInfo{})
	firstTrace.WroteRequest(httptrace.WroteRequestInfo{})
	if !first.shouldCommitEgress() {
		t.Fatal("first attempt should commit egress")
	}

	second := trace.newAttempt()
	secondTrace := second.clientTrace()
	secondTrace.GotConn(httptrace.GotConnInfo{})
	secondTrace.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("write failed")})
	if second.shouldCommitEgress() {
		t.Fatal("second attempt inherited first attempt write state")
	}
}
