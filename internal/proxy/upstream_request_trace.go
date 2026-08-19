package proxy

import (
	"net/http/httptrace"
	"sync/atomic"
)

// upstreamRequestTrace captures request-progress milestones reported by
// net/http transport so request-log egress bytes can be committed only when
// the request has actually been written to upstream.
type upstreamRequestTrace struct {
	gotFirstResponseByte func()
}

type upstreamRequestAttemptTrace struct {
	gotConn              atomic.Bool
	wroteRequest         atomic.Bool
	egressCommitted      atomic.Bool
	gotFirstResponseByte func()
}

func newUpstreamRequestTrace(gotFirstResponseByte ...func()) *upstreamRequestTrace {
	trace := &upstreamRequestTrace{}
	if len(gotFirstResponseByte) > 0 {
		trace.gotFirstResponseByte = gotFirstResponseByte[0]
	}
	return trace
}

func (t *upstreamRequestTrace) newAttempt() *upstreamRequestAttemptTrace {
	if t == nil {
		return &upstreamRequestAttemptTrace{}
	}
	return &upstreamRequestAttemptTrace{gotFirstResponseByte: t.gotFirstResponseByte}
}

func (t *upstreamRequestAttemptTrace) clientTrace() *httptrace.ClientTrace {
	if t == nil {
		return &httptrace.ClientTrace{}
	}
	return &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			t.gotConn.Store(true)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			// WroteRequest can also fire with Err!=nil for failed write attempts.
			if info.Err == nil {
				t.wroteRequest.Store(true)
			}
		},
		GotFirstResponseByte: func() {
			if t.gotFirstResponseByte != nil {
				t.gotFirstResponseByte()
			}
		},
	}
}

func (t *upstreamRequestAttemptTrace) shouldCommitEgress() bool {
	return t.gotConn.Load() && t.wroteRequest.Load()
}

// commitEgress acquires the one commit right for an attempt. A successful
// response is a transport-level proof that the request reached the upstream,
// even when a custom transport does not emit httptrace callbacks. Error
// responses have no such proof and are counted only after GotConn and a
// successful WroteRequest callback. Late callbacks only update this attempt's
// trace; they cannot submit a second accounting event.
func (t *upstreamRequestAttemptTrace) commitEgress(responseReceived bool, roundTripErr error) bool {
	if t == nil {
		return false
	}
	if !responseReceived && (roundTripErr == nil || !t.shouldCommitEgress()) {
		return false
	}
	return t.egressCommitted.CompareAndSwap(false, true)
}
