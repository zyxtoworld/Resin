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
	return clientTraceFor(&t.gotConn, &t.wroteRequest, t.gotFirstResponseByte)
}

func clientTraceFor(gotConn, wroteRequest *atomic.Bool, gotFirstResponseByte func()) *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			gotConn.Store(true)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			// Only mark as written when transport reports write success.
			// WroteRequest can also fire with Err!=nil for failed write attempts.
			if info.Err == nil {
				wroteRequest.Store(true)
			}
		},
		GotFirstResponseByte: func() {
			if gotFirstResponseByte != nil {
				gotFirstResponseByte()
			}
		},
	}
}

func (t *upstreamRequestAttemptTrace) shouldCommitEgress() bool {
	return t.gotConn.Load() && t.wroteRequest.Load()
}
