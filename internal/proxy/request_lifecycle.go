package proxy

import (
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/routing"
)

// requestLifecycle captures mutable per-request telemetry and emits both
// metrics and request-log events on completion.
type requestLifecycle struct {
	startedAt                   time.Time
	events                      EventEmitter
	finished                    RequestFinishedEvent
	log                         RequestLogEntry
	mu                          sync.Mutex
	finishOnce                  sync.Once
	finishedEmitted             bool
	firstByteDurationNs         atomic.Int64
	attemptDiagnosticsMu        sync.Mutex
	attemptDiagnostics          []*attemptDiagnostic
	attemptCount                int
	attemptDiagnosticsTruncated bool

	reqBodyCapture  *payloadCaptureReadCloser
	respBodyCapture *payloadCaptureReadCloser
}

func newRequestLifecycleFromMetadata(
	events EventEmitter,
	clientRemoteAddr string,
	method string,
	proxyType ProxyType,
	isConnect bool,
) *requestLifecycle {
	clientIP := ""
	if host, _, err := net.SplitHostPort(clientRemoteAddr); err == nil {
		clientIP = host
	} else {
		clientIP = clientRemoteAddr // fallback: bare IP or unparseable
	}
	now := time.Now()
	return &requestLifecycle{
		startedAt: now,
		events:    events,
		finished: RequestFinishedEvent{
			ProxyType: proxyType,
			IsConnect: isConnect,
		},
		log: RequestLogEntry{
			StartedAtNs: now.UnixNano(),
			ProxyType:   proxyType,
			ClientIP:    clientIP,
			HTTPMethod:  method,
		},
	}
}

func newRequestLifecycle(
	events EventEmitter,
	r *http.Request,
	proxyType ProxyType,
	isConnect bool,
) *requestLifecycle {
	method := ""
	clientRemoteAddr := ""
	if r != nil {
		method = r.Method
		clientRemoteAddr = r.RemoteAddr
	}
	return newRequestLifecycleFromMetadata(events, clientRemoteAddr, method, proxyType, isConnect)
}

func (l *requestLifecycle) finish() {
	if l == nil {
		return
	}
	l.finishOnce.Do(func() {
		l.mu.Lock()
		reqBodyCapture := l.reqBodyCapture
		respBodyCapture := l.respBodyCapture
		l.mu.Unlock()

		var reqBody []byte
		var reqBodyLen int
		var reqBodyTruncated bool
		if reqBodyCapture != nil {
			reqBody, reqBodyLen, reqBodyTruncated, _ = reqBodyCapture.Snapshot()
		}
		var respBody []byte
		var respBodyLen int
		var respBodyTruncated bool
		if respBodyCapture != nil {
			respBody = respBodyCapture.Payload()
			respBodyLen = respBodyCapture.TotalLen()
			respBodyTruncated = respBodyCapture.Truncated()
		}

		l.mu.Lock()
		if l.finishedEmitted {
			l.mu.Unlock()
			return
		}
		l.finishedEmitted = true
		if reqBody != nil || reqBodyLen != 0 || reqBodyTruncated {
			l.log.ReqBody = reqBody
			l.log.ReqBodyLen = reqBodyLen
			l.log.ReqBodyTruncated = reqBodyTruncated
		}
		l.log.RespBody = respBody
		l.log.RespBodyLen = respBodyLen
		l.log.RespBodyTruncated = respBodyTruncated
		durationNs := time.Since(l.startedAt).Nanoseconds()
		l.finished.DurationNs = durationNs
		l.log.DurationNs = durationNs
		l.log.FirstByteDurationNs = l.firstByteDurationNs.Load()
		l.attemptDiagnosticsMu.Lock()
		if len(l.attemptDiagnostics) > 0 || l.attemptCount > 0 {
			raw := make([]RequestAttemptDiagnostic, 0, len(l.attemptDiagnostics))
			for _, diagnostic := range l.attemptDiagnostics {
				raw = append(raw, diagnostic.snapshot())
			}
			l.log.AttemptDiagnostics, _ = NormalizeRequestAttemptDiagnostics(raw)
			l.log.AttemptCount = l.attemptCount
			l.log.AttemptDiagnosticsTruncated = l.attemptDiagnosticsTruncated
		}
		l.attemptDiagnosticsMu.Unlock()
		finished := l.finished
		logEntry := l.log
		l.mu.Unlock()

		l.events.EmitRequestFinished(finished)
		l.events.EmitRequestLog(logEntry)
	})
}

func (l *requestLifecycle) registerAttemptDiagnostic(diagnostic *attemptDiagnostic) {
	if l == nil || diagnostic == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.attemptDiagnosticsMu.Lock()
	l.attemptCount++
	if len(l.attemptDiagnostics) < MaxRequestAttemptDiagnostics {
		l.attemptDiagnostics = append(l.attemptDiagnostics, diagnostic)
	} else {
		// Preserve the first records and the newest terminal record while keeping
		// the request lifecycle bounded when a route advertises a large budget.
		l.attemptDiagnostics[len(l.attemptDiagnostics)-1] = diagnostic
		l.attemptDiagnosticsTruncated = true
	}
	l.attemptDiagnosticsMu.Unlock()
}

func (l *requestLifecycle) markFirstByteReceived() {
	if l == nil || l.startedAt.IsZero() {
		return
	}
	durationNs := time.Since(l.startedAt).Nanoseconds()
	if durationNs <= 0 {
		durationNs = 1
	}
	// 首字耗时只记录第一次观测到上游响应字节的时间，避免重试或多回调覆盖。
	l.firstByteDurationNs.CompareAndSwap(0, durationNs)
}

func (l *requestLifecycle) setHTTPStatus(code int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.log.HTTPStatus = code
}

func (l *requestLifecycle) setProxyError(pe *ProxyError) {
	if pe == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.log.ResinError = pe.ResinError
}

func (l *requestLifecycle) setUpstreamError(stage string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	if l.log.UpstreamStage == "" && stage != "" {
		l.log.UpstreamStage = stage
	}
	if err == nil || l.log.UpstreamErrMsg != "" {
		return
	}
	detail := summarizeUpstreamError(err)
	l.log.UpstreamErrKind = detail.Kind
	l.log.UpstreamErrno = detail.Errno
	l.log.UpstreamErrMsg = detail.Message
}

func (l *requestLifecycle) addIngressBytes(n int64) {
	if n > 0 {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.finishedEmitted {
			return
		}
		l.log.IngressBytes += n
	}
}

func (l *requestLifecycle) addEgressBytes(n int64) {
	if n > 0 {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.finishedEmitted {
			return
		}
		l.log.EgressBytes += n
	}
}

func (l *requestLifecycle) setNetOK(ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.finished.NetOK = ok
	l.log.NetOK = ok
}

func (l *requestLifecycle) setAccount(account string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.log.Account = account
}

func (l *requestLifecycle) setTarget(host, rawURL string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.log.TargetHost = host
	l.log.TargetURL = sanitizeLoggedTargetURL(rawURL)
}

// sanitizeLoggedTargetURL keeps request-log diagnostics useful without
// persisting credentials embedded in a proxy target URL. Only the origin is
// retained; path, query, and fragment may contain subscription credentials or
// other bearer material. The URL is still forwarded unchanged; this only
// affects the observability projection.
func sanitizeLoggedTargetURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	target, err := url.Parse(rawURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return "[redacted-url]"
	}
	return target.Scheme + "://" + target.Host
}

func (l *requestLifecycle) setReqHeadersCaptured(reqHeaders []byte, totalLen int, truncated bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.log.ReqHeaders = reqHeaders
	l.log.ReqHeadersLen = totalLen
	l.log.ReqHeadersTruncated = truncated
}

func (l *requestLifecycle) setReqBodyCapture(c *payloadCaptureReadCloser) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.reqBodyCapture = c
}

func (l *requestLifecycle) setRespHeadersCaptured(respHeaders []byte, totalLen int, truncated bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.log.RespHeaders = respHeaders
	l.log.RespHeadersLen = totalLen
	l.log.RespHeadersTruncated = truncated
}

func (l *requestLifecycle) setRespBodyCapture(c *payloadCaptureReadCloser) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.respBodyCapture = c
}

func (l *requestLifecycle) setRouteResult(result routing.RouteResult) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finishedEmitted {
		return
	}
	l.finished.PlatformID = result.PlatformID
	l.log.PlatformID = result.PlatformID
	l.log.PlatformName = result.PlatformName
	l.log.NodeHash = result.NodeHash.Hex()
	l.log.NodeTag = result.NodeTag
	l.log.EgressIP = result.EgressIP.String()
}
