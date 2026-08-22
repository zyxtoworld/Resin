package proxy

type ProxyType int

const (
	ProxyTypeForward       ProxyType = 1
	ProxyTypeReverse       ProxyType = 2
	ProxyTypeSocks5Forward ProxyType = 3
)

// ConnectionDirection indicates inbound vs outbound connection flow.
type ConnectionDirection int

const (
	ConnectionInbound ConnectionDirection = iota
	ConnectionOutbound
)

// ConnectionOp is the operation type for a connection lifecycle event.
type ConnectionOp int

const (
	ConnectionOpen ConnectionOp = iota
	ConnectionClose
)

// ConnectionLifecycleEvent tracks connection open/close.
type ConnectionLifecycleEvent struct {
	Direction ConnectionDirection
	Op        ConnectionOp
}

// RequestFinishedEvent is emitted when a proxy request completes.
// Used by the metrics subsystem (Phase 8).
type RequestFinishedEvent struct {
	PlatformID string
	ProxyType  ProxyType // 1=http forward, 2=reverse, 3=socks5 forward
	IsConnect  bool
	NetOK      bool
	DurationNs int64
}

// RequestLogEntry captures per-request details for the structured request log.
// Used by the requestlog subsystem (Phase 8).
type RequestLogEntry struct {
	ID                  string    // optional stable ID; repo generates one when empty
	StartedAtNs         int64     // request start time (Unix nano), used as ts_ns in DB
	ProxyType           ProxyType // 1=http forward, 2=reverse, 3=socks5 forward
	ClientIP            string
	PlatformID          string
	PlatformName        string
	Account             string
	TargetHost          string
	TargetURL           string
	NodeHash            string
	NodeTag             string // display tag: "<Subscription>/<Tag>" (DESIGN.md §601)
	EgressIP            string
	DurationNs          int64
	FirstByteDurationNs int64
	NetOK               bool
	HTTPMethod          string
	HTTPStatus          int
	ResinError          string // logical proxy error code, e.g. UPSTREAM_TIMEOUT
	UpstreamStage       string // where upstream/network failure happened
	UpstreamErrKind     string // normalized error family
	UpstreamErrno       string // normalized errno, when available
	UpstreamErrMsg      string // sanitized upstream error message
	IngressBytes        int64  // bytes from upstream to client (header + body)
	EgressBytes         int64  // bytes from client to upstream (header + body)

	// Optional detail payload (mainly for reverse proxy request logging).
	ReqHeaders           []byte
	ReqHeadersLen        int
	ReqHeadersTruncated  bool
	ReqBody              []byte
	ReqBodyLen           int
	ReqBodyTruncated     bool
	RespHeaders          []byte
	RespHeadersLen       int
	RespHeadersTruncated bool
	RespBody             []byte
	RespBodyLen          int
	RespBodyTruncated    bool
	// AttemptDiagnostics is a bounded, sanitized timeline for each upstream
	// route attempt. It contains no URL, header, body, credential, or account
	// data; timings are relative to the request start.
	AttemptDiagnostics []RequestAttemptDiagnostic
	// AttemptCount is the total number of attempts observed. The detail slice
	// may be capped and marked by AttemptDiagnosticsTruncated.
	AttemptCount                int
	AttemptDiagnosticsTruncated bool
}

// RequestAttemptDiagnostic is the production-safe per-attempt timeline used
// to distinguish route-budget expiry from an accepted response/body failure.
// Zero timing values mean that the milestone was not observed.
type RequestAttemptDiagnostic struct {
	Attempt                 int    `json:"attempt"`
	RouteGeneration         uint64 `json:"route_generation"`
	PlatformRevisionNs      int64  `json:"platform_revision_ns"`
	NodeHash                string `json:"node_hash"`
	EgressIP                string `json:"egress_ip"`
	Transport               string `json:"transport"`
	RetryBudget             int    `json:"retry_budget"`
	RequestTotalTimeoutMs   int64  `json:"request_total_timeout_ms"`
	RequestAttemptTimeoutMs int64  `json:"request_attempt_timeout_ms"`
	MaxAttempts             int    `json:"max_attempts"`
	AttemptDeadlineMs       int64  `json:"attempt_deadline_ms"`
	StartedMs               int64  `json:"started_ms"`
	GotConnMs               int64  `json:"got_conn_ms"`
	WroteRequestMs          int64  `json:"wrote_request_ms"`
	ResponseHeaderMs        int64  `json:"response_header_ms"`
	FirstResponseByteMs     int64  `json:"first_response_byte_ms"`
	BodyStartMs             int64  `json:"body_start_ms"`
	RoundTripEndMs          int64  `json:"round_trip_end_ms"`
	BodyFinishMs            int64  `json:"body_finish_ms"`
	RequestBodyFinishMs     int64  `json:"request_body_finish_ms"`
	ResponseStatus          int    `json:"response_status"`
	ResponseStarted         bool   `json:"response_started"`
	RequestBodyComplete     bool   `json:"request_body_complete"`
	RequestBodyBytes        int64  `json:"request_body_bytes"`
	ResponseBodyBytes       int64  `json:"response_body_bytes"`
	ResponseBodyComplete    bool   `json:"response_body_complete"`
	ErrorKind               string `json:"error_kind"`
	CancelReason            string `json:"cancel_reason"`
	ReleaseReason           string `json:"release_reason"`
}

// EventEmitter defines the interface for proxy-layer event emission.
// Covers both metrics and requestlog event paths (STAGES.md Task 8).
type EventEmitter interface {
	EmitRequestFinished(RequestFinishedEvent)
	EmitRequestLog(RequestLogEntry)
}

// NoOpEventEmitter is a no-op implementation used until Phase 7/8.
type NoOpEventEmitter struct{}

func (NoOpEventEmitter) EmitRequestFinished(RequestFinishedEvent) {}
func (NoOpEventEmitter) EmitRequestLog(RequestLogEntry)           {}

// RequestLogRuntimeConfig is the complete runtime configuration needed for one
// request-log event. The provider must return one immutable generation so an
// event cannot combine fields from two hot-reloaded configurations.
type RequestLogRuntimeConfig struct {
	Enabled             bool
	DetailEnabled       bool
	ReqHeadersMaxBytes  int
	ReqBodyMaxBytes     int
	RespHeadersMaxBytes int
	RespBodyMaxBytes    int
}

// ConfigAwareEventEmitter wraps another EventEmitter and gates request-log
// emission by one complete runtime configuration snapshot.
type ConfigAwareEventEmitter struct {
	Base                     EventEmitter
	RequestLogConfigProvider func() RequestLogRuntimeConfig
}

type reverseDetailCaptureConfig struct {
	Enabled             bool
	ReqHeadersMaxBytes  int
	ReqBodyMaxBytes     int
	RespHeadersMaxBytes int
	RespBodyMaxBytes    int
}

func (e ConfigAwareEventEmitter) emitBase() EventEmitter {
	if e.Base == nil {
		return NoOpEventEmitter{}
	}
	return e.Base
}

func (e ConfigAwareEventEmitter) requestLogRuntimeConfig() RequestLogRuntimeConfig {
	if e.RequestLogConfigProvider != nil {
		return e.RequestLogConfigProvider()
	}
	return RequestLogRuntimeConfig{
		Enabled:             true,
		DetailEnabled:       true,
		ReqHeadersMaxBytes:  -1,
		ReqBodyMaxBytes:     -1,
		RespHeadersMaxBytes: -1,
		RespBodyMaxBytes:    -1,
	}
}

func (e ConfigAwareEventEmitter) reverseDetailCaptureConfig() reverseDetailCaptureConfig {
	cfg := e.requestLogRuntimeConfig()
	return reverseDetailCaptureConfig{
		Enabled:             cfg.Enabled && cfg.DetailEnabled,
		ReqHeadersMaxBytes:  cfg.ReqHeadersMaxBytes,
		ReqBodyMaxBytes:     cfg.ReqBodyMaxBytes,
		RespHeadersMaxBytes: cfg.RespHeadersMaxBytes,
		RespBodyMaxBytes:    cfg.RespBodyMaxBytes,
	}
}

func normalizePayloadField(payload []byte, length int, truncated bool, max int) ([]byte, int, bool) {
	if length < len(payload) {
		length = len(payload)
	}
	if max >= 0 && len(payload) > max {
		payload = payload[:max]
		truncated = true
	}
	if len(payload) == 0 {
		payload = nil
	} else {
		payload = append([]byte(nil), payload...)
	}
	return payload, length, truncated
}

func clearReverseDetailPayload(ev *RequestLogEntry) {
	ev.ReqHeaders = nil
	ev.ReqHeadersLen = 0
	ev.ReqHeadersTruncated = false
	ev.ReqBody = nil
	ev.ReqBodyLen = 0
	ev.ReqBodyTruncated = false
	ev.RespHeaders = nil
	ev.RespHeadersLen = 0
	ev.RespHeadersTruncated = false
	ev.RespBody = nil
	ev.RespBodyLen = 0
	ev.RespBodyTruncated = false
}

func (e ConfigAwareEventEmitter) EmitRequestFinished(ev RequestFinishedEvent) {
	e.emitBase().EmitRequestFinished(ev)
}

func (e ConfigAwareEventEmitter) EmitRequestLog(ev RequestLogEntry) {
	cfg := e.requestLogRuntimeConfig()
	if !cfg.Enabled {
		return
	}
	originalAttemptCount := len(ev.AttemptDiagnostics)
	var diagnosticsTruncated bool
	ev.AttemptDiagnostics, diagnosticsTruncated = NormalizeRequestAttemptDiagnostics(ev.AttemptDiagnostics)
	if ev.AttemptCount < originalAttemptCount {
		ev.AttemptCount = originalAttemptCount
	}
	ev.AttemptDiagnosticsTruncated = ev.AttemptDiagnosticsTruncated || diagnosticsTruncated

	// Reverse proxy detail payload is controlled by runtime config.
	if ev.ProxyType == ProxyTypeReverse {
		if !cfg.DetailEnabled {
			clearReverseDetailPayload(&ev)
		} else {
			ev.ReqHeaders, ev.ReqHeadersLen, ev.ReqHeadersTruncated = normalizePayloadField(
				ev.ReqHeaders,
				ev.ReqHeadersLen,
				ev.ReqHeadersTruncated,
				cfg.ReqHeadersMaxBytes,
			)
			ev.ReqBody, ev.ReqBodyLen, ev.ReqBodyTruncated = normalizePayloadField(
				ev.ReqBody,
				ev.ReqBodyLen,
				ev.ReqBodyTruncated,
				cfg.ReqBodyMaxBytes,
			)
			ev.RespHeaders, ev.RespHeadersLen, ev.RespHeadersTruncated = normalizePayloadField(
				ev.RespHeaders,
				ev.RespHeadersLen,
				ev.RespHeadersTruncated,
				cfg.RespHeadersMaxBytes,
			)
			ev.RespBody, ev.RespBodyLen, ev.RespBodyTruncated = normalizePayloadField(
				ev.RespBody,
				ev.RespBodyLen,
				ev.RespBodyTruncated,
				cfg.RespBodyMaxBytes,
			)
		}
	}

	e.emitBase().EmitRequestLog(ev)
}
