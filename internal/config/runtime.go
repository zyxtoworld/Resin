package config

import (
	"fmt"
	"time"
)

// MaxReverseProxyLogCaptureBytes bounds each captured request/response header
// or body field. Capture uses an in-memory bytes.Buffer, so an unbounded hot
// configuration would turn one request into an unbounded allocation.
const MaxReverseProxyLogCaptureBytes = 16 * 1024 * 1024

// RuntimeConfig holds all hot-updatable global settings.
// These are persisted in the database and served via GET /system/config.
type RuntimeConfig struct {
	// Request log
	RequestLogEnabled                  bool `json:"request_log_enabled"`
	ReverseProxyLogDetailEnabled       bool `json:"reverse_proxy_log_detail_enabled"`
	ReverseProxyLogReqHeadersMaxBytes  int  `json:"reverse_proxy_log_req_headers_max_bytes"`
	ReverseProxyLogReqBodyMaxBytes     int  `json:"reverse_proxy_log_req_body_max_bytes"`
	ReverseProxyLogRespHeadersMaxBytes int  `json:"reverse_proxy_log_resp_headers_max_bytes"`
	ReverseProxyLogRespBodyMaxBytes    int  `json:"reverse_proxy_log_resp_body_max_bytes"`

	// Health check
	MaxConsecutiveFailures          int      `json:"max_consecutive_failures"`
	MaxLatencyTestInterval          Duration `json:"max_latency_test_interval"`
	MaxAuthorityLatencyTestInterval Duration `json:"max_authority_latency_test_interval"`
	MaxEgressTestInterval           Duration `json:"max_egress_test_interval"`

	// Probe
	LatencyTestURL     string   `json:"latency_test_url"`
	LatencyAuthorities []string `json:"latency_authorities"`

	// P2C
	P2CLatencyWindow   Duration `json:"p2c_latency_window"`
	LatencyDecayWindow Duration `json:"latency_decay_window"`

	// Persistence
	CacheFlushInterval       Duration `json:"cache_flush_interval"`
	CacheFlushDirtyThreshold int      `json:"cache_flush_dirty_threshold"`
}

// NewDefaultRuntimeConfig returns a RuntimeConfig populated with the default
// values specified in DESIGN.md §运行时全局设置项.
func NewDefaultRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		RequestLogEnabled:                  true,
		ReverseProxyLogDetailEnabled:       false,
		ReverseProxyLogReqHeadersMaxBytes:  4096,
		ReverseProxyLogReqBodyMaxBytes:     1024,
		ReverseProxyLogRespHeadersMaxBytes: 1024,
		ReverseProxyLogRespBodyMaxBytes:    1024,

		MaxConsecutiveFailures:          3,
		MaxLatencyTestInterval:          Duration(1 * time.Hour),
		MaxAuthorityLatencyTestInterval: Duration(3 * time.Hour),
		MaxEgressTestInterval:           Duration(24 * time.Hour),

		LatencyTestURL:     "https://www.gstatic.com/generate_204",
		LatencyAuthorities: []string{"gstatic.com", "google.com", "cloudflare.com", "github.com"},

		P2CLatencyWindow:   Duration(10 * time.Minute),
		LatencyDecayWindow: Duration(10 * time.Minute),

		CacheFlushInterval:       Duration(5 * time.Minute),
		CacheFlushDirtyThreshold: 1000,
	}
}

// ValidateRuntimeLogCaptureLimits validates all in-memory payload capture
// limits shared by runtime PATCH and persisted-config bootstrap.
func ValidateRuntimeLogCaptureLimits(cfg *RuntimeConfig) error {
	if cfg == nil {
		return nil
	}
	limits := []struct {
		name  string
		value int
	}{
		{"reverse_proxy_log_req_headers_max_bytes", cfg.ReverseProxyLogReqHeadersMaxBytes},
		{"reverse_proxy_log_req_body_max_bytes", cfg.ReverseProxyLogReqBodyMaxBytes},
		{"reverse_proxy_log_resp_headers_max_bytes", cfg.ReverseProxyLogRespHeadersMaxBytes},
		{"reverse_proxy_log_resp_body_max_bytes", cfg.ReverseProxyLogRespBodyMaxBytes},
	}
	for _, limit := range limits {
		if limit.value < 0 {
			return fmt.Errorf("%s: must be non-negative", limit.name)
		}
		if limit.value > MaxReverseProxyLogCaptureBytes {
			return fmt.Errorf("%s: must be <= %d", limit.name, MaxReverseProxyLogCaptureBytes)
		}
	}
	return nil
}
