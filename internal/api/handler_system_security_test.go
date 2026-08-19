package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Resinat/Resin/internal/config"
)

func TestSystemEnvConfig_DoesNotExposeDNSUpstreamSecrets(t *testing.T) {
	const (
		pathSecret  = "dns-api-path-secret"
		querySecret = "dns-api-query-secret"
	)
	envCfg := &config.EnvConfig{
		NodeDNSUpstreams: []string{
			"https://dns.example.com/dns/" + pathSecret + "?token=" + querySecret,
			"local",
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/config/env", nil)
	rec := httptest.NewRecorder()
	HandleSystemEnvConfig(envCfg).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, secret := range []string{pathSecret, querySecret} {
		if strings.Contains(body, secret) {
			t.Fatalf("system env response exposed DNS upstream secret %q: %s", secret, body)
		}
	}
	if !strings.Contains(body, `https://dns.example.com`) {
		t.Fatalf("system env response lost safe DNS origin: %s", body)
	}
	if !strings.Contains(body, `"local"`) {
		t.Fatalf("system env response lost local DNS sentinel: %s", body)
	}
	var payload struct {
		NodeDNSUpstreamsRedacted []bool `json:"node_dns_upstreams_redacted"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode system env response: %v", err)
	}
	if got, want := payload.NodeDNSUpstreamsRedacted, []bool{true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DNS redaction flags = %#v, want %#v", got, want)
	}
}
