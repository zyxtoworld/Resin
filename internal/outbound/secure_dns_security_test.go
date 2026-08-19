package outbound

import (
	"strings"
	"testing"
)

func TestNewSingboxBuilderWithConfig_DoesNotExposeDNSUpstreamSecrets(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		secrets []string
		origin  string
	}{
		{
			name: "userinfo-path-query",
			raw:  "https://user:dns-userinfo-secret@dns.example.com/dns/dns-path-secret?token=dns-query-secret",
			secrets: []string{
				"dns-userinfo-secret",
				"dns-path-secret",
				"dns-query-secret",
				"user:",
			},
			origin: "https://dns.example.com",
		},
		{
			name:    "unsupported-query-value",
			raw:     "https://dns.example.com/dns-query?token=dns-query-secret",
			secrets: []string{"dns-query-secret"},
			origin:  "https://dns.example.com",
		},
		{
			name:    "unsupported-bootstrap-value",
			raw:     "https://dns.example.com/dns-query?bootstrap=dns-bootstrap-secret",
			secrets: []string{"dns-bootstrap-secret"},
			origin:  "https://dns.example.com",
		},
		{
			name:    "malformed-url",
			raw:     "https://dns.example.com/%zz?token=dns-malformed-secret",
			secrets: []string{"dns-malformed-secret", "%zz"},
			origin:  "[redacted-url]",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSingboxBuilderWithConfig(SingboxBuilderConfig{DNSUpstreams: []string{tc.raw}})
			if err == nil {
				t.Fatal("expected invalid DNS upstream")
			}
			for _, secret := range tc.secrets {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("DNS upstream error exposed %q: %v", secret, err)
				}
			}
			if !strings.Contains(err.Error(), tc.origin) {
				t.Fatalf("DNS upstream error lost safe origin %q: %v", tc.origin, err)
			}
		})
	}
}
