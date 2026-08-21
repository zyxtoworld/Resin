package platform

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

func TestResponseRules_MatchConfiguredTransportFailure(t *testing.T) {
	rules, err := CompileResponseRules("failure-policy", []model.PlatformResponseRule{{
		ID:      "header-timeout",
		Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			FailureKinds: []string{"response_header_timeout"},
		},
		Action: model.PlatformResponseRuleAction{
			Type:          "cooldown_then_retry_next",
			CooldownScope: "egress_ip",
			Fallback:      "fixed_duration",
			FixedDuration: "30s",
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Unix(123, 0)
	match, ok := rules.MatchFailure("response_header_timeout", now)
	if !ok || !match.RetryNext() || !match.Cooldown || !match.Until.Equal(now.Add(30*time.Second)) {
		t.Fatalf("failure match = %#v, matched=%v", match, ok)
	}
	if _, ok := rules.MatchFailure("connect_timeout", now); ok {
		t.Fatal("failure rule matched an unrelated transport phase")
	}
}

func TestResponseRules_RejectsMixedTransportAndResponsePredicates(t *testing.T) {
	_, err := CompileResponseRules("failure-policy", []model.PlatformResponseRule{{
		ID: "mixed", Enabled: true,
		Match: model.PlatformResponseRuleMatch{
			FailureKinds: []string{"transport_error"},
			Headers:      []model.PlatformResponseHeaderMatch{{Name: "X-Mode", Op: "exists"}},
		},
		Action: model.PlatformResponseRuleAction{Type: "retry_next"},
	}})
	if err == nil {
		t.Fatal("mixed transport and response predicates unexpectedly compiled")
	}
}

func TestResponseRules_FailureKindsAliasesAndRejectsDeadConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		want string
	}{
		{name: "first byte aliases response header", kind: "response_header_timeout", want: "first_byte_timeout"},
		{name: "response header aliases first byte", kind: "first_byte_timeout", want: "response_header_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules, err := CompileResponseRules("failure-policy", []model.PlatformResponseRule{{
				ID: "timeout", Enabled: true,
				Match:  model.PlatformResponseRuleMatch{FailureKinds: []string{tc.want}},
				Action: model.PlatformResponseRuleAction{Type: "retry_next"},
			}})
			if err != nil {
				t.Fatalf("CompileResponseRules: %v", err)
			}
			if _, ok := rules.MatchFailure(tc.kind, time.Now()); !ok {
				t.Fatalf("failure kind %q did not match configured alias %q", tc.kind, tc.want)
			}
		})
	}

	deadExpiry := []model.PlatformResponseExpirySource{{Type: "retry_after"}}
	if _, err := CompileResponseRules("failure-policy", []model.PlatformResponseRule{{
		ID: "failure-expiry", Enabled: true,
		Match: model.PlatformResponseRuleMatch{FailureKinds: []string{"transport_error"}},
		Action: model.PlatformResponseRuleAction{
			Type:          "cooldown_then_retry_next",
			CooldownScope: "egress_ip",
			ExpirySources: deadExpiry,
			Fallback:      "next_utc_midnight",
		},
	}}); err == nil {
		t.Fatal("failure rule with response expiry source unexpectedly compiled")
	}
	if _, err := CompileResponseRules("failure-policy", []model.PlatformResponseRule{{
		ID: "cooldown-noop", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{FailureKinds: []string{"transport_error"}},
		Action: model.PlatformResponseRuleAction{Type: "cooldown", CooldownScope: "egress_ip", Fallback: "none"},
	}}); err == nil {
		t.Fatal("cooldown-only failure rule without a deadline unexpectedly compiled")
	}
	allowed, err := CompileResponseRules("failure-policy", []model.PlatformResponseRule{{
		ID: "retry-no-expiry", Enabled: true,
		Match:  model.PlatformResponseRuleMatch{FailureKinds: []string{"transport_error"}},
		Action: model.PlatformResponseRuleAction{Type: "cooldown_then_retry_next", CooldownScope: "egress_ip", Fallback: "none"},
	}})
	if err != nil {
		t.Fatalf("cooldown_then_retry_next without expiry should remain retryable: %v", err)
	}
	match, ok := allowed.MatchFailure("transport_error", time.Now())
	if !ok || !match.RetryNext() || match.Cooldown {
		t.Fatalf("retry-only failure action: match=%+v ok=%v", match, ok)
	}
}
