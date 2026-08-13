package platform

import (
	"net/http"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

func TestCompileResponseRules_MatchesBodyAndRetryAfter(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			Name:          "OpenCode free quota",
			StatusCodes:   []int{http.StatusTooManyRequests},
			ResponseRegex: `FreeUsageLimitError`,
			Scope:         "egress_ip",
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	match, ok := rules.Match(
		http.StatusTooManyRequests,
		[]byte(`{"type":"FreeUsageLimitError"}`),
		http.Header{"Retry-After": []string{"30"}},
		now,
	)
	if !ok {
		t.Fatal("expected response rule to match")
	}
	if match.Scope != ResponseRuleScopeEgressIP {
		t.Fatalf("scope: got %q, want %q", match.Scope, ResponseRuleScopeEgressIP)
	}
	want := now.Add(30 * time.Second)
	if !match.Until.Equal(want) {
		t.Fatalf("until: got %s, want %s", match.Until, want)
	}
}

func TestCompileResponseRules_WithoutResponseDeadlineDoesNotQuarantine(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			StatusCodes:   []int{http.StatusTooManyRequests},
			ResponseRegex: `FreeUsageLimitError`,
			Scope:         "egress_ip",
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	if _, ok := rules.Match(
		http.StatusTooManyRequests,
		[]byte(`{"type":"FreeUsageLimitError"}`),
		http.Header{},
		now,
	); ok {
		t.Fatal("response without a deadline must not quarantine the route")
	}
}

func TestCompileResponseRules_UsesExplicitCooldownAsFallback(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			StatusCodes:   []int{http.StatusServiceUnavailable},
			ResponseRegex: `temporary outage`,
			Cooldown:      "2h",
			Scope:         "node",
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	match, ok := rules.Match(
		http.StatusServiceUnavailable,
		[]byte(`temporary outage`),
		http.Header{},
		now,
	)
	if !ok {
		t.Fatal("expected explicit cooldown to match")
	}
	if want := now.Add(2 * time.Hour); !match.Until.Equal(want) {
		t.Fatalf("until: got %s, want %s", match.Until, want)
	}
}

func TestCompileResponseRules_ParsesExpiryFromResponseBody(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			StatusCodes:   []int{http.StatusTooManyRequests},
			ResponseRegex: `FreeUsageLimitError`,
			ExpiryRegex:   `reset_at=([^ ]+)`,
			ExpiryLayout:  "rfc3339",
			Scope:         "node",
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	match, ok := rules.Match(
		http.StatusTooManyRequests,
		[]byte(`FreeUsageLimitError reset_at=2026-08-13T12:05:00Z`),
		http.Header{},
		now,
	)
	if !ok {
		t.Fatal("expected response rule to match")
	}
	if match.Scope != ResponseRuleScopeNode {
		t.Fatalf("scope: got %q, want %q", match.Scope, ResponseRuleScopeNode)
	}
	want := now.Add(5 * time.Minute)
	if !match.Until.Equal(want) {
		t.Fatalf("until: got %s, want %s", match.Until, want)
	}
}
