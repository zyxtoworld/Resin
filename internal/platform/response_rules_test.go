package platform

import (
	"math"
	"net/http"
	"strconv"
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

func TestResponseRules_MatchLargeRetryAfterUsesExactDeltaSeconds(t *testing.T) {
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
	const seconds int64 = 18_446_744_074
	match, ok := rules.Match(
		http.StatusTooManyRequests,
		[]byte(`{"type":"FreeUsageLimitError"}`),
		http.Header{"Retry-After": []string{"18446744074"}},
		now,
	)
	if !ok {
		t.Fatal("expected response rule to match")
	}
	if got, want := match.Until.Unix(), now.Unix()+seconds; got != want {
		t.Fatalf("until unix seconds: got %d, want %d", got, want)
	}
	if got, want := match.Until.Nanosecond(), now.Nanosecond(); got != want {
		t.Fatalf("until nanoseconds: got %d, want %d", got, want)
	}
}

func TestResponseRules_RetryAfterDeltaSecondBoundaries(t *testing.T) {
	const maxDurationSeconds = int64(math.MaxInt64) / int64(time.Second)
	tests := []struct {
		name      string
		seconds   int64
		wantMatch bool
	}{
		{
			name:      "maximum whole duration seconds",
			seconds:   maxDurationSeconds,
			wantMatch: true,
		},
		{
			name:      "first value beyond duration range",
			seconds:   maxDurationSeconds + 1,
			wantMatch: true,
		},
		{
			name:      "unix deadline overflow",
			seconds:   math.MaxInt64,
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			match, ok := rules.Match(
				http.StatusTooManyRequests,
				[]byte(`{"type":"FreeUsageLimitError"}`),
				http.Header{"Retry-After": []string{strconv.FormatInt(tt.seconds, 10)}},
				now,
			)
			if ok != tt.wantMatch {
				t.Fatalf("match: got %v, want %v", ok, tt.wantMatch)
			}
			if !ok {
				return
			}
			if got, want := match.Until.Unix(), now.Unix()+tt.seconds; got != want {
				t.Fatalf("until unix seconds: got %d, want %d", got, want)
			}
			if got, want := match.Until.Nanosecond(), now.Nanosecond(); got != want {
				t.Fatalf("until nanoseconds: got %d, want %d", got, want)
			}
		})
	}
}

func TestResponseRules_MatchRetryAfterHTTPDate(t *testing.T) {
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
	want := now.Add(5 * time.Minute)
	match, ok := rules.Match(
		http.StatusTooManyRequests,
		[]byte(`{"type":"FreeUsageLimitError"}`),
		http.Header{"Retry-After": []string{want.Format(http.TimeFormat)}},
		now,
	)
	if !ok {
		t.Fatal("expected HTTP-date Retry-After to match")
	}
	if !match.Until.Equal(want) {
		t.Fatalf("until: got %s, want %s", match.Until, want)
	}
}

func TestResponseRules_InvalidRetryAfterFallsBackToExpiryRegex(t *testing.T) {
	for _, retryAfter := range []string{"not-a-date", strconv.FormatInt(math.MaxInt64, 10)} {
		t.Run(retryAfter, func(t *testing.T) {
			rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
				{
					StatusCodes:   []int{http.StatusTooManyRequests},
					ResponseRegex: `FreeUsageLimitError`,
					ExpiryRegex:   `reset_at=([^ ]+)`,
					ExpiryLayout:  "rfc3339",
					Scope:         "egress_ip",
				},
			})
			if err != nil {
				t.Fatalf("CompileResponseRules: %v", err)
			}

			now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
			match, ok := rules.Match(
				http.StatusTooManyRequests,
				[]byte(`FreeUsageLimitError reset_at=2026-08-13T12:05:00Z`),
				http.Header{"Retry-After": []string{retryAfter}},
				now,
			)
			if !ok {
				t.Fatal("expected expiry_regex fallback to match")
			}
			if want := now.Add(5 * time.Minute); !match.Until.Equal(want) {
				t.Fatalf("until: got %s, want %s", match.Until, want)
			}
		})
	}
}

func TestResponseRules_InvalidRetryAfterFallsBackToCooldown(t *testing.T) {
	for _, retryAfter := range []string{"not-a-date", strconv.FormatInt(math.MaxInt64, 10)} {
		t.Run(retryAfter, func(t *testing.T) {
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
				http.Header{"Retry-After": []string{retryAfter}},
				now,
			)
			if !ok {
				t.Fatal("expected cooldown fallback to match")
			}
			if want := now.Add(2 * time.Hour); !match.Until.Equal(want) {
				t.Fatalf("until: got %s, want %s", match.Until, want)
			}
		})
	}
}

func TestResponseRules_DeadlinePriorityIsHeaderThenExpiryThenCooldown(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			StatusCodes:   []int{http.StatusTooManyRequests},
			ResponseRegex: `FreeUsageLimitError`,
			ExpiryRegex:   `reset_at=([^ ]+)`,
			ExpiryLayout:  "rfc3339",
			Cooldown:      "2h",
			Scope:         "egress_ip",
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	body := []byte(`FreeUsageLimitError reset_at=2026-08-13T12:05:00Z`)

	match, ok := rules.Match(
		http.StatusTooManyRequests,
		body,
		http.Header{"Retry-After": []string{"30"}},
		now,
	)
	if !ok {
		t.Fatal("expected valid Retry-After to match")
	}
	if want := now.Add(30 * time.Second); !match.Until.Equal(want) {
		t.Fatalf("header priority: got %s, want %s", match.Until, want)
	}

	match, ok = rules.Match(http.StatusTooManyRequests, body, http.Header{}, now)
	if !ok {
		t.Fatal("expected expiry_regex to match")
	}
	if want := now.Add(5 * time.Minute); !match.Until.Equal(want) {
		t.Fatalf("expiry priority: got %s, want %s", match.Until, want)
	}

	match, ok = rules.Match(
		http.StatusTooManyRequests,
		[]byte(`FreeUsageLimitError reset_at=not-a-date`),
		http.Header{},
		now,
	)
	if !ok {
		t.Fatal("expected cooldown fallback to match")
	}
	if want := now.Add(2 * time.Hour); !match.Until.Equal(want) {
		t.Fatalf("cooldown fallback: got %s, want %s", match.Until, want)
	}
}

func TestResponseRules_ExpiredRetryAfterRemainsAuthoritative(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			StatusCodes:   []int{http.StatusTooManyRequests},
			ResponseRegex: `FreeUsageLimitError`,
			ExpiryRegex:   `reset_at=([^ ]+)`,
			ExpiryLayout:  "rfc3339",
			Cooldown:      "2h",
			Scope:         "egress_ip",
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	if _, ok := rules.Match(
		http.StatusTooManyRequests,
		[]byte(`FreeUsageLimitError reset_at=2026-08-13T12:05:00Z`),
		http.Header{"Retry-After": []string{"0"}},
		now,
	); ok {
		t.Fatal("expired Retry-After must not fall back to a later body/cooldown deadline")
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
