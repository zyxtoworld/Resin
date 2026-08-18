package platform

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

func TestResponseRules_GenericActionsAndFirstMatchWins(t *testing.T) {
	cooldown := model.PlatformResponseRuleAction{
		Type:          "cooldown",
		CooldownScope: "egress_ip",
		Fallback:      "fixed_duration",
		FixedDuration: "5m",
	}
	retry := model.PlatformResponseRuleAction{Type: "retry_next"}
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			ID: "header-policy", Enabled: true,
			Match: model.PlatformResponseRuleMatch{
				StatusCodes: []int{http.StatusForbidden},
				Headers:     []model.PlatformResponseHeaderMatch{{Name: "X-Quota", Op: "contains", Value: "limited"}},
			},
			Action: cooldown,
		},
		{
			ID: "body-policy", Enabled: true,
			Match:  model.PlatformResponseRuleMatch{StatusRange: []model.PlatformResponseStatusRange{{Min: 500, Max: 599}}, Body: &model.PlatformResponseBodyMatch{Op: "regex", Value: `temporary`}},
			Action: retry,
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	decision, ok := rules.Match(http.StatusForbidden, nil, true, http.Header{"X-Quota": []string{"limited-now"}}, now)
	if !ok || decision.RuleID != "header-policy" || decision.Action != ResponseRuleActionCooldown || !decision.Cooldown || !decision.Until.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("header decision: %+v, matched=%v", decision, ok)
	}
	decision, ok = rules.Match(http.StatusBadGateway, []byte("temporary upstream"), true, nil, now)
	if !ok || decision.RuleID != "body-policy" || !decision.RetryNext() || decision.Cooldown {
		t.Fatalf("body retry decision: %+v, matched=%v", decision, ok)
	}
}

func TestResponseRules_HeaderAndBodyNegativesRequirePresenceAndCompleteBody(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			ID: "negative", Enabled: true,
			Match: model.PlatformResponseRuleMatch{
				StatusCodes: []int{http.StatusBadGateway},
				Headers:     []model.PlatformResponseHeaderMatch{{Name: "X-Mode", Op: "not_contains", Value: "blocked"}},
				Body:        &model.PlatformResponseBodyMatch{Op: "not_contains", Value: "known-error"},
			},
			Action: model.PlatformResponseRuleAction{Type: "retry_next"},
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	if _, ok := rules.Match(http.StatusBadGateway, []byte("safe prefix"), false, http.Header{"X-Mode": []string{"normal"}}, now); ok {
		t.Fatal("negative body predicate matched an incomplete bounded prefix")
	}
	decision, ok := rules.Match(http.StatusBadGateway, []byte("safe complete"), true, http.Header{"X-Mode": []string{"normal"}}, now)
	if !ok || !decision.RetryNext() {
		t.Fatalf("complete negative match: %+v, matched=%v", decision, ok)
	}
	if _, ok := rules.Match(http.StatusBadGateway, []byte("safe complete"), true, nil, now); ok {
		t.Fatal("negative header predicate treated absence as a match")
	}
}

func TestResponseRules_StatusRangesAndDisabledRules(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{ID: "disabled", Enabled: false, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{403}}, Action: model.PlatformResponseRuleAction{Type: "retry_next"}},
		{ID: "range", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusRange: []model.PlatformResponseStatusRange{{Min: 404, Max: 499}}}, Action: model.PlatformResponseRuleAction{Type: "retry_next"}},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	if _, ok := rules.Match(403, nil, true, nil, time.Now()); ok {
		t.Fatal("disabled rule unexpectedly matched")
	}
	if decision, ok := rules.Match(404, nil, true, nil, time.Now()); !ok || decision.RuleID != "range" {
		t.Fatalf("range decision: %+v, matched=%v", decision, ok)
	}
}

func TestResponseRules_ExpirySourceChainAndUTCFallback(t *testing.T) {
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{
		{
			ID: "expiry", Enabled: true,
			Match: model.PlatformResponseRuleMatch{StatusCodes: []int{429}},
			Action: model.PlatformResponseRuleAction{
				Type: "cooldown", CooldownScope: "route_entry", Fallback: "next_utc_midnight",
				ExpirySources: []model.PlatformResponseExpirySource{
					{Type: "retry_after"},
					{Type: "header", Header: "X-Reset", Format: "rfc3339_utc"},
					{Type: "json_pointer", JSONPointer: "/reset", Format: "unix_seconds"},
					{Type: "body_regex", Regex: `delta=([0-9]+)`, Capture: 1, Format: "delta_seconds"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	decision, ok := rules.Match(429, []byte(`{"reset":"bad","delta=30"}`), true, http.Header{"Retry-After": []string{"60"}, "X-Reset": []string{"bad"}}, now)
	if !ok || !decision.Until.Equal(now.Add(time.Minute)) {
		t.Fatalf("Retry-After priority: %+v, matched=%v", decision, ok)
	}
	reset := now.Add(2 * time.Minute).Unix()
	decision, ok = rules.Match(429, []byte(`{"reset":0,"delta=30"}`), true, http.Header{"X-Reset": []string{"bad"}}, now)
	if !ok || !decision.Until.Equal(now.Add(30*time.Second)) {
		t.Fatalf("body regex fallback: %+v, matched=%v", decision, ok)
	}
	decision, ok = rules.Match(429, []byte(`{"reset":`+strconv.FormatInt(reset, 10)+`}`), true, http.Header{"X-Reset": []string{"bad"}}, now)
	if !ok || !decision.Until.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("JSON pointer source: %+v, matched=%v", decision, ok)
	}
	for _, body := range []string{
		`{"reset":` + strconv.FormatInt(reset, 10) + `} trailing`,
		`{"reset":` + strconv.FormatInt(reset, 10) + `} {"second":1}`,
	} {
		decision, ok = rules.Match(429, []byte(body), true, http.Header{"X-Reset": []string{"bad"}}, now)
		if !ok || !decision.Until.Equal(wantMidnightForTest(now)) {
			t.Fatalf("non-single JSON body was trusted: body=%q decision=%+v matched=%v", body, decision, ok)
		}
	}
	decision, ok = rules.Match(429, []byte(`{"reset":0,"delta=bad"}`), true, http.Header{"X-Reset": []string{"bad"}}, now)
	wantMidnight := wantMidnightForTest(now)
	if !ok || !decision.Until.Equal(wantMidnight) {
		t.Fatalf("UTC fallback: %+v, matched=%v", decision, ok)
	}
}

func wantMidnightForTest(now time.Time) time.Time {
	utc := now.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day()+1, 0, 0, 0, 0, time.UTC)
}

func TestResponseRules_ExpirySourcesOnlyUseConfiguredOrder(t *testing.T) {
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	headerUntil := now.Add(2 * time.Minute)
	header := headerUntil.Format(time.RFC3339)

	makeRules := func(sources []model.PlatformResponseExpirySource) ResponseRules {
		rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{{
			ID: "ordered", Enabled: true,
			Match: model.PlatformResponseRuleMatch{StatusCodes: []int{429}},
			Action: model.PlatformResponseRuleAction{
				Type: "cooldown", CooldownScope: "egress_ip", Fallback: "none", ExpirySources: sources,
			},
		}})
		if err != nil {
			t.Fatalf("CompileResponseRules: %v", err)
		}
		return rules
	}

	decision, ok := makeRules([]model.PlatformResponseExpirySource{{
		Type: "header", Header: "X-Reset", Format: "rfc3339_utc",
	}}).Match(429, nil, true, http.Header{
		"Retry-After": []string{"60"},
		"X-Reset":     []string{header},
	}, now)
	if !ok || !decision.Until.Equal(headerUntil) {
		t.Fatalf("unconfigured Retry-After overrode configured header: %+v, matched=%v", decision, ok)
	}

	decision, ok = makeRules([]model.PlatformResponseExpirySource{
		{Type: "header", Header: "X-Reset", Format: "rfc3339_utc"},
		{Type: "retry_after"},
	}).Match(429, nil, true, http.Header{
		"Retry-After": []string{"60"},
		"X-Reset":     []string{header},
	}, now)
	if !ok || !decision.Until.Equal(headerUntil) {
		t.Fatalf("header did not win when configured first: %+v, matched=%v", decision, ok)
	}

	decision, ok = makeRules([]model.PlatformResponseExpirySource{
		{Type: "retry_after"},
		{Type: "header", Header: "X-Reset", Format: "rfc3339_utc"},
	}).Match(429, nil, true, http.Header{
		"Retry-After": []string{"60"},
		"X-Reset":     []string{header},
	}, now)
	if !ok || !decision.Until.Equal(now.Add(time.Minute)) {
		t.Fatalf("Retry-After did not win when configured first: %+v, matched=%v", decision, ok)
	}
}

func TestResponseRules_LegacyRetryAfterFormatKeepsHTTPDateSemantics(t *testing.T) {
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	rules, err := CompileResponseRules("plat-legacy-retry-after", []model.PlatformResponseRule{{
		ID: "legacy-retry-after", Enabled: true,
		Match: model.PlatformResponseRuleMatch{StatusCodes: []int{http.StatusTooManyRequests}},
		Action: model.PlatformResponseRuleAction{
			Type: "cooldown", CooldownScope: "egress_ip", Fallback: "none",
			ExpirySources: []model.PlatformResponseExpirySource{{Type: "retry_after", Format: "delta_seconds"}},
		},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}

	until := now.Add(90 * time.Second)
	decision, ok := rules.Match(http.StatusTooManyRequests, nil, true, http.Header{
		"Retry-After": []string{until.Format(http.TimeFormat)},
	}, now)
	if !ok || !decision.Cooldown || !decision.Until.Equal(until) {
		t.Fatalf("legacy Retry-After HTTP-date was not parsed: decision=%+v matched=%v", decision, ok)
	}
}

func TestResponseRules_ExpiryRejectsPastAndFarFuture(t *testing.T) {
	rules, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{{
		ID: "expiry", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{429}},
		Action: model.PlatformResponseRuleAction{Type: "cooldown", CooldownScope: "egress_ip", Fallback: "next_utc_midnight", ExpirySources: []model.PlatformResponseExpirySource{{Type: "header", Header: "X-Reset", Format: "unix_seconds"}}},
	}})
	if err != nil {
		t.Fatalf("CompileResponseRules: %v", err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	for _, raw := range []string{"0", strconv.FormatInt(now.Unix()-1, 10), strconv.FormatInt(now.Add(367*24*time.Hour).Unix(), 10), strconv.FormatInt(math.MaxInt64, 10)} {
		decision, ok := rules.Match(429, nil, true, http.Header{"X-Reset": []string{raw}}, now)
		want := time.Date(2026, time.August, 14, 0, 0, 0, 0, time.UTC)
		if !ok || !decision.Until.Equal(want) {
			t.Fatalf("reset %q did not use UTC fallback: %+v, matched=%v", raw, decision, ok)
		}
	}
}

func TestCompileResponseRules_RejectsInvalidSchemaAtomically(t *testing.T) {
	valid := model.PlatformResponseRule{ID: "valid", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{403}}, Action: model.PlatformResponseRuleAction{Type: "passthrough"}}
	for name, rules := range map[string][]model.PlatformResponseRule{
		"duplicate id":        {valid, valid},
		"invalid regex":       {{ID: "bad", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{403}, Body: &model.PlatformResponseBodyMatch{Op: "regex", Value: "("}}, Action: model.PlatformResponseRuleAction{Type: "passthrough"}}},
		"unknown action":      {{ID: "bad", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{403}}, Action: model.PlatformResponseRuleAction{Type: "unknown"}}},
		"bad range":           {{ID: "bad", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusRange: []model.PlatformResponseStatusRange{{Min: 500, Max: 400}}}, Action: model.PlatformResponseRuleAction{Type: "passthrough"}}},
		"invalid header name": {{ID: "bad", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{403}, Headers: []model.PlatformResponseHeaderMatch{{Name: "X Bad", Op: "exists"}}}, Action: model.PlatformResponseRuleAction{Type: "passthrough"}}},
		"body value too long": {{ID: "bad", Enabled: true, Match: model.PlatformResponseRuleMatch{StatusCodes: []int{403}, Body: &model.PlatformResponseBodyMatch{Op: "contains", Value: strings.Repeat("x", responseRuleMaxValueBytes+1)}}, Action: model.PlatformResponseRuleAction{Type: "passthrough"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := CompileResponseRules("plat-1", rules); err == nil {
				t.Fatal("expected schema rejection")
			}
		})
	}

	var oldShape model.PlatformResponseRule
	if err := json.Unmarshal([]byte(`{"id":"x","enabled":true,"status_codes":[429]}`), &oldShape); err == nil {
		t.Fatal("old flat fields were silently accepted")
	}
	for _, raw := range []string{
		`{"id":"x","enabled":true,"match":{"status_codes":[429],"unknown":true},"action":{"type":"passthrough"}}`,
		`{"id":"x","enabled":true,"match":{"status_codes":[429]},"action":{"type":"passthrough"}} trailing`,
		`{"id":"x","enabled":true,"match":{"status_codes":[429]},"action":{"type":"passthrough"}} {"another":1}`,
	} {
		var rule model.PlatformResponseRule
		if err := json.Unmarshal([]byte(raw), &rule); err == nil {
			t.Fatalf("invalid JSON/schema was accepted: %s", raw)
		}
	}
	tooManyHeaders := valid
	tooManyHeaders.Match.Headers = make([]model.PlatformResponseHeaderMatch, responseRuleMaxHeaders+1)
	tooManyHeaders.Match.Headers[0] = model.PlatformResponseHeaderMatch{Name: "X-Test", Op: "exists"}
	if _, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{tooManyHeaders}); err == nil {
		t.Fatal("too many header predicates were accepted")
	}
	tooManySources := valid
	tooManySources.Action = model.PlatformResponseRuleAction{
		Type: "cooldown", CooldownScope: "egress_ip", Fallback: "none",
		ExpirySources: make([]model.PlatformResponseExpirySource, responseRuleMaxExpirySources+1),
	}
	for i := range tooManySources.Action.ExpirySources {
		tooManySources.Action.ExpirySources[i] = model.PlatformResponseExpirySource{Type: "header", Header: "X-Reset", Format: "unix_seconds"}
	}
	if _, err := CompileResponseRules("plat-1", []model.PlatformResponseRule{tooManySources}); err == nil {
		t.Fatal("too many expiry sources were accepted")
	}
}
