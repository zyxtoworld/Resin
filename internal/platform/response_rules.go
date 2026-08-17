package platform

import (
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

// ResponseRuleScope controls the unit that is quarantined after a matching
// response. Egress-IP scope is appropriate for provider quotas keyed by the
// public IP; node scope is appropriate for a single broken node.
type ResponseRuleScope string

const (
	ResponseRuleScopeNode     ResponseRuleScope = "node"
	ResponseRuleScopeEgressIP ResponseRuleScope = "egress_ip"
)

const (
	responseRuleDefaultExpiryLayout = "rfc3339"
	responseRuleMaxCount            = 32
	responseRuleMaxRegexBytes       = 4096
)

// ResponseRule is the compiled runtime form of model.PlatformResponseRule.
type ResponseRule struct {
	Name          string
	StatusCodes   []int
	ResponseRegex *regexp.Regexp
	ExpiryRegex   *regexp.Regexp
	ExpiryLayout  string
	Cooldown      time.Duration
	Scope         ResponseRuleScope
}

// ResponseRules is the immutable compiled rule set attached to a runtime
// platform and copied into each route result.
type ResponseRules []ResponseRule

// ResponseRuleMatch is the result of matching one response rule.
type ResponseRuleMatch struct {
	RuleName string
	Scope    ResponseRuleScope
	Until    time.Time
}

// CompileResponseRules validates and compiles persisted platform rules.
func CompileResponseRules(platformID string, raw []model.PlatformResponseRule) (ResponseRules, error) {
	if len(raw) > responseRuleMaxCount {
		return nil, fmt.Errorf("decode platform %s response_rules: at most %d rules are allowed", platformID, responseRuleMaxCount)
	}

	compiled := make(ResponseRules, 0, len(raw))
	for i, item := range raw {
		if len(item.StatusCodes) == 0 {
			return nil, fmt.Errorf("response_rules[%d].status_codes: at least one HTTP status code is required", i)
		}

		statusCodes := append([]int(nil), item.StatusCodes...)
		sort.Ints(statusCodes)
		deduped := statusCodes[:0]
		for _, code := range statusCodes {
			if code < 100 || code > 599 {
				return nil, fmt.Errorf("response_rules[%d].status_codes: %d is not an HTTP status code", i, code)
			}
			if len(deduped) == 0 || deduped[len(deduped)-1] != code {
				deduped = append(deduped, code)
			}
		}

		responseRegex, err := compileResponseRuleRegex(i, "response_regex", item.ResponseRegex)
		if err != nil {
			return nil, err
		}
		expiryRegex, err := compileResponseRuleRegex(i, "expiry_regex", item.ExpiryRegex)
		if err != nil {
			return nil, err
		}
		if expiryRegex != nil && expiryRegex.NumSubexp() < 1 {
			return nil, fmt.Errorf("response_rules[%d].expiry_regex: must contain a capture group", i)
		}

		expiryLayout := strings.ToLower(strings.TrimSpace(item.ExpiryLayout))
		if expiryRegex != nil && expiryLayout == "" {
			expiryLayout = responseRuleDefaultExpiryLayout
		}
		if !validResponseRuleExpiryLayout(expiryLayout) {
			return nil, fmt.Errorf("response_rules[%d].expiry_layout: unsupported layout %q", i, item.ExpiryLayout)
		}

		var cooldown time.Duration
		if rawCooldown := strings.TrimSpace(item.Cooldown); rawCooldown != "" {
			cooldown, err = time.ParseDuration(rawCooldown)
			if err != nil || cooldown <= 0 {
				return nil, fmt.Errorf("response_rules[%d].cooldown: must be a positive duration", i)
			}
		}
		scope := ResponseRuleScope(strings.ToLower(strings.TrimSpace(item.Scope)))
		if scope == "" {
			scope = ResponseRuleScopeEgressIP
		}
		if scope != ResponseRuleScopeNode && scope != ResponseRuleScopeEgressIP {
			return nil, fmt.Errorf("response_rules[%d].scope: must be %q or %q", i, ResponseRuleScopeNode, ResponseRuleScopeEgressIP)
		}

		compiled = append(compiled, ResponseRule{
			Name:          strings.TrimSpace(item.Name),
			StatusCodes:   deduped,
			ResponseRegex: responseRegex,
			ExpiryRegex:   expiryRegex,
			ExpiryLayout:  expiryLayout,
			Cooldown:      cooldown,
			Scope:         scope,
		})
	}
	return compiled, nil
}

func compileResponseRuleRegex(index int, field, raw string) (*regexp.Regexp, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > responseRuleMaxRegexBytes {
		return nil, fmt.Errorf("response_rules[%d].%s: regex is too long", index, field)
	}
	re, err := regexp.Compile(raw)
	if err != nil {
		return nil, fmt.Errorf("response_rules[%d].%s: invalid regex: %w", index, field, err)
	}
	return re, nil
}

func validResponseRuleExpiryLayout(layout string) bool {
	switch layout {
	case "", "rfc3339", "rfc3339nano", "unix_seconds", "unix_milliseconds", "duration":
		return true
	default:
		return false
	}
}

// NeedsBody reports whether any rule needs response-body inspection.
func (rules ResponseRules) NeedsBody() bool {
	for _, rule := range rules {
		if rule.ResponseRegex != nil || rule.ExpiryRegex != nil {
			return true
		}
	}
	return false
}

// NeedsBodyForStatus reports whether any rule for statusCode needs response
// body inspection. Rules for other statuses must not force the proxy to drain
// an unrelated response body.
func (rules ResponseRules) NeedsBodyForStatus(statusCode int) bool {
	for _, rule := range rules {
		if containsStatusCode(rule.StatusCodes, statusCode) &&
			(rule.ResponseRegex != nil || rule.ExpiryRegex != nil) {
			return true
		}
	}
	return false
}

// Match applies the first matching rule and resolves its quarantine deadline.
// Retry-After is authoritative when present; the body expiry capture and the
// explicitly configured fixed cooldown are fallbacks. If the response does
// not provide a deadline and no fixed cooldown is configured, it is ignored.
func (rules ResponseRules) Match(statusCode int, body []byte, headers http.Header, now time.Time) (ResponseRuleMatch, bool) {
	for _, rule := range rules {
		if !containsStatusCode(rule.StatusCodes, statusCode) {
			continue
		}
		if rule.ResponseRegex != nil && !rule.ResponseRegex.Match(body) {
			continue
		}

		until, ok := responseRuleDeadline(rule, body, headers, now)
		if !ok || !until.After(now) {
			continue
		}
		return ResponseRuleMatch{RuleName: rule.Name, Scope: rule.Scope, Until: until}, true
	}
	return ResponseRuleMatch{}, false
}

func containsStatusCode(codes []int, statusCode int) bool {
	for _, code := range codes {
		if code == statusCode {
			return true
		}
	}
	return false
}

func responseRuleDeadline(rule ResponseRule, body []byte, headers http.Header, now time.Time) (time.Time, bool) {
	if retryAfter := strings.TrimSpace(headers.Get("Retry-After")); retryAfter != "" {
		if until, ok := parseRetryAfter(retryAfter, now); ok {
			return until, true
		}
	}

	if rule.ExpiryRegex != nil {
		match := rule.ExpiryRegex.FindSubmatch(body)
		if len(match) > 1 {
			if until, ok := parseResponseExpiry(string(match[1]), rule.ExpiryLayout, now); ok {
				return until, true
			}
		}
	}

	if rule.Cooldown > 0 {
		return now.Add(rule.Cooldown), true
	}
	return time.Time{}, false
}

func parseRetryAfter(raw string, now time.Time) (time.Time, bool) {
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds >= 0 {
		// A valid delta-seconds value can exceed time.Duration's roughly
		// 292-year range. Add it in Unix seconds so it remains exact instead
		// of overflowing during seconds-to-nanoseconds conversion. If the
		// resulting Unix timestamp cannot be represented, report it as an
		// invalid deadline so the caller can use its configured fallbacks.
		unixSeconds := now.Unix()
		if unixSeconds > math.MaxInt64-seconds {
			return time.Time{}, false
		}
		return time.Unix(unixSeconds+seconds, int64(now.Nanosecond())).In(now.Location()), true
	}
	if when, err := http.ParseTime(raw); err == nil {
		return when, true
	}
	return time.Time{}, false
}

func parseResponseExpiry(raw, layout string, now time.Time) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	switch layout {
	case "rfc3339":
		when, err := time.Parse(time.RFC3339, raw)
		return when, err == nil
	case "rfc3339nano":
		when, err := time.Parse(time.RFC3339Nano, raw)
		return when, err == nil
	case "unix_seconds":
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		return time.Unix(seconds, 0), true
	case "unix_milliseconds":
		milliseconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		return time.UnixMilli(milliseconds), true
	case "duration":
		duration, err := time.ParseDuration(raw)
		if err != nil || duration <= 0 {
			return time.Time{}, false
		}
		return now.Add(duration), true
	default:
		return time.Time{}, false
	}
}
