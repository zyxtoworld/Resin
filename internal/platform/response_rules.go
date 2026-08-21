package platform

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

// ResponseRuleScope controls the unit that is quarantined after a matching
// response. It is deliberately independent of any upstream provider.
type ResponseRuleScope string

const (
	ResponseRuleScopeNode     ResponseRuleScope = "route_entry"
	ResponseRuleScopeEgressIP ResponseRuleScope = "egress_ip"
)

type ResponseRuleAction string

const (
	ResponseRuleActionPassthrough           ResponseRuleAction = "passthrough"
	ResponseRuleActionRetryNext             ResponseRuleAction = "retry_next"
	ResponseRuleActionCooldown              ResponseRuleAction = "cooldown"
	ResponseRuleActionCooldownThenRetryNext ResponseRuleAction = "cooldown_then_retry_next"
)

type ResponseExpiryFallback string

const (
	ResponseExpiryFallbackNextUTCMidnight ResponseExpiryFallback = "next_utc_midnight"
	ResponseExpiryFallbackFixedDuration   ResponseExpiryFallback = "fixed_duration"
	ResponseExpiryFallbackNone            ResponseExpiryFallback = "none"
)

const (
	responseRuleMaxCount          = 32
	responseRuleMaxStatusCodes    = 64
	responseRuleMaxStatusRanges   = 32
	responseRuleMaxHeaders        = 32
	responseRuleMaxExpirySources  = 16
	responseRuleMaxRegexBytes     = 4096
	responseRuleMaxValueBytes     = 4096
	responseRuleMaxExpiryBytes    = 256
	responseRuleMaxFieldNameBytes = 256
	responseRuleMaxFuture         = 366 * 24 * time.Hour
)

// ResponseRule is the immutable compiled runtime form of one persisted rule.
type ResponseRule struct {
	ID            string
	Enabled       bool
	Status        responseStatusMatcher
	Failures      responseFailureMatcher
	Headers       []responseHeaderMatcher
	Body          *responseBodyMatcher
	Action        ResponseRuleAction
	Scope         ResponseRuleScope
	Sources       []responseExpirySource
	Fallback      ResponseExpiryFallback
	FixedDuration time.Duration
}

type responseStatusMatcher struct {
	codes  []int
	ranges []model.PlatformResponseStatusRange
}

type responseFailureMatcher []string

type responseHeaderMatcher struct {
	name  string
	op    string
	value string
	re    *regexp.Regexp
}

type responseBodyMatcher struct {
	op    string
	value string
	re    *regexp.Regexp
}

type responseExpirySource struct {
	typ     string
	header  string
	pointer string
	re      *regexp.Regexp
	capture int
	format  string
}

// ResponseRules is the ordered, immutable compiled rule set attached to a
// runtime platform and copied into each route result.
type ResponseRules []ResponseRule

// ResponseRuleMatch describes the first matching rule. Until is zero when the
// action has no cooldown or no trusted deadline and its fallback is none.
type ResponseRuleMatch struct {
	RuleID   string
	Action   ResponseRuleAction
	Scope    ResponseRuleScope
	Until    time.Time
	Cooldown bool
}

func (m ResponseRuleMatch) RetryNext() bool {
	return m.Action == ResponseRuleActionRetryNext || m.Action == ResponseRuleActionCooldownThenRetryNext
}

// CompileResponseRules validates and compiles all rules before a platform is
// published. Any error leaves the previous runtime platform untouched at the
// service transaction boundary.
func CompileResponseRules(platformID string, raw []model.PlatformResponseRule) (ResponseRules, error) {
	if len(raw) > responseRuleMaxCount {
		return nil, fmt.Errorf("decode platform %s response_rules: at most %d rules are allowed", platformID, responseRuleMaxCount)
	}
	compiled := make(ResponseRules, 0, len(raw))
	seenIDs := make(map[string]struct{}, len(raw))
	for i, item := range raw {
		id := strings.TrimSpace(item.ID)
		if id == "" || len(id) > 128 {
			return nil, fmt.Errorf("response_rules[%d].id: must be 1..128 bytes", i)
		}
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("response_rules[%d].id: duplicate %q", i, id)
		}
		seenIDs[id] = struct{}{}

		failureKinds, err := compileResponseFailureKinds(i, item.Match.FailureKinds)
		if err != nil {
			return nil, err
		}
		if len(failureKinds) > 0 && (len(item.Match.Headers) > 0 || item.Match.Body != nil) {
			return nil, fmt.Errorf("response_rules[%d].match: failure_kinds cannot be combined with headers or body predicates", i)
		}
		status, err := compileResponseStatus(i, item.Match, len(failureKinds) != 0)
		if err != nil {
			return nil, err
		}
		headers, err := compileResponseHeaders(i, item.Match.Headers)
		if err != nil {
			return nil, err
		}
		body, err := compileResponseBody(i, item.Match.Body)
		if err != nil {
			return nil, err
		}
		action, err := compileResponseAction(i, item.Action)
		if err != nil {
			return nil, err
		}
		if len(failureKinds) > 0 && len(item.Action.ExpirySources) > 0 {
			return nil, fmt.Errorf("response_rules[%d].action.expiry_sources: failure_kinds rules cannot use response expiry sources", i)
		}
		if len(failureKinds) > 0 && action.Type == ResponseRuleActionCooldown && action.Fallback == ResponseExpiryFallbackNone {
			return nil, fmt.Errorf("response_rules[%d].action: cooldown failure rule needs retry or a cooldown deadline", i)
		}
		sources, err := compileResponseExpirySources(i, item.Action.ExpirySources)
		if err != nil {
			return nil, err
		}
		if !actionNeedsCooldown(action.Type) && len(sources) != 0 {
			return nil, fmt.Errorf("response_rules[%d].action.expiry_sources: only valid for cooldown actions", i)
		}
		if action.Type == ResponseRuleActionPassthrough && (item.Action.CooldownScope != "" || item.Action.Fallback != "" || item.Action.FixedDuration != "") {
			return nil, fmt.Errorf("response_rules[%d].action: passthrough cannot carry cooldown parameters", i)
		}

		compiled = append(compiled, ResponseRule{
			ID: id, Enabled: item.Enabled, Status: status, Headers: headers,
			Failures: failureKinds, Body: body, Action: action.Type, Scope: action.Scope,
			Sources: sources, Fallback: action.Fallback,
			FixedDuration: action.FixedDuration,
		})
	}
	return compiled, nil
}

func compileResponseStatus(index int, match model.PlatformResponseRuleMatch, hasFailureKinds bool) (responseStatusMatcher, error) {
	hasStatus := len(match.StatusCodes) != 0 || len(match.StatusRange) != 0
	if !hasStatus && !hasFailureKinds {
		return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match: status_codes, status_range, or failure_kinds is required", index)
	}
	if hasStatus && hasFailureKinds {
		return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match: status predicates and failure_kinds are mutually exclusive", index)
	}
	if len(match.StatusCodes) > responseRuleMaxStatusCodes {
		return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match.status_codes: at most %d values are allowed", index, responseRuleMaxStatusCodes)
	}
	if len(match.StatusRange) > responseRuleMaxStatusRanges {
		return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match.status_range: at most %d values are allowed", index, responseRuleMaxStatusRanges)
	}
	codes := append([]int(nil), match.StatusCodes...)
	seen := make(map[int]struct{}, len(codes))
	for _, code := range codes {
		if code < 100 || code > 599 {
			return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match.status_codes: %d is not an HTTP status code", index, code)
		}
		if _, ok := seen[code]; ok {
			return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match.status_codes: duplicate %d", index, code)
		}
		seen[code] = struct{}{}
	}
	ranges := append([]model.PlatformResponseStatusRange(nil), match.StatusRange...)
	for _, r := range ranges {
		if r.Min < 100 || r.Max > 599 || r.Min > r.Max {
			return responseStatusMatcher{}, fmt.Errorf("response_rules[%d].match.status_range: invalid [%d,%d]", index, r.Min, r.Max)
		}
	}
	return responseStatusMatcher{codes: codes, ranges: ranges}, nil
}

func compileResponseFailureKinds(index int, raw []string) (responseFailureMatcher, error) {
	if len(raw) > 8 {
		return nil, fmt.Errorf("response_rules[%d].match.failure_kinds: at most 8 values are allowed", index)
	}
	allowed := map[string]struct{}{
		"timeout":                 {},
		"transport_timeout":       {},
		"connect_timeout":         {},
		"response_header_timeout": {},
		"first_byte_timeout":      {},
		"idle_timeout":            {},
		"transport_error":         {},
	}
	seen := make(map[string]struct{}, len(raw))
	result := make(responseFailureMatcher, 0, len(raw))
	for i, value := range raw {
		kind := strings.ToLower(strings.TrimSpace(value))
		if _, ok := allowed[kind]; !ok {
			return nil, fmt.Errorf("response_rules[%d].match.failure_kinds[%d]: unsupported %q", index, i, value)
		}
		if _, ok := seen[kind]; ok {
			return nil, fmt.Errorf("response_rules[%d].match.failure_kinds[%d]: duplicate %q", index, i, value)
		}
		seen[kind] = struct{}{}
		result = append(result, kind)
	}
	return result, nil
}

func compileResponseHeaders(index int, raw []model.PlatformResponseHeaderMatch) ([]responseHeaderMatcher, error) {
	if len(raw) > responseRuleMaxHeaders {
		return nil, fmt.Errorf("response_rules[%d].match.headers: at most %d values are allowed", index, responseRuleMaxHeaders)
	}
	compiled := make([]responseHeaderMatcher, 0, len(raw))
	for j, item := range raw {
		if !validHTTPFieldName(item.Name) {
			return nil, fmt.Errorf("response_rules[%d].match.headers[%d].name: invalid header name", index, j)
		}
		name := textproto.CanonicalMIMEHeaderKey(item.Name)
		op := strings.ToLower(strings.TrimSpace(item.Op))
		if !validHeaderMatchOp(op) {
			return nil, fmt.Errorf("response_rules[%d].match.headers[%d].op: unsupported %q", index, j, item.Op)
		}
		value := item.Value
		if len(value) > responseRuleMaxValueBytes {
			return nil, fmt.Errorf("response_rules[%d].match.headers[%d].value: too long", index, j)
		}
		if (op == "exists" || op == "absent") && value != "" {
			return nil, fmt.Errorf("response_rules[%d].match.headers[%d].value: must be empty for %s", index, j, op)
		}
		if op != "exists" && op != "absent" && value == "" {
			return nil, fmt.Errorf("response_rules[%d].match.headers[%d].value: required for %s", index, j, op)
		}
		var re *regexp.Regexp
		if op == "regex" || op == "not_regex" {
			var err error
			re, err = compileResponseRegex(index, fmt.Sprintf("match.headers[%d].value", j), value)
			if err != nil {
				return nil, err
			}
		}
		compiled = append(compiled, responseHeaderMatcher{name: name, op: op, value: value, re: re})
	}
	return compiled, nil
}

func compileResponseBody(index int, raw *model.PlatformResponseBodyMatch) (*responseBodyMatcher, error) {
	if raw == nil {
		return nil, nil
	}
	op := strings.ToLower(strings.TrimSpace(raw.Op))
	if op != "regex" && op != "not_regex" && op != "contains" && op != "not_contains" {
		return nil, fmt.Errorf("response_rules[%d].match.body.op: unsupported %q", index, raw.Op)
	}
	if raw.Value == "" {
		return nil, fmt.Errorf("response_rules[%d].match.body.value: required", index)
	}
	if len(raw.Value) > responseRuleMaxValueBytes {
		return nil, fmt.Errorf("response_rules[%d].match.body.value: too long", index)
	}
	var re *regexp.Regexp
	var err error
	if op == "regex" || op == "not_regex" {
		re, err = compileResponseRegex(index, "match.body.value", raw.Value)
		if err != nil {
			return nil, err
		}
	}
	return &responseBodyMatcher{op: op, value: raw.Value, re: re}, nil
}

type compiledAction struct {
	Type          ResponseRuleAction
	Scope         ResponseRuleScope
	Fallback      ResponseExpiryFallback
	FixedDuration time.Duration
}

func compileResponseAction(index int, raw model.PlatformResponseRuleAction) (compiledAction, error) {
	typ := ResponseRuleAction(strings.ToLower(strings.TrimSpace(raw.Type)))
	switch typ {
	case ResponseRuleActionPassthrough, ResponseRuleActionRetryNext,
		ResponseRuleActionCooldown, ResponseRuleActionCooldownThenRetryNext:
	default:
		return compiledAction{}, fmt.Errorf("response_rules[%d].action.type: unsupported %q", index, raw.Type)
	}
	result := compiledAction{Type: typ}
	if actionNeedsCooldown(typ) {
		scope := ResponseRuleScope(strings.ToLower(strings.TrimSpace(raw.CooldownScope)))
		if scope != ResponseRuleScopeNode && scope != ResponseRuleScopeEgressIP {
			return compiledAction{}, fmt.Errorf("response_rules[%d].action.cooldown_scope: must be egress_ip or route_entry", index)
		}
		result.Scope = scope
		fallback := ResponseExpiryFallback(strings.ToLower(strings.TrimSpace(raw.Fallback)))
		switch fallback {
		case ResponseExpiryFallbackNextUTCMidnight, ResponseExpiryFallbackFixedDuration, ResponseExpiryFallbackNone:
		default:
			return compiledAction{}, fmt.Errorf("response_rules[%d].action.fallback: unsupported %q", index, raw.Fallback)
		}
		result.Fallback = fallback
		if fallback == ResponseExpiryFallbackFixedDuration {
			if len(raw.FixedDuration) > responseRuleMaxValueBytes {
				return compiledAction{}, fmt.Errorf("response_rules[%d].action.fixed_duration: too long", index)
			}
			d, err := time.ParseDuration(strings.TrimSpace(raw.FixedDuration))
			if err != nil || d <= 0 || d > responseRuleMaxFuture {
				return compiledAction{}, fmt.Errorf("response_rules[%d].action.fixed_duration: must be positive and no greater than the maximum horizon", index)
			}
			result.FixedDuration = d
		} else if raw.FixedDuration != "" {
			return compiledAction{}, fmt.Errorf("response_rules[%d].action.fixed_duration: only valid with fixed_duration fallback", index)
		}
	} else if raw.CooldownScope != "" || raw.Fallback != "" || raw.FixedDuration != "" {
		return compiledAction{}, fmt.Errorf("response_rules[%d].action: cooldown parameters require a cooldown action", index)
	}
	return result, nil
}

func compileResponseExpirySources(index int, raw []model.PlatformResponseExpirySource) ([]responseExpirySource, error) {
	if len(raw) > responseRuleMaxExpirySources {
		return nil, fmt.Errorf("response_rules[%d].action.expiry_sources: at most %d values are allowed", index, responseRuleMaxExpirySources)
	}
	compiled := make([]responseExpirySource, 0, len(raw))
	for j, item := range raw {
		typ := strings.ToLower(strings.TrimSpace(item.Type))
		format := strings.ToLower(strings.TrimSpace(item.Format))
		if format != "" && !validResponseExpiryFormat(format) {
			return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d].format: unsupported %q", index, j, item.Format)
		}
		source := responseExpirySource{typ: typ, format: format}
		switch typ {
		case "retry_after":
			if item.Header != "" || item.JSONPointer != "" || item.Regex != "" || item.Capture != 0 || (format != "" && format != "delta_seconds") {
				return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d]: invalid retry_after fields", index, j)
			}
		case "header":
			if !validHTTPFieldName(item.Header) || item.JSONPointer != "" || item.Regex != "" || item.Capture != 0 || format == "" {
				return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d]: invalid header source", index, j)
			}
			name := textproto.CanonicalMIMEHeaderKey(item.Header)
			if name == "Retry-After" {
				return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d]: Retry-After must use retry_after source", index, j)
			}
			source.header = name
		case "json_pointer":
			if !validResponseJSONPointer(item.JSONPointer) || item.Header != "" || item.Regex != "" || item.Capture != 0 || format == "" {
				return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d]: invalid json_pointer source", index, j)
			}
			source.pointer = item.JSONPointer
		case "body_regex":
			if item.Regex == "" || item.Header != "" || item.JSONPointer != "" || format == "" {
				return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d]: invalid body_regex source", index, j)
			}
			re, err := compileResponseRegex(index, fmt.Sprintf("action.expiry_sources[%d].regex", j), item.Regex)
			if err != nil {
				return nil, err
			}
			capture := item.Capture
			if capture == 0 {
				capture = 1
			}
			if capture < 1 || capture > re.NumSubexp() {
				return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d].capture: outside regex groups", index, j)
			}
			source.re, source.capture = re, capture
		default:
			return nil, fmt.Errorf("response_rules[%d].action.expiry_sources[%d].type: unsupported %q", index, j, item.Type)
		}
		compiled = append(compiled, source)
	}
	return compiled, nil
}

func compileResponseRegex(index int, field, raw string) (*regexp.Regexp, error) {
	if len(raw) > responseRuleMaxRegexBytes {
		return nil, fmt.Errorf("response_rules[%d].%s: regex is too long", index, field)
	}
	re, err := regexp.Compile(raw)
	if err != nil {
		return nil, fmt.Errorf("response_rules[%d].%s: invalid regex: %w", index, field, err)
	}
	return re, nil
}

func actionNeedsCooldown(action ResponseRuleAction) bool {
	return action == ResponseRuleActionCooldown || action == ResponseRuleActionCooldownThenRetryNext
}

func validHeaderMatchOp(op string) bool {
	switch op {
	case "exists", "absent", "regex", "not_regex", "contains", "not_contains":
		return true
	default:
		return false
	}
}

func validHTTPFieldName(name string) bool {
	if name == "" || len(name) > responseRuleMaxFieldNameBytes {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		default:
			return false
		}
	}
	return true
}

func validResponseJSONPointer(pointer string) bool {
	if pointer == "" || len(pointer) > responseRuleMaxValueBytes || pointer[0] != '/' {
		return false
	}
	for i := 1; i < len(pointer); i++ {
		if pointer[i] != '~' {
			continue
		}
		if i+1 >= len(pointer) || (pointer[i+1] != '0' && pointer[i+1] != '1') {
			return false
		}
		i++
	}
	return true
}

func validResponseExpiryFormat(format string) bool {
	switch format {
	case "rfc3339_utc", "unix_seconds", "unix_millis", "delta_seconds":
		return true
	default:
		return false
	}
}

// NeedsBodyForStatus reports whether a matching rule needs bounded body
// inspection for this status. Unrelated statuses never drain their bodies.
func (rules ResponseRules) NeedsBodyForStatus(statusCode int) bool {
	for _, rule := range rules {
		if !rule.Enabled || !rule.Status.matches(statusCode) {
			continue
		}
		if rule.Body != nil {
			return true
		}
		for _, source := range rule.Sources {
			if source.typ == "json_pointer" || source.typ == "body_regex" {
				return true
			}
		}
	}
	return false
}

func (rules ResponseRules) NeedsBody() bool {
	for _, rule := range rules {
		if rule.Enabled && (rule.Body != nil || len(rule.Sources) != 0) {
			return true
		}
	}
	return false
}

// Match applies the first enabled rule whose predicates all match. It does
// not reinterpret actions: callers decide whether to quarantine or retry from
// the returned generic action.
func (rules ResponseRules) Match(statusCode int, body []byte, bodyComplete bool, headers http.Header, now time.Time) (ResponseRuleMatch, bool) {
	for _, rule := range rules {
		if !rule.Enabled || !rule.Status.matches(statusCode) || !headersMatch(rule.Headers, headers) || !bodyMatches(rule.Body, body, bodyComplete) {
			continue
		}
		result := ResponseRuleMatch{RuleID: rule.ID, Action: rule.Action, Scope: rule.Scope}
		if actionNeedsCooldown(rule.Action) {
			if until, ok := responseRuleDeadline(rule, body, bodyComplete, headers, now); ok {
				result.Until, result.Cooldown = until, true
			}
		}
		return result, true
	}
	return ResponseRuleMatch{}, false
}

// MatchFailure applies the first enabled rule for a pre-response transport
// failure. Failure rules are deliberately separate from HTTP response rules:
// there is no response body or status to inspect, and callers must not retry
// after response bytes have been exposed downstream.
func (rules ResponseRules) MatchFailure(kind string, now time.Time) (ResponseRuleMatch, bool) {
	for _, rule := range rules {
		if !rule.Enabled || !rule.Failures.matches(kind) {
			continue
		}
		result := ResponseRuleMatch{RuleID: rule.ID, Action: rule.Action, Scope: rule.Scope}
		if actionNeedsCooldown(rule.Action) {
			if until, ok := responseRuleFallback(rule.Fallback, rule.FixedDuration, now); ok {
				result.Until, result.Cooldown = until, true
			}
		}
		return result, true
	}
	return ResponseRuleMatch{}, false
}

func (matcher responseFailureMatcher) matches(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	for _, wanted := range matcher {
		if wanted == kind {
			return true
		}
		if (wanted == "timeout" || wanted == "transport_timeout") && isResponseTimeoutFailure(kind) {
			return true
		}
		if (wanted == "first_byte_timeout" && kind == "response_header_timeout") ||
			(wanted == "response_header_timeout" && kind == "first_byte_timeout") {
			// No separate response-header milestone exists yet: both names mean
			// the request was written and no first response byte arrived.
			return true
		}
	}
	return false
}

func isResponseTimeoutFailure(kind string) bool {
	switch kind {
	case "timeout", "transport_timeout", "connect_timeout", "response_header_timeout", "first_byte_timeout", "idle_timeout":
		return true
	default:
		return false
	}
}

func (m responseStatusMatcher) matches(statusCode int) bool {
	for _, code := range m.codes {
		if code == statusCode {
			return true
		}
	}
	for _, r := range m.ranges {
		if statusCode >= r.Min && statusCode <= r.Max {
			return true
		}
	}
	return false
}

func headersMatch(matchers []responseHeaderMatcher, headers http.Header) bool {
	for _, matcher := range matchers {
		values := headers.Values(matcher.name)
		present := len(values) != 0
		switch matcher.op {
		case "exists":
			if !present {
				return false
			}
		case "absent":
			if present {
				return false
			}
		case "regex", "contains":
			matched := false
			for _, value := range values {
				if (matcher.re != nil && matcher.re.MatchString(value)) || (matcher.re == nil && strings.Contains(value, matcher.value)) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case "not_regex", "not_contains":
			if !present {
				return false
			}
			for _, value := range values {
				if (matcher.re != nil && matcher.re.MatchString(value)) || (matcher.re == nil && strings.Contains(value, matcher.value)) {
					return false
				}
			}
		}
	}
	return true
}

func bodyMatches(matcher *responseBodyMatcher, body []byte, complete bool) bool {
	if matcher == nil {
		return true
	}
	if matcher.op == "not_regex" || matcher.op == "not_contains" {
		if !complete {
			return false
		}
		if matcher.re != nil {
			return !matcher.re.Match(body)
		}
		return !strings.Contains(string(body), matcher.value)
	}
	if matcher.re != nil {
		return matcher.re.Match(body)
	}
	return strings.Contains(string(body), matcher.value)
}

func responseRuleDeadline(rule ResponseRule, body []byte, bodyComplete bool, headers http.Header, now time.Time) (time.Time, bool) {
	for _, source := range rule.Sources {
		var raw string
		var ok bool
		switch source.typ {
		case "header":
			raw, ok = singleResponseHeader(headers, source.header)
		case "json_pointer":
			if bodyComplete {
				raw, ok = jsonPointerScalar(body, source.pointer)
			}
		case "body_regex":
			if source.re != nil {
				match := source.re.FindSubmatch(body)
				if len(match) > source.capture {
					raw, ok = string(match[source.capture]), true
				}
			}
		case "retry_after":
			raw, ok = singleResponseHeader(headers, "Retry-After")
		}
		if ok {
			var until time.Time
			var valid bool
			// retry_after is an HTTP-defined source. Older persisted rules may
			// still carry format:"delta_seconds", but that legacy field must not
			// disable HTTP-date parsing.
			if source.typ == "retry_after" {
				until, valid = parseRetryAfter(raw, now)
			} else {
				until, valid = parseResponseExpiry(raw, source.format, now)
			}
			if valid {
				return until, true
			}
		}
	}
	return responseRuleFallback(rule.Fallback, rule.FixedDuration, now)
}

func responseRuleFallback(fallback ResponseExpiryFallback, fixed time.Duration, now time.Time) (time.Time, bool) {
	switch fallback {
	case ResponseExpiryFallbackNextUTCMidnight:
		utc := now.UTC()
		until := time.Date(utc.Year(), utc.Month(), utc.Day()+1, 0, 0, 0, 0, time.UTC)
		return until, until.After(now)
	case ResponseExpiryFallbackFixedDuration:
		return validateResponseExpiry(now.Add(fixed), true, now)
	default:
		return time.Time{}, false
	}
}

func singleResponseHeader(headers http.Header, name string) (string, bool) {
	if headers == nil || name == "" {
		return "", false
	}
	values := headers.Values(name)
	if len(values) != 1 {
		return "", false
	}
	value := strings.TrimSpace(values[0])
	if value == "" || len(value) > responseRuleMaxExpiryBytes {
		return "", false
	}
	return value, true
}

func parseRetryAfter(raw string, now time.Time) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds >= 0 {
		if seconds > int64(responseRuleMaxFuture/time.Second) {
			return time.Time{}, false
		}
		return validateResponseExpiry(addResponseExpiryDelta(now, time.Duration(seconds)*time.Second), true, now)
	}
	when, err := http.ParseTime(raw)
	return validateResponseExpiry(when, err == nil, now)
}

func parseResponseExpiry(raw, format string, now time.Time) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > responseRuleMaxExpiryBytes {
		return time.Time{}, false
	}
	switch format {
	case "rfc3339_utc":
		if !strings.HasSuffix(raw, "Z") {
			return time.Time{}, false
		}
		when, err := time.Parse(time.RFC3339Nano, raw)
		return validateResponseExpiry(when, err == nil, now)
	case "unix_seconds":
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || seconds < 0 {
			return time.Time{}, false
		}
		return validateResponseExpiry(time.Unix(seconds, 0), true, now)
	case "unix_millis":
		milliseconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || milliseconds < 0 {
			return time.Time{}, false
		}
		return validateResponseExpiry(time.UnixMilli(milliseconds), true, now)
	case "delta_seconds":
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || seconds <= 0 || seconds > int64(responseRuleMaxFuture/time.Second) {
			return time.Time{}, false
		}
		return validateResponseExpiry(addResponseExpiryDelta(now, time.Duration(seconds)*time.Second), true, now)
	default:
		return time.Time{}, false
	}
}

func addResponseExpiryDelta(now time.Time, delta time.Duration) time.Time {
	// delta is bounded by responseRuleMaxFuture before this helper is called;
	// time.Duration multiplication therefore cannot overflow, and time.Add
	// avoids Unix-second integer wraparound near the time.Time limits.
	return now.Add(delta)
}

func validateResponseExpiry(when time.Time, parsed bool, now time.Time) (time.Time, bool) {
	if !parsed || !when.After(now) {
		return time.Time{}, false
	}
	maxUntil := now.Add(responseRuleMaxFuture)
	if !maxUntil.After(now) || when.After(maxUntil) {
		return time.Time{}, false
	}
	return when, true
}

func jsonPointerScalar(body []byte, pointer string) (string, bool) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", false
	}
	for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[token]
			if !ok {
				return "", false
			}
		case []any:
			index, ok := jsonPointerArrayIndex(token, len(current))
			if !ok {
				return "", false
			}
			value = current[index]
		default:
			return "", false
		}
	}
	switch scalar := value.(type) {
	case string:
		return scalar, scalar != ""
	case json.Number:
		return scalar.String(), true
	case bool:
		return strconv.FormatBool(scalar), true
	default:
		return "", false
	}
}

func jsonPointerArrayIndex(token string, length int) (int, bool) {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return 0, false
	}
	for i := 0; i < len(token); i++ {
		if token[i] < '0' || token[i] > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(token)
	return index, err == nil && index < length
}
