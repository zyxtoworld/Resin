package service

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
	"golang.org/x/net/http/httpguts"
)

// ------------------------------------------------------------------
// Account Header Rules
// ------------------------------------------------------------------

// RuleResponse is the API response for an account header rule.
type RuleResponse struct {
	URLPrefix string   `json:"url_prefix"`
	Headers   []string `json:"headers"`
	UpdatedAt string   `json:"updated_at"`
}

const fallbackRulePrefix = "*"

func normalizeRulePrefix(prefix string) (string, *ServiceError) {
	normalized, err := proxy.NormalizeRulePrefix(prefix)
	if err != nil {
		return "", invalidArg(err.Error())
	}
	return normalized, nil
}

func ruleToResponse(r model.AccountHeaderRule) (RuleResponse, error) {
	headers := append([]string(nil), r.Headers...)
	for i, h := range headers {
		if strings.TrimSpace(h) == "" {
			return RuleResponse{}, fmt.Errorf("invalid headers[%d]: empty header name", i)
		}
		if !httpguts.ValidHeaderFieldName(h) {
			return RuleResponse{}, fmt.Errorf("invalid headers[%d] %q", i, h)
		}
	}
	if headers == nil {
		headers = []string{}
	}
	return RuleResponse{
		URLPrefix: r.URLPrefix,
		Headers:   headers,
		UpdatedAt: time.Unix(0, r.UpdatedAtNs).UTC().Format(time.RFC3339Nano),
	}, nil
}

// withRuleMutation keeps state-db admission active through the matching
// immutable matcher publish. Shutdown must not observe an idle state database
// while the same rule mutation can still change request-time behavior.
func (s *ControlPlaneService) withRuleMutation(fn func() error) error {
	err := s.Engine.WithStateWriteAdmission(fn)
	if errors.Is(err, state.ErrStateWriteAdmissionClosed) {
		return internal("rule mutation", err)
	}
	return err
}

// ListAccountHeaderRules returns all rules.
func (s *ControlPlaneService) ListAccountHeaderRules() ([]RuleResponse, error) {
	rules, err := s.Engine.ListAccountHeaderRules()
	if err != nil {
		return nil, internal("list rules", err)
	}
	resp := make([]RuleResponse, len(rules))
	for i, r := range rules {
		ruleResp, err := ruleToResponse(r)
		if err != nil {
			return nil, internal(fmt.Sprintf("decode rule %q", r.URLPrefix), err)
		}
		resp[i] = ruleResp
	}
	return resp, nil
}

// UpsertAccountHeaderRule creates or updates a rule. Returns (response, created, error).
func (s *ControlPlaneService) UpsertAccountHeaderRule(prefix string, headers []string) (*RuleResponse, bool, error) {
	normalizedPrefix, verr := normalizeRulePrefix(prefix)
	if verr != nil {
		return nil, false, verr
	}
	if len(headers) == 0 {
		return nil, false, invalidArg("headers: must be a non-empty array")
	}
	// Validate header names (RFC 7230 token chars).
	for i, h := range headers {
		if strings.TrimSpace(h) == "" {
			return nil, false, invalidArg(fmt.Sprintf("headers[%d]: must be non-empty", i))
		}
		if !httpguts.ValidHeaderFieldName(h) {
			return nil, false, invalidArg(fmt.Sprintf("headers[%d]: %q is not a valid HTTP header name (RFC 7230 token)", i, h))
		}
	}

	now := time.Now().UnixNano()

	rule := model.AccountHeaderRule{
		URLPrefix:   normalizedPrefix,
		Headers:     append([]string(nil), headers...),
		UpdatedAtNs: now,
	}

	s.runRuleMutationHook(ruleMutationBeforeLock)
	var response *RuleResponse
	var created bool
	err := s.withRuleMutation(func() error {
		s.ruleMu.Lock()
		defer s.ruleMu.Unlock()

		rules, err := s.Engine.ListAccountHeaderRules()
		if err != nil {
			return internal("load rules", err)
		}
		candidate := make([]model.AccountHeaderRule, 0, len(rules)+1)
		createdFromSnapshot := true
		for _, current := range rules {
			if current.URLPrefix == rule.URLPrefix {
				candidate = append(candidate, rule)
				createdFromSnapshot = false
				continue
			}
			candidate = append(candidate, current)
		}
		if createdFromSnapshot {
			candidate = append(candidate, rule)
		}
		s.runRuleMutationHook(ruleMutationAfterSnapshot)

		created, err = s.Engine.UpsertAccountHeaderRuleWithCreated(rule)
		if err != nil {
			return internal("persist rule", err)
		}
		s.runRuleMutationHook(ruleMutationAfterPersist)

		// ReplaceRules only builds an immutable matcher and swaps one atomic
		// pointer; it cannot fail. The candidate was fully loaded and constructed
		// before the database write, so no post-persist DB read can split runtime
		// from the committed row.
		s.publishAccountMatcherRules(candidate)

		r, err := ruleToResponse(rule)
		if err != nil {
			return internal(fmt.Sprintf("decode rule %q", rule.URLPrefix), err)
		}
		response = &r
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return response, created, nil
}

// DeleteAccountHeaderRule deletes a rule.
func (s *ControlPlaneService) DeleteAccountHeaderRule(prefix string) error {
	normalizedPrefix, verr := normalizeRulePrefix(prefix)
	if verr != nil {
		return verr
	}
	if normalizedPrefix == fallbackRulePrefix {
		return invalidArg(`fallback rule "*" cannot be deleted`)
	}

	s.runRuleMutationHook(ruleMutationBeforeLock)
	return s.withRuleMutation(func() error {
		s.ruleMu.Lock()
		defer s.ruleMu.Unlock()

		rules, err := s.Engine.ListAccountHeaderRules()
		if err != nil {
			return internal("load rules", err)
		}
		candidate := make([]model.AccountHeaderRule, 0, len(rules))
		found := false
		for _, current := range rules {
			if current.URLPrefix == normalizedPrefix {
				found = true
				continue
			}
			candidate = append(candidate, current)
		}
		if !found {
			return notFound("rule not found")
		}
		s.runRuleMutationHook(ruleMutationAfterSnapshot)

		if err := s.Engine.DeleteAccountHeaderRule(normalizedPrefix); err != nil {
			if errors.Is(err, state.ErrNotFound) {
				return notFound("rule not found")
			}
			return internal("delete rule", err)
		}
		s.runRuleMutationHook(ruleMutationAfterPersist)
		s.publishAccountMatcherRules(candidate)
		return nil
	})
}

// publishAccountMatcherRules atomically publishes a fully constructed rule
// snapshot. Building the matcher cannot fail, so callers must construct the
// candidate before committing the corresponding database mutation.
func (s *ControlPlaneService) publishAccountMatcherRules(rules []model.AccountHeaderRule) {
	if s.MatcherRuntime == nil {
		return
	}
	s.MatcherRuntime.ReplaceRules(rules)
}

// ResolveResult is the API response for rule resolution.
type ResolveResult struct {
	MatchedURLPrefix string   `json:"matched_url_prefix"`
	Headers          []string `json:"headers"`
}

// ResolveAccountHeaderRule resolves a URL against account header rules.
func (s *ControlPlaneService) ResolveAccountHeaderRule(rawURL string) (*ResolveResult, error) {
	if rawURL == "" {
		return nil, invalidArg("url is required")
	}
	u, verr := parseHTTPAbsoluteURL("url", rawURL)
	if verr != nil {
		return nil, verr
	}

	if s.MatcherRuntime == nil {
		return nil, internal("matcher not configured", nil)
	}
	matchedPrefix, headers := s.MatcherRuntime.MatchWithPrefix(u.Host, u.EscapedPath())
	if headers == nil {
		return &ResolveResult{}, nil
	}

	return &ResolveResult{
		MatchedURLPrefix: matchedPrefix,
		Headers:          headers,
	}, nil
}
