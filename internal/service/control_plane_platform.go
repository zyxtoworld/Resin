package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

// ------------------------------------------------------------------
// Platform
// ------------------------------------------------------------------

// PlatformResponse is the API response model for a platform.
type PlatformResponse struct {
	ID                               string                       `json:"id"`
	Name                             string                       `json:"name"`
	StickyTTL                        string                       `json:"sticky_ttl"`
	RegexFilters                     []string                     `json:"regex_filters"`
	RegionFilters                    []string                     `json:"region_filters"`
	ResponseRules                    []model.PlatformResponseRule `json:"response_rules"`
	RoutableNodeCount                int                          `json:"routable_node_count"`
	ReverseProxyMissAction           string                       `json:"reverse_proxy_miss_action"`
	ReverseProxyEmptyAccountBehavior string                       `json:"reverse_proxy_empty_account_behavior"`
	ReverseProxyFixedAccountHeader   string                       `json:"reverse_proxy_fixed_account_header"`
	AllocationPolicy                 string                       `json:"allocation_policy"`
	PassiveCircuitBreakerDisabled    bool                         `json:"passive_circuit_breaker_disabled"`
	ProxyRequestTotalTimeout         string                       `json:"proxy_request_total_timeout"`
	ProxyRequestAttemptTimeout       string                       `json:"proxy_request_attempt_timeout"`
	ProxyRequestMaxAttempts          int                          `json:"proxy_request_max_attempts"`
	UpdatedAt                        string                       `json:"updated_at"`
}

func platformToResponse(p model.Platform) PlatformResponse {
	behavior := normalizePlatformEmptyAccountBehavior(p.ReverseProxyEmptyAccountBehavior)
	fixedHeader := normalizeHeaderFieldName(p.ReverseProxyFixedAccountHeader)
	responseRules := append([]model.PlatformResponseRule{}, p.ResponseRules...)
	return PlatformResponse{
		ID:                               p.ID,
		Name:                             p.Name,
		StickyTTL:                        time.Duration(p.StickyTTLNs).String(),
		RegexFilters:                     append([]string(nil), p.RegexFilters...),
		RegionFilters:                    append([]string(nil), p.RegionFilters...),
		ResponseRules:                    responseRules,
		RoutableNodeCount:                0,
		ReverseProxyMissAction:           p.ReverseProxyMissAction,
		ReverseProxyEmptyAccountBehavior: behavior,
		ReverseProxyFixedAccountHeader:   fixedHeader,
		AllocationPolicy:                 p.AllocationPolicy,
		PassiveCircuitBreakerDisabled:    p.PassiveCircuitBreakerDisabled,
		ProxyRequestTotalTimeout:         platformProxyRequestTotalTimeoutString(p.ProxyRequestTotalTimeoutNs),
		ProxyRequestAttemptTimeout:       platformProxyRequestTotalTimeoutString(p.ProxyRequestAttemptTimeoutNs),
		ProxyRequestMaxAttempts:          p.ProxyRequestMaxAttempts,
		UpdatedAt:                        time.Unix(0, p.UpdatedAtNs).UTC().Format(time.RFC3339Nano),
	}
}

func platformProxyRequestTotalTimeoutString(ns int64) string {
	if ns <= 0 {
		return ""
	}
	return time.Duration(ns).String()
}

func (s *ControlPlaneService) withRoutableNodeCount(resp PlatformResponse) PlatformResponse {
	var result PlatformResponse
	s.withRuntimeRead(func() {
		result = s.routableNodeCount(resp)
	})
	return result
}

func (s *ControlPlaneService) routableNodeCount(resp PlatformResponse) PlatformResponse {
	if s == nil || s.Pool == nil {
		return resp
	}
	plat, ok := s.Pool.GetPlatform(resp.ID)
	if !ok || plat == nil {
		return resp
	}
	resp.RoutableNodeCount = plat.View().Size()
	return resp
}

type platformConfig struct {
	Name                             string
	StickyTTLNs                      int64
	RegexFilters                     []string
	RegionFilters                    []string
	ResponseRules                    []model.PlatformResponseRule
	ReverseProxyMissAction           string
	ReverseProxyEmptyAccountBehavior string
	ReverseProxyFixedAccountHeader   string
	AllocationPolicy                 string
	PassiveCircuitBreakerDisabled    bool
	ProxyRequestTotalTimeoutNs       int64
	ProxyRequestAttemptTimeoutNs     int64
	ProxyRequestMaxAttempts          int
}

func normalizePlatformMissAction(raw string) string {
	normalized := platform.NormalizeReverseProxyMissAction(raw)
	if normalized == "" {
		return ""
	}
	return string(normalized)
}

func normalizePlatformEmptyAccountBehavior(raw string) string {
	if platform.ReverseProxyEmptyAccountBehavior(raw).IsValid() {
		return raw
	}
	return string(platform.ReverseProxyEmptyAccountBehaviorRandom)
}

func (s *ControlPlaneService) defaultPlatformConfig(name string) platformConfig {
	return platformConfig{
		Name:                   name,
		StickyTTLNs:            int64(s.EnvCfg.DefaultPlatformStickyTTL),
		RegexFilters:           append([]string(nil), s.EnvCfg.DefaultPlatformRegexFilters...),
		RegionFilters:          append([]string(nil), s.EnvCfg.DefaultPlatformRegionFilters...),
		ReverseProxyMissAction: s.EnvCfg.DefaultPlatformReverseProxyMissAction,
		ReverseProxyEmptyAccountBehavior: normalizePlatformEmptyAccountBehavior(
			s.EnvCfg.DefaultPlatformReverseProxyEmptyAccountBehavior,
		),
		ReverseProxyFixedAccountHeader: normalizeHeaderFieldName(
			s.EnvCfg.DefaultPlatformReverseProxyFixedAccountHeader,
		),
		AllocationPolicy:             s.EnvCfg.DefaultPlatformAllocationPolicy,
		ProxyRequestTotalTimeoutNs:   0,
		ProxyRequestAttemptTimeoutNs: 0,
		ProxyRequestMaxAttempts:      0,
	}
}

func platformConfigFromModel(mp model.Platform) platformConfig {
	return platformConfig{
		Name:                             mp.Name,
		StickyTTLNs:                      mp.StickyTTLNs,
		RegexFilters:                     append([]string(nil), mp.RegexFilters...),
		RegionFilters:                    append([]string(nil), mp.RegionFilters...),
		ResponseRules:                    append([]model.PlatformResponseRule(nil), mp.ResponseRules...),
		ReverseProxyMissAction:           mp.ReverseProxyMissAction,
		ReverseProxyEmptyAccountBehavior: normalizePlatformEmptyAccountBehavior(mp.ReverseProxyEmptyAccountBehavior),
		ReverseProxyFixedAccountHeader:   normalizeHeaderFieldName(mp.ReverseProxyFixedAccountHeader),
		AllocationPolicy:                 mp.AllocationPolicy,
		PassiveCircuitBreakerDisabled:    mp.PassiveCircuitBreakerDisabled,
		ProxyRequestTotalTimeoutNs:       mp.ProxyRequestTotalTimeoutNs,
		ProxyRequestAttemptTimeoutNs:     mp.ProxyRequestAttemptTimeoutNs,
		ProxyRequestMaxAttempts:          mp.ProxyRequestMaxAttempts,
	}
}

func (cfg platformConfig) toModel(id string, updatedAtNs int64) model.Platform {
	responseRules := append([]model.PlatformResponseRule{}, cfg.ResponseRules...)
	return model.Platform{
		ID:                               id,
		Name:                             cfg.Name,
		StickyTTLNs:                      cfg.StickyTTLNs,
		RegexFilters:                     append([]string(nil), cfg.RegexFilters...),
		RegionFilters:                    append([]string(nil), cfg.RegionFilters...),
		ResponseRules:                    responseRules,
		ReverseProxyMissAction:           cfg.ReverseProxyMissAction,
		ReverseProxyEmptyAccountBehavior: cfg.ReverseProxyEmptyAccountBehavior,
		ReverseProxyFixedAccountHeader:   cfg.ReverseProxyFixedAccountHeader,
		AllocationPolicy:                 cfg.AllocationPolicy,
		PassiveCircuitBreakerDisabled:    cfg.PassiveCircuitBreakerDisabled,
		ProxyRequestTotalTimeoutNs:       cfg.ProxyRequestTotalTimeoutNs,
		ProxyRequestAttemptTimeoutNs:     cfg.ProxyRequestAttemptTimeoutNs,
		ProxyRequestMaxAttempts:          cfg.ProxyRequestMaxAttempts,
		UpdatedAtNs:                      updatedAtNs,
	}
}

func (cfg platformConfig) toRuntime(id string) (*platform.Platform, error) {
	compiledRegexFilters, err := platform.CompileRegexFilters(cfg.RegexFilters)
	if err != nil {
		return nil, err
	}
	plat := platform.NewConfiguredPlatform(
		id,
		cfg.Name,
		compiledRegexFilters,
		cfg.RegionFilters,
		cfg.StickyTTLNs,
		cfg.ReverseProxyMissAction,
		cfg.ReverseProxyEmptyAccountBehavior,
		cfg.ReverseProxyFixedAccountHeader,
		cfg.AllocationPolicy,
		cfg.PassiveCircuitBreakerDisabled,
	)
	plat.ProxyRequestTotalTimeoutNs = cfg.ProxyRequestTotalTimeoutNs
	plat.ProxyRequestAttemptTimeoutNs = cfg.ProxyRequestAttemptTimeoutNs
	plat.ProxyRequestMaxAttempts = cfg.ProxyRequestMaxAttempts
	responseRules, err := platform.CompileResponseRules(id, cfg.ResponseRules)
	if err != nil {
		return nil, err
	}
	plat.ResponseRules = responseRules
	return plat, nil
}

func validatePlatformMissAction(raw string) *ServiceError {
	if normalizePlatformMissAction(raw) != "" {
		return nil
	}
	return invalidArg(fmt.Sprintf(
		"reverse_proxy_miss_action: must be %s or %s",
		platform.ReverseProxyMissActionTreatAsEmpty,
		platform.ReverseProxyMissActionReject,
	))
}

func validatePlatformEmptyAccountBehavior(raw string) *ServiceError {
	if platform.ReverseProxyEmptyAccountBehavior(raw).IsValid() {
		return nil
	}
	return invalidArg(fmt.Sprintf(
		"reverse_proxy_empty_account_behavior: must be %s, %s, or %s",
		platform.ReverseProxyEmptyAccountBehaviorRandom,
		platform.ReverseProxyEmptyAccountBehaviorFixedHeader,
		platform.ReverseProxyEmptyAccountBehaviorAccountHeaderRule,
	))
}

func normalizeHeaderFieldName(raw string) string {
	normalized, _, err := platform.NormalizeFixedAccountHeaders(raw)
	if err != nil {
		return strings.TrimSpace(raw)
	}
	return normalized
}

func validatePlatformEmptyAccountConfig(cfg *platformConfig) *ServiceError {
	if cfg == nil {
		return invalidArg("platform config is required")
	}
	if err := validatePlatformEmptyAccountBehavior(cfg.ReverseProxyEmptyAccountBehavior); err != nil {
		return err
	}
	normalizedFixedHeaders, fixedHeaders, err := platform.NormalizeFixedAccountHeaders(cfg.ReverseProxyFixedAccountHeader)
	if err != nil {
		return invalidArg("reverse_proxy_fixed_account_header: " + err.Error())
	}
	cfg.ReverseProxyFixedAccountHeader = normalizedFixedHeaders
	if cfg.ReverseProxyEmptyAccountBehavior == string(platform.ReverseProxyEmptyAccountBehaviorFixedHeader) &&
		len(fixedHeaders) == 0 {
		return invalidArg(
			"reverse_proxy_fixed_account_header: required when reverse_proxy_empty_account_behavior is FIXED_HEADER",
		)
	}
	return nil
}

func validatePlatformAllocationPolicy(raw string) *ServiceError {
	if platform.AllocationPolicy(raw).IsValid() {
		return nil
	}
	return invalidArg(fmt.Sprintf(
		"allocation_policy: must be %s, %s, or %s",
		platform.AllocationPolicyBalanced,
		platform.AllocationPolicyPreferLowLatency,
		platform.AllocationPolicyPreferIdleIP,
	))
}

func setPlatformStickyTTL(cfg *platformConfig, d time.Duration) *ServiceError {
	if d <= 0 {
		return invalidArg("sticky_ttl: must be > 0")
	}
	if _, err := platform.StickyLeaseExpiryUnixNano(time.Now(), int64(d)); err != nil {
		return invalidArg("sticky_ttl: " + err.Error())
	}
	cfg.StickyTTLNs = int64(d)
	return nil
}

func setPlatformMissAction(cfg *platformConfig, missAction string) *ServiceError {
	if err := validatePlatformMissAction(missAction); err != nil {
		return err
	}
	cfg.ReverseProxyMissAction = normalizePlatformMissAction(missAction)
	return nil
}

func setPlatformEmptyAccountBehavior(cfg *platformConfig, behavior string) *ServiceError {
	if err := validatePlatformEmptyAccountBehavior(behavior); err != nil {
		return err
	}
	cfg.ReverseProxyEmptyAccountBehavior = behavior
	return nil
}

func setPlatformAllocationPolicy(cfg *platformConfig, policy string) *ServiceError {
	if err := validatePlatformAllocationPolicy(policy); err != nil {
		return err
	}
	cfg.AllocationPolicy = policy
	return nil
}

func setPlatformProxyRequestTotalTimeout(cfg *platformConfig, d time.Duration) *ServiceError {
	if d < 0 {
		return invalidArg("proxy_request_total_timeout: must be non-negative")
	}
	if err := platform.ValidateProxyRequestTotalTimeoutNs(int64(d)); err != nil {
		return invalidArg(err.Error())
	}
	cfg.ProxyRequestTotalTimeoutNs = int64(d)
	return nil
}

func setPlatformProxyRequestAttemptTimeout(cfg *platformConfig, d time.Duration) *ServiceError {
	if d < 0 {
		return invalidArg("proxy_request_attempt_timeout: must be non-negative")
	}
	if err := platform.ValidateProxyRequestAttemptTimeoutNs(int64(d)); err != nil {
		return invalidArg(err.Error())
	}
	cfg.ProxyRequestAttemptTimeoutNs = int64(d)
	return nil
}

func setPlatformProxyRequestMaxAttempts(cfg *platformConfig, attempts int) *ServiceError {
	if err := platform.ValidateProxyRequestMaxAttempts(attempts); err != nil {
		return invalidArg(err.Error())
	}
	cfg.ProxyRequestMaxAttempts = attempts
	return nil
}

func validatePlatformConfig(cfg *platformConfig, validateRegionFilters bool) *ServiceError {
	if cfg == nil {
		return invalidArg("platform config is required")
	}
	if cfg.StickyTTLNs > 0 {
		if _, err := platform.StickyLeaseExpiryUnixNano(time.Now(), cfg.StickyTTLNs); err != nil {
			return invalidArg("sticky_ttl: " + err.Error())
		}
	}
	if validateRegionFilters {
		if err := platform.ValidateRegionFilters(cfg.RegionFilters); err != nil {
			return invalidArg(err.Error())
		}
	}
	if err := validatePlatformEmptyAccountConfig(cfg); err != nil {
		return err
	}
	if err := platform.ValidateProxyRequestTotalTimeoutNs(cfg.ProxyRequestTotalTimeoutNs); err != nil {
		return invalidArg(err.Error())
	}
	if err := platform.ValidateProxyRequestAttemptTimeoutNs(cfg.ProxyRequestAttemptTimeoutNs); err != nil {
		return invalidArg(err.Error())
	}
	if err := platform.ValidateProxyRequestMaxAttempts(cfg.ProxyRequestMaxAttempts); err != nil {
		return invalidArg(err.Error())
	}
	if _, err := platform.CompileResponseRules("config", cfg.ResponseRules); err != nil {
		return invalidArg(err.Error())
	}
	return nil
}

func (s *ControlPlaneService) compileAndPersistPlatform(
	ctx context.Context,
	id string,
	cfg platformConfig,
	persist func(context.Context, model.Platform) error,
) (model.Platform, *platform.Platform, *ServiceError) {
	if err := platform.ValidatePlatformName(cfg.Name); err != nil {
		return model.Platform{}, nil, invalidArg("name: " + err.Error())
	}

	plat, err := cfg.toRuntime(id)
	if err != nil {
		return model.Platform{}, nil, invalidArg(err.Error())
	}
	mp := cfg.toModel(id, time.Now().UnixNano())
	if err := persist(ctx, mp); err != nil {
		if errors.Is(err, state.ErrConflict) {
			return model.Platform{}, nil, conflict("platform name already exists")
		}
		if strings.HasPrefix(err.Error(), "platform name: ") {
			return model.Platform{}, nil, invalidArg("name: " + strings.TrimPrefix(err.Error(), "platform name: "))
		}
		return model.Platform{}, nil, internal("persist platform", err)
	}
	if hook := s.afterPlatformPersistHook; hook != nil {
		hook()
	}
	return mp, plat, nil
}

func (s *ControlPlaneService) compileAndUpsertPlatform(id string, cfg platformConfig) (model.Platform, *platform.Platform, *ServiceError) {
	return s.compileAndPersistPlatform(context.Background(), id, cfg, func(_ context.Context, p model.Platform) error {
		return s.Engine.UpsertPlatform(p)
	})
}

func (s *ControlPlaneService) compileAndInsertPlatform(id string, cfg platformConfig) (model.Platform, *platform.Platform, *ServiceError) {
	return s.compileAndPersistPlatform(context.Background(), id, cfg, func(_ context.Context, p model.Platform) error {
		return s.Engine.InsertPlatform(p)
	})
}

func (s *ControlPlaneService) compileAndUpsertPlatformContext(ctx context.Context, id string, cfg platformConfig) (model.Platform, *platform.Platform, *ServiceError) {
	return s.compileAndPersistPlatform(ctx, id, cfg, s.Engine.UpsertPlatformContextAndCommit)
}

func (s *ControlPlaneService) compileAndInsertPlatformContext(ctx context.Context, id string, cfg platformConfig) (model.Platform, *platform.Platform, *ServiceError) {
	return s.compileAndPersistPlatform(ctx, id, cfg, s.Engine.InsertPlatformContextAndCommit)
}

// validateRuntimePlatformReplacement checks every failure that a runtime
// replacement can report while the caller already owns the pool publication
// capability. Keeping the check inside that capability closes the preflight→
// DB→publish TOCTOU window.
func validateRuntimePlatformReplacement(mutation topology.PlatformMutation, id, name string) *ServiceError {
	if mutation == nil {
		return internal("platform runtime unavailable", errors.New("platform mutation owner is nil"))
	}
	switch err := mutation.ValidatePlatformReplacement(id, name); {
	case err == nil:
		return nil
	case errors.Is(err, topology.ErrPlatformNameConflict):
		return conflict("platform name already exists")
	case errors.Is(err, topology.ErrPlatformNotRegistered):
		return internal("platform runtime unavailable", errors.New("platform is not registered in the runtime pool"))
	default:
		return internal("platform runtime unavailable", err)
	}
}

// validateRuntimePlatformRegistration checks every failure that a new
// runtime registration can report while the caller owns the same pool
// publication capability that will perform the registration.
func validateRuntimePlatformRegistration(mutation topology.PlatformMutation, id, name string) *ServiceError {
	if mutation == nil {
		return internal("platform runtime unavailable", errors.New("platform mutation owner is nil"))
	}
	switch err := mutation.ValidatePlatformRegistration(id, name); {
	case err == nil:
		return nil
	case errors.Is(err, topology.ErrPlatformAlreadyRegistered):
		return conflict("platform runtime already exists")
	case errors.Is(err, topology.ErrPlatformNameConflict):
		return conflict("platform name already exists")
	default:
		return internal("platform runtime unavailable", err)
	}
}

// withPlatformMutationAdmission keeps the pool publication owner and strong
// state-write admission together. Request cancellation can abandon the pool
// owner before any DB write; once both owners are admitted, commitCtx carries
// the complete DB→pool publication transaction past client disconnects.
func (s *ControlPlaneService) withPlatformMutationAdmission(fn func(topology.PlatformMutation) *ServiceError) *ServiceError {
	return s.withPlatformMutationAdmissionContext(context.Background(), func(_, _ context.Context, mutation topology.PlatformMutation) *ServiceError {
		if fn == nil {
			return nil
		}
		return fn(mutation)
	})
}

func (s *ControlPlaneService) withPlatformMutationAdmissionContext(
	ctx context.Context,
	fn func(context.Context, context.Context, topology.PlatformMutation) *ServiceError,
) *ServiceError {
	if s == nil || s.Engine == nil {
		return internal("platform persistence unavailable", errors.New("state engine is nil"))
	}
	if s.Pool == nil {
		return internal("platform runtime unavailable", errors.New("platform pool is nil"))
	}
	if hook := s.beforePlatformRuntimeAdmissionHook; hook != nil {
		hook()
	}
	err := s.Pool.WithPlatformMutationContext(ctx, func(mutation topology.PlatformMutation) error {
		return s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(writeCtx, commitCtx context.Context) error {
			if fn == nil {
				return nil
			}
			if serviceErr := fn(writeCtx, commitCtx, mutation); serviceErr != nil {
				return serviceErr
			}
			return nil
		})
	})
	if err == nil {
		return nil
	}
	var serviceErr *ServiceError
	if errors.As(err, &serviceErr) {
		return serviceErr
	}
	return internal("platform mutation", err)
}

// ListPlatforms returns all platforms from the database.
func (s *ControlPlaneService) ListPlatforms() ([]PlatformResponse, error) {
	return s.ListPlatformsContext(context.Background())
}

// ListPlatformsContext returns all platforms from one persisted/runtime
// generation while honoring request cancellation before each blocking owner.
func (s *ControlPlaneService) ListPlatformsContext(ctx context.Context) ([]PlatformResponse, error) {
	if hook := s.beforePlatformReadHook; hook != nil {
		hook()
	}
	// The database model and the runtime routable count are one published
	// platform generation. Serialize this read with platform mutations so it
	// cannot observe the row after persist but the old pool view before publish.
	if err := s.platformMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.platformMu.Unlock()

	platforms, err := s.Engine.ListPlatformsContext(ctx)
	if err != nil {
		return nil, internal("list platforms", err)
	}
	var resp []PlatformResponse
	if err := s.withRuntimeReadContext(ctx, func() {
		resp = make([]PlatformResponse, len(platforms))
		for i, p := range platforms {
			resp[i] = s.routableNodeCount(platformToResponse(p))
		}
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ControlPlaneService) getPlatformModelContext(ctx context.Context, id string) (*model.Platform, error) {
	if s.beforePlatformModelReadHook != nil {
		s.beforePlatformModelReadHook(ctx)
	}
	p, err := s.Engine.GetPlatformContext(ctx, id)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, notFound("platform not found")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, internal("get platform", err)
	}
	return p, nil
}

func (s *ControlPlaneService) getPlatformNameContext(ctx context.Context, id string) (string, error) {
	if s.beforePlatformModelReadHook != nil {
		s.beforePlatformModelReadHook(ctx)
	}
	return s.Engine.GetPlatformNameContext(ctx, id)
}

// GetPlatform returns a single platform by ID.
func (s *ControlPlaneService) GetPlatform(id string) (*PlatformResponse, error) {
	return s.GetPlatformContext(context.Background(), id)
}

// GetPlatformContext returns one persisted/runtime platform generation while
// honoring request cancellation before each blocking owner.
func (s *ControlPlaneService) GetPlatformContext(ctx context.Context, id string) (*PlatformResponse, error) {
	if hook := s.beforePlatformReadHook; hook != nil {
		hook()
	}
	// Keep the persisted model and runtime projection in the same platform
	// generation as UpdatePlatform/CreatePlatform/ResetPlatformToDefault.
	if err := s.platformMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.platformMu.Unlock()

	mp, err := s.getPlatformModelContext(ctx, id)
	if err != nil {
		return nil, err
	}
	var r PlatformResponse
	if err := s.withRuntimeReadContext(ctx, func() {
		r = s.routableNodeCount(platformToResponse(*mp))
	}); err != nil {
		return nil, err
	}
	return &r, nil
}

// CreatePlatformRequest holds create platform parameters.
type CreatePlatformRequest struct {
	Name                             *string                      `json:"name"`
	StickyTTL                        *string                      `json:"sticky_ttl"`
	RegexFilters                     []string                     `json:"regex_filters"`
	RegionFilters                    []string                     `json:"region_filters"`
	ReverseProxyMissAction           *string                      `json:"reverse_proxy_miss_action"`
	ReverseProxyEmptyAccountBehavior *string                      `json:"reverse_proxy_empty_account_behavior"`
	ReverseProxyFixedAccountHeader   *string                      `json:"reverse_proxy_fixed_account_header"`
	AllocationPolicy                 *string                      `json:"allocation_policy"`
	PassiveCircuitBreakerDisabled    *bool                        `json:"passive_circuit_breaker_disabled"`
	ProxyRequestTotalTimeout         *string                      `json:"proxy_request_total_timeout"`
	ProxyRequestAttemptTimeout       *string                      `json:"proxy_request_attempt_timeout"`
	ProxyRequestMaxAttempts          *int                         `json:"proxy_request_max_attempts"`
	ResponseRules                    []model.PlatformResponseRule `json:"response_rules"`
}

// CreatePlatform creates a new platform.
func (s *ControlPlaneService) CreatePlatform(req CreatePlatformRequest) (*PlatformResponse, error) {
	return s.CreatePlatformContext(context.Background(), req)
}

// CreatePlatformContext creates a new platform while honoring ctx for the
// persistence and runtime-publish transaction.
func (s *ControlPlaneService) CreatePlatformContext(ctx context.Context, req CreatePlatformRequest) (*PlatformResponse, error) {
	if err := s.platformMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.platformMu.Unlock()

	// Validate name.
	if req.Name == nil {
		return nil, invalidArg("name is required")
	}
	name := platform.NormalizePlatformName(*req.Name)
	if name == "" {
		return nil, invalidArg("name is required")
	}
	if err := platform.ValidatePlatformName(name); err != nil {
		return nil, invalidArg("name: " + err.Error())
	}
	if name == platform.DefaultPlatformName {
		return nil, conflict("cannot use reserved name 'Default'")
	}

	// Apply defaults from env and overlay request fields.
	cfg := s.defaultPlatformConfig(name)
	if req.StickyTTL != nil {
		d, err := time.ParseDuration(*req.StickyTTL)
		if err != nil {
			return nil, invalidArg("sticky_ttl: " + err.Error())
		}
		if err := setPlatformStickyTTL(&cfg, d); err != nil {
			return nil, err
		}
	}
	if req.RegexFilters != nil {
		cfg.RegexFilters = req.RegexFilters
	}
	if req.RegionFilters != nil {
		cfg.RegionFilters = req.RegionFilters
	}
	if req.ResponseRules != nil {
		cfg.ResponseRules = req.ResponseRules
	}
	if req.ReverseProxyMissAction != nil {
		if err := setPlatformMissAction(&cfg, *req.ReverseProxyMissAction); err != nil {
			return nil, err
		}
	}
	if req.ReverseProxyEmptyAccountBehavior != nil {
		if err := setPlatformEmptyAccountBehavior(&cfg, *req.ReverseProxyEmptyAccountBehavior); err != nil {
			return nil, err
		}
	}
	if req.ReverseProxyFixedAccountHeader != nil {
		cfg.ReverseProxyFixedAccountHeader = *req.ReverseProxyFixedAccountHeader
	}
	if req.AllocationPolicy != nil {
		if err := setPlatformAllocationPolicy(&cfg, *req.AllocationPolicy); err != nil {
			return nil, err
		}
	}
	if req.PassiveCircuitBreakerDisabled != nil {
		cfg.PassiveCircuitBreakerDisabled = *req.PassiveCircuitBreakerDisabled
	}
	if req.ProxyRequestTotalTimeout != nil {
		d, err := time.ParseDuration(strings.TrimSpace(*req.ProxyRequestTotalTimeout))
		if err != nil {
			return nil, invalidArg("proxy_request_total_timeout: " + err.Error())
		}
		if err := setPlatformProxyRequestTotalTimeout(&cfg, d); err != nil {
			return nil, err
		}
	}
	if req.ProxyRequestAttemptTimeout != nil {
		d, err := time.ParseDuration(strings.TrimSpace(*req.ProxyRequestAttemptTimeout))
		if err != nil {
			return nil, invalidArg("proxy_request_attempt_timeout: " + err.Error())
		}
		if err := setPlatformProxyRequestAttemptTimeout(&cfg, d); err != nil {
			return nil, err
		}
	}
	if req.ProxyRequestMaxAttempts != nil {
		if err := setPlatformProxyRequestMaxAttempts(&cfg, *req.ProxyRequestMaxAttempts); err != nil {
			return nil, err
		}
	}
	if err := validatePlatformConfig(&cfg, true); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	var mp model.Platform
	var plat *platform.Platform
	if svcErr := s.withPlatformMutationAdmissionContext(ctx, func(writeCtx, _ context.Context, mutation topology.PlatformMutation) *ServiceError {
		if err := validateRuntimePlatformRegistration(mutation, id, cfg.Name); err != nil {
			return err
		}
		var err *ServiceError
		mp, plat, err = s.compileAndInsertPlatformContext(writeCtx, id, cfg)
		if err != nil {
			return err
		}

		// Register in topology pool; registration rebuilds and publishes the
		// view under the pool's platform mutation lock.
		if err := mutation.RegisterPlatform(plat); err != nil {
			// The database write has already committed. Rollback must not be
			// canceled with the request, or a runtime publish failure could
			// leave a persisted platform with no runtime owner.
			if rollbackErr := s.Engine.DeletePlatform(id); rollbackErr != nil {
				return internal("register platform in pool", errors.Join(err, rollbackErr))
			}
			return internal("register platform in pool", err)
		}
		return nil
	}); svcErr != nil {
		return nil, svcErr
	}

	r := s.withRoutableNodeCount(platformToResponse(mp))
	return &r, nil
}

// UpdatePlatform applies a constrained partial patch to a platform.
// This is not RFC 7396 JSON Merge Patch: patch must be a non-empty object and
// null values are rejected.
func (s *ControlPlaneService) UpdatePlatform(id string, patchJSON json.RawMessage) (*PlatformResponse, error) {
	return s.UpdatePlatformContext(context.Background(), id, patchJSON)
}

// UpdatePlatformContext applies a constrained partial patch while honoring
// ctx for the persistence phase of the mutation.
func (s *ControlPlaneService) UpdatePlatformContext(ctx context.Context, id string, patchJSON json.RawMessage) (*PlatformResponse, error) {
	s.runPlatformMutationHook(platformMutationBeforeLock)
	if err := s.platformMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.platformMu.Unlock()

	patch, verr := parseMergePatch(patchJSON)
	if verr != nil {
		return nil, verr
	}
	if err := patch.validateFields(platformPatchAllowedFields, func(key string) string {
		return fmt.Sprintf("field %q is read-only or unknown", key)
	}); err != nil {
		return nil, err
	}

	// Load current.
	current, err := s.getPlatformModelContext(ctx, id)
	if err != nil {
		return nil, err
	}
	s.runPlatformMutationHook(platformMutationAfterLoad)
	if current.ID == platform.DefaultPlatformID {
		if nameVal, ok := patch["name"]; ok {
			nameStr, _ := nameVal.(string)
			if nameStr != platform.DefaultPlatformName {
				return nil, conflict("cannot rename Default platform")
			}
		}
	}

	cfg := platformConfigFromModel(*current)

	// Apply patch to current config.
	if nameStr, ok, err := patch.optionalNonEmptyString("name"); err != nil {
		return nil, err
	} else if ok {
		cfg.Name = platform.NormalizePlatformName(nameStr)
		if err := platform.ValidatePlatformName(cfg.Name); err != nil {
			return nil, invalidArg("name: " + err.Error())
		}
		if cfg.Name == platform.DefaultPlatformName && current.ID != platform.DefaultPlatformID {
			return nil, conflict("cannot use reserved name 'Default'")
		}
	}

	if d, ok, err := patch.optionalDurationString("sticky_ttl"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformStickyTTL(&cfg, d); err != nil {
			return nil, err
		}
	}

	if filters, ok, err := patch.optionalStringSlice("regex_filters"); err != nil {
		return nil, err
	} else if ok {
		cfg.RegexFilters = filters
	}

	regionFiltersPatched := false
	if filters, ok, err := patch.optionalStringSlice("region_filters"); err != nil {
		return nil, err
	} else if ok {
		regionFiltersPatched = true
		cfg.RegionFilters = filters
	}
	if rules, ok, err := patch.optionalResponseRules("response_rules"); err != nil {
		return nil, err
	} else if ok {
		cfg.ResponseRules = rules
	}

	if ma, ok, err := patch.optionalString("reverse_proxy_miss_action"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformMissAction(&cfg, ma); err != nil {
			return nil, err
		}
	}
	if behavior, ok, err := patch.optionalString("reverse_proxy_empty_account_behavior"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformEmptyAccountBehavior(&cfg, behavior); err != nil {
			return nil, err
		}
	}
	if fixedHeader, ok, err := patch.optionalString("reverse_proxy_fixed_account_header"); err != nil {
		return nil, err
	} else if ok {
		cfg.ReverseProxyFixedAccountHeader = fixedHeader
	}

	if ap, ok, err := patch.optionalString("allocation_policy"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformAllocationPolicy(&cfg, ap); err != nil {
			return nil, err
		}
	}
	if disabled, ok, err := patch.optionalBool("passive_circuit_breaker_disabled"); err != nil {
		return nil, err
	} else if ok {
		cfg.PassiveCircuitBreakerDisabled = disabled
	}
	if d, ok, err := patch.optionalNonNegativeDurationString("proxy_request_total_timeout"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformProxyRequestTotalTimeout(&cfg, d); err != nil {
			return nil, err
		}
	}
	if d, ok, err := patch.optionalNonNegativeDurationString("proxy_request_attempt_timeout"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformProxyRequestAttemptTimeout(&cfg, d); err != nil {
			return nil, err
		}
	}
	if attempts, ok, err := patch.optionalInt("proxy_request_max_attempts"); err != nil {
		return nil, err
	} else if ok {
		if err := setPlatformProxyRequestMaxAttempts(&cfg, attempts); err != nil {
			return nil, err
		}
	}
	if err := validatePlatformConfig(&cfg, regionFiltersPatched); err != nil {
		return nil, err
	}
	var mp model.Platform
	var plat *platform.Platform
	if svcErr := s.withPlatformMutationAdmissionContext(ctx, func(writeCtx, _ context.Context, mutation topology.PlatformMutation) *ServiceError {
		if err := validateRuntimePlatformReplacement(mutation, id, cfg.Name); err != nil {
			return err
		}
		var err *ServiceError
		mp, plat, err = s.compileAndUpsertPlatformContext(writeCtx, id, cfg)
		if err != nil {
			return err
		}

		// Replace in topology pool.
		if err := mutation.ReplacePlatform(plat); err != nil {
			return internal("replace platform in pool", err)
		}
		return nil
	}); svcErr != nil {
		return nil, svcErr
	}

	r := s.withRoutableNodeCount(platformToResponse(mp))
	return &r, nil
}

// DeletePlatform deletes a platform.
func (s *ControlPlaneService) DeletePlatform(id string) error {
	return s.DeletePlatformContext(context.Background(), id)
}

// DeletePlatformContext deletes a platform while honoring ctx for the
// persistence phase. Once the row is deleted, runtime cleanup still runs to
// preserve the single-generation mutation contract.
func (s *ControlPlaneService) DeletePlatformContext(ctx context.Context, id string) error {
	s.runPlatformMutationHook(platformMutationBeforeLock)
	if err := s.platformMu.lockContext(ctx); err != nil {
		return err
	}
	defer s.platformMu.Unlock()
	if s.Engine == nil {
		return internal("platform persistence unavailable", errors.New("state engine is nil"))
	}
	if s.Pool == nil {
		return internal("platform runtime unavailable", errors.New("platform pool is nil"))
	}
	if s.Router == nil {
		return internal("platform runtime unavailable", errors.New("router is nil"))
	}

	if id == platform.DefaultPlatformID {
		return conflict("cannot delete Default platform")
	}
	if hook := s.beforePlatformRuntimeAdmissionHook; hook != nil {
		hook()
	}
	err := s.Pool.WithPlatformMutationContext(ctx, func(mutation topology.PlatformMutation) error {
		if err := mutation.ValidatePlatformReplacement(id, ""); err != nil {
			if errors.Is(err, topology.ErrPlatformNotRegistered) {
				if _, nameErr := s.getPlatformNameContext(ctx, id); nameErr != nil {
					if errors.Is(nameErr, state.ErrNotFound) {
						return notFound("platform not found")
					}
					return internal("get platform", nameErr)
				}
				return internal(
					"platform runtime unavailable",
					errors.New("platform is not registered in the runtime pool"),
				)
			}
			return internal("platform runtime unavailable", err)
		}

		return s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(writeCtx, _ context.Context) error {
			if err := s.Engine.DeletePlatformContextAndCommit(writeCtx, id); err != nil {
				if errors.Is(err, state.ErrNotFound) {
					return notFound("platform not found")
				}
				return internal("delete platform", err)
			}
			if err := mutation.UnregisterPlatform(id); err != nil {
				return internal("unregister platform in pool", err)
			}
			if s.afterPlatformUnregisterHook != nil {
				s.afterPlatformUnregisterHook()
			}
			s.Router.RemovePlatformState(id)
			s.Router.WaitForLeaseEvents()
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, state.ErrStateWriteAdmissionClosed) {
			return internal("delete platform", err)
		}
		return err
	}
	return nil
}

// ResetPlatformToDefault resets a platform to env defaults.
func (s *ControlPlaneService) ResetPlatformToDefault(id string) (*PlatformResponse, error) {
	return s.ResetPlatformToDefaultContext(context.Background(), id)
}

// ResetPlatformToDefaultContext resets a platform to env defaults while
// honoring ctx for the persistence phase.
func (s *ControlPlaneService) ResetPlatformToDefaultContext(ctx context.Context, id string) (*PlatformResponse, error) {
	if err := s.platformMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.platformMu.Unlock()

	name, err := s.getPlatformNameContext(ctx, id)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, notFound("platform not found")
		}
		return nil, internal("get platform", err)
	}

	cfg := s.defaultPlatformConfig(name)
	if err := platform.ValidatePlatformName(cfg.Name); err != nil {
		return nil, invalidArg("name: " + err.Error())
	}
	var mp model.Platform
	var plat *platform.Platform
	if svcErr := s.withPlatformMutationAdmissionContext(ctx, func(writeCtx, _ context.Context, mutation topology.PlatformMutation) *ServiceError {
		if err := validateRuntimePlatformReplacement(mutation, id, cfg.Name); err != nil {
			return err
		}
		var err *ServiceError
		mp, plat, err = s.compileAndUpsertPlatformContext(writeCtx, id, cfg)
		if err != nil {
			return err
		}

		if err := mutation.ReplacePlatform(plat); err != nil {
			return internal("replace platform in pool", err)
		}
		return nil
	}); svcErr != nil {
		return nil, svcErr
	}

	r := s.withRoutableNodeCount(platformToResponse(mp))
	return &r, nil
}

// RebuildPlatformView triggers a full rebuild of the platform's routable view.
func (s *ControlPlaneService) RebuildPlatformView(id string) error {
	return s.RebuildPlatformViewContext(context.Background(), id)
}

// RebuildPlatformViewContext triggers a full rebuild while allowing an HTTP
// caller to abandon runtime-batch admission when its request is canceled.
func (s *ControlPlaneService) RebuildPlatformViewContext(ctx context.Context, id string) error {
	if s.beforePlatformRebuildHook != nil {
		s.beforePlatformRebuildHook()
	}
	var rebuildErr error
	if err := s.Pool.WithRuntimeReadContext(ctx, func() {
		plat, ok := s.Pool.GetPlatform(id)
		if !ok {
			rebuildErr = notFound("platform not found")
			return
		}
		rebuilt, err := s.Pool.RebuildPlatformIfCurrentContext(ctx, id, plat)
		if err != nil {
			rebuildErr = err
			return
		}
		if !rebuilt {
			rebuildErr = notFound("platform not found")
		}
	}); err != nil {
		return err
	}
	return rebuildErr
}

// PreviewFilterRequest holds preview filter parameters.
type PreviewFilterRequest struct {
	PlatformID   *string             `json:"platform_id"`
	PlatformSpec *PlatformSpecFilter `json:"platform_spec"`
}

type PlatformSpecFilter struct {
	RegexFilters  []string `json:"regex_filters"`
	RegionFilters []string `json:"region_filters"`
}

// NodeSummary is the API response for a node.
type NodeSummary struct {
	NodeHash                         string    `json:"node_hash"`
	CreatedAt                        string    `json:"created_at"`
	Enabled                          bool      `json:"enabled"`
	DisplayTag                       string    `json:"display_tag,omitempty"`
	HasOutbound                      bool      `json:"has_outbound"`
	LastError                        string    `json:"last_error,omitempty"`
	CircuitOpenSince                 *string   `json:"circuit_open_since"`
	FailureCount                     int       `json:"failure_count"`
	EgressIP                         string    `json:"egress_ip,omitempty"`
	Region                           string    `json:"region,omitempty"`
	LastEgressUpdate                 string    `json:"last_egress_update,omitempty"`
	LastLatencyProbeAttempt          string    `json:"last_latency_probe_attempt,omitempty"`
	LastAuthorityLatencyProbeAttempt string    `json:"last_authority_latency_probe_attempt,omitempty"`
	ReferenceLatencyMs               *float64  `json:"reference_latency_ms,omitempty"`
	LastEgressUpdateAttempt          string    `json:"last_egress_update_attempt,omitempty"`
	Tags                             []NodeTag `json:"tags"`
}

// IsHealthyAndEnabled follows the node-summary health rule used by API/UI
// aggregates: enabled, outbound-ready, and not circuit-open.
func (n NodeSummary) IsHealthyAndEnabled() bool {
	return n.Enabled && n.HasOutbound && n.CircuitOpenSince == nil
}

type NodeTag struct {
	SubscriptionID          string `json:"subscription_id"`
	SubscriptionName        string `json:"subscription_name"`
	Tag                     string `json:"tag"`
	SubscriptionCreatedAtNs int64  `json:"-"`
}

func (s *ControlPlaneService) nodeEntryToSummary(h node.Hash, entry *node.NodeEntry) NodeSummary {
	ns := NodeSummary{
		NodeHash:     h.Hex(),
		CreatedAt:    entry.CreatedAt.UTC().Format(time.RFC3339Nano),
		Enabled:      true,
		HasOutbound:  entry.HasOutbound(),
		LastError:    entry.GetLastError(),
		FailureCount: int(entry.FailureCount.Load()),
	}

	if s != nil && s.Pool != nil {
		ns.Enabled = !s.Pool.IsNodeDisabled(h)
		ns.DisplayTag = s.Pool.ResolveNodeDisplayTag(h)
	}

	if cos := entry.CircuitOpenSince.Load(); cos > 0 {
		t := time.Unix(0, cos).UTC().Format(time.RFC3339Nano)
		ns.CircuitOpenSince = &t
	}

	egressIP := entry.GetEgressIP()
	if egressIP.IsValid() {
		ns.EgressIP = egressIP.String()
		ns.Region = entry.GetRegion(nil)
		if s.GeoIP != nil {
			ns.Region = entry.GetRegion(s.GeoIP.Lookup)
		}
	}

	if leu := entry.LastEgressUpdate.Load(); leu > 0 {
		ns.LastEgressUpdate = time.Unix(0, leu).UTC().Format(time.RFC3339Nano)
	}
	if lastAny := entry.LastLatencyProbeAttempt.Load(); lastAny > 0 {
		ns.LastLatencyProbeAttempt = time.Unix(0, lastAny).UTC().Format(time.RFC3339Nano)
	}
	if lastAuthority := entry.LastAuthorityLatencyProbeAttempt.Load(); lastAuthority > 0 {
		ns.LastAuthorityLatencyProbeAttempt = time.Unix(0, lastAuthority).UTC().Format(time.RFC3339Nano)
	}
	if s != nil && s.RuntimeCfg != nil {
		if cfg := s.RuntimeCfg.Load(); cfg != nil {
			if avgMs, ok := node.AverageEWMAForDomainsMs(entry, cfg.LatencyAuthorities); ok {
				ns.ReferenceLatencyMs = &avgMs
			}
		}
	}
	if lastEgressAttempt := entry.LastEgressUpdateAttempt.Load(); lastEgressAttempt > 0 {
		ns.LastEgressUpdateAttempt = time.Unix(0, lastEgressAttempt).UTC().Format(time.RFC3339Nano)
	}

	// Build tags.
	subIDs := entry.SubscriptionIDs()
	for _, subID := range subIDs {
		sub := s.SubMgr.Lookup(subID)
		if sub == nil {
			continue
		}
		managed, ok := sub.ManagedNodes().LoadNode(h)
		if !ok {
			continue
		}
		tags := managed.Tags
		for _, tag := range tags {
			ns.Tags = append(ns.Tags, NodeTag{
				SubscriptionID:          subID,
				SubscriptionName:        sub.Name(),
				Tag:                     sub.Name() + "/" + tag,
				SubscriptionCreatedAtNs: sub.CreatedAtNs,
			})
		}
	}
	if ns.Tags == nil {
		ns.Tags = []NodeTag{}
	}
	return ns
}

// PreviewFilter returns nodes matching the given filter spec.
func (s *ControlPlaneService) PreviewFilter(req PreviewFilterRequest) ([]NodeSummary, error) {
	return s.PreviewFilterContext(context.Background(), req)
}

// PreviewFilterContext is the request-aware form of PreviewFilter. Cancellation
// is effective while waiting for a complete runtime generation; an admitted
// snapshot still runs to completion.
func (s *ControlPlaneService) PreviewFilterContext(ctx context.Context, req PreviewFilterRequest) ([]NodeSummary, error) {
	plan, err := preparePreviewFilter(req)
	if err != nil {
		return nil, err
	}
	var result []NodeSummary
	if readErr := s.withRuntimeReadContext(ctx, func() {
		result, err = s.previewFilter(plan)
	}); readErr != nil {
		return nil, readErr
	}
	return result, err
}

type previewFilterPlan struct {
	platformID    string
	fromPlatform  bool
	regexFilters  node.TagFilter
	regionFilters []string
}

func preparePreviewFilter(req PreviewFilterRequest) (previewFilterPlan, error) {
	hasPlatformID := req.PlatformID != nil && *req.PlatformID != ""
	hasPlatformSpec := req.PlatformSpec != nil

	if hasPlatformID == hasPlatformSpec {
		return previewFilterPlan{}, invalidArg("exactly one of platform_id or platform_spec is required")
	}

	if hasPlatformSpec {
		compiled, err := platform.CompileRegexFilters(req.PlatformSpec.RegexFilters)
		if err != nil {
			return previewFilterPlan{}, invalidArg(err.Error())
		}
		if err := platform.ValidateRegionFilters(req.PlatformSpec.RegionFilters); err != nil {
			return previewFilterPlan{}, invalidArg(err.Error())
		}
		return previewFilterPlan{
			regexFilters:  compiled,
			regionFilters: append([]string(nil), req.PlatformSpec.RegionFilters...),
		}, nil
	}

	return previewFilterPlan{platformID: *req.PlatformID, fromPlatform: true}, nil
}

func (s *ControlPlaneService) previewFilter(plan previewFilterPlan) ([]NodeSummary, error) {
	regexFilters := plan.regexFilters
	regionFilters := plan.regionFilters
	if plan.fromPlatform {
		plat, ok := s.Pool.GetPlatform(plan.platformID)
		if !ok {
			return nil, notFound("platform not found")
		}
		regexFilters = plat.RegexFilters
		regionFilters = plat.RegionFilters
	}

	var subLookup node.SubLookupFunc
	if s.Pool != nil {
		subLookup = s.Pool.MakeSubLookup()
	}
	var result []NodeSummary
	s.Pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
		if !entry.MatchTagFilter(regexFilters, subLookup) {
			return true
		}
		if len(regionFilters) > 0 {
			region := entry.GetRegion(nil)
			if s.GeoIP != nil {
				region = entry.GetRegion(s.GeoIP.Lookup)
			}
			if !platform.MatchRegionFilter(region, regionFilters) {
				return true
			}
		}
		result = append(result, s.nodeEntryToSummary(h, entry))
		return true
	})
	return result, nil
}
