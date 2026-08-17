package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

// ServiceError wraps an error with a code for API response mapping.
type ServiceError struct {
	Code    string // INVALID_ARGUMENT, NOT_FOUND, CONFLICT, INTERNAL
	Message string
	Err     error
}

// cancellableMutex serializes a mutation owner without turning a canceled
// request into an unbounded wait on sync.Mutex. Its zero value is ready for
// use, matching the previous configMu field semantics.
type cancellableMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *cancellableMutex) init() {
	m.token = make(chan struct{}, 1)
	m.token <- struct{}{}
}

func (m *cancellableMutex) lockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.once.Do(m.init)
	select {
	case <-m.token:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *cancellableMutex) unlock() {
	m.once.Do(m.init)
	m.token <- struct{}{}
}

func (m *cancellableMutex) Lock() {
	_ = m.lockContext(context.Background())
}

func (m *cancellableMutex) Unlock() {
	m.unlock()
}

// TryLock preserves the non-blocking inspection used by package tests while
// keeping the production owner cancellable for request-bound mutations.
func (m *cancellableMutex) TryLock() bool {
	m.once.Do(m.init)
	select {
	case <-m.token:
		return true
	default:
		return false
	}
}

// cancellableRWMutex is the service read/write boundary used by endpoint
// mutations. Readers remain parallel, writers have priority, and a waiting
// writer can leave when its request or shutdown admission is canceled.
type cancellableRWMutex struct {
	once           sync.Once
	mu             sync.Mutex
	cond           *sync.Cond
	readers        int
	writer         bool
	waitingWriters int
}

func (m *cancellableRWMutex) init() {
	m.cond = sync.NewCond(&m.mu)
}

func (m *cancellableRWMutex) ensure() {
	m.once.Do(m.init)
}

func (m *cancellableRWMutex) RLock() {
	_ = m.rLockContext(context.Background())
}

func (m *cancellableRWMutex) rLockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.ensure()
	m.mu.Lock()
	stopWake := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	for {
		if !m.writer && m.waitingWriters == 0 {
			m.readers++
			m.mu.Unlock()
			stopWake()
			return nil
		}
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			stopWake()
			return err
		}
		m.cond.Wait()
	}
}

func (m *cancellableRWMutex) RUnlock() {
	m.ensure()
	m.mu.Lock()
	if m.readers == 0 {
		m.mu.Unlock()
		panic("cancellableRWMutex: RUnlock of unlocked mutex")
	}
	m.readers--
	if m.readers == 0 {
		m.cond.Broadcast()
	}
	m.mu.Unlock()
}

func (m *cancellableRWMutex) lockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.ensure()
	m.mu.Lock()
	m.waitingWriters++
	stopWake := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	for {
		if !m.writer && m.readers == 0 {
			m.waitingWriters--
			m.writer = true
			m.mu.Unlock()
			stopWake()
			return nil
		}
		if err := ctx.Err(); err != nil {
			m.waitingWriters--
			m.cond.Broadcast()
			m.mu.Unlock()
			stopWake()
			return err
		}
		m.cond.Wait()
	}
}

func (m *cancellableRWMutex) unlock() {
	m.ensure()
	m.mu.Lock()
	if !m.writer {
		m.mu.Unlock()
		panic("cancellableRWMutex: Unlock of unlocked mutex")
	}
	m.writer = false
	m.cond.Broadcast()
	m.mu.Unlock()
}

func (m *cancellableRWMutex) Lock() {
	_ = m.lockContext(context.Background())
}

func (m *cancellableRWMutex) Unlock() {
	m.unlock()
}

func (e *ServiceError) Error() string { return e.Message }
func (e *ServiceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func invalidArg(msg string) *ServiceError {
	return &ServiceError{Code: "INVALID_ARGUMENT", Message: msg}
}

func notFound(msg string) *ServiceError {
	return &ServiceError{Code: "NOT_FOUND", Message: msg}
}

func conflict(msg string) *ServiceError {
	return &ServiceError{Code: "CONFLICT", Message: msg}
}

func internal(msg string, err error) *ServiceError {
	return &ServiceError{Code: "INTERNAL", Message: msg, Err: err}
}

// --- ControlPlaneService ---

// ControlPlaneService provides all control plane operations.
// Handlers call its methods; business logic lives here, not in handlers.
type ControlPlaneService struct {
	Engine          *state.StateEngine
	Pool            *topology.GlobalNodePool
	SubMgr          *topology.SubscriptionManager
	Scheduler       *topology.SubscriptionScheduler
	Router          *routing.Router
	GeoIP           *geoip.Service
	ProbeMgr        *probe.ProbeManager
	MatcherRuntime  *proxy.AccountMatcherRuntime
	RuntimeCfg      *atomic.Pointer[config.RuntimeConfig]
	EnvCfg          *config.EnvConfig
	EndpointRuntime EndpointRuntime

	configMu      cancellableMutex
	configVersion int
	endpointMu    cancellableRWMutex
	platformMu    cancellableMutex
	ruleMu        cancellableMutex

	// platformMutationHook is used by package tests to force a precise
	// interleaving around the real platform mutation path.
	platformMutationHook func(platformMutationStage)

	// subscriptionMutationHook is used by package tests to force a precise
	// interleaving around the real subscription mutation path.
	subscriptionMutationHook func(subscriptionMutationStage)

	// afterSubscriptionPersistHook is a package-test seam after the
	// subscription row is persisted and before its runtime mutation.
	afterSubscriptionPersistHook func()

	// afterSubscriptionRuntimeMutationHook is a package-test seam after a
	// subscription PATCH has applied its in-memory configuration and before
	// the subscription operation lock is released. Production leaves it nil.
	afterSubscriptionRuntimeMutationHook func()

	// afterSubscriptionNameReadHook is a package-test seam used to hold a
	// response after its immutable configuration snapshot is copied.
	// Production leaves it nil.
	afterSubscriptionNameReadHook func()

	// afterSubscriptionCleanupMutationHook is a package-test seam after the
	// cleanup operation lock is released. Production leaves it nil.
	afterSubscriptionCleanupMutationHook func()

	// beforeSubscriptionCleanupLockHook is a package-test seam between the
	// initial subscription lookup and acquisition of its operation lock.
	beforeSubscriptionCleanupLockHook func(id string, sub *subscription.Subscription)

	// ruleMutationHook is used by package tests to force a precise interleaving
	// around the real account-rule mutation path.
	ruleMutationHook func(ruleMutationStage)

	// beforeEndpointLockHook is a package-test seam immediately before an
	// endpoint mutation enters the service-owned read/write boundary.
	// Production leaves it nil.
	beforeEndpointLockHook func()

	// afterEndpointBeginPersistHook is a package-test seam after an enabled
	// endpoint stage has crossed its persistence boundary. Production leaves
	// it nil.
	afterEndpointBeginPersistHook func()

	// beforeEndpointDeletePersistHook is a package-test seam after a delete
	// owns the endpoint mutation lock and immediately before its SQL delete.
	// Production leaves it nil.
	beforeEndpointDeletePersistHook func()

	// beforeLeaseServiceRouterReadHook is a package-test seam immediately before
	// the atomic Router platform read.
	beforeLeaseServiceRouterReadHook func()

	// beforeLeaseInheritanceRouterCallHook is a package-test seam after a
	// platform name has been resolved and immediately before lease inheritance
	// is handed to Router. Production leaves it nil.
	beforeLeaseInheritanceRouterCallHook func()

	// beforeLeaseMutationAdmissionHook is a package-test seam immediately
	// before a lease deletion enters state-write admission. Production leaves
	// it nil.
	beforeLeaseMutationAdmissionHook func()

	// beforePlatformRebuildHook is a package-test seam immediately before the
	// platform view rebuild. Production leaves it nil.
	beforePlatformRebuildHook func()

	// beforeProbeManagerCallHook is a package-test seam after a control-plane
	// probe has captured its entry and immediately before handing off to the
	// probe manager. Production leaves it nil.
	beforeProbeManagerCallHook func()

	// afterProbeManagerResultHook is a package-test seam after the probe
	// manager has completed and before the control-plane response is built.
	// Production leaves it nil.
	afterProbeManagerResultHook func()

	// afterRuntimeReadLockHook is a package-test seam after a service runtime
	// read has acquired the pool read owner. Production leaves it nil.
	afterRuntimeReadLockHook func()

	// beforePlatformReadHook is a package-test seam immediately before a
	// platform read enters the service-owned platform generation boundary.
	// Production leaves it nil.
	beforePlatformReadHook func()

	// beforePlatformModelReadHook is a package-test seam immediately before a
	// persisted platform model/name read. The context identifies whether the
	// caller propagated its request cancellation. Production leaves it nil.
	beforePlatformModelReadHook func(context.Context)

	// afterPlatformPersistHook is a package-test seam after the platform row is
	// persisted and before the runtime pool publish. Production leaves it nil.
	afterPlatformPersistHook func()

	// beforePlatformRuntimeAdmissionHook marks the point at which a platform
	// mutation is about to acquire the pool's exclusive publication owner.
	// In production this is nil; tests use it to distinguish pre-DB admission
	// cancellation from the post-persist commit boundary.
	beforePlatformRuntimeAdmissionHook func()

	// afterPlatformUnregisterHook is a package-test seam after a platform is
	// removed from the runtime pool and before its Router state is removed.
	// Production leaves it nil.
	afterPlatformUnregisterHook func()

	// afterRuntimeConfigPersistHook is a package-test seam after the system
	// config row is persisted and before the runtime snapshot publish.
	afterRuntimeConfigPersistHook func()

	// Runtime config lock seams are package-test coordination around the
	// cancellable config mutation owner. Production leaves them nil.
	beforeRuntimeConfigLockHook func()
	afterRuntimeConfigLockHook  func()

	// Runtime read seams are package-test coordination for request-bound
	// generation admission. Production leaves them nil.
	beforeRuntimeReadLockHook   func()
	afterRuntimeReadAttemptHook func(error)
}

// withRuntimeRead keeps responses that combine subscription ManagedNodes with
// pool/node state on one published runtime generation. Pure database-only
// control-plane reads do not need this boundary.
func (s *ControlPlaneService) withRuntimeRead(fn func()) {
	_ = s.withRuntimeReadContext(context.Background(), fn)
}

// withRuntimeReadContext admits a request-bound read into one complete
// runtime generation. Cancellation is effective before admission; an
// admitted callback still runs to completion.
func (s *ControlPlaneService) withRuntimeReadContext(ctx context.Context, fn func()) error {
	if fn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if hook := s.beforeRuntimeReadLockHook; hook != nil {
		hook()
	}
	if s != nil && s.Pool != nil {
		err := s.Pool.WithRuntimeReadContext(ctx, func() {
			if hook := s.afterRuntimeReadLockHook; hook != nil {
				hook()
			}
			fn()
		})
		if hook := s.afterRuntimeReadAttemptHook; hook != nil {
			hook(err)
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		if hook := s.afterRuntimeReadAttemptHook; hook != nil {
			hook(err)
		}
		return err
	}
	fn()
	if hook := s.afterRuntimeReadAttemptHook; hook != nil {
		hook(nil)
	}
	return nil
}

type platformMutationStage uint8

const (
	platformMutationBeforeLock platformMutationStage = iota
	platformMutationAfterLoad
)

func (s *ControlPlaneService) runPlatformMutationHook(stage platformMutationStage) {
	if s.platformMutationHook != nil {
		s.platformMutationHook(stage)
	}
}

type subscriptionMutationStage uint8

const (
	subscriptionMutationBeforeLock subscriptionMutationStage = iota
	subscriptionMutationAfterLoad
)

func (s *ControlPlaneService) runSubscriptionMutationHook(stage subscriptionMutationStage) {
	if s.subscriptionMutationHook != nil {
		s.subscriptionMutationHook(stage)
	}
}

type ruleMutationStage uint8

const (
	ruleMutationBeforeLock ruleMutationStage = iota
	ruleMutationBeforeSnapshot
	ruleMutationAfterSnapshot
	ruleMutationAfterPersist
)

func (s *ControlPlaneService) runRuleMutationHook(stage ruleMutationStage) {
	if s.ruleMutationHook != nil {
		s.ruleMutationHook(stage)
	}
}

// ------------------------------------------------------------------
// System Config
// ------------------------------------------------------------------

// runtimeConfigAllowedFields is the set of JSON field names that can be patched.
var runtimeConfigAllowedFields = map[string]bool{
	"request_log_enabled":                      true,
	"reverse_proxy_log_detail_enabled":         true,
	"reverse_proxy_log_req_headers_max_bytes":  true,
	"reverse_proxy_log_req_body_max_bytes":     true,
	"reverse_proxy_log_resp_headers_max_bytes": true,
	"reverse_proxy_log_resp_body_max_bytes":    true,
	"max_consecutive_failures":                 true,
	"max_latency_test_interval":                true,
	"max_authority_latency_test_interval":      true,
	"max_egress_test_interval":                 true,
	"latency_test_url":                         true,
	"latency_authorities":                      true,
	"p2c_latency_window":                       true,
	"latency_decay_window":                     true,
	"cache_flush_interval":                     true,
	"cache_flush_dirty_threshold":              true,
}

var platformPatchAllowedFields = map[string]bool{
	"name":                                 true,
	"sticky_ttl":                           true,
	"regex_filters":                        true,
	"region_filters":                       true,
	"reverse_proxy_miss_action":            true,
	"reverse_proxy_empty_account_behavior": true,
	"reverse_proxy_fixed_account_header":   true,
	"allocation_policy":                    true,
	"passive_circuit_breaker_disabled":     true,
	"response_rules":                       true,
}

var subscriptionPatchAllowedFields = map[string]bool{
	"name":                       true,
	"url":                        true,
	"content":                    true,
	"update_interval":            true,
	"enabled":                    true,
	"ephemeral":                  true,
	"incremental_alive_nodes":    true,
	"ephemeral_node_evict_delay": true,
}

func parseRuntimeConfigPatch(patchJSON json.RawMessage, out *config.RuntimeConfig) *ServiceError {
	var rawPatch map[string]json.RawMessage
	if err := json.Unmarshal(patchJSON, &rawPatch); err != nil {
		return invalidArg("invalid JSON: " + err.Error())
	}
	if len(rawPatch) == 0 {
		return invalidArg("empty patch")
	}
	for key, raw := range rawPatch {
		if !runtimeConfigAllowedFields[key] {
			return invalidArg(fmt.Sprintf("unknown or read-only field: %q", key))
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return invalidArg(fmt.Sprintf("null value not allowed for field: %q", key))
		}
	}

	dec := json.NewDecoder(bytes.NewReader(patchJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return invalidArg("validation failed: " + err.Error())
	}
	return nil
}

func copyRuntimeConfig(cfg *config.RuntimeConfig) *config.RuntimeConfig {
	if cfg == nil {
		return config.NewDefaultRuntimeConfig()
	}
	out := *cfg
	out.LatencyAuthorities = append([]string(nil), cfg.LatencyAuthorities...)
	return &out
}

// PatchRuntimeConfig applies a constrained partial patch to the runtime config.
// This is not RFC 7396 JSON Merge Patch: patch must be a non-empty object and
// null values are rejected.
// Pipeline: validate → persist → atomic swap.
func (s *ControlPlaneService) PatchRuntimeConfig(patchJSON json.RawMessage) (*config.RuntimeConfig, error) {
	return s.PatchRuntimeConfigContext(context.Background(), patchJSON)
}

// PatchRuntimeConfigContext applies a runtime config patch with caller
// cancellation. Background callers retain the ordinary non-cancelable
// behavior through PatchRuntimeConfig.
func (s *ControlPlaneService) PatchRuntimeConfigContext(ctx context.Context, patchJSON json.RawMessage) (*config.RuntimeConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if hook := s.beforeRuntimeConfigLockHook; hook != nil {
		hook()
	}
	if err := s.configMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.configMu.unlock()
	if hook := s.afterRuntimeConfigLockHook; hook != nil {
		hook()
	}

	var published *config.RuntimeConfig
	err := s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(writeCtx, _ context.Context) error {
		if err := writeCtx.Err(); err != nil {
			return err
		}
		// 3. Deep-copy current config → apply patch.
		newCfg := copyRuntimeConfig(s.RuntimeCfg.Load())
		if verr := parseRuntimeConfigPatch(patchJSON, newCfg); verr != nil {
			return verr
		}

		// 4. Additional validation.
		if err := validateRuntimeConfig(newCfg); err != nil {
			return err
		}

		// On process start, initialize local configVersion from persisted state
		// so PATCH keeps monotonically increasing versions across restarts.
		if s.configVersion == 0 && s.Engine != nil {
			_, persistedVersion, err := s.Engine.GetSystemConfigContext(writeCtx)
			if err != nil {
				return internal("load persisted config version", err)
			}
			if persistedVersion > s.configVersion {
				s.configVersion = persistedVersion
			}
		}

		// 5. Persist.
		newVersion := s.configVersion + 1
		if err := s.Engine.SaveSystemConfigContextAndCommit(writeCtx, newCfg, newVersion, time.Now().UnixNano()); err != nil {
			return internal("persist config", err)
		}
		if hook := s.afterRuntimeConfigPersistHook; hook != nil {
			hook()
		}

		// 6. Atomic swap.
		s.RuntimeCfg.Store(newCfg)
		s.configVersion = newVersion
		published = newCfg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return published, nil
}

func validateRuntimeConfig(cfg *config.RuntimeConfig) *ServiceError {
	latencyURL := strings.TrimSpace(cfg.LatencyTestURL)
	u, verr := parseHTTPAbsoluteURL("latency_test_url", latencyURL)
	if verr != nil {
		return verr
	}
	latencyDomain := strings.ToLower(netutil.ExtractDomain(u.Host))
	if cfg.MaxConsecutiveFailures < 0 {
		return invalidArg("max_consecutive_failures: must be non-negative")
	}
	if cfg.CacheFlushDirtyThreshold < 0 {
		return invalidArg("cache_flush_dirty_threshold: must be non-negative")
	}
	if err := config.ValidateRuntimeLogCaptureLimits(cfg); err != nil {
		return invalidArg(err.Error())
	}
	minProbeInterval := 30 * time.Second
	// Probe intervals must be at least 30s (DESIGN.md).
	if time.Duration(cfg.MaxLatencyTestInterval) < minProbeInterval {
		return invalidArg("max_latency_test_interval: must be >= 30s")
	}
	if time.Duration(cfg.MaxAuthorityLatencyTestInterval) < minProbeInterval {
		return invalidArg("max_authority_latency_test_interval: must be >= 30s")
	}
	if time.Duration(cfg.MaxEgressTestInterval) < minProbeInterval {
		return invalidArg("max_egress_test_interval: must be >= 30s")
	}
	if cfg.P2CLatencyWindow < 0 {
		return invalidArg("p2c_latency_window: must be non-negative")
	}
	if cfg.LatencyDecayWindow < 0 {
		return invalidArg("latency_decay_window: must be non-negative")
	}
	minCacheFlushInterval := 5 * time.Second
	if time.Duration(cfg.CacheFlushInterval) < minCacheFlushInterval {
		return invalidArg("cache_flush_interval: must be >= 5s")
	}

	// LatencyTestURL domain must be in LatencyAuthorities.
	// If absent, append it instead of returning an error.
	if latencyDomain != "" {
		found := false
		for _, authority := range cfg.LatencyAuthorities {
			if strings.EqualFold(strings.TrimSpace(authority), latencyDomain) {
				found = true
				break
			}
		}
		if !found {
			cfg.LatencyAuthorities = append(cfg.LatencyAuthorities, latencyDomain)
		}
	}
	if err := node.ValidateLatencyAuthorities(cfg.LatencyAuthorities); err != nil {
		return invalidArg("latency_authorities: " + err.Error())
	}
	return nil
}

// ValidateRuntimeConfig applies the same complete contract used by runtime
// PATCH to a configuration loaded from persistence. The validator may append
// the latency-test URL's authority to cfg, matching PATCH semantics.
func ValidateRuntimeConfig(cfg *config.RuntimeConfig) error {
	return validateRuntimeConfig(cfg)
}
