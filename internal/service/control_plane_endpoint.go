package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/state"
)

const DefaultEndpointID = "default"

// EndpointRuntimeStatus describes the ephemeral state of one inbound listener.
type EndpointRuntimeStatus struct {
	State     string
	LastError string
}

// EndpointRuntimeStage owns a prepared listener/runtime candidate. Prepare
// happens before the strong-persist write; Abort closes an unpublished
// candidate, while Commit publishes it without another failure point.
type EndpointRuntimeStage interface {
	// BeginPersist reserves the stage across the database mutation. Shutdown
	// may cancel a prepared stage before this point, but must not abandon a
	// stage once its owning control-plane mutation has started persisting.
	BeginPersist() bool
	Abort()
	Commit()
}

// EndpointRuntime prepares and publishes persisted endpoint configuration to
// network listeners. Enabled creates and port changes must use the staged
// path so a bind/build failure happens before the database mutation.
type EndpointRuntime interface {
	PrepareEndpoint(model.Endpoint) (EndpointRuntimeStage, error)
	RemoveEndpoint(id string)
	EndpointStatus(id string) EndpointRuntimeStatus
}

type EndpointResponse struct {
	ID                   string `json:"id"`
	Port                 int    `json:"port"`
	Enabled              bool   `json:"enabled"`
	AllowManagement      bool   `json:"allow_management"`
	AllowProxy           bool   `json:"allow_proxy"`
	RequireProxyAuthInfo bool   `json:"require_proxy_auth_info"`
	AllowHTTPForward     bool   `json:"allow_http_forward"`
	AllowHTTPReverse     bool   `json:"allow_http_reverse"`
	AllowSOCKS5          bool   `json:"allow_socks5"`
	Source               string `json:"source"`
	ReadOnly             bool   `json:"read_only"`
	Status               string `json:"status"`
	LastError            string `json:"last_error,omitempty"`
	CreatedAt            string `json:"created_at,omitempty"`
	UpdatedAt            string `json:"updated_at,omitempty"`
}

type CreateEndpointRequest struct {
	Port                 int   `json:"port"`
	Enabled              *bool `json:"enabled,omitempty"`
	AllowManagement      *bool `json:"allow_management,omitempty"`
	AllowProxy           *bool `json:"allow_proxy,omitempty"`
	RequireProxyAuthInfo *bool `json:"require_proxy_auth_info,omitempty"`
	AllowHTTPForward     *bool `json:"allow_http_forward,omitempty"`
	AllowHTTPReverse     *bool `json:"allow_http_reverse,omitempty"`
	AllowSOCKS5          *bool `json:"allow_socks5,omitempty"`
}

var endpointPatchAllowedFields = map[string]bool{
	"enabled":                 true,
	"port":                    true,
	"allow_management":        true,
	"allow_proxy":             true,
	"require_proxy_auth_info": true,
	"allow_http_forward":      true,
	"allow_http_reverse":      true,
	"allow_socks5":            true,
}

func boolOrDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

// NewDefaultEndpoint builds the environment-defined, read-only endpoint policy.
func NewDefaultEndpoint(port int) model.Endpoint {
	if port == 0 {
		port = 2260
	}
	return model.Endpoint{
		ID:               DefaultEndpointID,
		Port:             port,
		Enabled:          true,
		AllowManagement:  true,
		AllowProxy:       true,
		AllowHTTPForward: true,
		AllowHTTPReverse: true,
		AllowSOCKS5:      true,
	}
}

func (s *ControlPlaneService) defaultEndpoint() model.Endpoint {
	port := 0
	if s != nil && s.EnvCfg != nil {
		port = s.EnvCfg.ResinPort
	}
	return NewDefaultEndpoint(port)
}

func (s *ControlPlaneService) endpointResponse(endpoint model.Endpoint, source string, readOnly bool) EndpointResponse {
	status := EndpointRuntimeStatus{State: "inactive"}
	if s != nil && s.EndpointRuntime != nil {
		status = s.EndpointRuntime.EndpointStatus(endpoint.ID)
		if status.State == "" {
			status.State = "inactive"
		}
	}
	response := EndpointResponse{
		ID:                   endpoint.ID,
		Port:                 endpoint.Port,
		Enabled:              endpoint.Enabled,
		AllowManagement:      endpoint.AllowManagement,
		AllowProxy:           endpoint.AllowProxy,
		RequireProxyAuthInfo: endpoint.RequireProxyAuthInfo,
		AllowHTTPForward:     endpoint.AllowHTTPForward,
		AllowHTTPReverse:     endpoint.AllowHTTPReverse,
		AllowSOCKS5:          endpoint.AllowSOCKS5,
		Source:               source,
		ReadOnly:             readOnly,
		Status:               status.State,
		LastError:            status.LastError,
	}
	if endpoint.CreatedAtNs > 0 {
		response.CreatedAt = time.Unix(0, endpoint.CreatedAtNs).UTC().Format(time.RFC3339Nano)
	}
	if endpoint.UpdatedAtNs > 0 {
		response.UpdatedAt = time.Unix(0, endpoint.UpdatedAtNs).UTC().Format(time.RFC3339Nano)
	}
	return response
}

func (s *ControlPlaneService) validateEndpoint(endpoint model.Endpoint) *ServiceError {
	if endpoint.Port < 1 || endpoint.Port > 65535 {
		return invalidArg("port: must be between 1 and 65535")
	}
	if endpoint.ID != DefaultEndpointID && endpoint.Port == s.defaultEndpoint().Port {
		return conflict("port is reserved by the default endpoint")
	}
	if !endpoint.AllowManagement && !endpoint.AllowProxy {
		return invalidArg("at least one of allow_management or allow_proxy must be enabled")
	}
	if !endpoint.AllowProxy {
		if endpoint.AllowHTTPForward || endpoint.AllowHTTPReverse || endpoint.AllowSOCKS5 || endpoint.RequireProxyAuthInfo {
			return invalidArg("proxy protocol settings must be disabled when allow_proxy is false")
		}
		return nil
	}
	if !endpoint.AllowHTTPForward && !endpoint.AllowHTTPReverse && !endpoint.AllowSOCKS5 {
		return invalidArg("at least one proxy protocol must be enabled when allow_proxy is true")
	}
	if endpoint.RequireProxyAuthInfo && !endpoint.AllowHTTPForward && !endpoint.AllowSOCKS5 {
		return invalidArg("require_proxy_auth_info requires HTTP forward proxy or SOCKS5")
	}
	return nil
}

// withEndpointMutation keeps state-db admission active through the matching
// runtime publish/remove. Shutdown must not observe an idle state database
// while the same endpoint mutation can still change the live listener set.
func (s *ControlPlaneService) withEndpointMutation(fn func() error) error {
	return s.withEndpointMutationContext(context.Background(), func(context.Context, context.Context) error {
		return fn()
	})
}

func (s *ControlPlaneService) withEndpointMutationContext(ctx context.Context, fn func(context.Context, context.Context) error) error {
	err := s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, fn)
	if errors.Is(err, state.ErrStateWriteAdmissionClosed) {
		return internal("endpoint mutation", err)
	}
	return err
}

func (s *ControlPlaneService) ListEndpoints() ([]EndpointResponse, error) {
	if s == nil || s.Engine == nil {
		return nil, internal("endpoint service is not initialized", nil)
	}
	s.endpointMu.RLock()
	defer s.endpointMu.RUnlock()

	custom, err := s.Engine.ListEndpoints()
	if err != nil {
		return nil, internal("list endpoints", err)
	}
	result := make([]EndpointResponse, 0, len(custom)+1)
	result = append(result, s.endpointResponse(s.defaultEndpoint(), "environment", true))
	for _, endpoint := range custom {
		result = append(result, s.endpointResponse(endpoint, "database", false))
	}
	return result, nil
}

func (s *ControlPlaneService) GetEndpoint(id string) (*EndpointResponse, error) {
	if id == DefaultEndpointID {
		response := s.endpointResponse(s.defaultEndpoint(), "environment", true)
		return &response, nil
	}
	if s == nil || s.Engine == nil {
		return nil, internal("endpoint service is not initialized", nil)
	}
	s.endpointMu.RLock()
	defer s.endpointMu.RUnlock()

	endpoint, err := s.Engine.GetEndpoint(id)
	if errors.Is(err, state.ErrNotFound) {
		return nil, notFound("endpoint not found")
	}
	if err != nil {
		return nil, internal("get endpoint", err)
	}
	response := s.endpointResponse(*endpoint, "database", false)
	return &response, nil
}

func (s *ControlPlaneService) CreateEndpoint(req CreateEndpointRequest) (*EndpointResponse, error) {
	return s.CreateEndpointContext(context.Background(), req)
}

// CreateEndpointContext creates an endpoint while honoring request
// cancellation through validation and runtime preparation. Once a prepared
// stage crosses BeginPersist, the shutdown-owned commit context carries the
// single DB write and runtime publish to completion.
func (s *ControlPlaneService) CreateEndpointContext(ctx context.Context, req CreateEndpointRequest) (*EndpointResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.Engine == nil {
		return nil, internal("endpoint service is not initialized", nil)
	}
	var response *EndpointResponse
	err := s.withEndpointMutationContext(ctx, func(writeCtx, commitCtx context.Context) error {
		if s.beforeEndpointLockHook != nil {
			s.beforeEndpointLockHook()
		}
		if err := s.endpointMu.lockContext(writeCtx); err != nil {
			return err
		}
		defer s.endpointMu.unlock()

		allowProxy := boolOrDefault(req.AllowProxy, true)
		now := time.Now().UnixNano()
		endpoint := model.Endpoint{
			ID:                   uuid.New().String(),
			Port:                 req.Port,
			Enabled:              boolOrDefault(req.Enabled, true),
			AllowManagement:      boolOrDefault(req.AllowManagement, false),
			AllowProxy:           allowProxy,
			RequireProxyAuthInfo: boolOrDefault(req.RequireProxyAuthInfo, false),
			AllowHTTPForward:     boolOrDefault(req.AllowHTTPForward, allowProxy),
			AllowHTTPReverse:     boolOrDefault(req.AllowHTTPReverse, allowProxy),
			AllowSOCKS5:          boolOrDefault(req.AllowSOCKS5, allowProxy),
			CreatedAtNs:          now,
			UpdatedAtNs:          now,
		}
		if err := s.validateEndpoint(endpoint); err != nil {
			return err
		}
		if err := writeCtx.Err(); err != nil {
			return err
		}
		var runtimeStage EndpointRuntimeStage
		if endpoint.Enabled && s.EndpointRuntime != nil {
			var prepareErr error
			runtimeStage, prepareErr = s.EndpointRuntime.PrepareEndpoint(endpoint)
			if prepareErr != nil {
				return conflict(fmt.Sprintf("listen on port %d: %v", endpoint.Port, prepareErr))
			}
			if err := writeCtx.Err(); err != nil {
				runtimeStage.Abort()
				return err
			}
			if !runtimeStage.BeginPersist() {
				runtimeStage.Abort()
				return internal("endpoint runtime unavailable", errors.New("endpoint stage canceled"))
			}
			if s.afterEndpointBeginPersistHook != nil {
				s.afterEndpointBeginPersistHook()
			}
		}
		if err := s.Engine.InsertEndpointContext(commitCtx, endpoint); err != nil {
			if runtimeStage != nil {
				runtimeStage.Abort()
			}
			if errors.Is(err, state.ErrConflict) {
				return conflict("endpoint port already exists")
			}
			return internal("persist endpoint", err)
		}
		if runtimeStage != nil {
			runtimeStage.Commit()
		}
		responseValue := s.endpointResponse(endpoint, "database", false)
		response = &responseValue
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *ControlPlaneService) UpdateEndpoint(id string, patchJSON json.RawMessage) (*EndpointResponse, error) {
	return s.UpdateEndpointContext(context.Background(), id, patchJSON)
}

// UpdateEndpointContext updates an endpoint while honoring request
// cancellation through validation and runtime preparation. A prepared runtime
// candidate is aborted on every pre-commit failure and, after BeginPersist,
// committed with the shutdown-owned context only after the DB write succeeds.
func (s *ControlPlaneService) UpdateEndpointContext(ctx context.Context, id string, patchJSON json.RawMessage) (*EndpointResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == DefaultEndpointID {
		return nil, conflict("default endpoint is read-only")
	}
	if s == nil || s.Engine == nil {
		return nil, internal("endpoint service is not initialized", nil)
	}
	patch, patchErr := parseMergePatch(patchJSON)
	if patchErr != nil {
		return nil, patchErr
	}
	if err := patch.validateFields(endpointPatchAllowedFields, func(key string) string {
		return fmt.Sprintf("field %q is read-only or unknown", key)
	}); err != nil {
		return nil, err
	}

	var response *EndpointResponse
	err := s.withEndpointMutationContext(ctx, func(writeCtx, commitCtx context.Context) error {
		if s.beforeEndpointLockHook != nil {
			s.beforeEndpointLockHook()
		}
		if err := s.endpointMu.lockContext(writeCtx); err != nil {
			return err
		}
		defer s.endpointMu.unlock()

		current, err := s.Engine.GetEndpointContext(writeCtx, id)
		if errors.Is(err, state.ErrNotFound) {
			return notFound("endpoint not found")
		}
		if err != nil {
			return internal("get endpoint", err)
		}
		next := *current
		if value, ok, parseErr := patch.optionalInt("port"); parseErr != nil {
			return parseErr
		} else if ok {
			next.Port = value
		}
		boolFields := []struct {
			name string
			set  func(bool)
		}{
			{"enabled", func(v bool) { next.Enabled = v }},
			{"allow_management", func(v bool) { next.AllowManagement = v }},
			{"allow_proxy", func(v bool) { next.AllowProxy = v }},
			{"require_proxy_auth_info", func(v bool) { next.RequireProxyAuthInfo = v }},
			{"allow_http_forward", func(v bool) { next.AllowHTTPForward = v }},
			{"allow_http_reverse", func(v bool) { next.AllowHTTPReverse = v }},
			{"allow_socks5", func(v bool) { next.AllowSOCKS5 = v }},
		}
		for _, field := range boolFields {
			value, ok, parseErr := patch.optionalBool(field.name)
			if parseErr != nil {
				return parseErr
			}
			if ok {
				field.set(value)
			}
		}
		if validationErr := s.validateEndpoint(next); validationErr != nil {
			return validationErr
		}
		if err := writeCtx.Err(); err != nil {
			return err
		}
		if next == *current {
			responseValue := s.endpointResponse(*current, "database", false)
			response = &responseValue
			return nil
		}
		next.UpdatedAtNs = time.Now().UnixNano()
		var runtimeStage EndpointRuntimeStage
		if next.Enabled && s.EndpointRuntime != nil {
			var prepareErr error
			runtimeStage, prepareErr = s.EndpointRuntime.PrepareEndpoint(next)
			if prepareErr != nil {
				return conflict(fmt.Sprintf("listen on port %d: %v", next.Port, prepareErr))
			}
			if err := writeCtx.Err(); err != nil {
				runtimeStage.Abort()
				return err
			}
			if !runtimeStage.BeginPersist() {
				runtimeStage.Abort()
				return internal("endpoint runtime unavailable", errors.New("endpoint stage canceled"))
			}
			if s.afterEndpointBeginPersistHook != nil {
				s.afterEndpointBeginPersistHook()
			}
		}
		if err := s.Engine.UpdateEndpointContext(commitCtx, next); err != nil {
			if runtimeStage != nil {
				runtimeStage.Abort()
			}
			if errors.Is(err, state.ErrConflict) {
				return conflict("endpoint port already exists")
			}
			if errors.Is(err, state.ErrNotFound) {
				return notFound("endpoint not found")
			}
			return internal("persist endpoint", err)
		}
		if runtimeStage != nil {
			runtimeStage.Commit()
		} else if s.EndpointRuntime != nil {
			s.EndpointRuntime.RemoveEndpoint(next.ID)
		}
		responseValue := s.endpointResponse(next, "database", false)
		response = &responseValue
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *ControlPlaneService) DeleteEndpoint(id string) error {
	return s.DeleteEndpointContext(context.Background(), id)
}

// DeleteEndpointContext deletes an endpoint while honoring request
// cancellation before the irreversible delete boundary. Once admitted, the
// shutdown-owned commit context keeps the DB delete and runtime removal in
// one mutation.
func (s *ControlPlaneService) DeleteEndpointContext(ctx context.Context, id string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == DefaultEndpointID {
		return conflict("default endpoint is read-only")
	}
	if s == nil || s.Engine == nil {
		return internal("endpoint service is not initialized", nil)
	}
	return s.withEndpointMutationContext(ctx, func(writeCtx, commitCtx context.Context) error {
		if s.beforeEndpointLockHook != nil {
			s.beforeEndpointLockHook()
		}
		if err := s.endpointMu.lockContext(writeCtx); err != nil {
			return err
		}
		defer s.endpointMu.unlock()
		if s.beforeEndpointDeletePersistHook != nil {
			s.beforeEndpointDeletePersistHook()
		}

		if err := s.Engine.DeleteEndpointContext(commitCtx, id); err != nil {
			if errors.Is(err, state.ErrNotFound) {
				return notFound("endpoint not found")
			}
			return internal("delete endpoint", err)
		}
		if s.EndpointRuntime != nil {
			s.EndpointRuntime.RemoveEndpoint(id)
		}
		return nil
	})
}
