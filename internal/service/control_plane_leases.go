package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/observability"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
)

// ------------------------------------------------------------------
// Leases
// ------------------------------------------------------------------

// LeaseResponse is the API response for a lease.
type LeaseResponse struct {
	PlatformID      string `json:"platform_id"`
	Account         string `json:"account"`
	AccountRedacted bool   `json:"account_redacted"`
	LeaseID         string `json:"lease_id"`
	NodeHash        string `json:"node_hash"`
	NodeTag         string `json:"node_tag"`
	EgressIP        string `json:"egress_ip"`
	Expiry          string `json:"expiry"`
	LastAccessed    string `json:"last_accessed"`
	accountKey      string
}

// withLeaseMutationContext keeps the state-write admission open through a
// control-plane lease mutation and its synchronous Router lease event.
// Test-only services without persistence keep the existing in-memory behavior.
func (s *ControlPlaneService) withLeaseMutationContext(ctx context.Context, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.Engine == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(context.Background())
	}
	if err := s.Engine.WithStateWriteAdmissionContextAndCommit(ctx, func(_, commitCtx context.Context) error {
		return fn(commitCtx)
	}); err != nil {
		if errors.Is(err, state.ErrStateWriteAdmissionClosed) {
			return internal("lease mutation", err)
		}
		return err
	}
	return nil
}

func leaseToResponse(projector *observability.Projector, lease model.Lease, nodeTag string) LeaseResponse {
	return LeaseResponse{
		PlatformID:      lease.PlatformID,
		Account:         projector.RedactAccount(lease.PlatformID, lease.Account),
		AccountRedacted: lease.Account != "",
		LeaseID:         projector.LeaseID(lease.PlatformID, lease.Account),
		NodeHash:        lease.NodeHash,
		NodeTag:         nodeTag,
		EgressIP:        lease.EgressIP,
		Expiry:          time.Unix(0, lease.ExpiryNs).UTC().Format(time.RFC3339Nano),
		LastAccessed:    time.Unix(0, lease.LastAccessedNs).UTC().Format(time.RFC3339Nano),
		accountKey:      lease.Account,
	}
}

var fallbackProjector = observability.NewRandomProjector()

func (s *ControlPlaneService) projector() *observability.Projector {
	if s != nil && s.Projector != nil {
		return s.Projector
	}
	return fallbackProjector
}

// MatchesAccountFilter keeps the original account inside the service boundary
// for server-side filtering while the response itself only contains its safe
// display projection.
func (l LeaseResponse) MatchesAccountFilter(query string, fuzzy bool) bool {
	query = strings.TrimSpace(query)
	if query == "" {
		return true
	}
	if fuzzy {
		needle := strings.ToLower(query)
		return strings.Contains(strings.ToLower(l.accountKey), needle) || strings.Contains(strings.ToLower(l.Account), needle)
	}
	return l.accountKey == query || l.Account == query
}

func (s *ControlPlaneService) resolveLeaseNodeTag(hash node.Hash) string {
	if s == nil || s.Pool == nil {
		return ""
	}
	return s.Pool.ResolveNodeDisplayTag(hash)
}

func (s *ControlPlaneService) resolveLeaseNodeTagFromHex(hashHex string) string {
	hash, err := node.ParseHex(hashHex)
	if err != nil {
		return ""
	}
	return s.resolveLeaseNodeTag(hash)
}

// ListLeases returns all leases for a platform.
func (s *ControlPlaneService) ListLeases(platformID string) ([]LeaseResponse, error) {
	return s.ListLeasesContext(context.Background(), platformID)
}

// ListLeasesContext returns all leases while honoring cancellation before
// entering the runtime generation read owner.
func (s *ControlPlaneService) ListLeasesContext(ctx context.Context, platformID string) ([]LeaseResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.beforeLeaseServiceRouterReadHook != nil {
		s.beforeLeaseServiceRouterReadHook()
	}
	var result []LeaseResponse
	var resultErr error
	projector := s.projector()
	if err := s.withRuntimeReadContext(ctx, func() {
		leases, exists, err := s.Router.ListLeasesForPlatformContext(ctx, platformID)
		if err != nil {
			resultErr = err
			return
		}
		if !exists {
			resultErr = notFound("platform not found")
			return
		}
		result = make([]LeaseResponse, 0, len(leases))
		for _, lease := range leases {
			result = append(result, leaseToResponse(projector, lease, s.resolveLeaseNodeTagFromHex(lease.NodeHash)))
		}
	}); err != nil {
		return nil, err
	}
	if resultErr != nil {
		return nil, resultErr
	}
	return result, nil
}

// GetLease returns a single lease.
func (s *ControlPlaneService) GetLease(platformID, account string) (*LeaseResponse, error) {
	return s.GetLeaseContext(context.Background(), platformID, account)
}

// GetLeaseContext returns one lease while honoring cancellation before
// entering the runtime generation read owner.
func (s *ControlPlaneService) GetLeaseContext(ctx context.Context, platformID, account string) (*LeaseResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result *LeaseResponse
	var resultErr error
	projector := s.projector()
	if err := s.withRuntimeReadContext(ctx, func() {
		ml, exists, err := s.Router.ReadLeaseForPlatformContext(ctx, model.LeaseKey{PlatformID: platformID, Account: account})
		if err != nil {
			resultErr = err
			return
		}
		if !exists {
			resultErr = notFound("platform not found")
			return
		}
		if ml == nil {
			resultErr = notFound("lease not found")
			return
		}
		resp := leaseToResponse(projector, *ml, s.resolveLeaseNodeTagFromHex(ml.NodeHash))
		result = &resp
	}); err != nil {
		return nil, err
	}
	if resultErr != nil {
		return nil, resultErr
	}
	return result, nil
}

// resolveLeaseIdentifierContext resolves only opaque lease IDs. Raw account
// paths are rejected at the administrative API boundary. Opaque IDs are
// process-generation bound to the random observability key; after restart or
// key rotation they fail closed as not found.
func (s *ControlPlaneService) resolveLeaseIdentifierContext(ctx context.Context, platformID, identifier string) (string, error) {
	if !observability.IsLeaseID(identifier) {
		return "", invalidArg("lease_id: invalid")
	}
	var account string
	var resultErr error
	projector := s.projector()
	if err := s.withRuntimeReadContext(ctx, func() {
		leases, exists, err := s.Router.ListLeasesForPlatformContext(ctx, platformID)
		if err != nil {
			resultErr = err
			return
		}
		if !exists {
			resultErr = notFound("platform not found")
			return
		}
		for _, lease := range leases {
			if projector.MatchesLeaseID(platformID, lease.Account, identifier) {
				account = lease.Account
				return
			}
		}
	}); err != nil {
		return "", err
	}
	if resultErr != nil {
		return "", resultErr
	}
	if account == "" {
		return "", notFound("lease not found")
	}
	return account, nil
}

func (s *ControlPlaneService) GetLeaseByIdentifierContext(ctx context.Context, platformID, identifier string) (*LeaseResponse, error) {
	account, err := s.resolveLeaseIdentifierContext(ctx, platformID, identifier)
	if err != nil {
		return nil, err
	}
	return s.GetLeaseContext(ctx, platformID, account)
}

// InheritLeaseByPlatformName copies a valid parent lease onto newAccount.
func (s *ControlPlaneService) InheritLeaseByPlatformName(platformName, parentAccount, newAccount string) error {
	return s.inheritLeaseByPlatformNameAt(platformName, parentAccount, newAccount, time.Now())
}

// InheritLeaseByPlatformNameContext is the request-aware form of
// InheritLeaseByPlatformName.
func (s *ControlPlaneService) InheritLeaseByPlatformNameContext(ctx context.Context, platformName, parentAccount, newAccount string) error {
	return s.inheritLeaseByPlatformNameContextAt(ctx, platformName, parentAccount, newAccount, time.Now())
}

func (s *ControlPlaneService) inheritLeaseByPlatformNameAt(
	platformName, parentAccount, newAccount string,
	now time.Time,
) error {
	return s.inheritLeaseByPlatformNameContextAt(context.Background(), platformName, parentAccount, newAccount, now)
}

func (s *ControlPlaneService) inheritLeaseByPlatformNameContextAt(
	ctx context.Context,
	platformName, parentAccount, newAccount string,
	now time.Time,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	platformName = strings.TrimSpace(platformName)
	if platformName == "" {
		return invalidArg("platform: must be non-empty")
	}
	parentAccount = strings.TrimSpace(parentAccount)
	if parentAccount == "" {
		return invalidArg("parent_account: must be non-empty")
	}
	newAccount = strings.TrimSpace(newAccount)
	if newAccount == "" {
		return invalidArg("new_account: must be non-empty")
	}
	if parentAccount == newAccount {
		return invalidArg("new_account: must differ from parent_account")
	}

	if err := s.platformMu.lockContext(ctx); err != nil {
		return err
	}
	defer s.platformMu.unlock()

	plat, ok := s.Pool.GetPlatformByName(platformName)
	if !ok || plat == nil {
		return notFound("platform not found")
	}

	if hook := s.beforeLeaseInheritanceRouterCallHook; hook != nil {
		hook()
	}

	if err := s.withLeaseMutationContext(ctx, func(context.Context) error {
		if err := s.Router.InheritLeaseForPlatformExact(plat, parentAccount, newAccount, now); err != nil {
			switch {
			case errors.Is(err, routing.ErrPlatformNotFound):
				return notFound("platform not found")
			case errors.Is(err, routing.ErrLeaseNotFound):
				return notFound("parent lease not found")
			default:
				return internal("inherit lease", err)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// DeleteLease removes a single lease.
func (s *ControlPlaneService) DeleteLease(platformID, account string) error {
	return s.DeleteLeaseContext(context.Background(), platformID, account)
}

// DeleteLeaseContext removes one lease while honoring cancellation before the
// state-write mutation is admitted. Once admitted, the existing commit owner
// keeps the Router mutation and synchronous lease event together.
func (s *ControlPlaneService) DeleteLeaseContext(ctx context.Context, platformID, account string) error {
	if hook := s.beforeLeaseMutationAdmissionHook; hook != nil {
		hook()
	}
	return s.withLeaseMutationContext(ctx, func(context.Context) error {
		deleted, exists := s.Router.DeleteLeaseForPlatform(platformID, account)
		if !exists {
			return notFound("platform not found")
		}
		if !deleted {
			return notFound("lease not found")
		}
		return nil
	})
}

func (s *ControlPlaneService) DeleteLeaseByIdentifierContext(ctx context.Context, platformID, identifier string) error {
	account, err := s.resolveLeaseIdentifierContext(ctx, platformID, identifier)
	if err != nil {
		return err
	}
	return s.DeleteLeaseContext(ctx, platformID, account)
}

// DeleteAllLeases removes all leases for a platform.
func (s *ControlPlaneService) DeleteAllLeases(platformID string) error {
	return s.DeleteAllLeasesContext(context.Background(), platformID)
}

// DeleteAllLeasesContext removes all leases for a platform while honoring
// cancellation before the state-write mutation is admitted.
func (s *ControlPlaneService) DeleteAllLeasesContext(ctx context.Context, platformID string) error {
	return s.withLeaseMutationContext(ctx, func(context.Context) error {
		_, exists := s.Router.DeleteAllLeasesForPlatform(platformID)
		if !exists {
			return notFound("platform not found")
		}
		return nil
	})
}

// IPLoadEntry is the API response for IP load stats.
type IPLoadEntry struct {
	EgressIP   string `json:"egress_ip"`
	LeaseCount int64  `json:"lease_count"`
}

// GetIPLoad returns IP load stats for a platform.
func (s *ControlPlaneService) GetIPLoad(platformID string) ([]IPLoadEntry, error) {
	return s.GetIPLoadContext(context.Background(), platformID)
}

// GetIPLoadContext observes IP load only after admitting the caller into one
// complete runtime generation. A canceled request must not read while a
// subscription/runtime mutation is publishing its corresponding generation.
func (s *ControlPlaneService) GetIPLoadContext(ctx context.Context, platformID string) ([]IPLoadEntry, error) {
	var result []IPLoadEntry
	var readErr error
	if err := s.withRuntimeReadContext(ctx, func() {
		snapshot, exists, err := s.Router.SnapshotIPLoadForPlatformContext(ctx, platformID)
		if err != nil {
			readErr = err
			return
		}
		if !exists {
			readErr = notFound("platform not found")
			return
		}
		result = make([]IPLoadEntry, 0, len(snapshot))
		for ip, count := range snapshot {
			result = append(result, IPLoadEntry{
				EgressIP:   ip.String(),
				LeaseCount: count,
			})
		}
	}); err != nil {
		return nil, err
	}
	return result, readErr
}
