package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
)

// ------------------------------------------------------------------
// Leases
// ------------------------------------------------------------------

// LeaseResponse is the API response for a lease.
type LeaseResponse struct {
	PlatformID   string `json:"platform_id"`
	Account      string `json:"account"`
	NodeHash     string `json:"node_hash"`
	NodeTag      string `json:"node_tag"`
	EgressIP     string `json:"egress_ip"`
	Expiry       string `json:"expiry"`
	LastAccessed string `json:"last_accessed"`
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

func leaseToResponse(lease model.Lease, nodeTag string) LeaseResponse {
	return LeaseResponse{
		PlatformID:   lease.PlatformID,
		Account:      lease.Account,
		NodeHash:     lease.NodeHash,
		NodeTag:      nodeTag,
		EgressIP:     lease.EgressIP,
		Expiry:       time.Unix(0, lease.ExpiryNs).UTC().Format(time.RFC3339Nano),
		LastAccessed: time.Unix(0, lease.LastAccessedNs).UTC().Format(time.RFC3339Nano),
	}
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
	if err := s.withRuntimeReadContext(ctx, func() {
		leases, exists := s.Router.ListLeasesForPlatform(platformID)
		if !exists {
			resultErr = notFound("platform not found")
			return
		}
		result = make([]LeaseResponse, 0, len(leases))
		for _, lease := range leases {
			result = append(result, leaseToResponse(lease, s.resolveLeaseNodeTagFromHex(lease.NodeHash)))
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
	if err := s.withRuntimeReadContext(ctx, func() {
		ml, exists := s.Router.ReadLeaseForPlatform(model.LeaseKey{PlatformID: platformID, Account: account})
		if !exists {
			resultErr = notFound("platform not found")
			return
		}
		if ml == nil {
			resultErr = notFound("lease not found")
			return
		}
		resp := leaseToResponse(*ml, s.resolveLeaseNodeTagFromHex(ml.NodeHash))
		result = &resp
	}); err != nil {
		return nil, err
	}
	if resultErr != nil {
		return nil, resultErr
	}
	return result, nil
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
	snapshot, exists := s.Router.SnapshotIPLoadForPlatform(platformID)
	if !exists {
		return nil, notFound("platform not found")
	}
	result := make([]IPLoadEntry, 0, len(snapshot))
	for ip, count := range snapshot {
		result = append(result, IPLoadEntry{
			EgressIP:   ip.String(),
			LeaseCount: count,
		})
	}
	return result, nil
}
