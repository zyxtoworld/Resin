package service

import (
	"context"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// PlatformRouteStateResponse is a bounded route-state observation for the
// platform detail page. Nodes and platform view membership are collected
// under one GlobalNodePool runtime read admission; Router lease and cooldown
// data are read under their own lifecycle locks during that admission. This
// prevents node/platform generation mixing, but is not one database-style
// atomic transaction across all three data sets.
type PlatformRouteStateResponse struct {
	PlatformID string                     `json:"platform_id"`
	ObservedAt string                     `json:"observed_at"`
	Nodes      []PlatformRouteNode        `json:"nodes"`
	Leases     PlatformLeasePage          `json:"leases"`
	Cooldowns  []PlatformCooldownSnapshot `json:"cooldowns"`
}

type PlatformLeasePage struct {
	Items      []LeaseResponse `json:"items"`
	Total      int             `json:"total"`
	Limit      int             `json:"limit"`
	HasMore    bool            `json:"has_more"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type PlatformRouteStateQuery struct {
	LeaseAccount string
	LeaseFuzzy   bool
	LeaseLimit   int
	LeaseCursor  string
	LeaseSortBy  string
	LeaseOrder   string
}

const maxPlatformRouteStateLeasePage = 1000

type PlatformRouteNode struct {
	NodeSummary
	Status     string `json:"status"`
	LeaseCount int    `json:"lease_count"`
}

type PlatformCooldownSnapshot struct {
	Scope    string `json:"scope"`
	NodeHash string `json:"node_hash,omitempty"`
	EgressIP string `json:"egress_ip,omitempty"`
	Until    string `json:"until"`
}

// GetPlatformRouteState returns a complete, request-independent snapshot.
func (s *ControlPlaneService) GetPlatformRouteState(platformID string) (*PlatformRouteStateResponse, error) {
	return s.GetPlatformRouteStateContext(context.Background(), platformID, PlatformRouteStateQuery{})
}

// GetPlatformRouteStateContext observes nodes, leases, and cooldowns during
// one runtime read admission. Cancellation only applies before read
// admission, just like the other generation-consistent control-plane reads.
func (s *ControlPlaneService) GetPlatformRouteStateContext(ctx context.Context, platformID string, query PlatformRouteStateQuery) (*PlatformRouteStateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if query.LeaseLimit > maxPlatformRouteStateLeasePage {
		return nil, invalidArg("lease limit exceeds route-state maximum")
	}
	limit := query.LeaseLimit
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPlatformRouteStateLeasePage {
		return nil, invalidArg("lease limit exceeds route-state maximum")
	}
	if s == nil || s.Router == nil {
		return nil, internal("route state", routing.ErrRouterStopped)
	}
	var result *PlatformRouteStateResponse
	var resultErr error
	if err := s.withRuntimeReadContext(ctx, func() {
		observedAt := time.Now().UTC()
		nodes, err := s.listNodes(NodeFilters{PlatformID: &platformID})
		if err != nil {
			resultErr = err
			return
		}
		leasePage, exists, err := s.Router.SnapshotLeasePageForPlatform(platformID, routing.LeasePageQuery{
			Account: query.LeaseAccount,
			Fuzzy:   query.LeaseFuzzy,
			Limit:   limit,
			Cursor:  query.LeaseCursor,
			SortBy:  query.LeaseSortBy,
			Desc:    query.LeaseOrder == "desc",
		})
		if err != nil {
			resultErr = invalidArg(err.Error())
			return
		}
		if !exists {
			resultErr = notFound("platform not found")
			return
		}
		cooldowns, exists := s.Router.SnapshotResponseCooldownsForPlatform(platformID, observedAt)
		if !exists {
			resultErr = notFound("platform not found")
			return
		}

		leaseCounts := make(map[string]int, len(leasePage.Counts))
		for hash, count := range leasePage.Counts {
			leaseCounts[hash.Hex()] = count
		}
		leaseResponses := PlatformLeasePage{
			Items:      make([]LeaseResponse, 0, len(leasePage.Items)),
			Total:      leasePage.Total,
			Limit:      limit,
			HasMore:    leasePage.HasMore,
			NextCursor: leasePage.NextCursor,
		}
		for _, item := range leasePage.Items {
			lease := model.Lease{
				PlatformID:     platformID,
				Account:        item.Account,
				NodeHash:       item.Lease.NodeHash.Hex(),
				EgressIP:       item.Lease.EgressIP.String(),
				CreatedAtNs:    item.Lease.CreatedAtNs,
				ExpiryNs:       item.Lease.ExpiryNs,
				LastAccessedNs: item.Lease.LastAccessedNs,
			}
			leaseResponses.Items = append(leaseResponses.Items, leaseToResponse(lease, s.resolveLeaseNodeTagFromHex(lease.NodeHash)))
		}

		cooldownResponses := make([]PlatformCooldownSnapshot, 0, len(cooldowns))
		for _, cooldown := range cooldowns {
			item := PlatformCooldownSnapshot{
				Scope: string(cooldown.Scope),
				Until: cooldown.Until.UTC().Format(time.RFC3339Nano),
			}
			if cooldown.NodeHash != node.Zero {
				item.NodeHash = cooldown.NodeHash.Hex()
			}
			if cooldown.EgressIP.IsValid() {
				item.EgressIP = cooldown.EgressIP.String()
			}
			cooldownResponses = append(cooldownResponses, item)
		}

		routeNodes := make([]PlatformRouteNode, 0, len(nodes))
		for _, node := range nodes {
			routeNodes = append(routeNodes, PlatformRouteNode{
				NodeSummary: node,
				Status:      platformRouteNodeStatus(node, cooldownResponses),
				LeaseCount:  leaseCounts[node.NodeHash],
			})
		}

		result = &PlatformRouteStateResponse{
			PlatformID: platformID,
			ObservedAt: observedAt.Format(time.RFC3339Nano),
			Nodes:      routeNodes,
			Leases:     leaseResponses,
			Cooldowns:  cooldownResponses,
		}
	}); err != nil {
		return nil, err
	}
	if resultErr != nil {
		return nil, resultErr
	}
	return result, nil
}

func platformRouteNodeStatus(node NodeSummary, cooldowns []PlatformCooldownSnapshot) string {
	if !node.Enabled {
		return "disabled"
	}
	if !node.HasOutbound {
		return "not_ready"
	}
	if node.CircuitOpenSince != nil {
		return "circuit_open"
	}
	for _, cooldown := range cooldowns {
		if cooldown.Scope == "route_entry" && cooldown.NodeHash == node.NodeHash ||
			cooldown.Scope == "egress_ip" && cooldown.EgressIP != "" && cooldown.EgressIP == node.EgressIP {
			return "cooling"
		}
	}
	return "available"
}
