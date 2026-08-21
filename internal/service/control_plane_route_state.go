package service

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
)

// PlatformRouteStateResponse is a bounded route-state observation for the
// platform detail page. Nodes and platform view membership are collected
// under one GlobalNodePool runtime read admission; Router lease and cooldown
// data are read under their own lifecycle locks during that admission. This
// prevents node/platform generation mixing, but is not one database-style
// atomic transaction across all three data sets.
type PlatformRouteStateResponse struct {
	PlatformID      string                     `json:"platform_id"`
	ObservedAt      string                     `json:"observed_at"`
	Nodes           []PlatformRouteNode        `json:"nodes"`
	NodesTotal      int                        `json:"nodes_total"`
	NodesLimit      int                        `json:"nodes_limit"`
	NodesHasMore    bool                       `json:"nodes_has_more"`
	NodesNextCursor string                     `json:"nodes_next_cursor,omitempty"`
	Leases          PlatformLeasePage          `json:"leases"`
	Cooldowns       []PlatformCooldownSnapshot `json:"cooldowns"`
	CooldownsTotal  int                        `json:"cooldowns_total"`
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
	NodeStatus   string
	NodeLimit    int
	NodeCursor   string
}

const maxPlatformRouteStateNodePage = 200
const maxPlatformRouteStateLeasePage = maxPlatformRouteStateNodePage

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

type platformRouteNodeHeap struct {
	items []PlatformRouteNode
}

func (h platformRouteNodeHeap) Len() int { return len(h.items) }

// Keep the greatest node hash at the root so only the requested top-k window
// is retained during a platform view walk.
func (h platformRouteNodeHeap) Less(i, j int) bool {
	return h.items[i].NodeHash > h.items[j].NodeHash
}

func (h platformRouteNodeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *platformRouteNodeHeap) Push(value any) {
	h.items = append(h.items, value.(PlatformRouteNode))
}

func (h *platformRouteNodeHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

func (s *ControlPlaneService) collectPlatformRouteNodePage(
	plat *platform.Platform,
	cooldowns routing.ResponseCooldownReadSnapshot,
	query PlatformRouteStateQuery,
	cursorHash *node.Hash,
	nodeLimit int,
) (page []PlatformRouteNode, total int, hasMore bool, hashes map[node.Hash]struct{}, egress map[string]struct{}, currentEntries map[node.Hash]*node.NodeEntry, err error) {
	if plat == nil {
		return nil, 0, false, nil, nil, nil, notFound("platform not found")
	}
	window := nodeLimit + 1
	selected := &platformRouteNodeHeap{}
	heap.Init(selected)
	cooldownNodeHashes := make(map[node.Hash]struct{})
	for _, item := range cooldowns.Items() {
		if item.Scope == platform.ResponseRuleScopeNode {
			cooldownNodeHashes[item.NodeHash] = struct{}{}
		}
	}
	currentEntries = make(map[node.Hash]*node.NodeEntry, len(cooldownNodeHashes))
	forEach := func(hash node.Hash, entry *node.NodeEntry) bool {
		if entry == nil {
			return true
		}
		if _, wantsCurrentEntry := cooldownNodeHashes[hash]; wantsCurrentEntry {
			currentEntries[hash] = entry
		}
		if query.NodeStatus == "" {
			total++
			hashHex := hash.Hex()
			if cursorHash != nil && hashHex <= cursorHash.Hex() {
				return true
			}
			if selected.Len() >= window && hashHex >= selected.items[0].NodeHash {
				return true
			}
			summary := s.nodeEntryToSummary(hash, entry)
			candidate := PlatformRouteNode{NodeSummary: summary, Status: routeNodeStatusForEntry(summary, entry, cooldowns)}
			if selected.Len() < window {
				heap.Push(selected, candidate)
			} else {
				heap.Pop(selected)
				heap.Push(selected, candidate)
			}
			return true
		}
		summary := s.nodeEntryToSummary(hash, entry)
		status := routeNodeStatusForEntry(summary, entry, cooldowns)
		if query.NodeStatus != "" && status != query.NodeStatus {
			return true
		}
		candidate := PlatformRouteNode{NodeSummary: summary, Status: status}
		total++
		if cursorHash != nil && candidate.NodeHash <= cursorHash.Hex() {
			return true
		}
		if selected.Len() < window {
			heap.Push(selected, candidate)
			return true
		}
		if candidate.NodeHash < selected.items[0].NodeHash {
			heap.Pop(selected)
			heap.Push(selected, candidate)
		}
		return true
	}
	plat.RangeViewEntries(forEach)

	items := make([]PlatformRouteNode, 0, selected.Len())
	for selected.Len() > 0 {
		items = append(items, heap.Pop(selected).(PlatformRouteNode))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].NodeHash < items[j].NodeHash })
	hasMore = len(items) > nodeLimit
	if len(items) > nodeLimit {
		items = items[:nodeLimit]
	}
	page = items
	hashes = make(map[node.Hash]struct{}, len(page))
	egress = make(map[string]struct{}, len(page))
	for _, item := range page {
		hash, err := node.ParseHex(item.NodeHash)
		if err == nil {
			hashes[hash] = struct{}{}
		}
		if item.EgressIP != "" {
			egress[item.EgressIP] = struct{}{}
		}
	}
	return page, total, hasMore, hashes, egress, currentEntries, nil
}

func routeNodeStatusForEntry(
	summary NodeSummary,
	entry *node.NodeEntry,
	cooldowns routing.ResponseCooldownReadSnapshot,
) string {
	if !summary.Enabled {
		return "disabled"
	}
	if !summary.HasOutbound {
		return "not_ready"
	}
	if summary.CircuitOpenSince != nil {
		return "circuit_open"
	}
	if cooldowns.IsCoolingForEntry(entry.Hash, entry, entry.GetEgressIP()) {
		return "cooling"
	}
	return "available"
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
	nodeLimit := query.NodeLimit
	if nodeLimit <= 0 {
		nodeLimit = 50
	}
	if nodeLimit > maxPlatformRouteStateNodePage {
		return nil, invalidArg("node limit exceeds route-state maximum")
	}
	if query.NodeStatus != "" {
		switch query.NodeStatus {
		case "available", "cooling", "circuit_open", "not_ready", "disabled":
		default:
			return nil, invalidArg("node status is invalid")
		}
	}
	if s == nil || s.Router == nil {
		return nil, internal("route state", routing.ErrRouterStopped)
	}
	var cursorHash node.Hash
	var nodeCursorHash *node.Hash
	var cursorGeneration uint64
	if strings.TrimSpace(query.NodeCursor) != "" {
		var err error
		cursorHash, cursorGeneration, err = routing.DecodeNodePageCursor(query.NodeCursor, platformID, query.NodeStatus, nodeLimit)
		if err != nil {
			return nil, invalidArg("invalid node route-state cursor")
		}
		nodeCursorHash = &cursorHash
	}
	// Platform replacement serializes the persisted model and pool view under
	// platformMu. Take the same owner before the runtime read so a route-state
	// response cannot copy the old node view and then observe Router state after
	// a new platform generation is published.
	if err := s.platformMu.lockContext(ctx); err != nil {
		return nil, err
	}
	defer s.platformMu.Unlock()

	var result *PlatformRouteStateResponse
	var resultErr error
	projector := s.projector()
	if err := s.withRuntimeReadContext(ctx, func() {
		observedAt := time.Now().UTC()
		plat, platformExists := s.Pool.GetPlatform(platformID)
		if !platformExists {
			resultErr = notFound("platform not found")
			return
		}

		var routeNodes []PlatformRouteNode
		var nodesTotal int
		var nodesHasMore bool
		var visibleHashes map[node.Hash]struct{}
		var visibleEgress map[string]struct{}
		var currentEntries map[node.Hash]*node.NodeEntry
		var cooldowns []routing.ResponseCooldownSnapshot
		cooldownTotal := 0
		var nodesNextCursor string
		cooldownExists, err := s.Router.WithPlatformResponseCooldownsContext(ctx, platformID, func(table *routing.ResponseCooldowns, generation uint64) error {
			if strings.TrimSpace(query.NodeCursor) != "" && cursorGeneration != generation {
				return routing.ErrNodePageCursorInvalid
			}
			cooldownSnapshot := routing.ResponseCooldownReadSnapshot{}
			if table != nil {
				cooldownSnapshot = table.ReadSnapshot(observedAt)
			}
			var collectErr error
			routeNodes, nodesTotal, nodesHasMore, visibleHashes, visibleEgress, currentEntries, collectErr = s.collectPlatformRouteNodePage(plat, cooldownSnapshot, query, nodeCursorHash, nodeLimit)
			if collectErr != nil {
				return collectErr
			}
			if nodesHasMore && len(routeNodes) > 0 {
				lastHash, parseErr := node.ParseHex(routeNodes[len(routeNodes)-1].NodeHash)
				if parseErr != nil {
					return parseErr
				}
				nodesNextCursor = routing.EncodeNodePageCursor(platformID, generation, query.NodeStatus, nodeLimit, lastHash)
				if nodesNextCursor == "" {
					return errors.New("failed to encode node route-state cursor")
				}
			}
			if hook := s.afterRouteStateNodesHook; hook != nil {
				hook()
			}
			isCurrent := func(item routing.ResponseCooldownSnapshot) bool {
				if item.Scope != platform.ResponseRuleScopeNode {
					return true
				}
				current, ok := currentEntries[item.NodeHash]
				if !ok || current == nil || (item.Entry != nil && current != item.Entry) {
					return false
				}
				return true
			}
			currentCooldowns := cooldownSnapshot.Filter(isCurrent)
			cooldowns, cooldownTotal, _ = currentCooldowns.SnapshotPageWithCount(0, nodeLimit, nil, func(item routing.ResponseCooldownSnapshot) bool {
				if item.Scope == platform.ResponseRuleScopeNode {
					_, ok := visibleHashes[item.NodeHash]
					return ok
				}
				_, ok := visibleEgress[item.EgressIP.String()]
				return ok
			})
			return nil
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				resultErr = err
				return
			}
			if errors.Is(err, routing.ErrNodePageCursorInvalid) {
				resultErr = invalidArg("invalid node route-state cursor")
			} else {
				resultErr = invalidArg(err.Error())
			}
			return
		}
		if !cooldownExists {
			resultErr = notFound("platform not found")
			return
		}

		leaseQuery := routing.LeasePageQuery{
			Account:         query.LeaseAccount,
			Fuzzy:           query.LeaseFuzzy,
			Limit:           limit,
			Cursor:          query.LeaseCursor,
			SortBy:          query.LeaseSortBy,
			Desc:            query.LeaseOrder == "desc",
			CountNodeHashes: visibleHashes,
		}
		if strings.TrimSpace(query.LeaseAccount) != "" {
			leaseQuery.AccountMatcher = func(rawAccount string) bool {
				return matchesAccountFilter(rawAccount, projector.RedactAccount(platformID, rawAccount), query.LeaseAccount, query.LeaseFuzzy)
			}
		}
		leasePage, leaseExists, err := s.Router.SnapshotLeasePageForPlatformContext(ctx, platformID, leaseQuery)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				resultErr = err
				return
			}
			resultErr = invalidArg(err.Error())
			return
		}
		if !leaseExists {
			resultErr = notFound("platform not found")
			return
		}

		leaseCounts := make(map[string]int, len(leasePage.Counts))
		for hash, count := range leasePage.Counts {
			leaseCounts[hash.Hex()] = count
		}
		for i := range routeNodes {
			routeNodes[i].LeaseCount = leaseCounts[routeNodes[i].NodeHash]
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
			leaseResponses.Items = append(leaseResponses.Items, leaseToResponse(projector, lease, s.resolveLeaseNodeTagFromHex(lease.NodeHash)))
		}

		cooldownResponses := make([]PlatformCooldownSnapshot, 0, len(cooldowns))
		for _, cooldown := range cooldowns {
			item := PlatformCooldownSnapshot{Scope: string(cooldown.Scope), Until: cooldown.Until.UTC().Format(time.RFC3339Nano)}
			if cooldown.NodeHash != node.Zero {
				item.NodeHash = cooldown.NodeHash.Hex()
			}
			if cooldown.EgressIP.IsValid() {
				item.EgressIP = cooldown.EgressIP.String()
			}
			cooldownResponses = append(cooldownResponses, item)
		}
		result = &PlatformRouteStateResponse{
			PlatformID:      platformID,
			ObservedAt:      observedAt.Format(time.RFC3339Nano),
			Nodes:           routeNodes,
			NodesTotal:      nodesTotal,
			NodesLimit:      nodeLimit,
			NodesHasMore:    nodesHasMore,
			NodesNextCursor: nodesNextCursor,
			Leases:          leaseResponses,
			Cooldowns:       cooldownResponses,
			CooldownsTotal:  cooldownTotal,
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
