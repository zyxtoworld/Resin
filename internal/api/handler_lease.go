package api

import (
	"cmp"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/Resinat/Resin/internal/observability"
	"github.com/Resinat/Resin/internal/service"
)

func validateAccountPath(r *http.Request) (string, error) {
	leaseID := PathParam(r, "account")
	if strings.TrimSpace(leaseID) == "" {
		return "", invalidArgumentError("lease_id: must be non-empty")
	}
	if !observability.IsLeaseID(leaseID) {
		return "", invalidArgumentError("lease_id: invalid")
	}
	return leaseID, nil
}

func leaseSortKey(sortBy string, l service.LeaseResponse) string {
	switch sortBy {
	case "expiry":
		return l.Expiry
	case "last_accessed":
		return l.LastAccessed
	default:
		return l.Account
	}
}

func sortLeaseResponses(leases []service.LeaseResponse, sorting Sorting) {
	slices.SortStableFunc(leases, func(a, b service.LeaseResponse) int {
		comparison := strings.Compare(leaseSortKey(sorting.SortBy, a), leaseSortKey(sorting.SortBy, b))
		if sorting.SortOrder == "desc" {
			comparison = -comparison
		}
		if comparison != 0 {
			return comparison
		}
		// Accounts are the lease identity; use them as a deterministic page tie-breaker.
		return strings.Compare(a.Account, b.Account)
	})
}

func compareIPLoadEntries(sortBy string, a, b service.IPLoadEntry) int {
	switch sortBy {
	case "egress_ip":
		return strings.Compare(a.EgressIP, b.EgressIP)
	default: // lease_count
		order := cmp.Compare(a.LeaseCount, b.LeaseCount)
		if order != 0 {
			return order
		}
		return strings.Compare(a.EgressIP, b.EgressIP)
	}
}

func sortIPLoadEntries(entries []service.IPLoadEntry, sorting Sorting) {
	slices.SortStableFunc(entries, func(a, b service.IPLoadEntry) int {
		return applySortOrder(compareIPLoadEntries(sorting.SortBy, a, b), sorting.SortOrder)
	})
}

// HandleListLeases returns a handler for GET /api/v1/platforms/{id}/leases.
func HandleListLeases(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformID, ok := requireUUIDPathParam(w, r, "id", "platform_id")
		if !ok {
			return
		}

		fuzzy, ok := parseStrictBoolQuery(w, r, "fuzzy")
		if !ok {
			return
		}
		useFuzzyAccountMatch := fuzzy != nil && *fuzzy
		accountQuery := r.URL.Query().Get("account")
		accountFilter := strings.TrimSpace(accountQuery)
		if accountQuery != "" && accountFilter == "" {
			writeInvalidArgument(w, "account query: must be non-empty when provided")
			return
		}
		sorting, ok := parseSortingOrWriteInvalid(w, r, []string{"account", "expiry", "last_accessed"}, "expiry", "asc")
		if !ok {
			return
		}
		pg, ok := parsePaginationOrWriteInvalid(w, r)
		if !ok {
			return
		}

		leases, err := cp.ListLeasesContext(r.Context(), platformID)
		if err != nil {
			writeServiceError(w, err)
			return
		}

		// Optional account filter.
		if accountQuery != "" {
			filtered := make([]service.LeaseResponse, 0, len(leases))
			for _, l := range leases {
				if l.MatchesAccountFilter(accountFilter, useFuzzyAccountMatch) {
					filtered = append(filtered, l)
				}
			}
			leases = filtered
		}

		sortLeaseResponses(leases, sorting)
		WritePage(w, http.StatusOK, leases, pg)
	}
}

// HandleGetLease returns a handler for GET /api/v1/platforms/{id}/leases/{account}.
func HandleGetLease(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformID, ok := requireUUIDPathParam(w, r, "id", "platform_id")
		if !ok {
			return
		}
		account, err := validateAccountPath(r)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		lease, err := cp.GetLeaseByIdentifierContext(r.Context(), platformID, account)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, lease)
	}
}

// HandleDeleteLease returns a handler for DELETE /api/v1/platforms/{id}/leases/{account}.
func HandleDeleteLease(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformID, ok := requireUUIDPathParam(w, r, "id", "platform_id")
		if !ok {
			return
		}
		account, err := validateAccountPath(r)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		if err := cp.DeleteLeaseByIdentifierContext(r.Context(), platformID, account); err != nil {
			writeServiceError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// HandleDeleteAllLeases returns a handler for DELETE /api/v1/platforms/{id}/leases.
func HandleDeleteAllLeases(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformID, ok := requireUUIDPathParam(w, r, "id", "platform_id")
		if !ok {
			return
		}
		if err := cp.DeleteAllLeasesContext(r.Context(), platformID); err != nil {
			writeServiceError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// HandleIPLoad returns a handler for GET /api/v1/platforms/{id}/ip-load.
func HandleIPLoad(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformID, ok := requireUUIDPathParam(w, r, "id", "platform_id")
		if !ok {
			return
		}

		sorting, ok := parseSortingOrWriteInvalid(w, r, []string{"egress_ip", "lease_count"}, "lease_count", "desc")
		if !ok {
			return
		}
		pg, ok := parsePaginationOrWriteInvalid(w, r)
		if !ok {
			return
		}

		entries, err := cp.GetIPLoadContext(r.Context(), platformID)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		sortIPLoadEntries(entries, sorting)
		WritePage(w, http.StatusOK, entries, pg)
	}
}

// HandlePlatformRouteState returns a bounded route-state observation taken
// during runtime read admission; Router lease and cooldown data retain their
// own lifecycle-lock semantics rather than forming one cross-store snapshot.
func HandlePlatformRouteState(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformID, ok := requireUUIDPathParam(w, r, "id", "platform_id")
		if !ok {
			return
		}
		fuzzy, ok := parseStrictBoolQuery(w, r, "fuzzy")
		if !ok {
			return
		}
		sorting, ok := parseSortingOrWriteInvalid(w, r, []string{"account", "expiry", "last_accessed"}, "expiry", "asc")
		if !ok {
			return
		}
		if r.URL.Query().Get("offset") != "" {
			writeInvalidArgument(w, "offset: not supported for route-state leases; use cursor")
			return
		}
		limit, ok := parseRequestLogLimitQuery(w, r)
		if !ok {
			return
		}
		nodeLimit, ok := parseRouteStateNodePage(w, r)
		if !ok {
			return
		}
		state, err := cp.GetPlatformRouteStateContext(r.Context(), platformID, service.PlatformRouteStateQuery{
			LeaseAccount: r.URL.Query().Get("account"),
			LeaseFuzzy:   fuzzy != nil && *fuzzy,
			LeaseLimit:   limit,
			LeaseCursor:  r.URL.Query().Get("cursor"),
			LeaseSortBy:  sorting.SortBy,
			LeaseOrder:   sorting.SortOrder,
			NodeStatus:   r.URL.Query().Get("node_status"),
			NodeLimit:    nodeLimit,
			NodeCursor:   r.URL.Query().Get("node_cursor"),
		})
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, state)
	}
}

func parseRouteStateNodePage(w http.ResponseWriter, r *http.Request) (int, bool) {
	if r.URL.Query().Get("node_offset") != "" {
		writeInvalidArgument(w, "node_offset: not supported; use node_cursor")
		return 0, false
	}
	limit := 50
	if value := r.URL.Query().Get("node_limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			writeInvalidArgument(w, "node_limit: must be a non-negative integer")
			return 0, false
		}
		if parsed > 200 {
			writeInvalidArgument(w, "node_limit: must be <= 200")
			return 0, false
		}
		limit = parsed
	}
	if limit == 0 {
		limit = 50
	}
	return limit, true
}
