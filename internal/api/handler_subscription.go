package api

import (
	"net/http"
	"slices"
	"strings"

	"github.com/Resinat/Resin/internal/service"
)

func subscriptionMatchesKeyword(s service.SubscriptionResponse, keyword string) bool {
	contains := func(v string) bool {
		return strings.Contains(strings.ToLower(v), keyword)
	}

	return contains(s.ID) || contains(s.Name) || contains(s.URL) || contains(s.SourceType)
}

func filterSubscriptionsByKeyword(subs []service.SubscriptionResponse, rawKeyword string) []service.SubscriptionResponse {
	keyword := strings.ToLower(strings.TrimSpace(rawKeyword))
	if keyword == "" {
		return subs
	}
	filtered := make([]service.SubscriptionResponse, 0, len(subs))
	for _, sub := range subs {
		if subscriptionMatchesKeyword(sub, keyword) {
			filtered = append(filtered, sub)
		}
	}
	return filtered
}

func subscriptionSortKey(sortBy string, s service.SubscriptionResponse) string {
	switch sortBy {
	case "created_at":
		return s.CreatedAt
	case "last_checked":
		return s.LastChecked
	case "last_updated":
		return s.LastUpdated
	default:
		return s.Name
	}
}

func sortSubscriptionResponses(subs []service.SubscriptionResponse, sorting Sorting) {
	slices.SortStableFunc(subs, func(a, b service.SubscriptionResponse) int {
		comparison := strings.Compare(subscriptionSortKey(sorting.SortBy, a), subscriptionSortKey(sorting.SortBy, b))
		if sorting.SortOrder == "desc" {
			comparison = -comparison
		}
		if comparison != 0 {
			return comparison
		}
		// The manager's concurrent map does not promise iteration order. Keep
		// offset pagination deterministic when the requested field ties.
		return strings.Compare(a.ID, b.ID)
	})
}

// HandleListSubscriptions returns a handler for GET /api/v1/subscriptions.
func HandleListSubscriptions(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		enabled, ok := parseBoolQueryOrWriteInvalid(w, r, "enabled")
		if !ok {
			return
		}
		sorting, ok := parseSortingOrWriteInvalid(
			w,
			r,
			[]string{"name", "created_at", "last_checked", "last_updated"},
			"created_at",
			"asc",
		)
		if !ok {
			return
		}
		pg, ok := parsePaginationOrWriteInvalid(w, r)
		if !ok {
			return
		}

		subs, err := cp.ListSubscriptionsContext(r.Context(), enabled)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		subs = filterSubscriptionsByKeyword(subs, r.URL.Query().Get("keyword"))
		sortSubscriptionResponses(subs, sorting)
		WritePage(w, http.StatusOK, subs, pg)
	}
}

// HandleGetSubscription returns a handler for GET /api/v1/subscriptions/{id}.
func HandleGetSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		s, err := cp.GetSubscriptionContext(r.Context(), id)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, s)
	}
}

// HandleCreateSubscription returns a handler for POST /api/v1/subscriptions.
func HandleCreateSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req service.CreateSubscriptionRequest
		if err := DecodeBody(r, &req); err != nil {
			writeDecodeBodyError(w, err)
			return
		}
		s, err := cp.CreateSubscriptionContext(r.Context(), req)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusCreated, s)
	}
}

// HandleUpdateSubscription returns a handler for PATCH /api/v1/subscriptions/{id}.
func HandleUpdateSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		body, ok := readRawBodyOrWriteInvalid(w, r)
		if !ok {
			return
		}
		s, err := cp.UpdateSubscriptionContext(r.Context(), id, body)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, s)
	}
}

// HandleDeleteSubscription returns a handler for DELETE /api/v1/subscriptions/{id}.
func HandleDeleteSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		if err := cp.DeleteSubscriptionContext(r.Context(), id); err != nil {
			writeServiceError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// HandleRefreshSubscription returns a handler for POST /api/v1/subscriptions/{id}/actions/refresh.
func HandleRefreshSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		if err := cp.RefreshSubscriptionContext(r.Context(), id); err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// HandleCleanupSubscriptionCircuitOpenNodes returns a handler for
// POST /api/v1/subscriptions/{id}/actions/cleanup-circuit-open-nodes.
func HandleCleanupSubscriptionCircuitOpenNodes(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		cleanedCount, err := cp.CleanupSubscriptionCircuitOpenNodesContext(r.Context(), id)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]int{"cleaned_count": cleanedCount})
	}
}
