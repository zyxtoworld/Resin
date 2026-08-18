package api

import "net/http"

// HealthzStatus is the small liveness projection returned by /healthz.
// A degraded status keeps the process reachable while making node-local
// bootstrap failures visible to operators.
type HealthzStatus struct {
	Status        string `json:"status"`
	DegradedNodes int    `json:"degraded_nodes,omitempty"`
}

// HandleHealthz returns a handler for GET /healthz.
// No authentication is required.
func HandleHealthz() http.HandlerFunc {
	return HandleHealthzWithStatus(nil)
}

// HandleHealthzWithStatus returns a handler whose status is obtained at
// request time. A nil provider preserves the ordinary healthy response.
func HandleHealthzWithStatus(provider func() HealthzStatus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := HealthzStatus{Status: "ok"}
		if provider != nil {
			status = provider()
			if status.Status == "" {
				status.Status = "ok"
			}
		}
		WriteJSON(w, http.StatusOK, status)
	}
}
