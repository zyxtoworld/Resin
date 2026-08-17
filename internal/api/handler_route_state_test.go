package api

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
)

func TestPlatformRouteStateHandlerReturnsBoundedGenerationSnapshot(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)
	platformID := mustCreatePlatform(t, srv, "route-state-api")
	now := time.Now().UnixNano()
	if err := cp.Router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        "route-state-account",
		NodeHash:       node.HashFromRawOptions([]byte(`{"type":"route-state"}`)).Hex(),
		EgressIP:       "198.51.100.91",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("seed lease: %v", err)
	}
	if err := cp.Router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        "route-state-account-2",
		NodeHash:       node.HashFromRawOptions([]byte(`{"type":"route-state-2"}`)).Hex(),
		EgressIP:       "198.51.100.92",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("seed second lease: %v", err)
	}

	rec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformID+"/route-state?limit=1", nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("route state status: got %d, body=%s", rec.Code, rec.Body.String())
	}
	body := decodeJSONMap(t, rec)
	leases, ok := body["leases"].(map[string]any)
	if !ok {
		t.Fatalf("route state leases shape: %#v", body["leases"])
	}
	if leases["total"] != float64(2) || leases["limit"] != float64(1) || leases["has_more"] != true {
		t.Fatalf("route state lease page: %#v", leases)
	}
	nextCursor, ok := leases["next_cursor"].(string)
	if !ok || nextCursor == "" {
		t.Fatalf("route state missing next cursor: %#v", leases)
	}
	if _, present := body["remaining_seconds"]; present {
		t.Fatal("route state exposes stale remaining_seconds")
	}
	cooldowns, ok := body["cooldowns"].([]any)
	if !ok {
		t.Fatalf("route state cooldowns shape: %#v", body["cooldowns"])
	}
	for _, raw := range cooldowns {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("route state cooldown item shape: %#v", raw)
		}
		if _, present := item["remaining_seconds"]; present {
			t.Fatalf("cooldown exposes stale remaining_seconds: %#v", item)
		}
	}
	offsetRec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformID+"/route-state?offset=1", nil, true)
	if offsetRec.Code != http.StatusBadRequest {
		t.Fatalf("route state offset status: got %d, body=%s", offsetRec.Code, offsetRec.Body.String())
	}

	tamperedBytes, err := base64.RawURLEncoding.DecodeString(nextCursor)
	if err != nil {
		t.Fatalf("decode generated cursor: %v", err)
	}
	tamperedBytes[len(tamperedBytes)-2] ^= 1
	badCursors := map[string]string{
		"garbage":   "not-a-cursor",
		"truncated": nextCursor[:len(nextCursor)/2],
		"tampered":  base64.RawURLEncoding.EncodeToString(tamperedBytes),
	}
	for name, cursor := range badCursors {
		rec = doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformID+"/route-state?limit=1&cursor="+url.QueryEscape(cursor), nil, true)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s cursor status: got %d, body=%s", name, rec.Code, rec.Body.String())
		}
	}
	for name, suffix := range map[string]string{
		"page-size": "limit=2",
		"sort":      "limit=1&sort_by=account",
		"filter":    "limit=1&account=other",
		"fuzzy":     "limit=1&fuzzy=true",
	} {
		rec = doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformID+"/route-state?"+suffix+"&cursor="+url.QueryEscape(nextCursor), nil, true)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s cursor contract status: got %d, body=%s", name, rec.Code, rec.Body.String())
		}
	}
}
