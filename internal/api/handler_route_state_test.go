package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/observability"
	"github.com/Resinat/Resin/internal/service"
)

func TestLeaseProjectionsNeverExposeAccountCredential(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)
	platformID := mustCreatePlatform(t, srv, "lease-redaction")
	const accountCredential = "Bearer lease-token username=lease-user password=lease-password https://lease-user:lease-password@example.invalid/sub/path?token=lease-query X-API-Key: lease-api-key Cookie: session=lease-cookie"
	now := time.Now().UnixNano()
	if err := cp.Router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        accountCredential,
		NodeHash:       node.HashFromRawOptions([]byte(`{"type":"lease-redaction"}`)).Hex(),
		EgressIP:       "198.51.100.93",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	leaseID := ""
	for name, path := range map[string]string{
		"list":        "/api/v1/platforms/" + platformID + "/leases?limit=1",
		"route-state": "/api/v1/platforms/" + platformID + "/route-state?limit=1",
	} {
		rec := doJSONRequest(t, srv, http.MethodGet, path, nil, true)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status: got %d", name, rec.Code)
		}
		body := rec.Body.String()
		for _, canary := range []string{
			"lease-token",
			"lease-user",
			"lease-password",
			"lease-query",
			"lease-api-key",
			"lease-cookie",
		} {
			if strings.Contains(body, canary) {
				t.Fatalf("%s response exposed an account credential canary", name)
			}
		}
		if !strings.Contains(body, `"account_redacted":true`) || !strings.Contains(body, `"lease_id":"`) {
			t.Fatalf("%s response omitted the safe account projection", name)
		}
		if name == "list" {
			var page struct {
				Items []struct {
					LeaseID string `json:"lease_id"`
				} `json:"items"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil || len(page.Items) != 1 {
				t.Fatalf("list response did not contain one safe lease item")
			}
			leaseID = page.Items[0].LeaseID
		}
	}

	rec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformID+"/leases/"+url.PathEscape(leaseID), nil, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("get status: got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, canary := range []string{"lease-token", "lease-user", "lease-password", "lease-query", "lease-api-key", "lease-cookie"} {
		if strings.Contains(body, canary) {
			t.Fatalf("get response exposed an account credential canary")
		}
	}
}

func TestLeaseMutationRequiresOpaquePlatformBoundLeaseID(t *testing.T) {
	srv, cp, _ := newControlPlaneTestServer(t)
	platformA := mustCreatePlatform(t, srv, "lease-id-a")
	platformB := mustCreatePlatform(t, srv, "lease-id-b")
	const account = "Bearer mutation-secret"
	now := time.Now().UnixNano()
	hash := node.HashFromRawOptions([]byte(`{"type":"lease-id"}`)).Hex()
	for _, platformID := range []string{platformA, platformB} {
		if err := cp.Router.UpsertLease(model.Lease{
			PlatformID:     platformID,
			Account:        account,
			NodeHash:       hash,
			EgressIP:       "198.51.100.94",
			CreatedAtNs:    now,
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now,
		}); err != nil {
			t.Fatalf("seed lease: %v", err)
		}
	}

	list := doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformA+"/leases?limit=1", nil, true)
	if list.Code != http.StatusOK {
		t.Fatalf("list status: got %d", list.Code)
	}
	var page struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.Items[0].LeaseID == "" {
		t.Fatal("list did not return an opaque lease ID")
	}
	leaseID := page.Items[0].LeaseID

	for name, path := range map[string]string{
		"raw":            "/api/v1/platforms/" + platformA + "/leases/" + url.PathEscape(account),
		"tampered":       "/api/v1/platforms/" + platformA + "/leases/" + leaseID[:len(leaseID)-1] + "x",
		"cross-platform": "/api/v1/platforms/" + platformB + "/leases/" + leaseID,
	} {
		rec := doJSONRequest(t, srv, http.MethodDelete, path, nil, true)
		if rec.Code != http.StatusBadRequest && rec.Code != http.StatusNotFound {
			t.Fatalf("%s delete status: got %d", name, rec.Code)
		}
	}
	rawGet := doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+platformA+"/leases/"+url.PathEscape(account), nil, true)
	if rawGet.Code != http.StatusBadRequest {
		t.Fatalf("raw account get status: got %d, want %d", rawGet.Code, http.StatusBadRequest)
	}
	if cp.Router.ReadLease(model.LeaseKey{PlatformID: platformA, Account: account}) == nil ||
		cp.Router.ReadLease(model.LeaseKey{PlatformID: platformB, Account: account}) == nil {
		t.Fatal("invalid or cross-platform lease ID mutated a lease")
	}

	rec := doJSONRequest(t, srv, http.MethodDelete, "/api/v1/platforms/"+platformA+"/leases/"+url.PathEscape(leaseID), nil, true)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("opaque lease delete status: got %d", rec.Code)
	}
	if cp.Router.ReadLease(model.LeaseKey{PlatformID: platformA, Account: account}) != nil {
		t.Fatal("opaque lease delete did not remove the selected lease")
	}
	if cp.Router.ReadLease(model.LeaseKey{PlatformID: platformB, Account: account}) == nil {
		t.Fatal("opaque lease delete crossed the platform boundary")
	}
}

func TestLeaseOpaqueIDFailsClosedAcrossProjectorGenerationHTTP(t *testing.T) {
	srvA, cpA, runtimeCfg := newControlPlaneTestServer(t)
	platformID := mustCreatePlatform(t, srvA, "lease-projector-generation")
	account := "projector-generation-account"
	now := time.Now().UnixNano()
	if err := cpA.Router.UpsertLease(model.Lease{
		PlatformID:     platformID,
		Account:        account,
		NodeHash:       node.HashFromRawOptions([]byte(`{"type":"projector-generation"}`)).Hex(),
		EgressIP:       "198.51.100.95",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	listA := doJSONRequest(t, srvA, http.MethodGet, "/api/v1/platforms/"+platformID+"/leases?limit=1", nil, true)
	if listA.Code != http.StatusOK {
		t.Fatalf("projector A list status: got %d", listA.Code)
	}
	var pageA struct {
		Items []struct {
			LeaseID string `json:"lease_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listA.Body.Bytes(), &pageA); err != nil || len(pageA.Items) != 1 || pageA.Items[0].LeaseID == "" {
		t.Fatal("projector A did not return an opaque lease ID")
	}
	oldLeaseID := pageA.Items[0].LeaseID

	cpB := &service.ControlPlaneService{
		Engine:    cpA.Engine,
		Pool:      cpA.Pool,
		SubMgr:    cpA.SubMgr,
		Router:    cpA.Router,
		EnvCfg:    cpA.EnvCfg,
		Projector: observability.NewProjector([]byte("projector-generation-key-b-000000")),
	}
	srvB := NewServer(0, testAdminToken, service.SystemInfo{}, runtimeCfg, cpA.EnvCfg, cpB, 1<<20, nil, nil)

	getB := doJSONRequest(t, srvB, http.MethodGet, "/api/v1/platforms/"+platformID+"/leases/"+url.PathEscape(oldLeaseID), nil, true)
	if getB.Code != http.StatusNotFound && getB.Code != http.StatusBadRequest {
		t.Fatalf("old projector lease GET status: got %d, want 404 or 400", getB.Code)
	}
	deleteB := doJSONRequest(t, srvB, http.MethodDelete, "/api/v1/platforms/"+platformID+"/leases/"+url.PathEscape(oldLeaseID), nil, true)
	if deleteB.Code != http.StatusNotFound && deleteB.Code != http.StatusBadRequest {
		t.Fatalf("old projector lease DELETE status: got %d, want 404 or 400", deleteB.Code)
	}
	if cpA.Router.ReadLease(model.LeaseKey{PlatformID: platformID, Account: account}) == nil {
		t.Fatal("old projector ID mutated the lease through the new service")
	}
}

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
	otherPlatformID := mustCreatePlatform(t, srv, "route-state-api-other")
	crossPlatformRec := doJSONRequest(t, srv, http.MethodGet, "/api/v1/platforms/"+otherPlatformID+"/route-state?limit=1&cursor="+url.QueryEscape(nextCursor), nil, true)
	if crossPlatformRec.Code != http.StatusBadRequest {
		t.Fatalf("cross-platform cursor status: got %d, body=%s", crossPlatformRec.Code, crossPlatformRec.Body.String())
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
