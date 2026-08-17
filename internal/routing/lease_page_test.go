package routing

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

func TestRouter_SnapshotLeasePageFiltersWithStableCursor(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("lease-page", "Lease Page", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	now := time.Now().UnixNano()
	hashA := node.HashFromRawOptions([]byte(`{"id":"lease-page-a"}`)).Hex()
	hashB := node.HashFromRawOptions([]byte(`{"id":"lease-page-b"}`)).Hex()
	for account, hash := range map[string]string{
		"alpha":     hashA,
		"alpha-two": hashB,
		"beta":      hashA,
	} {
		if err := router.UpsertLease(model.Lease{
			PlatformID:     plat.ID,
			Account:        account,
			NodeHash:       hash,
			EgressIP:       map[string]string{hashA: "198.51.100.101", hashB: "198.51.100.102"}[hash],
			CreatedAtNs:    now,
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now,
		}); err != nil {
			t.Fatalf("seed lease %s: %v", account, err)
		}
	}

	page, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, LeasePageQuery{
		Account: "ALPHA",
		Fuzzy:   true,
		Limit:   1,
		SortBy:  "account",
	})
	if err != nil || !ok {
		t.Fatalf("snapshot lease page: page=%#v ok=%v err=%v", page, ok, err)
	}
	if page.Total != 2 || len(page.Items) != 1 || page.Items[0].Account != "alpha" || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("filtered first page = %#v, want first alpha item and next cursor", page)
	}
	next, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, LeasePageQuery{
		Account: "ALPHA",
		Fuzzy:   true,
		Limit:   1,
		SortBy:  "account",
		Cursor:  page.NextCursor,
	})
	if err != nil || !ok || next.Total != 2 || len(next.Items) != 1 || next.Items[0].Account != "alpha-two" || next.HasMore {
		t.Fatalf("filtered second page = %#v ok=%v err=%v", next, ok, err)
	}
	parsedA, err := node.ParseHex(hashA)
	if err != nil {
		t.Fatalf("parse hash A: %v", err)
	}
	parsedB, err := node.ParseHex(hashB)
	if err != nil {
		t.Fatalf("parse hash B: %v", err)
	}
	if page.Counts[parsedA] != 2 || page.Counts[parsedB] != 1 {
		t.Fatalf("lease counts = %#v", page.Counts)
	}
	if _, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, LeasePageQuery{Limit: maxLeasePageLimit + 1}); err != ErrLeasePageTooWide || ok {
		t.Fatalf("wide page = ok=%v err=%v, want bounded error", ok, err)
	}
}

func TestRouter_SnapshotLeasePageCoversBeyondBoundedWindowWithAscDescCursors(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("lease-page-large", "Lease Page Large", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	now := time.Now().UnixNano()
	hash := node.HashFromRawOptions([]byte(`{"id":"lease-page-large-node"}`)).Hex()
	const totalLeases = 1500
	for i := 0; i < totalLeases; i++ {
		if err := router.UpsertLease(model.Lease{
			PlatformID:     plat.ID,
			Account:        fmt.Sprintf("account-%04d", i),
			NodeHash:       hash,
			EgressIP:       "198.51.100.110",
			CreatedAtNs:    now,
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now + int64(i),
		}); err != nil {
			t.Fatalf("seed lease %d: %v", i, err)
		}
	}

	walk := func(label string, query LeasePageQuery, expected int) {
		page, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, query)
		if err != nil || !ok {
			t.Fatalf("first cursor page %s: page=%#v ok=%v err=%v", label, page, ok, err)
		}
		seen := make(map[string]struct{}, expected)
		for {
			if page.Total != expected || len(page.Items) == 0 || page.Counts[page.Items[0].Lease.NodeHash] != totalLeases {
				t.Fatalf("cursor page %s = total:%d items:%d counts:%#v", label, page.Total, len(page.Items), page.Counts)
			}
			for _, item := range page.Items {
				if _, exists := seen[item.Account]; exists {
					t.Fatalf("duplicate account %s: %s", label, item.Account)
				}
				seen[item.Account] = struct{}{}
			}
			if !page.HasMore {
				break
			}
			query.Cursor = page.NextCursor
			page, ok, err = router.SnapshotLeasePageForPlatform(plat.ID, query)
			if err != nil || !ok {
				t.Fatalf("cursor page %s: page=%#v ok=%v err=%v", label, page, ok, err)
			}
		}
		if len(seen) != expected {
			t.Fatalf("cursor coverage %s = %d, want %d", label, len(seen), expected)
		}
	}

	for _, desc := range []bool{false, true} {
		walk(fmt.Sprintf("all desc=%v", desc), LeasePageQuery{Limit: 50, SortBy: "account", Desc: desc}, totalLeases)
		filteredTotal := 0
		for i := 0; i < totalLeases; i++ {
			if strings.Contains(fmt.Sprintf("account-%04d", i), "account-1") {
				filteredTotal++
			}
		}
		walk(fmt.Sprintf("filtered desc=%v", desc), LeasePageQuery{
			Account: "account-1",
			Fuzzy:   true,
			Limit:   50,
			SortBy:  "account",
			Desc:    desc,
		}, filteredTotal)
	}

	if _, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, LeasePageQuery{Limit: 50, SortBy: "account", Cursor: "not-a-cursor"}); err != ErrLeaseCursorInvalid || ok {
		t.Fatalf("invalid cursor = ok=%v err=%v, want cursor error", ok, err)
	}
}

func TestRouter_SnapshotLeasePageRejectsMalformedAndMismatchedCursors(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("lease-page-contract", "Lease Page Contract", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	now := time.Now().UnixNano()
	hash := node.HashFromRawOptions([]byte(`{"id":"lease-page-contract-node"}`)).Hex()
	for i := 0; i < 2; i++ {
		if err := router.UpsertLease(model.Lease{
			PlatformID:     plat.ID,
			Account:        fmt.Sprintf("account-%d", i),
			NodeHash:       hash,
			EgressIP:       "198.51.100.120",
			CreatedAtNs:    now,
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now + int64(i),
		}); err != nil {
			t.Fatalf("seed lease %d: %v", i, err)
		}
	}

	query := LeasePageQuery{Account: "account", Fuzzy: true, Limit: 1, SortBy: "account"}
	first, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, query)
	if err != nil || !ok || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first cursor page = %#v ok=%v err=%v", first, ok, err)
	}

	truncated := first.NextCursor[:len(first.NextCursor)/2]
	tamperedBytes, err := base64.RawURLEncoding.DecodeString(first.NextCursor)
	if err != nil {
		t.Fatalf("decode generated cursor: %v", err)
	}
	tamperedBytes[len(tamperedBytes)-2] ^= 1
	tampered := base64.RawURLEncoding.EncodeToString(tamperedBytes)
	for name, cursor := range map[string]string{
		"truncated": truncated,
		"tampered":  tampered,
		"garbage":   "not-a-cursor",
	} {
		_, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, LeasePageQuery{Limit: 1, SortBy: "account", Cursor: cursor})
		if err != ErrLeaseCursorInvalid || ok {
			t.Fatalf("%s cursor = ok=%v err=%v, want invalid cursor", name, ok, err)
		}
	}

	for name, changed := range map[string]LeasePageQuery{
		"sort":   {Limit: 1, SortBy: "expiry", Cursor: first.NextCursor, Account: "account", Fuzzy: true},
		"order":  {Limit: 1, SortBy: "account", Desc: true, Cursor: first.NextCursor, Account: "account", Fuzzy: true},
		"filter": {Limit: 1, SortBy: "account", Cursor: first.NextCursor, Account: "account-0", Fuzzy: true},
		"fuzzy":  {Limit: 1, SortBy: "account", Cursor: first.NextCursor, Account: "account", Fuzzy: false},
		"limit":  {Limit: 2, SortBy: "account", Cursor: first.NextCursor, Account: "account", Fuzzy: true},
	} {
		_, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, changed)
		if err != ErrLeaseCursorInvalid || ok {
			t.Fatalf("%s cursor contract = ok=%v err=%v, want invalid cursor", name, ok, err)
		}
	}
}

func TestRouter_SnapshotLeasePageRejectsCursorAfterProcessSecretRotation(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("lease-page-restart", "Lease Page Restart", nil, nil)
	pool.addPlatform(plat)
	router := newTestRouter(pool, nil)
	now := time.Now().UnixNano()
	hash := node.HashFromRawOptions([]byte(`{"id":"lease-page-restart-node"}`)).Hex()
	if err := router.UpsertLease(model.Lease{
		PlatformID:     plat.ID,
		Account:        "restart-account-1",
		NodeHash:       hash,
		EgressIP:       "198.51.100.121",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	}); err != nil {
		t.Fatalf("seed first lease: %v", err)
	}
	if err := router.UpsertLease(model.Lease{
		PlatformID:     plat.ID,
		Account:        "restart-account-2",
		NodeHash:       hash,
		EgressIP:       "198.51.100.121",
		CreatedAtNs:    now,
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now + 1,
	}); err != nil {
		t.Fatalf("seed second lease: %v", err)
	}
	query := LeasePageQuery{Limit: 1, SortBy: "account"}
	page, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, query)
	if err != nil || !ok || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("initial page = %#v ok=%v err=%v", page, ok, err)
	}

	previousSecret := leaseCursorSecret
	leaseCursorSecret = newLeaseCursorSecret()
	t.Cleanup(func() { leaseCursorSecret = previousSecret })
	query.Cursor = page.NextCursor
	if _, ok, err := router.SnapshotLeasePageForPlatform(plat.ID, query); err != ErrLeaseCursorInvalid || ok {
		t.Fatalf("rotated-secret cursor = ok=%v err=%v, want invalid cursor", ok, err)
	}
}
