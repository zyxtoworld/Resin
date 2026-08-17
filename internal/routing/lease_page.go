package routing

import (
	"bytes"
	"container/heap"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/Resinat/Resin/internal/node"
)

const maxLeasePageLimit = 1000

var (
	ErrLeasePageTooWide   = errors.New("lease page limit exceeds route-state maximum")
	ErrLeaseCursorInvalid = errors.New("invalid lease route-state cursor")
)

var leaseCursorSecret = newLeaseCursorSecret()

func newLeaseCursorSecret() []byte {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic("routing: cannot initialize lease cursor secret")
	}
	return secret
}

// LeasePageQuery describes a bounded cursor query performed while the Router
// lifecycle read owner is held. The cursor is exclusive and binds the
// requested platform, filter, sort, and page size; changing any of them
// requires a new empty cursor rather than silently reusing an incompatible
// position. platformID is filled by SnapshotLeasePageForPlatform and is not
// caller-controlled.
type LeasePageQuery struct {
	platformID string
	Account    string
	Fuzzy      bool
	Limit      int
	SortBy     string
	Desc       bool
	Cursor     string
}

type LeasePageItem struct {
	Account string
	Lease   Lease
}

type LeasePage struct {
	Items      []LeasePageItem
	Total      int
	Counts     map[node.Hash]int
	HasMore    bool
	NextCursor string
}

type leasePageCursor struct {
	Version        int    `json:"version"`
	PlatformID     string `json:"platform_id"`
	SortBy         string `json:"sort_by"`
	Desc           bool   `json:"desc"`
	FilterAccount  string `json:"filter_account"`
	Fuzzy          bool   `json:"fuzzy"`
	Limit          int    `json:"limit"`
	LastAccount    string `json:"last_account"`
	NodeHash       string `json:"node_hash"`
	ExpiryNs       int64  `json:"expiry_ns"`
	LastAccessedNs int64  `json:"last_accessed_ns"`
	MAC            string `json:"mac"`
}

type leasePageCandidate struct {
	LeasePageItem
}

type leasePageHeap struct {
	items []leasePageCandidate
	query LeasePageQuery
}

func (h leasePageHeap) Len() int { return len(h.items) }

// Less puts the worst retained item at the root. The heap therefore keeps
// only the earliest limit+1 items after the cursor in the requested order.
func (h leasePageHeap) Less(i, j int) bool {
	return compareLeasePageItems(h.items[i].LeasePageItem, h.items[j].LeasePageItem, h.query) > 0
}

func (h leasePageHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *leasePageHeap) Push(value any) {
	h.items = append(h.items, value.(leasePageCandidate))
}

func (h *leasePageHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

func normalizeLeaseSort(sortBy string) string {
	switch sortBy {
	case "account", "last_accessed", "expiry":
		return sortBy
	default:
		return "expiry"
	}
}

func compareLeasePageItems(left, right LeasePageItem, query LeasePageQuery) int {
	compare := 0
	switch normalizeLeaseSort(query.SortBy) {
	case "account":
		compare = strings.Compare(left.Account, right.Account)
	case "last_accessed":
		if left.Lease.LastAccessedNs < right.Lease.LastAccessedNs {
			compare = -1
		} else if left.Lease.LastAccessedNs > right.Lease.LastAccessedNs {
			compare = 1
		}
	default:
		if left.Lease.ExpiryNs < right.Lease.ExpiryNs {
			compare = -1
		} else if left.Lease.ExpiryNs > right.Lease.ExpiryNs {
			compare = 1
		}
	}
	if query.Desc && compare != 0 {
		compare = -compare
	}
	if compare == 0 {
		compare = strings.Compare(left.Account, right.Account)
	}
	if compare == 0 {
		compare = strings.Compare(left.Lease.NodeHash.Hex(), right.Lease.NodeHash.Hex())
	}
	return compare
}

func encodeLeasePageCursor(item LeasePageItem, query LeasePageQuery) string {
	cursor := leasePageCursor{
		Version:        1,
		PlatformID:     query.platformID,
		SortBy:         normalizeLeaseSort(query.SortBy),
		Desc:           query.Desc,
		FilterAccount:  strings.TrimSpace(query.Account),
		Fuzzy:          query.Fuzzy,
		Limit:          query.Limit,
		LastAccount:    item.Account,
		NodeHash:       item.Lease.NodeHash.Hex(),
		ExpiryNs:       item.Lease.ExpiryNs,
		LastAccessedNs: item.Lease.LastAccessedNs,
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	hasher := hmac.New(sha256.New, leaseCursorSecret)
	_, _ = hasher.Write(payload)
	cursor.MAC = base64.RawURLEncoding.EncodeToString(hasher.Sum(nil))
	raw, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeLeasePageCursor(raw string, query LeasePageQuery) (LeasePageItem, error) {
	if strings.TrimSpace(raw) == "" || len(raw) > 2048 {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	var cursor leasePageCursor
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	providedMAC, err := base64.RawURLEncoding.DecodeString(cursor.MAC)
	if err != nil || len(providedMAC) != sha256.Size {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	cursor.MAC = ""
	payload, err := json.Marshal(cursor)
	if err != nil {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	hasher := hmac.New(sha256.New, leaseCursorSecret)
	_, _ = hasher.Write(payload)
	if !hmac.Equal(providedMAC, hasher.Sum(nil)) {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF ||
		cursor.Version != 1 ||
		cursor.PlatformID != query.platformID ||
		cursor.SortBy != normalizeLeaseSort(query.SortBy) || cursor.Desc != query.Desc ||
		cursor.FilterAccount != strings.TrimSpace(query.Account) || cursor.Fuzzy != query.Fuzzy ||
		cursor.Limit != query.Limit || cursor.LastAccount == "" || cursor.NodeHash == "" {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	hash, err := node.ParseHex(cursor.NodeHash)
	if err != nil {
		return LeasePageItem{}, ErrLeaseCursorInvalid
	}
	return LeasePageItem{
		Account: cursor.LastAccount,
		Lease: Lease{
			NodeHash:       hash,
			ExpiryNs:       cursor.ExpiryNs,
			LastAccessedNs: cursor.LastAccessedNs,
		},
	}, nil
}

func leaseMatchesQuery(account string, query LeasePageQuery) bool {
	needle := strings.TrimSpace(query.Account)
	if needle == "" {
		return true
	}
	if query.Fuzzy {
		return strings.Contains(strings.ToLower(account), strings.ToLower(needle))
	}
	return account == needle
}

// Resin is currently a stateful single-process/single-container deployment:
// the router and SQLite-backed state are local, not shared across replicas.
// The cursor signing key is therefore intentionally process-local. A restart
// invalidates old cursors; the API returns ErrLeaseCursorInvalid (HTTP 400),
// and the WebUI resets the affected view to its first page.
//
// SnapshotLeasePageForPlatform performs filtering, exact counting, and
// bounded cursor selection before releasing the Router lifecycle read owner.
// It never materializes every lease or exposes routing internals to the
// service/API. Concurrent changes have best-effort cursor semantics: the
// cursor is a stable exclusive sort position for the observed generation.
func (r *Router) SnapshotLeasePageForPlatform(platformID string, query LeasePageQuery) (LeasePage, bool, error) {
	query.platformID = platformID
	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > maxLeasePageLimit {
		return LeasePage{}, false, ErrLeasePageTooWide
	}
	query.Limit = limit
	query.SortBy = normalizeLeaseSort(query.SortBy)

	var cursorItem *LeasePageItem
	if query.Cursor != "" {
		decoded, err := decodeLeasePageCursor(query.Cursor, query)
		if err != nil {
			return LeasePage{}, false, err
		}
		cursorItem = &decoded
	}

	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	if r.stopped || !r.platformExistsLocked(platformID) {
		return LeasePage{}, false, nil
	}

	page := LeasePage{Items: []LeasePageItem{}, Counts: make(map[node.Hash]int)}
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		return page, true, nil
	}
	selected := &leasePageHeap{query: query}
	heap.Init(selected)
	state.Leases.Range(func(account string, lease Lease) bool {
		page.Counts[lease.NodeHash]++
		if !leaseMatchesQuery(account, query) {
			return true
		}
		page.Total++
		candidate := leasePageCandidate{LeasePageItem: LeasePageItem{Account: account, Lease: lease}}
		if cursorItem != nil && compareLeasePageItems(candidate.LeasePageItem, *cursorItem, query) <= 0 {
			return true
		}
		if selected.Len() < limit+1 {
			heap.Push(selected, candidate)
			return true
		}
		if compareLeasePageItems(candidate.LeasePageItem, selected.items[0].LeasePageItem, query) < 0 {
			heap.Pop(selected)
			heap.Push(selected, candidate)
		}
		return true
	})

	items := make([]LeasePageItem, 0, selected.Len())
	for selected.Len() > 0 {
		items = append(items, heap.Pop(selected).(leasePageCandidate).LeasePageItem)
	}
	sort.Slice(items, func(i, j int) bool {
		return compareLeasePageItems(items[i], items[j], query) < 0
	})
	page.HasMore = len(items) > limit
	if page.HasMore {
		items = items[:limit]
		page.NextCursor = encodeLeasePageCursor(items[len(items)-1], query)
	}
	page.Items = append(page.Items, items...)
	return page, true, nil
}
