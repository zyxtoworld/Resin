package node

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
)

// DomainLatencyStats holds the TD-EWMA latency statistics for a single domain.
type DomainLatencyStats struct {
	Ewma        time.Duration
	LastUpdated time.Time
}

const latencyReadTouchMinInterval = 100 * time.Millisecond

// MaxLatencyAuthorityEntries is the hard per-node budget for resident
// authority-domain samples. Runtime configuration uses the same limit before
// it is published, while the table keeps the bound for defensive callers.
const MaxLatencyAuthorityEntries = 32

// ValidateLatencyAuthorities validates the configuration that controls the
// resident latency partition.
func ValidateLatencyAuthorities(authorities []string) error {
	if len(authorities) > MaxLatencyAuthorityEntries {
		return fmt.Errorf("at most %d authority domains are allowed", MaxLatencyAuthorityEntries)
	}
	return nil
}

type latencySlot struct {
	key          uint64
	domain       string
	stats        DomainLatencyStats
	lastAccessNs int64
	occupied     bool
}

type latencyEntry struct {
	domain string
	stats  DomainLatencyStats
}

// LatencyTable is a bounded, thread-safe per-domain latency table.
// It keeps authority domains in a resident partition and regular domains in a
// bounded partition evicted by least-recently-accessed timestamp.
// Domain lookup inside the table uses a 64-bit xxh3 hash key for compactness.
// This intentionally accepts extremely low-probability hash collisions.
type LatencyTable struct {
	mu                sync.Mutex
	authorityCapacity int

	authorities []latencySlot
	regular     []latencySlot
}

// NewLatencyTable creates a new LatencyTable whose regular partition
// is bounded to maxEntries.
func NewLatencyTable(maxEntries int) *LatencyTable {
	if maxEntries <= 0 {
		panic("node: latency table max entries must be positive")
	}
	return &LatencyTable{
		authorityCapacity: MaxLatencyAuthorityEntries,
		regular:           make([]latencySlot, maxEntries),
	}
}

// Update records a latency observation for the given domain using TD-EWMA.
// wasEmpty is true if the table had no entries before this update.
//
// TD-EWMA formula:
//
//	weight = exp(-Δt / decayWindow)
//	newEwma = oldEwma * weight + latency * (1 - weight)
//
// For the first observation of a domain, Ewma is set to the raw latency.
func (t *LatencyTable) Update(domain string, latency, decayWindow time.Duration) (wasEmpty bool) {
	wasEmpty, _, _ = t.UpdateClassified(domain, latency, decayWindow, false)
	return wasEmpty
}

// UpdateClassified records a latency observation and writes into either
// authority-resident partition or regular partition.
// It returns evicted=true with evictedDomain when either bounded partition
// discards a domain. Regular updates never evict another authority domain.
func (t *LatencyTable) UpdateClassified(
	domain string,
	latency, decayWindow time.Duration,
	isAuthority bool,
) (wasEmpty bool, evictedDomain string, evicted bool) {
	return t.updateClassifiedAt(domain, latency, decayWindow, isAuthority, time.Now())
}

func (t *LatencyTable) updateClassifiedAt(
	domain string,
	latency, decayWindow time.Duration,
	isAuthority bool,
	now time.Time,
) (wasEmpty bool, evictedDomain string, evicted bool) {
	domain = canonicalLatencyDomain(domain)
	key := domainKey(domain)
	nowNs := now.UnixNano()

	t.mu.Lock()
	defer t.mu.Unlock()

	wasEmpty = t.totalSizeLocked() == 0

	old, found := t.popDomainLocked(key)
	stats := tdEWMAUpdate(old, found, latency, decayWindow, now)

	if isAuthority {
		evictedDomain, evicted := t.upsertAuthorityLocked(key, domain, stats, nowNs)
		return wasEmpty, evictedDomain, evicted
	}
	evictedDomain, evicted = t.upsertRegularLocked(key, domain, stats, nowNs)
	return wasEmpty, evictedDomain, evicted
}

// GetDomainStats returns the latency stats for a domain, if present.
// Read touches are write-throttled: last-access timestamp is updated only when
// the last update is older than latencyReadTouchMinInterval.
func (t *LatencyTable) GetDomainStats(domain string) (DomainLatencyStats, bool) {
	domain = canonicalLatencyDomain(domain)
	key := domainKey(domain)
	nowNs := time.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := range t.authorities {
		if !t.authorities[i].occupied || t.authorities[i].key != key {
			continue
		}
		stats := t.authorities[i].stats
		if nowNs-t.authorities[i].lastAccessNs >= latencyReadTouchMinInterval.Nanoseconds() {
			t.authorities[i].lastAccessNs = nowNs
		}
		return stats, true
	}
	for i := range t.regular {
		if !t.regular[i].occupied || t.regular[i].key != key {
			continue
		}
		stats := t.regular[i].stats
		if nowNs-t.regular[i].lastAccessNs >= latencyReadTouchMinInterval.Nanoseconds() {
			t.regular[i].lastAccessNs = nowNs
		}
		return stats, true
	}
	return DomainLatencyStats{}, false
}

// LoadEntry stores a bootstrap-recovered entry directly (no TD-EWMA).
func (t *LatencyTable) LoadEntry(domain string, stats DomainLatencyStats) {
	_, _ = t.LoadEntryClassified(domain, stats, false)
}

// LoadEntryClassified stores a bootstrap-recovered entry directly (no TD-EWMA)
// into either authority or regular partition.
func (t *LatencyTable) LoadEntryClassified(
	domain string,
	stats DomainLatencyStats,
	isAuthority bool,
) (evictedDomain string, evicted bool) {
	return t.loadEntryClassifiedAt(domain, stats, isAuthority, time.Now())
}

// loadEntryClassifiedAt is the bootstrap insertion primitive with an explicit
// load timestamp. Persisted timestamps newer than the load are not valid LRU
// access order, so they are capped at loadedAt.
func (t *LatencyTable) loadEntryClassifiedAt(
	domain string,
	stats DomainLatencyStats,
	isAuthority bool,
	loadedAt time.Time,
) (evictedDomain string, evicted bool) {
	domain = canonicalLatencyDomain(domain)
	key := domainKey(domain)
	loadedAtNs := loadedAt.UnixNano()
	accessNs := stats.LastUpdated.UnixNano()
	if accessNs <= 0 || accessNs > loadedAtNs {
		accessNs = loadedAtNs
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.popDomainLocked(key)
	if isAuthority {
		return t.upsertAuthorityLocked(key, domain, stats, accessNs)
	}
	return t.upsertRegularLocked(key, domain, stats, accessNs)
}

// Size returns the number of domains with latency data.
func (t *LatencyTable) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalSizeLocked()
}

// Range iterates all domain entries. Returning false stops iteration.
func (t *LatencyTable) Range(fn func(domain string, stats DomainLatencyStats) bool) {
	t.mu.Lock()
	snapshot := make([]latencyEntry, 0, t.totalSizeLocked())
	for i := range t.authorities {
		if !t.authorities[i].occupied {
			continue
		}
		snapshot = append(snapshot, latencyEntry{
			domain: t.authorities[i].domain,
			stats:  t.authorities[i].stats,
		})
	}
	for i := range t.regular {
		if !t.regular[i].occupied {
			continue
		}
		snapshot = append(snapshot, latencyEntry{
			domain: t.regular[i].domain,
			stats:  t.regular[i].stats,
		})
	}
	t.mu.Unlock()

	for _, e := range snapshot {
		if !fn(e.domain, e.stats) {
			return
		}
	}
}

// Close is kept for lifecycle symmetry. LatencyTable currently has no
// background resources and Close is a no-op.
func (t *LatencyTable) Close() {}

func (t *LatencyTable) totalSizeLocked() int {
	count := 0
	for i := range t.authorities {
		if t.authorities[i].occupied {
			count++
		}
	}
	for i := range t.regular {
		if t.regular[i].occupied {
			count++
		}
	}
	return count
}

func (t *LatencyTable) popDomainLocked(key uint64) (DomainLatencyStats, bool) {
	for i := range t.authorities {
		if t.authorities[i].occupied && t.authorities[i].key == key {
			stats := t.authorities[i].stats
			t.authorities = deleteLatencySlot(t.authorities, i)
			return stats, true
		}
	}
	for i := range t.regular {
		if t.regular[i].occupied && t.regular[i].key == key {
			stats := t.regular[i].stats
			t.regular[i] = latencySlot{}
			return stats, true
		}
	}
	return DomainLatencyStats{}, false
}

func (t *LatencyTable) upsertAuthorityLocked(
	key uint64,
	domain string,
	stats DomainLatencyStats,
	accessNs int64,
) (evictedDomain string, evicted bool) {
	if len(t.authorities) < t.authorityCapacity {
		t.authorities = append(t.authorities, latencySlot{
			key:          key,
			domain:       domain,
			stats:        stats,
			lastAccessNs: accessNs,
			occupied:     true,
		})
		return "", false
	}

	oldestIdx := t.oldestAuthorityIndexLocked()
	if oldestIdx < 0 {
		return "", false
	}
	evictedDomain = t.authorities[oldestIdx].domain
	t.authorities[oldestIdx] = latencySlot{
		key:          key,
		domain:       domain,
		stats:        stats,
		lastAccessNs: accessNs,
		occupied:     true,
	}
	return evictedDomain, true
}

func (t *LatencyTable) oldestAuthorityIndexLocked() int {
	oldestIdx := -1
	for i := range t.authorities {
		if !t.authorities[i].occupied {
			continue
		}
		if oldestIdx < 0 || latencyRankLess(
			t.authorities[i].lastAccessNs,
			t.authorities[i].domain,
			t.authorities[oldestIdx].lastAccessNs,
			t.authorities[oldestIdx].domain,
		) {
			oldestIdx = i
		}
	}
	return oldestIdx
}

func latencyRankLess(accessNs int64, domain string, otherAccessNs int64, otherDomain string) bool {
	if accessNs != otherAccessNs {
		return accessNs < otherAccessNs
	}
	return domain < otherDomain
}

func (t *LatencyTable) upsertRegularLocked(
	key uint64,
	domain string,
	stats DomainLatencyStats,
	accessNs int64,
) (evictedDomain string, evicted bool) {
	emptyIdx := -1
	oldestIdx := -1
	for i := range t.regular {
		slot := t.regular[i]
		if !slot.occupied {
			if emptyIdx < 0 {
				emptyIdx = i
			}
			continue
		}
		if oldestIdx < 0 || latencyRankLess(
			slot.lastAccessNs,
			slot.domain,
			t.regular[oldestIdx].lastAccessNs,
			t.regular[oldestIdx].domain,
		) {
			oldestIdx = i
		}
	}

	targetIdx := emptyIdx
	if targetIdx < 0 {
		targetIdx = oldestIdx
		if targetIdx >= 0 {
			evictedDomain = t.regular[targetIdx].domain
			evicted = true
		}
	}
	if targetIdx < 0 {
		return "", false
	}

	t.regular[targetIdx] = latencySlot{
		key:          key,
		domain:       domain,
		stats:        stats,
		lastAccessNs: accessNs,
		occupied:     true,
	}
	return evictedDomain, evicted
}

// domainKey builds a compact 64-bit key for domain matching in LatencyTable.
// Collision handling is intentionally omitted as a memory/perf trade-off.
func domainKey(domain string) uint64 {
	return xxh3.HashString(canonicalLatencyDomain(domain))
}

func canonicalLatencyDomain(domain string) string {
	return strings.ToLower(strings.TrimSpace(domain))
}

func deleteLatencySlot(slots []latencySlot, idx int) []latencySlot {
	if idx < 0 || idx >= len(slots) {
		return slots
	}
	copy(slots[idx:], slots[idx+1:])
	slots[len(slots)-1] = latencySlot{}
	return slots[:len(slots)-1]
}

func tdEWMAUpdate(
	old DomainLatencyStats,
	found bool,
	latency, decayWindow time.Duration,
	now time.Time,
) DomainLatencyStats {
	if !found {
		return DomainLatencyStats{
			Ewma:        latency,
			LastUpdated: now,
		}
	}

	elapsed := now.Sub(old.LastUpdated)
	if elapsed < 0 {
		// A future persisted sample cannot provide a valid decay interval.
		// Rebuild from the current observation instead of allowing weight > 1
		// to produce a negative or overflowing EWMA.
		return DomainLatencyStats{
			Ewma:        latency,
			LastUpdated: now,
		}
	}
	dt := elapsed.Seconds()
	decay := decayWindow.Seconds()
	if decay <= 0 {
		decay = 1 // prevent division by zero
	}
	weight := math.Exp(-dt / decay)
	newEwma := time.Duration(float64(old.Ewma)*weight + float64(latency)*(1-weight))

	return DomainLatencyStats{
		Ewma:        newEwma,
		LastUpdated: now,
	}
}

// AverageEWMAForDomainsMs returns the average EWMA latency in milliseconds
// across domains that exist in the node's latency table.
func AverageEWMAForDomainsMs(entry *NodeEntry, domains []string) (float64, bool) {
	if entry == nil || entry.LatencyTable == nil || entry.LatencyTable.Size() == 0 || len(domains) == 0 {
		return 0, false
	}

	var sumMs float64
	var count int
	for _, domain := range domains {
		domain = strings.TrimSpace(domain)
		if domain == "" {
			continue
		}
		stats, ok := entry.LatencyTable.GetDomainStats(domain)
		if !ok {
			continue
		}
		sumMs += float64(stats.Ewma.Milliseconds())
		count++
	}
	if count == 0 {
		return 0, false
	}
	return sumMs / float64(count), true
}
