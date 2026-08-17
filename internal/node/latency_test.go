package node

import (
	"strconv"
	"testing"
	"time"
)

func TestLatencyTable_FirstRecord(t *testing.T) {
	lt := NewLatencyTable(16)

	wasEmpty := lt.Update("example.com", 100*time.Millisecond, 30*time.Second)
	if !wasEmpty {
		t.Fatal("should report wasEmpty on first-ever record")
	}

	stats, ok := lt.GetDomainStats("example.com")
	if !ok {
		t.Fatal("should find stats for example.com")
	}
	if stats.Ewma != 100*time.Millisecond {
		t.Fatalf("first Ewma should equal raw latency, got %v", stats.Ewma)
	}
}

func TestLatencyTable_SecondRecord_NotWasEmpty(t *testing.T) {
	lt := NewLatencyTable(16)

	lt.Update("example.com", 100*time.Millisecond, 30*time.Second)
	wasEmpty := lt.Update("example.com", 200*time.Millisecond, 30*time.Second)
	if wasEmpty {
		t.Fatal("should not report wasEmpty on second record")
	}
}

func TestLatencyTable_TDEWMA_Decay(t *testing.T) {
	lt := NewLatencyTable(16)

	// Preload with known stats.
	base := time.Now().Add(-10 * time.Second)
	lt.LoadEntry("example.com", DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: base,
	})

	// Update with a much higher value.
	lt.Update("example.com", 500*time.Millisecond, 30*time.Second)

	stats, _ := lt.GetDomainStats("example.com")
	// New EWMA should be between old (100ms) and new (500ms).
	if stats.Ewma <= 100*time.Millisecond || stats.Ewma >= 500*time.Millisecond {
		t.Fatalf("EWMA should be between 100ms and 500ms, got %v", stats.Ewma)
	}
}

func TestLatencyTable_BoundedEviction_RegularLRU(t *testing.T) {
	lt := NewLatencyTable(2)
	lt.Update("a.com", 10*time.Millisecond, 30*time.Second)
	lt.Update("b.com", 20*time.Millisecond, 30*time.Second)
	lt.Update("c.com", 30*time.Millisecond, 30*time.Second)

	if _, ok := lt.GetDomainStats("a.com"); ok {
		t.Fatal("expected oldest regular entry to be evicted")
	}
	if _, ok := lt.GetDomainStats("b.com"); !ok {
		t.Fatal("expected b.com to remain in regular LRU")
	}
	if _, ok := lt.GetDomainStats("c.com"); !ok {
		t.Fatal("expected c.com to remain in regular LRU")
	}
}

func TestLatencyTable_Get_TouchesRegularLRU(t *testing.T) {
	lt := NewLatencyTable(2)
	lt.Update("a.com", 10*time.Millisecond, 30*time.Second)
	lt.Update("b.com", 20*time.Millisecond, 30*time.Second)

	// Read touch is throttled; wait over the minimum interval first.
	time.Sleep(latencyReadTouchMinInterval + 20*time.Millisecond)
	if _, ok := lt.GetDomainStats("a.com"); !ok {
		t.Fatal("expected a.com to exist")
	}
	lt.Update("c.com", 30*time.Millisecond, 30*time.Second)

	if _, ok := lt.GetDomainStats("b.com"); ok {
		t.Fatal("expected b.com to be evicted after read-touch on a.com")
	}
	if _, ok := lt.GetDomainStats("a.com"); !ok {
		t.Fatal("expected a.com to stay due to read-touch")
	}
	if _, ok := lt.GetDomainStats("c.com"); !ok {
		t.Fatal("expected c.com to exist")
	}
}

func TestLatencyTable_AuthorityResident(t *testing.T) {
	lt := NewLatencyTable(1)

	lt.UpdateClassified("gstatic.com", 5*time.Millisecond, 30*time.Second, true)
	lt.Update("a.com", 10*time.Millisecond, 30*time.Second)
	lt.Update("b.com", 20*time.Millisecond, 30*time.Second)

	if _, ok := lt.GetDomainStats("gstatic.com"); !ok {
		t.Fatal("authority domain should stay resident")
	}
	if _, ok := lt.GetDomainStats("a.com"); ok {
		t.Fatal("oldest regular entry should be evicted")
	}
	if _, ok := lt.GetDomainStats("b.com"); !ok {
		t.Fatal("latest regular entry should remain")
	}
}

func TestLatencyTable_BoundsAuthorityResidentPartition(t *testing.T) {
	lt := NewLatencyTable(1)
	for i := 0; i <= MaxLatencyAuthorityEntries; i++ {
		lt.UpdateClassified(
			"authority-"+strconv.Itoa(i)+".example",
			time.Millisecond,
			30*time.Second,
			true,
		)
	}

	lt.mu.Lock()
	got := len(lt.authorities)
	lt.mu.Unlock()
	if got > MaxLatencyAuthorityEntries {
		t.Fatalf("authority latency partition grew beyond budget: got %d, want <= %d", got, MaxLatencyAuthorityEntries)
	}
}

func TestLatencyTable_RotatesAuthorityAtCapacity(t *testing.T) {
	lt := NewLatencyTable(1)
	base := time.Unix(123, 0)
	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		if evictedDomain, evicted := lt.LoadEntryClassified(
			"old-"+strconv.Itoa(i)+".example",
			DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: base.Add(time.Duration(i) * time.Second)},
			true,
		); evicted || evictedDomain != "" {
			t.Fatalf("initial authority load unexpectedly evicted %q", evictedDomain)
		}
	}

	evictedDomain, evicted := lt.LoadEntryClassified(
		"new.example",
		DomainLatencyStats{Ewma: 2 * time.Millisecond, LastUpdated: base.Add(time.Duration(MaxLatencyAuthorityEntries) * time.Second)},
		true,
	)
	if !evicted || evictedDomain == "" {
		t.Fatalf("authority rotation did not report eviction: domain=%q evicted=%v", evictedDomain, evicted)
	}
	if _, ok := lt.GetDomainStats("new.example"); !ok {
		t.Fatal("new authority was not admitted after rotation")
	}
	if _, ok := lt.GetDomainStats(evictedDomain); ok {
		t.Fatalf("evicted authority %q remained resident", evictedDomain)
	}
}

func TestLatencyTable_RuntimeAuthorityUpdateAdmitsFutureResident(t *testing.T) {
	lt := NewLatencyTable(1)
	future := time.Now().Add(time.Hour)
	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		if evictedDomain, evicted := lt.LoadEntryClassified(
			"runtime-old-"+strconv.Itoa(i)+".com",
			DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: future.Add(time.Duration(i) * time.Second)},
			true,
		); evicted || evictedDomain != "" {
			t.Fatalf("initial authority load unexpectedly evicted %q", evictedDomain)
		}
	}

	_, evictedDomain, evicted := lt.UpdateClassified(
		"runtime-new.com",
		time.Millisecond,
		30*time.Second,
		true,
	)
	if !evicted || evictedDomain == "" {
		t.Fatalf("runtime authority update did not report eviction: domain=%q evicted=%v", evictedDomain, evicted)
	}
	if _, ok := lt.GetDomainStats("runtime-new.com"); !ok {
		t.Fatal("runtime authority update was discarded when resident timestamps were in the future")
	}
	if _, ok := lt.GetDomainStats(evictedDomain); ok {
		t.Fatalf("runtime authority eviction left %q resident", evictedDomain)
	}
}

func TestLatencyTable_RuntimeAuthorityRotationNormalizesFutureBootstrapAccess(t *testing.T) {
	lt := NewLatencyTable(1)
	loadedAt := time.Unix(2_000_000_000, 0)
	future := loadedAt.Add(time.Hour)
	runtimeNow := loadedAt.Add(time.Second)
	oldAuthorities := make(map[string]struct{}, MaxLatencyAuthorityEntries)
	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		domain := "future-old-" + strconv.Itoa(i) + ".com"
		oldAuthorities[domain] = struct{}{}
		if evictedDomain, evicted := lt.loadEntryClassifiedAt(
			domain,
			DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: future},
			true,
			loadedAt,
		); evicted || evictedDomain != "" {
			t.Fatalf("initial authority load unexpectedly evicted %q", evictedDomain)
		}
	}

	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		domain := "runtime-new-" + strconv.Itoa(i) + ".com"
		_, evictedDomain, evicted := lt.updateClassifiedAt(domain, time.Millisecond, 30*time.Second, true, runtimeNow)
		if !evicted {
			t.Fatalf("runtime rotation did not evict an authority at step %d", i)
		}
		if _, ok := oldAuthorities[evictedDomain]; !ok {
			t.Fatalf("runtime rotation evicted %q, want a future bootstrap authority", evictedDomain)
		}
		delete(oldAuthorities, evictedDomain)
	}

	if len(oldAuthorities) != 0 {
		t.Fatalf("future bootstrap authorities survived full rotation: %v", oldAuthorities)
	}
	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		domain := "future-old-" + strconv.Itoa(i) + ".com"
		if _, ok := lt.GetDomainStats(domain); ok {
			t.Fatalf("future bootstrap authority remained after full runtime rotation: %q", domain)
		}
	}
	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		domain := "runtime-new-" + strconv.Itoa(i) + ".com"
		if _, ok := lt.GetDomainStats(domain); !ok {
			t.Fatalf("runtime authority was lost during full rotation: %q", domain)
		}
	}
}

func TestLatencyTable_FutureSampleRebuildsEWMA(t *testing.T) {
	lt := NewLatencyTable(1)
	now := time.Unix(1_000_000_000, 0)
	future := now.Add(time.Hour)
	lt.LoadEntry("ewma-future.com", DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: future,
	})

	lt.updateClassifiedAt("ewma-future.com", 200*time.Millisecond, 30*time.Second, false, now)
	stats, ok := lt.GetDomainStats("ewma-future.com")
	if !ok {
		t.Fatal("future EWMA sample disappeared")
	}
	if stats.Ewma != 200*time.Millisecond {
		t.Fatalf("future EWMA sample was not rebuilt from the new observation: got %v", stats.Ewma)
	}
	if !stats.LastUpdated.Equal(now) {
		t.Fatalf("future EWMA sample kept the wrong timestamp: got %v, want %v", stats.LastUpdated, now)
	}
}

func TestLatencyTable_AuthorityTieUsesStableVictim(t *testing.T) {
	lt := NewLatencyTable(1)
	base := time.Unix(123, 0)
	for i := 0; i < MaxLatencyAuthorityEntries; i++ {
		if evictedDomain, evicted := lt.LoadEntryClassified(
			"domain-0"+strconv.Itoa(i)+".com",
			DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: base},
			true,
		); evicted || evictedDomain != "" {
			t.Fatalf("initial authority load unexpectedly evicted %q", evictedDomain)
		}
	}

	_, evictedDomain, evicted := lt.updateClassifiedAt(
		"zzz.com",
		2*time.Millisecond,
		30*time.Second,
		true,
		base,
	)
	if !evicted || evictedDomain != "domain-00.com" {
		t.Fatalf("runtime tie did not evict the stable oldest domain: domain=%q evicted=%v", evictedDomain, evicted)
	}
	if _, ok := lt.GetDomainStats("zzz.com"); !ok {
		t.Fatal("runtime tie winner was not retained")
	}

	evictedDomain, evicted = lt.LoadEntryClassified(
		"aaa.com",
		DomainLatencyStats{Ewma: 3 * time.Millisecond, LastUpdated: base},
		true,
	)
	if !evicted || evictedDomain != "domain-01.com" {
		t.Fatalf("load tie did not use the same stable victim: domain=%q evicted=%v", evictedDomain, evicted)
	}
	if _, ok := lt.GetDomainStats("aaa.com"); !ok {
		t.Fatal("loaded tie winner was not retained")
	}
}

func TestLatencyTable_RegularTieUsesStableVictim(t *testing.T) {
	lt := NewLatencyTable(2)
	base := time.Unix(123, 0)
	for _, domain := range []string{"z.com", "a.com"} {
		if evictedDomain, evicted := lt.LoadEntryClassified(
			domain,
			DomainLatencyStats{Ewma: time.Millisecond, LastUpdated: base},
			false,
		); evicted || evictedDomain != "" {
			t.Fatalf("initial regular load unexpectedly evicted %q", evictedDomain)
		}
	}

	evictedDomain, evicted := lt.LoadEntryClassified(
		"m.com",
		DomainLatencyStats{Ewma: 2 * time.Millisecond, LastUpdated: base},
		false,
	)
	if !evicted || evictedDomain != "a.com" {
		t.Fatalf("regular tie did not evict the stable oldest domain: domain=%q evicted=%v", evictedDomain, evicted)
	}
	if _, ok := lt.GetDomainStats("z.com"); !ok {
		t.Fatal("regular tie winner was not retained")
	}
	if _, ok := lt.GetDomainStats("m.com"); !ok {
		t.Fatal("new regular entry was not retained")
	}
}

func TestLatencyTable_ClassifiedUpdate_MigratesPartitions(t *testing.T) {
	lt := NewLatencyTable(1)

	lt.UpdateClassified("x.com", 10*time.Millisecond, 30*time.Second, true)
	lt.UpdateClassified("x.com", 20*time.Millisecond, 30*time.Second, false) // authority -> regular
	lt.Update("y.com", 30*time.Millisecond, 30*time.Second)                  // evicts oldest regular (x.com)

	if _, ok := lt.GetDomainStats("x.com"); ok {
		t.Fatal("x.com should be evicted after migrating to regular partition")
	}
	if _, ok := lt.GetDomainStats("y.com"); !ok {
		t.Fatal("y.com should remain in regular partition")
	}
}

func TestLatencyTable_Range(t *testing.T) {
	lt := NewLatencyTable(16)

	lt.Update("a.com", 10*time.Millisecond, 30*time.Second)
	lt.Update("b.com", 20*time.Millisecond, 30*time.Second)

	count := 0
	lt.Range(func(domain string, stats DomainLatencyStats) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expected 2 entries in Range, got %d", count)
	}
}

func TestLatencyTable_NotFound(t *testing.T) {
	lt := NewLatencyTable(16)

	_, ok := lt.GetDomainStats("nonexistent.com")
	if ok {
		t.Fatal("should not find stats for nonexistent domain")
	}
}

func TestLatencyTable_LoadEntry(t *testing.T) {
	lt := NewLatencyTable(16)
	now := time.Now()

	lt.LoadEntry("test.com", DomainLatencyStats{
		Ewma:        50 * time.Millisecond,
		LastUpdated: now,
	})

	stats, ok := lt.GetDomainStats("test.com")
	if !ok {
		t.Fatal("should find loaded entry")
	}
	if stats.Ewma != 50*time.Millisecond {
		t.Fatalf("LoadEntry should preserve exact Ewma, got %v", stats.Ewma)
	}
}

func TestAverageEWMAForDomainsMs(t *testing.T) {
	entry := NewNodeEntry(HashFromRawOptions([]byte(`{"type":"ss","server":"1.1.1.1","port":443}`)), nil, time.Now(), 16)
	entry.LatencyTable.LoadEntry("cloudflare.com", DomainLatencyStats{
		Ewma:        40 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("github.com", DomainLatencyStats{
		Ewma:        60 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("example.com", DomainLatencyStats{
		Ewma:        10 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	avg, ok := AverageEWMAForDomainsMs(entry, []string{"cloudflare.com", "github.com", "gstatic.com"})
	if !ok {
		t.Fatal("expected average to be available")
	}
	if avg != 50 {
		t.Fatalf("average ms: got %v, want 50", avg)
	}
}

func TestAverageEWMAForDomainsMs_NoMatches(t *testing.T) {
	entry := NewNodeEntry(HashFromRawOptions([]byte(`{"type":"ss","server":"1.1.1.1","port":443}`)), nil, time.Now(), 16)
	entry.LatencyTable.LoadEntry("cloudflare.com", DomainLatencyStats{
		Ewma:        40 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	if _, ok := AverageEWMAForDomainsMs(entry, []string{"github.com"}); ok {
		t.Fatal("expected no average when no domains match")
	}
}
