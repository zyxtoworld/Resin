package routing

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestLookupRecentDomainLatency_RejectsFutureSamples(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	entry := node.NewNodeEntry(
		node.HashFromRawOptions([]byte(`{"id":"future-latency"}`)),
		[]byte(`{"id":"future-latency"}`),
		now,
		4,
	)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        25 * time.Millisecond,
		LastUpdated: now.Add(time.Second),
	})

	if _, ok := lookupRecentDomainLatency(entry, "example.com", now, time.Minute); ok {
		t.Fatal("future latency sample must not be considered recent")
	}
}

func TestIsRecent_UsesClosedPastWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	window := time.Minute
	for name, sample := range map[string]time.Time{
		"at now":             now,
		"at window boundary": now.Add(-window),
	} {
		if !isRecent(sample, now, window) {
			t.Errorf("%s: sample should be recent", name)
		}
	}
	if isRecent(now.Add(-(window + time.Nanosecond)), now, window) {
		t.Fatal("sample older than the window must not be recent")
	}
}
