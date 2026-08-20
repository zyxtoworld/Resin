package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestNewRealtimeRingRejectsCapacityBeforeAllocation(t *testing.T) {
	_, err := NewRealtimeRing(MaxRealtimeRingSamples + 1)
	if err == nil {
		t.Fatal("expected realtime ring capacity error")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("error = %q, want maximum-capacity diagnostic", err)
	}
}

func TestRealtimeRingQueryDoesNotAssumeWallClockMonotonicity(t *testing.T) {
	ring, err := NewRealtimeRing(4)
	if err != nil {
		t.Fatalf("NewRealtimeRing: %v", err)
	}

	// The production sampler timestamps with time.Now(). A wall-clock step
	// backwards can produce this order without changing ring insertion order.
	ring.Push(RealtimeSample{Timestamp: time.Unix(20, 0)})
	ring.Push(RealtimeSample{Timestamp: time.Unix(10, 0)})
	ring.Push(RealtimeSample{Timestamp: time.Unix(15, 0)})

	got := ring.Query(time.Unix(15, 0), time.Unix(20, 0))
	if len(got) != 2 {
		t.Fatalf("query returned %d samples, want 2", len(got))
	}
	if !got[0].Timestamp.Equal(time.Unix(15, 0)) || !got[1].Timestamp.Equal(time.Unix(20, 0)) {
		t.Fatalf("query order/content = %v, want timestamps 15 then 20", []time.Time{got[0].Timestamp, got[1].Timestamp})
	}
}
