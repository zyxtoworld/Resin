package metrics

import (
	"strings"
	"testing"
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
