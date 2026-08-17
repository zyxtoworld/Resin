package metricsconfig

import (
	"strings"
	"testing"
)

func TestRealtimeCapacityRoundsUp(t *testing.T) {
	got, err := RealtimeCapacity(11, 5)
	if err != nil {
		t.Fatalf("RealtimeCapacity: %v", err)
	}
	if got != 3 {
		t.Fatalf("capacity: got %d, want 3", got)
	}
}

func TestRealtimeCapacityRejectsBudgetOverflow(t *testing.T) {
	_, err := RealtimeCapacity(MaxRealtimeRingSamples+1, 1)
	if err == nil {
		t.Fatal("expected sample budget error")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("error = %q, want maximum-capacity diagnostic", err)
	}
}

func TestValidateDurationSecondsRejectsMultiplicationOverflow(t *testing.T) {
	if err := ValidateDurationSeconds(9223372037); err == nil {
		t.Fatal("expected duration multiplication overflow error")
	}
}

func TestLatencyHistogramBucketCountRoundsWithoutOverflow(t *testing.T) {
	got, err := LatencyHistogramBucketCount(100, 3000)
	if err != nil {
		t.Fatalf("LatencyHistogramBucketCount: %v", err)
	}
	if got != 31 {
		t.Fatalf("bucket count: got %d, want 31", got)
	}
}

func TestLatencyHistogramBucketCountRejectsBudgetOverflow(t *testing.T) {
	_, err := LatencyHistogramBucketCount(1, MaxLatencyHistogramBuckets)
	if err == nil {
		t.Fatal("expected latency histogram budget error")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("error = %q, want maximum-capacity diagnostic", err)
	}
}

func TestLatencyHistogramBucketCountRejectsArithmeticOverflowInput(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	_, err := LatencyHistogramBucketCount(2, maxInt)
	if err == nil {
		t.Fatal("expected oversized latency histogram to be rejected")
	}
}
