package metrics

import "github.com/Resinat/Resin/internal/metricsconfig"

// MaxRealtimeRingSamples is the per-ring sample allocation budget.
const MaxRealtimeRingSamples = metricsconfig.MaxRealtimeRingSamples

// MaxLatencyHistogramBuckets is the per-histogram allocation budget.
const MaxLatencyHistogramBuckets = metricsconfig.MaxLatencyHistogramBuckets

// ValidateDurationSeconds validates a positive duration before conversion.
func ValidateDurationSeconds(seconds int) error {
	return metricsconfig.ValidateDurationSeconds(seconds)
}

// RealtimeCapacity calculates and validates a realtime ring capacity.
func RealtimeCapacity(retentionSec, intervalSec int) (int, error) {
	return metricsconfig.RealtimeCapacity(retentionSec, intervalSec)
}

// ValidateRealtimeRingCapacity validates the ring allocation boundary.
func ValidateRealtimeRingCapacity(capacity int) error {
	return metricsconfig.ValidateRealtimeRingCapacity(capacity)
}
