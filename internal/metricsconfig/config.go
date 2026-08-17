package metricsconfig

import "fmt"

// MaxRealtimeRingSamples is the per-ring sample budget. RealtimeSample is a
// fixed-size value, so this keeps each ring near a 16 MiB backing allocation
// with the current layout and bounds the three rings before make is reached.
const MaxRealtimeRingSamples = 1 << 18

// MaxLatencyHistogramBuckets bounds one global or per-platform histogram.
// Each bucket is an atomic int64, so this keeps one histogram's counter array
// within a bounded allocation before make is reached.
const MaxLatencyHistogramBuckets = 1 << 16

const maxDurationSeconds = (1<<63 - 1) / int64(1e9)

// ValidateDurationSeconds validates a positive number of seconds before it is
// converted to time.Duration and multiplied by time.Second.
func ValidateDurationSeconds(seconds int) error {
	if seconds <= 0 {
		return fmt.Errorf("must be positive, got %d", seconds)
	}
	if int64(seconds) > maxDurationSeconds {
		return fmt.Errorf("exceeds maximum duration of %d seconds", maxDurationSeconds)
	}
	return nil
}

// RealtimeCapacity returns the number of samples needed to retain the
// requested interval, rounded up, without overflowing retention+interval-1.
// It rejects configurations that exceed the allocation budget instead of
// silently shrinking the requested retention.
func RealtimeCapacity(retentionSec, intervalSec int) (int, error) {
	if retentionSec <= 0 {
		return 0, fmt.Errorf("retention must be positive, got %d", retentionSec)
	}
	if intervalSec <= 0 {
		return 0, fmt.Errorf("interval must be positive, got %d", intervalSec)
	}

	capacity := retentionSec / intervalSec
	if retentionSec%intervalSec != 0 {
		capacity++
	}
	if err := ValidateRealtimeRingCapacity(capacity); err != nil {
		return 0, err
	}
	return capacity, nil
}

// ValidateRealtimeRingCapacity checks the allocation boundary used by every
// realtime ring constructor.
func ValidateRealtimeRingCapacity(capacity int) error {
	if capacity <= 0 {
		return fmt.Errorf("sample capacity must be positive, got %d", capacity)
	}
	if capacity > MaxRealtimeRingSamples {
		return fmt.Errorf("sample capacity %d exceeds maximum %d", capacity, MaxRealtimeRingSamples)
	}
	return nil
}

// LatencyHistogramBucketCount validates the latency histogram configuration
// and returns the total number of regular plus overflow buckets. The ceiling
// calculation avoids overflow in overflowMs+binMs-1.
func LatencyHistogramBucketCount(binMs, overflowMs int) (int, error) {
	if binMs <= 0 {
		return 0, fmt.Errorf("latency bin width must be positive, got %d", binMs)
	}
	if overflowMs <= 0 {
		return 0, fmt.Errorf("latency overflow threshold must be positive, got %d", overflowMs)
	}

	regularBuckets := overflowMs / binMs
	if overflowMs%binMs != 0 {
		regularBuckets++
	}
	if regularBuckets > MaxLatencyHistogramBuckets-1 {
		return 0, fmt.Errorf(
			"latency histogram requires %d buckets, exceeds maximum %d",
			regularBuckets+1,
			MaxLatencyHistogramBuckets,
		)
	}
	return regularBuckets + 1, nil
}
