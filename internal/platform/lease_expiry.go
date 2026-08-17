package platform

import (
	"fmt"
	"math"
	"time"
)

// StickyLeaseExpiryUnixNano computes a sticky lease deadline without passing
// through time.Time.Add/UnixNano when the resulting Unix-nanosecond value
// cannot fit in the persisted int64 representation.
func StickyLeaseExpiryUnixNano(now time.Time, ttlNs int64) (int64, error) {
	if ttlNs <= 0 {
		return 0, fmt.Errorf("sticky ttl must be positive")
	}
	nowNs := now.UnixNano()
	if nowNs > math.MaxInt64-ttlNs {
		return 0, fmt.Errorf("sticky ttl overflows lease expiry")
	}
	return nowNs + ttlNs, nil
}
