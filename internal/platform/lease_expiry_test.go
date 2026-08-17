package platform

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestStickyLeaseExpiryUnixNanoBoundaries(t *testing.T) {
	now := time.Unix(1_700_000_000, 123).UTC()
	remaining := int64(math.MaxInt64 - now.UnixNano())

	got, err := StickyLeaseExpiryUnixNano(now, remaining)
	if err != nil {
		t.Fatalf("exact int64 boundary rejected: %v", err)
	}
	if got != math.MaxInt64 {
		t.Fatalf("exact boundary expiry = %d, want %d", got, int64(math.MaxInt64))
	}

	if _, err := StickyLeaseExpiryUnixNano(now, remaining+1); err == nil {
		t.Fatal("expected one-nanosecond overflow error")
	} else if !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("unexpected overflow error: %v", err)
	}

	for _, ttl := range []int64{0, -1} {
		if _, err := StickyLeaseExpiryUnixNano(now, ttl); err == nil {
			t.Fatalf("ttl %d unexpectedly accepted", ttl)
		}
	}
}
