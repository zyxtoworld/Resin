package scanloop

import (
	"math/rand/v2"
	"time"
)

const (
	// DefaultMinInterval and DefaultJitterRange define the shared scan cadence.
	DefaultMinInterval = 13 * time.Second
	DefaultJitterRange = 4 * time.Second
)

// Run executes fn at a jittered interval until stopCh is closed.
// The interval is: minInterval + random([0, jitterRange)).
func Run(stopCh <-chan struct{}, minInterval, jitterRange time.Duration, fn func()) {
	if minInterval <= 0 {
		minInterval = time.Second
	}
	if jitterRange < 0 {
		jitterRange = 0
	}

	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C // drain initial fire

	for {
		interval := minInterval
		if jitterRange > 0 {
			interval += time.Duration(rand.Int64N(int64(jitterRange)))
		}

		timer.Reset(interval)
		if !waitForTimerOrStop(stopCh, timer.C) {
			return
		}
		fn()
	}
}

// waitForTimerOrStop gives a stop signal precedence over a timer that is
// already ready. The first select alone is insufficient: when both channels
// are ready, Go deliberately chooses a random case. The second non-blocking
// check is the stop linearization point immediately before invoking fn.
func waitForTimerOrStop(stopCh <-chan struct{}, timerCh <-chan time.Time) bool {
	return waitForTimerOrStopWithHook(stopCh, timerCh, nil)
}

func waitForTimerOrStopWithHook(
	stopCh <-chan struct{},
	timerCh <-chan time.Time,
	afterTimerReceive func(),
) bool {
	select {
	case <-stopCh:
		return false
	case <-timerCh:
		if afterTimerReceive != nil {
			afterTimerReceive()
		}
		select {
		case <-stopCh:
			return false
		default:
			return true
		}
	}
}
