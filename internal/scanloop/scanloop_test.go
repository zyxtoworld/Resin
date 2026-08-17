package scanloop

import (
	"testing"
	"time"
)

func TestWaitForTimerOrStop_StopWinsWhenBothAreReady(t *testing.T) {
	for i := 0; i < 128; i++ {
		stopCh := make(chan struct{})
		timerCh := make(chan time.Time)
		close(stopCh)
		close(timerCh)

		if waitForTimerOrStop(stopCh, timerCh) {
			t.Fatalf("timer won after stop was already ready at iteration %d", i)
		}
	}
}

func TestWaitForTimerOrStop_RechecksStopAfterTimerReceive(t *testing.T) {
	stopCh := make(chan struct{})
	timerCh := make(chan time.Time)
	close(timerCh)

	if waitForTimerOrStopWithHook(stopCh, timerCh, func() { close(stopCh) }) {
		t.Fatal("timer callback was admitted after stop closed between receive and callback")
	}
}

func TestWaitForTimerOrStop_TimerWinsWhenStopIsNotReady(t *testing.T) {
	stopCh := make(chan struct{})
	timerCh := make(chan time.Time)
	close(timerCh)

	if !waitForTimerOrStop(stopCh, timerCh) {
		t.Fatal("ready timer was rejected while stop was not ready")
	}
}
