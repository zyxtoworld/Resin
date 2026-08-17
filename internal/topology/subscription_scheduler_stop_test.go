package topology

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSubscriptionScheduler_StopContextWaitsForSingleOwner(t *testing.T) {
	scheduler := NewSubscriptionScheduler(SchedulerConfig{})
	entered := make(chan struct{})
	release := make(chan struct{})
	mutationDone := make(chan struct{})

	go func() {
		if err := scheduler.RunMutation(func() error {
			close(entered)
			<-release
			return nil
		}); err != nil {
			t.Errorf("RunMutation: %v", err)
		}
		close(mutationDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("tracked scheduler mutation did not start")
	}

	firstCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- scheduler.StopContext(firstCtx) }()
	select {
	case err := <-firstDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first StopContext error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first StopContext did not honor its caller deadline")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- scheduler.StopContext(context.Background()) }()
	select {
	case err := <-secondDone:
		t.Fatalf("second StopContext returned before the owner completed: %v", err)
	default:
	}

	close(release)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second StopContext: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second StopContext did not join the stop owner")
	}
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("tracked mutation did not finish")
	}

	// Stop closes admission permanently. A later Start must not create a new
	// worker or a second lifecycle owner.
	scheduler.Start()
	if err := scheduler.StopContext(context.Background()); err != nil {
		t.Fatalf("StopContext after Start-after-stop: %v", err)
	}
}

func TestSubscriptionScheduler_StopBeforeStartIsTerminal(t *testing.T) {
	scheduler := NewSubscriptionScheduler(SchedulerConfig{})
	scheduler.Stop()
	scheduler.Start()
	if err := scheduler.StopContext(context.Background()); err != nil {
		t.Fatalf("StopContext after pre-start Stop: %v", err)
	}
}
