package topology

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/platform"
)

func TestPlatformMutationCapabilityCannotEscapeInFlightOwner(t *testing.T) {
	pool := newTestPool(nil)
	current := platform.NewPlatform("platform-mutation-owner", "current", nil, nil)
	if err := pool.RegisterPlatform(current); err != nil {
		t.Fatalf("RegisterPlatform: %v", err)
	}

	methodEntered := make(chan struct{})
	releaseMethod := make(chan struct{})
	var methodOnce sync.Once
	pool.beforePlatformReplaceLockHook = func() {
		methodOnce.Do(func() { close(methodEntered) })
		<-releaseMethod
	}
	defer func() {
		select {
		case <-releaseMethod:
		default:
			close(releaseMethod)
		}
	}()

	writerQueued := make(chan struct{})
	var writerOnce sync.Once
	pool.platformBatchMu.afterWriterQueued = func() {
		writerOnce.Do(func() { close(writerQueued) })
	}
	defer func() { pool.platformBatchMu.afterWriterQueued = nil }()

	var saved PlatformMutation
	ownerDone := make(chan error, 1)
	go func() {
		ownerDone <- pool.WithPlatformMutationContext(context.Background(), func(owner PlatformMutation) error {
			saved = owner
			methodDone := make(chan error, 1)
			go func() {
				methodDone <- owner.ReplacePlatform(platform.NewPlatform(
					current.ID,
					"next",
					nil,
					nil,
				))
			}()
			select {
			case <-methodEntered:
			case <-time.After(time.Second):
				t.Error("capability method did not enter its owner mutex")
			}
			return nil
		})
	}()

	select {
	case <-methodEntered:
	case <-time.After(time.Second):
		t.Fatal("capability method did not start")
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- pool.RegisterPlatform(platform.NewPlatform("late-writer", "late-writer", nil, nil))
	}()
	select {
	case <-writerQueued:
	case <-time.After(time.Second):
		t.Fatal("late writer did not observe the still-owned platform mutation")
	}
	select {
	case err := <-writerDone:
		t.Fatalf("late writer completed before in-flight capability method: %v", err)
	default:
	}

	close(releaseMethod)
	if err := <-ownerDone; err != nil {
		t.Fatalf("WithPlatformMutationContext: %v", err)
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("late writer after owner release: %v", err)
	}
	if saved == nil {
		t.Fatal("mutation capability was not captured")
	}
	if err := saved.RegisterPlatform(platform.NewPlatform("escaped", "escaped", nil, nil)); !errors.Is(err, ErrPlatformMutationDone) {
		t.Fatalf("escaped capability error = %v, want ErrPlatformMutationDone", err)
	}
}
