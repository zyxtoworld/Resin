package topology

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitGateSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestRuntimeBatchGate_AllowsConcurrentReaders(t *testing.T) {
	var gate runtimeBatchGate
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for _, entered := range []chan struct{}{firstEntered, secondEntered} {
		go func(entered chan struct{}) {
			defer wg.Done()
			gate.readLock()
			close(entered)
			<-release
			gate.readUnlock()
		}(entered)
	}
	waitGateSignal(t, firstEntered, "first reader")
	waitGateSignal(t, secondEntered, "second reader")
	close(release)
	wg.Wait()
}

func TestRuntimeBatchGate_WriterIsExclusiveWithReadersAndWriters(t *testing.T) {
	var gate runtimeBatchGate
	gate.readLock()

	writerQueued := make(chan struct{}, 2)
	gate.afterWriterQueued = func() { writerQueued <- struct{}{} }
	writerEntered := make(chan struct{})
	allowWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		gate.writeLock()
		close(writerEntered)
		<-allowWriter
		gate.writeUnlock()
		close(writerDone)
	}()

	waitGateSignal(t, writerQueued, "first writer queueing")
	select {
	case <-writerEntered:
		t.Fatal("writer entered while reader was active")
	default:
	}

	readerBlocked := make(chan struct{})
	gate.afterReaderBlocked = func() { close(readerBlocked) }
	readerEntered := make(chan struct{})
	go func() {
		gate.readLock()
		close(readerEntered)
		gate.readUnlock()
	}()
	waitGateSignal(t, readerBlocked, "reader blocking behind writer")
	select {
	case <-readerEntered:
		t.Fatal("reader entered while writer was waiting")
	default:
	}

	gate.readUnlock()
	waitGateSignal(t, writerEntered, "exclusive writer")
	secondWriterEntered := make(chan struct{})
	allowSecondWriter := make(chan struct{})
	secondWriterDone := make(chan struct{})
	go func() {
		gate.writeLock()
		close(secondWriterEntered)
		<-allowSecondWriter
		gate.writeUnlock()
		close(secondWriterDone)
	}()
	waitGateSignal(t, writerQueued, "second writer queueing")
	select {
	case <-secondWriterEntered:
		t.Fatal("second writer entered while first writer was active")
	default:
	}
	select {
	case <-readerEntered:
		t.Fatal("reader entered while writer was active")
	default:
	}
	close(allowWriter)
	waitGateSignal(t, writerDone, "writer completion")
	waitGateSignal(t, secondWriterEntered, "second writer after first writer")
	select {
	case <-readerEntered:
		t.Fatal("reader entered while second writer was active")
	default:
	}
	close(allowSecondWriter)
	waitGateSignal(t, secondWriterDone, "second writer completion")
	waitGateSignal(t, readerEntered, "reader after writer")
}

func TestRuntimeBatchGate_WaitingWriterHasPriorityOverNewReaders(t *testing.T) {
	var gate runtimeBatchGate
	gate.readLock()

	writerQueued := make(chan struct{})
	gate.afterWriterQueued = func() { close(writerQueued) }
	writerEntered := make(chan struct{})
	allowWriter := make(chan struct{})
	go func() {
		if err := gate.writeLockContext(context.Background()); err != nil {
			t.Errorf("writeLockContext: %v", err)
			return
		}
		close(writerEntered)
		<-allowWriter
		gate.writeUnlock()
	}()
	waitGateSignal(t, writerQueued, "priority writer queueing")

	readerBlocked := make(chan struct{})
	gate.afterReaderBlocked = func() { close(readerBlocked) }
	newReaderEntered := make(chan struct{})
	go func() {
		gate.readLock()
		close(newReaderEntered)
		gate.readUnlock()
	}()
	waitGateSignal(t, readerBlocked, "new reader behind waiting writer")
	select {
	case <-newReaderEntered:
		t.Fatal("new reader entered while writer was waiting")
	default:
	}

	gate.readUnlock()
	waitGateSignal(t, writerEntered, "priority writer")
	select {
	case <-newReaderEntered:
		t.Fatal("new reader entered while priority writer was active")
	default:
	}
	close(allowWriter)
	waitGateSignal(t, newReaderEntered, "new reader after priority writer")
}

func TestRuntimeBatchGate_CanceledWriterLeavesAdmissionForReaders(t *testing.T) {
	var gate runtimeBatchGate
	gate.readLock()

	writerQueued := make(chan struct{})
	gate.afterWriterQueued = func() { close(writerQueued) }
	ctx, cancel := context.WithCancel(context.Background())
	writerDone := make(chan error, 1)
	callbackCalled := make(chan struct{})
	go func() {
		if err := gate.writeLockContext(ctx); err != nil {
			writerDone <- err
			return
		}
		close(callbackCalled)
		gate.writeUnlock()
		writerDone <- nil
	}()
	waitGateSignal(t, writerQueued, "cancelable writer queueing")
	cancel()
	<-ctx.Done()
	gate.readUnlock()

	if err := <-writerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled writer error = %v, want context.Canceled", err)
	}
	select {
	case <-callbackCalled:
		t.Fatal("canceled writer callback executed")
	default:
	}

	readerEntered := make(chan struct{})
	go func() {
		gate.readLock()
		close(readerEntered)
		gate.readUnlock()
	}()
	waitGateSignal(t, readerEntered, "reader after canceled writer")

	gate.mu.Lock()
	if gate.waitingWriter != 0 || gate.writer {
		t.Fatalf("gate state after canceled writer: waiting=%d writer=%v", gate.waitingWriter, gate.writer)
	}
	gate.mu.Unlock()
}

func TestRuntimeBatchGate_CancelBeforeLastReaderReleaseNeverRunsCallback(t *testing.T) {
	for i := 0; i < 64; i++ {
		var gate runtimeBatchGate
		gate.readLock()
		writerQueued := make(chan struct{})
		gate.afterWriterQueued = func() { close(writerQueued) }
		ctx, cancel := context.WithCancel(context.Background())
		callbackCalled := make(chan struct{})
		writerDone := make(chan error, 1)
		go func() {
			if err := gate.writeLockContext(ctx); err != nil {
				writerDone <- err
				return
			}
			close(callbackCalled)
			gate.writeUnlock()
			writerDone <- nil
		}()
		waitGateSignal(t, writerQueued, "cancelable writer queueing")
		cancel()
		<-ctx.Done()
		gate.readUnlock()
		if err := <-writerDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d writer error = %v, want context.Canceled", i, err)
		}
		select {
		case <-callbackCalled:
			t.Fatalf("iteration %d canceled writer callback executed", i)
		default:
		}
	}
}

func TestGlobalNodePool_WithRuntimeMutationBackgroundWaitsAndPublishes(t *testing.T) {
	pool := NewGlobalNodePool(PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return time.Minute },
		MaxLatencyTableEntries: 1,
	})
	pool.runtimeBatchMu.readLock()
	writerQueued := make(chan struct{})
	pool.runtimeBatchMu.afterWriterQueued = func() { close(writerQueued) }
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		pool.WithRuntimeMutation(func() { close(entered) })
		close(done)
	}()
	waitGateSignal(t, writerQueued, "background runtime mutation queueing")
	select {
	case <-entered:
		t.Fatal("background runtime mutation entered while reader was active")
	default:
	}
	pool.runtimeBatchMu.readUnlock()
	waitGateSignal(t, entered, "background runtime mutation admission")
	waitGateSignal(t, done, "background runtime mutation completion")
}
