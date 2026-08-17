package state

import (
	"context"
	"log"
	"sync"
	"time"
)

// CacheFlushWorker periodically flushes dirty sets to cache.db.
// It triggers a flush when:
//   - DirtyCount() >= threshold, OR
//   - time.Since(lastFlush) >= interval (and dirty count > 0)
//
// Stop waits for the single stop owner, which performs the final flush before
// it completes. A context-bounded caller may return before that owner.
type CacheFlushWorker struct {
	engine      *StateEngine
	readers     CacheReaders
	thresholdFn func() int
	intervalFn  func() time.Duration
	checkTick   time.Duration // how often to check conditions

	stopCh    chan struct{}
	wg        sync.WaitGroup
	runCtx    context.Context
	runCancel context.CancelFunc
	stopMu    sync.Mutex
	stopDone  chan struct{}
	stopErr   error
	stopping  bool
	// lifecycleMu serializes worker admission with Stop's Wait. A worker is
	// counted before the lock is released, so Stop can never return while a
	// concurrently admitted Start can still launch a goroutine.
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
}

// NewCacheFlushWorker creates a flush worker that pulls threshold/interval
// from callbacks on each check cycle.
// checkTick controls how often flush conditions are evaluated (e.g. 5s).
func NewCacheFlushWorker(
	engine *StateEngine,
	readers CacheReaders,
	thresholdFn func() int,
	intervalFn func() time.Duration,
	checkTick time.Duration,
) *CacheFlushWorker {
	if thresholdFn == nil {
		panic("state: NewCacheFlushWorker requires non-nil thresholdFn")
	}
	if intervalFn == nil {
		panic("state: NewCacheFlushWorker requires non-nil intervalFn")
	}
	if checkTick <= 0 {
		panic("state: NewCacheFlushWorker requires positive checkTick")
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	return &CacheFlushWorker{
		engine:      engine,
		readers:     readers,
		thresholdFn: thresholdFn,
		intervalFn:  intervalFn,
		checkTick:   checkTick,
		stopCh:      make(chan struct{}),
		runCtx:      runCtx,
		runCancel:   runCancel,
	}
}

// Start launches the background flush goroutine.
func (w *CacheFlushWorker) Start() {
	w.lifecycleMu.Lock()
	if w.started || w.stopped {
		w.lifecycleMu.Unlock()
		return
	}
	w.started = true
	w.wg.Add(1)
	w.lifecycleMu.Unlock()
	go w.run()
}

// Stop signals the worker to stop and performs a final flush.
// Blocks until the goroutine exits.
func (w *CacheFlushWorker) Stop() {
	_ = w.StopContext(context.Background())
}

// StopContext stops the worker and performs one final flush. The caller's ctx
// only bounds how long that caller waits; the single stop owner continues with
// its own context after a waiter deadline so an admitted batch is not lost.
// All callers wait for that same stop owner with their own ctx, and the owner
// never starts a second final flush.
func (w *CacheFlushWorker) StopContext(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.stopMu.Lock()
	if w.stopping {
		done := w.stopDone
		w.stopMu.Unlock()
		select {
		case <-done:
			w.stopMu.Lock()
			err := w.stopErr
			w.stopMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.stopping = true
	done := make(chan struct{})
	w.stopDone = done
	w.stopMu.Unlock()

	go func() {
		// Do not let one waiter's deadline cancel the shared stop owner. A
		// caller may return while a non-cooperative reader unwinds; the owner
		// must still finish the one final flush before the DB can be closed.
		err := w.stopOwner(context.Background())
		w.stopMu.Lock()
		w.stopErr = err
		close(done)
		w.stopMu.Unlock()
	}()

	select {
	case <-done:
		w.stopMu.Lock()
		err := w.stopErr
		w.stopMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *CacheFlushWorker) stopOwner(ctx context.Context) error {
	w.lifecycleMu.Lock()
	w.stopped = true
	started := w.started
	if w.runCancel != nil {
		w.runCancel()
	}
	close(w.stopCh)
	w.lifecycleMu.Unlock()

	if started {
		w.wg.Wait()
	}
	// Control-plane mutations can persist state first and emit their cache
	// delete/upsert only during runtime cleanup. Keep dirty admission open until
	// that compound state owner has returned, or its final cache flush can miss
	// the runtime half of an already-committed mutation.
	w.engine.waitForStateWrites()
	w.CloseDirtyWriteAdmissionAndWait()
	if err := ctx.Err(); err != nil {
		return err
	}
	// The worker no longer flushes on its exit path. This owner flush runs
	// after all admitted marks and the worker are quiescent.
	err := w.doFlush(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// CloseDirtyWriteAdmission rejects late dirty marks without waiting for
// already-admitted marks. It is safe for a bounded shutdown barrier to call
// while the stop owner is still responsible for the final flush.
func (w *CacheFlushWorker) CloseDirtyWriteAdmission() {
	if w == nil || w.engine == nil {
		return
	}
	w.engine.CloseDirtyWriteAdmission()
}

// CloseDirtyWriteAdmissionAndWait rejects late dirty marks and waits for
// already-admitted marks. It does not stop the worker or flush; the stop owner
// uses it before its single final flush.
func (w *CacheFlushWorker) CloseDirtyWriteAdmissionAndWait() {
	if w == nil || w.engine == nil {
		return
	}
	w.engine.CloseDirtyWriteAdmissionAndWait()
}

func (w *CacheFlushWorker) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.checkTick)
	defer ticker.Stop()

	lastFlush := time.Now()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			dirty := w.engine.DirtyCount()
			if dirty == 0 {
				continue // Skip empty flush.
			}

			threshold := w.thresholdFn()
			interval := w.intervalFn()
			if dirty >= threshold || time.Since(lastFlush) >= interval {
				_ = w.doFlush(w.runCtx)
				lastFlush = time.Now()
			}
		}
	}
}

func (w *CacheFlushWorker) doFlush(ctx context.Context) error {
	err := w.engine.FlushDirtySetsContext(ctx, w.readers)
	if err != nil {
		log.Printf("[state] flush error (entries re-merged): %v", err)
	}
	return err
}
