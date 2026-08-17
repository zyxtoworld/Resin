package requestlog

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const requestLogFlushAttemptTimeout = 100 * time.Millisecond
const requestLogFlushRetryDelay = 10 * time.Millisecond

// Service provides an async request log writer.
// EmitRequestLog performs a non-blocking channel send (drops on overflow).
// A background goroutine flushes batches to the Repo.
type Service struct {
	repo      *Repo
	queue     chan proxy.RequestLogEntry
	batchSize int
	interval  time.Duration
	flushReq  chan chan struct{}

	stopCh       chan struct{}
	wg           sync.WaitGroup
	runCtx       context.Context
	runCancel    context.CancelFunc
	stopMu       sync.Mutex
	stopDone     chan struct{}
	stopErr      error
	stopCtx      context.Context
	stopFlushErr error
	stopping     bool
	repoCloseMu  sync.Mutex
	repoClosed   bool
	// lifecycleMu serializes worker admission with Stop. The worker count is
	// incremented before Start releases this lock, so Stop cannot wait on an
	// empty WaitGroup and then race a late worker launch.
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool

	// emitMu closes admission before the flush loop is stopped. activeEmits
	// tracks EmitRequestLog calls that already passed admission so Stop cannot
	// finish while one can still enqueue into the queue.
	emitMu      sync.Mutex
	emitCond    *sync.Cond
	emitsClosed bool
	activeEmits int
	// Package-private seams for deterministic admission/Stop tests.
	beforeEmitHook           func()
	beforeEmitDrainHook      func()
	beforeStopWorkerWaitHook func()
	beforeStopFinalDrainHook func()
}

// ServiceConfig configures the request log service.
type ServiceConfig struct {
	Repo          *Repo
	QueueSize     int
	FlushBatch    int
	FlushInterval time.Duration
}

// NewService creates a new request log service.
func NewService(cfg ServiceConfig) *Service {
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 8192
	}
	batchSize := cfg.FlushBatch
	if batchSize <= 0 {
		batchSize = 4096
	}
	interval := cfg.FlushInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	s := &Service{
		repo:      cfg.Repo,
		queue:     make(chan proxy.RequestLogEntry, queueSize),
		batchSize: batchSize,
		interval:  interval,
		flushReq:  make(chan chan struct{}, 64),
		stopCh:    make(chan struct{}),
		runCtx:    runCtx,
		runCancel: runCancel,
	}
	s.emitCond = sync.NewCond(&s.emitMu)
	return s
}

// Start launches the background flush goroutine.
func (s *Service) Start() {
	s.lifecycleMu.Lock()
	if s.started || s.stopped {
		s.lifecycleMu.Unlock()
		return
	}
	s.started = true
	if s.repo != nil {
		s.repo.setReadBarrier(s.FlushNowContext)
	}
	s.wg.Add(1)
	s.lifecycleMu.Unlock()
	go s.flushLoop()
}

// Stop signals the flush loop to stop, drains remaining entries, and returns.
func (s *Service) Stop() {
	_ = s.StopContext(context.Background())
}

// StopContext stops the writer and drains queued entries. The caller's ctx
// only bounds how long that caller waits; the single admitted stop owner uses
// its own context so a waiter deadline cannot discard the final batch.
func (s *Service) StopContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.stopMu.Lock()
	if s.stopping {
		done := s.stopDone
		s.stopMu.Unlock()
		select {
		case <-done:
			s.stopMu.Lock()
			err := s.stopErr
			s.stopMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.stopping = true
	s.stopDone = make(chan struct{})
	done := s.stopDone
	s.stopMu.Unlock()

	go func() {
		// The first caller owns the shutdown sequence, but not its waiter
		// deadline. Once admitted, the owner must finish the final drain even if
		// that caller returns on context cancellation.
		err := s.stopOwner(context.Background())
		s.stopMu.Lock()
		s.stopErr = err
		close(done)
		s.stopMu.Unlock()
	}()

	select {
	case <-done:
		s.stopMu.Lock()
		err := s.stopErr
		s.stopMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseContext stops the writer and closes the owned repository after the
// single stop owner has completed.
func (s *Service) CloseContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stopErr := s.StopContext(ctx)
	if !s.stopCompleted() {
		return stopErr
	}

	s.repoCloseMu.Lock()
	defer s.repoCloseMu.Unlock()
	if s.repoClosed {
		return stopErr
	}
	var closeErr error
	if s.repo != nil {
		closeErr = s.repo.CloseContext(ctx)
	}
	if closeErr == nil {
		s.repoClosed = true
	}
	return errors.Join(stopErr, closeErr)
}

func (s *Service) stopCompleted() bool {
	s.stopMu.Lock()
	done := s.stopDone
	s.stopMu.Unlock()
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func (s *Service) stopOwner(ctx context.Context) error {
	s.stopMu.Lock()
	s.stopCtx = ctx
	s.stopMu.Unlock()

	s.lifecycleMu.Lock()
	started := s.started
	s.stopped = true
	if s.repo != nil {
		s.repo.setReadBarrier(nil)
	}
	s.lifecycleMu.Unlock()

	s.emitMu.Lock()
	s.emitsClosed = true
	hook := s.beforeEmitDrainHook
	s.emitMu.Unlock()
	if hook != nil {
		hook()
	}

	s.emitMu.Lock()
	for s.activeEmits != 0 {
		s.emitCond.Wait()
	}
	s.emitMu.Unlock()

	close(s.stopCh)
	// Interrupt any in-flight worker transaction before waiting for the worker.
	// The final drain below uses the independent stop-owner context, so a batch
	// aborted here can still be retried after an individual waiter returns.
	if s.runCancel != nil {
		s.runCancel()
	}
	if started {
		if hook := s.beforeStopWorkerWaitHook; hook != nil {
			hook()
		}
		workerDone := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(workerDone)
		}()
		select {
		case <-workerDone:
		case <-ctx.Done():
			if s.runCancel != nil {
				s.runCancel()
			}
			<-workerDone
		}
	}
	// No worker exists to drain the queue. Preserve Stop's drain contract
	// synchronously for entries emitted before Start.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.drainAndFlush(nil, ctx); err != nil {
		return err
	}
	s.stopMu.Lock()
	workerErr := s.stopFlushErr
	s.stopMu.Unlock()
	return workerErr
}

// EmitRequestLog enqueues a log entry. Non-blocking; drops on overflow.
func (s *Service) EmitRequestLog(entry proxy.RequestLogEntry) {
	if s == nil {
		return
	}
	s.emitMu.Lock()
	if s.emitsClosed {
		s.emitMu.Unlock()
		return
	}
	s.activeEmits++
	s.emitMu.Unlock()
	defer func() {
		s.emitMu.Lock()
		s.activeEmits--
		if s.activeEmits == 0 {
			s.emitCond.Broadcast()
		}
		s.emitMu.Unlock()
	}()
	if hook := s.beforeEmitHook; hook != nil {
		hook()
	}
	select {
	case s.queue <- entry:
	default:
		// Queue full — drop entry to avoid blocking hot path.
	}
}

// FlushNow asks the background writer to flush current buffered data to DB,
// then blocks until that flush attempt completes.
func (s *Service) FlushNow() {
	_ = s.FlushNowContext(context.Background())
}

// FlushNowContext asks the background writer to flush current buffered data
// to DB. The context bounds both queue admission and the wait for this
// barrier; the worker still owns the queued request and may finish it after
// the caller returns.
func (s *Service) FlushNowContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycleMu.Lock()
	started := s.started
	stopped := s.stopped
	s.lifecycleMu.Unlock()
	if !started || stopped {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	done := make(chan struct{})
	select {
	case s.flushReq <- done:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return nil
	}
}

// flushLoop runs until stopCh is closed, flushing on batch-size or timer.
func (s *Service) flushLoop() {
	defer s.wg.Done()

	batch := make([]proxy.RequestLogEntry, 0, s.batchSize)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case entry := <-s.queue:
			batch = append(batch, entry)
			if len(batch) >= s.batchSize {
				if err := s.flushWithRetry(batch, s.runCtx); err == nil {
					batch = batch[:0]
				}
			}

		case <-ticker.C:
			if len(batch) > 0 {
				if err := s.flushWithRetry(batch, s.runCtx); err == nil {
					batch = batch[:0]
				}
			}

		case done := <-s.flushReq:
			batch = s.flushOnBarrier(batch, done, s.runCtx)

		case <-s.stopCh:
			if hook := s.beforeStopFinalDrainHook; hook != nil {
				hook()
			}
			if err := s.drainAndFlush(batch, s.stopContext()); err != nil {
				s.recordStopFlushError(err)
			}
			return
		}
	}
}

func (s *Service) stopContext() context.Context {
	s.stopMu.Lock()
	ctx := s.stopCtx
	s.stopMu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *Service) recordStopFlushError(err error) {
	if err == nil {
		return
	}
	s.stopMu.Lock()
	if s.stopFlushErr == nil {
		s.stopFlushErr = err
	}
	s.stopMu.Unlock()
}

func (s *Service) flushOnBarrier(batch []proxy.RequestLogEntry, firstWaiter chan struct{}, ctx context.Context) []proxy.RequestLogEntry {
	waiters := []chan struct{}{firstWaiter}
	for {
		select {
		case done := <-s.flushReq:
			waiters = append(waiters, done)
		default:
			goto flushed
		}
	}

flushed:
	// Bound barrier work to current queue depth snapshot so queries cannot be
	// blocked indefinitely by sustained write traffic.
	pending := len(s.queue)
drainLoop:
	for i := 0; i < pending; i++ {
		select {
		case entry := <-s.queue:
			batch = append(batch, entry)
			if len(batch) >= s.batchSize {
				if err := s.flushWithRetry(batch, ctx); err != nil {
					return closeBarrierWaiters(waiters, batch)
				}
				batch = batch[:0]
			}
		default:
			break drainLoop
		}
	}
	if len(batch) > 0 {
		if err := s.flushWithRetry(batch, ctx); err != nil {
			return closeBarrierWaiters(waiters, batch)
		}
		batch = batch[:0]
	}
	return closeBarrierWaiters(waiters, batch)
}

func closeBarrierWaiters(waiters []chan struct{}, batch []proxy.RequestLogEntry) []proxy.RequestLogEntry {
	for _, done := range waiters {
		close(done)
	}
	return batch
}

func (s *Service) drainAndFlush(batch []proxy.RequestLogEntry, ctx context.Context) error {
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		select {
		case entry := <-s.queue:
			batch = append(batch, entry)
			if len(batch) >= s.batchSize {
				if err := s.flushWithRetry(batch, ctx); err != nil {
					return err
				}
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				return s.flushWithRetry(batch, ctx)
			}
			return nil
		}
	}
}

// flushWithRetry keeps the normal five-second SQLite wait budget while
// avoiding one uncancelable busy-handler sleep for contexts that only expose
// cancellation. Each attempt is short enough for Stop to interrupt. A SQLite
// busy result is retryable even when the driver returns it before the attempt
// context itself reports a deadline; that is still a transient lock, not a
// permanent flush failure.
func (s *Service) flushWithRetry(entries []proxy.RequestLogEntry, ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, hasDeadline := ctx.Deadline()
	retryUntil := time.Now().Add(time.Duration(state.DefaultSQLiteBusyTimeoutMs) * time.Millisecond)
	for {
		attemptCtx := ctx
		cancel := func() {}
		if ctx.Done() != nil && !hasDeadline {
			attemptCtx, cancel = context.WithTimeout(ctx, requestLogFlushAttemptTimeout)
		}
		err := s.flush(entries, attemptCtx)
		attemptErr := attemptCtx.Err()
		cancel()
		if err == nil || ctx.Err() != nil {
			return err
		}
		retryable := errors.Is(attemptErr, context.DeadlineExceeded) ||
			errors.Is(err, context.DeadlineExceeded) ||
			isSQLiteBusyError(err)
		if !retryable || !time.Now().Before(retryUntil) {
			return err
		}
		timer := time.NewTimer(requestLogFlushRetryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return err
		}
	}
}

func isSQLiteBusyError(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return sqliteErr.Code()&0xff == sqlite3.SQLITE_BUSY
}

func (s *Service) flush(entries []proxy.RequestLogEntry, ctx context.Context) error {
	if s.repo == nil {
		return nil
	}
	var n int
	var err error
	if ctx == nil || ctx.Done() == nil {
		n, err = s.repo.InsertBatch(entries)
	} else {
		n, err = s.repo.InsertBatchContext(ctx, entries)
	}
	if err != nil {
		log.Printf("[requestlog] flush %d entries failed: %v", len(entries), err)
	} else if n > 0 {
		log.Printf("[requestlog] flushed %d entries", n)
	}
	return err
}

// Repo returns the underlying repository for query access.
func (s *Service) Repo() *Repo {
	return s.repo
}
