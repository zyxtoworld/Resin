package topology

import (
	"context"
	"sync"
)

// runtimeBatchGate coordinates complete runtime generations. Reads remain
// concurrent, writers are exclusive, and a waiting writer can abandon its
// admission when its caller context is canceled.
type runtimeBatchGate struct {
	once sync.Once
	mu   sync.Mutex
	cond *sync.Cond

	readers       int
	writer        bool
	waitingWriter int

	// Package-private test seams. Production leaves them nil.
	afterReaderBlocked func()
	afterWriterQueued  func()
}

func (g *runtimeBatchGate) init() {
	g.once.Do(func() {
		g.cond = sync.NewCond(&g.mu)
	})
}

func (g *runtimeBatchGate) readLock() {
	_ = g.readLockContext(context.Background())
}

func (g *runtimeBatchGate) readLockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	g.init()
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	if g.writer || g.waitingWriter > 0 {
		if hook := g.afterReaderBlocked; hook != nil {
			hook()
		}
	}
	var stopWake func() bool
	if ctx.Done() != nil {
		stopWake = context.AfterFunc(ctx, func() {
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}
	for g.writer || g.waitingWriter > 0 {
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			if stopWake != nil {
				stopWake()
			}
			return err
		}
		g.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		if stopWake != nil {
			stopWake()
		}
		return err
	}
	g.readers++
	g.mu.Unlock()
	if stopWake != nil {
		stopWake()
	}
	return nil
}

func (g *runtimeBatchGate) readUnlock() {
	g.mu.Lock()
	if g.readers == 0 {
		g.mu.Unlock()
		panic("topology: runtime batch read unlock without read lock")
	}
	g.readers--
	if g.readers == 0 {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

func (g *runtimeBatchGate) tryReadLock() bool {
	g.init()
	g.mu.Lock()
	if g.writer || g.waitingWriter > 0 {
		g.mu.Unlock()
		return false
	}
	g.readers++
	g.mu.Unlock()
	return true
}

func (g *runtimeBatchGate) writeLock() {
	_ = g.writeLockContext(context.Background())
}

func (g *runtimeBatchGate) writeLockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	g.init()
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	g.waitingWriter++
	if hook := g.afterWriterQueued; hook != nil {
		hook()
	}
	stopWake := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	})

	for g.writer || g.readers > 0 {
		if err := ctx.Err(); err != nil {
			g.waitingWriter--
			g.cond.Broadcast()
			g.mu.Unlock()
			stopWake()
			return err
		}
		g.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		g.waitingWriter--
		g.cond.Broadcast()
		g.mu.Unlock()
		stopWake()
		return err
	}

	g.waitingWriter--
	g.writer = true
	g.mu.Unlock()
	stopWake()
	return nil
}

func (g *runtimeBatchGate) writeUnlock() {
	g.mu.Lock()
	if !g.writer {
		g.mu.Unlock()
		panic("topology: runtime batch write unlock without write lock")
	}
	g.writer = false
	g.cond.Broadcast()
	g.mu.Unlock()
}
