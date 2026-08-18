package routing

import (
	"context"
	"sync"
)

// contextRWMutex is the lifecycle owner used by Router. The zero value is
// ready for use, readers may run in parallel, and waiting writers have
// priority so a steady stream of snapshots cannot starve platform removal.
type contextRWMutex struct {
	once           sync.Once
	mu             sync.Mutex
	cond           *sync.Cond
	readers        int
	writer         bool
	waitingWriters int
}

func (m *contextRWMutex) ensure() {
	m.once.Do(func() {
		m.cond = sync.NewCond(&m.mu)
	})
}

func (m *contextRWMutex) RLock() {
	m.ensure()
	m.mu.Lock()
	for m.writer || m.waitingWriters > 0 {
		m.cond.Wait()
	}
	m.readers++
	m.mu.Unlock()
}

func (m *contextRWMutex) rLockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.ensure()
	m.mu.Lock()
	stopWake := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	for {
		if !m.writer && m.waitingWriters == 0 {
			m.readers++
			m.mu.Unlock()
			stopWake()
			return nil
		}
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			stopWake()
			return err
		}
		m.cond.Wait()
	}
}

func (m *contextRWMutex) RUnlock() {
	m.ensure()
	m.mu.Lock()
	if m.readers == 0 {
		m.mu.Unlock()
		panic("contextRWMutex: RUnlock of unlocked mutex")
	}
	m.readers--
	if m.readers == 0 {
		m.cond.Broadcast()
	}
	m.mu.Unlock()
}

func (m *contextRWMutex) Lock() {
	m.ensure()
	m.mu.Lock()
	for m.writer || m.readers > 0 {
		m.cond.Wait()
	}
	m.writer = true
	m.mu.Unlock()
}

func (m *contextRWMutex) lockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.ensure()
	m.mu.Lock()
	m.waitingWriters++
	stopWake := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	for {
		if !m.writer && m.readers == 0 {
			m.waitingWriters--
			m.writer = true
			m.mu.Unlock()
			stopWake()
			return nil
		}
		if err := ctx.Err(); err != nil {
			m.waitingWriters--
			m.cond.Broadcast()
			m.mu.Unlock()
			stopWake()
			return err
		}
		m.cond.Wait()
	}
}

func (m *contextRWMutex) Unlock() {
	m.ensure()
	m.mu.Lock()
	if !m.writer {
		m.mu.Unlock()
		panic("contextRWMutex: Unlock of unlocked mutex")
	}
	m.writer = false
	m.cond.Broadcast()
	m.mu.Unlock()
}
