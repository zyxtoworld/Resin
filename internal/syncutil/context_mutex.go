package syncutil

import (
	"context"
	"math"
	"sync"

	"golang.org/x/sync/semaphore"
)

// Mutex is a zero-value, cancellation-aware exclusive lock.
type Mutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *Mutex) init() {
	m.once.Do(func() {
		m.sem = semaphore.NewWeighted(1)
	})
}

func (m *Mutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *Mutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.init()
	return m.sem.Acquire(ctx, 1)
}

func (m *Mutex) Unlock() {
	m.init()
	m.sem.Release(1)
}

const rwMutexWriteWeight int64 = math.MaxInt64

// RWMutex is a zero-value, cancellation-aware reader/writer lock. Readers run
// concurrently; queued writers block later readers, preventing writer
// starvation while still allowing a waiting caller to abandon admission.
type RWMutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *RWMutex) init() {
	m.once.Do(func() {
		m.sem = semaphore.NewWeighted(rwMutexWriteWeight)
	})
}

func (m *RWMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *RWMutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.init()
	return m.sem.Acquire(ctx, rwMutexWriteWeight)
}

func (m *RWMutex) Unlock() {
	m.init()
	m.sem.Release(rwMutexWriteWeight)
}

func (m *RWMutex) RLock() {
	_ = m.RLockContext(context.Background())
}

func (m *RWMutex) RLockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.init()
	return m.sem.Acquire(ctx, 1)
}

func (m *RWMutex) RUnlock() {
	m.init()
	m.sem.Release(1)
}
