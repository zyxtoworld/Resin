package platform

import (
	"context"
	"sync"
)

// contextMutex is a zero-value, cancellation-aware mutex for a small
// admission boundary. Cancellation only affects callers waiting to acquire;
// once admitted, the owner must finish its publication and unlock.
type contextMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *contextMutex) init() {
	m.once.Do(func() {
		m.token = make(chan struct{}, 1)
		m.token <- struct{}{}
	})
}

func (m *contextMutex) lockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.init()
	select {
	case <-m.token:
		if err := ctx.Err(); err != nil {
			m.token <- struct{}{}
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *contextMutex) unlock() {
	m.init()
	m.token <- struct{}{}
}
