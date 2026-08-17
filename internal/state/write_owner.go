package state

import (
	"context"
	"sync"
)

// stateWriteMutex serializes strong state-repository mutations while allowing
// a request-bound waiter to abandon admission before the mutation starts.
// Its zero value is ready for use; Lock/Unlock preserve the Background caller
// contract used by non-request paths and existing tests.
type stateWriteMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *stateWriteMutex) init() {
	m.token = make(chan struct{}, 1)
	m.token <- struct{}{}
}

func (m *stateWriteMutex) lockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.once.Do(m.init)
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

func (m *stateWriteMutex) Lock() {
	_ = m.lockContext(context.Background())
}

func (m *stateWriteMutex) Unlock() {
	m.once.Do(m.init)
	m.token <- struct{}{}
}
