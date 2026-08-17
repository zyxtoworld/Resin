package state

import (
	"context"
	"time"
)

// DefaultSQLiteBusyTimeoutMs is the normal SQLite lock wait budget used by
// every Resin SQLite database.
const DefaultSQLiteBusyTimeoutMs int64 = 5000

// ContextSQLiteBusyTimeoutMs keeps a cancelable write from sleeping inside
// SQLite's busy handler for the full background wait budget. The repository
// retries SQLITE_BUSY while the request context is still alive.
const contextSQLiteBusyTimeoutMs int64 = 50

type contextSQLiteWriteKey struct{}

func isContextSQLiteWrite(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	interruptible, _ := ctx.Value(contextSQLiteWriteKey{}).(bool)
	return interruptible
}

// SQLiteBusyTimeoutMs returns a per-operation busy timeout that respects ctx's
// deadline without ever extending the normal SQLite wait budget.
func SQLiteBusyTimeoutMs(ctx context.Context) (int64, error) {
	if ctx == nil {
		return DefaultSQLiteBusyTimeoutMs, nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return DefaultSQLiteBusyTimeoutMs, nil
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return 0, context.DeadlineExceeded
	}

	remainingMs := int64(remaining / time.Millisecond)
	if remaining%time.Millisecond != 0 {
		remainingMs++
	}
	if remainingMs < 1 {
		remainingMs = 1
	}
	if remainingMs > DefaultSQLiteBusyTimeoutMs {
		return DefaultSQLiteBusyTimeoutMs, nil
	}
	return remainingMs, nil
}
