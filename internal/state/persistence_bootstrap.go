package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// persistenceCloser holds DB handles for cleanup. Implements io.Closer.
type persistenceCloser struct {
	engine  *StateEngine
	stateDB *sql.DB
	cacheDB *sql.DB
	closeMu sync.Mutex
	closed  bool
}

// PersistenceCloser owns the database handles returned by PersistenceBootstrap.
// CloseContext is the shutdown-aware form; Close retains the ordinary
// non-cancelable cleanup contract for callers that have no deadline.
type PersistenceCloser interface {
	io.Closer
	CloseContext(context.Context) error
}

// CloseContext closes write admission and waits for admitted strong writes
// within ctx. If ctx expires first, database handles remain open so an
// in-flight writer cannot race a close; a later caller can finish cleanup.
func (c *persistenceCloser) CloseContext(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// A canceled caller must not close the handles while another shutdown
	// owner (for example, the cache flush owner) is still unwinding. A later
	// non-canceled caller can finish the idempotent close.
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.engine != nil {
		if err := c.engine.CloseStateWriteAdmissionAndWait(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.closed = true
	return errors.Join(c.stateDB.Close(), c.cacheDB.Close())
}

func (c *persistenceCloser) Close() error {
	return c.CloseContext(context.Background())
}

// PersistenceBootstrap initializes both databases, runs consistency repair,
// and returns a ready-to-use StateEngine plus a PersistenceCloser for the DB
// handles.
//
// Steps:
//  1. Open/create state.db and cache.db with recommended pragmas.
//  2. Run schema migrations on both databases.
//  3. Run consistency repair (cross-db orphan cleanup).
//  4. Construct and return StateEngine.
func PersistenceBootstrap(stateDir, cacheDir string) (engine *StateEngine, closer PersistenceCloser, err error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create state dir %s: %w", stateDir, err)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create cache dir %s: %w", cacheDir, err)
	}

	stateDBPath := filepath.Join(stateDir, "state.db")
	cacheDBPath := filepath.Join(cacheDir, "cache.db")

	stateDB, err := OpenDB(stateDBPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open state.db: %w", err)
	}

	cacheDB, err := OpenDB(cacheDBPath)
	if err != nil {
		stateDB.Close()
		return nil, nil, fmt.Errorf("open cache.db: %w", err)
	}

	if err := MigrateStateDB(stateDB); err != nil {
		stateDB.Close()
		cacheDB.Close()
		return nil, nil, fmt.Errorf("migrate state.db: %w", err)
	}

	if err := MigrateCacheDB(cacheDB); err != nil {
		stateDB.Close()
		cacheDB.Close()
		return nil, nil, fmt.Errorf("migrate cache.db: %w", err)
	}

	if err := RepairConsistency(stateDBPath, cacheDB); err != nil {
		stateDB.Close()
		cacheDB.Close()
		return nil, nil, fmt.Errorf("repair consistency: %w", err)
	}

	stateRepo := newStateRepo(stateDB)
	cacheRepo := newCacheRepo(cacheDB)
	engine = newStateEngine(stateRepo, cacheRepo)

	return engine, &persistenceCloser{engine: engine, stateDB: stateDB, cacheDB: cacheDB}, nil
}
