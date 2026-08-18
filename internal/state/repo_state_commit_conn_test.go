package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
)

type stateCommitResetFailureDriver struct {
	failResetOnce atomic.Bool
}

var stateCommitResetFailureDriverID atomic.Uint64

func (d *stateCommitResetFailureDriver) Open(string) (driver.Conn, error) {
	return &stateCommitResetFailureConn{
		driver:        d,
		busyTimeoutMs: DefaultSQLiteBusyTimeoutMs,
	}, nil
}

type stateCommitResetFailureConn struct {
	driver        *stateCommitResetFailureDriver
	mu            sync.Mutex
	busyTimeoutMs int64
}

func (c *stateCommitResetFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *stateCommitResetFailureConn) Close() error { return nil }

func (c *stateCommitResetFailureConn) Begin() (driver.Tx, error) {
	return stateCommitResetFailureTx{}, nil
}

func (c *stateCommitResetFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(query, "PRAGMA busy_timeout=") {
		value, err := strconv.ParseInt(strings.TrimPrefix(query, "PRAGMA busy_timeout="), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse busy timeout: %w", err)
		}
		c.mu.Lock()
		c.busyTimeoutMs = value
		c.mu.Unlock()
		if value == DefaultSQLiteBusyTimeoutMs && c.driver.failResetOnce.CompareAndSwap(false, true) {
			c.mu.Lock()
			c.busyTimeoutMs = 50
			c.mu.Unlock()
			return nil, errors.New("reset failed after applying replacement timeout")
		}
	}
	return stateCommitResetFailureResult{}, nil
}

func (c *stateCommitResetFailureConn) busyTimeout() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busyTimeoutMs
}

type stateCommitResetFailureTx struct{}

func (stateCommitResetFailureTx) Commit() error   { return nil }
func (stateCommitResetFailureTx) Rollback() error { return nil }

type stateCommitResetFailureResult struct{}

func (stateCommitResetFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (stateCommitResetFailureResult) RowsAffected() (int64, error) { return 0, nil }

func TestStateRepo_WithCommitTxDiscardsConnectionWhenBusyTimeoutResetFails(t *testing.T) {
	driverName := fmt.Sprintf("resin-state-commit-reset-failure-%d", stateCommitResetFailureDriverID.Add(1))
	drv := &stateCommitResetFailureDriver{}
	sql.Register(driverName, drv)

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	repo := newStateRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := repo.withCommitTx(ctx, func(context.Context, *sql.Conn) error { return nil }); err != nil {
		t.Fatalf("withCommitTx: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn after failed reset: %v", err)
	}
	defer conn.Close()

	var got int64
	if err := conn.Raw(func(raw any) error {
		fake, ok := raw.(*stateCommitResetFailureConn)
		if !ok {
			return errors.New("unexpected driver connection type")
		}
		got = fake.busyTimeout()
		return nil
	}); err != nil {
		t.Fatalf("inspect pooled connection: %v", err)
	}
	if got != DefaultSQLiteBusyTimeoutMs {
		t.Fatalf("reset failure returned a polluted connection: busy_timeout=%d, want %d", got, DefaultSQLiteBusyTimeoutMs)
	}
}

type stateCommitRollbackFailureDriver struct {
	failCommit bool
	failSetup  bool
	opens      atomic.Int32
	setupOnce  atomic.Bool
}

var stateCommitRollbackFailureDriverID atomic.Uint64

func (d *stateCommitRollbackFailureDriver) Open(string) (driver.Conn, error) {
	return &stateCommitRollbackFailureConn{
		driver:        d,
		connectionID:  d.opens.Add(1),
		busyTimeoutMs: DefaultSQLiteBusyTimeoutMs,
	}, nil
}

type stateCommitRollbackFailureConn struct {
	driver        *stateCommitRollbackFailureDriver
	connectionID  int32
	mu            sync.Mutex
	busyTimeoutMs int64
}

func (c *stateCommitRollbackFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *stateCommitRollbackFailureConn) Close() error { return nil }

func (c *stateCommitRollbackFailureConn) Begin() (driver.Tx, error) {
	return stateCommitRollbackFailureTx{}, nil
}

func (c *stateCommitRollbackFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	query = strings.TrimSpace(query)
	switch {
	case strings.HasPrefix(query, "PRAGMA busy_timeout="):
		value, err := strconv.ParseInt(strings.TrimPrefix(query, "PRAGMA busy_timeout="), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse busy timeout: %w", err)
		}
		c.mu.Lock()
		c.busyTimeoutMs = value
		c.mu.Unlock()
		if c.driver.failSetup && value != DefaultSQLiteBusyTimeoutMs && c.driver.setupOnce.CompareAndSwap(false, true) {
			return nil, errors.New("setup failed after applying replacement timeout")
		}
	case query == "ROLLBACK":
		return nil, errors.New("rollback failed")
	case query == "COMMIT" && c.driver.failCommit:
		return nil, errors.New("commit failed")
	}
	return stateCommitRollbackFailureResult{}, nil
}

func (c *stateCommitRollbackFailureConn) connectionIDValue() int32 {
	return c.connectionID
}

func (c *stateCommitRollbackFailureConn) busyTimeout() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busyTimeoutMs
}

type stateCommitRollbackFailureTx struct{}

func (stateCommitRollbackFailureTx) Commit() error   { return nil }
func (stateCommitRollbackFailureTx) Rollback() error { return nil }

type stateCommitRollbackFailureResult struct{}

func (stateCommitRollbackFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (stateCommitRollbackFailureResult) RowsAffected() (int64, error) { return 0, nil }

func TestStateRepo_WithCommitTxDiscardsConnectionWhenRollbackFails(t *testing.T) {
	tests := []struct {
		name       string
		failCommit bool
		fnErr      error
	}{
		{name: "function", fnErr: errors.New("mutation failed")},
		{name: "commit", failCommit: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			driverName := fmt.Sprintf("resin-state-commit-rollback-failure-%d", stateCommitRollbackFailureDriverID.Add(1))
			drv := &stateCommitRollbackFailureDriver{failCommit: tc.failCommit}
			sql.Register(driverName, drv)

			db, err := sql.Open(driverName, "")
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)

			repo := newStateRepo(db)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var gotFnErr error
			if tc.fnErr != nil {
				gotFnErr = repo.withCommitTx(ctx, func(context.Context, *sql.Conn) error {
					return tc.fnErr
				})
			} else {
				gotFnErr = repo.withCommitTx(ctx, func(context.Context, *sql.Conn) error { return nil })
			}
			if tc.fnErr != nil && !errors.Is(gotFnErr, tc.fnErr) {
				t.Fatalf("withCommitTx error = %v, want function error %v", gotFnErr, tc.fnErr)
			}
			if tc.failCommit && !strings.Contains(gotFnErr.Error(), "commit failed") {
				t.Fatalf("withCommitTx error = %v, want commit failure", gotFnErr)
			}

			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatalf("db.Conn after rollback failure: %v", err)
			}
			defer conn.Close()
			var gotID int32
			if err := conn.Raw(func(raw any) error {
				fake, ok := raw.(*stateCommitRollbackFailureConn)
				if !ok {
					return errors.New("unexpected driver connection type")
				}
				gotID = fake.connectionIDValue()
				return nil
			}); err != nil {
				t.Fatalf("inspect replacement connection: %v", err)
			}
			if gotID == 1 {
				t.Fatalf("rollback failure returned connection %d to pool; want a replacement connection", gotID)
			}
		})
	}
}

func TestStateRepo_WithCommitTxDiscardsConnectionWhenBusyTimeoutSetupFails(t *testing.T) {
	driverName := fmt.Sprintf("resin-state-commit-setup-failure-%d", stateCommitRollbackFailureDriverID.Add(1))
	drv := &stateCommitRollbackFailureDriver{failSetup: true}
	sql.Register(driverName, drv)

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	repo := newStateRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := repo.withCommitTx(ctx, func(context.Context, *sql.Conn) error { return nil }); err == nil || !strings.Contains(err.Error(), "setup failed") {
		t.Fatalf("withCommitTx error = %v, want setup failure", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn after failed setup: %v", err)
	}
	defer conn.Close()
	var gotID int32
	var gotTimeout int64
	if err := conn.Raw(func(raw any) error {
		fake, ok := raw.(*stateCommitRollbackFailureConn)
		if !ok {
			return errors.New("unexpected driver connection type")
		}
		gotID = fake.connectionIDValue()
		gotTimeout = fake.busyTimeout()
		return nil
	}); err != nil {
		t.Fatalf("inspect replacement connection: %v", err)
	}
	if gotID == 1 {
		t.Fatalf("setup failure returned connection %d to pool; want a replacement connection", gotID)
	}
	if gotTimeout != DefaultSQLiteBusyTimeoutMs {
		t.Fatalf("replacement connection busy_timeout=%d, want %d", gotTimeout, DefaultSQLiteBusyTimeoutMs)
	}
}

func TestStateRepo_LegacyWriteUsesSingleConnectionOwner(t *testing.T) {
	dbPath := t.TempDir() + "/state.db"
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	if err := MigrateStateDB(db); err != nil {
		t.Fatalf("MigrateStateDB: %v", err)
	}
	db.SetMaxOpenConns(1)

	repo := newStateRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint := model.Endpoint{
		ID:               "single-connection-owner",
		Port:             19081,
		Enabled:          true,
		AllowManagement:  true,
		AllowHTTPForward: true,
		AllowHTTPReverse: true,
		AllowSOCKS5:      true,
		CreatedAtNs:      time.Now().UnixNano(),
		UpdatedAtNs:      time.Now().UnixNano(),
	}
	if err := repo.InsertEndpointContext(ctx, endpoint); err != nil {
		t.Fatalf("InsertEndpointContext with one connection: %v", err)
	}
	got, err := repo.GetEndpoint(endpoint.ID)
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if got.Port != endpoint.Port || !got.Enabled {
		t.Fatalf("persisted endpoint = %+v, want %+v", got, endpoint)
	}
}
