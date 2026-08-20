package requestlog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
)

type requestLogSetupFailureDriver struct {
	failOnce atomic.Bool
}

var requestLogSetupFailureDriverID atomic.Uint64

func (d *requestLogSetupFailureDriver) Open(string) (driver.Conn, error) {
	return &requestLogSetupFailureConn{driver: d, busyTimeoutMs: state.DefaultSQLiteBusyTimeoutMs}, nil
}

type requestLogSetupFailureConn struct {
	driver        *requestLogSetupFailureDriver
	mu            sync.Mutex
	busyTimeoutMs int64
}

func (c *requestLogSetupFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *requestLogSetupFailureConn) Close() error { return nil }

func (c *requestLogSetupFailureConn) Begin() (driver.Tx, error) {
	return requestLogSetupFailureTx{}, nil
}

func (c *requestLogSetupFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(query, "PRAGMA busy_timeout=") {
		value, err := strconv.ParseInt(strings.TrimPrefix(query, "PRAGMA busy_timeout="), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse busy timeout: %w", err)
		}
		c.mu.Lock()
		c.busyTimeoutMs = value
		c.mu.Unlock()
		if value != state.DefaultSQLiteBusyTimeoutMs && c.driver.failOnce.CompareAndSwap(false, true) {
			return nil, errors.New("setup failed after applying busy timeout")
		}
	}
	return requestLogSetupFailureResult{}, nil
}

func (c *requestLogSetupFailureConn) busyTimeout() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busyTimeoutMs
}

type requestLogSetupFailureTx struct{}

func (requestLogSetupFailureTx) Commit() error   { return nil }
func (requestLogSetupFailureTx) Rollback() error { return nil }

type requestLogSetupFailureResult struct{}

func (requestLogSetupFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (requestLogSetupFailureResult) RowsAffected() (int64, error) { return 0, nil }

func TestRepo_InsertBatchContext_DiscardsConnectionWhenBusyTimeoutSetupFails(t *testing.T) {
	driverName := fmt.Sprintf("resin-requestlog-setup-failure-%d", requestLogSetupFailureDriverID.Add(1))
	drv := &requestLogSetupFailureDriver{}
	sql.Register(driverName, drv)

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	repo := NewRepo(t.TempDir(), 1<<60, 2)
	repo.activeDB = db
	repo.activePath = filepath.Join(repo.logDir, "active.db")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = repo.insertBatch(ctx, []proxy.RequestLogEntry{{ID: "setup-failure"}}, true)
	if err == nil {
		t.Fatal("insertBatch unexpectedly succeeded after busy-timeout setup failure")
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn after failed setup: %v", err)
	}
	defer conn.Close()

	var got int64
	if err := conn.Raw(func(raw any) error {
		fake, ok := raw.(*requestLogSetupFailureConn)
		if !ok {
			return errors.New("unexpected driver connection type")
		}
		got = fake.busyTimeout()
		return nil
	}); err != nil {
		t.Fatalf("inspect pooled connection: %v", err)
	}
	if got != state.DefaultSQLiteBusyTimeoutMs {
		t.Fatalf("failed setup returned a polluted connection: busy_timeout=%d, want %d", got, state.DefaultSQLiteBusyTimeoutMs)
	}
}

type requestLogTxCleanupFailureMode int

const (
	requestLogTxStatementAndRollbackFail requestLogTxCleanupFailureMode = iota
	requestLogTxCommitFail
)

var (
	errRequestLogTxStatement = errors.New("statement failed")
	errRequestLogTxRollback  = errors.New("rollback failed")
	errRequestLogTxCommit    = errors.New("commit failed")
)

type requestLogTxCleanupFailureDriver struct {
	mode   requestLogTxCleanupFailureMode
	nextID atomic.Int64
}

func (d *requestLogTxCleanupFailureDriver) Open(string) (driver.Conn, error) {
	return &requestLogTxCleanupFailureConn{driver: d, id: d.nextID.Add(1)}, nil
}

type requestLogTxCleanupFailureConn struct {
	driver *requestLogTxCleanupFailureDriver
	id     int64
}

func (c *requestLogTxCleanupFailureConn) Prepare(query string) (driver.Stmt, error) {
	return &requestLogTxCleanupFailureStmt{conn: c, query: query}, nil
}

func (c *requestLogTxCleanupFailureConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (*requestLogTxCleanupFailureConn) Close() error { return nil }

func (c *requestLogTxCleanupFailureConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *requestLogTxCleanupFailureConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &requestLogTxCleanupFailureTx{conn: c}, nil
}

func (*requestLogTxCleanupFailureConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return requestLogSetupFailureResult{}, nil
}

type requestLogTxCleanupFailureStmt struct {
	conn  *requestLogTxCleanupFailureConn
	query string
}

func (*requestLogTxCleanupFailureStmt) Close() error  { return nil }
func (*requestLogTxCleanupFailureStmt) NumInput() int { return -1 }

func (s *requestLogTxCleanupFailureStmt) Exec([]driver.Value) (driver.Result, error) {
	return s.exec()
}

func (s *requestLogTxCleanupFailureStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return s.exec()
}

func (s *requestLogTxCleanupFailureStmt) exec() (driver.Result, error) {
	if s.conn.driver.mode == requestLogTxStatementAndRollbackFail && strings.Contains(s.query, "INSERT OR IGNORE INTO request_logs") {
		return nil, errRequestLogTxStatement
	}
	return requestLogSetupFailureResult{}, nil
}

func (*requestLogTxCleanupFailureStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("query is not supported")
}

type requestLogTxCleanupFailureTx struct {
	conn *requestLogTxCleanupFailureConn
}

func (tx *requestLogTxCleanupFailureTx) Commit() error {
	if tx.conn.driver.mode == requestLogTxCommitFail {
		return errRequestLogTxCommit
	}
	return nil
}

func (tx *requestLogTxCleanupFailureTx) Rollback() error {
	if tx.conn.driver.mode == requestLogTxStatementAndRollbackFail {
		return errRequestLogTxRollback
	}
	return nil
}

func TestRepo_InsertBatchContext_DiscardsConnectionAfterTransactionCleanupFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		mode    requestLogTxCleanupFailureMode
		wantErr error
	}{
		"statement and rollback fail": {mode: requestLogTxStatementAndRollbackFail, wantErr: errRequestLogTxStatement},
		"commit fails":                {mode: requestLogTxCommitFail, wantErr: errRequestLogTxCommit},
	} {
		t.Run(name, func(t *testing.T) {
			driverName := fmt.Sprintf("resin-requestlog-tx-cleanup-failure-%d", requestLogSetupFailureDriverID.Add(1))
			drv := &requestLogTxCleanupFailureDriver{mode: tc.mode}
			sql.Register(driverName, drv)

			db, err := sql.Open(driverName, "")
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)

			repo := NewRepo(t.TempDir(), 1<<60, 2)
			repo.activeDB = db
			repo.activePath = filepath.Join(repo.logDir, "active.db")
			if _, err := repo.InsertBatchContext(context.Background(), []proxy.RequestLogEntry{{ID: "tx-cleanup-failure"}}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("InsertBatchContext error = %v, want primary error %v", err, tc.wantErr)
			}

			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatalf("db.Conn after failed transaction: %v", err)
			}
			defer conn.Close()
			var id int64
			if err := conn.Raw(func(raw any) error {
				fake, ok := raw.(*requestLogTxCleanupFailureConn)
				if !ok {
					return fmt.Errorf("unexpected driver connection type %T", raw)
				}
				id = fake.id
				return nil
			}); err != nil {
				t.Fatalf("inspect replacement connection: %v", err)
			}
			if id == 1 {
				t.Fatal("transaction state became unknown but the same connection returned to the pool")
			}
		})
	}
}
