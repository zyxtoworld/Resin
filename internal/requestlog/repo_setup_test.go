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
