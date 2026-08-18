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
)

type cacheSetupFailureDriver struct {
	failOnce atomic.Bool
}

var cacheSetupFailureDriverID atomic.Uint64

func (d *cacheSetupFailureDriver) Open(string) (driver.Conn, error) {
	return &cacheSetupFailureConn{driver: d, busyTimeoutMs: DefaultSQLiteBusyTimeoutMs}, nil
}

type cacheSetupFailureConn struct {
	driver        *cacheSetupFailureDriver
	mu            sync.Mutex
	busyTimeoutMs int64
	closed        bool
}

func (c *cacheSetupFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *cacheSetupFailureConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *cacheSetupFailureConn) Begin() (driver.Tx, error) {
	return cacheSetupFailureTx{}, nil
}

func (c *cacheSetupFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(query, "PRAGMA busy_timeout=") {
		value, err := strconv.ParseInt(strings.TrimPrefix(query, "PRAGMA busy_timeout="), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse busy timeout: %w", err)
		}
		c.mu.Lock()
		c.busyTimeoutMs = value
		c.mu.Unlock()
		if value != DefaultSQLiteBusyTimeoutMs && c.driver.failOnce.CompareAndSwap(false, true) {
			return nil, errors.New("setup failed after applying busy timeout")
		}
	}
	return cacheSetupFailureResult{}, nil
}

func (c *cacheSetupFailureConn) busyTimeout() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busyTimeoutMs
}

type cacheSetupFailureTx struct{}

func (cacheSetupFailureTx) Commit() error   { return nil }
func (cacheSetupFailureTx) Rollback() error { return nil }

type cacheSetupFailureResult struct{}

func (cacheSetupFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (cacheSetupFailureResult) RowsAffected() (int64, error) { return 0, nil }

func TestCacheFlushTxContext_DiscardsConnectionWhenBusyTimeoutSetupFails(t *testing.T) {
	driverName := fmt.Sprintf("resin-cache-setup-failure-%d", cacheSetupFailureDriverID.Add(1))
	drv := &cacheSetupFailureDriver{}
	sql.Register(driverName, drv)

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	repo := newCacheRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := repo.flushTx(ctx, FlushOps{}, true); err == nil {
		t.Fatal("flushTx unexpectedly succeeded after busy-timeout setup failure")
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn after failed setup: %v", err)
	}
	defer conn.Close()

	var got int64
	if err := conn.Raw(func(raw any) error {
		fake, ok := raw.(*cacheSetupFailureConn)
		if !ok {
			return errors.New("unexpected driver connection type")
		}
		got = fake.busyTimeout()
		return nil
	}); err != nil {
		t.Fatalf("inspect pooled connection: %v", err)
	}
	if got != DefaultSQLiteBusyTimeoutMs {
		t.Fatalf("failed setup returned a polluted connection: busy_timeout=%d, want %d", got, DefaultSQLiteBusyTimeoutMs)
	}
}
