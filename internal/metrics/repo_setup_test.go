package metrics

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

	"github.com/Resinat/Resin/internal/state"
)

type metricsSetupFailureDriver struct {
	failOnce atomic.Bool
}

var metricsSetupFailureDriverID atomic.Uint64

func (d *metricsSetupFailureDriver) Open(string) (driver.Conn, error) {
	return &metricsSetupFailureConn{driver: d, busyTimeoutMs: state.DefaultSQLiteBusyTimeoutMs}, nil
}

type metricsSetupFailureConn struct {
	driver        *metricsSetupFailureDriver
	mu            sync.Mutex
	busyTimeoutMs int64
}

func (c *metricsSetupFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *metricsSetupFailureConn) Close() error { return nil }

func (c *metricsSetupFailureConn) Begin() (driver.Tx, error) {
	return metricsSetupFailureTx{}, nil
}

func (c *metricsSetupFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
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
	return metricsSetupFailureResult{}, nil
}

func (c *metricsSetupFailureConn) busyTimeout() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busyTimeoutMs
}

type metricsSetupFailureTx struct{}

func (metricsSetupFailureTx) Commit() error   { return nil }
func (metricsSetupFailureTx) Rollback() error { return nil }

type metricsSetupFailureResult struct{}

func (metricsSetupFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (metricsSetupFailureResult) RowsAffected() (int64, error) { return 0, nil }

func TestMetricsRepo_AcquireContextConnDiscardsConnectionWhenBusyTimeoutSetupFails(t *testing.T) {
	driverName := fmt.Sprintf("resin-metrics-setup-failure-%d", metricsSetupFailureDriverID.Add(1))
	drv := &metricsSetupFailureDriver{}
	sql.Register(driverName, drv)

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	repo := &MetricsRepo{db: db}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if conn, release, err := repo.acquireContextConn(ctx); err == nil {
		if release != nil {
			release()
		}
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatal("acquireContextConn unexpectedly succeeded after busy-timeout setup failure")
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn after failed setup: %v", err)
	}
	defer conn.Close()

	var got int64
	if err := conn.Raw(func(raw any) error {
		fake, ok := raw.(*metricsSetupFailureConn)
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
