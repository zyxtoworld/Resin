package metrics

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

var (
	errMetricsWrite    = errors.New("metrics write failed")
	errMetricsCommit   = errors.New("metrics commit failed")
	errMetricsRollback = errors.New("metrics rollback failed")
)

type metricsTransactionFailureDriver struct {
	mode  string
	opens atomic.Int32
}

type metricsTransactionFailureConn struct {
	driver *metricsTransactionFailureDriver
	id     int32
}

type metricsTransactionFailureTx struct {
	driver *metricsTransactionFailureDriver
}

type metricsTransactionFailureResult struct{}

var metricsTransactionFailureDriverID atomic.Uint64

func (d *metricsTransactionFailureDriver) Open(string) (driver.Conn, error) {
	return &metricsTransactionFailureConn{driver: d, id: d.opens.Add(1)}, nil
}

func (c *metricsTransactionFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (*metricsTransactionFailureConn) Close() error { return nil }

func (c *metricsTransactionFailureConn) Begin() (driver.Tx, error) {
	return &metricsTransactionFailureTx{driver: c.driver}, nil
}

func (c *metricsTransactionFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "PRAGMA busy_timeout=") {
		return metricsTransactionFailureResult{}, nil
	}
	if c.driver.mode == "write" {
		c.driver.mode = "write-seen"
		return nil, errMetricsWrite
	}
	return metricsTransactionFailureResult{}, nil
}

func (tx *metricsTransactionFailureTx) Commit() error {
	if tx.driver.mode == "commit" {
		return errMetricsCommit
	}
	return nil
}

func (tx *metricsTransactionFailureTx) Rollback() error { return errMetricsRollback }

func (metricsTransactionFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (metricsTransactionFailureResult) RowsAffected() (int64, error) { return 0, nil }

func TestMetricsRepo_FailedTransactionDiscardsConnectionWhenRollbackOrCommitFails(t *testing.T) {
	for _, mode := range []string{"write", "commit"} {
		t.Run(mode, func(t *testing.T) {
			driverName := fmt.Sprintf("resin-metrics-transaction-failure-%d", metricsTransactionFailureDriverID.Add(1))
			drv := &metricsTransactionFailureDriver{mode: mode}
			sql.Register(driverName, drv)

			db, err := sql.Open(driverName, "")
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)

			repo := &MetricsRepo{db: db}
			writeErr := repo.WritePersistTaskContext(context.Background(), &BucketFlushData{
				BucketStartUnix: 1,
				Requests:        map[string]requestAccum{"": {Total: 1}},
			}, nil, nil, nil)
			if mode == "write" && !errors.Is(writeErr, errMetricsWrite) {
				t.Fatalf("write failure = %v, want %v", writeErr, errMetricsWrite)
			}
			if mode == "write" && !errors.Is(writeErr, errMetricsRollback) {
				t.Fatalf("write failure lost rollback error: %v", writeErr)
			}
			if mode == "commit" && !errors.Is(writeErr, errMetricsCommit) {
				t.Fatalf("commit failure = %v, want %v", writeErr, errMetricsCommit)
			}

			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatalf("db.Conn after failed transaction: %v", err)
			}
			defer conn.Close()
			var id int32
			if err := conn.Raw(func(raw any) error {
				fake, ok := raw.(*metricsTransactionFailureConn)
				if !ok {
					return errors.New("unexpected driver connection type")
				}
				id = fake.id
				return nil
			}); err != nil {
				t.Fatalf("inspect replacement connection: %v", err)
			}
			if id == 1 {
				t.Fatalf("failed %s transaction returned connection 1 to pool; want a replacement", mode)
			}
		})
	}
}
