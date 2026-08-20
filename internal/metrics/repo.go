package metrics

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/Resinat/Resin/internal/state"
)

const defaultSQLiteBusyTimeoutMs = state.DefaultSQLiteBusyTimeoutMs

// MetricsDBDDL defines the schema for metrics.db.
const MetricsDBDDL = `
CREATE TABLE IF NOT EXISTS metric_traffic_bucket (
	bucket_start_unix INTEGER PRIMARY KEY,
	ingress_bytes     INTEGER NOT NULL DEFAULT 0,
	egress_bytes      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metric_request_bucket (
	bucket_start_unix  INTEGER NOT NULL,
	platform_id        TEXT,
	total_requests     INTEGER NOT NULL DEFAULT 0,
	success_requests   INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_request_bucket_dim
	ON metric_request_bucket(bucket_start_unix, platform_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_request_bucket_global
	ON metric_request_bucket(bucket_start_unix)
	WHERE platform_id IS NULL;

CREATE TABLE IF NOT EXISTS metric_access_latency_bucket (
	bucket_start_unix INTEGER NOT NULL,
	platform_id       TEXT,
	buckets_json      TEXT NOT NULL DEFAULT '[]'
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_access_latency_bucket_dim
	ON metric_access_latency_bucket(bucket_start_unix, platform_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_access_latency_bucket_global
	ON metric_access_latency_bucket(bucket_start_unix)
	WHERE platform_id IS NULL;

CREATE TABLE IF NOT EXISTS metric_probe_bucket (
	bucket_start_unix INTEGER PRIMARY KEY,
	total_count       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metric_node_pool_bucket (
	bucket_start_unix INTEGER PRIMARY KEY,
	total_nodes       INTEGER NOT NULL DEFAULT 0,
	healthy_nodes     INTEGER NOT NULL DEFAULT 0,
	egress_ip_count   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metric_lease_lifetime_bucket (
	bucket_start_unix INTEGER NOT NULL,
	platform_id       TEXT NOT NULL,
	sample_count      INTEGER NOT NULL DEFAULT 0,
	p1_ms             REAL NOT NULL DEFAULT 0,
	p5_ms             REAL NOT NULL DEFAULT 0,
	p50_ms            REAL NOT NULL DEFAULT 0,
	PRIMARY KEY (bucket_start_unix, platform_id)
);
`

// MetricsRepo handles persistence of metric buckets to metrics.db.
type MetricsRepo struct {
	db *sql.DB

	// Package-private seam for deterministic query lifecycle tests. It runs
	// after a query has acquired its rows and before the caller consumes them.
	afterQueryHook func(context.Context)
}

// NewMetricsRepo opens (or creates) metrics.db at the given path and initializes the schema.
func NewMetricsRepo(path string) (*MetricsRepo, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("metrics repo mkdir: %w", err)
	}
	db, err := state.OpenDB(path)
	if err != nil {
		return nil, fmt.Errorf("metrics repo open: %w", err)
	}
	if err := state.InitDB(db, MetricsDBDDL); err != nil {
		db.Close()
		return nil, fmt.Errorf("metrics repo init: %w", err)
	}
	return &MetricsRepo{db: db}, nil
}

// Close closes the database.
func (r *MetricsRepo) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// WriteBucket persists a bucket flush data set in a single transaction.
func (r *MetricsRepo) WriteBucket(data *BucketFlushData) error {
	return r.WriteBucketContext(context.Background(), data)
}

// WriteBucketContext persists a bucket flush data set with a cancellable
// SQLite transaction. Shutdown uses this path so a database lock cannot
// outlive the caller's lifecycle deadline.
func (r *MetricsRepo) WriteBucketContext(ctx context.Context, data *BucketFlushData) error {
	if data == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	return r.withMetricsTransaction(ctx, func(tx *sql.Tx) error {
		return writeBucketExec(ctx, tx, data)
	})
}

// WriteNodePoolSnapshot writes a node pool snapshot for a bucket.
func (r *MetricsRepo) WriteNodePoolSnapshot(bucketStartUnix int64, totalNodes, healthyNodes, egressIPCount int) error {
	return r.WriteNodePoolSnapshotContext(context.Background(), bucketStartUnix, totalNodes, healthyNodes, egressIPCount)
}

// WriteNodePoolSnapshotContext writes a node pool snapshot with a cancellable
// database operation.
func (r *MetricsRepo) WriteNodePoolSnapshotContext(ctx context.Context, bucketStartUnix int64, totalNodes, healthyNodes, egressIPCount int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, release, err := r.acquireContextConn(ctx)
	if err != nil {
		return fmt.Errorf("metrics repo acquire connection: %w", err)
	}
	defer release()
	return writeNodePoolExec(ctx, conn, bucketStartUnix, totalNodes, healthyNodes, egressIPCount)
}

// WriteLatencyBucket writes access latency histogram for a bucket.
func (r *MetricsRepo) WriteLatencyBucket(bucketStartUnix int64, platformID string, buckets []int64) error {
	return r.WriteLatencyBucketContext(context.Background(), bucketStartUnix, platformID, buckets)
}

// WriteLatencyBucketContext writes an access latency histogram with a
// cancellable database operation.
func (r *MetricsRepo) WriteLatencyBucketContext(ctx context.Context, bucketStartUnix int64, platformID string, buckets []int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, release, err := r.acquireContextConn(ctx)
	if err != nil {
		return fmt.Errorf("metrics repo acquire connection: %w", err)
	}
	defer release()
	return writeLatencyExec(ctx, conn, bucketStartUnix, platformID, buckets)
}

// WritePersistTaskContext commits one complete bucket persistence task as one
// transaction. A task is the retry ticket owned by MetricsManager; publishing
// only part of it would make a failed retry observable as a mixed generation.
func (r *MetricsRepo) WritePersistTaskContext(
	ctx context.Context,
	data *BucketFlushData,
	nodePool *nodePoolSnapshot,
	globalLatency []int64,
	platformLatency map[string][]int64,
) error {
	if data == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	return r.withMetricsTransaction(ctx, func(tx *sql.Tx) error {
		if err := writeBucketExec(ctx, tx, data); err != nil {
			return err
		}
		if nodePool != nil {
			if err := writeNodePoolExec(
				ctx,
				tx,
				data.BucketStartUnix,
				nodePool.TotalNodes,
				nodePool.HealthyNodes,
				nodePool.EgressIPCount,
			); err != nil {
				return fmt.Errorf("metrics repo upsert node pool snapshot: %w", err)
			}
		}
		if err := writeLatencyExec(ctx, tx, data.BucketStartUnix, "", globalLatency); err != nil {
			return fmt.Errorf("metrics repo upsert global latency bucket: %w", err)
		}
		for platformID, buckets := range platformLatency {
			if err := writeLatencyExec(ctx, tx, data.BucketStartUnix, platformID, buckets); err != nil {
				return fmt.Errorf("metrics repo upsert platform latency bucket %s: %w", platformID, err)
			}
		}
		return nil
	})
}

// withMetricsTransaction owns the complete sql.Tx lifecycle. A failed
// rollback or COMMIT leaves the driver's transaction state unknowable, so the
// connection is marked bad instead of being returned to database/sql's pool.
func (r *MetricsRepo) withMetricsTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	conn, release, err := r.acquireContextConn(ctx)
	if err != nil {
		return fmt.Errorf("metrics repo acquire connection: %w", err)
	}
	released := false
	discard := func() {
		if released {
			return
		}
		released = true
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}
	defer func() {
		if !released {
			release()
		}
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		discard()
		return fmt.Errorf("metrics repo begin: %w", err)
	}
	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			discard()
			return errors.Join(err, fmt.Errorf("metrics repo rollback: %w", rollbackErr))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		discard()
		return err
	}
	return nil
}

type metricsExecContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func writeBucketExec(ctx context.Context, exec metricsExecContext, data *BucketFlushData) error {
	// Traffic.
	_, err := exec.ExecContext(ctx, `INSERT INTO metric_traffic_bucket (bucket_start_unix, ingress_bytes, egress_bytes)
		VALUES (?,?,?) ON CONFLICT(bucket_start_unix)
		DO UPDATE SET ingress_bytes = excluded.ingress_bytes, egress_bytes = excluded.egress_bytes`,
		data.BucketStartUnix, data.Traffic.IngressBytes, data.Traffic.EgressBytes)
	if err != nil {
		return fmt.Errorf("metrics repo upsert global traffic: %w", err)
	}

	// Requests.
	globalRequests := requestAccum{}
	if rq, ok := data.Requests[""]; ok {
		globalRequests = rq
	}
	_, err = exec.ExecContext(ctx, `INSERT INTO metric_request_bucket (bucket_start_unix, platform_id, total_requests, success_requests)
		VALUES (?,NULL,?,?) ON CONFLICT(bucket_start_unix) WHERE platform_id IS NULL
		DO UPDATE SET total_requests = excluded.total_requests, success_requests = excluded.success_requests`,
		data.BucketStartUnix, globalRequests.Total, globalRequests.Success)
	if err != nil {
		return fmt.Errorf("metrics repo upsert global request: %w", err)
	}

	for pid, rq := range data.Requests {
		if pid == "" {
			continue
		}
		_, err = exec.ExecContext(ctx, `INSERT INTO metric_request_bucket (bucket_start_unix, platform_id, total_requests, success_requests)
			VALUES (?,?,?,?) ON CONFLICT(bucket_start_unix, platform_id)
			DO UPDATE SET total_requests = excluded.total_requests, success_requests = excluded.success_requests`,
			data.BucketStartUnix, pid, rq.Total, rq.Success)
		if err != nil {
			return fmt.Errorf("metrics repo upsert request: %w", err)
		}
	}

	// Probes.
	_, err = exec.ExecContext(ctx, `INSERT INTO metric_probe_bucket (bucket_start_unix, total_count)
		VALUES (?,?) ON CONFLICT(bucket_start_unix)
		DO UPDATE SET total_count = excluded.total_count`,
		data.BucketStartUnix, data.Probes.Total)
	if err != nil {
		return fmt.Errorf("metrics repo upsert probe: %w", err)
	}

	// Lease lifetimes.
	for pid, acc := range data.LeaseLifetimes {
		if len(acc.Samples) == 0 {
			continue
		}
		p1, p5, p50 := computePercentiles(acc.Samples)
		_, err := exec.ExecContext(ctx, `INSERT INTO metric_lease_lifetime_bucket (bucket_start_unix, platform_id, sample_count, p1_ms, p5_ms, p50_ms)
			VALUES (?,?,?,?,?,?) ON CONFLICT(bucket_start_unix, platform_id)
			DO UPDATE SET sample_count = excluded.sample_count, p1_ms = excluded.p1_ms, p5_ms = excluded.p5_ms, p50_ms = excluded.p50_ms`,
			data.BucketStartUnix, pid, len(acc.Samples), p1, p5, p50)
		if err != nil {
			return fmt.Errorf("metrics repo upsert lease lifetime: %w", err)
		}
	}
	return nil
}

func writeNodePoolExec(
	ctx context.Context,
	exec metricsExecContext,
	bucketStartUnix int64,
	totalNodes, healthyNodes, egressIPCount int,
) error {
	_, err := exec.ExecContext(ctx, `INSERT INTO metric_node_pool_bucket (bucket_start_unix, total_nodes, healthy_nodes, egress_ip_count)
		VALUES (?,?,?,?) ON CONFLICT(bucket_start_unix)
		DO UPDATE SET total_nodes = excluded.total_nodes, healthy_nodes = excluded.healthy_nodes, egress_ip_count = excluded.egress_ip_count`,
		bucketStartUnix, totalNodes, healthyNodes, egressIPCount)
	return err
}

func writeLatencyExec(
	ctx context.Context,
	exec metricsExecContext,
	bucketStartUnix int64,
	platformID string,
	buckets []int64,
) error {
	bucketsJSON, err := json.Marshal(buckets)
	if err != nil {
		return err
	}
	if platformID == "" {
		_, err = exec.ExecContext(ctx, `INSERT INTO metric_access_latency_bucket (bucket_start_unix, platform_id, buckets_json)
			VALUES (?,NULL,?) ON CONFLICT(bucket_start_unix) WHERE platform_id IS NULL
			DO UPDATE SET buckets_json = excluded.buckets_json`,
			bucketStartUnix, string(bucketsJSON))
		return err
	}
	_, err = exec.ExecContext(ctx, `INSERT INTO metric_access_latency_bucket (bucket_start_unix, platform_id, buckets_json)
		VALUES (?,?,?) ON CONFLICT(bucket_start_unix, platform_id)
		DO UPDATE SET buckets_json = excluded.buckets_json`,
		bucketStartUnix, platformID, string(bucketsJSON))
	return err
}

func (r *MetricsRepo) acquireContextConn(ctx context.Context) (*sql.Conn, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}

	busyTimeoutMs, timeoutErr := state.SQLiteBusyTimeoutMs(ctx)
	if timeoutErr != nil {
		_ = conn.Close()
		return nil, nil, timeoutErr
	}
	if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMs)); err != nil {
		// The driver may apply the pragma before reporting an error. Mark
		// the connection bad before Close so database/sql cannot return a
		// partially configured connection to the pool.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
		return nil, nil, err
	}
	release := func() {
		if ctx.Err() != nil {
			// A canceled operation may still be blocked by the same SQLite
			// writer lock. Do not run a background reset while releasing it;
			// discard the connection instead.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		} else if !resetSQLiteBusyTimeout(conn) {
			// Returning driver.ErrBadConn from Raw makes database/sql discard the
			// underlying connection instead of putting it back in the idle pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}
	return conn, release, nil
}

func resetSQLiteBusyTimeout(conn *sql.Conn) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", state.DefaultSQLiteBusyTimeoutMs))
	return err == nil
}

// TrafficBucketRow holds a single traffic bucket result.
type TrafficBucketRow struct {
	BucketStartUnix int64 `json:"bucket_start_unix"`
	IngressBytes    int64 `json:"ingress_bytes"`
	EgressBytes     int64 `json:"egress_bytes"`
}

// QueryTraffic returns traffic buckets in a time range.
func (r *MetricsRepo) QueryTraffic(from, to int64) ([]TrafficBucketRow, error) {
	return r.queryTraffic(context.Background(), from, to)
}

func (r *MetricsRepo) queryTraffic(ctx context.Context, from, to int64) ([]TrafficBucketRow, error) {
	q := `SELECT bucket_start_unix, ingress_bytes, egress_bytes
		FROM metric_traffic_bucket WHERE bucket_start_unix >= ? AND bucket_start_unix <= ?`
	args := []interface{}{from, to}
	q += " ORDER BY bucket_start_unix"
	rows, err := r.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TrafficBucketRow
	for rows.Next() {
		var row TrafficBucketRow
		if err := rows.Scan(&row.BucketStartUnix, &row.IngressBytes, &row.EgressBytes); err != nil {
			return nil, fmt.Errorf("metrics repo scan traffic bucket: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// RequestBucketRow holds a single request bucket result.
type RequestBucketRow struct {
	BucketStartUnix int64  `json:"bucket_start_unix"`
	PlatformID      string `json:"platform_id"`
	TotalRequests   int64  `json:"total_requests"`
	SuccessRequests int64  `json:"success_requests"`
}

// QueryRequests returns request buckets in a time range.
func (r *MetricsRepo) QueryRequests(from, to int64, platformID string) ([]RequestBucketRow, error) {
	return r.queryRequests(context.Background(), from, to, platformID)
}

func (r *MetricsRepo) queryRequests(ctx context.Context, from, to int64, platformID string) ([]RequestBucketRow, error) {
	q := `SELECT bucket_start_unix, platform_id, total_requests, success_requests
		FROM metric_request_bucket WHERE bucket_start_unix >= ? AND bucket_start_unix <= ?`
	args := []interface{}{from, to}
	if platformID != "" {
		q += " AND platform_id = ?"
		args = append(args, platformID)
	} else {
		// Empty platformID means global scope only.
		q += " AND platform_id IS NULL"
	}
	q += " ORDER BY bucket_start_unix"
	rows, err := r.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []RequestBucketRow
	for rows.Next() {
		var row RequestBucketRow
		var pid sql.NullString
		if err := rows.Scan(&row.BucketStartUnix, &pid, &row.TotalRequests, &row.SuccessRequests); err != nil {
			return nil, fmt.Errorf("metrics repo scan request bucket: %w", err)
		}
		if pid.Valid {
			row.PlatformID = pid.String
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// ProbeBucketRow holds a single probe bucket result.
type ProbeBucketRow struct {
	BucketStartUnix int64 `json:"bucket_start_unix"`
	TotalCount      int64 `json:"total_count"`
}

// QueryProbes returns probe buckets in a time range.
func (r *MetricsRepo) QueryProbes(from, to int64) ([]ProbeBucketRow, error) {
	return r.queryProbes(context.Background(), from, to)
}

func (r *MetricsRepo) queryProbes(ctx context.Context, from, to int64) ([]ProbeBucketRow, error) {
	rows, err := r.query(ctx, `SELECT bucket_start_unix, total_count
		FROM metric_probe_bucket WHERE bucket_start_unix >= ? AND bucket_start_unix <= ?
		ORDER BY bucket_start_unix`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ProbeBucketRow
	for rows.Next() {
		var row ProbeBucketRow
		if err := rows.Scan(&row.BucketStartUnix, &row.TotalCount); err != nil {
			return nil, fmt.Errorf("metrics repo scan probe bucket: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// NodePoolBucketRow holds a single node pool bucket result.
type NodePoolBucketRow struct {
	BucketStartUnix int64 `json:"bucket_start_unix"`
	TotalNodes      int   `json:"total_nodes"`
	HealthyNodes    int   `json:"healthy_nodes"`
	EgressIPCount   int   `json:"egress_ip_count"`
}

// QueryNodePool returns node pool buckets in a time range.
func (r *MetricsRepo) QueryNodePool(from, to int64) ([]NodePoolBucketRow, error) {
	return r.queryNodePool(context.Background(), from, to)
}

func (r *MetricsRepo) queryNodePool(ctx context.Context, from, to int64) ([]NodePoolBucketRow, error) {
	rows, err := r.query(ctx, `SELECT bucket_start_unix, total_nodes, healthy_nodes, egress_ip_count
		FROM metric_node_pool_bucket WHERE bucket_start_unix >= ? AND bucket_start_unix <= ?
		ORDER BY bucket_start_unix`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []NodePoolBucketRow
	for rows.Next() {
		var row NodePoolBucketRow
		if err := rows.Scan(&row.BucketStartUnix, &row.TotalNodes, &row.HealthyNodes, &row.EgressIPCount); err != nil {
			return nil, fmt.Errorf("metrics repo scan node pool bucket: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// computePercentiles computes P1, P5, P50 from a slice of nanosecond values, returning milliseconds.
func computePercentiles(samples []int64) (p1, p5, p50 float64) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sorted := make([]int64, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	nsToMs := func(ns int64) float64 { return float64(ns) / 1e6 }

	percentile := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return nsToMs(sorted[idx])
	}

	return percentile(0.01), percentile(0.05), percentile(0.50)
}

// AccessLatencyBucketRow holds a single access latency histogram bucket result.
type AccessLatencyBucketRow struct {
	BucketStartUnix int64  `json:"bucket_start_unix"`
	PlatformID      string `json:"platform_id"`
	BucketsJSON     string `json:"buckets_json"`
}

// QueryAccessLatency returns access latency histogram buckets in a time range.
func (r *MetricsRepo) QueryAccessLatency(from, to int64, platformID string) ([]AccessLatencyBucketRow, error) {
	return r.queryAccessLatency(context.Background(), from, to, platformID)
}

func (r *MetricsRepo) queryAccessLatency(ctx context.Context, from, to int64, platformID string) ([]AccessLatencyBucketRow, error) {
	q := `SELECT bucket_start_unix, platform_id, buckets_json
		FROM metric_access_latency_bucket WHERE bucket_start_unix >= ? AND bucket_start_unix <= ?`
	args := []interface{}{from, to}
	if platformID != "" {
		q += " AND platform_id = ?"
		args = append(args, platformID)
	} else {
		// Empty platformID means global scope only.
		q += " AND platform_id IS NULL"
	}
	q += " ORDER BY bucket_start_unix"
	rows, err := r.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AccessLatencyBucketRow
	for rows.Next() {
		var row AccessLatencyBucketRow
		var pid sql.NullString
		if err := rows.Scan(&row.BucketStartUnix, &pid, &row.BucketsJSON); err != nil {
			return nil, fmt.Errorf("metrics repo scan access latency bucket: %w", err)
		}
		if pid.Valid {
			row.PlatformID = pid.String
		}
		var buckets []int64
		if err := json.Unmarshal([]byte(row.BucketsJSON), &buckets); err != nil {
			return nil, fmt.Errorf("metrics repo decode access latency bucket: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// LeaseLifetimeBucketRow holds a single lease lifetime histogram bucket result.
type LeaseLifetimeBucketRow struct {
	BucketStartUnix int64   `json:"bucket_start_unix"`
	PlatformID      string  `json:"platform_id"`
	SampleCount     int     `json:"sample_count"`
	P1Ms            float64 `json:"p1_ms"`
	P5Ms            float64 `json:"p5_ms"`
	P50Ms           float64 `json:"p50_ms"`
}

// QueryLeaseLifetime returns lease lifetime buckets in a time range.
func (r *MetricsRepo) QueryLeaseLifetime(from, to int64, platformID string) ([]LeaseLifetimeBucketRow, error) {
	return r.queryLeaseLifetime(context.Background(), from, to, platformID)
}

func (r *MetricsRepo) queryLeaseLifetime(ctx context.Context, from, to int64, platformID string) ([]LeaseLifetimeBucketRow, error) {
	q := `SELECT bucket_start_unix, platform_id, sample_count, p1_ms, p5_ms, p50_ms
		FROM metric_lease_lifetime_bucket WHERE bucket_start_unix >= ? AND bucket_start_unix <= ?`
	args := []interface{}{from, to}
	if platformID != "" {
		q += " AND platform_id = ?"
		args = append(args, platformID)
	}
	q += " ORDER BY bucket_start_unix"
	rows, err := r.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []LeaseLifetimeBucketRow
	for rows.Next() {
		var row LeaseLifetimeBucketRow
		if err := rows.Scan(&row.BucketStartUnix, &row.PlatformID, &row.SampleCount, &row.P1Ms, &row.P5Ms, &row.P50Ms); err != nil {
			return nil, fmt.Errorf("metrics repo scan lease lifetime bucket: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (r *MetricsRepo) query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if hook := r.afterQueryHook; hook != nil {
		hook(ctx)
	}
	return rows, nil
}
