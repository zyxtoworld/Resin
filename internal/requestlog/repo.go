package requestlog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
)

const logSummarySelectColumns = "id, ts_ns, proxy_type, client_ip, platform_id, platform_name, account, target_host, target_url, node_hash, node_tag, egress_ip, duration_ns, first_byte_duration_ns, net_ok, http_method, http_status, resin_error, upstream_stage, upstream_err_kind, upstream_errno, upstream_err_msg, ingress_bytes, egress_bytes, payload_present, req_headers_len, req_body_len, resp_headers_len, resp_body_len, req_headers_truncated, req_body_truncated, resp_headers_truncated, resp_body_truncated"

// shardGate protects the lifetime of the active database and retained shard
// files. Readers may run concurrently, but a writer gets priority so a
// rotation cannot remove a shard after a reader has snapshotted its path.
// Writer admission is context-aware; a canceled interruptible write never
// waits forever for a long-running read query.
type shardGate struct {
	mu             sync.Mutex
	cond           *sync.Cond
	readers        int
	writer         bool
	waitingWriters int
}

func newShardGate() *shardGate {
	g := &shardGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *shardGate) readLock() {
	_ = g.readLockContext(context.Background(), nil)
}

func (g *shardGate) readLockContext(ctx context.Context, onWait func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	waitHookCalled := false
	var stopCancel func() bool
	if ctx.Done() != nil {
		stopCancel = context.AfterFunc(ctx, func() {
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}
	for g.writer || g.waitingWriters != 0 {
		if !waitHookCalled {
			waitHookCalled = true
			if onWait != nil {
				onWait()
			}
		}
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			if stopCancel != nil {
				stopCancel()
			}
			return err
		}
		g.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		if stopCancel != nil {
			stopCancel()
		}
		return err
	}
	g.readers++
	g.mu.Unlock()
	if stopCancel != nil {
		stopCancel()
	}
	return nil
}

func (g *shardGate) readUnlock() {
	g.mu.Lock()
	g.readers--
	if g.readers == 0 {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

func (g *shardGate) writeLock(ctx context.Context, onWait func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	g.waitingWriters++
	waitHookCalled := false
	var stopCancel func() bool
	if ctx.Done() != nil {
		stopCancel = context.AfterFunc(ctx, func() {
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}
	for g.writer || g.readers != 0 {
		if !waitHookCalled {
			waitHookCalled = true
			if onWait != nil {
				onWait()
			}
		}
		if err := ctx.Err(); err != nil {
			g.waitingWriters--
			g.cond.Broadcast()
			g.mu.Unlock()
			if stopCancel != nil {
				stopCancel()
			}
			return err
		}
		g.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		g.waitingWriters--
		g.cond.Broadcast()
		g.mu.Unlock()
		if stopCancel != nil {
			stopCancel()
		}
		return err
	}
	g.waitingWriters--
	g.writer = true
	g.mu.Unlock()
	if stopCancel != nil {
		stopCancel()
	}
	return nil
}

func (g *shardGate) writeUnlock() {
	g.mu.Lock()
	g.writer = false
	g.cond.Broadcast()
	g.mu.Unlock()
}

// Repo manages rolling SQLite databases for request logs.
// Each DB is named request_logs-<unix_ms>.db and lives in logDir.
type Repo struct {
	logDir      string
	maxBytes    int64
	retainCount int

	// Active DB handle and path.
	activeDB   *sql.DB
	activePath string
	shardGate  *shardGate

	// readBarrier runs before read queries to improve freshness.
	readBarrierMu sync.RWMutex
	readBarrier   func(context.Context) error

	// Package-private test seam for invalidating an interruptible connection
	// immediately before its busy-timeout restoration.
	beforeContextConnResetHook func(*sql.Conn)
	// Package-private test seam for coordinating the real transaction begin.
	beforeContextTxBeginHook func()
	// Package-private test seam for canceling after a replacement DB is ready,
	// but before it can become the active owner.
	beforeContextRotationCommitHook func(*sql.DB, string)
	// Package-private test seam after a read has snapshotted retained shard
	// paths and before it opens the first shard.
	beforeReadShardOpenHook func([]string)
	// Package-private test seam called when a writer must wait for an active
	// read lease.
	beforeWriteShardWaitHook func()
	// Package-private test seam for coordinating either transaction begin.
	beforeTxBeginHook func()
	// Package-private test seam for observing a query blocked behind a writer.
	beforeReadShardWaitHook func()
}

// NewRepo creates a Repo that manages rolling request log databases.
// maxBytes controls when the active DB is rotated; retainCount sets
// how many database shard files are kept in total.
func NewRepo(logDir string, maxBytes int64, retainCount int) *Repo {
	if maxBytes <= 0 {
		maxBytes = 512 * 1024 * 1024 // 512 MB default
	}
	if retainCount <= 0 {
		retainCount = 2
	}
	return &Repo{
		logDir:      logDir,
		maxBytes:    maxBytes,
		retainCount: retainCount,
		shardGate:   newShardGate(),
	}
}

// Open opens (or creates) the active request log database.
// If a previous DB exists in the directory it is reused as active;
// a new one is created only when no existing DB is found.
func (r *Repo) Open() error {
	if err := r.shardGate.writeLock(context.Background(), nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()

	if err := os.MkdirAll(r.logDir, 0o755); err != nil {
		return fmt.Errorf("requestlog repo mkdir %s: %w", r.logDir, err)
	}

	files, err := r.listDBFiles()
	if err != nil {
		return fmt.Errorf("requestlog repo open: %w", err)
	}

	if len(files) > 0 {
		// Re-use latest as active.
		latest := files[len(files)-1]
		if err := r.openDB(latest); err != nil {
			return err
		}
		// DESIGN.md §576: prune old files on startup.
		if err := r.cleanupLocked(); err != nil {
			return err
		}
		r.migrateRetainedDBs()
		return nil
	}
	return r.rotateDBLocked()
}

// Close closes the active DB.
func (r *Repo) Close() error {
	return r.CloseContext(context.Background())
}

// CloseContext closes the active DB after all in-flight shard readers leave.
// A canceled context leaves the active owner untouched so callers retain a
// valid handle rather than closing it underneath a read query.
func (r *Repo) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.shardGate.writeLock(ctx, r.beforeWriteShardWaitHook); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()

	if r.activeDB != nil {
		err := r.activeDB.Close()
		r.activeDB = nil
		r.activePath = ""
		return err
	}
	return nil
}

// InsertBatch inserts a batch of log entries + optional payloads in a single
// transaction. Returns the number of rows successfully inserted.
func (r *Repo) InsertBatch(entries []proxy.RequestLogEntry) (int, error) {
	return r.insertBatch(context.Background(), entries, false)
}

// InsertBatchContext inserts a batch using an interruptible SQLite connection.
// A canceled or busy transaction returns without a background writer.
func (r *Repo) InsertBatchContext(ctx context.Context, entries []proxy.RequestLogEntry) (int, error) {
	return r.insertBatch(ctx, entries, true)
}

func (r *Repo) insertBatch(ctx context.Context, entries []proxy.RequestLogEntry, interruptible bool) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if interruptible {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	writeCtx := context.Background()
	if interruptible {
		writeCtx = ctx
	}
	if err := r.shardGate.writeLock(writeCtx, r.beforeWriteShardWaitHook); err != nil {
		return 0, err
	}
	defer r.shardGate.writeUnlock()

	if r.activeDB == nil {
		var recoverErr error
		if interruptible {
			recoverErr = r.recoverActiveDBContextLocked(ctx)
		} else {
			recoverErr = r.recoverActiveDBLocked()
		}
		if recoverErr != nil {
			return 0, recoverErr
		}
		if interruptible {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
	}

	// Check if rotation is needed before insert.
	var rotateErr error
	if interruptible {
		rotateErr = r.maybeRotateContextLocked(ctx)
	} else {
		rotateErr = r.maybeRotateLocked()
	}
	if rotateErr != nil {
		return 0, fmt.Errorf("requestlog repo rotate: %w", rotateErr)
	}
	if interruptible {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}

	var (
		tx          *sql.Tx
		conn        *sql.Conn
		err         error
		discardConn bool
	)
	if interruptible {
		conn, err = r.activeDB.Conn(ctx)
		if err != nil {
			return 0, fmt.Errorf("requestlog repo acquire connection: %w", err)
		}
		busyTimeoutMs, timeoutErr := state.SQLiteBusyTimeoutMs(ctx)
		if timeoutErr != nil {
			_ = conn.Close()
			return 0, fmt.Errorf("requestlog repo compute busy timeout: %w", timeoutErr)
		}
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMs)); err != nil {
			// The driver may apply the pragma before reporting an error. Mark
			// the connection bad before Close so database/sql cannot return a
			// partially configured connection to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			_ = conn.Close()
			return 0, fmt.Errorf("requestlog repo set busy timeout: %w", err)
		}
		defer func() {
			if hook := r.beforeContextConnResetHook; hook != nil {
				hook(conn)
			}
			if discardConn || ctx.Err() != nil {
				// A canceled transaction may still be blocked by the same
				// SQLite writer lock. A failed rollback or commit likewise
				// leaves its transaction state unknown. Do not return either
				// connection to the idle pool.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			} else if !resetSQLiteBusyTimeout(conn) {
				// Returning driver.ErrBadConn from Raw makes database/sql discard
				// the underlying connection instead of returning it to the idle pool.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
			_ = conn.Close()
		}()
		if hook := r.beforeContextTxBeginHook; hook != nil {
			hook()
		}
		if hook := r.beforeTxBeginHook; hook != nil {
			hook()
		}
		tx, err = conn.BeginTx(ctx, nil)
	} else {
		if hook := r.beforeTxBeginHook; hook != nil {
			hook()
		}
		tx, err = r.activeDB.Begin()
	}
	if err != nil {
		return 0, fmt.Errorf("requestlog repo begin tx: %w", err)
	}
	rollback := true
	committed := false
	defer func() {
		if rollback && !committed {
			if err := tx.Rollback(); err != nil {
				discardConn = true
			}
		}
	}()

	insertLog, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO request_logs (
		id, ts_ns, proxy_type, client_ip,
		platform_id, platform_name, account,
		target_host, target_url, node_hash, node_tag, egress_ip,
		duration_ns, first_byte_duration_ns, net_ok, http_method, http_status,
		resin_error, upstream_stage, upstream_err_kind, upstream_errno, upstream_err_msg,
		ingress_bytes, egress_bytes,
		payload_present,
		req_headers_len, req_body_len, resp_headers_len, resp_body_len,
		req_headers_truncated, req_body_truncated, resp_headers_truncated, resp_body_truncated
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, fmt.Errorf("requestlog repo prepare log: %w", err)
	}
	defer func() {
		if ctx.Err() == nil {
			_ = insertLog.Close()
		}
	}()

	insertPayload, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO request_log_payloads (
		log_id, req_headers, req_body, resp_headers, resp_body
	) VALUES (?,?,?,?,?)`)
	if err != nil {
		return 0, fmt.Errorf("requestlog repo prepare payload: %w", err)
	}
	defer func() {
		if ctx.Err() == nil {
			_ = insertPayload.Close()
		}
	}()

	inserted := 0
	for i := range entries {
		e := &entries[i]
		id := e.ID
		if id == "" {
			id = uuid.NewString()
		}
		netOK := 0
		if e.NetOK {
			netOK = 1
		}
		hasPayload := 0
		if e.ReqHeaders != nil || e.ReqBody != nil || e.RespHeaders != nil || e.RespBody != nil {
			hasPayload = 1
		}

		_, err := insertLog.ExecContext(ctx,
			id, e.StartedAtNs, int(e.ProxyType), e.ClientIP,
			e.PlatformID, e.PlatformName, e.Account,
			e.TargetHost, e.TargetURL, e.NodeHash, e.NodeTag, e.EgressIP,
			e.DurationNs, e.FirstByteDurationNs, netOK, e.HTTPMethod, e.HTTPStatus,
			e.ResinError, e.UpstreamStage, e.UpstreamErrKind, e.UpstreamErrno, e.UpstreamErrMsg,
			e.IngressBytes, e.EgressBytes,
			hasPayload,
			e.ReqHeadersLen, e.ReqBodyLen, e.RespHeadersLen, e.RespBodyLen,
			boolToInt(e.ReqHeadersTruncated), boolToInt(e.ReqBodyTruncated),
			boolToInt(e.RespHeadersTruncated), boolToInt(e.RespBodyTruncated),
		)
		if err != nil {
			if interruptible {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					rollback = false
				}
				return 0, fmt.Errorf("requestlog repo insert log id=%q: %w", id, err)
			}
			log.Printf("[requestlog] warning: skip log row id=%q insert failed: %v", id, err)
			continue // skip individual row errors
		}

		if hasPayload == 1 {
			if _, err := insertPayload.ExecContext(ctx, id, e.ReqHeaders, e.ReqBody, e.RespHeaders, e.RespBody); err != nil {
				if interruptible {
					if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						rollback = false
					}
					return 0, fmt.Errorf("requestlog repo insert payload id=%q: %w", id, err)
				}
				log.Printf("[requestlog] warning: payload insert failed for id=%q: %v", id, err)
			}
		}
		inserted++
	}

	if err := tx.Commit(); err != nil {
		discardConn = true
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rollback = false
		}
		return 0, fmt.Errorf("requestlog repo commit: %w", err)
	}
	committed = true
	return inserted, nil
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

// recoverActiveDB attempts to recover from a missing active DB handle.
// This can happen if a previous rotation closed the old DB but failed
// to open a new one. We keep the documented rotation semantics (close then create)
// and only recover on subsequent writes.
func (r *Repo) recoverActiveDBLocked() error {
	if r.activeDB != nil {
		return nil
	}
	if r.activePath == "" {
		return fmt.Errorf("requestlog repo: no active db")
	}
	if err := r.rotateDBLocked(); err != nil {
		return fmt.Errorf("requestlog repo recover active db: %w", err)
	}
	return nil
}

func (r *Repo) recoverActiveDBContextLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.activeDB != nil {
		return nil
	}
	if r.activePath == "" {
		return fmt.Errorf("requestlog repo: no active db")
	}
	if err := r.rotateDBContextLocked(ctx); err != nil {
		return fmt.Errorf("requestlog repo recover active db: %w", err)
	}
	return nil
}

// LogSummary is the result of listing logs (without payload blobs).
type LogSummary struct {
	ID                  string `json:"id"`
	TsNs                int64  `json:"ts_ns"`
	ProxyType           int    `json:"proxy_type"`
	ClientIP            string `json:"client_ip"`
	PlatformID          string `json:"platform_id"`
	PlatformName        string `json:"platform_name"`
	Account             string `json:"account"`
	TargetHost          string `json:"target_host"`
	TargetURL           string `json:"target_url"`
	NodeHash            string `json:"node_hash"`
	NodeTag             string `json:"node_tag"`
	EgressIP            string `json:"egress_ip"`
	DurationNs          int64  `json:"duration_ns"`
	FirstByteDurationNs int64  `json:"first_byte_duration_ns"`
	NetOK               bool   `json:"net_ok"`
	HTTPMethod          string `json:"http_method"`
	HTTPStatus          int    `json:"http_status"`
	ResinError          string `json:"resin_error"`
	UpstreamStage       string `json:"upstream_stage"`
	UpstreamErrKind     string `json:"upstream_err_kind"`
	UpstreamErrno       string `json:"upstream_errno"`
	UpstreamErrMsg      string `json:"upstream_err_msg"`
	IngressBytes        int64  `json:"ingress_bytes"`
	EgressBytes         int64  `json:"egress_bytes"`

	PayloadPresent       bool `json:"payload_present"`
	ReqHeadersLen        int  `json:"req_headers_len"`
	ReqBodyLen           int  `json:"req_body_len"`
	RespHeadersLen       int  `json:"resp_headers_len"`
	RespBodyLen          int  `json:"resp_body_len"`
	ReqHeadersTruncated  bool `json:"req_headers_truncated"`
	ReqBodyTruncated     bool `json:"req_body_truncated"`
	RespHeadersTruncated bool `json:"resp_headers_truncated"`
	RespBodyTruncated    bool `json:"resp_body_truncated"`
}

// PayloadRow holds the payload data for a single log entry.
type PayloadRow struct {
	LogID       string `json:"log_id"`
	ReqHeaders  []byte `json:"req_headers"`
	ReqBody     []byte `json:"req_body"`
	RespHeaders []byte `json:"resp_headers"`
	RespBody    []byte `json:"resp_body"`
}

// ListFilter specifies query filters for listing logs.
type ListFilter struct {
	ProxyType    *int
	PlatformID   string
	PlatformName string
	Account      string
	TargetHost   string
	Fuzzy        bool // Enables case-insensitive substring matching on platform_id/platform_name/account/target_host.
	EgressIP     string
	NetOK        *bool // true/false filter
	HTTPStatus   *int  // exact match
	Before       int64 // ts_ns < Before (0 means no upper bound)
	After        int64 // ts_ns > After (0 means no lower bound)
	Limit        int
	Cursor       *ListCursor
}

// ListCursor encodes a request-log pagination position.
// Ordering is ts_ns DESC then id ASC.
type ListCursor struct {
	TsNs int64
	ID   string
}

// List queries all retained DBs and returns a page of matching log summaries
// ordered by ts_ns DESC, same ts_ns by id ASC.
func (r *Repo) List(f ListFilter) ([]LogSummary, bool, *ListCursor, error) {
	return r.ListContext(context.Background(), f)
}

// ListContext queries retained request-log shards with cancellation-aware
// read admission and SQLite queries. The context bounds both the read barrier
// and waiting behind a rotation/close writer.
func (r *Repo) ListContext(ctx context.Context, f ListFilter) ([]LogSummary, bool, *ListCursor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.runReadBarrier(ctx); err != nil {
		return nil, false, nil, err
	}
	if err := r.shardGate.readLockContext(ctx, r.beforeReadShardWaitHook); err != nil {
		return nil, false, nil, err
	}
	defer r.shardGate.readUnlock()
	if err := ctx.Err(); err != nil {
		return nil, false, nil, err
	}

	files, err := r.listDBFiles()
	if err != nil {
		return nil, false, nil, err
	}
	if hook := r.beforeReadShardOpenHook; hook != nil {
		hook(files)
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	// Fetch one extra row across retained DBs to derive has_more.
	fetchLimit := limit + 1
	var results []LogSummary
	// Iterate every retained DB, then globally merge-sort.
	// We must not early-stop by file order because request ts_ns can be out-of-order
	// relative to DB filename time (e.g. long-lived requests flushed later).
	for i := len(files) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, false, nil, err
		}
		db, err := r.openReadOnly(files[i])
		if err != nil {
			log.Printf("[requestlog] warning: list open db failed path=%q: %v", files[i], err)
			continue
		}
		rows, err := r.queryLogsContext(ctx, db, f, fetchLimit)
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("[requestlog] warning: list close db failed path=%q: %v", files[i], closeErr)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, false, nil, ctx.Err()
			}
			log.Printf("[requestlog] warning: list query failed path=%q: %v", files[i], err)
			continue
		}
		results = append(results, rows...)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, nil, err
	}

	// Global merge sort: DESIGN.md §577 requires ts_ns DESC, same ts_ns by id ASC.
	sort.Slice(results, func(i, j int) bool {
		if results[i].TsNs != results[j].TsNs {
			return results[i].TsNs > results[j].TsNs
		}
		return results[i].ID < results[j].ID
	})
	if len(results) == 0 {
		return []LogSummary{}, false, nil, nil
	}

	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}

	var nextCursor *ListCursor
	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		nextCursor = &ListCursor{TsNs: last.TsNs, ID: last.ID}
	}
	return results, hasMore, nextCursor, nil
}

// GetByID looks up a single log entry across all retained DBs.
func (r *Repo) GetByID(id string) (*LogSummary, error) {
	return r.GetByIDContext(context.Background(), id)
}

// GetByIDContext looks up one request-log row with a cancellation-aware read
// lease and SQLite query.
func (r *Repo) GetByIDContext(ctx context.Context, id string) (*LogSummary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.runReadBarrier(ctx); err != nil {
		return nil, err
	}
	if err := r.shardGate.readLockContext(ctx, r.beforeReadShardWaitHook); err != nil {
		return nil, err
	}
	defer r.shardGate.readUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	files, err := r.listDBFiles()
	if err != nil {
		return nil, err
	}

	var result *LogSummary
	err = r.queryAcrossRetainedDBsContext(ctx, files, "get_by_id", "id", id, func(db *sql.DB) (bool, error) {
		row, err := r.queryLogByIDContext(ctx, db, id)
		if err != nil {
			return false, err
		}
		if row != nil {
			result = row
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetPayloads retrieves payload data for a given log ID across all retained DBs.
func (r *Repo) GetPayloads(logID string) (*PayloadRow, error) {
	return r.GetPayloadsContext(context.Background(), logID)
}

// GetPayloadsContext retrieves payloads with a cancellation-aware read lease
// and SQLite query.
func (r *Repo) GetPayloadsContext(ctx context.Context, logID string) (*PayloadRow, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.runReadBarrier(ctx); err != nil {
		return nil, err
	}
	if err := r.shardGate.readLockContext(ctx, r.beforeReadShardWaitHook); err != nil {
		return nil, err
	}
	defer r.shardGate.readUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	files, err := r.listDBFiles()
	if err != nil {
		return nil, err
	}

	var result *PayloadRow
	err = r.queryAcrossRetainedDBsContext(ctx, files, "get_payloads", "log_id", logID, func(db *sql.DB) (bool, error) {
		row, err := r.queryPayloadContext(ctx, db, logID)
		if err != nil {
			return false, err
		}
		if row != nil {
			result = row
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repo) queryAcrossRetainedDBsContext(
	ctx context.Context,
	files []string,
	op string,
	keyName string,
	keyValue string,
	query func(*sql.DB) (bool, error),
) error {
	for i := len(files) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := files[i]
		db, err := r.openReadOnly(path)
		if err != nil {
			log.Printf("[requestlog] warning: %s open db failed path=%q %s=%q: %v", op, path, keyName, keyValue, err)
			continue
		}
		row, err := query(db)
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("[requestlog] warning: %s close db failed path=%q %s=%q: %v", op, path, keyName, keyValue, closeErr)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.Printf("[requestlog] warning: %s query failed path=%q %s=%q: %v", op, path, keyName, keyValue, err)
		}
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && row {
			return nil
		}
	}
	return ctx.Err()
}

func (r *Repo) setReadBarrier(fn func(context.Context) error) {
	r.readBarrierMu.Lock()
	r.readBarrier = fn
	r.readBarrierMu.Unlock()
}

func (r *Repo) runReadBarrier(ctx context.Context) error {
	r.readBarrierMu.RLock()
	barrier := r.readBarrier
	r.readBarrierMu.RUnlock()
	if barrier != nil {
		return barrier(ctx)
	}
	return nil
}

// --- internal helpers ---

func (r *Repo) prepareDB(path string) (*sql.DB, error) {
	db, err := state.OpenDB(path)
	if err != nil {
		return nil, err
	}
	if err := state.InitDB(db, CreateDDL); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureRequestLogSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (r *Repo) openDB(path string) error {
	db, err := r.prepareDB(path)
	if err != nil {
		return err
	}
	r.activeDB = db
	r.activePath = path
	return nil
}

func (r *Repo) migrateRetainedDBs() {
	files, err := r.listDBFiles()
	if err != nil {
		log.Printf("[requestlog] warning: list db files for migration failed: %v", err)
		return
	}
	for _, path := range files {
		if path == r.activePath {
			continue
		}
		db, err := state.OpenDB(path)
		if err != nil {
			log.Printf("[requestlog] warning: open db for migration failed path=%q: %v", path, err)
			continue
		}
		if err := ensureRequestLogSchema(db); err != nil {
			log.Printf("[requestlog] warning: migrate db failed path=%q: %v", path, err)
		}
		if err := db.Close(); err != nil {
			log.Printf("[requestlog] warning: close migrated db failed path=%q: %v", path, err)
		}
	}
}

func (r *Repo) rotateDB() error {
	if err := r.shardGate.writeLock(context.Background(), nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()
	return r.rotateDBLocked()
}

func (r *Repo) rotateDBLocked() error {
	oldDB := r.activeDB
	oldPath := r.activePath
	path := r.nextDBPath()
	db, err := r.prepareDB(path)
	if err != nil {
		// prepareDB leaves the existing fields untouched on failure. Restore them
		// explicitly so this helper remains safe if that implementation changes.
		r.activeDB = oldDB
		r.activePath = oldPath
		return fmt.Errorf("requestlog rotate: %w", err)
	}
	r.activeDB = db
	r.activePath = path
	if oldDB != nil && oldDB != db {
		_ = oldDB.Close()
	}
	return r.cleanupLocked()
}

func (r *Repo) rotateDBContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.shardGate.writeLock(ctx, nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()
	return r.rotateDBContextLocked(ctx)
}

func (r *Repo) rotateDBContextLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	oldDB := r.activeDB
	oldPath := r.activePath
	path := r.nextDBPath()
	_, statErr := os.Stat(path)
	pathExisted := statErr == nil

	db, err := r.prepareDB(path)
	if err != nil {
		if !pathExisted {
			removeDBArtifacts(path)
		}
		r.activeDB = oldDB
		r.activePath = oldPath
		return fmt.Errorf("requestlog rotate: %w", err)
	}
	discardPrepared := func() {
		_ = db.Close()
		if !pathExisted {
			removeDBArtifacts(path)
		}
	}

	if hook := r.beforeContextRotationCommitHook; hook != nil {
		hook(db, path)
	}
	if err := ctx.Err(); err != nil {
		discardPrepared()
		r.activeDB = oldDB
		r.activePath = oldPath
		return err
	}

	r.activeDB = db
	r.activePath = path
	if oldDB != nil && oldDB != db {
		_ = oldDB.Close()
	}
	if err := ctx.Err(); err != nil {
		// The replacement was already committed before cancellation. Keep the
		// valid new owner, but do not perform optional file cleanup after the
		// deadline.
		return err
	}
	return r.cleanupContextLocked(ctx)
}

func (r *Repo) nextDBPath() string {
	timestamp := time.Now().UnixMilli()
	for {
		path := filepath.Join(r.logDir, fmt.Sprintf("request_logs-%d.db", timestamp))
		if path != r.activePath {
			_, err := os.Stat(path)
			if errors.Is(err, os.ErrNotExist) {
				return path
			}
			if err != nil {
				return path
			}
		}
		timestamp++
	}
}

func (r *Repo) maybeRotate() error {
	if err := r.shardGate.writeLock(context.Background(), nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()
	return r.maybeRotateLocked()
}

func (r *Repo) maybeRotateLocked() error {
	if r.activePath == "" {
		return r.rotateDBLocked()
	}
	totalSize, err := sqliteFilesSize(r.activePath)
	if err != nil {
		log.Printf("[requestlog] warning: stat active db failed path=%q: %v", r.activePath, err)
		return nil // can't stat; skip rotation check
	}
	if totalSize >= r.maxBytes {
		return r.rotateDBLocked()
	}
	return nil
}

func (r *Repo) maybeRotateContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.shardGate.writeLock(ctx, nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()
	return r.maybeRotateContextLocked(ctx)
}

func (r *Repo) maybeRotateContextLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.activePath == "" {
		return r.rotateDBContextLocked(ctx)
	}
	totalSize, err := sqliteFilesSize(r.activePath)
	if err != nil {
		log.Printf("[requestlog] warning: stat active db failed path=%q: %v", r.activePath, err)
		return nil // can't stat; skip rotation check
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if totalSize >= r.maxBytes {
		return r.rotateDBContextLocked(ctx)
	}
	return nil
}

func (r *Repo) cleanup() error {
	if err := r.shardGate.writeLock(context.Background(), nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()
	return r.cleanupLocked()
}

func (r *Repo) cleanupLocked() error {
	files, err := r.listDBFiles()
	if err != nil {
		return err
	}
	// Keep retainCount most recent files (the active one is always latest).
	if len(files) <= r.retainCount {
		return nil
	}
	toRemove := files[:len(files)-r.retainCount]
	for _, f := range toRemove {
		os.Remove(f)
		// Also clean up WAL/SHM files.
		os.Remove(f + "-wal")
		os.Remove(f + "-shm")
	}
	return nil
}

func (r *Repo) cleanupContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.shardGate.writeLock(ctx, nil); err != nil {
		return err
	}
	defer r.shardGate.writeUnlock()
	return r.cleanupContextLocked(ctx)
}

func (r *Repo) cleanupContextLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := r.listDBFiles()
	if err != nil {
		return err
	}
	if len(files) <= r.retainCount {
		return nil
	}
	toRemove := files[:len(files)-r.retainCount]
	for _, f := range toRemove {
		if err := ctx.Err(); err != nil {
			return err
		}
		os.Remove(f)
		os.Remove(f + "-wal")
		os.Remove(f + "-shm")
	}
	return nil
}

func removeDBArtifacts(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func (r *Repo) listDBFiles() ([]string, error) {
	entries, err := os.ReadDir(r.logDir)
	if err != nil {
		return nil, fmt.Errorf("requestlog list dir %s: %w", r.logDir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "request_logs-") && strings.HasSuffix(name, ".db") {
			files = append(files, filepath.Join(r.logDir, name))
		}
	}
	sort.Strings(files) // lexicographic sort == chronological for our naming
	return files, nil
}

func (r *Repo) openReadOnly(path string) (*sql.DB, error) {
	dsn := path + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func (r *Repo) queryLogs(db *sql.DB, f ListFilter, limit int) ([]LogSummary, error) {
	return r.queryLogsContext(context.Background(), db, f, limit)
}

func (r *Repo) queryLogsContext(ctx context.Context, db *sql.DB, f ListFilter, limit int) ([]LogSummary, error) {
	var where []string
	var args []interface{}

	if f.ProxyType != nil {
		where = append(where, "proxy_type = ?")
		args = append(args, *f.ProxyType)
	}
	if f.PlatformID != "" {
		if f.Fuzzy {
			where = append(where, "instr(lower(platform_id), ?) > 0")
			args = append(args, strings.ToLower(f.PlatformID))
		} else {
			where = append(where, "platform_id = ?")
			args = append(args, f.PlatformID)
		}
	}
	if f.PlatformName != "" {
		if f.Fuzzy {
			where = append(where, "instr(lower(platform_name), ?) > 0")
			args = append(args, strings.ToLower(f.PlatformName))
		} else {
			where = append(where, "platform_name = ?")
			args = append(args, f.PlatformName)
		}
	}
	if f.Account != "" {
		if f.Fuzzy {
			where = append(where, "instr(lower(account), ?) > 0")
			args = append(args, strings.ToLower(f.Account))
		} else {
			where = append(where, "account = ? AND account <> ''")
			args = append(args, f.Account)
		}
	}
	if f.TargetHost != "" {
		if f.Fuzzy {
			where = append(where, "instr(lower(target_host), ?) > 0")
			args = append(args, strings.ToLower(f.TargetHost))
		} else {
			where = append(where, "target_host = ?")
			args = append(args, f.TargetHost)
		}
	}
	if f.EgressIP != "" {
		where = append(where, "egress_ip = ?")
		args = append(args, f.EgressIP)
	}
	if f.NetOK != nil {
		where = append(where, "net_ok = ?")
		args = append(args, boolToInt(*f.NetOK))
	}
	if f.HTTPStatus != nil {
		where = append(where, "http_status = ?")
		args = append(args, *f.HTTPStatus)
	}
	if f.Before > 0 {
		where = append(where, "ts_ns < ?")
		args = append(args, f.Before)
	}
	if f.After > 0 {
		where = append(where, "ts_ns > ?")
		args = append(args, f.After)
	}
	if f.Cursor != nil {
		// Pagination condition for ORDER BY ts_ns DESC, id ASC:
		// next rows are strictly "after" the cursor in that ordering.
		where = append(where, "(ts_ns < ? OR (ts_ns = ? AND id > ?))")
		args = append(args, f.Cursor.TsNs, f.Cursor.TsNs, f.Cursor.ID)
	}

	q := "SELECT " + logSummarySelectColumns + " FROM request_logs"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY ts_ns DESC, id ASC LIMIT ?"
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanLogSummaries(rows)
}

func (r *Repo) queryLogByID(db *sql.DB, id string) (*LogSummary, error) {
	return r.queryLogByIDContext(context.Background(), db, id)
}

func (r *Repo) queryLogByIDContext(ctx context.Context, db *sql.DB, id string) (*LogSummary, error) {
	row := db.QueryRowContext(ctx, "SELECT "+logSummarySelectColumns+" FROM request_logs WHERE id = ?", id)
	s, err := scanLogSummary(row)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (r *Repo) queryPayload(db *sql.DB, logID string) (*PayloadRow, error) {
	return r.queryPayloadContext(context.Background(), db, logID)
}

func (r *Repo) queryPayloadContext(ctx context.Context, db *sql.DB, logID string) (*PayloadRow, error) {
	row := db.QueryRowContext(ctx, "SELECT log_id, req_headers, req_body, resp_headers, resp_body FROM request_log_payloads WHERE log_id = ?", logID)
	var p PayloadRow
	err := row.Scan(&p.LogID, &p.ReqHeaders, &p.ReqBody, &p.RespHeaders, &p.RespBody)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func scanLogSummaries(rows *sql.Rows) ([]LogSummary, error) {
	var results []LogSummary
	for rows.Next() {
		s, err := scanLogSummary(rows)
		if err != nil {
			log.Printf("[requestlog] warning: skip malformed log row during scan: %v", err)
			continue
		}
		results = append(results, s)
	}
	return results, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanLogSummary(s rowScanner) (LogSummary, error) {
	var row LogSummary
	var netOK, payloadPresent, rht, rbt, rsht, rsbt int
	err := s.Scan(
		&row.ID, &row.TsNs, &row.ProxyType, &row.ClientIP,
		&row.PlatformID, &row.PlatformName, &row.Account,
		&row.TargetHost, &row.TargetURL, &row.NodeHash, &row.NodeTag, &row.EgressIP,
		&row.DurationNs, &row.FirstByteDurationNs, &netOK, &row.HTTPMethod, &row.HTTPStatus,
		&row.ResinError, &row.UpstreamStage, &row.UpstreamErrKind, &row.UpstreamErrno, &row.UpstreamErrMsg,
		&row.IngressBytes, &row.EgressBytes,
		&payloadPresent,
		&row.ReqHeadersLen, &row.ReqBodyLen, &row.RespHeadersLen, &row.RespBodyLen,
		&rht, &rbt, &rsht, &rsbt,
	)
	if err != nil {
		return LogSummary{}, err
	}
	row.NetOK = netOK != 0
	row.PayloadPresent = payloadPresent != 0
	row.ReqHeadersTruncated = rht != 0
	row.ReqBodyTruncated = rbt != 0
	row.RespHeadersTruncated = rsht != 0
	row.RespBodyTruncated = rsbt != 0
	return row, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// sqliteFilesSize returns the total size of a SQLite database set:
// base db file + optional -wal and -shm sidecar files.
func sqliteFilesSize(basePath string) (int64, error) {
	paths := []string{basePath, basePath + "-wal", basePath + "-shm"}
	var total int64
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}
