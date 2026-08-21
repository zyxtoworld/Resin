package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/platform"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// StateRepo wraps state.db and provides transactional CRUD for strong-persist data.
// All writes are serialized by an internal mutex.
type StateRepo struct {
	db *sql.DB
	mu stateWriteMutex

	writeMu      sync.Mutex
	writeDone    chan struct{}
	writeCtx     context.Context
	writeCancel  context.CancelFunc
	writeClosed  bool
	activeWrites int
	// beforeWriteHook is package-private coordination for lifecycle tests.
	// Production leaves it nil.
	beforeWriteHook func()
	// beforeWriteMutexHook is package-private coordination immediately before
	// the serialized repository write owner is acquired. Production leaves it
	// nil.
	beforeWriteMutexHook func()
	// beforeContextReadPragmaHook is a package-private test seam immediately
	// before a request-bound read connection receives its short busy timeout.
	// Production leaves it nil.
	beforeContextReadPragmaHook func(*sql.Conn) error
	// beforeContextReadResetHook is a package-private test seam immediately
	// before a request-bound read connection is reset and returned.
	// Production leaves it nil.
	beforeContextReadResetHook func(*sql.Conn)
	// beforeContextReadQueryHook is a package-private test seam immediately
	// before a request-bound SQL read starts. Production leaves it nil.
	beforeContextReadQueryHook func(*sql.Conn)
	// afterWriteBeginHook is package-private coordination after a write has
	// acquired SQLite's IMMEDIATE transaction. Production leaves it nil.
	afterWriteBeginHook func()
}

// newStateRepo creates a StateRepo for the given state.db connection.
func newStateRepo(db *sql.DB) *StateRepo {
	writeCtx, writeCancel := context.WithCancel(context.Background())
	return &StateRepo{db: db, writeCtx: writeCtx, writeCancel: writeCancel}
}

var ErrStateWriteAdmissionClosed = errors.New("state write admission closed")

type stateWriteExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// withWrite admits one strong-persistence mutation. Shutdown closes this
// admission before closing state.db so a handler that outlives HTTP draining
// cannot start a new state mutation.
func (r *StateRepo) withWrite(fn func(context.Context) error) error {
	return r.withWriteContext(context.Background(), fn)
}

// withWriteContext admits one strong-persistence mutation and combines the
// caller context with the state-write shutdown context. The caller can
// therefore cancel an HTTP mutation without weakening shutdown cancellation
// for ordinary background callers.
func (r *StateRepo) withWriteContext(parent context.Context, fn func(context.Context) error) error {
	return r.withWriteContextAndCommit(parent, func(writeCtx, _ context.Context) error {
		return fn(writeCtx)
	})
}

// withWriteContextAndCommit admits one strong-persistence mutation and gives
// the callback two contexts: writeCtx is request-cancelable admission context;
// commitCtx is canceled only when the state-write admission is shut down.
// Callers must use commitCtx after their mutation has crossed its own
// serialization/validation boundary, so a disconnected HTTP client cannot
// split a committed database row from its in-memory publish.
func (r *StateRepo) withWriteContextAndCommit(parent context.Context, fn func(context.Context, context.Context) error) error {
	if r == nil {
		return ErrStateWriteAdmissionClosed
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return err
	}
	interruptible := parent.Done() != nil

	r.writeMu.Lock()
	if r.writeCtx == nil {
		r.writeCtx, r.writeCancel = context.WithCancel(context.Background())
	}
	if r.writeClosed {
		r.writeMu.Unlock()
		return ErrStateWriteAdmissionClosed
	}
	r.activeWrites++
	if r.activeWrites == 1 {
		r.writeDone = make(chan struct{})
	}
	ctx := r.writeCtx
	r.writeMu.Unlock()
	commitCtx := context.WithValue(ctx, contextSQLiteWriteKey{}, true)
	mergedCtx, cancel := context.WithCancel(parent)
	if interruptible {
		mergedCtx = context.WithValue(mergedCtx, contextSQLiteWriteKey{}, true)
	}
	stopAdmissionCancel := context.AfterFunc(ctx, cancel)
	defer func() {
		stopAdmissionCancel()
		cancel()
		r.writeMu.Lock()
		r.activeWrites--
		if r.activeWrites == 0 && r.writeDone != nil {
			close(r.writeDone)
			r.writeDone = nil
		}
		r.writeMu.Unlock()
	}()
	if hook := r.beforeWriteHook; hook != nil {
		hook()
	}
	if err := mergedCtx.Err(); err != nil {
		return err
	}
	retryUntil := time.Now().Add(time.Duration(DefaultSQLiteBusyTimeoutMs) * time.Millisecond)
	for {
		err := fn(mergedCtx, commitCtx)
		if err == nil || !interruptible || mergedCtx.Err() != nil || !isSQLiteBusyError(err) || !time.Now().Before(retryUntil) {
			return err
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-mergedCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return mergedCtx.Err()
		}
	}
}

func (r *StateRepo) lockWriteContext(ctx context.Context) (stateWriteExecutor, func(), error) {
	if err := r.mu.lockContext(ctx); err != nil {
		return nil, nil, err
	}
	if !isContextSQLiteWrite(ctx) {
		return r.db, r.mu.Unlock, nil
	}

	conn, err := r.db.Conn(ctx)
	if err != nil {
		r.mu.Unlock()
		return nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", contextSQLiteBusyTimeoutMs)); err != nil {
		// The driver may apply the pragma before reporting an error. Do not
		// return the request-sized connection to database/sql's idle pool.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
		r.mu.Unlock()
		return nil, nil, err
	}
	return conn, func() {
		if !resetStateSQLiteBusyTimeout(conn) {
			// A failed reset leaves the connection state unknown. It must not
			// be reused by a later legacy write.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
		r.mu.Unlock()
	}, nil
}

// contextReadConn gives request-bound reads the same short SQLite busy
// interval as the write owner. Without this, a snapshot read performed before
// the write transaction can sleep in SQLite's default 5s busy handler after
// the HTTP request has already been canceled.
func (r *StateRepo) contextReadConn(ctx context.Context) (*sql.Conn, func(), error) {
	if !isContextSQLiteWrite(ctx) {
		return nil, func() {}, nil
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	if hook := r.beforeContextReadPragmaHook; hook != nil {
		if err := hook(conn); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			_ = conn.Close()
			return nil, nil, err
		}
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", contextSQLiteBusyTimeoutMs)); err != nil {
		// The driver may have applied the pragma before reporting a
		// cancellation/error. Never return that connection to the pool with a
		// request-sized busy timeout.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, func() {
		if hook := r.beforeContextReadResetHook; hook != nil {
			hook(conn)
		}
		if ctx.Err() != nil {
			// A canceled read may still be unwinding under the same SQLite
			// writer lock. Do not run a background PRAGMA here: discard the
			// connection instead of blocking the canceled caller.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		} else if !resetStateSQLiteBusyTimeout(conn) {
			// A failed reset must not return a connection with a request-sized
			// busy timeout to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}, nil
}

func resetStateSQLiteBusyTimeout(conn *sql.Conn) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", DefaultSQLiteBusyTimeoutMs))
	return err == nil
}

// withCommitTx separates SQLite write-owner admission from the irreversible
// transaction. parent is honored while the repository mutex and BEGIN
// IMMEDIATE wait for the SQLite writer slot. Once BEGIN IMMEDIATE succeeds, fn
// and COMMIT use the shutdown-owned commit context so a client disconnect
// cannot leave a database row committed without the matching runtime publish.
func (r *StateRepo) withCommitTx(
	parent context.Context,
	fn func(context.Context, *sql.Conn) error,
) error {
	if r == nil {
		return ErrStateWriteAdmissionClosed
	}
	return r.withWriteContextAndCommit(parent, func(writeCtx, commitCtx context.Context) error {
		if err := writeCtx.Err(); err != nil {
			return err
		}
		if err := r.mu.lockContext(writeCtx); err != nil {
			return err
		}
		defer r.mu.Unlock()

		conn, err := r.db.Conn(writeCtx)
		if err != nil {
			return err
		}
		defer func() {
			if !resetStateSQLiteBusyTimeout(conn) {
				// A failed reset may leave the driver-side timeout changed. Do
				// not return that connection to database/sql's idle pool.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
			_ = conn.Close()
		}()

		busyTimeoutMs, err := SQLiteBusyTimeoutMs(writeCtx)
		if err != nil {
			return err
		}
		if isContextSQLiteWrite(writeCtx) && busyTimeoutMs > contextSQLiteBusyTimeoutMs {
			busyTimeoutMs = contextSQLiteBusyTimeoutMs
		}
		if _, err := conn.ExecContext(writeCtx, fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMs)); err != nil {
			// The driver may apply the pragma before reporting an error. The
			// setup failed, so never return this connection to the idle pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			return err
		}
		rollback := true
		defer func() {
			if rollback {
				// BEGIN IMMEDIATE is raw SQL rather than database/sql.Tx. If
				// its context-aware call returns after acquiring the writer,
				// the connection still needs an explicit rollback before it
				// can return to the pool.
				if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
					// The transaction state is unknown. Do not let a possibly
					// open transaction return to database/sql's idle pool.
					_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				}
			}
		}()
		if _, err := conn.ExecContext(writeCtx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
		if hook := r.afterWriteBeginHook; hook != nil {
			hook()
		}

		if fn != nil {
			if err := fn(commitCtx, conn); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(commitCtx, "COMMIT"); err != nil {
			return err
		}
		rollback = false
		return nil
	})
}

// CloseStateWriteAdmissionAndWait rejects new strong writes, cancels writes
// already admitted, and waits for them until ctx expires. A later caller may
// use a different context to finish observing the same admission owner.
func (r *StateRepo) CloseStateWriteAdmissionAndWait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.writeMu.Lock()
	if r.writeCtx == nil {
		r.writeCtx, r.writeCancel = context.WithCancel(context.Background())
	}
	if !r.writeClosed {
		r.writeClosed = true
		if r.writeCancel != nil {
			r.writeCancel()
		}
	}
	done := r.writeDone
	if r.activeWrites == 0 || done == nil {
		r.writeMu.Unlock()
		return nil
	}
	r.writeMu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForWrites waits until every admitted strong mutation has returned after
// write admission has been closed. If admission is still open, this method is
// not the owner of the strong-write lifecycle and returns immediately.
func (r *StateRepo) waitForWrites() {
	if r == nil {
		return
	}

	r.writeMu.Lock()
	done := r.writeDone
	if !r.writeClosed || r.activeWrites == 0 || done == nil {
		r.writeMu.Unlock()
		return
	}
	r.writeMu.Unlock()
	<-done
}

func encodeStringSliceJSON(values []string) (string, error) {
	if values == nil {
		values = []string{}
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeStringSliceJSON(raw string) ([]string, error) {
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = []string{}
	}
	return out, nil
}

// --- system_config ---

// GetSystemConfig loads the runtime config and version from state.db.
// Returns nil config and version 0 if no row exists.
func (r *StateRepo) GetSystemConfig() (*config.RuntimeConfig, int, error) {
	return r.GetSystemConfigContext(context.Background())
}

// GetSystemConfigContext loads the runtime config with caller cancellation.
func (r *StateRepo) GetSystemConfigContext(ctx context.Context) (*config.RuntimeConfig, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	row := r.db.QueryRowContext(ctx, "SELECT config_json, version FROM system_config WHERE id = 1")
	var configJSON string
	var version int
	if err := row.Scan(&configJSON, &version); err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("scan system_config: %w", err)
	}
	cfg := &config.RuntimeConfig{}
	if err := json.Unmarshal([]byte(configJSON), cfg); err != nil {
		return nil, 0, fmt.Errorf("unmarshal system_config: %w", err)
	}
	return cfg, version, nil
}

// SaveSystemConfig persists the runtime config with the given version.
func (r *StateRepo) SaveSystemConfig(cfg *config.RuntimeConfig, version int, updatedAtNs int64) error {
	return r.SaveSystemConfigContext(context.Background(), cfg, version, updatedAtNs)
}

// SaveSystemConfigContext persists the runtime config with caller
// cancellation while retaining the state-write shutdown admission contract.
func (r *StateRepo) SaveSystemConfigContext(ctx context.Context, cfg *config.RuntimeConfig, version int, updatedAtNs int64) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal system_config: %w", err)
	}

	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		_, err = exec.ExecContext(writeCtx, systemConfigUpsertSQL,
			string(data), version, updatedAtNs)
		return err
	})
}

const systemConfigUpsertSQL = `
			INSERT INTO system_config (id, config_json, version, updated_at_ns)
			VALUES (1, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				config_json   = excluded.config_json,
				version       = excluded.version,
				updated_at_ns = excluded.updated_at_ns
`

// SaveSystemConfigContextAndCommit persists the runtime config after a
// cancelable SQLite writer admission, then executes the statement and COMMIT
// with the shutdown-owned context. Once the transaction begins, a client
// disconnect cannot make the caller observe a failed write while the row was
// already committed.
func (r *StateRepo) SaveSystemConfigContextAndCommit(ctx context.Context, cfg *config.RuntimeConfig, version int, updatedAtNs int64) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal system_config: %w", err)
	}

	return r.withCommitTx(ctx, func(commitCtx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(commitCtx, systemConfigUpsertSQL,
			string(data), version, updatedAtNs)
		return err
	})
}

// --- platforms ---

const platformInsertSQL = `
		INSERT INTO platforms (id, name, sticky_ttl_ns, regex_filters_json, region_filters_json,
		                       response_rules_json,
		                       reverse_proxy_miss_action, reverse_proxy_empty_account_behavior,
			                       reverse_proxy_fixed_account_header, allocation_policy,
			                       passive_circuit_breaker_disabled, proxy_request_total_timeout_ns,
			                       proxy_request_attempt_timeout_ns, proxy_request_max_attempts, updated_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const platformUpsertSQL = platformInsertSQL + `
		ON CONFLICT(id) DO UPDATE SET
			name                     = excluded.name,
			sticky_ttl_ns            = excluded.sticky_ttl_ns,
			regex_filters_json       = excluded.regex_filters_json,
			region_filters_json      = excluded.region_filters_json,
			response_rules_json      = excluded.response_rules_json,
			reverse_proxy_miss_action = excluded.reverse_proxy_miss_action,
			reverse_proxy_empty_account_behavior = excluded.reverse_proxy_empty_account_behavior,
			reverse_proxy_fixed_account_header   = excluded.reverse_proxy_fixed_account_header,
			allocation_policy        = excluded.allocation_policy,
			passive_circuit_breaker_disabled = excluded.passive_circuit_breaker_disabled,
			proxy_request_total_timeout_ns = excluded.proxy_request_total_timeout_ns,
			proxy_request_attempt_timeout_ns = excluded.proxy_request_attempt_timeout_ns,
			proxy_request_max_attempts = excluded.proxy_request_max_attempts,
			updated_at_ns            = excluded.updated_at_ns`

func preparePlatformPersistence(p model.Platform) (model.Platform, string, string, string, error) {
	p.Name = platform.NormalizePlatformName(p.Name)
	if err := platform.ValidatePlatformName(p.Name); err != nil {
		return model.Platform{}, "", "", "", fmt.Errorf("platform name: %w", err)
	}

	// Validate strongly-typed filters before persistence.
	if _, err := platform.CompileRegexFilters(p.RegexFilters); err != nil {
		return model.Platform{}, "", "", "", err
	}
	if err := platform.ValidateRegionFilters(p.RegionFilters); err != nil {
		return model.Platform{}, "", "", "", err
	}
	if _, err := platform.CompileResponseRules(p.ID, p.ResponseRules); err != nil {
		return model.Platform{}, "", "", "", err
	}
	if err := platform.ValidateProxyRequestTotalTimeoutNs(p.ProxyRequestTotalTimeoutNs); err != nil {
		return model.Platform{}, "", "", "", err
	}
	if err := platform.ValidateProxyRequestAttemptTimeoutNs(p.ProxyRequestAttemptTimeoutNs); err != nil {
		return model.Platform{}, "", "", "", err
	}
	if err := platform.ValidateProxyRequestMaxAttempts(p.ProxyRequestMaxAttempts); err != nil {
		return model.Platform{}, "", "", "", err
	}
	if p.ResponseRules == nil {
		p.ResponseRules = []model.PlatformResponseRule{}
	}
	missAction := platform.NormalizeReverseProxyMissAction(p.ReverseProxyMissAction)
	if missAction == "" {
		return model.Platform{}, "", "", "", fmt.Errorf("reverse_proxy_miss_action: invalid value %q", p.ReverseProxyMissAction)
	}
	p.ReverseProxyMissAction = string(missAction)
	if !platform.AllocationPolicy(p.AllocationPolicy).IsValid() {
		return model.Platform{}, "", "", "", fmt.Errorf("allocation_policy: invalid value %q", p.AllocationPolicy)
	}
	behavior := platform.ReverseProxyEmptyAccountBehavior(strings.TrimSpace(p.ReverseProxyEmptyAccountBehavior))
	if behavior == "" {
		behavior = platform.ReverseProxyEmptyAccountBehaviorRandom
	}
	if !behavior.IsValid() {
		return model.Platform{}, "", "", "", fmt.Errorf("reverse_proxy_empty_account_behavior: invalid value %q", p.ReverseProxyEmptyAccountBehavior)
	}
	p.ReverseProxyEmptyAccountBehavior = string(behavior)
	normalizedFixedHeaders, fixedHeaders, err := platform.NormalizeFixedAccountHeaders(p.ReverseProxyFixedAccountHeader)
	if err != nil {
		return model.Platform{}, "", "", "", fmt.Errorf("reverse_proxy_fixed_account_header: %w", err)
	}
	p.ReverseProxyFixedAccountHeader = normalizedFixedHeaders
	if behavior == platform.ReverseProxyEmptyAccountBehaviorFixedHeader && len(fixedHeaders) == 0 {
		return model.Platform{}, "", "", "", fmt.Errorf(
			"reverse_proxy_fixed_account_header: required when reverse_proxy_empty_account_behavior is %s",
			platform.ReverseProxyEmptyAccountBehaviorFixedHeader,
		)
	}
	regexFiltersJSON, err := encodeStringSliceJSON(p.RegexFilters)
	if err != nil {
		return model.Platform{}, "", "", "", fmt.Errorf("encode platform %s regex_filters: %w", p.ID, err)
	}
	regionFiltersJSON, err := encodeStringSliceJSON(p.RegionFilters)
	if err != nil {
		return model.Platform{}, "", "", "", fmt.Errorf("encode platform %s region_filters: %w", p.ID, err)
	}
	responseRulesJSON, err := json.Marshal(p.ResponseRules)
	if err != nil {
		return model.Platform{}, "", "", "", fmt.Errorf("encode platform %s response_rules: %w", p.ID, err)
	}
	return p, regexFiltersJSON, regionFiltersJSON, string(responseRulesJSON), nil
}

func platformPersistenceArgs(p model.Platform, regexFiltersJSON, regionFiltersJSON, responseRulesJSON string) []any {
	return []any{
		p.ID, p.Name, p.StickyTTLNs, regexFiltersJSON, regionFiltersJSON, responseRulesJSON,
		p.ReverseProxyMissAction, p.ReverseProxyEmptyAccountBehavior, p.ReverseProxyFixedAccountHeader,
		p.AllocationPolicy, p.PassiveCircuitBreakerDisabled, p.ProxyRequestTotalTimeoutNs,
		p.ProxyRequestAttemptTimeoutNs, p.ProxyRequestMaxAttempts, p.UpdatedAtNs,
	}
}

// UpsertPlatform inserts or updates a platform by ID.
// If the name collides with a different platform's name, ErrConflict is returned.
func (r *StateRepo) UpsertPlatform(p model.Platform) error {
	return r.UpsertPlatformContext(context.Background(), p)
}

// UpsertPlatformContext inserts or updates a platform by ID while honoring ctx.
func (r *StateRepo) UpsertPlatformContext(ctx context.Context, p model.Platform) error {
	p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON, err := preparePlatformPersistence(p)
	if err != nil {
		return err
	}

	return r.withWriteContext(ctx, func(ctx context.Context) error {
		exec, unlock, err := r.lockWriteContext(ctx)
		if err != nil {
			return err
		}
		defer unlock()

		_, err = exec.ExecContext(ctx, platformUpsertSQL, platformPersistenceArgs(p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON)...)
		if err != nil {
			if isSQLiteUniqueConstraint(err) {
				return fmt.Errorf("%w: platform name already exists", ErrConflict)
			}
			return err
		}
		return nil
	})
}

// UpsertPlatformContextAndCommit persists a platform after a cancelable
// SQLite writer admission, then commits with the shutdown-owned context.
func (r *StateRepo) UpsertPlatformContextAndCommit(ctx context.Context, p model.Platform) error {
	p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON, err := preparePlatformPersistence(p)
	if err != nil {
		return err
	}
	return r.withCommitTx(ctx, func(commitCtx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(commitCtx, platformUpsertSQL, platformPersistenceArgs(p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON)...)
		if err != nil {
			if isSQLiteUniqueConstraint(err) {
				return fmt.Errorf("%w: platform name already exists", ErrConflict)
			}
			return err
		}
		return nil
	})
}

// InsertPlatform persists a new platform and rejects both duplicate IDs and
// duplicate names. Create paths must use this strict operation; UpsertPlatform
// is reserved for updates and bootstrap reconciliation.
func (r *StateRepo) InsertPlatform(p model.Platform) error {
	return r.InsertPlatformContext(context.Background(), p)
}

// InsertPlatformContext persists a new platform while honoring ctx.
func (r *StateRepo) InsertPlatformContext(ctx context.Context, p model.Platform) error {
	p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON, err := preparePlatformPersistence(p)
	if err != nil {
		return err
	}

	return r.withWriteContext(ctx, func(ctx context.Context) error {
		exec, unlock, err := r.lockWriteContext(ctx)
		if err != nil {
			return err
		}
		defer unlock()

		_, err = exec.ExecContext(ctx, platformInsertSQL, platformPersistenceArgs(p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON)...)
		if err != nil {
			if isSQLitePlatformConflict(err) {
				return fmt.Errorf("%w: platform ID or name already exists", ErrConflict)
			}
			return err
		}
		return nil
	})
}

// InsertPlatformContextAndCommit persists a platform after a cancelable
// SQLite writer admission, then commits with the shutdown-owned context.
func (r *StateRepo) InsertPlatformContextAndCommit(ctx context.Context, p model.Platform) error {
	p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON, err := preparePlatformPersistence(p)
	if err != nil {
		return err
	}
	return r.withCommitTx(ctx, func(commitCtx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(commitCtx, platformInsertSQL, platformPersistenceArgs(p, regexFiltersJSON, regionFiltersJSON, responseRulesJSON)...)
		if err != nil {
			if isSQLitePlatformConflict(err) {
				return fmt.Errorf("%w: platform ID or name already exists", ErrConflict)
			}
			return err
		}
		return nil
	})
}

func isSQLiteUniqueConstraint(err error) bool {
	var sqlErr *sqlite.Error
	if !errors.As(err, &sqlErr) {
		return false
	}
	return sqlErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

func isSQLiteBusyError(err error) bool {
	var sqlErr *sqlite.Error
	if !errors.As(err, &sqlErr) {
		return false
	}
	return sqlErr.Code()&0xff == sqlite3.SQLITE_BUSY
}

func isSQLitePlatformConflict(err error) bool {
	var sqlErr *sqlite.Error
	if !errors.As(err, &sqlErr) {
		return false
	}
	switch sqlErr.Code() {
	case sqlite3.SQLITE_CONSTRAINT_UNIQUE, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY:
		return true
	default:
		return false
	}
}

// DeletePlatform removes a platform by ID.
func (r *StateRepo) DeletePlatform(id string) error {
	return r.DeletePlatformContext(context.Background(), id)
}

// DeletePlatformContext removes a platform by ID while honoring ctx.
func (r *StateRepo) DeletePlatformContext(ctx context.Context, id string) error {
	return r.withWriteContext(ctx, func(ctx context.Context) error {
		exec, unlock, err := r.lockWriteContext(ctx)
		if err != nil {
			return err
		}
		defer unlock()

		result, err := exec.ExecContext(ctx, "DELETE FROM platforms WHERE id = ?", id)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// DeletePlatformContextAndCommit deletes a platform after a cancelable
// SQLite writer admission, then commits with the shutdown-owned context.
func (r *StateRepo) DeletePlatformContextAndCommit(ctx context.Context, id string) error {
	return r.withCommitTx(ctx, func(commitCtx context.Context, conn *sql.Conn) error {
		result, err := conn.ExecContext(commitCtx, "DELETE FROM platforms WHERE id = ?", id)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// GetPlatformName returns platform name by ID without decoding filter columns.
func (r *StateRepo) GetPlatformName(id string) (string, error) {
	return r.GetPlatformNameContext(context.Background(), id)
}

// GetPlatformNameContext returns a platform name while honoring ctx during
// the SQLite query.
func (r *StateRepo) GetPlatformNameContext(ctx context.Context, id string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	row := r.db.QueryRowContext(ctx, `SELECT name FROM platforms WHERE id = ?`, id)
	var name string
	if err := row.Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return "", ErrNotFound
		}
		return "", err
	}
	return name, nil
}

const platformSelectSQL = `SELECT id, name, sticky_ttl_ns, regex_filters_json, region_filters_json, response_rules_json,
	reverse_proxy_miss_action, reverse_proxy_empty_account_behavior, reverse_proxy_fixed_account_header,
	allocation_policy, passive_circuit_breaker_disabled, proxy_request_total_timeout_ns,
	proxy_request_attempt_timeout_ns, proxy_request_max_attempts, updated_at_ns FROM platforms`

type platformRowScanner interface {
	Scan(dest ...any) error
}

func scanPlatformRow(row platformRowScanner) (model.Platform, error) {
	var p model.Platform
	var regexFiltersJSON, regionFiltersJSON, responseRulesJSON string
	var passiveCircuitBreakerDisabled int
	if err := row.Scan(&p.ID, &p.Name, &p.StickyTTLNs, &regexFiltersJSON,
		&regionFiltersJSON, &responseRulesJSON, &p.ReverseProxyMissAction, &p.ReverseProxyEmptyAccountBehavior,
		&p.ReverseProxyFixedAccountHeader, &p.AllocationPolicy, &passiveCircuitBreakerDisabled,
		&p.ProxyRequestTotalTimeoutNs, &p.ProxyRequestAttemptTimeoutNs, &p.ProxyRequestMaxAttempts,
		&p.UpdatedAtNs); err != nil {
		return model.Platform{}, err
	}
	p.PassiveCircuitBreakerDisabled = passiveCircuitBreakerDisabled != 0
	regexFilters, err := decodeStringSliceJSON(regexFiltersJSON)
	if err != nil {
		return model.Platform{}, fmt.Errorf("decode platform %s regex_filters_json: %w", p.ID, err)
	}
	regionFilters, err := decodeStringSliceJSON(regionFiltersJSON)
	if err != nil {
		return model.Platform{}, fmt.Errorf("decode platform %s region_filters_json: %w", p.ID, err)
	}
	p.RegexFilters = regexFilters
	p.RegionFilters = regionFilters
	if err := json.Unmarshal([]byte(responseRulesJSON), &p.ResponseRules); err != nil {
		return model.Platform{}, fmt.Errorf("decode platform %s response_rules_json: %w", p.ID, err)
	}
	if p.ResponseRules == nil {
		p.ResponseRules = []model.PlatformResponseRule{}
	}
	return p, nil
}

// GetPlatform returns one platform by ID.
func (r *StateRepo) GetPlatform(id string) (*model.Platform, error) {
	return r.GetPlatformContext(context.Background(), id)
}

// GetPlatformContext returns one platform by ID while honoring ctx during the
// SQLite query. The context-aware form is used by request-bound API reads.
func (r *StateRepo) GetPlatformContext(ctx context.Context, id string) (*model.Platform, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	row := r.db.QueryRowContext(ctx, platformSelectSQL+" WHERE id = ?", id)
	p, err := scanPlatformRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &p, nil
}

// ListPlatforms returns all platforms.
func (r *StateRepo) ListPlatforms() ([]model.Platform, error) {
	return r.ListPlatformsContext(context.Background())
}

// ListPlatformsContext returns all platforms while honoring ctx during the
// SQLite query and row iteration.
func (r *StateRepo) ListPlatformsContext(ctx context.Context) ([]model.Platform, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := r.db.QueryContext(ctx, platformSelectSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.Platform
	for rows.Next() {
		p, err := scanPlatformRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

// --- subscriptions ---

const subscriptionUpsertSQL = `
		INSERT INTO subscriptions (id, name, source_type, url, content, update_interval_ns, enabled,
		                           ephemeral, incremental_alive_nodes, ephemeral_node_evict_delay_ns, created_at_ns, updated_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name               = excluded.name,
			source_type        = excluded.source_type,
			url                = excluded.url,
			content            = excluded.content,
			update_interval_ns = excluded.update_interval_ns,
			enabled            = excluded.enabled,
			ephemeral          = excluded.ephemeral,
			incremental_alive_nodes = excluded.incremental_alive_nodes,
			ephemeral_node_evict_delay_ns = excluded.ephemeral_node_evict_delay_ns,
			updated_at_ns      = excluded.updated_at_ns
`

func normalizeSubscriptionPersistence(s model.Subscription) (model.Subscription, error) {
	const minInterval = int64(30 * time.Second)
	if s.UpdateIntervalNs < minInterval {
		return model.Subscription{}, fmt.Errorf("update_interval_ns: must be >= %d (30s), got %d", minInterval, s.UpdateIntervalNs)
	}
	if s.SourceType == "" {
		s.SourceType = "remote"
	}
	if s.SourceType != "remote" && s.SourceType != "local" {
		return model.Subscription{}, fmt.Errorf("source_type: must be remote or local, got %q", s.SourceType)
	}
	return s, nil
}

// UpsertSubscription inserts or updates a subscription by ID.
// On update, created_at_ns is preserved (not overwritten).
func (r *StateRepo) UpsertSubscription(s model.Subscription) error {
	return r.UpsertSubscriptionContext(context.Background(), s)
}

// UpsertSubscriptionContext inserts or updates a subscription while honoring
// the caller's cancellation during the write owner and SQLite operation.
func (r *StateRepo) UpsertSubscriptionContext(ctx context.Context, s model.Subscription) error {
	var err error
	if s, err = normalizeSubscriptionPersistence(s); err != nil {
		return err
	}

	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if err := writeCtx.Err(); err != nil {
			return err
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		_, err = exec.ExecContext(writeCtx, subscriptionUpsertSQL,
			s.ID, s.Name, s.SourceType, s.URL, s.Content, s.UpdateIntervalNs, s.Enabled,
			s.Ephemeral, s.IncrementalAliveNodes, s.EphemeralNodeEvictDelayNs, s.CreatedAtNs, s.UpdatedAtNs)
		return err
	})
}

// UpsertSubscriptionContextAndCommit persists a subscription after a
// cancelable SQLite writer admission, then completes SQL and COMMIT with the
// shutdown-owned context. Callers can safely publish the matching runtime
// generation after this method returns.
func (r *StateRepo) UpsertSubscriptionContextAndCommit(ctx context.Context, s model.Subscription) error {
	var err error
	if s, err = normalizeSubscriptionPersistence(s); err != nil {
		return err
	}
	return r.withCommitTx(ctx, func(commitCtx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(commitCtx, subscriptionUpsertSQL,
			s.ID, s.Name, s.SourceType, s.URL, s.Content, s.UpdateIntervalNs, s.Enabled,
			s.Ephemeral, s.IncrementalAliveNodes, s.EphemeralNodeEvictDelayNs, s.CreatedAtNs, s.UpdatedAtNs)
		return err
	})
}

// DeleteSubscription removes a subscription by ID.
func (r *StateRepo) DeleteSubscription(id string) error {
	return r.DeleteSubscriptionContext(context.Background(), id)
}

// DeleteSubscriptionContext removes a subscription while honoring the
// caller's cancellation during the write owner and SQLite operation.
func (r *StateRepo) DeleteSubscriptionContext(ctx context.Context, id string) error {
	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if err := writeCtx.Err(); err != nil {
			return err
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		result, err := exec.ExecContext(writeCtx, "DELETE FROM subscriptions WHERE id = ?", id)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// DeleteSubscriptionContextAndCommit deletes a subscription after a
// cancelable SQLite writer admission, then completes the delete and COMMIT
// with the shutdown-owned context.
func (r *StateRepo) DeleteSubscriptionContextAndCommit(ctx context.Context, id string) error {
	return r.withCommitTx(ctx, func(commitCtx context.Context, conn *sql.Conn) error {
		result, err := conn.ExecContext(commitCtx, "DELETE FROM subscriptions WHERE id = ?", id)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ListSubscriptions returns all subscriptions.
func (r *StateRepo) ListSubscriptions() ([]model.Subscription, error) {
	rows, err := r.db.Query(`SELECT id, name, source_type, url, content, update_interval_ns, enabled,
		ephemeral, incremental_alive_nodes, ephemeral_node_evict_delay_ns, created_at_ns, updated_at_ns FROM subscriptions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.Subscription
	for rows.Next() {
		var s model.Subscription
		if err := rows.Scan(&s.ID, &s.Name, &s.SourceType, &s.URL, &s.Content, &s.UpdateIntervalNs, &s.Enabled,
			&s.Ephemeral, &s.IncrementalAliveNodes, &s.EphemeralNodeEvictDelayNs, &s.CreatedAtNs, &s.UpdatedAtNs); err != nil {
			return nil, err
		}
		if s.SourceType == "" {
			s.SourceType = "remote"
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// --- endpoints ---

// InsertEndpoint persists a new custom endpoint.
func (r *StateRepo) InsertEndpoint(endpoint model.Endpoint) error {
	return r.InsertEndpointContext(context.Background(), endpoint)
}

// InsertEndpointContext persists a new custom endpoint while honoring the
// caller's cancellation during the SQLite operation.
func (r *StateRepo) InsertEndpointContext(ctx context.Context, endpoint model.Endpoint) error {
	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		_, err = exec.ExecContext(writeCtx, `
		INSERT INTO endpoints (
			id, port, enabled, allow_management, allow_proxy, require_proxy_auth_info,
			allow_http_forward, allow_http_reverse, allow_socks5, created_at_ns, updated_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, endpoint.ID, endpoint.Port, endpoint.Enabled, endpoint.AllowManagement, endpoint.AllowProxy,
			endpoint.RequireProxyAuthInfo, endpoint.AllowHTTPForward, endpoint.AllowHTTPReverse,
			endpoint.AllowSOCKS5, endpoint.CreatedAtNs, endpoint.UpdatedAtNs)
		if isSQLiteUniqueConstraint(err) {
			return fmt.Errorf("%w: endpoint id or port already exists", ErrConflict)
		}
		return err
	})
}

// UpdateEndpoint replaces the mutable fields of an existing custom endpoint.
func (r *StateRepo) UpdateEndpoint(endpoint model.Endpoint) error {
	return r.UpdateEndpointContext(context.Background(), endpoint)
}

// UpdateEndpointContext replaces endpoint fields while honoring the caller's
// cancellation during the SQLite operation.
func (r *StateRepo) UpdateEndpointContext(ctx context.Context, endpoint model.Endpoint) error {
	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		result, err := exec.ExecContext(writeCtx, `
		UPDATE endpoints SET
			port = ?,
			enabled = ?,
			allow_management = ?,
			allow_proxy = ?,
			require_proxy_auth_info = ?,
			allow_http_forward = ?,
			allow_http_reverse = ?,
			allow_socks5 = ?,
			updated_at_ns = ?
		WHERE id = ?
		`, endpoint.Port, endpoint.Enabled, endpoint.AllowManagement, endpoint.AllowProxy,
			endpoint.RequireProxyAuthInfo, endpoint.AllowHTTPForward, endpoint.AllowHTTPReverse,
			endpoint.AllowSOCKS5, endpoint.UpdatedAtNs, endpoint.ID)
		if isSQLiteUniqueConstraint(err) {
			return fmt.Errorf("%w: endpoint port already exists", ErrConflict)
		}
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// DeleteEndpoint removes a custom endpoint by ID.
func (r *StateRepo) DeleteEndpoint(id string) error {
	return r.DeleteEndpointContext(context.Background(), id)
}

// DeleteEndpointContext removes an endpoint while honoring the caller's
// cancellation during the SQLite operation.
func (r *StateRepo) DeleteEndpointContext(ctx context.Context, id string) error {
	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		result, err := exec.ExecContext(writeCtx, "DELETE FROM endpoints WHERE id = ?", id)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// GetEndpoint returns one persisted custom endpoint.
func (r *StateRepo) GetEndpoint(id string) (*model.Endpoint, error) {
	return r.GetEndpointContext(context.Background(), id)
}

// GetEndpointContext returns one endpoint while honoring caller cancellation
// during the SQLite read.
func (r *StateRepo) GetEndpointContext(ctx context.Context, id string) (*model.Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, release, err := r.contextReadConn(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	queryRow := func() *sql.Row {
		if conn != nil {
			if hook := r.beforeContextReadQueryHook; hook != nil {
				hook(conn)
			}
			return conn.QueryRowContext(ctx, `
			SELECT id, port, enabled, allow_management, allow_proxy, require_proxy_auth_info,
			       allow_http_forward, allow_http_reverse, allow_socks5, created_at_ns, updated_at_ns
			FROM endpoints WHERE id = ?
		`, id)
		}
		return r.db.QueryRowContext(ctx, `
		SELECT id, port, enabled, allow_management, allow_proxy, require_proxy_auth_info,
		       allow_http_forward, allow_http_reverse, allow_socks5, created_at_ns, updated_at_ns
		FROM endpoints WHERE id = ?
		`, id)
	}
	row := queryRow()
	endpoint, err := scanEndpoint(row.Scan)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &endpoint, nil
}

// ListEndpoints returns all persisted custom endpoints ordered by port.
func (r *StateRepo) ListEndpoints() ([]model.Endpoint, error) {
	return r.ListEndpointsContext(context.Background())
}

// ListEndpointsContext returns all persisted custom endpoints while honoring
// caller cancellation during the SQLite read.
func (r *StateRepo) ListEndpointsContext(ctx context.Context) ([]model.Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, port, enabled, allow_management, allow_proxy, require_proxy_auth_info,
		       allow_http_forward, allow_http_reverse, allow_socks5, created_at_ns, updated_at_ns
		FROM endpoints ORDER BY port ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]model.Endpoint, 0)
	for rows.Next() {
		endpoint, scanErr := scanEndpoint(rows.Scan)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, endpoint)
	}
	return result, rows.Err()
}

type endpointScanner func(dest ...any) error

func scanEndpoint(scan endpointScanner) (model.Endpoint, error) {
	var endpoint model.Endpoint
	var enabled, allowManagement, allowProxy, requireProxyAuthInfo int
	var allowHTTPForward, allowHTTPReverse, allowSOCKS5 int
	err := scan(
		&endpoint.ID,
		&endpoint.Port,
		&enabled,
		&allowManagement,
		&allowProxy,
		&requireProxyAuthInfo,
		&allowHTTPForward,
		&allowHTTPReverse,
		&allowSOCKS5,
		&endpoint.CreatedAtNs,
		&endpoint.UpdatedAtNs,
	)
	if err != nil {
		return model.Endpoint{}, err
	}
	endpoint.Enabled = enabled != 0
	endpoint.AllowManagement = allowManagement != 0
	endpoint.AllowProxy = allowProxy != 0
	endpoint.RequireProxyAuthInfo = requireProxyAuthInfo != 0
	endpoint.AllowHTTPForward = allowHTTPForward != 0
	endpoint.AllowHTTPReverse = allowHTTPReverse != 0
	endpoint.AllowSOCKS5 = allowSOCKS5 != 0
	return endpoint, nil
}

// --- account_header_rules ---

// EnsureAccountHeaderRule inserts a rule by url_prefix only when it does not
// already exist and reports whether the row was newly created.
func (r *StateRepo) EnsureAccountHeaderRule(rule model.AccountHeaderRule) (bool, error) {
	return r.EnsureAccountHeaderRuleContext(context.Background(), rule)
}

// EnsureAccountHeaderRuleContext is the request-aware form of
// EnsureAccountHeaderRule. The repository mutex and SQLite write both honor
// cancellation before any row is changed.
func (r *StateRepo) EnsureAccountHeaderRuleContext(ctx context.Context, rule model.AccountHeaderRule) (bool, error) {
	headersJSON, err := encodeStringSliceJSON(rule.Headers)
	if err != nil {
		return false, fmt.Errorf("encode account header rule %q headers: %w", rule.URLPrefix, err)
	}

	var created bool
	err = r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		result, err := exec.ExecContext(writeCtx, `
		INSERT INTO account_header_rules (url_prefix, headers_json, updated_at_ns)
		VALUES (?, ?, ?)
		ON CONFLICT(url_prefix) DO NOTHING
		`, rule.URLPrefix, headersJSON, rule.UpdatedAtNs)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		created = n > 0
		return nil
	})
	return created, err
}

// UpsertAccountHeaderRuleWithCreated inserts or updates a rule by url_prefix and
// reports whether the row was newly created.
func (r *StateRepo) UpsertAccountHeaderRuleWithCreated(rule model.AccountHeaderRule) (bool, error) {
	return r.UpsertAccountHeaderRuleWithCreatedContext(context.Background(), rule)
}

// UpsertAccountHeaderRuleWithCreatedContext is the request-aware form of
// UpsertAccountHeaderRuleWithCreated.
func (r *StateRepo) UpsertAccountHeaderRuleWithCreatedContext(ctx context.Context, rule model.AccountHeaderRule) (bool, error) {
	headersJSON, err := encodeStringSliceJSON(rule.Headers)
	if err != nil {
		return false, fmt.Errorf("encode account header rule %q headers: %w", rule.URLPrefix, err)
	}

	var inserted bool
	err = r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		tx, err := exec.BeginTx(writeCtx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		insertRes, err := tx.ExecContext(writeCtx, `
		INSERT INTO account_header_rules (url_prefix, headers_json, updated_at_ns)
		VALUES (?, ?, ?)
		ON CONFLICT(url_prefix) DO NOTHING
		`, rule.URLPrefix, headersJSON, rule.UpdatedAtNs)
		if err != nil {
			return err
		}

		inserted = false
		if n, _ := insertRes.RowsAffected(); n > 0 {
			inserted = true
		} else {
			// Existing row: apply update path.
			if _, err := tx.ExecContext(writeCtx, `
				UPDATE account_header_rules
				SET headers_json = ?, updated_at_ns = ?
				WHERE url_prefix = ?
			`, headersJSON, rule.UpdatedAtNs, rule.URLPrefix); err != nil {
				return err
			}
		}

		return tx.Commit()
	})
	return inserted, err
}

// DeleteAccountHeaderRule removes a rule by url_prefix.
func (r *StateRepo) DeleteAccountHeaderRule(prefix string) error {
	return r.DeleteAccountHeaderRuleContext(context.Background(), prefix)
}

// DeleteAccountHeaderRuleContext is the request-aware form of
// DeleteAccountHeaderRule.
func (r *StateRepo) DeleteAccountHeaderRuleContext(ctx context.Context, prefix string) error {
	return r.withWriteContext(ctx, func(writeCtx context.Context) error {
		if hook := r.beforeWriteMutexHook; hook != nil {
			hook()
		}
		exec, unlock, err := r.lockWriteContext(writeCtx)
		if err != nil {
			return err
		}
		defer unlock()

		result, err := exec.ExecContext(writeCtx, "DELETE FROM account_header_rules WHERE url_prefix = ?", prefix)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ListAccountHeaderRules returns all rules.
func (r *StateRepo) ListAccountHeaderRules() ([]model.AccountHeaderRule, error) {
	return r.ListAccountHeaderRulesContext(context.Background())
}

// ListAccountHeaderRulesContext returns all rules while honoring caller
// cancellation during the database query and row iteration.
func (r *StateRepo) ListAccountHeaderRulesContext(ctx context.Context) ([]model.AccountHeaderRule, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, release, err := r.contextReadConn(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	var rows *sql.Rows
	if conn != nil {
		if hook := r.beforeContextReadQueryHook; hook != nil {
			hook(conn)
		}
		rows, err = conn.QueryContext(ctx, "SELECT url_prefix, headers_json, updated_at_ns FROM account_header_rules")
	} else {
		rows, err = r.db.QueryContext(ctx, "SELECT url_prefix, headers_json, updated_at_ns FROM account_header_rules")
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.AccountHeaderRule
	for rows.Next() {
		var rule model.AccountHeaderRule
		var headersJSON string
		if err := rows.Scan(&rule.URLPrefix, &headersJSON, &rule.UpdatedAtNs); err != nil {
			return nil, err
		}
		headers, err := decodeStringSliceJSON(headersJSON)
		if err != nil {
			return nil, fmt.Errorf("decode account header rule %q headers_json: %w", rule.URLPrefix, err)
		}
		rule.Headers = headers
		result = append(result, rule)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, rows.Err()
}
