package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Resinat/Resin/internal/model"
)

// CacheRepo wraps cache.db and provides batch read/write for weak-persist data.
type CacheRepo struct {
	db *sql.DB

	// Package-private test seam for invalidating an interruptible connection
	// immediately before its busy-timeout restoration.
	beforeContextConnResetHook func(*sql.Conn)
	// Package-private test seam for coordinating the real transaction begin.
	beforeContextTxBeginHook func()
}

// newCacheRepo creates a CacheRepo for the given cache.db connection.
func newCacheRepo(db *sql.DB) *CacheRepo {
	return &CacheRepo{db: db}
}

// --- nodes_static ---

// BulkUpsertNodesStatic batch-inserts or updates node static records.
func (r *CacheRepo) BulkUpsertNodesStatic(nodes []model.NodeStatic) error {
	return bulkExecRows(
		r,
		upsertNodesStaticSQL,
		nodes,
		func(stmt *sql.Stmt, n model.NodeStatic) error {
			_, err := stmt.Exec(n.Hash, string(n.RawOptions), n.CreatedAtNs)
			return err
		},
	)
}

// BulkDeleteNodesStatic batch-deletes node static records by hash.
func (r *CacheRepo) BulkDeleteNodesStatic(hashes []string) error {
	return bulkExecRows(
		r,
		deleteNodesStaticSQL,
		hashes,
		func(stmt *sql.Stmt, hash string) error {
			_, err := stmt.Exec(hash)
			return err
		},
	)
}

// LoadAllNodesStatic reads all node static records.
func (r *CacheRepo) LoadAllNodesStatic() ([]model.NodeStatic, error) {
	rows, err := r.db.Query("SELECT hash, raw_options_json, created_at_ns FROM nodes_static")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeStatic
	for rows.Next() {
		var n model.NodeStatic
		var rawOptionsJSON string
		if err := rows.Scan(&n.Hash, &rawOptionsJSON, &n.CreatedAtNs); err != nil {
			return nil, err
		}
		n.RawOptions = json.RawMessage(rawOptionsJSON)
		result = append(result, n)
	}
	return result, rows.Err()
}

// --- nodes_dynamic ---

// BulkUpsertNodesDynamic batch-inserts or updates node dynamic records.
func (r *CacheRepo) BulkUpsertNodesDynamic(nodes []model.NodeDynamic) error {
	return bulkExecRows(
		r,
		upsertNodesDynamicSQL,
		nodes,
		func(stmt *sql.Stmt, n model.NodeDynamic) error {
			_, err := stmt.Exec(
				n.Hash,
				n.FailureCount,
				n.CircuitOpenSince,
				n.EgressIP,
				n.EgressRegion,
				n.EgressUpdatedAtNs,
				n.LastLatencyProbeAttemptNs,
				n.LastAuthorityLatencyProbeAttemptNs,
				n.LastEgressUpdateAttemptNs,
			)
			return err
		},
	)
}

// BulkDeleteNodesDynamic batch-deletes node dynamic records by hash.
func (r *CacheRepo) BulkDeleteNodesDynamic(hashes []string) error {
	return bulkExecRows(
		r,
		deleteNodesDynamicSQL,
		hashes,
		func(stmt *sql.Stmt, hash string) error {
			_, err := stmt.Exec(hash)
			return err
		},
	)
}

// LoadAllNodesDynamic reads all node dynamic records.
func (r *CacheRepo) LoadAllNodesDynamic() ([]model.NodeDynamic, error) {
	rows, err := r.db.Query(`
		SELECT hash, failure_count, circuit_open_since, egress_ip, egress_region, egress_updated_at_ns,
		       last_latency_probe_attempt_ns, last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns
		FROM nodes_dynamic`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeDynamic
	for rows.Next() {
		var n model.NodeDynamic
		if err := rows.Scan(
			&n.Hash,
			&n.FailureCount,
			&n.CircuitOpenSince,
			&n.EgressIP,
			&n.EgressRegion,
			&n.EgressUpdatedAtNs,
			&n.LastLatencyProbeAttemptNs,
			&n.LastAuthorityLatencyProbeAttemptNs,
			&n.LastEgressUpdateAttemptNs,
		); err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

// --- node_latency ---

// BulkUpsertNodeLatency batch-inserts or updates node latency records.
func (r *CacheRepo) BulkUpsertNodeLatency(entries []model.NodeLatency) error {
	return bulkExecRows(
		r,
		upsertNodeLatencySQL,
		entries,
		func(stmt *sql.Stmt, e model.NodeLatency) error {
			_, err := stmt.Exec(e.NodeHash, e.Domain, e.EwmaNs, e.LastUpdatedNs)
			return err
		},
	)
}

// BulkDeleteNodeLatency batch-deletes node latency records by composite key.
func (r *CacheRepo) BulkDeleteNodeLatency(keys []model.NodeLatencyKey) error {
	return bulkExecRows(
		r,
		deleteNodeLatencySQL,
		keys,
		func(stmt *sql.Stmt, key model.NodeLatencyKey) error {
			_, err := stmt.Exec(key.NodeHash, key.Domain)
			return err
		},
	)
}

// LoadAllNodeLatency reads all node latency records.
func (r *CacheRepo) LoadAllNodeLatency() ([]model.NodeLatency, error) {
	rows, err := r.db.Query("SELECT node_hash, domain, ewma_ns, last_updated_ns FROM node_latency")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeLatency
	for rows.Next() {
		var e model.NodeLatency
		if err := rows.Scan(&e.NodeHash, &e.Domain, &e.EwmaNs, &e.LastUpdatedNs); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// --- leases ---

// BulkUpsertLeases batch-inserts or updates lease records.
func (r *CacheRepo) BulkUpsertLeases(leases []model.Lease) error {
	return bulkExecRows(
		r,
		upsertLeasesSQL,
		leases,
		func(stmt *sql.Stmt, l model.Lease) error {
			_, err := stmt.Exec(l.PlatformID, l.Account, l.NodeHash, l.EgressIP, l.CreatedAtNs, l.ExpiryNs, l.LastAccessedNs)
			return err
		},
	)
}

// BulkDeleteLeases batch-deletes lease records by composite key.
func (r *CacheRepo) BulkDeleteLeases(keys []model.LeaseKey) error {
	return bulkExecRows(
		r,
		deleteLeasesSQL,
		keys,
		func(stmt *sql.Stmt, key model.LeaseKey) error {
			_, err := stmt.Exec(key.PlatformID, key.Account)
			return err
		},
	)
}

// LoadAllLeases reads all lease records.
func (r *CacheRepo) LoadAllLeases() ([]model.Lease, error) {
	rows, err := r.db.Query("SELECT platform_id, account, node_hash, egress_ip, created_at_ns, expiry_ns, last_accessed_ns FROM leases")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.Lease
	for rows.Next() {
		var l model.Lease
		if err := rows.Scan(&l.PlatformID, &l.Account, &l.NodeHash, &l.EgressIP, &l.CreatedAtNs, &l.ExpiryNs, &l.LastAccessedNs); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

// --- subscription_nodes ---

// BulkUpsertSubscriptionNodes batch-inserts or updates subscription-node links.
func (r *CacheRepo) BulkUpsertSubscriptionNodes(nodes []model.SubscriptionNode) error {
	return bulkExecRows(
		r,
		upsertSubscriptionNodesSQL,
		nodes,
		func(stmt *sql.Stmt, sn model.SubscriptionNode) error {
			tagsJSON, err := encodeStringSliceJSON(sn.Tags)
			if err != nil {
				return fmt.Errorf("encode subscription node tags: %w", err)
			}
			_, err = stmt.Exec(sn.SubscriptionID, sn.NodeHash, tagsJSON, sn.Evicted)
			return err
		},
	)
}

// BulkDeleteSubscriptionNodes batch-deletes subscription-node links by composite key.
func (r *CacheRepo) BulkDeleteSubscriptionNodes(keys []model.SubscriptionNodeKey) error {
	return bulkExecRows(
		r,
		deleteSubscriptionNodesSQL,
		keys,
		func(stmt *sql.Stmt, key model.SubscriptionNodeKey) error {
			_, err := stmt.Exec(key.SubscriptionID, key.NodeHash)
			return err
		},
	)
}

// LoadAllSubscriptionNodes reads all subscription-node links.
func (r *CacheRepo) LoadAllSubscriptionNodes() ([]model.SubscriptionNode, error) {
	rows, err := r.db.Query("SELECT subscription_id, node_hash, tags_json, evicted FROM subscription_nodes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.SubscriptionNode
	for rows.Next() {
		var sn model.SubscriptionNode
		var tagsJSON string
		if err := rows.Scan(&sn.SubscriptionID, &sn.NodeHash, &tagsJSON, &sn.Evicted); err != nil {
			return nil, err
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode subscription node tags_json: %w", err)
		}
		sn.Tags = tags
		result = append(result, sn)
	}
	return result, rows.Err()
}

// --- internal helpers ---

// bulkExecTx runs a prepared statement within an existing transaction for n rows.
func bulkExecTx(tx *sql.Tx, query string, n int, execFn func(stmt *sql.Stmt, i int) error) error {
	return bulkExecTxContext(context.Background(), tx, query, n, execFn)
}

func bulkExecTxContext(ctx context.Context, tx *sql.Tx, query string, n int, execFn func(stmt *sql.Stmt, i int) error) error {
	if n == 0 {
		return nil
	}

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer func() {
		if ctx.Err() == nil {
			_ = stmt.Close()
		}
	}()

	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := execFn(stmt, i); err != nil {
			return fmt.Errorf("exec row %d: %w", i, err)
		}
	}
	return nil
}

// bulkExec runs a prepared statement in its own transaction for n rows.
// Used by individual BulkUpsert*/BulkDelete* methods (tests, bootstrap).
func (r *CacheRepo) bulkExec(query string, n int, execFn func(stmt *sql.Stmt, i int) error) error {
	if n == 0 {
		return nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := bulkExecTx(tx, query, n, execFn); err != nil {
		return err
	}
	return tx.Commit()
}

func bulkExecRows[T any](
	r *CacheRepo,
	query string,
	rows []T,
	execFn func(stmt *sql.Stmt, row T) error,
) error {
	return r.bulkExec(query, len(rows), func(stmt *sql.Stmt, i int) error {
		return execFn(stmt, rows[i])
	})
}

// FlushOps holds all upsert/delete slices for a single-transaction cache flush.
type FlushOps struct {
	UpsertNodesStatic       []model.NodeStatic
	DeleteNodesStatic       []string
	UpsertSubscriptionNodes []model.SubscriptionNode
	DeleteSubscriptionNodes []model.SubscriptionNodeKey
	UpsertNodesDynamic      []model.NodeDynamic
	DeleteNodesDynamic      []string
	UpsertNodeLatency       []model.NodeLatency
	DeleteNodeLatency       []model.NodeLatencyKey
	UpsertLeases            []model.Lease
	DeleteLeases            []model.LeaseKey
}

// FlushTx executes all upserts and deletes in a single transaction.
//
// Upsert order: nodes_static → subscription_nodes → nodes_dynamic → node_latency → leases
// Delete order: leases → node_latency → nodes_dynamic → subscription_nodes → nodes_static
func (r *CacheRepo) FlushTx(ops FlushOps) error {
	return r.flushTx(context.Background(), ops, false)
}

// FlushTxContext executes a cache flush with cancellation-aware transaction
// setup and statement execution. A canceled flush leaves its dirty batch for
// the caller to re-merge.
func (r *CacheRepo) FlushTxContext(ctx context.Context, ops FlushOps) error {
	return r.flushTx(ctx, ops, true)
}

func (r *CacheRepo) flushTx(ctx context.Context, ops FlushOps, interruptible bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var (
		tx          *sql.Tx
		conn        *sql.Conn
		err         error
		discardConn bool
	)
	if interruptible {
		conn, err = r.db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("acquire flush connection: %w", err)
		}
		busyTimeoutMs, timeoutErr := SQLiteBusyTimeoutMs(ctx)
		if timeoutErr != nil {
			_ = conn.Close()
			return fmt.Errorf("compute flush busy timeout: %w", timeoutErr)
		}
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMs)); err != nil {
			// The driver may apply the pragma before reporting an error. Mark
			// the connection bad before Close so database/sql cannot return a
			// partially configured connection to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			_ = conn.Close()
			return fmt.Errorf("set flush busy timeout: %w", err)
		}
		defer func() {
			if hook := r.beforeContextConnResetHook; hook != nil {
				hook(conn)
			}
			if discardConn || ctx.Err() != nil {
				// A canceled transaction can still be blocked by the same
				// SQLite writer lock. A failed rollback or commit also leaves
				// transaction state unknown. Discard the exact connection instead
				// of returning it to the pool in either case.
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
		tx, err = conn.BeginTx(ctx, nil)
	} else {
		tx, err = r.db.BeginTx(ctx, nil)
	}
	if err != nil {
		return fmt.Errorf("begin flush tx: %w", err)
	}
	rollback := true
	committed := false
	defer func() {
		// database/sql rolls a context-canceled transaction back through the
		// context watcher. Calling Rollback here can itself wait on the same
		// SQLite lock that caused cancellation, defeating the deadline.
		if rollback && !committed {
			if err := tx.Rollback(); err != nil {
				discardConn = true
			}
		}
	}()

	// Upserts in dependency order.
	steps := []struct {
		name  string
		query string
		n     int
		exec  func(*sql.Stmt, int) error
	}{
		{"upsert_nodes_static", upsertNodesStaticSQL, len(ops.UpsertNodesStatic), func(s *sql.Stmt, i int) error {
			n := ops.UpsertNodesStatic[i]
			_, err := s.ExecContext(ctx, n.Hash, string(n.RawOptions), n.CreatedAtNs)
			return err
		}},
		{"upsert_subscription_nodes", upsertSubscriptionNodesSQL, len(ops.UpsertSubscriptionNodes), func(s *sql.Stmt, i int) error {
			sn := ops.UpsertSubscriptionNodes[i]
			tagsJSON, err := encodeStringSliceJSON(sn.Tags)
			if err != nil {
				return fmt.Errorf("encode subscription node tags: %w", err)
			}
			_, err = s.ExecContext(ctx, sn.SubscriptionID, sn.NodeHash, tagsJSON, sn.Evicted)
			return err
		}},
		{"upsert_nodes_dynamic", upsertNodesDynamicSQL, len(ops.UpsertNodesDynamic), func(s *sql.Stmt, i int) error {
			n := ops.UpsertNodesDynamic[i]
			_, err := s.ExecContext(ctx,
				n.Hash,
				n.FailureCount,
				n.CircuitOpenSince,
				n.EgressIP,
				n.EgressRegion,
				n.EgressUpdatedAtNs,
				n.LastLatencyProbeAttemptNs,
				n.LastAuthorityLatencyProbeAttemptNs,
				n.LastEgressUpdateAttemptNs,
			)
			return err
		}},
		{"upsert_node_latency", upsertNodeLatencySQL, len(ops.UpsertNodeLatency), func(s *sql.Stmt, i int) error {
			e := ops.UpsertNodeLatency[i]
			_, err := s.ExecContext(ctx, e.NodeHash, e.Domain, e.EwmaNs, e.LastUpdatedNs)
			return err
		}},
		{"upsert_leases", upsertLeasesSQL, len(ops.UpsertLeases), func(s *sql.Stmt, i int) error {
			l := ops.UpsertLeases[i]
			_, err := s.ExecContext(ctx, l.PlatformID, l.Account, l.NodeHash, l.EgressIP, l.CreatedAtNs, l.ExpiryNs, l.LastAccessedNs)
			return err
		}},
		// Deletes in reverse dependency order.
		{"delete_leases", deleteLeasesSQL, len(ops.DeleteLeases), func(s *sql.Stmt, i int) error {
			_, err := s.ExecContext(ctx, ops.DeleteLeases[i].PlatformID, ops.DeleteLeases[i].Account)
			return err
		}},
		{"delete_node_latency", deleteNodeLatencySQL, len(ops.DeleteNodeLatency), func(s *sql.Stmt, i int) error {
			_, err := s.ExecContext(ctx, ops.DeleteNodeLatency[i].NodeHash, ops.DeleteNodeLatency[i].Domain)
			return err
		}},
		{"delete_nodes_dynamic", deleteNodesDynamicSQL, len(ops.DeleteNodesDynamic), func(s *sql.Stmt, i int) error {
			_, err := s.ExecContext(ctx, ops.DeleteNodesDynamic[i])
			return err
		}},
		{"delete_subscription_nodes", deleteSubscriptionNodesSQL, len(ops.DeleteSubscriptionNodes), func(s *sql.Stmt, i int) error {
			_, err := s.ExecContext(ctx, ops.DeleteSubscriptionNodes[i].SubscriptionID, ops.DeleteSubscriptionNodes[i].NodeHash)
			return err
		}},
		{"delete_nodes_static", deleteNodesStaticSQL, len(ops.DeleteNodesStatic), func(s *sql.Stmt, i int) error {
			_, err := s.ExecContext(ctx, ops.DeleteNodesStatic[i])
			return err
		}},
	}

	for _, step := range steps {
		if err := bulkExecTxContext(ctx, tx, step.query, step.n, step.exec); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				rollback = false
			}
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		discardConn = true
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rollback = false
		}
		return err
	}
	committed = true
	return nil
}

func resetSQLiteBusyTimeout(conn *sql.Conn) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", DefaultSQLiteBusyTimeoutMs))
	return err == nil
}

// SQL constants for FlushTx. Extracted to avoid string duplication.
const (
	upsertNodesStaticSQL = `INSERT INTO nodes_static (hash, raw_options_json, created_at_ns)
		 VALUES (?, ?, ?)
		 ON CONFLICT(hash) DO UPDATE SET
			raw_options_json = excluded.raw_options_json,
			created_at_ns    = excluded.created_at_ns`

	upsertNodesDynamicSQL = `INSERT INTO nodes_dynamic (
			hash, failure_count, circuit_open_since, egress_ip, egress_region, egress_updated_at_ns,
			last_latency_probe_attempt_ns, last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns
		)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(hash) DO UPDATE SET
			failure_count                          = excluded.failure_count,
			circuit_open_since                     = excluded.circuit_open_since,
			egress_ip                              = excluded.egress_ip,
			egress_region                          = excluded.egress_region,
			egress_updated_at_ns                   = excluded.egress_updated_at_ns,
			last_latency_probe_attempt_ns          = excluded.last_latency_probe_attempt_ns,
			last_authority_latency_probe_attempt_ns = excluded.last_authority_latency_probe_attempt_ns,
			last_egress_update_attempt_ns          = excluded.last_egress_update_attempt_ns`

	upsertNodeLatencySQL = `INSERT INTO node_latency (node_hash, domain, ewma_ns, last_updated_ns)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(node_hash, domain) DO UPDATE SET
			ewma_ns         = excluded.ewma_ns,
			last_updated_ns = excluded.last_updated_ns`

	upsertLeasesSQL = `INSERT INTO leases (platform_id, account, node_hash, egress_ip, created_at_ns, expiry_ns, last_accessed_ns)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(platform_id, account) DO UPDATE SET
			node_hash       = excluded.node_hash,
			egress_ip       = excluded.egress_ip,
			created_at_ns   = excluded.created_at_ns,
			expiry_ns       = excluded.expiry_ns,
			last_accessed_ns = excluded.last_accessed_ns`

	upsertSubscriptionNodesSQL = `INSERT INTO subscription_nodes (subscription_id, node_hash, tags_json, evicted)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(subscription_id, node_hash) DO UPDATE SET
			tags_json = excluded.tags_json,
			evicted = excluded.evicted`

	deleteNodesStaticSQL       = "DELETE FROM nodes_static WHERE hash = ?"
	deleteNodesDynamicSQL      = "DELETE FROM nodes_dynamic WHERE hash = ?"
	deleteNodeLatencySQL       = "DELETE FROM node_latency WHERE node_hash = ? AND domain = ?"
	deleteLeasesSQL            = "DELETE FROM leases WHERE platform_id = ? AND account = ?"
	deleteSubscriptionNodesSQL = "DELETE FROM subscription_nodes WHERE subscription_id = ? AND node_hash = ?"
)
