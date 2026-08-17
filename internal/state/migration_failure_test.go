package state

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrateStateDB_RetryAfterTransactionalMigrationFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(dir + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	seedVersionThreeStateDB(t, db)
	if _, err := db.Exec(`
		CREATE TRIGGER fail_normalize_miss_action
		BEFORE UPDATE ON platforms
		BEGIN
			SELECT RAISE(ABORT, 'injected migration failure');
		END;
	`); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	if err := MigrateStateDB(db); err == nil {
		t.Fatal("MigrateStateDB unexpectedly succeeded with injected migration failure")
	}

	var version int
	var dirty bool
	if err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read migration version after failed migration: %v", err)
	}
	if dirty || version != stateVersionAddFixedAccountHeader {
		t.Fatalf("failed migration changed version state: version=%d dirty=%v", version, dirty)
	}
	var missAction string
	if err := db.QueryRow(`SELECT reverse_proxy_miss_action FROM platforms WHERE id = 'p1'`).Scan(&missAction); err != nil {
		t.Fatalf("read platform after failed migration: %v", err)
	}
	if missAction != "RANDOM" {
		t.Fatalf("failed migration changed data: got %q, want %q", missAction, "RANDOM")
	}
	if ok, err := hasTableColumn(db, "platforms", "response_rules_json"); err != nil {
		t.Fatalf("check later migration column: %v", err)
	} else if ok {
		t.Fatal("failed migration unexpectedly exposed a later schema column")
	}

	if _, err := db.Exec(`DROP TRIGGER fail_normalize_miss_action`); err != nil {
		t.Fatalf("drop failure trigger: %v", err)
	}

	if err := MigrateStateDB(db); err != nil {
		t.Fatalf("retry after transactional migration failure: %v", err)
	}

	if err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read migration version after retry: %v", err)
	}
	if dirty {
		t.Fatal("migration retry left schema_migrations dirty")
	}
	if version != stateLatestVersion {
		t.Fatalf("migration version after retry: got %d, want %d", version, stateLatestVersion)
	}

	if err := db.QueryRow(`SELECT reverse_proxy_miss_action FROM platforms WHERE id = 'p1'`).Scan(&missAction); err != nil {
		t.Fatalf("read migrated platform: %v", err)
	}
	if missAction != "TREAT_AS_EMPTY" {
		t.Fatalf("migrated miss action: got %q, want %q", missAction, "TREAT_AS_EMPTY")
	}
}

func TestMigrateStateDB_PreservesPreexistingDirtyVersion(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(dir + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE schema_migrations (version uint64 NOT NULL PRIMARY KEY, dirty bool NOT NULL);
		INSERT INTO schema_migrations (version, dirty) VALUES (3, 1);
	`); err != nil {
		t.Fatalf("seed preexisting dirty database: %v", err)
	}

	if err := MigrateStateDB(db); err == nil {
		t.Fatal("MigrateStateDB unexpectedly accepted a preexisting dirty database")
	}
	var version int
	var dirty bool
	if err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read preexisting dirty version: %v", err)
	}
	if version != 3 || !dirty {
		t.Fatalf("preexisting dirty version was rewritten: version=%d dirty=%v", version, dirty)
	}
}

func TestMigrateStateDB_ReportsVersionRestoreFailure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	seedVersionThreeStateDB(t, db)
	if _, err := db.Exec(`
		CREATE TRIGGER fail_normalize_miss_action
		BEFORE UPDATE ON platforms
		BEGIN
			SELECT RAISE(ABORT, 'injected migration failure');
		END;
		CREATE TRIGGER fail_restore_version
		BEFORE DELETE ON schema_migrations
		WHEN OLD.version = 4
		BEGIN
			SELECT RAISE(ABORT, 'injected restore failure');
		END;
	`); err != nil {
		t.Fatalf("install failure triggers: %v", err)
	}

	err = MigrateStateDB(db)
	if err == nil {
		t.Fatal("MigrateStateDB unexpectedly succeeded with failed version restore")
	}
	if !strings.Contains(err.Error(), "injected migration failure") ||
		!strings.Contains(err.Error(), "recover failed migration") ||
		!strings.Contains(err.Error(), "injected restore failure") {
		t.Fatalf("migration error lost original/restore failure: %v", err)
	}
	var version int
	var dirty bool
	if err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read dirty version after failed recovery: %v", err)
	}
	if version != stateVersionNormalizeMissAction || !dirty {
		t.Fatalf("failed recovery did not remain dirty: version=%d dirty=%v", version, dirty)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_restore_version`); err != nil {
		t.Fatalf("drop restore failure trigger: %v", err)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_normalize_miss_action`); err != nil {
		t.Fatalf("drop migration failure trigger: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close database after failed recovery: %v", err)
	}
	reopened, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("reopen database after failed recovery: %v", err)
	}
	defer reopened.Close()
	if err := reopened.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read reopened dirty version: %v", err)
	}
	if version != stateVersionNormalizeMissAction || !dirty {
		t.Fatalf("reopened database changed failed recovery state: version=%d dirty=%v", version, dirty)
	}
}

func TestMigrateStateDB_RetryAfterLaterMigrationFailureKeepsCommittedPredecessors(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(dir + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	seedVersionThreeStateDB(t, db)
	if _, err := db.Exec(`
		ALTER TABLE platforms
		ADD COLUMN passive_circuit_breaker_disabled INTEGER NOT NULL DEFAULT 0;
	`); err != nil {
		t.Fatalf("install migration 6 conflict: %v", err)
	}

	if err := MigrateStateDB(db); err == nil {
		t.Fatal("MigrateStateDB unexpectedly succeeded with migration 6 conflict")
	}

	var version int
	var dirty bool
	if err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read version after later migration failure: %v", err)
	}
	if version != stateVersionAddIncrementalAliveNodes || dirty {
		t.Fatalf("later migration recovery version: got version=%d dirty=%v, want 5/false", version, dirty)
	}
	var missAction string
	if err := db.QueryRow(`SELECT reverse_proxy_miss_action FROM platforms WHERE id = 'p1'`).Scan(&missAction); err != nil {
		t.Fatalf("read committed migration data: %v", err)
	}
	if missAction != "TREAT_AS_EMPTY" {
		t.Fatalf("committed migration data was rolled back: got %q", missAction)
	}
	if ok, err := hasTableColumn(db, "subscriptions", "incremental_alive_nodes"); err != nil || !ok {
		t.Fatalf("committed migration schema missing subscriptions.incremental_alive_nodes: ok=%v err=%v", ok, err)
	}

	// Remove only the injected conflict. Migrations 4 and 5 remain committed.
	if _, err := db.Exec(`
		ALTER TABLE platforms RENAME TO platforms_with_injected_column;
		CREATE TABLE platforms (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			sticky_ttl_ns INTEGER NOT NULL,
			regex_filters_json TEXT NOT NULL DEFAULT '[]',
			region_filters_json TEXT NOT NULL DEFAULT '[]',
			reverse_proxy_miss_action TEXT NOT NULL DEFAULT 'TREAT_AS_EMPTY',
			reverse_proxy_empty_account_behavior TEXT NOT NULL DEFAULT 'RANDOM',
			reverse_proxy_fixed_account_header TEXT NOT NULL DEFAULT '',
			allocation_policy TEXT NOT NULL DEFAULT 'BALANCED',
			updated_at_ns INTEGER NOT NULL
		);
		INSERT INTO platforms (
			id, name, sticky_ttl_ns, regex_filters_json, region_filters_json,
			reverse_proxy_miss_action, reverse_proxy_empty_account_behavior,
			reverse_proxy_fixed_account_header, allocation_policy, updated_at_ns
		)
		SELECT id, name, sticky_ttl_ns, regex_filters_json, region_filters_json,
			reverse_proxy_miss_action, reverse_proxy_empty_account_behavior,
			reverse_proxy_fixed_account_header, allocation_policy, updated_at_ns
		FROM platforms_with_injected_column;
		DROP TABLE platforms_with_injected_column;
	`); err != nil {
		t.Fatalf("remove migration 6 conflict: %v", err)
	}

	if err := MigrateStateDB(db); err != nil {
		t.Fatalf("retry after later migration failure: %v", err)
	}
	if err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read final version: %v", err)
	}
	if version != stateLatestVersion || dirty {
		t.Fatalf("final migration version: got version=%d dirty=%v", version, dirty)
	}
}

func TestPersistenceBootstrap_MigrationFailureReleasesHandlesForRetry(t *testing.T) {
	stateDir := t.TempDir()
	cacheDir := t.TempDir()
	statePath := filepath.Join(stateDir, "state.db")

	db, err := OpenDB(statePath)
	if err != nil {
		t.Fatal(err)
	}
	seedVersionThreeStateDB(t, db)
	if _, err := db.Exec(`
		CREATE TRIGGER fail_normalize_miss_action
		BEFORE UPDATE ON platforms
		BEGIN
			SELECT RAISE(ABORT, 'injected bootstrap migration failure');
		END;
	`); err != nil {
		db.Close()
		t.Fatalf("install bootstrap failure trigger: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seeded state database: %v", err)
	}

	if _, closer, err := PersistenceBootstrap(stateDir, cacheDir); err == nil || closer != nil {
		t.Fatalf("failed bootstrap result: err=%v closer=%v", err, closer)
	}

	db, err = OpenDB(statePath)
	if err != nil {
		t.Fatalf("reopen state database after failed bootstrap: %v", err)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_normalize_miss_action`); err != nil {
		db.Close()
		t.Fatalf("remove bootstrap failure trigger: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close repaired state database: %v", err)
	}

	engine, closer, err := PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("retry bootstrap: %v", err)
	}
	if engine == nil || closer == nil {
		t.Fatal("retry bootstrap returned incomplete resources")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close retried bootstrap: %v", err)
	}
}

func seedVersionThreeStateDB(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE schema_migrations (version uint64 NOT NULL PRIMARY KEY, dirty bool NOT NULL);
		INSERT INTO schema_migrations (version, dirty) VALUES (3, 0);
		CREATE TABLE platforms (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			sticky_ttl_ns INTEGER NOT NULL,
			regex_filters_json TEXT NOT NULL DEFAULT '[]',
			region_filters_json TEXT NOT NULL DEFAULT '[]',
			reverse_proxy_miss_action TEXT NOT NULL DEFAULT 'RANDOM',
			reverse_proxy_empty_account_behavior TEXT NOT NULL DEFAULT 'RANDOM',
			reverse_proxy_fixed_account_header TEXT NOT NULL DEFAULT '',
			allocation_policy TEXT NOT NULL DEFAULT 'BALANCED',
			updated_at_ns INTEGER NOT NULL
		);
		CREATE TABLE subscriptions (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			source_type TEXT NOT NULL DEFAULT 'remote',
			url TEXT NOT NULL,
			content TEXT NOT NULL DEFAULT '',
			update_interval_ns INTEGER NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			ephemeral INTEGER NOT NULL DEFAULT 0,
			ephemeral_node_evict_delay_ns INTEGER NOT NULL,
			created_at_ns INTEGER NOT NULL,
			updated_at_ns INTEGER NOT NULL
		);
		INSERT INTO platforms (
			id, name, sticky_ttl_ns, regex_filters_json, region_filters_json,
			reverse_proxy_miss_action, reverse_proxy_empty_account_behavior,
			reverse_proxy_fixed_account_header, allocation_policy, updated_at_ns
		) VALUES ('p1', 'p1', 1, '[]', '[]', 'RANDOM', 'RANDOM', '', 'BALANCED', 1);
	`); err != nil {
		t.Fatalf("seed version 3 database: %v", err)
	}
}
