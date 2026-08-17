package state

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	migratedb "github.com/golang-migrate/migrate/v4/database"
	migratesqlite "github.com/golang-migrate/migrate/v4/database/sqlite"
	migratesource "github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

const (
	stateMigrationsPath = "migrations/state"
	cacheMigrationsPath = "migrations/cache"

	// Keep these version markers in sync with SQL files under migrations/state/.
	// stateLegacyBaselineVersion must remain fixed to the highest migration
	// version covered by compatibility detection for pre-migrate databases.
	stateVersionBaseSchema                       = 1
	stateVersionAddEmptyAccountBehavior          = 2
	stateVersionAddFixedAccountHeader            = 3
	stateVersionNormalizeMissAction              = 4
	stateVersionAddIncrementalAliveNodes         = 5
	stateVersionAddPassiveCircuitBreakerDisabled = 6
	stateVersionAddEndpoints                     = 7
	stateVersionAddEndpointEnabled               = 8
	stateVersionPlatformRegexFilterRules         = 9
	stateVersionPlatformResponseRules            = 10
	stateLatestVersion                           = stateVersionPlatformResponseRules
	stateLegacyBaselineVersion                   = stateVersionAddFixedAccountHeader

	stateBaseSchemaMigration = stateMigrationsPath + "/000001_state_base.up.sql"
)

//go:embed migrations/state/*.sql migrations/cache/*.sql
var migrationsFS embed.FS

type preMigrateHook func(db *sql.DB, driver migratedb.Driver) error

// MigrateStateDB applies state.db migrations.
func MigrateStateDB(db *sql.DB) error {
	return migrateSQLiteDB(db, stateMigrationsPath, migrateDefaultTable, prepareLegacyStateBaseline)
}

// MigrateCacheDB applies cache.db migrations.
func MigrateCacheDB(db *sql.DB) error {
	return migrateSQLiteDB(db, cacheMigrationsPath, migrateDefaultTable, nil)
}

const migrateDefaultTable = "schema_migrations"

func migrateSQLiteDB(db *sql.DB, fsPath, migrationsTable string, preHook preMigrateHook) error {
	if db == nil {
		return fmt.Errorf("migrate %s: nil db", fsPath)
	}

	sourceDriver, err := iofs.New(migrationsFS, fsPath)
	if err != nil {
		return fmt.Errorf("migrate %s: init source: %w", fsPath, err)
	}

	dbDriver, err := migratesqlite.WithInstance(db, &migratesqlite.Config{
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return fmt.Errorf("migrate %s: init db driver: %w", fsPath, err)
	}

	if preHook != nil {
		if err := preHook(db, dbDriver); err != nil {
			return fmt.Errorf("migrate %s: prehook: %w", fsPath, err)
		}
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite", dbDriver)
	if err != nil {
		return fmt.Errorf("migrate %s: init migrator: %w", fsPath, err)
	}

	_, initialDirty, err := dbDriver.Version()
	if err != nil {
		return fmt.Errorf("migrate %s: read initial version: %w", fsPath, err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		if !initialDirty {
			if recoveryErr := recoverFailedMigration(db, dbDriver, sourceDriver, migrationsTable); recoveryErr != nil {
				return fmt.Errorf(
					"migrate %s: %w",
					fsPath,
					errors.Join(err, fmt.Errorf("recover failed migration: %w", recoveryErr)),
				)
			}
		}
		return fmt.Errorf("migrate %s: up: %w", fsPath, err)
	}
	return nil
}

// recoverFailedMigration is only used after golang-migrate's SQLite Run
// returned an error. Its Run path uses the driver's transaction wrapper, so
// the schema is at the predecessor of the dirty version. The version-table
// repair itself is kept local so every failure rolls back and leaves the DB
// dirty rather than pretending a partial migration is clean.
func recoverFailedMigration(
	db *sql.DB,
	driver migratedb.Driver,
	sourceDriver migratesource.Driver,
	migrationsTable string,
) error {
	version, dirty, err := driver.Version()
	if err != nil {
		return fmt.Errorf("read dirty version: %w", err)
	}
	if !dirty {
		return nil
	}

	previousVersion := migratedb.NilVersion
	if version >= 0 {
		prev, err := sourceDriver.Prev(uint(version))
		if errors.Is(err, os.ErrNotExist) {
			// The failed migration was the first one.
		} else if err != nil {
			return fmt.Errorf("find predecessor of version %d: %w", version, err)
		} else {
			previousVersion = int(prev)
		}
	}

	return writeCleanMigrationVersion(db, migrationsTable, previousVersion)
}

func writeCleanMigrationVersion(db *sql.DB, migrationsTable string, version int) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin version recovery: %w", err)
	}

	rollback := func(cause error) error {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			cause = errors.Join(cause, rollbackErr)
		}
		return cause
	}

	if _, err := tx.Exec("DELETE FROM " + migrationsTable); err != nil {
		return rollback(fmt.Errorf("delete dirty version: %w", err))
	}
	if version >= 0 {
		if _, err := tx.Exec(
			fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, migrationsTable),
			version,
			false,
		); err != nil {
			return rollback(fmt.Errorf("write clean version %d: %w", version, err))
		}
	}
	if err := tx.Commit(); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
		return fmt.Errorf("commit version recovery: %w", err)
	}
	return nil
}

// prepareLegacyStateBaseline aligns migration version metadata for databases
// created before golang-migrate was introduced.
func prepareLegacyStateBaseline(db *sql.DB, driver migratedb.Driver) error {
	hasVersion, err := hasMigrationVersion(db, migrateDefaultTable)
	if err != nil {
		return err
	}
	if hasVersion {
		return nil
	}

	hasPlatforms, err := hasTable(db, "platforms")
	if err != nil {
		return err
	}
	if !hasPlatforms {
		return nil
	}

	hasEmptyBehavior, err := hasTableColumn(db, "platforms", "reverse_proxy_empty_account_behavior")
	if err != nil {
		return err
	}
	hasFixedHeader, err := hasTableColumn(db, "platforms", "reverse_proxy_fixed_account_header")
	if err != nil {
		return err
	}
	hasPassiveCircuitBreakerDisabled, err := hasTableColumn(db, "platforms", "passive_circuit_breaker_disabled")
	if err != nil {
		return err
	}
	hasIncrementalAliveNodes, err := hasTableColumn(db, "subscriptions", "incremental_alive_nodes")
	if err != nil {
		return err
	}

	switch {
	case hasEmptyBehavior && hasFixedHeader && hasIncrementalAliveNodes && hasPassiveCircuitBreakerDisabled:
		return setLegacyMigrationVersion(db, driver, stateVersionAddPassiveCircuitBreakerDisabled)
	case hasEmptyBehavior && hasFixedHeader && hasIncrementalAliveNodes:
		return setLegacyMigrationVersion(db, driver, stateVersionAddIncrementalAliveNodes)
	case hasEmptyBehavior && hasFixedHeader:
		return setLegacyMigrationVersion(db, driver, stateLegacyBaselineVersion)
	case hasEmptyBehavior && !hasFixedHeader:
		return setLegacyMigrationVersion(db, driver, stateVersionAddEmptyAccountBehavior)
	case !hasEmptyBehavior && hasFixedHeader:
		// This mixed state should not happen in normal upgrades. Repair it once.
		if err := ensureTableColumn(
			db,
			"platforms",
			"reverse_proxy_empty_account_behavior",
			`reverse_proxy_empty_account_behavior TEXT NOT NULL DEFAULT 'RANDOM'`,
		); err != nil {
			return err
		}
		return setLegacyMigrationVersion(db, driver, stateLegacyBaselineVersion)
	default:
		// No baseline metadata: migrate from base schema.
		return nil
	}
}

func hasMigrationVersion(db *sql.DB, table string) (bool, error) {
	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		return false, fmt.Errorf("read %s: %w", table, err)
	}
	return count > 0, nil
}

func setMigrationVersion(driver migratedb.Driver, version int) error {
	if err := driver.SetVersion(version, false); err != nil {
		return fmt.Errorf("set migration version=%d: %w", version, err)
	}
	return nil
}

func setLegacyMigrationVersion(db *sql.DB, driver migratedb.Driver, version int) error {
	if err := ensureStateBaseSchema(db); err != nil {
		return err
	}
	return setMigrationVersion(driver, version)
}

func ensureStateBaseSchema(db *sql.DB) error {
	schema, err := migrationsFS.ReadFile(stateBaseSchemaMigration)
	if err != nil {
		return fmt.Errorf("read state base schema: %w", err)
	}
	if _, err := db.Exec(string(schema)); err != nil {
		return fmt.Errorf("ensure state base schema: %w", err)
	}
	return nil
}

func hasTable(db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup table %s: %w", table, err)
	}
	return true, nil
}
