package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
)

type schemaMigration struct {
	version int
	name    string
	run     func(*sql.Tx) error
}

var versionedMigrations = []schemaMigration{
	{version: 1, name: "librarr_2_schema_foundation", run: migrateLibrarr2SchemaFoundation},
	{version: 2, name: "librarr_2_file_metadata_json", run: migrateLibrarr2FileMetadataJSON},
	{version: 3, name: "librarr_2_backfill_state", run: migrateLibrarr2BackfillState},
	{version: 4, name: "librarr_2_metadata_provenance", run: migrateLibrarr2MetadataProvenance},
	{version: 5, name: "librarr_2_wanted_books", run: migrateLibrarr2WantedBooks},
}

func (d *DB) runVersionedMigrations() error {
	if _, err := d.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		applied_at DATETIME NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := d.appliedSchemaMigrations()
	if err != nil {
		return err
	}

	migrations := append([]schemaMigration(nil), versionedMigrations...)
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	seen := make(map[int]bool, len(migrations))
	for _, migration := range migrations {
		if seen[migration.version] {
			return fmt.Errorf("duplicate schema migration version %d", migration.version)
		}
		seen[migration.version] = true
		if applied[migration.version] {
			continue
		}
		if err := d.applySchemaMigration(migration); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) appliedSchemaMigrations() (map[int]bool, error) {
	rows, err := d.db.Query("SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("load schema migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

func (d *DB) applySchemaMigration(migration schemaMigration) error {
	slog.Info("applying schema migration", "version", migration.version, "name", migration.name)
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin schema migration %d: %w", migration.version, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := migration.run(tx); err != nil {
		return fmt.Errorf("apply schema migration %d %s: %w", migration.version, migration.name, err)
	}
	if _, err := tx.Exec(
		"INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, datetime('now'))",
		migration.version,
		migration.name,
	); err != nil {
		return fmt.Errorf("record schema migration %d: %w", migration.version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migration %d: %w", migration.version, err)
	}
	committed = true
	slog.Info("schema migration applied", "version", migration.version, "name", migration.name)
	return nil
}

func migrateLibrarr2SchemaFoundation(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			sort_title TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			publication_year INTEGER,
			language TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			monitored INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_books_title ON books(title)`,
		`CREATE INDEX IF NOT EXISTS idx_books_sort_title ON books(sort_title)`,
		`CREATE INDEX IF NOT EXISTS idx_books_media_type ON books(media_type)`,

		`CREATE TABLE IF NOT EXISTS editions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			book_id INTEGER NOT NULL,
			title TEXT NOT NULL DEFAULT '',
			subtitle TEXT NOT NULL DEFAULT '',
			publisher TEXT NOT NULL DEFAULT '',
			publication_date TEXT NOT NULL DEFAULT '',
			language TEXT NOT NULL DEFAULT '',
			page_count INTEGER,
			edition_name TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_editions_book_id ON editions(book_id)`,

		`CREATE TABLE IF NOT EXISTS contributors (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL DEFAULT '',
			sort_name TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_contributors_name ON contributors(name)`,
		`CREATE INDEX IF NOT EXISTS idx_contributors_sort_name ON contributors(sort_name)`,

		`CREATE TABLE IF NOT EXISTS edition_contributors (
			edition_id INTEGER NOT NULL,
			contributor_id INTEGER NOT NULL,
			role TEXT NOT NULL DEFAULT 'author',
			position INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (edition_id, contributor_id, role),
			FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE,
			FOREIGN KEY (contributor_id) REFERENCES contributors(id) ON DELETE RESTRICT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_edition_contributors_edition ON edition_contributors(edition_id)`,
		`CREATE INDEX IF NOT EXISTS idx_edition_contributors_contributor ON edition_contributors(contributor_id)`,

		`CREATE TABLE IF NOT EXISTS series (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_series_title ON series(title)`,

		`CREATE TABLE IF NOT EXISTS book_series (
			book_id INTEGER NOT NULL,
			series_id INTEGER NOT NULL,
			position REAL,
			display_position TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (book_id, series_id),
			FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
			FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE RESTRICT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_book_series_book ON book_series(book_id)`,
		`CREATE INDEX IF NOT EXISTS idx_book_series_series ON book_series(series_id)`,

		`CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			edition_id INTEGER NOT NULL,
			media_type TEXT NOT NULL DEFAULT 'ebook',
			format TEXT NOT NULL DEFAULT '',
			file_path TEXT,
			original_path TEXT NOT NULL DEFAULT '',
			file_size INTEGER NOT NULL DEFAULT 0,
			content_hash TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			source_type TEXT NOT NULL DEFAULT '',
			quality TEXT NOT NULL DEFAULT '',
			is_managed INTEGER NOT NULL DEFAULT 1,
			imported_at DATETIME,
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_files_file_path_unique ON files(file_path) WHERE file_path IS NOT NULL AND file_path <> ''`,
		`CREATE INDEX IF NOT EXISTS idx_files_edition_id ON files(edition_id)`,
		`CREATE INDEX IF NOT EXISTS idx_files_media_type ON files(media_type)`,
		`CREATE INDEX IF NOT EXISTS idx_files_format ON files(format)`,
		`CREATE INDEX IF NOT EXISTS idx_files_content_hash ON files(content_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_files_source_id ON files(source_id)`,

		`CREATE TABLE IF NOT EXISTS identifiers (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			book_id INTEGER,
			edition_id INTEGER,
			provider TEXT NOT NULL DEFAULT '',
			identifier TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			CHECK ((book_id IS NOT NULL AND edition_id IS NULL) OR (book_id IS NULL AND edition_id IS NOT NULL)),
			FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
			FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_identifiers_provider_identifier ON identifiers(provider, identifier)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_identifiers_book_unique ON identifiers(book_id, provider, identifier) WHERE book_id IS NOT NULL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_identifiers_edition_unique ON identifiers(edition_id, provider, identifier) WHERE edition_id IS NOT NULL`,

		`CREATE TABLE IF NOT EXISTS covers (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			book_id INTEGER,
			edition_id INTEGER,
			source TEXT NOT NULL DEFAULT '',
			source_url TEXT NOT NULL DEFAULT '',
			local_path TEXT NOT NULL DEFAULT '',
			mime_type TEXT NOT NULL DEFAULT '',
			width INTEGER,
			height INTEGER,
			is_primary INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
			CHECK ((book_id IS NOT NULL AND edition_id IS NULL) OR (book_id IS NULL AND edition_id IS NOT NULL)),
			FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
			FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_covers_book_id ON covers(book_id)`,
		`CREATE INDEX IF NOT EXISTS idx_covers_edition_id ON covers(edition_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_covers_primary_book ON covers(book_id) WHERE book_id IS NOT NULL AND is_primary = 1`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_covers_primary_edition ON covers(edition_id) WHERE edition_id IS NOT NULL AND is_primary = 1`,

		`CREATE TABLE IF NOT EXISTS library_item_migration_map (
			library_item_id INTEGER PRIMARY KEY,
			book_id INTEGER,
			edition_id INTEGER,
			file_id INTEGER,
			status TEXT NOT NULL,
			reason TEXT,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			FOREIGN KEY (library_item_id) REFERENCES library_items(id) ON DELETE CASCADE,
			FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE SET NULL,
			FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE SET NULL,
			FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_library_item_migration_map_status ON library_item_migration_map(status)`,
	}

	for _, stmt := range statements {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("execute statement: %w\nSQL: %s", err, stmt)
		}
	}
	return nil
}

func migrateLibrarr2FileMetadataJSON(tx *sql.Tx) error {
	exists, err := columnExistsInTx(tx, "files", "embedded_metadata_json")
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = tx.Exec(`ALTER TABLE files ADD COLUMN embedded_metadata_json TEXT NOT NULL DEFAULT '{}'`)
	if err != nil {
		return fmt.Errorf("add files.embedded_metadata_json: %w", err)
	}
	return nil
}

func migrateLibrarr2MetadataProvenance(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS metadata_values (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scope_type TEXT NOT NULL,
			scope_id INTEGER NOT NULL,
			field TEXT NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			confidence TEXT NOT NULL DEFAULT 'none',
			manual_override INTEGER NOT NULL DEFAULT 0,
			updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			UNIQUE(scope_type, scope_id, field)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_metadata_values_scope ON metadata_values(scope_type, scope_id)`,
		`CREATE INDEX IF NOT EXISTS idx_metadata_values_field ON metadata_values(field)`,
		`CREATE TABLE IF NOT EXISTS metadata_evidence (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scope_type TEXT NOT NULL,
			scope_id INTEGER NOT NULL,
			field TEXT NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			confidence TEXT NOT NULL DEFAULT 'none',
			manual_override INTEGER NOT NULL DEFAULT 0,
			selected INTEGER NOT NULL DEFAULT 0,
			updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			UNIQUE(scope_type, scope_id, field, value, source)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_metadata_evidence_scope ON metadata_evidence(scope_type, scope_id)`,
		`CREATE INDEX IF NOT EXISTS idx_metadata_evidence_field ON metadata_evidence(field)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func migrateLibrarr2WantedBooks(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS wanted_books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			author TEXT NOT NULL DEFAULT '',
			isbn TEXT NOT NULL DEFAULT '',
			asin TEXT NOT NULL DEFAULT '',
			series TEXT NOT NULL DEFAULT '',
			publisher TEXT NOT NULL DEFAULT '',
			language TEXT NOT NULL DEFAULT '',
			cover_url TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			monitored INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'wanted',
			added_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_wanted_books_status ON wanted_books(status)`,
		`CREATE INDEX IF NOT EXISTS idx_wanted_books_monitored ON wanted_books(monitored)`,
		`CREATE INDEX IF NOT EXISTS idx_wanted_books_title ON wanted_books(title)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_wanted_books_identity ON wanted_books(
			lower(trim(title)),
			lower(trim(author)),
			lower(trim(isbn)),
			lower(trim(asin)),
			lower(trim(media_type))
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func columnExistsInTx(tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func migrateLibrarr2BackfillState(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS backfill_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			version TEXT NOT NULL DEFAULT '',
			started_at DATETIME NOT NULL DEFAULT (datetime('now')),
			completed_at DATETIME,
			status TEXT NOT NULL DEFAULT 'running',
			dry_run INTEGER NOT NULL DEFAULT 0,
			rows_processed INTEGER NOT NULL DEFAULT 0,
			rows_migrated INTEGER NOT NULL DEFAULT 0,
			rows_skipped INTEGER NOT NULL DEFAULT 0,
			errors INTEGER NOT NULL DEFAULT 0,
			resume_checkpoint INTEGER NOT NULL DEFAULT 0,
			report_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE INDEX IF NOT EXISTS idx_backfill_runs_status ON backfill_runs(status)`,
		`CREATE TABLE IF NOT EXISTS backfill_state (
			legacy_item_id INTEGER PRIMARY KEY,
			run_id INTEGER,
			status TEXT NOT NULL DEFAULT 'pending',
			book_id INTEGER,
			edition_id INTEGER,
			file_id INTEGER,
			error TEXT NOT NULL DEFAULT '',
			updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (run_id) REFERENCES backfill_runs(id) ON DELETE SET NULL,
			FOREIGN KEY (legacy_item_id) REFERENCES library_items(id) ON DELETE CASCADE,
			FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE SET NULL,
			FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE SET NULL,
			FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_backfill_state_status ON backfill_state(status)`,
	}
	for _, stmt := range statements {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("execute statement: %w\nSQL: %s", err, stmt)
		}
	}
	return nil
}
