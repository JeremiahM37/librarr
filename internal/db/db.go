// Package db provides SQLite persistence for librarr: users, library
// items, download jobs, requests, and related records.
package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// DB wraps a SQLite database for library tracking and download jobs.
type DB struct {
	db   *sql.DB
	mu   sync.Mutex
	path string
}

// New opens (or creates) the SQLite database at the given path.
func New(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	// modernc.org/sqlite reads DSN options as _pragma=NAME(VALUE), applied
	// to every pooled connection. The mattn-style keys used before
	// (_journal_mode=WAL&_busy_timeout=10000) were silently ignored, which
	// left the file in rollback-journal mode with a zero busy timeout: a
	// read on one pooled connection made a concurrent write fail at once
	// with "database is locked", and callers that dropped the error (an
	// import racing the scheduler) lost the write without a trace.
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	d := &DB{db: db, path: path}
	if err := d.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	slog.Info("database initialized", "path", path)
	return d, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS library_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			author TEXT NOT NULL DEFAULT '',
			file_path TEXT NOT NULL DEFAULT '',
			original_path TEXT NOT NULL DEFAULT '',
			file_size INTEGER NOT NULL DEFAULT 0,
			file_format TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			source TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			metadata TEXT NOT NULL DEFAULT '{}',
			content_hash TEXT NOT NULL DEFAULT '',
			added_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS activity_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_type TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT '',
			library_item_id INTEGER,
			job_id TEXT NOT NULL DEFAULT '',
			user TEXT NOT NULL DEFAULT '',
			timestamp REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS download_jobs (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL DEFAULT '',
				source TEXT NOT NULL DEFAULT '',
				status TEXT NOT NULL DEFAULT 'queued',
				detail TEXT NOT NULL DEFAULT '',
				error TEXT NOT NULL DEFAULT '',
				url TEXT NOT NULL DEFAULT '',
				md5 TEXT NOT NULL DEFAULT '',
				source_id TEXT NOT NULL DEFAULT '',
				media_type TEXT NOT NULL DEFAULT 'ebook',
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 2,
			status_history TEXT NOT NULL DEFAULT '[]',
			created_at REAL NOT NULL DEFAULT (strftime('%s','now')),
			updated_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS wishlist (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			author TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			added_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS nzb_jobs (
			nzo_id TEXT PRIMARY KEY,
			title TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			imported INTEGER NOT NULL DEFAULT 0,
			created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_library_items_source_id ON library_items(source_id)`,
		`CREATE INDEX IF NOT EXISTS idx_library_items_media_type ON library_items(media_type)`,
		`CREATE INDEX IF NOT EXISTS idx_activity_log_timestamp ON activity_log(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs(status)`,
	}

	// Users table for multi-user auth.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT UNIQUE NOT NULL,
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'user',
		totp_secret TEXT,
		totp_enabled INTEGER DEFAULT 0,
		created_at REAL NOT NULL DEFAULT (strftime('%s','now')),
		last_login REAL
	)`)
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS backup_codes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		code_hash TEXT NOT NULL,
		used INTEGER DEFAULT 0,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`)

	// Requests table for book request workflow.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS requests (
		id TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		username TEXT NOT NULL,
		title TEXT NOT NULL,
		author TEXT,
		book_type TEXT NOT NULL DEFAULT 'ebook',
		status TEXT NOT NULL DEFAULT 'pending',
		cover_url TEXT,
		description TEXT,
		year TEXT,
		series_name TEXT,
		series_position TEXT,
		search_query TEXT,
		selected_result_id TEXT,
		download_id TEXT,
		attention_note TEXT,
		auto_approved INTEGER DEFAULT 0,
		retry_count INTEGER DEFAULT 0,
		created_at REAL NOT NULL,
		updated_at REAL NOT NULL
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_requests_user_id ON requests(user_id)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status)`)

	// Notifications table for in-app notifications.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS notifications (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		type TEXT NOT NULL,
		title TEXT NOT NULL,
		message TEXT,
		request_id TEXT,
		read INTEGER DEFAULT 0,
		created_at REAL NOT NULL
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_notifications_read ON notifications(user_id, read)`)

	// Uploads table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS uploads (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user TEXT NOT NULL DEFAULT '',
		filename TEXT NOT NULL DEFAULT '',
		original_name TEXT NOT NULL DEFAULT '',
		file_type TEXT NOT NULL DEFAULT '',
		file_size INTEGER NOT NULL DEFAULT 0,
		organized_to TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending',
		error TEXT NOT NULL DEFAULT '',
		created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_uploads_created ON uploads(created_at)`)

	// Webhook configs table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS webhook_configs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		url TEXT NOT NULL,
		type TEXT NOT NULL DEFAULT 'generic',
		enabled INTEGER NOT NULL DEFAULT 1,
		events TEXT NOT NULL DEFAULT '*'
	)`)

	// Reading history table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS reading_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL DEFAULT 0,
		book_title TEXT NOT NULL DEFAULT '',
		author TEXT NOT NULL DEFAULT '',
		format TEXT NOT NULL DEFAULT '',
		started_at REAL,
		finished_at REAL,
		rating INTEGER,
		notes TEXT NOT NULL DEFAULT '',
		library_item_id INTEGER,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_reading_history_user ON reading_history(user_id)`)

	// Series tracking table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS series_tracking (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		series_name TEXT NOT NULL UNIQUE,
		known_total INTEGER NOT NULL DEFAULT 0,
		owned_count INTEGER NOT NULL DEFAULT 0,
		last_checked REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)

	// Quality profiles table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS quality_profiles (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		format_ranking TEXT NOT NULL DEFAULT '[]',
		preferred_size_min INTEGER NOT NULL DEFAULT 0,
		preferred_size_max INTEGER NOT NULL DEFAULT 0,
		upgrade_allowed INTEGER NOT NULL DEFAULT 0,
		cutoff_format TEXT NOT NULL DEFAULT ''
	)`)

	// Blocklist table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS blocklist (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT NOT NULL DEFAULT '',
		source TEXT NOT NULL DEFAULT '',
		download_url TEXT NOT NULL DEFAULT '',
		info_hash TEXT NOT NULL DEFAULT '',
		reason TEXT NOT NULL DEFAULT '',
		created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_blocklist_info_hash ON blocklist(info_hash)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_blocklist_download_url ON blocklist(download_url)`)

	// Release profiles table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS release_profiles (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		must_contain TEXT NOT NULL DEFAULT '[]',
		must_not_contain TEXT NOT NULL DEFAULT '[]',
		preferred TEXT NOT NULL DEFAULT '[]',
		enabled INTEGER NOT NULL DEFAULT 1
	)`)

	// Tags tables.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS tags (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		color TEXT NOT NULL DEFAULT '#6366f1'
	)`)
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS item_tags (
		item_id INTEGER NOT NULL,
		tag_id INTEGER NOT NULL,
		PRIMARY KEY (item_id, tag_id),
		FOREIGN KEY (item_id) REFERENCES library_items(id) ON DELETE CASCADE,
		FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_item_tags_tag ON item_tags(tag_id)`)

	// Monitored authors table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS monitored_authors (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		last_checked REAL NOT NULL DEFAULT 0,
		last_book_found TEXT NOT NULL DEFAULT '',
		check_interval_days INTEGER NOT NULL DEFAULT 7
	)`)

	// Invite codes for secure self-registration.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS invite_codes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		code TEXT UNIQUE NOT NULL,
		created_by INTEGER NOT NULL,
		role TEXT NOT NULL DEFAULT 'user',
		max_uses INTEGER NOT NULL DEFAULT 1,
		uses INTEGER NOT NULL DEFAULT 0,
		expires_at REAL,
		created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)

	for _, m := range migrations {
		if _, err := d.db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}

	// Additive migrations — add columns that may not exist on databases
	// created by older versions of the schema. Run AFTER the CREATE TABLE
	// migrations so the target tables already exist; otherwise the ALTER
	// silently no-ops on a fresh DB and a later INSERT against the column
	// fails. Duplicate-column errors are expected on already-upgraded DBs
	// and are ignored.
	addColumns := []string{
		`ALTER TABLE library_items ADD COLUMN content_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_jobs ADD COLUMN status_history TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE download_jobs ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE activity_log ADD COLUMN user TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE reading_history ADD COLUMN status TEXT NOT NULL DEFAULT ''`,

		// Wanted-list state machine (wishlist rows became persistent,
		// monitored entities with a quality profile instead of one-shot
		// requests that were deleted on grab).
		`ALTER TABLE wishlist ADD COLUMN monitored INTEGER NOT NULL DEFAULT 1`,
		`ALTER TABLE wishlist ADD COLUMN quality_profile_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE wishlist ADD COLUMN library_item_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE wishlist ADD COLUMN active_job_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE wishlist ADD COLUMN last_searched REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE wishlist ADD COLUMN last_result TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE wishlist ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'`,
		// A grab links back to the wanted row it satisfies.
		`ALTER TABLE download_jobs ADD COLUMN wanted_id INTEGER NOT NULL DEFAULT 0`,
		// Profiles are per media type; built-in ones cannot be deleted.
		`ALTER TABLE quality_profiles ADD COLUMN media_type TEXT NOT NULL DEFAULT 'ebook'`,
		`ALTER TABLE quality_profiles ADD COLUMN builtin INTEGER NOT NULL DEFAULT 0`,
		// Author monitoring can add new works to the wanted list.
		`ALTER TABLE monitored_authors ADD COLUMN auto_add INTEGER NOT NULL DEFAULT 1`,
	}
	for _, stmt := range addColumns {
		if _, err := d.db.Exec(stmt); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("additive migration failed: %w\nSQL: %s", err, stmt)
		}
	}
	if _, err := d.db.Exec(`CREATE INDEX IF NOT EXISTS idx_library_items_content_hash ON library_items(content_hash)`); err != nil {
		return fmt.Errorf("create library content hash index: %w", err)
	}
	// Works an author monitor has already seen, keyed by Open Library work
	// key so a reissued edition of an old book is not mistaken for a release.
	postMigrations := []string{
		`CREATE TABLE IF NOT EXISTS author_seen_works (
			author_id INTEGER NOT NULL,
			work_key TEXT NOT NULL,
			title TEXT NOT NULL DEFAULT '',
			year INTEGER NOT NULL DEFAULT 0,
			first_seen REAL NOT NULL DEFAULT (strftime('%s','now')),
			PRIMARY KEY (author_id, work_key),
			FOREIGN KEY (author_id) REFERENCES monitored_authors(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_wishlist_active_job ON wishlist(active_job_id)`,
		`CREATE INDEX IF NOT EXISTS idx_wishlist_library_item ON wishlist(library_item_id)`,
	}
	for _, m := range postMigrations {
		if _, err := d.db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}
	if err := d.seedDefaultQualityProfiles(); err != nil {
		return fmt.Errorf("seed quality profiles: %w", err)
	}
	if err := d.backfillLibraryContentHashes(); err != nil {
		return fmt.Errorf("backfill library content hashes: %w", err)
	}

	return nil
}

func (d *DB) GetDBPath() string {
	return d.path
}

// ItemToJSON converts a LibraryItem to a JSON-friendly map.
