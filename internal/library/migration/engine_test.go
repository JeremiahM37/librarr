package migration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jamie75/librarr/internal/library"
	_ "modernc.org/sqlite"
)

func TestEngineEmptyDatabase(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()

	report, err := engine.Run(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.LegacyRowsTotal != 0 || report.RowsProcessed != 0 {
		t.Fatalf("report = %+v", report)
	}
	assertCount(t, db, "books", 0)
	assertCount(t, db, "backfill_runs", 1)
}

func TestEngineDryRunWritesNothing(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{"title":"Dune"}`)

	report, err := engine.DryRun(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || report.RowsProcessed != 1 || report.BooksCreated != 1 {
		t.Fatalf("report = %+v", report)
	}
	assertCount(t, db, "books", 0)
	assertCount(t, db, "files", 0)
	assertCount(t, db, "library_item_migration_map", 0)
	assertCount(t, db, "backfill_runs", 0)
}

func TestEngineRunIsIdempotent(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{"title":"Dune"}`)

	first, err := engine.Run(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Run(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if first.RowsCompleted != 1 || second.RowsCompleted != 1 || second.DuplicatesMerged != 1 {
		t.Fatalf("first = %+v second = %+v", first, second)
	}
	assertCount(t, db, "books", 1)
	assertCount(t, db, "editions", 1)
	assertCount(t, db, "files", 1)
	assertCount(t, db, "contributors", 1)
	assertCount(t, db, "identifiers", 1)
	assertCount(t, db, "library_item_migration_map", 1)

	var metadata string
	if err := db.QueryRow(`SELECT embedded_metadata_json FROM files LIMIT 1`).Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	if metadata != `{"title":"Dune"}` {
		t.Fatalf("metadata = %q", metadata)
	}
}

func TestEngineDuplicateLegacyRowsReuseNormalizedFile(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{}`)
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{}`)

	report, err := engine.Run(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.RowsCompleted != 2 || report.FilesCreated != 1 || report.FilesReused != 1 {
		t.Fatalf("report = %+v", report)
	}
	assertCount(t, db, "books", 1)
	assertCount(t, db, "files", 1)
	assertCount(t, db, "library_item_migration_map", 2)
}

func TestEngineResumeAfterInterruptedRun(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{}`)
	insertLegacy(t, db, "Hyperion", "Dan Simmons", "/books/hyperion.epub", "epub", "ebook", "torrent", "hash-2", `{}`)

	partial, err := engine.Run(context.Background(), Options{StopAfterRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	if partial.RowsProcessed != 1 {
		t.Fatalf("partial = %+v", partial)
	}
	full, err := engine.Resume(context.Background(), partial.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if full.RowsProcessed != 2 || full.RowsCompleted != 2 {
		t.Fatalf("full = %+v", full)
	}
	assertCount(t, db, "books", 2)
	assertCount(t, db, "files", 2)
	assertCount(t, db, "library_item_migration_map", 2)
}

func TestEngineCreatesExactBookWhenOnlyPartialTitleMatches(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := engine.repo.CreateBook(ctx, library.Book{Title: "Dune Messiah", MediaType: library.MediaTypeEbook}); err != nil {
		t.Fatal(err)
	}
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{}`)

	report, err := engine.Run(ctx, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.BooksCreated != 1 || report.BooksReused != 0 {
		t.Fatalf("report = %+v", report)
	}
	assertCount(t, db, "books", 2)
}

func TestEngineDoesNotReuseSourceIDAcrossDifferentSources(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacyWithHash(t, db, "Book One", "Author", "/books/one.epub", "epub", "ebook", "prowlarr", "same-id", "hash-1", `{}`)
	insertLegacyWithHash(t, db, "Book Two", "Author", "/books/two.epub", "epub", "ebook", "manual", "same-id", "hash-2", `{}`)

	report, err := engine.Run(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.FilesCreated != 2 || report.FilesReused != 0 {
		t.Fatalf("report = %+v", report)
	}
	assertCount(t, db, "files", 2)
}

func TestEngineInvalidMetadataFallsBackToEmptyObject(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{bad json`)

	report, err := engine.Run(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("unexpected run warnings = %+v", report.Warnings)
	}
	var metadata string
	if err := db.QueryRow(`SELECT embedded_metadata_json FROM files LIMIT 1`).Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	if metadata != `{}` {
		t.Fatalf("metadata = %q", metadata)
	}
}

func TestEngineValidationPassesAfterRun(t *testing.T) {
	engine, db, cleanup := newTestEngine(t)
	defer cleanup()
	insertLegacy(t, db, "Dune", "Frank Herbert", "/books/dune.epub", "epub", "ebook", "torrent", "hash-1", `{}`)

	if _, err := engine.Run(context.Background(), Options{}); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Validate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK || len(result.Errors) != 0 || len(result.Warnings) != 0 {
		t.Fatalf("validation = %+v", result)
	}
}

func newTestEngine(t *testing.T) (*LibraryMigrationEngine, *sql.DB, func()) {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/migration.db?_busy_timeout=10000")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range migrationTestSchema {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("schema failed: %v\n%s", err, stmt)
		}
	}
	repo, err := library.NewNormalizedRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore(db)
	engine, err := NewEngine(store, store, repo)
	if err != nil {
		t.Fatal(err)
	}
	return engine, db, func() { _ = db.Close() }
}

func insertLegacy(t *testing.T, db *sql.DB, title, author, path, format, mediaType, source, sourceID, metadata string) {
	t.Helper()
	insertLegacyWithHash(t, db, title, author, path, format, mediaType, source, sourceID, sourceID, metadata)
}

func insertLegacyWithHash(t *testing.T, db *sql.DB, title, author, path, format, mediaType, source, sourceID, hash, metadata string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO library_items
		(title, author, file_path, original_path, file_size, file_format, media_type, source, source_id, metadata, content_hash)
		VALUES (?, ?, ?, '', 123, ?, ?, ?, ?, ?, ?)`,
		title, author, path, format, mediaType, source, sourceID, metadata, hash)
	if err != nil {
		t.Fatal(err)
	}
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}

var migrationTestSchema = []string{
	`CREATE TABLE library_items (
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
	`CREATE TABLE books (
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
	`CREATE TABLE editions (
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
	`CREATE TABLE contributors (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		sort_name TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
	)`,
	`CREATE TABLE edition_contributors (
		edition_id INTEGER NOT NULL,
		contributor_id INTEGER NOT NULL,
		role TEXT NOT NULL DEFAULT 'author',
		position INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (edition_id, contributor_id, role),
		FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE,
		FOREIGN KEY (contributor_id) REFERENCES contributors(id) ON DELETE RESTRICT
	)`,
	`CREATE TABLE series (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
	)`,
	`CREATE TABLE book_series (
		book_id INTEGER NOT NULL,
		series_id INTEGER NOT NULL,
		position REAL,
		display_position TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (book_id, series_id),
		FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
		FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE RESTRICT
	)`,
	`CREATE TABLE files (
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
		embedded_metadata_json TEXT NOT NULL DEFAULT '{}',
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
		FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE
	)`,
	`CREATE UNIQUE INDEX idx_files_file_path_unique ON files(file_path) WHERE file_path IS NOT NULL AND file_path <> ''`,
	`CREATE TABLE identifiers (
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
	`CREATE UNIQUE INDEX idx_identifiers_book_unique ON identifiers(book_id, provider, identifier) WHERE book_id IS NOT NULL`,
	`CREATE UNIQUE INDEX idx_identifiers_edition_unique ON identifiers(edition_id, provider, identifier) WHERE edition_id IS NOT NULL`,
	`CREATE TABLE covers (
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
	`CREATE TABLE library_item_migration_map (
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
	`CREATE TABLE backfill_runs (
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
	`CREATE TABLE backfill_state (
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
}
