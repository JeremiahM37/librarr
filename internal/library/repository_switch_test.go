package library

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
)

func TestConfiguredLibraryServiceDefaultSelectsLegacy(t *testing.T) {
	d := switchTestDB(t)
	selection, err := NewConfiguredLibraryService(context.Background(), &config.Config{}, d)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Mode != RepositoryModeLegacy || selection.Implementation != "legacy" {
		t.Fatalf("selection = %+v", selection)
	}
	if _, ok := selection.Repository.(*LegacyLibraryRepository); !ok {
		t.Fatalf("repository type = %T", selection.Repository)
	}
}

func TestConfiguredLibraryServiceExplicitLegacySelectsLegacy(t *testing.T) {
	d := switchTestDB(t)
	cfg := &config.Config{LibraryRepositoryMode: "legacy"}
	selection, err := NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Implementation != "legacy" {
		t.Fatalf("implementation = %q", selection.Implementation)
	}
}

func TestConfiguredLibraryServiceNormalizedSelectsNormalizedWhenReady(t *testing.T) {
	d := switchTestDB(t)
	legacyID, fileID := seedReadyNormalizedLibrary(t, d, "Normalized Title", "Normalized Author")
	if legacyID == 0 || fileID == 0 {
		t.Fatal("expected seeded IDs")
	}

	selection, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "normalized"}, d)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Implementation != "normalized" {
		t.Fatalf("implementation = %q", selection.Implementation)
	}
	if _, ok := selection.Repository.(*NormalizedRepository); !ok {
		t.Fatalf("repository type = %T", selection.Repository)
	}

	items, err := selection.LibraryService.ListLegacyItems(context.Background(), "ebook", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Title != "Normalized Title" || items[0].Author != "Normalized Author" {
		t.Fatalf("items = %+v", items)
	}
}

func TestConfiguredLibraryServiceNormalizedAllowsFreshEmptyInstall(t *testing.T) {
	d := switchTestDB(t)

	selection, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "normalized"}, d)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Implementation != "normalized" {
		t.Fatalf("implementation = %q", selection.Implementation)
	}
	if !selection.Readiness.FreshInstall || !selection.Readiness.Ready {
		t.Fatalf("readiness = %+v", selection.Readiness)
	}
}

func TestConfiguredLibraryServiceNormalizedFailsWhenBackfillIncomplete(t *testing.T) {
	d := switchTestDB(t)
	legacyID := seedLegacyItem(t, d)
	insertBackfillRun(t, d, "running", true)
	insertBackfillState(t, d, legacyID, "pending", 0, 0, 0)

	_, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "normalized"}, d)
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfiguredLibraryServiceNormalizedFailsWhenValidationFailed(t *testing.T) {
	d := switchTestDB(t)
	legacyID, fileID := seedNormalizedRows(t, d, "Bad Validation", "Author")
	insertBackfillRun(t, d, "completed", false)
	insertBackfillState(t, d, legacyID, "completed", 1, 1, fileID)

	_, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "normalized"}, d)
	if err == nil || !strings.Contains(err.Error(), "validation") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfiguredLibraryServiceNormalizedFailsWhenMigrationStateMissing(t *testing.T) {
	d := switchTestDB(t)
	seedLegacyItem(t, d)
	insertBackfillRun(t, d, "completed", true)

	_, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "normalized"}, d)
	if err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfiguredLibraryServiceSwitchingBackToLegacyLeavesNormalizedData(t *testing.T) {
	d := switchTestDB(t)
	seedReadyNormalizedLibrary(t, d, "Normalized Title", "Author")

	selection, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "legacy"}, d)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Implementation != "legacy" {
		t.Fatalf("implementation = %q", selection.Implementation)
	}
	assertSQLCount(t, d, "books", 1)
	assertSQLCount(t, d, "files", 1)
}

func TestConfiguredLibraryServiceRejectsInvalidMode(t *testing.T) {
	d := switchTestDB(t)
	_, err := NewConfiguredLibraryService(context.Background(), &config.Config{LibraryRepositoryMode: "future"}, d)
	if err == nil || !strings.Contains(err.Error(), "invalid LIBRARR_LIBRARY_REPOSITORY_MODE") {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckNormalizedReadinessFailsWhenSchemaMigrationMissing(t *testing.T) {
	d := switchTestDB(t)
	if _, err := d.SQLDB().Exec(`DELETE FROM schema_migrations WHERE version = 3`); err != nil {
		t.Fatal(err)
	}
	readiness := CheckNormalizedReadiness(context.Background(), d.SQLDB())
	if readiness.Ready || readiness.MigrationsOK {
		t.Fatalf("readiness = %+v", readiness)
	}
}

func TestCheckNormalizedReadinessFreshInstallIsReady(t *testing.T) {
	d := switchTestDB(t)
	readiness := CheckNormalizedReadiness(context.Background(), d.SQLDB())
	if !readiness.Ready || !readiness.FreshInstall || readiness.LegacyItems != 0 {
		t.Fatalf("readiness = %+v", readiness)
	}
}

func switchTestDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func seedReadyNormalizedLibrary(t *testing.T, d *db.DB, title, author string) (int64, int64) {
	t.Helper()
	legacyID, fileID := seedNormalizedRows(t, d, title, author)
	insertBackfillRun(t, d, "completed", true)
	insertBackfillState(t, d, legacyID, "completed", 1, 1, fileID)
	return legacyID, fileID
}

func seedNormalizedRows(t *testing.T, d *db.DB, title, author string) (int64, int64) {
	t.Helper()
	legacyID := seedLegacyItem(t, d)
	res, err := d.SQLDB().Exec(`INSERT INTO books (id, title, sort_title, media_type, status) VALUES (1, ?, ?, 'ebook', 'owned')`, title, NormalizeKey(title))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.RowsAffected(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO editions (id, book_id, title) VALUES (1, 1, ?)`, title); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO contributors (id, name, sort_name) VALUES (1, ?, ?)`, author, NormalizeKey(author)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO edition_contributors (edition_id, contributor_id, role, position) VALUES (1, 1, 'author', 1)`); err != nil {
		t.Fatal(err)
	}
	fileRes, err := d.SQLDB().Exec(`INSERT INTO files (edition_id, media_type, format, file_path, original_path, file_size, source_id, source_type, embedded_metadata_json)
		VALUES (1, 'ebook', 'epub', '/books/normalized.epub', '/incoming/normalized.epub', 42, 'source-1', 'test', '{}')`)
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := fileRes.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return legacyID, fileID
}

func seedLegacyItem(t *testing.T, d *db.DB) int64 {
	t.Helper()
	res, err := d.SQLDB().Exec(`INSERT INTO library_items (title, author, file_path, file_format, media_type, source_id)
		VALUES ('Legacy Title', 'Legacy Author', '/books/legacy.epub', 'epub', 'ebook', 'source-1')`)
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insertBackfillRun(t *testing.T, d *db.DB, status string, validationOK bool) int64 {
	t.Helper()
	report := `{"validation":{"ok":true,"errors":[]},"errors":[]}`
	if !validationOK {
		report = `{"validation":{"ok":false,"errors":["validation failed"]},"errors":[]}`
	}
	res, err := d.SQLDB().Exec(`INSERT INTO backfill_runs (version, status, dry_run, report_json, completed_at)
		VALUES (?, ?, 0, ?, datetime('now'))`, requiredBackfillVersion, status, report)
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insertBackfillState(t *testing.T, d *db.DB, legacyID int64, status string, bookID, editionID, fileID int64) {
	t.Helper()
	_, err := d.SQLDB().Exec(`INSERT INTO backfill_state (legacy_item_id, run_id, status, book_id, edition_id, file_id)
		VALUES (?, 1, ?, NULLIF(?, 0), NULLIF(?, 0), NULLIF(?, 0))`, legacyID, status, bookID, editionID, fileID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.SQLDB().Exec(`INSERT INTO library_item_migration_map (library_item_id, book_id, edition_id, file_id, status, created_at, updated_at)
		VALUES (?, NULLIF(?, 0), NULLIF(?, 0), NULLIF(?, 0), ?, datetime('now'), datetime('now'))`, legacyID, bookID, editionID, fileID, status)
	if err != nil {
		t.Fatal(err)
	}
}

func assertSQLCount(t *testing.T, d *db.DB, table string, want int) {
	t.Helper()
	var got int
	if err := d.SQLDB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}
