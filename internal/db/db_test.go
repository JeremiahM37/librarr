package db

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/webhook"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	d, err := New(path)
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func tableExists(t *testing.T, d *DB, name string) bool {
	t.Helper()
	return countRows(t, d, "sqlite_master", "type = 'table' AND name = ?", name) == 1
}

func indexExists(t *testing.T, d *DB, name string) bool {
	t.Helper()
	return countRows(t, d, "sqlite_master", "type = 'index' AND name = ?", name) == 1
}

func columnExists(t *testing.T, d *DB, table, column string) bool {
	t.Helper()
	rows, err := d.db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue interface{}
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatal(err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return false
}

func schemaMigrationVersions(t *testing.T, d *DB) map[int]string {
	t.Helper()
	rows, err := d.db.Query("SELECT version, name FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	versions := make(map[int]string)
	for rows.Next() {
		var version int
		var name string
		if err := rows.Scan(&version, &name); err != nil {
			t.Fatal(err)
		}
		versions[version] = name
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return versions
}

func countRows(t *testing.T, d *DB, table, where string, args ...interface{}) int {
	t.Helper()
	var count int
	query := "SELECT COUNT(*) FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	if err := d.db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestNew_CreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "deep", "test.db")
	d, err := New(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer d.Close()

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		t.Error("expected directory to be created")
	}
}

func TestSchemaFoundation_FreshDatabaseCreatesLegacyAndNormalizedTables(t *testing.T) {
	d := newTestDB(t)

	for _, table := range []string{
		"library_items",
		"schema_migrations",
		"books",
		"editions",
		"contributors",
		"edition_contributors",
		"series",
		"book_series",
		"files",
		"identifiers",
		"covers",
		"library_item_migration_map",
		"backfill_runs",
		"backfill_state",
	} {
		if !tableExists(t, d, table) {
			t.Fatalf("expected table %s to exist", table)
		}
	}

	if !indexExists(t, d, "idx_files_file_path_unique") {
		t.Fatal("expected file path unique index")
	}
	if !indexExists(t, d, "idx_backfill_runs_status") {
		t.Fatal("expected backfill runs status index")
	}
	if !indexExists(t, d, "idx_backfill_state_status") {
		t.Fatal("expected backfill state status index")
	}
	if !columnExists(t, d, "files", "embedded_metadata_json") {
		t.Fatal("expected files.embedded_metadata_json column")
	}
}

func TestSchemaFoundation_ExistingLibraryItemsUpgradeWithoutDataLoss(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy.db")
	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`CREATE TABLE library_items (
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
		added_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`INSERT INTO library_items (title, author, file_path, file_format, media_type, source_id)
		VALUES ('Legacy Book', 'Legacy Author', '/books/legacy.epub', 'epub', 'ebook', 'legacy-1')`); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	d, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	items, err := d.GetItems("ebook", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Title != "Legacy Book" || items[0].SourceID != "legacy-1" {
		t.Fatalf("legacy items after migration = %+v", items)
	}
	if !tableExists(t, d, "books") || !tableExists(t, d, "files") {
		t.Fatal("normalized tables were not created")
	}
}

func TestSchemaFoundation_MigrationVersionsRecordedAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "library.db")
	d, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	first := schemaMigrationVersions(t, d)
	if err := d.migrate(); err != nil {
		t.Fatal(err)
	}
	second := schemaMigrationVersions(t, d)
	d.Close()

	if len(first) != len(versionedMigrations) {
		t.Fatalf("recorded migrations = %v, want %d entries", first, len(versionedMigrations))
	}
	if len(second) != len(first) {
		t.Fatalf("migration rerun changed versions: before %v after %v", first, second)
	}
	for version, name := range first {
		if second[version] != name {
			t.Fatalf("version %d name changed from %q to %q", version, name, second[version])
		}
	}

	d, err = New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	third := schemaMigrationVersions(t, d)
	if len(third) != len(first) {
		t.Fatalf("reopen migrations = %v, want %v", third, first)
	}
}

func TestWantedReleaseContextMigrationPreservesExistingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "wanted-old.db")
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at DATETIME NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range versionedMigrations {
		if migration.version >= 7 {
			continue
		}
		if _, err := raw.Exec(`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, datetime('now'))`, migration.version, migration.name); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := raw.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateLibrarr2WantedBooks(tx); err != nil {
		t.Fatal(err)
	}
	if err := migrateLibrarr2WantedMonitoring(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO wanted_books (title, author, media_type, monitored, status) VALUES ('Existing Wanted', '', 'ebook', 1, 'wanted')`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	d, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if !columnExists(t, d, "wanted_books", "origin_release_title") || !columnExists(t, d, "wanted_books", "preferred_format") {
		t.Fatalf("release context columns were not added")
	}
	items, err := d.ListWantedBooks()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Title != "Existing Wanted" {
		t.Fatalf("items after migration = %+v", items)
	}
}

func TestWantedReleasesStoreRetrieveAndReplace(t *testing.T) {
	d := newTestDB(t)
	book, err := d.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := d.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty release list = %+v", empty)
	}
	if err := d.ReplaceWantedReleases(book.ID, []models.WantedRelease{
		{Title: "Lower Score", Score: 70, Format: "epub", Language: "en", Protocol: "torrent", Seeders: 3, SearchQuery: "martian", SearchTime: time.Unix(10, 0).UTC()},
		{Title: "Higher Score", Score: 95, Format: "mobi", Language: "en", Protocol: "torrent", Seeders: 12, SearchQuery: "martian", SearchTime: time.Unix(10, 0).UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	releases, err := d.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 2 || releases[0].Title != "Higher Score" || releases[1].Title != "Lower Score" {
		t.Fatalf("releases = %+v", releases)
	}
	if releases[0].Format != "mobi" || releases[0].Language != "en" || releases[0].Seeders != 12 {
		t.Fatalf("release fields = %+v", releases[0])
	}
	if err := d.ReplaceWantedReleases(book.ID, []models.WantedRelease{
		{Title: "Replacement", Score: 88, Format: "pdf", Protocol: "usenet", SearchQuery: "martian pdf", SearchTime: time.Unix(20, 0).UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	releases, err = d.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 || releases[0].Title != "Replacement" || releases[0].Protocol != "usenet" {
		t.Fatalf("replaced releases = %+v", releases)
	}
}

func TestWantedManualDownloadMigrationAndState(t *testing.T) {
	d := newTestDB(t)
	if !columnExists(t, d, "wanted_books", "selected_release_id") ||
		!columnExists(t, d, "wanted_books", "download_started_at") ||
		!columnExists(t, d, "wanted_books", "download_error") {
		t.Fatalf("wanted manual download columns were not created")
	}

	book, err := d.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "found",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ReplaceWantedReleases(book.ID, []models.WantedRelease{
		{Title: "The Martian release", Score: 95, Format: "epub", Protocol: "torrent", DownloadURL: "magnet:?xt=urn:btih:abc"},
	}); err != nil {
		t.Fatal(err)
	}
	releases, err := d.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := d.MarkWantedDownloading(book.ID, releases[0].ID, releases[0].Title, "qbittorrent", "", time.Unix(100, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != "downloading" || updated.SelectedReleaseID != releases[0].ID || updated.SelectedReleaseTitle != releases[0].Title || updated.DownloadClient != "qbittorrent" || updated.DownloadStartedAt == nil {
		t.Fatalf("updated wanted download state = %+v", updated)
	}
}

func TestListMonitoredWantedBooksSkipsTerminalAndDownloadingStatuses(t *testing.T) {
	d := newTestDB(t)
	statuses := []string{"wanted", "found", "missing", "downloading", "downloaded", "imported", "ignored"}
	for _, status := range statuses {
		if _, err := d.CreateWantedBook(models.WantedBook{
			Title:     "Book " + status,
			Author:    "Author " + status,
			MediaType: "ebook",
			Monitored: true,
			Status:    status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := d.ListMonitoredWantedBooks()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, item := range items {
		got[item.Status] = true
	}
	for _, status := range []string{"wanted", "found", "missing"} {
		if !got[status] {
			t.Fatalf("expected searchable status %q in %+v", status, got)
		}
	}
	for _, status := range []string{"downloading", "downloaded", "imported", "ignored"} {
		if got[status] {
			t.Fatalf("status %q should not be monitored for search: %+v", status, got)
		}
	}
}

func TestSchemaFoundation_FailedMigrationIsNotRecorded(t *testing.T) {
	d := newTestDB(t)
	err := d.applySchemaMigration(schemaMigration{
		version: 999,
		name:    "intentional_failure",
		run: func(tx *sql.Tx) error {
			if _, err := tx.Exec("CREATE TABLE failed_migration_marker (id INTEGER PRIMARY KEY)"); err != nil {
				return err
			}
			return errors.New("stop migration")
		},
	})
	if err == nil {
		t.Fatal("expected failed migration to return an error")
	}
	if _, ok := schemaMigrationVersions(t, d)[999]; ok {
		t.Fatal("failed migration version was recorded")
	}
	if tableExists(t, d, "failed_migration_marker") {
		t.Fatal("failed transactional migration left its table behind")
	}
}

func TestSchemaFoundation_NormalizedRelationshipsAndConstraints(t *testing.T) {
	d := newTestDB(t)

	bookID, err := d.CreateBook(&Book{Title: "The Guardian's Path", SortTitle: "guardian's path", MediaType: "ebook", Monitored: true, Status: "owned"})
	if err != nil {
		t.Fatal(err)
	}
	gotBook, err := d.GetBook(bookID)
	if err != nil {
		t.Fatal(err)
	}
	if gotBook.Title != "The Guardian's Path" || !gotBook.Monitored {
		t.Fatalf("GetBook = %+v", gotBook)
	}

	firstEditionID, err := d.CreateEdition(&Edition{BookID: bookID, Title: "The Guardian's Path", EditionName: "Retail"})
	if err != nil {
		t.Fatal(err)
	}
	secondEditionID, err := d.CreateEdition(&Edition{BookID: bookID, Title: "The Guardian's Path", EditionName: "PDF"})
	if err != nil {
		t.Fatal(err)
	}
	if firstEditionID == secondEditionID {
		t.Fatal("multiple editions produced same ID")
	}

	authorID, err := d.CreateContributor(&Contributor{Name: "Carla Jablonski", SortName: "Jablonski, Carla"})
	if err != nil {
		t.Fatal(err)
	}
	illustratorID, err := d.CreateContributor(&Contributor{Name: "Disney", SortName: "Disney"})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.AddEditionContributor(EditionContributor{EditionID: firstEditionID, ContributorID: authorID, Role: "author", Position: 1}); err != nil {
		t.Fatal(err)
	}
	if err := d.AddEditionContributor(EditionContributor{EditionID: firstEditionID, ContributorID: illustratorID, Role: "illustrator", Position: 2}); err != nil {
		t.Fatal(err)
	}

	seriesID, err := d.CreateSeries(&Series{Title: "Prince of Persia"})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.AddBookSeries(BookSeries{
		BookID: bookID, SeriesID: seriesID,
		Position:        sql.NullFloat64{Float64: 1, Valid: true},
		DisplayPosition: "1",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := d.CreateFile(&File{EditionID: firstEditionID, MediaType: "ebook", Format: "epub", FilePath: "/books/guardian.epub", ContentHash: "hash-epub", IsManaged: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateFile(&File{EditionID: firstEditionID, MediaType: "ebook", Format: "mobi", FilePath: "/books/guardian.mobi", ContentHash: "hash-mobi", IsManaged: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateFile(&File{EditionID: firstEditionID, MediaType: "ebook", Format: "azw3", FilePath: "/books/guardian.epub", IsManaged: true}); err == nil {
		t.Fatal("expected duplicate file_path to be rejected")
	}

	if _, err := d.AddIdentifier(&Identifier{BookID: sql.NullInt64{Int64: bookID, Valid: true}, Provider: "isbn13", Identifier: "9780000000001"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddIdentifier(&Identifier{EditionID: sql.NullInt64{Int64: firstEditionID, Valid: true}, Provider: "openlibrary", Identifier: "OL1M"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddIdentifier(&Identifier{Provider: "asin", Identifier: "B000000"}); err == nil {
		t.Fatal("expected identifier without owner to fail")
	}
	if _, err := d.AddIdentifier(&Identifier{
		BookID: sql.NullInt64{Int64: bookID, Valid: true}, EditionID: sql.NullInt64{Int64: firstEditionID, Valid: true},
		Provider: "bad", Identifier: "both",
	}); err == nil {
		t.Fatal("expected identifier with two owners to fail")
	}

	if _, err := d.AddCover(&Cover{BookID: sql.NullInt64{Int64: bookID, Valid: true}, Source: "provider", LocalPath: "/covers/book.jpg", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddCover(&Cover{EditionID: sql.NullInt64{Int64: firstEditionID, Valid: true}, Source: "embedded", LocalPath: "/covers/edition.jpg", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddCover(&Cover{Source: "bad", LocalPath: "/covers/bad.jpg"}); err == nil {
		t.Fatal("expected cover without owner to fail")
	}
	if _, err := d.AddCover(&Cover{
		BookID: sql.NullInt64{Int64: bookID, Valid: true}, EditionID: sql.NullInt64{Int64: firstEditionID, Valid: true},
		Source: "bad", LocalPath: "/covers/both.jpg",
	}); err == nil {
		t.Fatal("expected cover with two owners to fail")
	}
}

func TestSchemaFoundation_DeleteBehavior(t *testing.T) {
	d := newTestDB(t)
	bookID, err := d.CreateBook(&Book{Title: "Cascade Book", MediaType: "ebook"})
	if err != nil {
		t.Fatal(err)
	}
	editionID, err := d.CreateEdition(&Edition{BookID: bookID, Title: "Cascade Book"})
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := d.CreateFile(&File{EditionID: editionID, MediaType: "ebook", Format: "epub", FilePath: "/books/cascade.epub", IsManaged: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.db.Exec("DELETE FROM files WHERE id = ?", fileID); err != nil {
		t.Fatal(err)
	}
	if countRows(t, d, "editions", "id = ?", editionID) != 1 || countRows(t, d, "books", "id = ?", bookID) != 1 {
		t.Fatal("deleting a file should not delete its edition or book")
	}

	if _, err := d.CreateFile(&File{EditionID: editionID, MediaType: "ebook", Format: "mobi", FilePath: "/books/cascade.mobi", IsManaged: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.db.Exec("DELETE FROM books WHERE id = ?", bookID); err != nil {
		t.Fatal(err)
	}
	if countRows(t, d, "books", "id = ?", bookID) != 0 {
		t.Fatal("book was not deleted")
	}
	if countRows(t, d, "editions", "id = ?", editionID) != 0 {
		t.Fatal("deleting a book should cascade to editions")
	}
	if countRows(t, d, "files", "edition_id = ?", editionID) != 0 {
		t.Fatal("deleting a book should cascade to files through editions")
	}
}

func TestLibraryItems_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("add and retrieve item", func(t *testing.T) {
		item := &models.LibraryItem{
			Title:      "The Great Gatsby",
			Author:     "F. Scott Fitzgerald",
			FilePath:   "/books/gatsby.epub",
			FileSize:   500000,
			FileFormat: "epub",
			MediaType:  "ebook",
			Source:     "annas",
			SourceID:   "md5-abc123",
		}

		id, err := d.AddItem(item)
		if err != nil {
			t.Fatalf("AddItem failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}
	})

	t.Run("find by title", func(t *testing.T) {
		items, err := d.FindByTitle("The Great Gatsby")
		if err != nil {
			t.Fatalf("FindByTitle failed: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Author != "F. Scott Fitzgerald" {
			t.Errorf("expected author F. Scott Fitzgerald, got %s", items[0].Author)
		}
	})

	t.Run("find by title case insensitive", func(t *testing.T) {
		items, err := d.FindByTitle("the great gatsby")
		if err != nil {
			t.Fatalf("FindByTitle failed: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item (case insensitive), got %d", len(items))
		}
	})

	t.Run("has source id", func(t *testing.T) {
		if !d.HasSourceID("md5-abc123") {
			t.Error("expected HasSourceID to return true")
		}
		if d.HasSourceID("nonexistent") {
			t.Error("expected HasSourceID to return false for unknown ID")
		}
		if d.HasSourceID("") {
			t.Error("expected HasSourceID to return false for empty string")
		}
	})

	t.Run("count items", func(t *testing.T) {
		count, err := d.CountItems("")
		if err != nil {
			t.Fatalf("CountItems failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}

		count, err = d.CountItems("ebook")
		if err != nil {
			t.Fatalf("CountItems ebook failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 ebook, got %d", count)
		}

		count, err = d.CountItems("audiobook")
		if err != nil {
			t.Fatalf("CountItems audiobook failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 audiobooks, got %d", count)
		}
	})

	t.Run("get items paginated", func(t *testing.T) {
		items, err := d.GetItems("", 10, 0)
		if err != nil {
			t.Fatalf("GetItems failed: %v", err)
		}
		if len(items) != 1 {
			t.Errorf("expected 1 item, got %d", len(items))
		}
	})

	t.Run("delete item", func(t *testing.T) {
		items, _ := d.GetItems("", 10, 0)
		if len(items) == 0 {
			t.Fatal("no items to delete")
		}
		err := d.DeleteItem(items[0].ID)
		if err != nil {
			t.Fatalf("DeleteItem failed: %v", err)
		}

		count, _ := d.CountItems("")
		if count != 0 {
			t.Errorf("expected 0 items after delete, got %d", count)
		}
	})

	t.Run("delete nonexistent item", func(t *testing.T) {
		err := d.DeleteItem(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent item")
		}
	})
}

func TestBackfillLibraryMetadata(t *testing.T) {
	dir := t.TempDir()
	database, err := New(filepath.Join(dir, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	metadataFile := filepath.Join(dir, "library", "Torrent Name", "book.epub")
	if err := os.MkdirAll(filepath.Dir(metadataFile), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataFile, []byte("ebook"), 0644); err != nil {
		t.Fatal(err)
	}
	missingFile := filepath.Join(dir, "library", "Missing", "book.epub")
	userEditedFile := filepath.Join(dir, "library", "Torrent Name", "edited.epub")
	if err := os.WriteFile(userEditedFile, []byte("edited"), 0644); err != nil {
		t.Fatal(err)
	}

	for _, item := range []*models.LibraryItem{
		{Title: "Torrent Name", Author: "", FilePath: metadataFile, OriginalPath: "/incoming/Torrent Name/book.epub", MediaType: "ebook"},
		{Title: "Missing", Author: "", FilePath: missingFile, OriginalPath: "/incoming/Missing/book.epub", MediaType: "ebook"},
		{Title: "User Edited", Author: "Someone", FilePath: userEditedFile, OriginalPath: "/incoming/Torrent Name/edited.epub", MediaType: "ebook"},
	} {
		if _, err := database.AddItem(item); err != nil {
			t.Fatal(err)
		}
	}

	updated, err := database.BackfillLibraryMetadata(func(path string) (string, string) {
		if path == metadataFile {
			return "The Guardian's Path", "Jane Doe"
		}
		return "", ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated != 1 {
		t.Fatalf("BackfillLibraryMetadata updated %d rows, want 1", updated)
	}

	items, err := database.GetItems("ebook", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	byPath := make(map[string]models.LibraryItem)
	for _, item := range items {
		byPath[item.FilePath] = item
	}
	if got := byPath[metadataFile]; got.Title != "The Guardian's Path" || got.Author != "Jane Doe" || got.FileFormat != "epub" {
		t.Fatalf("metadata row = %+v, want corrected metadata and format", got)
	}
	if got := byPath[userEditedFile]; got.Title != "User Edited" || got.Author != "Someone" {
		t.Fatalf("user-edited row changed: %+v", got)
	}
	if got := byPath[missingFile]; got.Title != "Missing" {
		t.Fatalf("missing-file row changed: %+v", got)
	}
}

func TestAddItemIsIdempotentByEquivalentPathAndContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(path, []byte("ebook bytes"), 0644); err != nil {
		t.Fatal(err)
	}
	d := newTestDB(t)

	first := &models.LibraryItem{Title: "First title", Author: "First author", FilePath: path, FileFormat: "epub", MediaType: "ebook"}
	firstOutcome, err := d.AddItemWithOutcome(first)
	if err != nil || !firstOutcome.Inserted {
		t.Fatalf("first AddItemWithOutcome = %+v, %v", firstOutcome, err)
	}

	equivalentPath := filepath.Join(dir, "nested", "..", "book.epub")
	second := &models.LibraryItem{Title: "Different metadata", Author: "Different author", FilePath: equivalentPath, FileFormat: "epub", MediaType: "ebook"}
	secondOutcome, err := d.AddItemWithOutcome(second)
	if err != nil {
		t.Fatalf("second AddItemWithOutcome: %v", err)
	}
	if secondOutcome.Inserted || secondOutcome.ID != firstOutcome.ID || secondOutcome.Reason != "duplicate_destination_path" {
		t.Fatalf("second outcome = %+v, want reuse of first row", secondOutcome)
	}

	items, err := d.GetItems("ebook", 10, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems = %d, %v; want one row", len(items), err)
	}
}

func TestAddItemRemainsIdempotentAfterDatabaseReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "library.db")
	filePath := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(filePath, []byte("persistent ebook"), 0644); err != nil {
		t.Fatal(err)
	}

	d, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := d.AddItem(&models.LibraryItem{FilePath: filePath, FileFormat: "epub", MediaType: "ebook"})
	if err != nil {
		d.Close()
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	secondID, err := d.AddItem(&models.LibraryItem{FilePath: filePath, FileFormat: "epub", MediaType: "ebook"})
	if err != nil || secondID != firstID {
		t.Fatalf("reopened AddItem = id %d, err %v; want existing id %d", secondID, err, firstID)
	}
}

func TestAddItemDoesNotMergeDifferentFormatsOrFilesByMetadata(t *testing.T) {
	d := newTestDB(t)
	dir := t.TempDir()
	epubPath := filepath.Join(dir, "book.epub")
	mobiPath := filepath.Join(dir, "book.mobi")
	otherPath := filepath.Join(dir, "other.epub")
	for path, body := range map[string]string{epubPath: "same bytes", mobiPath: "same bytes", otherPath: "other epub bytes"} {
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}

	items := []*models.LibraryItem{
		{Title: "Same Book", Author: "Same Author", FilePath: epubPath, FileFormat: "epub", MediaType: "ebook"},
		{Title: "Same Book", Author: "Same Author", FilePath: mobiPath, FileFormat: "mobi", MediaType: "ebook"},
		{Title: "Same Book", Author: "Same Author", FilePath: otherPath, FileFormat: "epub", MediaType: "ebook"},
	}
	ids := make(map[int64]bool)
	for _, item := range items {
		id, err := d.AddItem(item)
		if err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	if len(ids) != len(items) {
		t.Fatalf("metadata-similar files produced %d IDs, want %d", len(ids), len(items))
	}
}

func TestAddItemConcurrentDuplicateImportsCreateOneRow(t *testing.T) {
	d := newTestDB(t)
	filePath := filepath.Join(t.TempDir(), "book.epub")
	if err := os.WriteFile(filePath, []byte("concurrent ebook"), 0644); err != nil {
		t.Fatal(err)
	}

	const attempts = 8
	ids := make(chan int64, attempts)
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := d.AddItem(&models.LibraryItem{FilePath: filePath, FileFormat: "epub", MediaType: "ebook"})
			ids <- id
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)

	var wantID int64
	for id := range ids {
		if id == 0 {
			t.Fatal("concurrent AddItem returned zero ID")
		}
		if wantID == 0 {
			wantID = id
		} else if id != wantID {
			t.Fatalf("concurrent AddItem IDs differ: got %d and %d", id, wantID)
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent AddItem error: %v", err)
		}
	}
	count, err := d.CountItems("ebook")
	if err != nil || count != 1 {
		t.Fatalf("CountItems = %d, %v; want one row", count, err)
	}
}

func TestDownloadJobs_CRUD(t *testing.T) {
	d := newTestDB(t)

	job := &models.DownloadJob{
		ID:         "test-job-1",
		Title:      "Test Book",
		Source:     "annas",
		Status:     "queued",
		URL:        "https://example.com",
		MD5:        "abc123",
		SourceID:   "zlibrary-42-abc123",
		MediaType:  "ebook",
		MaxRetries: 2,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	t.Run("save and get job", func(t *testing.T) {
		err := d.SaveJob(job)
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		got, err := d.GetJob("test-job-1")
		if err != nil {
			t.Fatalf("GetJob failed: %v", err)
		}
		if got.Title != "Test Book" {
			t.Errorf("expected title Test Book, got %s", got.Title)
		}
		if got.Source != "annas" {
			t.Errorf("expected source annas, got %s", got.Source)
		}
		if got.SourceID != "zlibrary-42-abc123" {
			t.Errorf("expected source_id zlibrary-42-abc123, got %s", got.SourceID)
		}
	})

	t.Run("update job status", func(t *testing.T) {
		err := d.UpdateJobStatus("test-job-1", "downloading", "50% complete", "")
		if err != nil {
			t.Fatalf("UpdateJobStatus failed: %v", err)
		}

		got, _ := d.GetJob("test-job-1")
		if got.Status != "downloading" {
			t.Errorf("expected status downloading, got %s", got.Status)
		}
		if got.Detail != "50% complete" {
			t.Errorf("expected detail '50%% complete', got %s", got.Detail)
		}
	})

	t.Run("get all jobs", func(t *testing.T) {
		jobs, err := d.GetJobs()
		if err != nil {
			t.Fatalf("GetJobs failed: %v", err)
		}
		if len(jobs) != 1 {
			t.Errorf("expected 1 job, got %d", len(jobs))
		}
	})

	t.Run("clear finished jobs", func(t *testing.T) {
		// Mark job as completed
		d.UpdateJobStatus("test-job-1", "completed", "Done", "")
		count, err := d.ClearFinishedJobs()
		if err != nil {
			t.Fatalf("ClearFinishedJobs failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 cleared, got %d", count)
		}

		jobs, _ := d.GetJobs()
		if len(jobs) != 0 {
			t.Errorf("expected 0 jobs after clear, got %d", len(jobs))
		}
	})

	t.Run("delete job", func(t *testing.T) {
		job2 := &models.DownloadJob{
			ID: "test-job-2", Title: "Another", Source: "gutenberg",
			Status: "queued", CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		d.SaveJob(job2)
		err := d.DeleteJob("test-job-2")
		if err != nil {
			t.Fatalf("DeleteJob failed: %v", err)
		}
	})

	t.Run("get nonexistent job", func(t *testing.T) {
		_, err := d.GetJob("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent job")
		}
	})
}

func TestDownloadJobs_StatusHistory(t *testing.T) {
	d := newTestDB(t)

	job := &models.DownloadJob{
		ID:        "history-job",
		Title:     "History Test",
		Source:    "annas",
		Status:    "queued",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		StatusHistory: []models.StatusTransition{
			{From: "queued", To: "downloading", Detail: "Starting", Timestamp: time.Now().Format(time.RFC3339)},
		},
	}

	if err := d.SaveJob(job); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	got, err := d.GetJob("history-job")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if len(got.StatusHistory) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(got.StatusHistory))
	}
	if got.StatusHistory[0].From != "queued" {
		t.Errorf("expected From=queued, got %s", got.StatusHistory[0].From)
	}
}

func TestWishlist_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("add and list", func(t *testing.T) {
		id, err := d.AddWishlistItem("Dune", "Frank Herbert", "ebook")
		if err != nil {
			t.Fatalf("AddWishlistItem failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		items, err := d.GetWishlist()
		if err != nil {
			t.Fatalf("GetWishlist failed: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Title != "Dune" {
			t.Errorf("expected title Dune, got %s", items[0].Title)
		}
		if items[0].Author != "Frank Herbert" {
			t.Errorf("expected author Frank Herbert, got %s", items[0].Author)
		}
	})

	t.Run("default media type", func(t *testing.T) {
		id, err := d.AddWishlistItem("Book", "Author", "")
		if err != nil {
			t.Fatalf("AddWishlistItem failed: %v", err)
		}
		_ = id

		items, _ := d.GetWishlist()
		// Find the one with empty media type input (should be "ebook")
		found := false
		for _, item := range items {
			if item.Title == "Book" && item.MediaType == "ebook" {
				found = true
			}
		}
		if !found {
			t.Error("expected default media type 'ebook'")
		}
	})

	t.Run("delete", func(t *testing.T) {
		items, _ := d.GetWishlist()
		if len(items) == 0 {
			t.Fatal("no items to delete")
		}
		err := d.DeleteWishlistItem(items[0].ID)
		if err != nil {
			t.Fatalf("DeleteWishlistItem failed: %v", err)
		}
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		err := d.DeleteWishlistItem(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent wishlist item")
		}
	})
}

func TestUsers_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("create and get user", func(t *testing.T) {
		id, err := d.CreateUser("testuser", "hashedpw", "admin")
		if err != nil {
			t.Fatalf("CreateUser failed: %v", err)
		}

		user, err := d.GetUser(id)
		if err != nil {
			t.Fatalf("GetUser failed: %v", err)
		}
		if user.Username != "testuser" {
			t.Errorf("expected username testuser, got %s", user.Username)
		}
		if user.Role != "admin" {
			t.Errorf("expected role admin, got %s", user.Role)
		}
	})

	t.Run("get by username case insensitive", func(t *testing.T) {
		user, err := d.GetUserByUsername("TESTUSER")
		if err != nil {
			t.Fatalf("GetUserByUsername failed: %v", err)
		}
		if user.Username != "testuser" {
			t.Errorf("expected testuser, got %s", user.Username)
		}
	})

	t.Run("list users", func(t *testing.T) {
		users, err := d.ListUsers()
		if err != nil {
			t.Fatalf("ListUsers failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user, got %d", len(users))
		}
	})

	t.Run("count users", func(t *testing.T) {
		count, err := d.CountUsers()
		if err != nil {
			t.Fatalf("CountUsers failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}
	})

	t.Run("update user", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.UpdateUser(users[0].ID, "testuser", "user")
		if err != nil {
			t.Fatalf("UpdateUser failed: %v", err)
		}
		user, _ := d.GetUser(users[0].ID)
		if user.Role != "user" {
			t.Errorf("expected role user, got %s", user.Role)
		}
	})

	t.Run("update password", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.UpdateUserPassword(users[0].ID, "newhash")
		if err != nil {
			t.Fatalf("UpdateUserPassword failed: %v", err)
		}
		user, _ := d.GetUser(users[0].ID)
		if user.PasswordHash != "newhash" {
			t.Errorf("expected password hash updated")
		}
	})

	t.Run("enable and disable user", func(t *testing.T) {
		users, _ := d.ListUsers()
		if !users[0].Enabled {
			t.Fatal("new users should be enabled by default")
		}
		if err := d.SetUserEnabled(users[0].ID, false); err != nil {
			t.Fatalf("SetUserEnabled false failed: %v", err)
		}
		user, _ := d.GetUser(users[0].ID)
		if user.Enabled {
			t.Fatal("expected user to be disabled")
		}
		if err := d.SetUserEnabled(users[0].ID, true); err != nil {
			t.Fatalf("SetUserEnabled true failed: %v", err)
		}
		user, _ = d.GetUser(users[0].ID)
		if !user.Enabled {
			t.Fatal("expected user to be enabled")
		}
	})

	t.Run("update last login", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.UpdateLastLogin(users[0].ID)
		if err != nil {
			t.Fatalf("UpdateLastLogin failed: %v", err)
		}
	})

	t.Run("TOTP flow", func(t *testing.T) {
		users, _ := d.ListUsers()
		uid := users[0].ID

		err := d.SetTOTPSecret(uid, "JBSWY3DPEHPK3PXP")
		if err != nil {
			t.Fatalf("SetTOTPSecret failed: %v", err)
		}

		err = d.EnableTOTP(uid)
		if err != nil {
			t.Fatalf("EnableTOTP failed: %v", err)
		}

		user, _ := d.GetUser(uid)
		if !user.TOTPEnabled {
			t.Error("expected TOTP to be enabled")
		}
		if user.TOTPSecret != "JBSWY3DPEHPK3PXP" {
			t.Errorf("expected TOTP secret, got %s", user.TOTPSecret)
		}

		err = d.DisableTOTP(uid)
		if err != nil {
			t.Fatalf("DisableTOTP failed: %v", err)
		}

		user, _ = d.GetUser(uid)
		if user.TOTPEnabled {
			t.Error("expected TOTP to be disabled")
		}
	})

	t.Run("backup codes", func(t *testing.T) {
		users, _ := d.ListUsers()
		uid := users[0].ID

		hashes := []string{"hash1", "hash2", "hash3"}
		err := d.SaveBackupCodes(uid, hashes)
		if err != nil {
			t.Fatalf("SaveBackupCodes failed: %v", err)
		}

		// Use a backup code
		used, err := d.UseBackupCode(uid, "hash1")
		if err != nil {
			t.Fatalf("UseBackupCode failed: %v", err)
		}
		if !used {
			t.Error("expected backup code to be used")
		}

		// Try to use same code again
		used, _ = d.UseBackupCode(uid, "hash1")
		if used {
			t.Error("expected already-used code to fail")
		}

		// Use nonexistent code
		used, _ = d.UseBackupCode(uid, "nonexistent")
		if used {
			t.Error("expected nonexistent code to fail")
		}
	})

	t.Run("delete user", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.DeleteUser(users[0].ID)
		if err != nil {
			t.Fatalf("DeleteUser failed: %v", err)
		}

		count, _ := d.CountUsers()
		if count != 0 {
			t.Errorf("expected 0 users after delete, got %d", count)
		}
	})

	t.Run("delete nonexistent user", func(t *testing.T) {
		err := d.DeleteUser(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent user")
		}
	})

	t.Run("duplicate username", func(t *testing.T) {
		d.CreateUser("dup", "hash", "user")
		_, err := d.CreateUser("dup", "hash2", "user")
		if err == nil {
			t.Error("expected error on duplicate username")
		}
	})
}

func TestActivityLog(t *testing.T) {
	d := newTestDB(t)

	t.Run("log and retrieve events", func(t *testing.T) {
		err := d.LogEvent("download_complete", "Test Book", "Downloaded successfully", nil, "job-1")
		if err != nil {
			t.Fatalf("LogEvent failed: %v", err)
		}

		events, err := d.GetActivity(10, 0)
		if err != nil {
			t.Fatalf("GetActivity failed: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
		if events[0].EventType != "download_complete" {
			t.Errorf("expected event type download_complete, got %s", events[0].EventType)
		}
		if events[0].Title != "Test Book" {
			t.Errorf("expected title Test Book, got %s", events[0].Title)
		}
	})

	t.Run("count activity", func(t *testing.T) {
		count, err := d.CountActivity()
		if err != nil {
			t.Fatalf("CountActivity failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}
	})

	// LogActivity inserts into the activity_log.user column. On older
	// schemas that column was added via ALTER TABLE in an additive migration
	// that ran *before* the CREATE TABLE, so on a fresh DB the column did
	// not exist and the INSERT failed silently with "no column named user".
	// This test guards against that regression by reading the row back.
	t.Run("LogActivity persists user column on fresh DB", func(t *testing.T) {
		d2 := newTestDB(t)
		d2.LogActivity("alice", "settings_changed", "settings", "Settings updated")

		entries, err := d2.GetActivityLog("", "", 10, 0)
		if err != nil {
			t.Fatalf("GetActivityLog failed: %v", err)
		}
		found := false
		for _, e := range entries {
			if e.User == "alice" && e.Action == "settings_changed" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("LogActivity did not persist user=alice; entries=%+v", entries)
		}
	})
}

func TestGetStats(t *testing.T) {
	d := newTestDB(t)

	d.AddItem(&models.LibraryItem{Title: "Book 1", MediaType: "ebook", Source: "annas"})
	d.AddItem(&models.LibraryItem{Title: "Book 2", MediaType: "ebook", Source: "annas"})
	d.AddItem(&models.LibraryItem{Title: "Audiobook 1", MediaType: "audiobook", Source: "librivox"})

	stats, err := d.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats["total_items"].(int) != 3 {
		t.Errorf("expected total 3, got %v", stats["total_items"])
	}
	if stats["ebooks"].(int) != 2 {
		t.Errorf("expected 2 ebooks, got %v", stats["ebooks"])
	}
	if stats["audiobooks"].(int) != 1 {
		t.Errorf("expected 1 audiobook, got %v", stats["audiobooks"])
	}
}

func TestItemToJSON(t *testing.T) {
	item := models.LibraryItem{
		ID:       1,
		Title:    "Test",
		Author:   "Author",
		Metadata: `{"key": "value"}`,
		AddedAt:  time.Now(),
	}

	m := ItemToJSON(item)
	if m["title"] != "Test" {
		t.Errorf("expected title Test, got %v", m["title"])
	}
	if m["author"] != "Author" {
		t.Errorf("expected author Author, got %v", m["author"])
	}
	if m["metadata"] == nil {
		t.Error("expected metadata to be parsed")
	}

	// Test with empty metadata
	item2 := models.LibraryItem{Title: "No Meta", Metadata: "{}", AddedAt: time.Now()}
	m2 := ItemToJSON(item2)
	if _, ok := m2["metadata"]; ok {
		t.Error("expected no metadata key for empty JSON")
	}
}

func TestBlocklist_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("add and check", func(t *testing.T) {
		id, err := d.AddBlocklistEntry("Bad Book", "annas", "http://example.com/bad", "abc123hash", "wrong file")
		if err != nil {
			t.Fatalf("AddBlocklistEntry failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		if !d.IsBlocklisted("http://example.com/bad", "") {
			t.Error("expected URL to be blocklisted")
		}
		if !d.IsBlocklisted("", "abc123hash") {
			t.Error("expected info hash to be blocklisted")
		}
		if d.IsBlocklisted("http://example.com/good", "") {
			t.Error("expected non-blocklisted URL to return false")
		}
		if d.IsBlocklisted("", "otherhash") {
			t.Error("expected non-blocklisted hash to return false")
		}
		if d.IsBlocklisted("", "") {
			t.Error("expected empty strings to return false")
		}
	})

	t.Run("list entries", func(t *testing.T) {
		entries, err := d.GetBlocklist(10, 0)
		if err != nil {
			t.Fatalf("GetBlocklist failed: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].Title != "Bad Book" {
			t.Errorf("expected title Bad Book, got %s", entries[0].Title)
		}
		if entries[0].Source != "annas" {
			t.Errorf("expected source annas, got %s", entries[0].Source)
		}
		if entries[0].Reason != "wrong file" {
			t.Errorf("expected reason wrong file, got %s", entries[0].Reason)
		}
	})

	t.Run("delete entry", func(t *testing.T) {
		entries, _ := d.GetBlocklist(10, 0)
		err := d.DeleteBlocklistEntry(entries[0].ID)
		if err != nil {
			t.Fatalf("DeleteBlocklistEntry failed: %v", err)
		}
		if d.IsBlocklisted("http://example.com/bad", "") {
			t.Error("expected URL to no longer be blocklisted after delete")
		}
	})

	t.Run("delete nonexistent entry", func(t *testing.T) {
		err := d.DeleteBlocklistEntry(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent blocklist entry")
		}
	})

	t.Run("clear blocklist", func(t *testing.T) {
		d.AddBlocklistEntry("Book A", "src", "urlA", "hashA", "reason")
		d.AddBlocklistEntry("Book B", "src", "urlB", "hashB", "reason")

		err := d.ClearBlocklist()
		if err != nil {
			t.Fatalf("ClearBlocklist failed: %v", err)
		}

		entries, _ := d.GetBlocklist(10, 0)
		if len(entries) != 0 {
			t.Errorf("expected 0 entries after clear, got %d", len(entries))
		}
	})
}

func TestQualityProfile_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("create and get", func(t *testing.T) {
		qp := &QualityProfile{
			Name:             "High Quality",
			FormatRanking:    []string{"epub", "mobi", "pdf"},
			PreferredSizeMin: 100000,
			PreferredSizeMax: 50000000,
			UpgradeAllowed:   true,
			CutoffFormat:     "epub",
		}

		id, err := d.CreateQualityProfile(qp)
		if err != nil {
			t.Fatalf("CreateQualityProfile failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		got, err := d.GetQualityProfile(id)
		if err != nil {
			t.Fatalf("GetQualityProfile failed: %v", err)
		}
		if got.Name != "High Quality" {
			t.Errorf("expected name High Quality, got %s", got.Name)
		}
		if len(got.FormatRanking) != 3 {
			t.Fatalf("expected 3 formats, got %d", len(got.FormatRanking))
		}
		if got.FormatRanking[0] != "epub" {
			t.Errorf("expected first format epub, got %s", got.FormatRanking[0])
		}
		if !got.UpgradeAllowed {
			t.Error("expected UpgradeAllowed to be true")
		}
		if got.CutoffFormat != "epub" {
			t.Errorf("expected cutoff epub, got %s", got.CutoffFormat)
		}
	})

	t.Run("list profiles", func(t *testing.T) {
		profiles, err := d.GetQualityProfiles()
		if err != nil {
			t.Fatalf("GetQualityProfiles failed: %v", err)
		}
		if len(profiles) != 1 {
			t.Errorf("expected 1 profile, got %d", len(profiles))
		}
	})

	t.Run("update profile", func(t *testing.T) {
		profiles, _ := d.GetQualityProfiles()
		qp := profiles[0]
		qp.Name = "Updated Quality"
		qp.UpgradeAllowed = false

		err := d.UpdateQualityProfile(&qp)
		if err != nil {
			t.Fatalf("UpdateQualityProfile failed: %v", err)
		}

		got, _ := d.GetQualityProfile(qp.ID)
		if got.Name != "Updated Quality" {
			t.Errorf("expected name Updated Quality, got %s", got.Name)
		}
		if got.UpgradeAllowed {
			t.Error("expected UpgradeAllowed to be false")
		}
	})

	t.Run("delete profile", func(t *testing.T) {
		profiles, _ := d.GetQualityProfiles()
		err := d.DeleteQualityProfile(profiles[0].ID)
		if err != nil {
			t.Fatalf("DeleteQualityProfile failed: %v", err)
		}

		result, _ := d.GetQualityProfiles()
		if len(result) != 0 {
			t.Errorf("expected 0 profiles after delete, got %d", len(result))
		}
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		err := d.DeleteQualityProfile(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent quality profile")
		}
	})

	t.Run("get nonexistent", func(t *testing.T) {
		_, err := d.GetQualityProfile(99999)
		if err == nil {
			t.Error("expected error getting nonexistent quality profile")
		}
	})
}

func TestReleaseProfile_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("create and get", func(t *testing.T) {
		rp := &ReleaseProfile{
			Name:           "Ebook Preferred",
			MustContain:    []string{"epub"},
			MustNotContain: []string{"sample", "preview"},
			Preferred: []PreferredWord{
				{Word: "calibre", Score: 10},
				{Word: "retail", Score: 20},
			},
			Enabled: true,
		}

		id, err := d.CreateReleaseProfile(rp)
		if err != nil {
			t.Fatalf("CreateReleaseProfile failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		got, err := d.GetReleaseProfile(id)
		if err != nil {
			t.Fatalf("GetReleaseProfile failed: %v", err)
		}
		if got.Name != "Ebook Preferred" {
			t.Errorf("expected name Ebook Preferred, got %s", got.Name)
		}
		if len(got.MustContain) != 1 || got.MustContain[0] != "epub" {
			t.Errorf("expected MustContain [epub], got %v", got.MustContain)
		}
		if len(got.MustNotContain) != 2 {
			t.Errorf("expected 2 MustNotContain entries, got %d", len(got.MustNotContain))
		}
		if len(got.Preferred) != 2 {
			t.Fatalf("expected 2 preferred words, got %d", len(got.Preferred))
		}
		if got.Preferred[1].Word != "retail" || got.Preferred[1].Score != 20 {
			t.Errorf("expected preferred retail/20, got %s/%d", got.Preferred[1].Word, got.Preferred[1].Score)
		}
		if !got.Enabled {
			t.Error("expected profile to be enabled")
		}
	})

	t.Run("list profiles", func(t *testing.T) {
		profiles, err := d.GetReleaseProfiles()
		if err != nil {
			t.Fatalf("GetReleaseProfiles failed: %v", err)
		}
		if len(profiles) != 1 {
			t.Errorf("expected 1 profile, got %d", len(profiles))
		}
	})

	t.Run("update profile", func(t *testing.T) {
		profiles, _ := d.GetReleaseProfiles()
		rp := profiles[0]
		rp.Name = "Updated Release"
		rp.Enabled = false

		err := d.UpdateReleaseProfile(&rp)
		if err != nil {
			t.Fatalf("UpdateReleaseProfile failed: %v", err)
		}

		got, _ := d.GetReleaseProfile(rp.ID)
		if got.Name != "Updated Release" {
			t.Errorf("expected Updated Release, got %s", got.Name)
		}
		if got.Enabled {
			t.Error("expected profile to be disabled")
		}
	})

	t.Run("delete profile", func(t *testing.T) {
		profiles, _ := d.GetReleaseProfiles()
		err := d.DeleteReleaseProfile(profiles[0].ID)
		if err != nil {
			t.Fatalf("DeleteReleaseProfile failed: %v", err)
		}

		result, _ := d.GetReleaseProfiles()
		if len(result) != 0 {
			t.Errorf("expected 0 profiles after delete, got %d", len(result))
		}
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		err := d.DeleteReleaseProfile(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent release profile")
		}
	})
}

func TestTags_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("create and list", func(t *testing.T) {
		id1, err := d.CreateTag("fiction", "#3498db")
		if err != nil {
			t.Fatalf("CreateTag failed: %v", err)
		}
		if id1 <= 0 {
			t.Errorf("expected positive ID, got %d", id1)
		}

		id2, err := d.CreateTag("sci-fi", "#e74c3c")
		if err != nil {
			t.Fatalf("CreateTag failed: %v", err)
		}

		tags, err := d.GetTags()
		if err != nil {
			t.Fatalf("GetTags failed: %v", err)
		}
		if len(tags) != 2 {
			t.Fatalf("expected 2 tags, got %d", len(tags))
		}
		_ = id2
	})

	t.Run("add tag to item", func(t *testing.T) {
		// Create a library item first.
		itemID, err := d.AddItem(&models.LibraryItem{
			Title:     "Tagged Book",
			Author:    "Author",
			MediaType: "ebook",
			Source:    "test",
		})
		if err != nil {
			t.Fatalf("AddItem failed: %v", err)
		}

		tags, _ := d.GetTags()
		err = d.AddItemTag(itemID, tags[0].ID)
		if err != nil {
			t.Fatalf("AddItemTag failed: %v", err)
		}

		// Get item tags.
		itemTags, err := d.GetItemTags(itemID)
		if err != nil {
			t.Fatalf("GetItemTags failed: %v", err)
		}
		if len(itemTags) != 1 {
			t.Fatalf("expected 1 tag, got %d", len(itemTags))
		}
		if itemTags[0].Name != tags[0].Name {
			t.Errorf("expected tag %s, got %s", tags[0].Name, itemTags[0].Name)
		}
	})

	t.Run("add duplicate tag to item is idempotent", func(t *testing.T) {
		items, _ := d.GetItems("", 10, 0)
		tags, _ := d.GetTags()
		// Adding same tag again should not error (INSERT OR IGNORE).
		err := d.AddItemTag(items[0].ID, tags[0].ID)
		if err != nil {
			t.Fatalf("duplicate AddItemTag should not error: %v", err)
		}
		itemTags, _ := d.GetItemTags(items[0].ID)
		if len(itemTags) != 1 {
			t.Errorf("expected still 1 tag after duplicate add, got %d", len(itemTags))
		}
	})

	t.Run("remove tag from item", func(t *testing.T) {
		items, _ := d.GetItems("", 10, 0)
		tags, _ := d.GetTags()

		err := d.RemoveItemTag(items[0].ID, tags[0].ID)
		if err != nil {
			t.Fatalf("RemoveItemTag failed: %v", err)
		}

		itemTags, _ := d.GetItemTags(items[0].ID)
		if len(itemTags) != 0 {
			t.Errorf("expected 0 tags after remove, got %d", len(itemTags))
		}
	})

	t.Run("get items by tag", func(t *testing.T) {
		items, _ := d.GetItems("", 10, 0)
		tags, _ := d.GetTags()
		d.AddItemTag(items[0].ID, tags[0].ID)

		taggedItems, err := d.GetItemsByTag(tags[0].ID, 10, 0)
		if err != nil {
			t.Fatalf("GetItemsByTag failed: %v", err)
		}
		if len(taggedItems) != 1 {
			t.Errorf("expected 1 tagged item, got %d", len(taggedItems))
		}
	})

	t.Run("delete tag cascades", func(t *testing.T) {
		tags, _ := d.GetTags()
		items, _ := d.GetItems("", 10, 0)

		err := d.DeleteTag(tags[0].ID)
		if err != nil {
			t.Fatalf("DeleteTag failed: %v", err)
		}

		// Tag should be removed from item.
		itemTags, _ := d.GetItemTags(items[0].ID)
		for _, it := range itemTags {
			if it.ID == tags[0].ID {
				t.Error("expected tag to be removed from item after delete")
			}
		}
	})

	t.Run("delete nonexistent tag", func(t *testing.T) {
		err := d.DeleteTag(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent tag")
		}
	})
}

func TestReadingHistory_CRUD(t *testing.T) {
	d := newTestDB(t)

	// Create a user for history entries.
	userID, err := d.CreateUser("reader", "hash", "user")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	t.Run("add reading entry", func(t *testing.T) {
		now := time.Now()
		rating := 5
		id, err := d.AddReadingHistory(userID, "Dune", "Frank Herbert", "epub", &now, nil, &rating, "Great book", nil)
		if err != nil {
			t.Fatalf("AddReadingHistory failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}
	})

	t.Run("list reading history", func(t *testing.T) {
		entries, err := d.GetReadingHistory(userID, "", 10, 0)
		if err != nil {
			t.Fatalf("GetReadingHistory failed: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].BookTitle != "Dune" {
			t.Errorf("expected Dune, got %s", entries[0].BookTitle)
		}
		if entries[0].Author != "Frank Herbert" {
			t.Errorf("expected Frank Herbert, got %s", entries[0].Author)
		}
		if entries[0].Status != "reading" {
			t.Errorf("expected status reading, got %s", entries[0].Status)
		}
		if entries[0].Rating == nil || *entries[0].Rating != 5 {
			t.Error("expected rating 5")
		}
	})

	t.Run("filter by status reading", func(t *testing.T) {
		entries, err := d.GetReadingHistory(userID, "reading", 10, 0)
		if err != nil {
			t.Fatalf("GetReadingHistory reading failed: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("expected 1 reading entry, got %d", len(entries))
		}
	})

	t.Run("filter by status finished", func(t *testing.T) {
		entries, err := d.GetReadingHistory(userID, "finished", 10, 0)
		if err != nil {
			t.Fatalf("GetReadingHistory finished failed: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 finished entries, got %d", len(entries))
		}
	})

	t.Run("update reading entry", func(t *testing.T) {
		entries, _ := d.GetReadingHistory(userID, "", 10, 0)
		now := time.Now()
		newRating := 4
		newNotes := "Updated notes"

		err := d.UpdateReadingHistory(entries[0].ID, userID, &now, &newRating, &newNotes)
		if err != nil {
			t.Fatalf("UpdateReadingHistory failed: %v", err)
		}

		updated, _ := d.GetReadingHistory(userID, "", 10, 0)
		if updated[0].Status != "finished" {
			t.Errorf("expected status finished after setting finishedAt, got %s", updated[0].Status)
		}
		if updated[0].Rating == nil || *updated[0].Rating != 4 {
			t.Error("expected updated rating 4")
		}
		if updated[0].Notes != "Updated notes" {
			t.Errorf("expected Updated notes, got %s", updated[0].Notes)
		}
	})

	t.Run("update nonexistent entry", func(t *testing.T) {
		now := time.Now()
		err := d.UpdateReadingHistory(99999, userID, &now, nil, nil)
		if err == nil {
			t.Error("expected error updating nonexistent entry")
		}
	})

	t.Run("reading stats", func(t *testing.T) {
		stats, err := d.GetReadingStats(userID)
		if err != nil {
			t.Fatalf("GetReadingStats failed: %v", err)
		}
		if stats["total_finished"].(int) != 1 {
			t.Errorf("expected 1 finished, got %v", stats["total_finished"])
		}
		if stats["currently_reading"].(int) != 0 {
			t.Errorf("expected 0 currently reading, got %v", stats["currently_reading"])
		}
	})

	t.Run("delete reading entry", func(t *testing.T) {
		entries, _ := d.GetReadingHistory(userID, "", 10, 0)
		err := d.DeleteReadingHistory(entries[0].ID, userID)
		if err != nil {
			t.Fatalf("DeleteReadingHistory failed: %v", err)
		}

		remaining, _ := d.GetReadingHistory(userID, "", 10, 0)
		if len(remaining) != 0 {
			t.Errorf("expected 0 entries after delete, got %d", len(remaining))
		}
	})

	t.Run("delete nonexistent entry", func(t *testing.T) {
		err := d.DeleteReadingHistory(99999, userID)
		if err == nil {
			t.Error("expected error deleting nonexistent reading history entry")
		}
	})

	t.Run("add entry with nil optional fields", func(t *testing.T) {
		id, err := d.AddReadingHistory(userID, "Simple Book", "Author", "pdf", nil, nil, nil, "", nil)
		if err != nil {
			t.Fatalf("AddReadingHistory with nils failed: %v", err)
		}
		if id <= 0 {
			t.Error("expected positive ID")
		}
	})
}

func TestMonitoredAuthors_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("add and list", func(t *testing.T) {
		id, err := d.AddMonitoredAuthor("Brandon Sanderson", 7)
		if err != nil {
			t.Fatalf("AddMonitoredAuthor failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		authors, err := d.GetMonitoredAuthors()
		if err != nil {
			t.Fatalf("GetMonitoredAuthors failed: %v", err)
		}
		if len(authors) != 1 {
			t.Fatalf("expected 1 author, got %d", len(authors))
		}
		if authors[0].Name != "Brandon Sanderson" {
			t.Errorf("expected Brandon Sanderson, got %s", authors[0].Name)
		}
		if authors[0].CheckIntervalDays != 7 {
			t.Errorf("expected interval 7, got %d", authors[0].CheckIntervalDays)
		}
		if !authors[0].LastChecked.IsZero() {
			t.Error("expected LastChecked to be zero initially")
		}
		if authors[0].LastBookFound != "" {
			t.Errorf("expected empty LastBookFound, got %s", authors[0].LastBookFound)
		}
	})

	t.Run("update last checked", func(t *testing.T) {
		authors, _ := d.GetMonitoredAuthors()
		err := d.UpdateMonitoredAuthorCheck(authors[0].ID, "Wind and Truth")
		if err != nil {
			t.Fatalf("UpdateMonitoredAuthorCheck failed: %v", err)
		}

		updated, _ := d.GetMonitoredAuthors()
		if updated[0].LastBookFound != "Wind and Truth" {
			t.Errorf("expected LastBookFound Wind and Truth, got %s", updated[0].LastBookFound)
		}
		if updated[0].LastChecked.IsZero() {
			t.Error("expected LastChecked to be set after update")
		}
	})

	t.Run("add multiple authors", func(t *testing.T) {
		d.AddMonitoredAuthor("Patrick Rothfuss", 30)
		authors, _ := d.GetMonitoredAuthors()
		if len(authors) != 2 {
			t.Errorf("expected 2 authors, got %d", len(authors))
		}
	})

	t.Run("delete author", func(t *testing.T) {
		authors, _ := d.GetMonitoredAuthors()
		err := d.DeleteMonitoredAuthor(authors[0].ID)
		if err != nil {
			t.Fatalf("DeleteMonitoredAuthor failed: %v", err)
		}

		remaining, _ := d.GetMonitoredAuthors()
		if len(remaining) != 1 {
			t.Errorf("expected 1 author after delete, got %d", len(remaining))
		}
	})

	t.Run("delete nonexistent author", func(t *testing.T) {
		err := d.DeleteMonitoredAuthor(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent monitored author")
		}
	})
}

func TestWebhookConfig_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("create and list", func(t *testing.T) {
		cfg := &webhook.Config{
			Name:    "Discord Notifications",
			URL:     "https://discord.com/api/webhooks/123/abc",
			Type:    "discord",
			Enabled: true,
			Events:  "download_complete,download_failed",
		}

		id, err := d.CreateWebhookConfig(cfg)
		if err != nil {
			t.Fatalf("CreateWebhookConfig failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		configs, err := d.GetWebhookConfigs()
		if err != nil {
			t.Fatalf("GetWebhookConfigs failed: %v", err)
		}
		if len(configs) != 1 {
			t.Fatalf("expected 1 config, got %d", len(configs))
		}
		if configs[0].Name != "Discord Notifications" {
			t.Errorf("expected name Discord Notifications, got %s", configs[0].Name)
		}
		if configs[0].URL != "https://discord.com/api/webhooks/123/abc" {
			t.Errorf("expected URL, got %s", configs[0].URL)
		}
		if configs[0].Type != "discord" {
			t.Errorf("expected type discord, got %s", configs[0].Type)
		}
		if !configs[0].Enabled {
			t.Error("expected webhook to be enabled")
		}
		if configs[0].Events != "download_complete,download_failed" {
			t.Errorf("expected events string, got %s", configs[0].Events)
		}
	})

	t.Run("create disabled webhook", func(t *testing.T) {
		cfg := &webhook.Config{
			Name:    "Generic Webhook",
			URL:     "https://example.com/hook",
			Type:    "generic",
			Enabled: false,
			Events:  "*",
		}
		d.CreateWebhookConfig(cfg)

		configs, _ := d.GetWebhookConfigs()
		if len(configs) != 2 {
			t.Fatalf("expected 2 configs, got %d", len(configs))
		}

		// Find the disabled one.
		found := false
		for _, c := range configs {
			if c.Name == "Generic Webhook" && !c.Enabled {
				found = true
			}
		}
		if !found {
			t.Error("expected to find disabled Generic Webhook")
		}
	})

	t.Run("delete webhook", func(t *testing.T) {
		configs, _ := d.GetWebhookConfigs()
		err := d.DeleteWebhookConfig(configs[0].ID)
		if err != nil {
			t.Fatalf("DeleteWebhookConfig failed: %v", err)
		}

		remaining, _ := d.GetWebhookConfigs()
		if len(remaining) != 1 {
			t.Errorf("expected 1 config after delete, got %d", len(remaining))
		}
	})

	t.Run("delete nonexistent webhook", func(t *testing.T) {
		err := d.DeleteWebhookConfig(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent webhook config")
		}
	})
}

func TestNotifications_CRUD(t *testing.T) {
	d := newTestDB(t)

	userID, err := d.CreateUser("notifyuser", "hash", "admin")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	t.Run("create and list", func(t *testing.T) {
		n := &models.Notification{
			UserID:    userID,
			Type:      "request_completed",
			Title:     "Download Complete",
			Message:   "Dune has been downloaded",
			CreatedAt: time.Now(),
		}

		id, err := d.CreateNotification(n)
		if err != nil {
			t.Fatalf("CreateNotification failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		notifications, err := d.GetNotifications(userID, 10, 0)
		if err != nil {
			t.Fatalf("GetNotifications failed: %v", err)
		}
		if len(notifications) != 1 {
			t.Fatalf("expected 1 notification, got %d", len(notifications))
		}
		if notifications[0].Title != "Download Complete" {
			t.Errorf("expected title Download Complete, got %s", notifications[0].Title)
		}
		if notifications[0].Read {
			t.Error("expected notification to be unread initially")
		}
	})

	t.Run("count unread", func(t *testing.T) {
		count, err := d.CountUnreadNotifications(userID)
		if err != nil {
			t.Fatalf("CountUnreadNotifications failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 unread, got %d", count)
		}
	})

	t.Run("mark read", func(t *testing.T) {
		notifications, _ := d.GetNotifications(userID, 10, 0)
		err := d.MarkNotificationRead(notifications[0].ID, userID)
		if err != nil {
			t.Fatalf("MarkNotificationRead failed: %v", err)
		}

		count, _ := d.CountUnreadNotifications(userID)
		if count != 0 {
			t.Errorf("expected 0 unread after marking read, got %d", count)
		}
	})

	t.Run("mark all read", func(t *testing.T) {
		// Add more notifications.
		d.CreateNotification(&models.Notification{UserID: userID, Type: "info", Title: "Test 1", CreatedAt: time.Now()})
		d.CreateNotification(&models.Notification{UserID: userID, Type: "info", Title: "Test 2", CreatedAt: time.Now()})

		err := d.MarkAllNotificationsRead(userID)
		if err != nil {
			t.Fatalf("MarkAllNotificationsRead failed: %v", err)
		}

		count, _ := d.CountUnreadNotifications(userID)
		if count != 0 {
			t.Errorf("expected 0 unread after mark all, got %d", count)
		}
	})

	t.Run("delete notification", func(t *testing.T) {
		notifications, _ := d.GetNotifications(userID, 10, 0)
		err := d.DeleteNotification(notifications[0].ID, userID)
		if err != nil {
			t.Fatalf("DeleteNotification failed: %v", err)
		}
	})

	t.Run("delete nonexistent notification", func(t *testing.T) {
		err := d.DeleteNotification(99999, userID)
		if err == nil {
			t.Error("expected error deleting nonexistent notification")
		}
	})

	t.Run("mark nonexistent notification read", func(t *testing.T) {
		err := d.MarkNotificationRead(99999, userID)
		if err == nil {
			t.Error("expected error marking nonexistent notification read")
		}
	})
}
