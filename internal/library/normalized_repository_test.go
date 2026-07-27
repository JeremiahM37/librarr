package library

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestNormalizedRepositoryBookEditionFileFlow(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	book, err := repo.CreateBook(ctx, Book{Title: "Dune", MediaType: MediaTypeEbook, Monitored: true, Status: BookStatusOwned})
	if err != nil {
		t.Fatal(err)
	}
	if book.ID == 0 || book.Title != "Dune" {
		t.Fatalf("book = %+v", book)
	}

	book.Description = "Updated"
	updated, err := repo.UpdateBook(ctx, *book)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Description != "Updated" {
		t.Fatalf("updated book = %+v", updated)
	}

	edition, err := repo.CreateEdition(ctx, Edition{BookID: book.ID, Title: "Dune", Publisher: "Ace", PageCount: 688})
	if err != nil {
		t.Fatal(err)
	}
	if edition.BookID != book.ID || edition.Publisher != "Ace" {
		t.Fatalf("edition = %+v", edition)
	}

	filePath := filepath.Join(t.TempDir(), "dune.epub")
	if err := osWriteTestFile(filePath); err != nil {
		t.Fatal(err)
	}
	file, err := repo.AttachFile(ctx, BookFile{
		EditionID:  edition.ID,
		MediaType:  MediaTypeEbook,
		Format:     "epub",
		Path:       filePath,
		Size:       4,
		Managed:    true,
		ImportedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if file.BookID != book.ID || file.Path != filePath {
		t.Fatalf("file = %+v", file)
	}
	if err := repo.ValidateManagedFile(ctx, file.ID); err != nil {
		t.Fatalf("ValidateManagedFile: %v", err)
	}

	files, err := repo.GetBookFiles(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].ID != file.ID {
		t.Fatalf("files = %+v", files)
	}

	moved, err := repo.MoveFile(ctx, file.ID, filepath.Join(t.TempDir(), "moved.epub"))
	if err != nil {
		t.Fatal(err)
	}
	if moved.Path == filePath {
		t.Fatalf("file was not moved: %+v", moved)
	}
	if err := repo.DetachFile(ctx, moved.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetFile(ctx, moved.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetFile after detach error = %v", err)
	}
}

func TestNormalizedRepositorySearchCountRecentAndDelete(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	first, err := repo.CreateBook(ctx, Book{Title: "The Left Hand of Darkness", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.CreateBook(ctx, Book{Title: "A Wizard of Earthsea", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}

	found, err := repo.FindBook(ctx, BookQuery{Title: "wizard", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	if found.ID != second.ID {
		t.Fatalf("found = %+v, want %d", found, second.ID)
	}
	count, err := repo.CountBooks(ctx, BookQuery{MediaType: MediaTypeEbook})
	if err != nil || count != 2 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	recent, err := repo.RecentBooks(ctx, ListBooksQuery{MediaType: MediaTypeEbook, Limit: 10})
	if err != nil || len(recent) != 2 {
		t.Fatalf("recent = %+v, err = %v", recent, err)
	}
	if err := repo.DeleteBook(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetBook(ctx, first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBook deleted error = %v", err)
	}
}

func TestNormalizedRepositoryMergeBooksMovesEditionsFilesAndKeepsTargetCover(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	target, err := repo.CreateBook(ctx, Book{Title: "Ameritopia: The Unmaking of America", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	targetEdition, err := repo.CreateEdition(ctx, Edition{BookID: target.ID, Title: target.Title})
	if err != nil {
		t.Fatal(err)
	}
	source, err := repo.CreateBook(ctx, Book{Title: "Ameritopia-The Unmaking of America", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	sourceEdition, err := repo.CreateEdition(ctx, Edition{BookID: source.ID, Title: source.Title})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachContributor(ctx, targetEdition.ID, Contributor{Name: "Mark R. Levin", Roles: []ContributorRole{RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachContributor(ctx, sourceEdition.ID, Contributor{Name: "Mark R. Levin", Roles: []ContributorRole{RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachContributor(ctx, sourceEdition.ID, Contributor{Name: "Source Narrator", Roles: []ContributorRole{RoleNarrator}}); err != nil {
		t.Fatal(err)
	}
	targetCoverPath := filepath.Join(t.TempDir(), "target-cover.jpg")
	if err := os.WriteFile(targetCoverPath, []byte("target cover"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AttachCover(ctx, Cover{BookID: target.ID, LocalPath: targetCoverPath, MimeType: "image/jpeg", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AttachCover(ctx, Cover{BookID: source.ID, LocalPath: filepath.Join(t.TempDir(), "source-cover.jpg"), MimeType: "image/jpeg", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AttachFile(ctx, BookFile{EditionID: targetEdition.ID, MediaType: MediaTypeEbook, Format: "epub", Path: filepath.Join(t.TempDir(), "ameritopia.epub")}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AttachFile(ctx, BookFile{EditionID: sourceEdition.ID, MediaType: MediaTypeEbook, Format: "mobi", Path: filepath.Join(t.TempDir(), "ameritopia.mobi")}); err != nil {
		t.Fatal(err)
	}

	merged, err := repo.MergeBooks(ctx, source.ID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if merged.ID != target.ID {
		t.Fatalf("merged book id = %d, want %d", merged.ID, target.ID)
	}
	if _, err := repo.GetBook(ctx, source.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("source book error = %v", err)
	}
	files, err := repo.GetBookFiles(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
	contributors, err := repo.GetEditionContributors(ctx, targetEdition.ID)
	if err != nil {
		t.Fatal(err)
	}
	foundNarrator := false
	for _, contributor := range contributors {
		if contributor.Name == "Source Narrator" {
			foundNarrator = true
		}
	}
	if !foundNarrator {
		t.Fatalf("source-only contributor was not moved: %+v", contributors)
	}
	cover, err := repo.GetPrimaryCover(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cover.LocalPath != targetCoverPath {
		t.Fatalf("cover path = %q, want %q", cover.LocalPath, targetCoverPath)
	}
}

func TestNormalizedRepositoryContributorsSeriesIdentifiersAndCovers(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	book, _ := repo.CreateBook(ctx, Book{Title: "The Fellowship of the Ring", MediaType: MediaTypeEbook})
	edition, _ := repo.CreateEdition(ctx, Edition{BookID: book.ID, Title: "The Fellowship of the Ring"})

	if err := repo.AttachContributor(ctx, edition.ID, Contributor{Name: "J. R. R. Tolkien", Roles: []ContributorRole{RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachContributor(ctx, edition.ID, Contributor{Name: "Narrator Person", Roles: []ContributorRole{RoleNarrator}}); err != nil {
		t.Fatal(err)
	}
	contributors, err := repo.GetEditionContributors(ctx, edition.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(contributors) != 2 {
		t.Fatalf("contributors = %+v", contributors)
	}

	if err := repo.AttachBook(ctx, book.ID, BookSeries{Series: Series{Title: "The Lord of the Rings"}, Position: 1, DisplayPosition: "1"}); err != nil {
		t.Fatal(err)
	}
	series, err := repo.GetSeries(ctx, "the lord of the rings")
	if err != nil {
		t.Fatal(err)
	}
	pos, err := repo.SeriesPosition(ctx, book.ID, series.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pos.Position != 1 {
		t.Fatalf("series position = %+v", pos)
	}

	identifier, err := repo.AddIdentifier(ctx, Identifier{Scope: IdentifierScopeBook, Provider: "isbn13", Value: "9780000000001", Source: fmt.Sprint(book.ID)})
	if err != nil {
		t.Fatal(err)
	}
	matches, err := repo.FindByIdentifier(ctx, *identifier)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].BookID != book.ID {
		t.Fatalf("matches = %+v", matches)
	}
	if _, err := repo.AddIdentifier(ctx, Identifier{Scope: IdentifierScopeBook, Provider: "isbn13", Value: "9780000000001", Source: fmt.Sprint(book.ID)}); !errors.Is(err, ErrDuplicateBook) {
		t.Fatalf("duplicate identifier error = %v", err)
	}

	cover, err := repo.AttachCover(ctx, Cover{BookID: book.ID, LocalPath: "/covers/fellowship.jpg", MimeType: "image/jpeg", IsPrimary: true})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := repo.PrimaryCover(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if primary.ID != cover.ID {
		t.Fatalf("primary cover = %+v, want %+v", primary, cover)
	}
	replaced, err := repo.ReplaceCover(ctx, Cover{ID: cover.ID, BookID: book.ID, LocalPath: "/covers/fellowship-2.jpg", MimeType: "image/jpeg", IsPrimary: true})
	if err != nil {
		t.Fatal(err)
	}
	if replaced.LocalPath != "/covers/fellowship-2.jpg" {
		t.Fatalf("replaced cover = %+v", replaced)
	}
	if err := repo.RemoveCover(ctx, replaced.ID); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizedRepositoryTransactionsAndFactory(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	err := repo.WithinTransaction(ctx, func(txCtx context.Context) error {
		if _, err := repo.CreateBook(txCtx, Book{Title: "Rollback Me", MediaType: MediaTypeEbook}); err != nil {
			return err
		}
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
	count, err := repo.CountBooks(ctx, BookQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count after rollback = %d", count)
	}

	factoryRepo, err := NewRepository(NormalizedRepositoryMode, repo.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := factoryRepo.(*NormalizedRepository); !ok {
		t.Fatalf("factory repo type = %T", factoryRepo)
	}
	legacyRepo, err := NewRepository(LegacyRepositoryMode, repo.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := legacyRepo.(*LegacyLibraryRepository); !ok {
		t.Fatalf("legacy factory repo type = %T", legacyRepo)
	}
}

func TestNormalizedRepositoryListBookReadModelsUsesBoundedQueries(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		book, err := repo.CreateBook(ctx, Book{
			Title:     fmt.Sprintf("Book %d", i),
			SortTitle: fmt.Sprintf("Book %d", i),
			MediaType: MediaTypeEbook,
			Status:    BookStatusOwned,
		})
		if err != nil {
			t.Fatal(err)
		}
		edition, err := repo.CreateEdition(ctx, Edition{BookID: book.ID, Title: book.Title})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.AttachContributor(ctx, edition.ID, Contributor{Name: fmt.Sprintf("Author %d", i), Roles: []ContributorRole{RoleAuthor}}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.AttachFile(ctx, BookFile{
			EditionID:   edition.ID,
			MediaType:   MediaTypeEbook,
			Format:      "epub",
			Path:        fmt.Sprintf("/books/book-%d.epub", i),
			ContentHash: fmt.Sprintf("hash-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	queryCount := 0
	repo.queryHook = func() { queryCount++ }
	defer func() { repo.queryHook = nil }()

	items, err := repo.ListBookReadModels(ctx, ListBooksQuery{MediaType: MediaTypeEbook, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 5 {
		t.Fatalf("items = %d", len(items))
	}
	if queryCount > 6 {
		t.Fatalf("expected bounded query count, got %d", queryCount)
	}
}

func TestNormalizedRepositoryBookMetadataPatchAndProvenance(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	book, err := repo.CreateBook(ctx, Book{Title: "Project Hail Mary", MediaType: MediaTypeEbook, Description: "Old"})
	if err != nil {
		t.Fatal(err)
	}
	edition, err := repo.CreateEdition(ctx, Edition{BookID: book.ID, Title: "Project Hail Mary"})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := repo.PatchBookMetadata(ctx, book.ID, BookMetadataPatch{
		Fields: map[MetadataField]string{
			MetadataFieldTitle:       "Project Hail Mary (Edited)",
			MetadataFieldSubtitle:    "A Novel",
			MetadataFieldDescription: "A better description",
			MetadataFieldGenres:      "Science Fiction, Space Opera, Science Fiction",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Fields[MetadataFieldTitle].Value != "Project Hail Mary (Edited)" || !updated.Fields[MetadataFieldTitle].ManualOverride {
		t.Fatalf("title metadata = %+v", updated.Fields[MetadataFieldTitle])
	}
	if updated.Fields[MetadataFieldGenres].Value != "Science Fiction, Space Opera" {
		t.Fatalf("genres metadata = %+v", updated.Fields[MetadataFieldGenres])
	}

	reloadedBook, err := repo.GetBook(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedBook.Title != "Project Hail Mary (Edited)" || reloadedBook.Description != "A better description" {
		t.Fatalf("book sync = %+v", reloadedBook)
	}
	reloadedEdition, err := repo.GetEdition(ctx, edition.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedEdition.Subtitle != "A Novel" {
		t.Fatalf("edition sync = %+v", reloadedEdition)
	}

	provenance, err := repo.GetBookProvenance(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(provenance.Fields[MetadataFieldTitle]) != 1 || !provenance.Fields[MetadataFieldTitle][0].Selected {
		t.Fatalf("title provenance = %+v", provenance.Fields[MetadataFieldTitle])
	}
}

func TestNormalizedRepositoryApplyBookMetadataSourcePreservesManualOverrides(t *testing.T) {
	repo, cleanup := newNormalizedRepo(t)
	defer cleanup()
	ctx := context.Background()

	book, err := repo.CreateBook(ctx, Book{Title: "The Martian", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateEdition(ctx, Edition{BookID: book.ID, Title: "The Martian"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PatchBookMetadata(ctx, book.ID, BookMetadataPatch{
		Fields: map[MetadataField]string{MetadataFieldTitle: "The Martian (Manual)"},
	}); err != nil {
		t.Fatal(err)
	}

	metadata, err := repo.ApplyBookMetadataSource(ctx, MetadataUpdate{
		BookID:     book.ID,
		Source:     "OpenLibrary",
		Confidence: ConfidenceHigh,
		Fields: map[MetadataField]string{
			MetadataFieldTitle:       "The Martian",
			MetadataFieldDescription: "Mars survival story",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Fields[MetadataFieldTitle].Value != "The Martian (Manual)" {
		t.Fatalf("manual override was replaced: %+v", metadata.Fields[MetadataFieldTitle])
	}
	if metadata.Fields[MetadataFieldDescription].Value != "Mars survival story" {
		t.Fatalf("provider description not applied: %+v", metadata.Fields[MetadataFieldDescription])
	}

	provenance, err := repo.GetBookProvenance(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(provenance.Fields[MetadataFieldTitle]) < 2 {
		t.Fatalf("expected competing title evidence, got %+v", provenance.Fields[MetadataFieldTitle])
	}
}

func newNormalizedRepo(t *testing.T) (*NormalizedRepository, func()) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "normalized.db")+"?_busy_timeout=10000")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range normalizedTestSchema {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("schema failed: %v\n%s", err, stmt)
		}
	}
	repo, err := NewNormalizedRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	return repo, func() { _ = db.Close() }
}

func osWriteTestFile(path string) error {
	return os.WriteFile(path, []byte("test"), 0644)
}

var normalizedTestSchema = []string{
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
	`CREATE TABLE metadata_values (
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
	`CREATE TABLE metadata_evidence (
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
	`CREATE UNIQUE INDEX idx_covers_primary_book ON covers(book_id) WHERE book_id IS NOT NULL AND is_primary = 1`,
	`CREATE UNIQUE INDEX idx_covers_primary_edition ON covers(edition_id) WHERE edition_id IS NOT NULL AND is_primary = 1`,
}
