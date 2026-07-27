package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
)

func TestLibraryHandlerUsesSelectedNormalizedRepository(t *testing.T) {
	d, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seedAPIRepositorySwitchData(t, d)

	cfg := &config.Config{LibraryRepositoryMode: "normalized"}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: cfg, db: d, libraryService: selection.LibraryService}

	req := httptest.NewRequest(http.MethodGet, "/api/library", nil)
	rr := httptest.NewRecorder()
	s.handleLibrary(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Total != 1 || len(body.Items) != 1 {
		t.Fatalf("body = %+v", body)
	}
	if got := body.Items[0]["title"]; got != "Normalized Handler Title" {
		t.Fatalf("title = %v", got)
	}
}

func TestExportLibraryUsesSelectedNormalizedRepository(t *testing.T) {
	d, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seedAPIRepositorySwitchData(t, d)

	cfg := &config.Config{LibraryRepositoryMode: "normalized"}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: cfg, db: d, libraryService: selection.LibraryService}

	req := httptest.NewRequest(http.MethodGet, "/api/export/library", nil)
	rr := httptest.NewRecorder()
	s.handleExportLibrary(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body ExportEnvelope
	raw := struct {
		Version    string           `json:"version"`
		ExportedAt string           `json:"exported_at"`
		ItemCount  int              `json:"item_count"`
		Items      []map[string]any `json:"items"`
	}{}
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	body.ItemCount = raw.ItemCount
	if body.ItemCount != 1 || len(raw.Items) != 1 {
		t.Fatalf("export body = %+v", raw)
	}
	if got := raw.Items[0]["title"]; got != "Normalized Handler Title" {
		t.Fatalf("exported title = %v", got)
	}
}

func TestImportLibraryUsesSelectedNormalizedRepository(t *testing.T) {
	d, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seedCompletedEmptyBackfill(t, d)

	cfg := &config.Config{LibraryRepositoryMode: "normalized"}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: cfg, db: d, libraryService: selection.LibraryService}

	body := []byte(`{"items":[{"title":"Imported Normalized","author":"Selected Repo","file_path":"/books/imported.epub","file_format":"epub","media_type":"ebook","source":"json","source_id":"json-1"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/import/library", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleImportLibrary(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["imported"].(float64) != 1 || resp["skipped"].(float64) != 0 {
		t.Fatalf("import response = %+v", resp)
	}
	var normalizedBooks int
	if err := d.SQLDB().QueryRow(`SELECT COUNT(*) FROM books WHERE title = 'Imported Normalized'`).Scan(&normalizedBooks); err != nil {
		t.Fatal(err)
	}
	if normalizedBooks != 1 {
		t.Fatalf("normalized book count = %d", normalizedBooks)
	}
	var legacyRows int
	if err := d.SQLDB().QueryRow(`SELECT COUNT(*) FROM library_items`).Scan(&legacyRows); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 0 {
		t.Fatalf("legacy library_items count = %d", legacyRows)
	}
}

func TestNewServerWithLibraryServiceRequiresExplicitService(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewServerWithLibraryService(&config.Config{}, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestServerLibraryPanicsWhenNormalizedModeMissingService(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	s := &Server{cfg: &config.Config{LibraryRepositoryMode: "normalized"}}
	_ = s.library()
}

func seedAPIRepositorySwitchData(t *testing.T, d *db.DB) {
	t.Helper()
	legacyRes, err := d.SQLDB().Exec(`INSERT INTO library_items (title, author, file_path, file_format, media_type, source_id)
		VALUES ('Legacy Handler Title', 'Legacy Author', '/books/legacy.epub', 'epub', 'ebook', 'api-source-1')`)
	if err != nil {
		t.Fatal(err)
	}
	legacyID, err := legacyRes.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO books (id, title, sort_title, media_type, status) VALUES (1, 'Normalized Handler Title', 'normalized handler title', 'ebook', 'owned')`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO editions (id, book_id, title) VALUES (1, 1, 'Normalized Handler Title')`); err != nil {
		t.Fatal(err)
	}
	fileRes, err := d.SQLDB().Exec(`INSERT INTO files (edition_id, media_type, format, file_path, original_path, file_size, source_id, source_type, embedded_metadata_json)
		VALUES (1, 'ebook', 'epub', '/books/normalized-handler.epub', '/incoming/normalized-handler.epub', 42, 'api-source-1', 'test', '{}')`)
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := fileRes.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO backfill_runs (version, status, dry_run, report_json, completed_at)
		VALUES ('librarr-2-backfill-v1', 'completed', 0, '{"validation":{"ok":true,"errors":[]},"errors":[]}', datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO backfill_state (legacy_item_id, run_id, status, book_id, edition_id, file_id)
		VALUES (?, 1, 'completed', 1, 1, ?)`, legacyID, fileID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`INSERT INTO library_item_migration_map (library_item_id, book_id, edition_id, file_id, status, created_at, updated_at)
		VALUES (?, 1, 1, ?, 'completed', datetime('now'), datetime('now'))`, legacyID, fileID); err != nil {
		t.Fatal(err)
	}
}

func seedCompletedEmptyBackfill(t *testing.T, d *db.DB) {
	t.Helper()
	if _, err := d.SQLDB().Exec(`INSERT INTO backfill_runs (version, status, dry_run, report_json, completed_at)
		VALUES ('librarr-2-backfill-v1', 'completed', 0, '{"validation":{"ok":true,"errors":[]},"errors":[]}', datetime('now'))`); err != nil {
		t.Fatal(err)
	}
}
