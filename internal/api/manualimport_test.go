package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	"github.com/jamie75/librarr/internal/organize"
	"github.com/jamie75/librarr/internal/search"
)

func manualImportTestServer(t *testing.T, roots map[string]string) *Server {
	return manualImportTestServerWithEngine(t, roots, nil)
}

func manualImportTestServerWithEngine(t *testing.T, roots map[string]string, engine libraryimport.ImportEngine) *Server {
	t.Helper()
	dir := t.TempDir()

	cfg := &config.Config{
		EbookDir:     roots["ebook"],
		AudiobookDir: roots["audiobook"],
		IncomingDir:  roots["incoming"],
	}

	for k, v := range roots {
		if v == "" {
			continue
		}
		if err := os.MkdirAll(v, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", k, err)
		}
	}

	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	health := search.NewHealthTracker(3, 300)
	searchMgr := search.NewManager(cfg, nil, health)
	organizer := organize.NewOrganizer(cfg)
	if engine == nil {
		engine = libraryimport.NewLegacyImportEngine(database)
	}

	return &Server{cfg: cfg, db: database, searchMgr: searchMgr, organizer: organizer, importEngine: engine}
}

func TestValidateAllowedPath_RejectsOutsideRoots(t *testing.T) {
	dir := t.TempDir()
	allowed := filepath.Join(dir, "books")
	s := manualImportTestServer(t, map[string]string{"ebook": allowed})

	body, _ := json.Marshal(map[string]string{"path": "/etc"})
	req := httptest.NewRequest(http.MethodPost, "/api/import/scan", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	rr := httptest.NewRecorder()
	s.handleScanImport(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestValidateAllowedPath_AllowsInsideRoot(t *testing.T) {
	dir := t.TempDir()
	allowed := filepath.Join(dir, "books")
	s := manualImportTestServer(t, map[string]string{"ebook": allowed})

	body, _ := json.Marshal(map[string]string{"path": allowed})
	req := httptest.NewRequest(http.MethodPost, "/api/import/scan", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	rr := httptest.NewRecorder()
	s.handleScanImport(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestValidateTestURL_AllowsHomelabHosts(t *testing.T) {
	cases := []string{
		"http://127.0.0.1:8080",
		"http://localhost:9696",
		"http://192.168.70.100:1111",
		"http://10.0.0.1/",
		"https://prowlarr.example:9696",
	}
	for _, u := range cases {
		if err := validateTestURL(u); err != nil {
			t.Errorf("validateTestURL(%q) should pass for homelab integration tests, got %v", u, err)
		}
	}
}

func TestValidateTestURL_BlocksMetadata(t *testing.T) {
	cases := []string{
		"http://metadata.google.internal/",
		"http://169.254.169.254/latest/meta-data/",
	}
	for _, u := range cases {
		if err := validateTestURL(u); err == nil {
			t.Errorf("validateTestURL(%q) should fail", u)
		}
	}
}

func TestHandleImportFilesUsesConfiguredEngine(t *testing.T) {
	dir := t.TempDir()
	roots := map[string]string{
		"ebook":    filepath.Join(dir, "library"),
		"incoming": filepath.Join(dir, "incoming"),
	}
	sourceFile := filepath.Join(roots["incoming"], "Example.epub")
	if err := os.MkdirAll(filepath.Dir(sourceFile), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourceFile, []byte("epub"), 0644); err != nil {
		t.Fatal(err)
	}

	engine := &spyImportEngine{result: &libraryimport.EngineResult{InsertedCount: 1}}
	s := manualImportTestServerWithEngine(t, roots, engine)

	body, _ := json.Marshal(map[string]any{
		"files": []map[string]any{{
			"path":       sourceFile,
			"title":      "Example Book",
			"author":     "Jane Doe",
			"media_type": "ebook",
			"copy":       false,
		}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/import/files", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	rr := httptest.NewRecorder()

	s.handleImportFiles(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(engine.requests) != 1 {
		t.Fatalf("engine requests = %d, want 1", len(engine.requests))
	}
	got := engine.requests[0]
	if got.Source.Name != "manual_import" || got.Source.MediaType != library.MediaTypeEbook {
		t.Fatalf("request source = %+v", got.Source)
	}
	if got.OriginalPath != sourceFile {
		t.Fatalf("original path = %q, want %q", got.OriginalPath, sourceFile)
	}
	if got.TitleHint != "Example Book" || got.AuthorHint != "Jane Doe" {
		t.Fatalf("request hints = %+v", got)
	}
	if got.RootPath != sourceFile && filepath.Dir(got.RootPath) != filepath.Join(roots["ebook"], "Jane Doe") {
		t.Fatalf("root path = %q", got.RootPath)
	}
}

type spyImportEngine struct {
	requests []libraryimport.ImportRequest
	result   *libraryimport.EngineResult
	err      error
}

func (s *spyImportEngine) Import(_ context.Context, request libraryimport.ImportRequest) (*libraryimport.EngineResult, error) {
	s.requests = append(s.requests, request)
	if s.result == nil {
		s.result = &libraryimport.EngineResult{InsertedCount: 1}
	}
	return s.result, s.err
}
