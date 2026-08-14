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

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/search"
)

func manualImportTestServer(t *testing.T, roots map[string]string) *Server {
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

	return &Server{cfg: cfg, db: database, searchMgr: searchMgr}
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

// TestImportFilesRejectsTraversalTitle replays the reported PoC (Mahmoud
// Hassan): POST /api/import/files with a manga title of "../../../pwned" moved
// the source file outside MangaDir and created the directory on the way.
func TestImportFilesRejectsTraversalTitle(t *testing.T) {
	dir := t.TempDir()
	ebookDir := filepath.Join(dir, "ebooks")
	mangaDir := filepath.Join(dir, "manga")
	outside := filepath.Join(dir, "pwned")

	s := manualImportTestServer(t, map[string]string{"ebook": ebookDir})
	s.cfg.MangaDir = mangaDir
	s.cfg.FileOrgEnabled = true
	if err := os.MkdirAll(mangaDir, 0755); err != nil {
		t.Fatal(err)
	}
	s.organizer = organize.NewOrganizer(s.cfg)

	src := filepath.Join(ebookDir, "librarr-upload-2915691677.epub")
	if err := os.WriteFile(src, []byte("payload"), 0644); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"files": []map[string]string{{
			"path":       src,
			"media_type": "manga",
			"title":      "../pwned",
		}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/import/files", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	rr := httptest.NewRecorder()
	s.handleImportFiles(rr, req)

	if _, err := os.Stat(outside); err == nil {
		t.Fatalf("traversal title created a directory outside MangaDir: %s", outside)
	}

	// The import itself still succeeds — the file lands under the sanitized
	// series name inside MangaDir rather than being rejected outright.
	contained := false
	_ = filepath.Walk(mangaDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			contained = true
		}
		return nil
	})
	if !contained {
		t.Errorf("expected the file to be imported inside MangaDir, found nothing under %s", mangaDir)
	}
}
