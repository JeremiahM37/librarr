package download

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	"github.com/jamie75/librarr/internal/organize"
	"github.com/jamie75/librarr/internal/search"
)

// TestPipelineDirectDownloadOrganizeImport exercises search → download → organize → DB import
// without external network dependencies.
func TestPipelineDirectDownloadOrganizeImport(t *testing.T) {
	epubHeader := []byte{0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00}
	payload := append(epubHeader, make([]byte, 2000)...)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/epub+zip")
		w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := &config.Config{
		IncomingDir:    filepath.Join(dir, "incoming"),
		EbookDir:       filepath.Join(dir, "ebooks"),
		FileOrgEnabled: true,
		UserAgent:      "pipeline-test",
		MaxRetries:     0,
	}

	database, err := db.New(filepath.Join(dir, "pipeline.db"))
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	defer database.Close()

	health := search.NewHealthTracker(3, 300)
	direct := NewDirectDownloader(cfg, srv.Client())
	direct.validate = nil // httptest serves on loopback; not exercising the SSRF guard here
	organizer := organize.NewOrganizer(cfg)
	mgr := NewManager(cfg, database, nil, nil, direct, organizer, nil, health)

	job, err := mgr.StartDirectDownload(srv.URL, "Pipeline Test Book", "test", "pipeline-test-1", "")
	if err != nil {
		t.Fatalf("StartDirectDownload: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	var finalStatus string
	for time.Now().Before(deadline) {
		mgr.mu.Lock()
		j, ok := mgr.jobs[job.ID]
		if ok {
			finalStatus = j.Status
		}
		mgr.mu.Unlock()
		if finalStatus == "completed" || finalStatus == "error" || finalStatus == "dead_letter" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalStatus != "completed" {
		t.Fatalf("expected completed status, got %q", finalStatus)
	}

	if !database.HasSourceID("pipeline-test-1") {
		t.Fatal("expected library item with source_id pipeline-test-1")
	}

	items, err := database.FindByTitle("Pipeline Test Book")
	if err != nil || len(items) == 0 {
		t.Fatalf("expected library item for title, err=%v len=%d", err, len(items))
	}
}

func TestPipelineDirectDownloadUsesConfiguredImportEngine(t *testing.T) {
	epubHeader := []byte{0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00}
	payload := append(epubHeader, make([]byte, 2000)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/epub+zip")
		w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := &config.Config{
		IncomingDir:    filepath.Join(dir, "incoming"),
		EbookDir:       filepath.Join(dir, "ebooks"),
		FileOrgEnabled: true,
		UserAgent:      "pipeline-test",
		MaxRetries:     0,
	}

	database, err := db.New(filepath.Join(dir, "pipeline.db"))
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	defer database.Close()

	health := search.NewHealthTracker(3, 300)
	direct := NewDirectDownloader(cfg, srv.Client())
	direct.validate = nil
	organizer := organize.NewOrganizer(cfg)
	engine := &managerSpyImportEngine{result: &libraryimport.EngineResult{InsertedCount: 1}}
	mgr := NewManagerWithImportEngine(cfg, database, nil, nil, direct, organizer, nil, health, nil, engine, libraryimport.EngineModeV2)

	job, err := mgr.StartDirectDownload(srv.URL, "Pipeline V2 Book", "test", "pipeline-v2-1", "Jane Doe")
	if err != nil {
		t.Fatalf("StartDirectDownload: %v", err)
	}

	waitForJobStatus(t, mgr, job.ID, "completed")

	if len(engine.requests) != 1 {
		t.Fatalf("engine requests = %d, want 1", len(engine.requests))
	}
	got := engine.requests[0]
	if got.Source.Name != "test" || got.Source.SourceID != "pipeline-v2-1" || got.Source.MediaType != library.MediaTypeEbook {
		t.Fatalf("request source = %+v", got.Source)
	}
	if got.TitleHint != "Pipeline V2 Book" {
		t.Fatalf("title hint = %q", got.TitleHint)
	}
}

func TestPipelineDirectDownloadDoesNotImportWhenOrganizationFails(t *testing.T) {
	epubHeader := []byte{0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00}
	payload := append(epubHeader, make([]byte, 2000)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/epub+zip")
		w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	ebookRoot := filepath.Join(dir, "ebooks-as-file")
	if err := os.WriteFile(ebookRoot, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		IncomingDir:    filepath.Join(dir, "incoming"),
		EbookDir:       ebookRoot,
		FileOrgEnabled: true,
		UserAgent:      "pipeline-test",
		MaxRetries:     0,
	}

	database, err := db.New(filepath.Join(dir, "pipeline.db"))
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	defer database.Close()

	health := search.NewHealthTracker(3, 300)
	direct := NewDirectDownloader(cfg, srv.Client())
	direct.validate = nil
	organizer := organize.NewOrganizer(cfg)
	mgr := NewManager(cfg, database, nil, nil, direct, organizer, nil, health)

	job, err := mgr.StartDirectDownload(srv.URL, "Organization Failure Book", "test", "org-failure-1", "")
	if err != nil {
		t.Fatalf("StartDirectDownload: %v", err)
	}

	waitForJobTerminalStatus(t, mgr, job.ID, "error")

	if database.HasSourceID("org-failure-1") {
		t.Fatal("library item should not be inserted when organization fails")
	}
}

func waitForJobStatus(t *testing.T, mgr *Manager, jobID, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mgr.mu.Lock()
		job := mgr.jobs[jobID]
		status := ""
		if job != nil {
			status = job.Status
		}
		mgr.mu.Unlock()
		if status == want {
			return
		}
		if status == "error" || status == "dead_letter" {
			t.Fatalf("job reached unexpected terminal status %q", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job status %q", want)
}

func waitForJobTerminalStatus(t *testing.T, mgr *Manager, jobID, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mgr.mu.Lock()
		job := mgr.jobs[jobID]
		status := ""
		if job != nil {
			status = job.Status
		}
		mgr.mu.Unlock()
		if status == want {
			return
		}
		if status == "completed" || status == "error" || status == "dead_letter" {
			t.Fatalf("job reached terminal status %q, want %q", status, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job status %q", want)
}

type managerSpyImportEngine struct {
	requests []libraryimport.ImportRequest
	result   *libraryimport.EngineResult
	err      error
}

func (s *managerSpyImportEngine) Import(_ context.Context, request libraryimport.ImportRequest) (*libraryimport.EngineResult, error) {
	s.requests = append(s.requests, request)
	if s.result == nil {
		s.result = &libraryimport.EngineResult{InsertedCount: 1}
	}
	return s.result, s.err
}
