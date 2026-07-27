package download

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/organize"
)

func newNZBTestWatcher(t *testing.T, cfg *config.Config) (*Watcher, *db.DB) {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return &Watcher{
		cfg:       cfg,
		db:        database,
		organizer: organize.NewOrganizer(cfg),
	}, database
}

// TestImportNZBIntoLibrary is the regression for finding 3: a completed SAB
// download must be imported, not left on disk.
func TestImportNZBIntoLibrary(t *testing.T) {
	incoming := t.TempDir()
	cfg := &config.Config{IncomingDir: incoming, FileOrgEnabled: false}
	w, database := newNZBTestWatcher(t, cfg)

	if err := database.RecordNZBJob("nzo_1", "Some Book", "ebook"); err != nil {
		t.Fatal(err)
	}

	// SAB unpacked the download into INCOMING_DIR/<name>.
	jobDir := filepath.Join(incoming, "Some Book")
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "book.epub"), []byte("epub"), 0644); err != nil {
		t.Fatal(err)
	}

	w.importNZB(SABnzbdHistorySlot{NzoID: "nzo_1", Name: "Some Book", Status: "Completed"}, "ebook")

	count, err := database.CountItems("ebook")
	if err != nil || count != 1 {
		t.Fatalf("CountItems(ebook) = %d, %v; want 1 imported item", count, err)
	}
	pending, err := database.PendingNZBJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending NZB jobs = %d, want 0 after import", len(pending))
	}
}

// TestImportNZBPendingWhenNotYetOnDisk ensures a not-yet-visible download stays
// pending and is retried, rather than being marked imported with nothing to show.
func TestImportNZBPendingWhenNotYetOnDisk(t *testing.T) {
	cfg := &config.Config{IncomingDir: t.TempDir(), FileOrgEnabled: false}
	w, database := newNZBTestWatcher(t, cfg)
	if err := database.RecordNZBJob("nzo_1", "Missing", "ebook"); err != nil {
		t.Fatal(err)
	}

	w.importNZB(SABnzbdHistorySlot{NzoID: "nzo_1", Name: "Missing", Status: "Completed"}, "ebook")

	pending, err := database.PendingNZBJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1 (retry preserved)", len(pending))
	}
}

func TestResolveNZBPathPrefersStorageWhenPresent(t *testing.T) {
	storage := t.TempDir()
	cfg := &config.Config{IncomingDir: "/incoming", FileOrgEnabled: false}
	w := &Watcher{cfg: cfg}

	got := w.resolveNZBPath(SABnzbdHistorySlot{Name: "Book", Storage: storage}, "ebook")
	if got != storage {
		t.Fatalf("resolveNZBPath = %q, want storage path %q", got, storage)
	}

	// When storage is absent, fall back to the incoming dir joined with name.
	got = w.resolveNZBPath(SABnzbdHistorySlot{Name: "Book"}, "ebook")
	if got != filepath.Join("/incoming", "Book") {
		t.Fatalf("resolveNZBPath fallback = %q, want /incoming/Book", got)
	}
}

// TestCheckCompletedNZBResolvesFailedJobs ensures a failed SAB job is dropped
// from the pending list instead of lingering forever.
func TestCheckCompletedNZBResolvesFailedJobs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := SABnzbdHistoryResponse{}
		resp.History.Slots = []SABnzbdHistorySlot{
			{NzoID: "nzo_fail", Name: "Broken", Status: "Failed", FailMessage: "unpack error"},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := &config.Config{IncomingDir: t.TempDir(), SABnzbdURL: srv.URL, SABnzbdAPIKey: "k"}
	w, database := newNZBTestWatcher(t, cfg)
	w.sab = NewSABnzbdClient(cfg)
	if err := database.RecordNZBJob("nzo_fail", "Broken", "ebook"); err != nil {
		t.Fatal(err)
	}

	w.checkCompletedNZB()

	pending, err := database.PendingNZBJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %d, want 0 (failed job should be resolved)", len(pending))
	}
}

// TestCheckCompletedNZBImportsCompleted drives the full poll path against a
// fake SAB history endpoint.
func TestCheckCompletedNZBImportsCompleted(t *testing.T) {
	incoming := t.TempDir()
	jobDir := filepath.Join(incoming, "Done Book")
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "book.epub"), []byte("epub"), 0644); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := SABnzbdHistoryResponse{}
		resp.History.Slots = []SABnzbdHistorySlot{
			{NzoID: "nzo_done", Name: "Done Book", Status: "Completed"},
			{NzoID: "nzo_untracked", Name: "Not Ours", Status: "Completed"},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := &config.Config{IncomingDir: incoming, FileOrgEnabled: false, SABnzbdURL: srv.URL, SABnzbdAPIKey: "k"}
	w, database := newNZBTestWatcher(t, cfg)
	w.sab = NewSABnzbdClient(cfg)
	if err := database.RecordNZBJob("nzo_done", "Done Book", "ebook"); err != nil {
		t.Fatal(err)
	}

	w.checkCompletedNZB()

	// importNZB runs in a goroutine; wait briefly for it to finish.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, _ := database.CountItems("ebook"); c == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if c, _ := database.CountItems("ebook"); c != 1 {
		t.Fatalf("CountItems(ebook) = %d, want 1", c)
	}
}
