package download

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/search"
)

// A minimal-but-sniffable EPUB (zip magic) and PDF (%PDF- magic) so the
// direct downloader records the real format of what landed on disk.
var (
	fakeEPUB = append([]byte{0x50, 0x4B, 0x03, 0x04}, bytes.Repeat([]byte("fake epub body for tests "), 4096)...)
	fakePDF  = append([]byte("%PDF-1.4 "), bytes.Repeat([]byte("fake pdf body for tests "), 4096)...)
)

type wantedHarness struct {
	t        *testing.T
	cfg      *config.Config
	db       *db.DB
	manager  *Manager
	server   *httptest.Server
	servePDF atomic.Bool
}

func newWantedHarness(t *testing.T) *wantedHarness {
	t.Helper()
	dir := t.TempDir()
	h := &wantedHarness{t: t}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.servePDF.Load() {
			w.Header().Set("Content-Type", "application/pdf")
			_, _ = w.Write(fakePDF)
			return
		}
		w.Header().Set("Content-Type", "application/epub+zip")
		_, _ = w.Write(fakeEPUB)
	}))
	t.Cleanup(h.server.Close)

	h.cfg = &config.Config{
		IncomingDir:    filepath.Join(dir, "incoming"),
		EbookDir:       filepath.Join(dir, "ebooks"),
		AudiobookDir:   filepath.Join(dir, "audiobooks"),
		FileOrgEnabled: true,
		UserAgent:      "test",
		MaxRetries:     0,
	}
	var err error
	h.db, err = db.New(filepath.Join(dir, "librarr.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.db.Close() })

	direct := NewDirectDownloader(h.cfg, h.server.Client())
	direct.validate = nil // the stub lives on 127.0.0.1
	h.manager = NewManager(h.cfg, h.db, nil, nil, direct, organize.NewOrganizer(h.cfg), nil, search.NewHealthTracker(3, 300))
	return h
}

func (h *wantedHarness) waitJob(job *models.DownloadJob, want string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.manager.mu.Lock()
		status := job.Status
		h.manager.mu.Unlock()
		if status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()
	h.t.Fatalf("job %s stuck at %q (%s / %s), want %q", job.ID, job.Status, job.Detail, job.Error, want)
}

func libraryFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// TestWantedGrab_UpgradeReplacesOldFile is the arr loop end to end at the
// manager level: a PDF grab satisfies the row, a later EPUB grab upgrades it,
// and the PDF is retired from disk and from the library table.
func TestWantedGrab_UpgradeReplacesOldFile(t *testing.T) {
	h := newWantedHarness(t)
	wantedID, err := h.db.AddWishlistItem("Format Ladder", "Test Author", "ebook")
	if err != nil {
		t.Fatal(err)
	}

	// 1. PDF lands first.
	h.servePDF.Store(true)
	job, err := h.manager.StartDirectDownloadFor(h.server.URL+"/book", "Format Ladder", "gutenberg", "gutenberg-1", "Test Author", wantedID)
	if err != nil {
		t.Fatal(err)
	}
	if job.WantedID != wantedID {
		t.Fatalf("job should carry wanted id, got %d", job.WantedID)
	}
	if saved, _ := h.db.GetJob(job.ID); saved == nil || saved.WantedID != wantedID {
		t.Fatalf("persisted job lost wanted id: %+v", saved)
	}
	h.waitJob(job, "completed")

	w, err := h.db.GetWishlistItem(wantedID)
	if err != nil {
		t.Fatal(err)
	}
	if w.LibraryItemID == 0 || w.CurrentFormat != "pdf" || w.ActiveJobID != "" {
		t.Fatalf("after PDF grab: %+v", w)
	}
	pdfItem, _ := h.db.GetItem(w.LibraryItemID)
	if pdfItem.FileFormat != "pdf" || filepath.Ext(pdfItem.FilePath) != ".pdf" {
		t.Fatalf("library should record the real format, got %+v", pdfItem)
	}
	if files := libraryFiles(t, h.cfg.EbookDir); len(files) != 1 {
		t.Fatalf("expected 1 library file, got %v", files)
	}

	// 2. EPUB upgrade lands.
	h.servePDF.Store(false)
	job2, err := h.manager.StartDirectDownloadFor(h.server.URL+"/book", "Format Ladder", "gutenberg", "gutenberg-1", "Test Author", wantedID)
	if err != nil {
		t.Fatal(err)
	}
	h.waitJob(job2, "completed")

	w, _ = h.db.GetWishlistItem(wantedID)
	if w.CurrentFormat != "epub" || w.LibraryItemID == pdfItem.ID {
		t.Fatalf("after EPUB upgrade: %+v", w)
	}
	if _, err := h.db.GetItem(pdfItem.ID); err == nil {
		t.Fatal("superseded PDF row should be gone")
	}
	if _, err := os.Stat(pdfItem.FilePath); !os.IsNotExist(err) {
		t.Fatalf("superseded PDF file should be deleted: %v", err)
	}
	files := libraryFiles(t, h.cfg.EbookDir)
	if len(files) != 1 || filepath.Ext(files[0]) != ".epub" {
		t.Fatalf("library should hold exactly the EPUB, got %v", files)
	}
	if n, _ := h.db.CountItems("ebook"); n != 1 {
		t.Fatalf("expected 1 library row, got %d", n)
	}
	events, _ := h.db.GetActivity(20, 0)
	sawUpgrade := false
	for _, e := range events {
		if e.EventType == "wanted_upgraded" {
			sawUpgrade = true
		}
	}
	if !sawUpgrade {
		t.Fatal("expected a wanted_upgraded activity event")
	}
}

func TestWantedGrab_KeepOldFilesLeavesBoth(t *testing.T) {
	h := newWantedHarness(t)
	h.cfg.UpgradeKeepOldFiles = true
	wantedID, _ := h.db.AddWishlistItem("Keeper", "", "ebook")

	h.servePDF.Store(true)
	job, _ := h.manager.StartDirectDownloadFor(h.server.URL+"/k", "Keeper", "gutenberg", "g-2", "", wantedID)
	h.waitJob(job, "completed")
	h.servePDF.Store(false)
	job2, _ := h.manager.StartDirectDownloadFor(h.server.URL+"/k", "Keeper", "gutenberg", "g-2", "", wantedID)
	h.waitJob(job2, "completed")

	w, _ := h.db.GetWishlistItem(wantedID)
	if w.CurrentFormat != "epub" {
		t.Fatalf("row should point at the EPUB: %+v", w)
	}
	if n, _ := h.db.CountItems("ebook"); n != 2 {
		t.Fatalf("both rows should remain, got %d", n)
	}
	if files := libraryFiles(t, h.cfg.EbookDir); len(files) != 2 {
		t.Fatalf("both files should remain, got %v", files)
	}
}

func TestWantedGrab_FailureReleasesActiveJob(t *testing.T) {
	h := newWantedHarness(t)
	wantedID, _ := h.db.AddWishlistItem("Unreachable", "", "ebook")
	_ = h.db.SetWishlistActiveJob(wantedID, "placeholder")
	h.server.Close() // every request now fails

	job, err := h.manager.StartDirectDownloadFor(h.server.URL+"/gone", "Unreachable", "gutenberg", "g-3", "", wantedID)
	if err != nil {
		t.Fatal(err)
	}
	_ = h.db.SetWishlistActiveJob(wantedID, job.ID)
	h.waitJob(job, "error")

	w, _ := h.db.GetWishlistItem(wantedID)
	if w.ActiveJobID != "" || w.LibraryItemID != 0 {
		t.Fatalf("failed grab must release the row: %+v", w)
	}
}

func TestWantedGrab_PlainDownloadDoesNotTouchWishlist(t *testing.T) {
	h := newWantedHarness(t)
	wantedID, _ := h.db.AddWishlistItem("Bystander", "", "ebook")
	job, err := h.manager.StartDirectDownload(h.server.URL+"/b", "Bystander", "gutenberg", "g-4", "")
	if err != nil {
		t.Fatal(err)
	}
	h.waitJob(job, "completed")
	w, _ := h.db.GetWishlistItem(wantedID)
	if w.LibraryItemID != 0 {
		t.Fatalf("a grab started without a wanted id must not link rows by itself: %+v", w)
	}
}

func TestTorrentWantedRef(t *testing.T) {
	if TorrentWantedRef("") != "" {
		t.Fatal("empty hash → empty ref")
	}
	if got := TorrentWantedRef(" ABCDEF "); got != "torrent:abcdef" {
		t.Fatalf("got %q", got)
	}
}

func TestLinkTorrentToWanted(t *testing.T) {
	h := newWantedHarness(t)
	wantedID, _ := h.db.AddWishlistItem("Torrented", "", "ebook")
	_ = h.db.SetWishlistActiveJob(wantedID, TorrentWantedRef("DEADBEEF"))
	path := filepath.Join(h.cfg.EbookDir, "T", "T.epub")
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, fakeEPUB, 0o644)
	itemID, _ := h.db.AddItem(&models.LibraryItem{Title: "Torrented", FilePath: path, FileFormat: "epub", MediaType: "ebook", Source: "prowlarr", SourceID: "deadbeef"})

	outcome := db.AddItemOutcome{ID: itemID, Inserted: true}
	linkTorrentToWanted(h.db, organize.NewOrganizer(h.cfg), h.cfg, TorrentInfo{Hash: "deadbeef"}, outcome)
	w, _ := h.db.GetWishlistItem(wantedID)
	if w.LibraryItemID != itemID || w.ActiveJobID != "" {
		t.Fatalf("torrent import should satisfy the row: %+v", w)
	}
	// Unknown hash is a no-op.
	linkTorrentToWanted(h.db, nil, h.cfg, TorrentInfo{Hash: "cafebabe"}, outcome)
	linkTorrentToWanted(h.db, nil, h.cfg, TorrentInfo{}, outcome)
}

// TestWantedGrab_SameFileAgainIsRejectedAndBlocklisted: the source claimed a
// better format but delivered the very PDF already held. The row keeps its
// file, the release goes on the blocklist, and no duplicate row appears.
func TestWantedGrab_SameFileAgainIsRejectedAndBlocklisted(t *testing.T) {
	h := newWantedHarness(t)
	wantedID, _ := h.db.AddWishlistItem("Liar", "", "ebook")
	h.servePDF.Store(true)
	job, _ := h.manager.StartDirectDownloadFor(h.server.URL+"/liar?rel=1", "Liar", "gutenberg", "g-9", "", wantedID)
	h.waitJob(job, "completed")
	first, _ := h.db.GetWishlistItem(wantedID)

	job2, _ := h.manager.StartDirectDownloadFor(h.server.URL+"/liar?rel=1", "Liar", "gutenberg", "g-9", "", wantedID)
	h.waitJob(job2, "completed")
	w, _ := h.db.GetWishlistItem(wantedID)
	if w.LibraryItemID != first.LibraryItemID || w.CurrentFormat != "pdf" || w.ActiveJobID != "" {
		t.Fatalf("row must keep its file: %+v", w)
	}
	if !strings.Contains(w.LastResult, "blocklisted") {
		t.Fatalf("last result should mention the blocklist: %q", w.LastResult)
	}
	if !h.db.IsBlocklisted(h.server.URL+"/liar?rel=1", "") {
		t.Fatal("release URL should be blocklisted")
	}
	if n, _ := h.db.CountItems("ebook"); n != 1 {
		t.Fatalf("no duplicate rows expected, got %d", n)
	}
	if files := libraryFiles(t, h.cfg.EbookDir); len(files) != 1 {
		t.Fatalf("expected the single PDF to remain, got %v", files)
	}
}

// TestWantedGrab_WorseFormatIsRejectedAndRemoved: an EPUB is held, a release
// claiming EPUB delivers a PDF. The PDF must not replace the EPUB; it is
// removed again and the release blocklisted.
func TestWantedGrab_WorseFormatIsRejectedAndRemoved(t *testing.T) {
	h := newWantedHarness(t)
	wantedID, _ := h.db.AddWishlistItem("Downgrade", "", "ebook")
	h.servePDF.Store(false)
	job, _ := h.manager.StartDirectDownloadFor(h.server.URL+"/d?rel=good", "Downgrade", "gutenberg", "g-10", "", wantedID)
	h.waitJob(job, "completed")
	epub, _ := h.db.GetWishlistItem(wantedID)
	if epub.CurrentFormat != "epub" {
		t.Fatalf("setup: %+v", epub)
	}

	h.servePDF.Store(true)
	job2, _ := h.manager.StartDirectDownloadFor(h.server.URL+"/d?rel=bad", "Downgrade", "gutenberg", "g-10", "", wantedID)
	h.waitJob(job2, "completed")
	w, _ := h.db.GetWishlistItem(wantedID)
	if w.LibraryItemID != epub.LibraryItemID || w.CurrentFormat != "epub" {
		t.Fatalf("EPUB must survive a worse delivery: %+v", w)
	}
	if !h.db.IsBlocklisted(h.server.URL+"/d?rel=bad", "") || h.db.IsBlocklisted(h.server.URL+"/d?rel=good", "") {
		t.Fatal("only the bad release should be blocklisted")
	}
	files := libraryFiles(t, h.cfg.EbookDir)
	if len(files) != 1 || filepath.Ext(files[0]) != ".epub" {
		t.Fatalf("rejected PDF should be removed, library holds %v", files)
	}
	if n, _ := h.db.CountItems("ebook"); n != 1 {
		t.Fatalf("rejected row should be removed, got %d rows", n)
	}
	events, _ := h.db.GetActivity(20, 0)
	saw := false
	for _, e := range events {
		if e.EventType == "wanted_rejected" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("expected a wanted_rejected activity event")
	}
}
