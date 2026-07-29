package download

import (
	"context"
	"encoding/json"
	"errors"
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
)

func TestRecordTorrentItemIsIdempotentAcrossWatcherPolls(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	filePath := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(filePath, []byte("same torrent file"), 0644); err != nil {
		t.Fatal(err)
	}
	w := &Watcher{db: database}
	torrent := TorrentInfo{Name: "Book", Hash: "torrent-hash"}

	first, err := w.recordTorrentItem(torrent, "ebook", "/downloads/book.epub", filePath, "Book", "Author", "Book", "Author", "epub", 0)
	if err != nil || !first {
		t.Fatalf("first recordTorrentItem = inserted %v, err %v", first, err)
	}
	second, err := w.recordTorrentItem(torrent, "ebook", "/downloads/book.epub", filePath, "Book", "Author", "Book", "Author", "epub", 0)
	if err != nil || second {
		t.Fatalf("second recordTorrentItem = inserted %v, err %v; want idempotent reuse", second, err)
	}

	count, err := database.CountItems("ebook")
	if err != nil || count != 1 {
		t.Fatalf("CountItems = %d, %v; want one row", count, err)
	}
}

func TestMapTorrentPathRemoteRootToLocalRoot(t *testing.T) {
	got, ok := mapTorrentPath("/downloads/rclone-mnt/downloads/Prince Of Persia", "/downloads/rclone-mnt/downloads", "/downloads")
	if !ok || got != "/downloads/Prince Of Persia" {
		t.Fatalf("mapTorrentPath = (%q, %v), want (/downloads/Prince Of Persia, true)", got, ok)
	}
}

func TestMapTorrentPathSingleFile(t *testing.T) {
	got, ok := mapTorrentPath("/remote/books/Book.epub", "/remote/books", "/local/incoming")
	if !ok || got != "/local/incoming/Book.epub" {
		t.Fatalf("mapTorrentPath = (%q, %v), want (/local/incoming/Book.epub, true)", got, ok)
	}
}

func TestMapTorrentPathMultiFileDirectory(t *testing.T) {
	got, ok := mapTorrentPath("/remote/books/Series/Book", "/remote/books", "/local/incoming")
	if !ok || got != "/local/incoming/Series/Book" {
		t.Fatalf("mapTorrentPath = (%q, %v), want (/local/incoming/Series/Book, true)", got, ok)
	}
}

func TestMapTorrentPathIdenticalRoots(t *testing.T) {
	got, ok := mapTorrentPath("/downloads/Book/file.epub", "/downloads", "/downloads")
	if !ok || got != "/downloads/Book/file.epub" {
		t.Fatalf("mapTorrentPath = (%q, %v), want unchanged path, true", got, ok)
	}
}

func TestMapTorrentPathRejectsOutsideRemoteRoot(t *testing.T) {
	if got, ok := mapTorrentPath("/other/Book.epub", "/remote/books", "/local/incoming"); ok || got != "" {
		t.Fatalf("mapTorrentPath = (%q, %v), want empty path, false", got, ok)
	}
}

func TestMapTorrentPathRejectsTraversal(t *testing.T) {
	if got, ok := mapTorrentPath("/remote/books/../secret/Book.epub", "/remote/books", "/local/incoming"); ok || got != "" {
		t.Fatalf("mapTorrentPath = (%q, %v), want empty path, false", got, ok)
	}
}

func TestResolveLocalPathMapsConfiguredQBRoot(t *testing.T) {
	w := &Watcher{cfg: &config.Config{
		QBSavePath:  "/downloads/rclone-mnt/downloads",
		IncomingDir: "/downloads",
		QBCategory:  "librarr",
	}}

	got := w.resolveLocalPath(TorrentInfo{
		ContentPath: "/downloads/rclone-mnt/downloads/Prince Of Persia",
		SavePath:    "/downloads/rclone-mnt/downloads",
	}, "ebook")
	if got != "/downloads/Prince Of Persia" {
		t.Fatalf("resolveLocalPath = %q, want /downloads/Prince Of Persia", got)
	}
}

func TestResolveLocalPathExactReportedPathWins(t *testing.T) {
	dir := t.TempDir()
	exactPath := filepath.Join(dir, "mounted", "Exact Book.epub")
	if err := os.MkdirAll(filepath.Dir(exactPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exactPath, []byte("book"), 0644); err != nil {
		t.Fatal(err)
	}
	w := &Watcher{cfg: &config.Config{
		QBSavePath:  "/remote/downloads",
		IncomingDir: filepath.Join(dir, "incoming"),
	}}

	got := w.resolveLocalPath(TorrentInfo{
		ContentPath: exactPath,
		SavePath:    "/remote/downloads",
	}, "ebook")
	if got != exactPath {
		t.Fatalf("resolveLocalPath = %q, want exact mounted path %q", got, exactPath)
	}
}

func TestResolveLocalPathRemoteSingleFileFallsBackToIncomingBasename(t *testing.T) {
	w := &Watcher{cfg: &config.Config{
		QBSavePath:  "/different/remote/root",
		IncomingDir: "/data/incoming",
	}}

	got := w.resolveLocalPath(TorrentInfo{
		ContentPath: "/downloads/rclone-mnt/downloads/Unfreedom of the Press(Unabridged)e.mp3",
	}, "audiobook")
	want := "/data/incoming/Unfreedom of the Press(Unabridged)e.mp3"
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathRemoteDirectoryFallsBackToIncomingBasename(t *testing.T) {
	w := &Watcher{cfg: &config.Config{
		QBSavePath:  "/different/remote/root",
		IncomingDir: "/data/incoming",
	}}

	got := w.resolveLocalPath(TorrentInfo{
		ContentPath: "/downloads/rclone-mnt/downloads/Some Audiobook",
	}, "audiobook")
	want := "/data/incoming/Some Audiobook"
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathRejectsMalformedRelativeContentPath(t *testing.T) {
	w := &Watcher{cfg: &config.Config{IncomingDir: "/data/incoming"}}

	resolved := w.resolveLocalPathResult(TorrentInfo{
		ContentPath: "../secret/book.epub",
	}, "ebook")
	if resolved.Path != "/data/incoming" || resolved.Failure == "" {
		t.Fatalf("resolveLocalPathResult = %+v, want safe incoming root with failure", resolved)
	}
}

func TestResolveLocalPathAudiobookUsesContentPath(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name:        "Brigands &amp; Breadknives (Legends &amp; Lattes) - Travis Baldree",
		ContentPath: "/downloads/audiobooks-incoming/Brigands &amp; Breadknives.m4b",
	}, "audiobook")

	want := "/downloads/incoming/Brigands & Breadknives.m4b"
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathAudiobookMapsRemoteContentPathToLocalIncoming(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/data/audiobooks-incoming",
			IncomingDir:         "/data/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name:        "Brigands &amp; Breadknives (Legends &amp; Lattes) - Travis Baldree",
		ContentPath: "/downloads/audiobooks-incoming/Brigands &amp; Breadknives.m4b",
		SavePath:    "/downloads/audiobooks-incoming",
	}, "audiobook")

	want := "/data/incoming/Brigands & Breadknives.m4b"
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathAudiobookPreservesRelativeContentPath(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/data/audiobooks-incoming",
			IncomingDir:         "/data/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name:        "Some Book",
		ContentPath: "Series/Some Book/part01.mp3",
	}, "audiobook")

	want := filepath.Join("/data/incoming", "Series/Some Book/part01.mp3")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathAudiobookMapsRemoteSaveRootToLocalIncoming(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/data/audiobooks-incoming",
			IncomingDir:         "/data/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name:        "Some Book",
		ContentPath: "/downloads/audiobooks-incoming",
		SavePath:    "/downloads/audiobooks-incoming",
	}, "audiobook")

	want := "/data/incoming"
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathAudiobookFallsBackToName(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "Brigands &amp; Breadknives (Legends &amp; Lattes) - Travis Baldree",
	}, "audiobook")

	want := filepath.Join("/downloads/incoming", "Brigands & Breadknives (Legends & Lattes) - Travis Baldree")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathEbookFallsBackToName(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			IncomingDir: "/downloads/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "Some Book - Author",
	}, "ebook")

	want := filepath.Join("/downloads/incoming", "Some Book - Author")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathMangaFallsBackToIncomingDir(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			IncomingDir: "/downloads/incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "One Piece Vol 100",
	}, "manga")

	want := filepath.Join("/downloads/incoming", "One Piece Vol 100")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathMangaUsesConfiguredDir(t *testing.T) {
	w := &Watcher{
		cfg: &config.Config{
			IncomingDir:      "/downloads/incoming",
			MangaIncomingDir: "/downloads/manga-incoming",
		},
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "One Piece Vol 100",
	}, "manga")

	want := filepath.Join("/downloads/manga-incoming", "One Piece Vol 100")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

// newMockQBServer creates a test server that serves both login and torrents/files endpoints.
func newMockQBServer(files map[string][]TorrentFile) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "test"})
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Ok."))
		case "/api/v2/torrents/files":
			hash := r.URL.Query().Get("hash")
			if f, ok := files[hash]; ok {
				json.NewEncoder(w).Encode(f)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestResolveLocalPathUsesGetTorrentFilesWhenContentPathEmpty(t *testing.T) {
	srv := newMockQBServer(map[string][]TorrentFile{
		"abc123": {
			{Name: "Sublimation/track01.mp3"},
			{Name: "Sublimation/track02.mp3"},
		},
	})
	defer srv.Close()

	qb := newTestQBClient(srv.URL)
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
		torrent: qb,
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "Sublimation - Isabel J. Kim",
		Hash: "abc123",
	}, "audiobook")

	want := filepath.Join("/downloads/incoming", "Sublimation")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathSingleFileNoSubfolder(t *testing.T) {
	srv := newMockQBServer(map[string][]TorrentFile{
		"def456": {
			{Name: "The_Unicorn_Hunters.m4b"},
		},
	})
	defer srv.Close()

	qb := newTestQBClient(srv.URL)
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
		torrent: qb,
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "The Unicorn Hunters - Katherine Arden",
		Hash: "def456",
	}, "audiobook")

	want := filepath.Join("/downloads/incoming", "The_Unicorn_Hunters.m4b")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathMultiFileDifferentRootsFallsBack(t *testing.T) {
	srv := newMockQBServer(map[string][]TorrentFile{
		"ghi789": {
			{Name: "track1.mp3"},
			{Name: "track2.mp3"},
		},
	})
	defer srv.Close()

	qb := newTestQBClient(srv.URL)
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
		torrent: qb,
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "Some Audiobook - Author",
		Hash: "ghi789",
	}, "audiobook")

	// Multiple files without a common root -> falls back to t.Name
	want := filepath.Join("/downloads/incoming", "Some Audiobook - Author")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathAPIErrorFallsBackToName(t *testing.T) {
	// Server that always returns 500 for files endpoint.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "test"})
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Ok."))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	qb := newTestQBClient(srv.URL)
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
		torrent: qb,
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name: "Some Book - Author",
		Hash: "fail",
	}, "audiobook")

	want := filepath.Join("/downloads/incoming", "Some Book - Author")
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestResolveLocalPathContentPathTakesPrecedence(t *testing.T) {
	// Even with a qb client that has files, ContentPath should win.
	srv := newMockQBServer(map[string][]TorrentFile{
		"xyz": {{Name: "WrongFolder/file.mp3"}},
	})
	defer srv.Close()

	qb := newTestQBClient(srv.URL)
	w := &Watcher{
		cfg: &config.Config{
			QBAudiobookSavePath: "/downloads/audiobooks-incoming",
			IncomingDir:         "/downloads/incoming",
		},
		torrent: qb,
	}

	got := w.resolveLocalPath(TorrentInfo{
		Name:        "Some Name",
		Hash:        "xyz",
		ContentPath: "/downloads/audiobooks-incoming/CorrectFolder",
	}, "audiobook")

	want := "/downloads/incoming/CorrectFolder"
	if got != want {
		t.Fatalf("resolveLocalPath = %q, want %q", got, want)
	}
}

func TestNormalizeTorrentPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"  ", ""},
		{"simple name", "simple name"},
		{"Brigands &amp; Breadknives", "Brigands & Breadknives"},
		{"  /path/to/file.m4b  ", "/path/to/file.m4b"},
		{"Title &lt;Special&gt;", "Title <Special>"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeTorrentPath(tt.input)
			if got != tt.want {
				t.Errorf("normalizeTorrentPath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestWatcherUsesConfiguredImportEngine(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sourceFile := filepath.Join(dir, "source.epub")
	destFile := filepath.Join(dir, "organized.epub")
	if err := os.WriteFile(sourceFile, []byte("book"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destFile, []byte("book"), 0644); err != nil {
		t.Fatal(err)
	}

	engine := &watcherSpyImportEngine{result: &libraryimport.EngineResult{InsertedCount: 1}}
	w := &Watcher{db: database, importer: engine}

	inserted, err := w.importTorrentItem(context.Background(), TorrentInfo{Name: "Torrent Book", Hash: "torrent-1"}, library.MediaTypeEbook, sourceFile, destFile, "Torrent Book", "Jane Doe", "Torrent Book", "Jane Doe", "epub", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected inserted result")
	}
	if len(engine.requests) != 1 {
		t.Fatalf("engine requests = %d, want 1", len(engine.requests))
	}
	got := engine.requests[0]
	if got.Source.Name != "torrent" || got.Source.SourceID != "torrent-1" || got.Source.MediaType != library.MediaTypeEbook {
		t.Fatalf("request source = %+v", got.Source)
	}
	if got.RootPath != destFile || got.OriginalPath != sourceFile {
		t.Fatalf("request paths = %+v", got)
	}
}

func TestWatcherImportsAudiobookFromRemoteContentPathMappedToIncoming(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	incoming := filepath.Join(dir, "incoming")
	sourceFile := filepath.Join(incoming, "Unfreedom of the Press(Unabridged)e.mp3")
	if err := os.MkdirAll(incoming, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourceFile, []byte("audio"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		FileOrgEnabled:           false,
		IncomingDir:              incoming,
		QBAudiobookSavePath:      "/downloads/rclone-mnt/downloads",
		QBAudiobookCategory:      "audiobook",
		RemoveTorrentAfterImport: false,
	}
	engine := &watcherSpyImportEngine{result: &libraryimport.EngineResult{InsertedCount: 1}}
	w := &Watcher{
		cfg:       cfg,
		db:        database,
		organizer: organize.NewOrganizer(cfg),
		importer:  engine,
	}

	w.importTorrent(TorrentInfo{
		Name:        "Mark R. Levin - Unfreedom of the Press",
		Hash:        "audio-hash",
		ContentPath: "/downloads/rclone-mnt/downloads/Unfreedom of the Press(Unabridged)e.mp3",
		SavePath:    "/downloads/rclone-mnt/downloads",
		Progress:    1,
	}, "audiobook")

	if len(engine.requests) != 1 {
		t.Fatalf("engine requests = %d, want 1", len(engine.requests))
	}
	got := engine.requests[0]
	if got.Source.MediaType != library.MediaTypeAudiobook {
		t.Fatalf("media type = %q, want audiobook", got.Source.MediaType)
	}
	if got.OriginalPath != sourceFile || got.RootPath != sourceFile {
		t.Fatalf("request paths = original %q root %q, want %q", got.OriginalPath, got.RootPath, sourceFile)
	}
	if _, ok := w.imported.Load("audio-hash"); !ok {
		t.Fatal("torrent hash should be marked imported after successful audiobook import")
	}
}

func TestWatcherKeepsMissingRemoteContentPathPending(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	cfg := &config.Config{
		FileOrgEnabled:      false,
		IncomingDir:         filepath.Join(dir, "incoming"),
		QBAudiobookSavePath: "/downloads/rclone-mnt/downloads",
	}
	engine := &watcherSpyImportEngine{result: &libraryimport.EngineResult{InsertedCount: 1}}
	w := &Watcher{
		cfg:       cfg,
		db:        database,
		organizer: organize.NewOrganizer(cfg),
		importer:  engine,
	}

	w.importTorrent(TorrentInfo{
		Name:        "Mark R. Levin - Unfreedom of the Press",
		Hash:        "pending-audio",
		ContentPath: "/downloads/rclone-mnt/downloads/Unfreedom of the Press(Unabridged)e.mp3",
		SavePath:    "/downloads/rclone-mnt/downloads",
		Progress:    1,
	}, "audiobook")

	if len(engine.requests) != 0 {
		t.Fatalf("engine requests = %d, want 0 while local synchronized file is missing", len(engine.requests))
	}
	if _, ok := w.imported.Load("pending-audio"); ok {
		t.Fatal("missing local file should not be marked imported")
	}
}

func TestWatcherDoesNotImportEbookWhenOrganizationFails(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	saveDir := filepath.Join(dir, "incoming", "Broken Org")
	if err := os.MkdirAll(saveDir, 0755); err != nil {
		t.Fatal(err)
	}
	sourceFile := filepath.Join(saveDir, "book.epub")
	if err := os.WriteFile(sourceFile, []byte("book"), 0644); err != nil {
		t.Fatal(err)
	}
	ebookRoot := filepath.Join(dir, "ebooks-as-file")
	if err := os.WriteFile(ebookRoot, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{FileOrgEnabled: true, EbookDir: ebookRoot}
	w := &Watcher{
		cfg:       cfg,
		db:        database,
		organizer: organize.NewOrganizer(cfg),
	}

	err = w.importEbook(TorrentInfo{Name: "Broken Org", Hash: "hash-org-failure", TotalSize: 123}, saveDir)
	if err == nil {
		t.Fatal("expected organization failure")
	}
	if !errors.Is(err, errTorrentContentPending) {
		t.Fatalf("expected pending organization error, got %v", err)
	}
	if database.HasSourceID("hash-org-failure") {
		t.Fatal("library item should not be inserted when organization fails")
	}
}

type watcherSpyImportEngine struct {
	requests []libraryimport.ImportRequest
	result   *libraryimport.EngineResult
	err      error
}

func (s *watcherSpyImportEngine) Import(_ context.Context, request libraryimport.ImportRequest) (*libraryimport.EngineResult, error) {
	s.requests = append(s.requests, request)
	if s.result == nil {
		s.result = &libraryimport.EngineResult{InsertedCount: 1}
	}
	return s.result, s.err
}
