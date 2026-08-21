package download

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/organize"
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

	first, err := w.recordTorrentItem("torrent", torrent, "ebook", "/downloads/book.epub", filePath, "Book", "Author", "Book", "Author", "epub", 0)
	if err != nil || !first {
		t.Fatalf("first recordTorrentItem = inserted %v, err %v", first, err)
	}
	second, err := w.recordTorrentItem("torrent", torrent, "ebook", "/downloads/book.epub", filePath, "Book", "Author", "Book", "Author", "epub", 0)
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

	want := "/downloads/audiobooks-incoming/Brigands & Breadknives.m4b"
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

	want := "/data/audiobooks-incoming/Brigands & Breadknives.m4b"
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

	want := filepath.Join("/data/audiobooks-incoming", "Series/Some Book/part01.mp3")
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

	want := "/data/audiobooks-incoming"
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

	want := filepath.Join("/downloads/audiobooks-incoming", "Brigands & Breadknives (Legends & Lattes) - Travis Baldree")
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

	want := filepath.Join("/downloads/audiobooks-incoming", "Sublimation")
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

	want := filepath.Join("/downloads/audiobooks-incoming", "The_Unicorn_Hunters.m4b")
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
	want := filepath.Join("/downloads/audiobooks-incoming", "Some Audiobook - Author")
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

	want := filepath.Join("/downloads/audiobooks-incoming", "Some Book - Author")
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

	want := "/downloads/audiobooks-incoming/CorrectFolder"
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

// TestImportMangaRejectsTraversalTorrentName covers the second sink of the
// manga path-traversal report (Mahmoud Hassan): the download watcher passes the
// torrent name straight into OrganizeManga, so a malicious torrent name is an
// unauthenticated route to the same arbitrary write.
func TestImportMangaRejectsTraversalTorrentName(t *testing.T) {
	root := t.TempDir()
	mangaDir := filepath.Join(root, "manga")
	savePath := filepath.Join(root, "downloads", "series")
	outside := filepath.Join(root, "pwned")
	if err := os.MkdirAll(savePath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(mangaDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(savePath, "ch1.cbz"), []byte("payload"), 0644); err != nil {
		t.Fatal(err)
	}

	database, err := db.New(filepath.Join(root, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	cfg := &config.Config{FileOrgEnabled: true, MangaDir: mangaDir}
	w := NewWatcher(cfg, database, nil, nil, organize.NewOrganizer(cfg), nil, nil)

	info := TorrentInfo{Name: "../pwned", Hash: "abc123", TotalSize: 7}
	_, _ = w.importManga(info, savePath, "test")

	if _, err := os.Stat(outside); err == nil {
		t.Fatalf("malicious torrent name wrote outside MangaDir: %s", outside)
	}
}

// fakeTorrentClient records post-import deletions so tests can assert whether
// the payload was deleted along with the torrent record.
type fakeTorrentClient struct {
	deletes []deleteCall
}

type deleteCall struct {
	hash        string
	deleteFiles bool
}

func (f *fakeTorrentClient) AddTorrent(string, string, string, string, string) error { return nil }
func (f *fakeTorrentClient) GetTorrents(string) ([]TorrentInfo, error)               { return nil, nil }
func (f *fakeTorrentClient) GetTorrentFiles(string) ([]TorrentFile, error)           { return nil, nil }
func (f *fakeTorrentClient) DeleteTorrent(hash string, deleteFiles bool) error {
	f.deletes = append(f.deletes, deleteCall{hash: hash, deleteFiles: deleteFiles})
	return nil
}
func (f *fakeTorrentClient) Diagnose() map[string]interface{} { return nil }
func (f *fakeTorrentClient) Name() string                     { return "fake" }

// newImportFixture wires a watcher over a real temp filesystem holding one
// finished ebook download, and returns the watcher, the fake client and the
// payload path inside the download folder.
func newImportFixture(t *testing.T, importMode string, fileOrg, removeAfterImport bool) (*Watcher, *fakeTorrentClient, string, TorrentInfo) {
	t.Helper()
	root := t.TempDir()
	incoming := filepath.Join(root, "incoming", "Some Book")
	if err := os.MkdirAll(incoming, 0755); err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(incoming, "book.epub")
	if err := os.WriteFile(payload, []byte("book payload"), 0644); err != nil {
		t.Fatal(err)
	}

	database, err := db.New(filepath.Join(root, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })

	cfg := &config.Config{
		FileOrgEnabled:           fileOrg,
		EbookDir:                 filepath.Join(root, "books"),
		IncomingDir:              filepath.Join(root, "incoming"),
		QBSavePath:               filepath.Join(root, "incoming"),
		ImportMode:               importMode, // config.ImportModeAuto exercises the inferred mode
		RemoveTorrentAfterImport: removeAfterImport,
	}
	client := &fakeTorrentClient{}
	w := NewWatcher(cfg, database, client, nil, organize.NewOrganizer(cfg), nil, nil)

	info := TorrentInfo{
		Name:        "Some Book",
		Hash:        "hash-1",
		Progress:    1.0,
		ContentPath: incoming,
		SavePath:    filepath.Join(root, "incoming"),
	}
	return w, client, payload, info
}

// In move mode the payload is already out of the download folder, so the
// torrent record is removed without touching files.
func TestImportTorrentMoveModeRemovesRecordOnly(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeMove, true, true)

	w.importTorrent(info, "ebook")

	if len(client.deletes) != 1 {
		t.Fatalf("DeleteTorrent calls = %d, want 1", len(client.deletes))
	}
	if client.deletes[0].deleteFiles {
		t.Error("move mode must not ask the client to delete files")
	}
	if _, err := os.Stat(payload); !os.IsNotExist(err) {
		t.Errorf("move mode should have consumed the payload, stat err = %v", err)
	}
}

// In hardlink mode the payload is still in the download folder. Removing the
// torrent without its files would orphan it, so the files go too — the library
// keeps its own link to the same data.
func TestImportTorrentHardlinkModeRemovesTorrentWithFiles(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeHardlink, true, true)

	w.importTorrent(info, "ebook")

	if len(client.deletes) != 1 {
		t.Fatalf("DeleteTorrent calls = %d, want 1", len(client.deletes))
	}
	if !client.deletes[0].deleteFiles {
		t.Error("hardlink mode should remove the orphaned payload with the torrent")
	}
	if _, err := os.Stat(payload); err != nil {
		t.Errorf("librarr itself must not remove the payload: %v", err)
	}
}

// The fix for issue #59: with the torrent kept and a payload-preserving mode,
// nothing touches the download folder, so the torrent can really keep seeding.
func TestImportTorrentKeepsPayloadWhenTorrentIsKept(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeHardlink, true, false)

	w.importTorrent(info, "ebook")

	if len(client.deletes) != 0 {
		t.Fatalf("DeleteTorrent calls = %d, want 0", len(client.deletes))
	}
	if _, err := os.Stat(payload); err != nil {
		t.Errorf("payload must stay in the download folder for seeding: %v", err)
	}
}

// With organization off the download folder holds the only copy — the library
// row points straight at it — so its files must never be deleted.
func TestImportTorrentNeverDeletesFilesWhenOrganizationDisabled(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeHardlink, false, true)

	w.importTorrent(info, "ebook")

	if len(client.deletes) != 1 {
		t.Fatalf("DeleteTorrent calls = %d, want 1", len(client.deletes))
	}
	if client.deletes[0].deleteFiles {
		t.Error("must not delete files that are the library's only copy")
	}
	if _, err := os.Stat(payload); err != nil {
		t.Errorf("payload should be untouched: %v", err)
	}
}

// A failed organize leaves that file in the download folder as the only copy,
// which must veto the file deletion for the whole torrent.
func TestImportTorrentDoesNotDeleteFilesWhenOrganizeFails(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeHardlink, true, true)
	// A regular file where the library directory must go makes MkdirAll fail,
	// so OrganizeEbook returns the source path unchanged.
	if err := os.WriteFile(w.cfg.EbookDir, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	w.importTorrent(info, "ebook")

	if len(client.deletes) != 1 {
		t.Fatalf("DeleteTorrent calls = %d, want 1", len(client.deletes))
	}
	if client.deletes[0].deleteFiles {
		t.Error("a failed organize must veto deleting the payload")
	}
	if _, err := os.Stat(payload); err != nil {
		t.Errorf("payload should be untouched: %v", err)
	}
}

// The single-knob contract, end to end through the watcher: the operator turns
// off "remove torrents" and nothing else, and the payload survives the import.
func TestImportTorrentKeepingTorrentsAloneKeepsThePayload(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeAuto, true, false)

	w.importTorrent(info, "ebook")

	if got := w.cfg.EffectiveImportMode(); got != config.ImportModeHardlink {
		t.Fatalf("effective import mode = %q, want %q", got, config.ImportModeHardlink)
	}
	if len(client.deletes) != 0 {
		t.Fatalf("DeleteTorrent calls = %d, want 0", len(client.deletes))
	}
	payloadStat, err := os.Stat(payload)
	if err != nil {
		t.Fatalf("payload must stay in the download folder for seeding: %v", err)
	}
	imported := findImportedEbook(t, w.cfg.EbookDir)
	importedStat, err := os.Stat(imported)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(payloadStat, importedStat) {
		t.Error("library file should share the payload's data (hardlink)")
	}
}

// findImportedEbook returns the single ebook the import placed in the library.
func findImportedEbook(t *testing.T, libraryDir string) string {
	t.Helper()
	var found string
	err := filepath.WalkDir(libraryDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.HasSuffix(path, ".epub") {
			found = path
		}
		return nil
	})
	if err != nil || found == "" {
		t.Fatalf("no imported ebook under %s (walk err %v)", libraryDir, err)
	}
	return found
}

// And the default deployment — nothing configured at all — still moves, so
// existing installs see no change.
func TestImportTorrentDefaultsToMoveWhenTorrentsAreRemoved(t *testing.T) {
	w, client, payload, info := newImportFixture(t, config.ImportModeAuto, true, true)

	w.importTorrent(info, "ebook")

	if got := w.cfg.EffectiveImportMode(); got != config.ImportModeMove {
		t.Fatalf("effective import mode = %q, want %q", got, config.ImportModeMove)
	}
	if len(client.deletes) != 1 || client.deletes[0].deleteFiles {
		t.Fatalf("deletes = %+v, want one record-only removal", client.deletes)
	}
	if _, err := os.Stat(payload); !os.IsNotExist(err) {
		t.Errorf("move mode should have consumed the payload, stat err = %v", err)
	}
}
