package organize

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

// TestMain silences slog — the guard tests deliberately exercise paths that
// log, and that noise is not a failure.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// TestImportAudiobook_TriggersScanWithSchemeLessConfiguredURL is the
// end-to-end regression for issue #92: an ABS URL configured without a scheme
// (typed as "audiobookshelf:13378" in the UI, or set that way in ABS_URL) made
// every scan die with `unsupported protocol scheme ""`, so nothing ever
// imported. config.Load must hand organize a URL net/http can dial.
func TestImportAudiobook_TriggersScanWithSchemeLessConfiguredURL(t *testing.T) {
	var (
		mu     sync.Mutex
		hits   []string
		auth   string
		method string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, r.URL.Path)
		auth = r.Header.Get("Authorization")
		method = r.Method
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Strip the scheme the test server hands us — this is exactly the value
	// the issue reporter had configured.
	schemeLess := strings.TrimPrefix(srv.URL, "http://")

	dir := t.TempDir()
	t.Setenv("SETTINGS_FILE", filepath.Join(dir, "settings.json"))
	t.Setenv("ABS_URL", schemeLess)
	t.Setenv("ABS_TOKEN", "test-token")
	t.Setenv("ABS_LIBRARY_ID", "lib-1")

	cfg := config.Load()
	if cfg.ABSURL != srv.URL {
		t.Fatalf("config.Load left ABSURL = %q, want %q", cfg.ABSURL, srv.URL)
	}

	NewLibraryTargets(cfg).ImportAudiobook()

	mu.Lock()
	defer mu.Unlock()
	if len(hits) != 1 {
		t.Fatalf("expected exactly 1 request to ABS, got %d: %v", len(hits), hits)
	}
	if hits[0] != "/api/libraries/lib-1/scan" {
		t.Errorf("path = %q, want /api/libraries/lib-1/scan", hits[0])
	}
	if method != http.MethodPost {
		t.Errorf("method = %q, want POST", method)
	}
	if auth != "Bearer test-token" {
		t.Errorf("Authorization = %q, want Bearer test-token", auth)
	}
}

// kavitaStub stands in for a Kavita server: it answers the login handshake
// with a JWT and records every other request it receives.
type kavitaStub struct {
	*httptest.Server
	mu   sync.Mutex
	hits []string // "METHOD /path?query" for everything except the login call
}

func newKavitaStub(t *testing.T) *kavitaStub {
	t.Helper()
	k := &kavitaStub{}
	k.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/Account/login" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"token":"jwt-123"}`)
			return
		}
		k.mu.Lock()
		k.hits = append(k.hits, r.Method+" "+r.URL.RequestURI())
		k.mu.Unlock()
		// Real Kavita answers 400 when /api/Library/scan is called without a
		// libraryId — mirror that so a regression to the parameter-less call
		// is visible as a failed scan, not a silent pass.
		if r.URL.Path == "/api/Library/scan" && r.URL.Query().Get("libraryId") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(k.Close)
	return k
}

func (k *kavitaStub) requests() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.hits...)
}

func (k *kavitaStub) config() *config.Config {
	return &config.Config{
		KavitaURL:  k.URL,
		KavitaUser: "u",
		KavitaPass: "p",
	}
}

// TestImportEbook_TriggersKavitaScan is the regression for issue #98: an ebook
// organized into a folder Kavita watches stayed invisible because only manga
// imports ever asked Kavita to scan.
func TestImportEbook_TriggersKavitaScan(t *testing.T) {
	k := newKavitaStub(t)
	lt := NewLibraryTargets(k.config())

	lt.ImportEbook(filepath.Join(t.TempDir(), "book.epub"), "Some Book", "Some Author")

	got := k.requests()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 Kavita call, got %d: %v", len(got), got)
	}
	// No library ID configured, so every library must be scanned — the
	// parameter-less /api/Library/scan is a 400 and scans nothing.
	if got[0] != "POST /api/Library/scan-all" {
		t.Errorf("request = %q, want POST /api/Library/scan-all", got[0])
	}
}

// TestImportEbook_ScansConfiguredLibraryOnly — with an ID configured, only that
// library is scanned rather than the whole server.
func TestImportEbook_ScansConfiguredLibraryOnly(t *testing.T) {
	k := newKavitaStub(t)
	cfg := k.config()
	cfg.KavitaEbookLibraryID = "3"
	lt := NewLibraryTargets(cfg)

	lt.ImportEbook(filepath.Join(t.TempDir(), "book.epub"), "Some Book", "Some Author")

	got := k.requests()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 Kavita call, got %d: %v", len(got), got)
	}
	if got[0] != "POST /api/Library/scan?libraryId=3&force=false" {
		t.Errorf("request = %q, want POST /api/Library/scan?libraryId=3&force=false", got[0])
	}
}

// TestImportManga_TriggersUsableKavitaScan — manga always *called* Kavita, but
// the call it made was rejected with a 400. It must now hit a real endpoint,
// and honour a configured manga library ID.
func TestImportManga_TriggersUsableKavitaScan(t *testing.T) {
	for _, tc := range []struct {
		name      string
		libraryID string
		want      string
	}{
		{"no library id", "", "POST /api/Library/scan-all"},
		{"library id set", "7", "POST /api/Library/scan?libraryId=7&force=false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newKavitaStub(t)
			cfg := k.config()
			cfg.KavitaMangaLibraryID = tc.libraryID
			NewLibraryTargets(cfg).ImportManga(filepath.Join(t.TempDir(), "series"), "Some Series")

			got := k.requests()
			if len(got) != 1 {
				t.Fatalf("expected exactly 1 Kavita call, got %d: %v", len(got), got)
			}
			if got[0] != tc.want {
				t.Errorf("request = %q, want %q", got[0], tc.want)
			}
		})
	}
}

// TestKavitaLibraryIDIsEscaped — an ID is user-supplied config and must not be
// able to smuggle extra query parameters into the scan URL.
func TestKavitaLibraryIDIsEscaped(t *testing.T) {
	k := newKavitaStub(t)
	cfg := k.config()
	cfg.KavitaEbookLibraryID = "1&force=true"
	NewLibraryTargets(cfg).ImportEbook(filepath.Join(t.TempDir(), "book.epub"), "T", "A")

	got := k.requests()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 Kavita call, got %d: %v", len(got), got)
	}
	if strings.Contains(got[0], "force=true") {
		t.Errorf("library ID leaked into the query string: %q", got[0])
	}
}

// TestScanKavita_SkipsWhenUnconfigured — partial Kavita config must produce no
// outbound request at all (a nil client would panic if one were attempted).
func TestScanKavita_SkipsWhenUnconfigured(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.Config
	}{
		{"nothing set", &config.Config{}},
		{"url only", &config.Config{KavitaURL: "http://kavita:5000"}},
		{"url and user, no password", &config.Config{KavitaURL: "http://kavita:5000", KavitaUser: "u"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lt := &LibraryTargets{cfg: tc.cfg}
			lt.scanKavita(tc.cfg.KavitaEbookLibraryID)
		})
	}
}

// TestImportAudiobook_SkipsWhenUnconfigured — no ABS config must mean no
// outbound request at all, rather than a request to a relative URL.
func TestImportAudiobook_SkipsWhenUnconfigured(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.Config
	}{
		{"no url or token", &config.Config{ABSLibraryID: "lib-1"}},
		{"url but no token", &config.Config{ABSURL: "http://abs:13378", ABSLibraryID: "lib-1"}},
		{"configured but no library id", &config.Config{ABSURL: "http://abs:13378", ABSToken: "t"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A nil http.Client would panic if a request were attempted, which
			// is the assertion: the guards must return before that.
			lt := &LibraryTargets{cfg: tc.cfg}
			lt.ImportAudiobook()
		})
	}
}
