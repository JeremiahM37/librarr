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
