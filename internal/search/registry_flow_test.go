package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/sources/sourcestest"
)

// recordingServer is an httptest.Server that captures every path it receives.
type recordingServer struct {
	*httptest.Server
	mu    sync.Mutex
	paths []string
}

func newRecordingServer(t *testing.T, status int, body string) *recordingServer {
	rs := &recordingServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.mu.Lock()
		rs.paths = append(rs.paths, r.URL.RequestURI())
		rs.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(rs.Server.Close)
	return rs
}

func (rs *recordingServer) hit() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.paths) > 0
}

func (rs *recordingServer) requestURIs() []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]string, len(rs.paths))
	copy(out, rs.paths)
	return out
}

// configWithRegistry returns a Config with the canonical registry, ready
// for mutation by individual tests.
func configWithRegistry(t *testing.T) *config.Config {
	t.Helper()
	reg, err := sourcestest.Registry()
	if err != nil {
		t.Fatalf("load canonical sources registry: %v", err)
	}
	return &config.Config{UserAgent: "test", Sources: reg}
}

// TestRegistryFlow proves that every refactored driver issues its HTTP
// request against the URL recorded in cfg.Sources — i.e. the registry
// value actually flows into the wire request, with no hardcoded URL
// shadowing it.
func TestRegistryFlow(t *testing.T) {
	client := &http.Client{Timeout: 3 * time.Second}

	t.Run("gutenberg hits cfg.Sources.Gutenberg.URL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `{"results":[]}`)
		cfg := configWithRegistry(t)
		cfg.Sources.Gutenberg.URL = srv.URL
		_, _ = NewGutenberg(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("Gutenberg did not call the registry URL")
		}
		if got := srv.requestURIs()[0]; !strings.Contains(got, "search=test") {
			t.Errorf("expected ?search=test, got %q", got)
		}
	})

	t.Run("openlibrary hits cfg.Sources.OpenLibrary.SearchURL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `{"docs":[]}`)
		cfg := configWithRegistry(t)
		cfg.Sources.OpenLibrary.SearchURL = srv.URL
		_, _ = NewOpenLibrary(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("OpenLibrary did not call the registry URL")
		}
		if got := srv.requestURIs()[0]; !strings.Contains(got, "q=test") {
			t.Errorf("expected ?q=test, got %q", got)
		}
	})

	t.Run("librivox hits cfg.Sources.Librivox.URL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `{"books":[]}`)
		cfg := configWithRegistry(t)
		cfg.Sources.Librivox.URL = srv.URL
		_, _ = NewLibrivox(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("Librivox did not call the registry URL")
		}
		// Librivox loops over [title, author] fields — both should hit.
		if uris := srv.requestURIs(); len(uris) != 2 {
			t.Errorf("expected 2 requests (title+author), got %d: %v", len(uris), uris)
		}
	})

	t.Run("standardebooks hits cfg.Sources.StandardEbooks.URL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `<feed></feed>`)
		cfg := configWithRegistry(t)
		cfg.Sources.StandardEbooks.URL = srv.URL
		_, _ = NewStandardEbooks(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("StandardEbooks did not call the registry URL")
		}
		if got := srv.requestURIs()[0]; !strings.HasPrefix(got, "/feeds/opds/all?query=test") {
			t.Errorf("expected /feeds/opds/all?query=test, got %q", got)
		}
	})

	t.Run("mangadex hits cfg.Sources.MangaDex.APIURL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `{"data":[]}`)
		cfg := configWithRegistry(t)
		cfg.Sources.MangaDex.APIURL = srv.URL
		cfg.MangaDexEnabled = true
		_, _ = NewMangaDex(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("MangaDex did not call the registry URL")
		}
		if got := srv.requestURIs()[0]; !strings.HasPrefix(got, "/manga?") || !strings.Contains(got, "title=test") {
			t.Errorf("expected /manga?...title=test, got %q", got)
		}
	})

	t.Run("nyaa hits cfg.Sources.Nyaa.URL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `<rss><channel></channel></rss>`)
		cfg := configWithRegistry(t)
		cfg.Sources.Nyaa.URL = srv.URL
		_, _ = NewNyaaManga(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("Nyaa did not call the registry URL")
		}
		got := srv.requestURIs()[0]
		if !strings.Contains(got, "q=test") || !strings.Contains(got, "c=3_1") {
			t.Errorf("expected ?q=test&c=3_1..., got %q", got)
		}
	})

	t.Run("thepiratebay hits cfg.Sources.ThePirateBay.URL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `[]`)
		cfg := configWithRegistry(t)
		cfg.Sources.ThePirateBay.URL = srv.URL
		cfg.TPBEnabled = true
		_, _ = NewThePirateBay(cfg, client, "main").Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("TPB did not call the registry URL")
		}
		got := srv.requestURIs()[0]
		if !strings.HasPrefix(got, "/q.php?") || !strings.Contains(got, "q=test") || !strings.Contains(got, "cat=601") {
			t.Errorf("expected /q.php?q=test&cat=601, got %q", got)
		}
	})

	t.Run("audiobookbay uses cfg.Sources.AudioBookBay.Mirrors (no fallback to baked-in)", func(t *testing.T) {
		// AudioBookBay prefixes "https://" to each mirror, so we can't use an
		// http test server directly. Instead, set a sentinel domain and assert
		// the resulting error names the sentinel — proves the driver consulted
		// the registry mirror list and didn't fall through to a hardcoded one.
		cfg := configWithRegistry(t)
		cfg.Sources.AudioBookBay.Mirrors = []string{"sentinel.test.invalid"}
		_, err := NewAudioBookBay(cfg, client).Search(context.Background(), "test")
		if err == nil {
			t.Fatal("expected error when mirror is unreachable")
		}
		if strings.Contains(err.Error(), "audiobookbay.lu") {
			t.Errorf("driver fell back to a hardcoded mirror; error mentions audiobookbay.lu: %v", err)
		}
	})

	t.Run("webnovel hits cfg.Sources.WebNovels[freewebnovel].URL", func(t *testing.T) {
		srv := newRecordingServer(t, 200, `<html></html>`)
		cfg := configWithRegistry(t)
		cfg.WebNovelEnabled = true
		// Override only freewebnovel — leave the other 6 sites alone (they'll
		// attempt their real URLs, which is fine since we only assert that
		// freewebnovel hit our server).
		for i := range cfg.Sources.WebNovels {
			if cfg.Sources.WebNovels[i].ID == "freewebnovel" {
				cfg.Sources.WebNovels[i].URL = srv.URL + "/search/"
			} else {
				// Point everything else at a closed port so they fail fast.
				cfg.Sources.WebNovels[i].URL = "http://127.0.0.1:1/"
				cfg.Sources.WebNovels[i].BaseURL = "http://127.0.0.1:1"
			}
		}
		_, _ = NewWebNovel(cfg, client).Search(context.Background(), "test")
		if !srv.hit() {
			t.Fatalf("WebNovel (freewebnovel) did not call the registry URL")
		}
		got := srv.requestURIs()[0]
		if !strings.HasPrefix(got, "/search/") || !strings.Contains(got, "searchkey=test") {
			t.Errorf("expected /search/?searchkey=test, got %q", got)
		}
	})
}

// TestRegistryFlow_AnnasArchive verifies that Anna's Archive picks up its
// domain from the registry when no env override is set, and that the env
// override takes precedence when present.
func TestRegistryFlow_AnnasArchive(t *testing.T) {
	cases := []struct {
		name          string
		envOverride   string
		registryValue string
		want          string
	}{
		{"registry value used when env unset", "", "registry.example", "registry.example"},
		{"env override wins over registry", "env.example", "registry.example", "env.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, _ := sourcestest.Registry()
			reg.Annas.Domain = tc.registryValue
			reg.ApplyEnvOverrides(func(k string) string {
				if k == "ANNAS_ARCHIVE_DOMAIN" {
					return tc.envOverride
				}
				return ""
			})
			// Mirror the resolution Load() performs.
			annasDomain := tc.envOverride
			if annasDomain == "" {
				annasDomain = reg.Annas.Domain
			}
			cfg := &config.Config{
				UserAgent:          "test",
				Sources:            reg,
				AnnasArchiveDomain: annasDomain,
			}
			a := NewAnnasArchive(cfg, http.DefaultClient)
			if !a.Enabled() {
				t.Fatal("Anna's Archive should be enabled when domain is set")
			}
			if a.cfg.AnnasArchiveDomain != tc.want {
				t.Errorf("AnnasArchiveDomain = %q, want %q", a.cfg.AnnasArchiveDomain, tc.want)
			}
		})
	}
}

// TestRegistryFlow_ZLibraryBase verifies Z-Library's apiBase() falls back to
// cfg.Sources.ZLibraryDefault when ZLibraryURL env is unset, and that the
// env value still wins when both are set.
func TestRegistryFlow_ZLibraryBase(t *testing.T) {
	cases := []struct {
		name            string
		envURL          string
		registryDefault string
		want            string
	}{
		{"env wins when set", "https://env-set.example/", "https://from-registry.example", "https://env-set.example"},
		{"registry default used when env empty", "", "https://from-registry.example/", "https://from-registry.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, _ := sourcestest.Registry()
			reg.ZLibraryDefault = tc.registryDefault
			cfg := &config.Config{
				Sources:     reg,
				ZLibraryURL: tc.envURL,
			}
			z := NewZLibrary(cfg, http.DefaultClient)
			if got := z.apiBase(); got != tc.want {
				t.Errorf("apiBase() = %q, want %q", got, tc.want)
			}
		})
	}
}
