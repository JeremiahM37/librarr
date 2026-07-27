package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/sources/sourcestest"
)

// queryBoundedInt rejects out-of-range and unparseable values, returning fallback.
// Regression test for `/api/activity?limit=-1` returning the entire activity log
// (potential DoS / data-exfil) — fuzz-discovered against prod v1.1.0.
func TestQueryBoundedInt(t *testing.T) {
	cases := []struct {
		name, qs string
		want     int
	}{
		{"missing -> fallback", "", 50},
		{"valid in range", "?limit=10", 10},
		{"negative -> fallback", "?limit=-1", 50},
		{"zero -> fallback (below min)", "?limit=0", 50},
		{"above max -> fallback", "?limit=999999", 50},
		{"garbage -> fallback", "?limit=garbage", 50},
		{"empty -> fallback", "?limit=", 50},
		{"min boundary", "?limit=1", 1},
		{"max boundary", "?limit=500", 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/x"+tc.qs, nil)
			got := queryBoundedInt(r, "limit", 50, 1, 500)
			if got != tc.want {
				t.Errorf("queryBoundedInt(%q) = %d, want %d", tc.qs, got, tc.want)
			}
		})
	}
}

func TestResolveTorrentURLUsesABBURL(t *testing.T) {
	reg, err := sourcestest.Registry()
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	cfg := &config.Config{
		UserAgent: "test-agent",
		Sources:   reg,
	}
	cfg.Sources.AudioBookBay.Mirrors = []string{"audiobookbay.lu"}
	cfg.Sources.AudioBookBay.Trackers = []string{"udp://tracker.example:1337/announce"}

	s := &Server{cfg: cfg}

	oldResolve := resolveABBMagnetFn
	defer func() { resolveABBMagnetFn = oldResolve }()

	called := false
	resolveABBMagnetFn = func(ctx context.Context, client *http.Client, userAgent, abbPath string, mirrors, fallbackTrackers []string) (string, error) {
		called = true
		if userAgent != "test-agent" {
			t.Fatalf("userAgent = %q, want test-agent", userAgent)
		}
		if abbPath != "/abss/the-martian-andy-weir/" {
			t.Fatalf("abbPath = %q, want /abss/the-martian-andy-weir/", abbPath)
		}
		if len(mirrors) != 1 || mirrors[0] != "audiobookbay.lu" {
			t.Fatalf("mirrors = %#v, want audiobookbay.lu", mirrors)
		}
		if len(fallbackTrackers) != 1 || fallbackTrackers[0] != "udp://tracker.example:1337/announce" {
			t.Fatalf("fallbackTrackers = %#v, want configured tracker", fallbackTrackers)
		}
		return "magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567", nil
	}

	got, err := s.resolveTorrentURL(context.Background(), models.DownloadRequest{
		AbbURL: "/abss/the-martian-andy-weir/",
	}, models.SearchResult{})
	if err != nil {
		t.Fatalf("resolveTorrentURL returned error: %v", err)
	}
	if !called {
		t.Fatal("expected ABB resolver to be called")
	}
	want := "magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567"
	if got != want {
		t.Fatalf("resolveTorrentURL = %q, want %q", got, want)
	}
}

func TestResolveTorrentURLPrefersDirectURL(t *testing.T) {
	reg, err := sourcestest.Registry()
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	s := &Server{cfg: &config.Config{Sources: reg}}

	oldResolve := resolveABBMagnetFn
	defer func() { resolveABBMagnetFn = oldResolve }()
	resolveABBMagnetFn = func(context.Context, *http.Client, string, string, []string, []string) (string, error) {
		t.Fatal("ABB resolver should not be called when download_url is present")
		return "", nil
	}

	got, err := s.resolveTorrentURL(context.Background(), models.DownloadRequest{
		DownloadURL: "https://example.com/book.torrent",
		AbbURL:      "/abss/the-martian-andy-weir/",
	}, models.SearchResult{})
	if err != nil {
		t.Fatalf("resolveTorrentURL returned error: %v", err)
	}
	if got != "https://example.com/book.torrent" {
		t.Fatalf("resolveTorrentURL = %q, want direct download URL", got)
	}
}
