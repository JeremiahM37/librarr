package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/sources/sourcestest"
)

// testCfgWithSourcesURL returns a config wired up with a registry whose
// ThePirateBay endpoint points at the given test-server URL.
func testCfgWithSourcesURL(tpbURL string) *config.Config {
	reg, _ := sourcestest.Registry()
	reg.ThePirateBay.URL = tpbURL
	return &config.Config{TPBEnabled: true, UserAgent: "test", Sources: reg}
}

// apibayFixture is a two-item response; the second entry has 0 seeders
// and should be filtered out. The third entry is a duplicate info_hash
// that should also be dropped.
const apibayFixture = `[
  {"id":"1","name":"Programming Go EPUB","info_hash":"AABBCCDD0011","leechers":"4","seeders":"17","size":"4194304","num_files":"1","username":"anon","added":"1700000000","category":"601"},
  {"id":"2","name":"Dead Torrent","info_hash":"11223344DEAD","leechers":"0","seeders":"0","size":"1024","num_files":"1","username":"anon","added":"1700000000","category":"601"},
  {"id":"3","name":"Programming Go (dup)","info_hash":"AABBCCDD0011","leechers":"2","seeders":"5","size":"4194304","num_files":"1","username":"anon","added":"1700000000","category":"601"}
]`

func TestThePirateBaySearch(t *testing.T) {
	var gotCat, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCat = r.URL.Query().Get("cat")
		gotQuery = r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(apibayFixture))
	}))
	defer srv.Close()

	cfg := testCfgWithSourcesURL(srv.URL)
	s := NewThePirateBay(cfg, &http.Client{Timeout: 5 * time.Second}, "main")

	if !s.Enabled() {
		t.Fatal("TPB should be enabled")
	}
	if s.Name() != "tpb" {
		t.Errorf("Name() = %q, want tpb", s.Name())
	}

	results, err := s.Search(context.Background(), "programming go")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if gotQuery != "programming go" {
		t.Errorf("server got q=%q", gotQuery)
	}
	if gotCat != "601" {
		t.Errorf("server got cat=%q, want 601 (ebooks)", gotCat)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1 (0-seeder + dup should be dropped)", len(results))
	}
	r := results[0]
	if r.Source != "tpb" {
		t.Errorf("Source = %q", r.Source)
	}
	if r.Seeders != 17 {
		t.Errorf("Seeders = %d, want 17", r.Seeders)
	}
	if r.Leechers != 4 {
		t.Errorf("Leechers = %d, want 4 (was previously hardcoded to 0)", r.Leechers)
	}
	if r.InfoHash != "AABBCCDD0011" {
		t.Errorf("InfoHash = %q", r.InfoHash)
	}
	if r.Format != "epub" {
		t.Errorf("Format = %q, want epub", r.Format)
	}
	if !strings.HasPrefix(r.MagnetURL, "magnet:?xt=urn:btih:AABBCCDD0011") {
		t.Errorf("MagnetURL missing info_hash: %q", r.MagnetURL)
	}
	if !strings.Contains(r.MagnetURL, "tr=") || !strings.Contains(r.MagnetURL, "opentrackr.org") {
		t.Errorf("MagnetURL missing tracker params (peer discovery will fail): %q", r.MagnetURL)
	}
}

func TestThePirateBayAudiobookTab(t *testing.T) {
	var gotCat string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCat = r.URL.Query().Get("cat")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	cfg := testCfgWithSourcesURL(srv.URL)
	s := NewThePirateBay(cfg, &http.Client{Timeout: 5 * time.Second}, "audiobook")

	if s.Name() != "tpb_audiobook" {
		t.Errorf("Name() = %q, want tpb_audiobook", s.Name())
	}
	if _, err := s.Search(context.Background(), "x"); err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if gotCat != "102" {
		t.Errorf("audiobook tab queried cat=%q, want 102", gotCat)
	}
}

func TestBuildMagnetHasTrackers(t *testing.T) {
	m := buildMagnet("ABCDEF", "My Torrent")
	if !strings.HasPrefix(m, "magnet:?xt=urn:btih:ABCDEF") {
		t.Errorf("bad prefix: %q", m)
	}
	if !strings.Contains(m, "&dn=My+Torrent") {
		t.Errorf("dn not URL-encoded: %q", m)
	}
	trackerCount := strings.Count(m, "&tr=")
	if trackerCount < 3 {
		t.Errorf("want at least 3 trackers appended, got %d: %q", trackerCount, m)
	}
}
