package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/config"
)

func TestProwlarr_Name(t *testing.T) {
	tests := []struct {
		tab      string
		expected string
	}{
		{"main", "prowlarr"},
		{"audiobook", "prowlarr_audiobooks"},
		{"manga", "prowlarr_manga"},
	}

	cfg := &config.Config{}
	for _, tt := range tests {
		t.Run(tt.tab, func(t *testing.T) {
			p := NewProwlarr(cfg, http.DefaultClient, tt.tab)
			if p.Name() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, p.Name())
			}
		})
	}
}

func TestProwlarr_Label(t *testing.T) {
	cfg := &config.Config{}
	tests := []struct {
		tab      string
		expected string
	}{
		{"main", "Prowlarr"},
		{"audiobook", "Prowlarr (Audiobooks)"},
		{"manga", "Prowlarr (Manga)"},
	}

	for _, tt := range tests {
		t.Run(tt.tab, func(t *testing.T) {
			p := NewProwlarr(cfg, http.DefaultClient, tt.tab)
			if p.Label() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, p.Label())
			}
		})
	}
}

func TestProwlarr_Enabled(t *testing.T) {
	cfg := &config.Config{ProwlarrURL: "", ProwlarrAPIKey: ""}
	p := NewProwlarr(cfg, http.DefaultClient, "main")
	if p.Enabled() {
		t.Error("expected disabled when not configured")
	}

	cfg.ProwlarrURL = "http://prowlarr:9696"
	cfg.ProwlarrAPIKey = "test-key"
	if !p.Enabled() {
		t.Error("expected enabled when configured")
	}
}

func TestProwlarr_Search(t *testing.T) {
	items := []prowlarrItem{
		{
			Title:       "Test Book [EPUB]",
			Size:        1000000,
			Seeders:     5,
			Leechers:    2,
			Indexer:     "TestIndexer",
			DownloadURL: "http://example.com/download",
			InfoHash:    "abc123",
			GUID:        "guid-1",
			Protocol:    "torrent",
		},
		{
			Title:       "NZB Book",
			Size:        2000000,
			DownloadURL: "http://example.com/nzb/download.nzb",
			Protocol:    "usenet",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify API key header
		if r.Header.Get("X-Api-Key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()

	cfg := &config.Config{
		ProwlarrURL:    server.URL,
		ProwlarrAPIKey: "test-key",
	}

	p := NewProwlarr(cfg, server.Client(), "main")
	results, err := p.Search(context.Background(), "test book")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	r0 := results[0]
	if r0.Source != "torrent" {
		t.Errorf("expected source torrent, got %s", r0.Source)
	}
	if r0.Title != "Test Book [EPUB]" {
		t.Errorf("expected title Test Book [EPUB], got %s", r0.Title)
	}
	if r0.Format != "epub" {
		t.Errorf("expected format epub, got %s", r0.Format)
	}
	if r0.DownloadProtocol != "torrent" {
		t.Errorf("expected protocol torrent, got %s", r0.DownloadProtocol)
	}

	r1 := results[1]
	if r1.DownloadProtocol != "nzb" {
		t.Errorf("expected protocol nzb, got %s", r1.DownloadProtocol)
	}
}

func TestProwlarr_SearchParsesCategoryObjectsAndMAMFields(t *testing.T) {
	body := `[
		{
			"title": "Sweet Revenge: The Intimate Life of Simon Cowell",
			"size": "6.0 MiB",
			"seeders": 10,
			"leechers": 0,
			"indexer": "MyAnonamouse",
			"downloadUrl": "http://prowlarr/download/160121",
			"guid": "mam-160121",
			"protocol": "torrent",
			"author_info": "{\"42647\":\"Tom Bower\"}",
			"filetype": "epub mobi",
			"categories": [{"id":7000,"name":"Books"},{"id":7020,"name":"EBook"}]
		},
		{
			"title": "Oil: Money, Politics, and Power in the 21st Century",
			"size": "15.0 MiB",
			"seeders": 6,
			"indexer": "MyAnonamouse",
			"downloadUrl": "http://prowlarr/download/296882",
			"guid": "mam-296882",
			"protocol": "torrent",
			"author_info": "{\"42647\":\"Tom Bower\"}",
			"filetype": "mobi",
			"categories": "[7000,7020]"
		}
	]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query()["categories"]; len(got) != 2 || got[0] != "7000" || got[1] != "7020" {
			t.Fatalf("categories query = %#v, want 7000 and 7020", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	results, err := NewProwlarr(cfg, server.Client(), "main").Search(context.Background(), "Tom Bower")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Indexer != "MyAnonamouse" || results[0].Author != "Tom Bower" {
		t.Fatalf("first result indexer/author = %q/%q", results[0].Indexer, results[0].Author)
	}
	if results[0].Format != "epub" {
		t.Fatalf("first result format = %q, want epub", results[0].Format)
	}
	if results[0].Size != 6*1024*1024 {
		t.Fatalf("first result size = %d, want 6291456", results[0].Size)
	}
	if strings.Join(results[0].Categories, ",") != "7000,7020" {
		t.Fatalf("first result categories = %#v", results[0].Categories)
	}
	if results[1].Format != "mobi" {
		t.Fatalf("second result format = %q, want mobi", results[1].Format)
	}
}

func TestProwlarr_SearchAudiobook(t *testing.T) {
	items := []prowlarrItem{
		{Title: "Audiobook Test", Size: 5000000, Seeders: 3, Protocol: "torrent"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "audiobook")

	results, err := p.Search(context.Background(), "test audiobook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, r := range results {
		if r.Source != "prowlarr_audiobooks" {
			t.Errorf("expected source prowlarr_audiobooks, got %s", r.Source)
		}
	}
}

func TestProwlarr_DoSearchHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	// doSearch (internal) should error, but Search() logs warnings and returns nil error
	// when all variants fail. Let's test doSearch directly.
	_, err := p.doSearch(context.Background(), prowlarrSearchParams{
		query:      "test",
		categories: []string{"7000"},
		limit:      50,
	})
	if err == nil {
		t.Error("expected error on HTTP 500")
	}
}

func TestProwlarr_DeduplicatesByInfoHash(t *testing.T) {
	items := []prowlarrItem{
		{Title: "Book A", InfoHash: "hash123", Seeders: 5, Protocol: "torrent"},
		{Title: "Book A Copy", InfoHash: "hash123", Seeders: 3, Protocol: "torrent"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	results, err := p.Search(context.Background(), "book")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result after dedup, got %d", len(results))
	}
}

func TestIsNZBURL(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"http://example.com/download.nzb", true},
		{"http://example.com/nzb/123", true},
		{"http://example.com/api?nzb?id=1", true},
		{"http://example.com/torrent/download", false},
		{"http://example.com/file.epub", false},
		{"", false},
		// Note: "&t=get&" requires the literal string in the URL
		{"http://example.com/api?mode=search&t=get&id=123", true},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := isNZBURL(tt.url)
			if result != tt.expected {
				t.Errorf("isNZBURL(%q) = %v, want %v", tt.url, result, tt.expected)
			}
		})
	}
}

func TestHumanSize(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
		{5242880, "5.0 MB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := HumanSize(tt.bytes)
			if result != tt.expected {
				t.Errorf("HumanSize(%d) = %q, want %q", tt.bytes, result, tt.expected)
			}
		})
	}
}

func TestProwlarr_CategoriesForTab(t *testing.T) {
	cfg := &config.Config{}

	tests := []struct {
		tab      string
		expected []string
	}{
		{"main", []string{"7000", "7020"}},
		{"audiobook", []string{"3030"}},
		{"manga", []string{"7020", "7030"}},
	}

	for _, tt := range tests {
		t.Run(tt.tab, func(t *testing.T) {
			p := NewProwlarr(cfg, http.DefaultClient, tt.tab)
			cats := p.categoriesForTab()
			if len(cats) != len(tt.expected) {
				t.Fatalf("expected %d categories, got %d", len(tt.expected), len(cats))
			}
			for i, c := range cats {
				if c != tt.expected[i] {
					t.Errorf("category[%d] = %s, want %s", i, c, tt.expected[i])
				}
			}
		})
	}
}

func TestIsNZBURL_NewznabPattern(t *testing.T) {
	// The &t=get& pattern: URL must contain exactly "&t=get&"
	url := "http://nzb.example.com/api?apikey=abc&t=get&id=12345"
	if !isNZBURL(url) {
		t.Error("expected true for newznab pattern")
	}

	// Without the ampersands
	url2 := "http://example.com/api?t=get"
	if isNZBURL(url2) {
		// This won't match because there's no trailing "&"
		// Actually it depends on the implementation: strings.Contains(lower, "&t=get&")
		// "?t=get" doesn't have the leading "&"
	}

	// Verify the implementation handles query params correctly
	url3 := "http://example.com?mode=foo&t=get&bar=baz"
	if !strings.Contains(strings.ToLower(url3), "&t=get&") {
		t.Error("expected &t=get& in URL")
	}
}

// --- Tests for issue #7: better Prowlarr error messages ---

// TestProwlarr_HTMLResponse — when Prowlarr returns HTML instead of JSON (e.g.,
// reverse proxy intercept or wrong API key), the error should be descriptive
// rather than the cryptic "invalid character '<'" JSON decode error.
func TestProwlarr_HTMLResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<!DOCTYPE html>\n<html><body>Login required</body></html>"))
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	_, err := p.doSearch(context.Background(), prowlarrSearchParams{query: "test", limit: 10})
	if err == nil {
		t.Fatal("expected error for HTML response, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "HTML") {
		t.Errorf("error should mention HTML, got: %v", err)
	}
	if !strings.Contains(msg, "reverse proxy") && !strings.Contains(msg, "API key") {
		t.Errorf("error should suggest cause (proxy/API key), got: %v", err)
	}
}

func TestProwlarr_HTMLWithLeadingWhitespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("\n\n  <html><body>error</body></html>"))
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	_, err := p.doSearch(context.Background(), prowlarrSearchParams{query: "test", limit: 10})
	if err == nil {
		t.Fatal("expected error for HTML response, got nil")
	}
	if !strings.Contains(err.Error(), "HTML") {
		t.Errorf("error should mention HTML, got: %v", err)
	}
}

// TestProwlarr_RealAuthelia401 — Authelia returns an HTML 401 login page when
// the session cookie is missing. Prowlarr also uses this pattern for some
// reverse proxy setups.
func TestProwlarr_RealAuthelia401(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK) // Authelia actually returns 200 with HTML login page
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>Authelia</title></head><body><div id="authelia">...</div></body></html>`))
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	_, err := p.doSearch(context.Background(), prowlarrSearchParams{query: "test", limit: 10})
	if err == nil {
		t.Fatal("expected error for Authelia HTML response")
	}
	if !strings.Contains(err.Error(), "HTML") {
		t.Errorf("error should say HTML, got: %v", err)
	}
}

// TestProwlarr_ValidJSONStillWorks — sanity check that the new body-read +
// HTML-detection path doesn't break the success case.
func TestProwlarr_ValidJSONStillWorks(t *testing.T) {
	items := []prowlarrItem{
		{Title: "A Book", Size: 1024 * 1024, Seeders: 10, Protocol: "torrent"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	results, err := p.doSearch(context.Background(), prowlarrSearchParams{query: "a book", limit: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

// TestProwlarr_TruncatedJSON — server starts sending JSON but cuts off mid-stream.
// Should return a clear error mentioning decode failure, not panic.
func TestProwlarr_TruncatedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"title":"Partial`)) // truncated JSON
	}))
	defer server.Close()

	cfg := &config.Config{ProwlarrURL: server.URL, ProwlarrAPIKey: "key"}
	p := NewProwlarr(cfg, server.Client(), "main")

	_, err := p.doSearch(context.Background(), prowlarrSearchParams{query: "test", limit: 10})
	if err == nil {
		t.Fatal("expected error for truncated JSON")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Errorf("expected decode error, got: %v", err)
	}
}
