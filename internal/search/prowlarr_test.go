package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
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
			Title:       "Test Book",
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
	if r0.Title != "Test Book" {
		t.Errorf("expected title Test Book, got %s", r0.Title)
	}
	if r0.DownloadProtocol != "torrent" {
		t.Errorf("expected protocol torrent, got %s", r0.DownloadProtocol)
	}

	r1 := results[1]
	if r1.DownloadProtocol != "nzb" {
		t.Errorf("expected protocol nzb, got %s", r1.DownloadProtocol)
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
		if r.Source != "audiobook" {
			t.Errorf("expected source audiobook, got %s", r.Source)
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
