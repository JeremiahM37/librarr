package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/sources/sourcestest"
)

func TestDirectProvidersPopulateKnownFormats(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		configure  func(*config.Config, string)
		searcher   func(*config.Config, *http.Client) Searcher
		wantFormat string
	}{
		{
			name: "gutenberg epub",
			body: `{"results":[{"id":1,"title":"Test Book","formats":{"application/epub+zip":"https://example.com/book.epub"}}]}`,
			configure: func(cfg *config.Config, url string) {
				cfg.Sources.Gutenberg.URL = url
			},
			searcher:   func(cfg *config.Config, client *http.Client) Searcher { return NewGutenberg(cfg, client) },
			wantFormat: "epub",
		},
		{
			name: "standard ebooks epub",
			body: `<feed><entry><title>Test Book</title><id>/ebooks/test-author/test-book</id><author><name>Test Author</name></author></entry></feed>`,
			configure: func(cfg *config.Config, url string) {
				cfg.Sources.StandardEbooks.URL = url
			},
			searcher:   func(cfg *config.Config, client *http.Client) Searcher { return NewStandardEbooks(cfg, client) },
			wantFormat: "epub",
		},
		{
			name: "librivox zip",
			body: `{"books":[{"id":"1","title":"Test Book","url_zip_file":"https://example.com/book.zip"}]}`,
			configure: func(cfg *config.Config, url string) {
				cfg.Sources.Librivox.URL = url
			},
			searcher:   func(cfg *config.Config, client *http.Client) Searcher { return NewLibrivox(cfg, client) },
			wantFormat: "zip",
		},
		{
			name: "nyaa title format",
			body: `<rss><channel><item><title>Test Book [CBZ]</title><link>https://example.com/book.torrent</link><seeders>1</seeders></item></channel></rss>`,
			configure: func(cfg *config.Config, url string) {
				cfg.Sources.Nyaa.URL = url
			},
			searcher:   func(cfg *config.Config, client *http.Client) Searcher { return NewNyaaManga(cfg, client) },
			wantFormat: "cbz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()

			registry, err := sourcestest.Registry()
			if err != nil {
				t.Fatalf("load test registry: %v", err)
			}
			cfg := &config.Config{Sources: registry, UserAgent: "test"}
			tc.configure(cfg, server.URL)
			results, err := tc.searcher(cfg, server.Client()).Search(context.Background(), "Test Book")
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			assertSingleFormat(t, results, tc.wantFormat)
		})
	}
}

func assertSingleFormat(t *testing.T, results []models.SearchResult, want string) {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if results[0].Format != want {
		t.Errorf("format = %q, want %q", results[0].Format, want)
	}
}
