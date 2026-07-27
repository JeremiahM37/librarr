package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
)

const flibustaOPDSFixture = `<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Война и мир</title>
    <author><name>Лев Толстой</name></author>
    <id>urn:b:12345</id>
    <link rel="http://opds-spec.org/image" href="/covers/12345.jpg"/>
    <link rel="http://opds-spec.org/acquisition" href="/b/12345/epub" type="application/epub+zip"/>
    <link rel="http://opds-spec.org/acquisition" href="/b/12345/fb2" type="application/fb2+zip"/>
    <content>2.1 МБ</content>
  </entry>
  <entry>
    <title>Anna Karenina</title>
    <author><name>Leo Tolstoy</name></author>
    <id>urn:b:67890</id>
    <link rel="http://opds-spec.org/acquisition" href="http://example.com/abs/67890.epub" type="application/epub+zip"/>
  </entry>
</feed>`

func TestFlibustaSearch(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("searchTerm")
		w.Header().Set("Content-Type", "application/atom+xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(flibustaOPDSFixture))
	}))
	defer srv.Close()

	cfg := &config.Config{
		FlibustaURL:     srv.URL,
		FlibustaEnabled: true,
		UserAgent:       "test-agent",
	}
	f := NewFlibusta(cfg, &http.Client{Timeout: 5 * time.Second})

	if !f.Enabled() {
		t.Fatalf("Flibusta should be enabled")
	}

	results, err := f.Search(context.Background(), "война")
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if gotQuery != "война" {
		t.Errorf("server got query %q, want %q", gotQuery, "война")
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	// First entry: prefers EPUB over FB2, resolves relative URL against base.
	r0 := results[0]
	if r0.Source != "flibusta" {
		t.Errorf("Source = %q, want flibusta", r0.Source)
	}
	if r0.Title != "Война и мир" {
		t.Errorf("Title = %q, want 'Война и мир'", r0.Title)
	}
	if r0.Author != "Лев Толстой" {
		t.Errorf("Author = %q, want 'Лев Толстой'", r0.Author)
	}
	if r0.Format != "EPUB" {
		t.Errorf("Format = %q, want EPUB (should prefer epub over fb2)", r0.Format)
	}
	if !strings.HasPrefix(r0.DownloadURL, srv.URL) || !strings.HasSuffix(r0.DownloadURL, "/b/12345/epub") {
		t.Errorf("DownloadURL = %q, want absolute URL ending in /b/12345/epub", r0.DownloadURL)
	}
	if r0.SourceID != "flibusta-12345" {
		t.Errorf("SourceID = %q, want flibusta-12345", r0.SourceID)
	}

	// Second entry: absolute URL should pass through unchanged.
	r1 := results[1]
	if r1.DownloadURL != "http://example.com/abs/67890.epub" {
		t.Errorf("absolute DownloadURL mangled: %q", r1.DownloadURL)
	}
}

func TestFlibustaSearchHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := &config.Config{FlibustaURL: srv.URL, FlibustaEnabled: true, UserAgent: "test"}
	f := NewFlibusta(cfg, &http.Client{Timeout: 5 * time.Second})

	if _, err := f.Search(context.Background(), "x"); err == nil {
		t.Fatalf("expected error on HTTP 500, got nil")
	}
}

func TestFlibustaDisabled(t *testing.T) {
	cfg := &config.Config{FlibustaURL: "", FlibustaEnabled: false}
	f := NewFlibusta(cfg, &http.Client{})
	if f.Enabled() {
		t.Errorf("Enabled() true with empty URL; want false")
	}
}
