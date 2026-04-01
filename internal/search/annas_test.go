package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestAnnasArchive_Metadata(t *testing.T) {
	cfg := &config.Config{AnnasArchiveDomain: "annas-archive.gl"}
	a := NewAnnasArchive(cfg, http.DefaultClient)

	if a.Name() != "annas" {
		t.Errorf("expected name annas, got %s", a.Name())
	}
	if a.Label() != "Anna's Archive" {
		t.Errorf("expected label Anna's Archive, got %s", a.Label())
	}
	if !a.Enabled() {
		t.Error("expected enabled when domain is set")
	}
	if a.SearchTab() != "main" {
		t.Errorf("expected tab main, got %s", a.SearchTab())
	}
	if a.DownloadType() != "direct" {
		t.Errorf("expected download type direct, got %s", a.DownloadType())
	}
}

func TestAnnasArchive_Disabled(t *testing.T) {
	cfg := &config.Config{AnnasArchiveDomain: ""}
	a := NewAnnasArchive(cfg, http.DefaultClient)
	if a.Enabled() {
		t.Error("expected disabled when domain is empty")
	}
}

func TestAnnasArchive_DoSearchParsesHTML(t *testing.T) {
	htmlContent := `<html><body>
	<div class="results">
		<a href="/md5/abc123def456789012345678901234ab">
			<div class="leading-[1.2]">English [en] · EPUB · 1.5MB · 2020</div>
			Fitzgerald, F. Scott - The Great Gatsby
		</a>
		<a href="/md5/def456789012345678901234567890cd">
			<div class="leading-[1.2]">English [en] · EPUB · 2.3MB · 2019</div>
			Another Book Title
		</a>
	</div>
	</body></html>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(htmlContent))
	}))
	defer server.Close()

	// Extract host from server URL to use as domain
	serverHost := strings.TrimPrefix(server.URL, "http://")

	cfg := &config.Config{
		AnnasArchiveDomain: serverHost,
		UserAgent:          "test",
	}

	// Create a client that doesn't use HTTPS (since test server is HTTP)
	a := &AnnasArchive{cfg: cfg, client: server.Client()}

	// We need to override the HTTPS scheme. Since doSearch uses https://{domain},
	// and our test server is HTTP, let's test parsing differently.
	// Instead, use a transport that rewrites URLs.
	transport := &rewriteTransport{base: server.Client().Transport, serverURL: server.URL}
	client := &http.Client{Transport: transport}
	a.client = client

	seenMD5 := make(map[string]bool)
	results, err := a.doSearch(context.Background(), "gatsby", "epub", seenMD5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].MD5 != "abc123def456789012345678901234ab" {
		t.Errorf("expected MD5 abc123..., got %s", results[0].MD5)
	}
	if results[0].Source != "annas" {
		t.Errorf("expected source annas, got %s", results[0].Source)
	}
}

// rewriteTransport redirects all HTTPS requests to the test server.
type rewriteTransport struct {
	base      http.RoundTripper
	serverURL string
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.serverURL, "http://")
	if t.base != nil {
		return t.base.RoundTrip(req)
	}
	return http.DefaultTransport.RoundTrip(req)
}

func TestAnnasArchive_DoSearchHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	cfg := &config.Config{
		AnnasArchiveDomain: "example.com",
		UserAgent:          "test",
	}

	transport := &rewriteTransport{serverURL: server.URL}
	client := &http.Client{Transport: transport}
	a := &AnnasArchive{cfg: cfg, client: client}
	seenMD5 := make(map[string]bool)

	_, err := a.doSearch(context.Background(), "test", "", seenMD5)
	if err == nil {
		t.Error("expected error on HTTP 403")
	}
}

func TestAnnasArchive_SeenMD5Dedup(t *testing.T) {
	htmlContent := `<html><body>
		<a href="/md5/abc123def456789012345678901234ab">Book A</a>
	</body></html>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(htmlContent))
	}))
	defer server.Close()

	cfg := &config.Config{AnnasArchiveDomain: "example.com", UserAgent: "test"}
	transport := &rewriteTransport{serverURL: server.URL}
	client := &http.Client{Transport: transport}
	a := &AnnasArchive{cfg: cfg, client: client}

	seenMD5 := map[string]bool{"abc123def456789012345678901234ab": true}
	results, err := a.doSearch(context.Background(), "test", "", seenMD5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results (already seen MD5), got %d", len(results))
	}
}

func TestParseSizeBytes_EdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"0.5 MB", 0.5e6},
		{"1 GB", 1e9},
		{"100 KB", 100e3},
		{"50 B", 50},
		{"", 0},
		{"no size here", 0},
		{"1.5 TB", 0}, // TB not supported in regex
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSizeBytes(tt.input)
			if result != tt.expected {
				t.Errorf("parseSizeBytes(%q) = %f, want %f", tt.input, result, tt.expected)
			}
		})
	}
}
