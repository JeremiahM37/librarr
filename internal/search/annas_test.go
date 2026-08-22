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
	if results[0].Format != "epub" {
		t.Errorf("expected format epub, got %s", results[0].Format)
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

func TestAnnasArchive_FormatStaysWithResultCard(t *testing.T) {
	htmlContent := `<html><body>
		<div class="flex"><div class="flex"><a href="/md5/11111111111111111111111111111111">No Metadata</a></div></div>
		<div class="flex"><div class="flex"><a href="/md5/22222222222222222222222222222222">PDF Book</a><div class="font-semibold">English [en] · PDF · 1.2MB</div></div></div>
	</body></html>`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(htmlContent))
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "example.com", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}
	results, err := a.doSearch(context.Background(), "book", "", make(map[string]bool))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[0].Format != "" || results[1].Format != "pdf" {
		t.Errorf("formats = [%q, %q], want [\"\", \"pdf\"]", results[0].Format, results[1].Format)
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

// annasCardHTML mirrors the markup annas-archive.gl actually serves for one
// search hit: a cover link, a title link, icon-tagged author/publisher links, a
// description, and the interpunct metadata line — inline <script> included,
// because that script is what breaks a naive .Text() read of the line.
func annasCardHTML(md5, title, author, publisher, metaLine string) string {
	return `
	<div class="flex pt-3 pb-3 border-b last:border-b-0 border-gray-100">
	  <a href="/md5/` + md5 + `" class="custom-a block mr-2 sm:mr-4 hover:opacity-80">
	    <div class="w-20 h-[7.5rem] rounded shadow relative overflow-hidden text-left">
	      <img src="https://example.invalid/cover.jpg" alt=""/>
	    </div>
	  </a>
	  <div class="max-w-full overflow-hidden flex flex-col justify-around">
	    <div>
	      <div class="line-clamp-[2] text-[9px] text-gray-500 font-mono">lgli/` + author + ` - ` + title + `.epub</div>
	      <a href="/md5/` + md5 + `" class="js-vim-focus custom-a font-semibold text-lg leading-[1.2]">` + title + `</a>
	      <a href="/search?q=x" class="custom-a text-sm leading-[1.2]"><span class="icon-[mdi--user-edit] text-base align-sub"></span> ` + author + `</a>
	      <a href="/search?q=y" class="custom-a text-sm leading-[1.2]"><span class="icon-[mdi--company] text-base align-sub"></span> ` + publisher + `</a>
	    </div>
	    <div>
	      <div class="text-sm text-gray-600 mt-2 mb-2 leading-[1.3]">In The Son of Neptune, Percy and Frank met at Camp Jupiter.</div>
	    </div>
	    <div class="text-gray-800 font-semibold text-sm leading-[1.2] mt-2">` + metaLine + ` · <a href="#" class="custom-a font-semibold text-sm">Save<script>var aarecord_id = "md5:` + md5 + `";</script></a></div>
	  </div>
	</div>`
}

func TestParseAnnasMetaLine(t *testing.T) {
	tests := []struct {
		name string
		line string
		want annasCardMeta
	}{
		{
			name: "full line",
			line: "English [en] · EPUB · 1.2MB · 2012 · 📕 Book (fiction) · 🚀/lgli/zlib",
			want: annasCardMeta{Language: "en", Format: "epub", Size: "1.2MB", Year: "2012"},
		},
		{
			name: "no year",
			line: "German [de] · PDF · 12.4MB · 📕 Book (fiction)",
			want: annasCardMeta{Language: "de", Format: "pdf", Size: "12.4MB"},
		},
		{
			name: "no language",
			line: "AZW3 · 900KB · 1998",
			want: annasCardMeta{Format: "azw3", Size: "900KB", Year: "1998"},
		},
		{
			name: "multiword language name",
			line: "Chinese (Simplified) [zh] · EPUB · 3.0MB",
			want: annasCardMeta{Language: "zh", Format: "epub", Size: "3.0MB"},
		},
		{
			name: "segments in an unexpected order",
			line: "2012 · 4.5MiB · epub · Spanish [es]",
			want: annasCardMeta{Language: "es", Format: "epub", Size: "4.5MiB", Year: "2012"},
		},
		{
			name: "content type is not mistaken for a format",
			line: "English [en] · 1.1MB · 📗 Book (unknown)",
			want: annasCardMeta{Language: "en", Size: "1.1MB"},
		},
		{
			name: "page counts and ids are not mistaken for years",
			line: "English [en] · EPUB · 2.0MB · 9781423140672",
			want: annasCardMeta{Language: "en", Format: "epub", Size: "2.0MB"},
		},
		{
			name: "empty",
			line: "",
			want: annasCardMeta{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseAnnasMetaLine(tt.line); got != tt.want {
				t.Errorf("parseAnnasMetaLine(%q) = %+v, want %+v", tt.line, got, tt.want)
			}
		})
	}
}

func TestAnnasPublisherImprint(t *testing.T) {
	tests := []struct {
		citation string
		want     string
	}{
		{"Hyperion Book CH, The Heroes of Olympus, Book Three, New York, USA, 2012", "Hyperion Book CH"},
		{"Disney Book Group : Made available through hoopla", "Disney Book Group"},
		{"Thorndike Press;Disney Hyperion", "Thorndike Press"},
		{"Penguin Books", "Penguin Books"},
		{"", ""},
		{strings.Repeat("x", 200), ""}, // not a citation shape — drop rather than show noise
	}

	for _, tt := range tests {
		t.Run(tt.citation, func(t *testing.T) {
			if got := annasPublisherImprint(tt.citation); got != tt.want {
				t.Errorf("annasPublisherImprint(%q) = %q, want %q", tt.citation, got, tt.want)
			}
		})
	}
}

func TestAnnasArchive_SurfacesCardMetadata(t *testing.T) {
	htmlContent := "<html><body>" +
		annasCardHTML(
			"3e8184fac9f9d2413af8260dbf240ac9",
			"The Mark of Athena",
			"Rick Riordan [Riordan, Rick]",
			"Hyperion Book CH, The Heroes of Olympus, Book Three, New York, USA, October 2, 2012",
			"English [en] · EPUB · 1.2MB · 2012 · 📕 Book (fiction)",
		) +
		annasCardHTML(
			"e90a8c3dcdb0a30eb61b0b9b0c686502",
			"Das Zeichen der Athene",
			"Rick Riordan",
			"Carlsen Verlag GmbH",
			"German [de] · EPUB · 0.7MB",
		) +
		"</body></html>"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(htmlContent))
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "annas-archive.gl", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}

	results, err := a.doSearch(context.Background(), "mark of athena", "epub", make(map[string]bool))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}

	got := results[0]
	if got.Title != "The Mark of Athena" {
		t.Errorf("Title = %q, want %q", got.Title, "The Mark of Athena")
	}
	// The "Last, First" alias Anna's appends to the author link is noise.
	if got.Author != "Rick Riordan" {
		t.Errorf("Author = %q, want %q", got.Author, "Rick Riordan")
	}
	if got.Language != "en" {
		t.Errorf("Language = %q, want %q", got.Language, "en")
	}
	if got.Year != "2012" {
		t.Errorf("Year = %q, want %q", got.Year, "2012")
	}
	if got.Publisher != "Hyperion Book CH" {
		t.Errorf("Publisher = %q, want %q", got.Publisher, "Hyperion Book CH")
	}
	if got.SizeHuman != "1.2MB" {
		t.Errorf("SizeHuman = %q, want %q", got.SizeHuman, "1.2MB")
	}
	if got.Format != "epub" {
		t.Errorf("Format = %q, want %q", got.Format, "epub")
	}

	if results[1].Language != "de" {
		t.Errorf("second result Language = %q, want %q", results[1].Language, "de")
	}
	if results[1].Publisher != "Carlsen Verlag GmbH" {
		t.Errorf("second result Publisher = %q, want %q", results[1].Publisher, "Carlsen Verlag GmbH")
	}
	// This card carries no year; nothing should be invented for it.
	if results[1].Year != "" {
		t.Errorf("second result Year = %q, want empty", results[1].Year)
	}
}

func TestAnnasArchive_TitlePatternAuthorStillWins(t *testing.T) {
	// The legacy "Last, First - Title" heuristic must keep working on cards that
	// carry no author link at all.
	htmlContent := `<html><body>
		<div class="flex">
			<a href="/md5/abc123def456789012345678901234ab">Fitzgerald, F. Scott - The Great Gatsby</a>
			<div class="font-semibold leading-[1.2]">English [en] · EPUB · 1.5MB · 1925</div>
		</div>
	</body></html>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(htmlContent))
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "annas-archive.gl", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}
	results, err := a.doSearch(context.Background(), "gatsby", "epub", make(map[string]bool))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if results[0].Author != "Fitzgerald, F. Scott" {
		t.Errorf("Author = %q, want %q", results[0].Author, "Fitzgerald, F. Scott")
	}
	if results[0].Title != "The Great Gatsby" {
		t.Errorf("Title = %q, want %q", results[0].Title, "The Great Gatsby")
	}
	if results[0].Language != "en" || results[0].Year != "1925" || results[0].SizeHuman != "1.5MB" {
		t.Errorf("metadata = {lang:%q year:%q size:%q}, want {en 1925 1.5MB}",
			results[0].Language, results[0].Year, results[0].SizeHuman)
	}
}

func TestAnnasCardLink_IgnoresAmbiguousContainers(t *testing.T) {
	// If Anna's markup ever changes such that Closest("div.flex") lands on a
	// container holding several cards, the author links become ambiguous.
	// Reporting nothing is correct; reporting a neighbouring book's author is not.
	htmlContent := `<html><body>
		<div class="flex">
			<a href="/md5/11111111111111111111111111111111">Book One</a>
			<a href="/search?q=a"><span class="icon-[mdi--user-edit]"></span> Author One</a>
			<a href="/md5/22222222222222222222222222222222">Book Two</a>
			<a href="/search?q=b"><span class="icon-[mdi--user-edit]"></span> Author Two</a>
		</div>
	</body></html>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(htmlContent))
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "annas-archive.gl", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}
	results, err := a.doSearch(context.Background(), "book", "epub", make(map[string]bool))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	for i, r := range results {
		if r.Author != "" {
			t.Errorf("results[%d].Author = %q, want empty for an ambiguous container", i, r.Author)
		}
	}
}

// TestAnnasArchive_SearchAllVariantsFailReturnsError covers the failure shape
// that hid a real outage: when Anna's Archive moved behind a DDoS-Guard
// challenge, every search variant failed, but Search returned (nil, nil). The
// source therefore reported a successful empty search — its health score stayed
// at 100 with search_ok climbing — while it produced no results at all. An
// empty success is indistinguishable from "this query has no matches", so
// nothing surfaced the breakage.
func TestAnnasArchive_SearchAllVariantsFailReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "example.com", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}

	results, err := a.Search(context.Background(), "dune")
	if err == nil {
		t.Fatal("every variant failed but Search reported success — the circuit breaker never trips and the source looks healthy while returning nothing")
	}
	if len(results) != 0 {
		t.Errorf("expected no results alongside the error, got %d", len(results))
	}
}

// A genuinely empty result set is still a success: "no matches for this query"
// must not be reported as a source failure, or the circuit breaker would open
// on obscure searches.
func TestAnnasArchive_SearchEmptyButHealthyIsNotAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><body>no results</body></html>`))
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "example.com", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}

	results, err := a.Search(context.Background(), "zzzz-no-such-book")
	if err != nil {
		t.Fatalf("an empty but healthy search must not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

// One variant failing while the other succeeds must still return the hits.
func TestAnnasArchive_SearchPartialFailureStillReturnsResults(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`<html><body>
			<a href="/md5/abc123def456789012345678901234ab">Dune</a>
		</body></html>`))
	}))
	defer server.Close()

	a := &AnnasArchive{
		cfg:    &config.Config{AnnasArchiveDomain: "example.com", UserAgent: "test"},
		client: &http.Client{Transport: &rewriteTransport{serverURL: server.URL}},
	}

	results, err := a.Search(context.Background(), "dune")
	if err != nil {
		t.Fatalf("a partial failure must not fail the whole search: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected the surviving variant's results to come through")
	}
}
