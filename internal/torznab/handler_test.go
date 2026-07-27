package torznab

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/search"
)

func newTestHandler(apiKey string) *Handler {
	cfg := &config.Config{
		TorznabAPIKey:       apiKey,
		MinTorrentSizeBytes: 10000,
		MaxTorrentSizeBytes: 2000000000,
	}
	health := search.NewHealthTracker(3, 300)
	manager := search.NewManager(cfg, nil, health)
	return NewHandler(cfg, manager)
}

func TestHandler_CapsEndpoint(t *testing.T) {
	h := newTestHandler("")

	req := httptest.NewRequest("GET", "/torznab/api?t=caps", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "<caps>") {
		t.Error("expected caps XML in response")
	}
	if !strings.Contains(body, "Librarr") {
		t.Error("expected Librarr server title")
	}
}

func TestHandler_APIKeyValidation(t *testing.T) {
	h := newTestHandler("secret-key")

	t.Run("missing API key", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/torznab/api?t=caps", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", w.Code)
		}
	})

	t.Run("wrong API key", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/torznab/api?t=caps&apikey=wrong", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", w.Code)
		}
	})

	t.Run("correct API key", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/torznab/api?t=caps&apikey=secret-key", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})
}

func TestHandler_NoAPIKeyConfigured(t *testing.T) {
	h := newTestHandler("")

	req := httptest.NewRequest("GET", "/torznab/api?t=caps", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when no API key configured, got %d", w.Code)
	}
}

func TestHandler_UnknownFunction(t *testing.T) {
	h := newTestHandler("")

	req := httptest.NewRequest("GET", "/torznab/api?t=unknown", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var errResp models.TorznabError
	body := w.Body.Bytes()
	// Skip XML header
	xmlContent := strings.TrimPrefix(string(body), xml.Header)
	if err := xml.Unmarshal([]byte(xmlContent), &errResp); err != nil {
		t.Fatalf("failed to parse error XML: %v\nbody: %s", err, string(body))
	}
	if errResp.Code != "202" {
		t.Errorf("expected error code 202, got %s", errResp.Code)
	}
}

// TestHandler_MissingQueryReturnsProbeResponse — Prowlarr's RSS health check
// polls t=search with no query and REQUIRES ≥1 item (for Generic Torznab
// indexers) to confirm the integration works. Returning 400 broke save
// entirely; returning a truly empty <rss> broke Prowlarr's validator.
// Compromise: a single labeled placeholder item with a guid prefixed
// "librarr-placeholder-" so downstream *arr apps won't try to grab it.
// Regression guard for issue #20.
func TestHandler_MissingQueryReturnsProbeResponse(t *testing.T) {
	h := newTestHandler("")

	req := httptest.NewRequest("GET", "/torznab/api?t=search", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<rss") {
		t.Errorf("expected RSS feed, got: %s", body[:min(200, len(body))])
	}
	if strings.Contains(body, "<error") {
		t.Errorf("expected no <error> element: %s", body[:min(300, len(body))])
	}
	if !strings.Contains(body, "<item>") {
		t.Errorf("Prowlarr requires ≥1 item in the probe response, got 0: %s",
			body[:min(400, len(body))])
	}
	// Downstream *arr apps key on this prefix to avoid auto-grabbing probe items.
	if !strings.Contains(body, "librarr-placeholder-") {
		t.Errorf("probe item guid must include 'librarr-placeholder-' sentinel: %s",
			body[:min(400, len(body))])
	}
	// Prowlarr's RSS validator rejects items without a pubDate — must be
	// present and RFC-1123 formatted. Otherwise save fails with "Indexer
	// feed is not supported: Each item in the RSS feed must have a pubDate".
	if !strings.Contains(body, "<pubDate>") {
		t.Errorf("probe item must include pubDate (Prowlarr requirement): %s",
			body[:min(400, len(body))])
	}
	// These fields were individually required by Prowlarr's Torznab validator
	// before it would accept the indexer save. Regression guards.
	for _, need := range []string{"<enclosure", "<torznab:attr", `name="seeders"`, `name="category"`} {
		if !strings.Contains(body, need) {
			t.Errorf("probe item missing required field %q (Prowlarr would reject): %s",
				need, body[:min(600, len(body))])
		}
	}
	// Size must be > 0 — Prowlarr rejects items with size=0.
	if strings.Contains(body, "<size>0</size>") {
		t.Errorf("probe item size must be > 0 (Prowlarr rejects zero-size items)")
	}
}

// TestHandler_BareSearchTypesAllProbe — every search-family t= value
// should return the probe response (not 400) when q is missing. This
// matches Prowlarr's discovery flow across search/book/audio types.
func TestHandler_BareSearchTypesAllProbe(t *testing.T) {
	h := newTestHandler("")
	for _, fn := range []string{"search", "book", "audio"} {
		t.Run(fn, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/torznab/api?t="+fn, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("t=%s empty query: expected 200, got %d", fn, w.Code)
			}
			if !strings.Contains(w.Body.String(), "<item>") {
				t.Errorf("t=%s empty query should include ≥1 item for Prowlarr probe", fn)
			}
		})
	}
}

// TestHandler_AliasMatchesCanonical — /api is an alias for /torznab/api
// (what Prowlarr uses during indexer discovery). ServeHTTPAlias must
// return identical output for the same query.
func TestHandler_AliasMatchesCanonical(t *testing.T) {
	h := newTestHandler("")

	canonical := httptest.NewRecorder()
	h.ServeHTTP(canonical, httptest.NewRequest("GET", "/torznab/api?t=caps", nil))

	alias := httptest.NewRecorder()
	h.ServeHTTPAlias(alias, httptest.NewRequest("GET", "/api?t=caps", nil))

	if canonical.Code != alias.Code {
		t.Errorf("status differs: canonical=%d alias=%d", canonical.Code, alias.Code)
	}
	if canonical.Body.String() != alias.Body.String() {
		t.Errorf("body differs between canonical and alias")
	}
}

// TestHandler_AliasAuthenticates — the alias must enforce the same API-key
// check as the canonical route; otherwise mounting /api bypasses auth.
func TestHandler_AliasAuthenticates(t *testing.T) {
	h := newTestHandler("secret-key")

	// No key → 401
	req := httptest.NewRequest("GET", "/api?t=caps", nil)
	w := httptest.NewRecorder()
	h.ServeHTTPAlias(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with no key, got %d", w.Code)
	}

	// Correct key → 200
	req = httptest.NewRequest("GET", "/api?t=caps&apikey=secret-key", nil)
	w = httptest.NewRecorder()
	h.ServeHTTPAlias(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with valid key, got %d", w.Code)
	}
}

func TestHandler_TVSearchReturnsEmpty(t *testing.T) {
	h := newTestHandler("")

	for _, fn := range []string{"tvsearch", "movie"} {
		t.Run(fn, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/torznab/api?t="+fn, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("expected 200 for %s, got %d", fn, w.Code)
			}

			body := w.Body.String()
			if !strings.Contains(body, "No results") {
				t.Errorf("expected empty results for %s", fn)
			}
		})
	}
}

func TestHandler_BookSearchUsesParams(t *testing.T) {
	h := newTestHandler("")

	// Book search with title and author params
	req := httptest.NewRequest("GET", "/torznab/api?t=book&title=Gatsby&author=Fitzgerald", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Should return 200 (search with combined query)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
