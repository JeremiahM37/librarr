package torznab

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/search"
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

func TestHandler_MissingQuery(t *testing.T) {
	h := newTestHandler("")

	req := httptest.NewRequest("GET", "/torznab/api?t=search", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
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
