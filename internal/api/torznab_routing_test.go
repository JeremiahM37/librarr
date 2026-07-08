package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestTorznabAliasDoesNotShadowJSONAPI — mounting the torznab handler on
// the bare /api path is only safe because Go 1.22's ServeMux routes
// "GET /api" as an exact match and "GET /api/xxx" as a longer pattern.
// This test verifies that assumption by building a tiny mux that mirrors
// the real registration and asserting dispatch.
//
// Regression guard for issue #20: without this, a well-meaning future
// refactor could change "/api" to "/api/" and silently start shadowing
// every JSON endpoint with the torznab handler.
func TestTorznabAliasDoesNotShadowJSONAPI(t *testing.T) {
	mux := http.NewServeMux()

	torznabCalls := 0
	jsonCalls := 0

	mux.HandleFunc("GET /api", func(w http.ResponseWriter, _ *http.Request) {
		torznabCalls++
		w.Write([]byte("<rss></rss>"))
	})
	mux.HandleFunc("GET /api/search", func(w http.ResponseWriter, _ *http.Request) {
		jsonCalls++
		w.Write([]byte(`{"results":[]}`))
	})
	mux.HandleFunc("GET /api/library", func(w http.ResponseWriter, _ *http.Request) {
		jsonCalls++
		w.Write([]byte(`{"items":[]}`))
	})

	tests := []struct {
		name           string
		path           string
		wantTorznab    int
		wantJSON       int
		wantBodyPrefix string
	}{
		{"bare /api goes to torznab", "/api?t=caps", 1, 0, "<rss"},
		{"/api/search goes to JSON", "/api/search?q=dune", 0, 1, `{"results"`},
		{"/api/library goes to JSON", "/api/library", 0, 1, `{"items"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			torznabCalls = 0
			jsonCalls = 0
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if torznabCalls != tt.wantTorznab || jsonCalls != tt.wantJSON {
				t.Errorf("dispatch wrong: torznab=%d json=%d (want torznab=%d json=%d)",
					torznabCalls, jsonCalls, tt.wantTorznab, tt.wantJSON)
			}
			if !strings.HasPrefix(w.Body.String(), tt.wantBodyPrefix) {
				t.Errorf("body prefix: got %q", w.Body.String()[:30])
			}
		})
	}
}

// TestTorznabAliasHandlesTrailingSlashCorrectly — Go's ServeMux treats
// "GET /api" and "GET /api/" as distinct patterns; the bare-path
// registration must not swallow the trailing-slash form.
func TestTorznabAliasHandlesTrailingSlashCorrectly(t *testing.T) {
	mux := http.NewServeMux()
	torznabHit := false
	mux.HandleFunc("GET /api", func(w http.ResponseWriter, _ *http.Request) {
		torznabHit = true
	})
	// Note: we do NOT register /api/ — ServeMux will 404 a slash-terminated
	// request that has no handler, which is the safe behavior.
	req := httptest.NewRequest("GET", "/api/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if torznabHit {
		t.Error("/api/ should NOT match /api pattern — trailing slash must be handled separately")
	}
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unregistered /api/, got %d", w.Code)
	}
}

func TestStatusWriterPreservesFlusher(t *testing.T) {
	rr := httptest.NewRecorder()
	wrapped := &statusWriter{ResponseWriter: rr, status: http.StatusOK}

	flusher, ok := interface{}(wrapped).(http.Flusher)
	if !ok {
		t.Fatal("statusWriter should implement http.Flusher")
	}
	flusher.Flush()

	if !rr.Flushed {
		t.Fatal("Flush did not reach the underlying response writer")
	}
}
