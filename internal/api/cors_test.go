package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCORS_APIKeyInQueryParamSetsOrigin — clients that fetch() from a PWA
// cannot set custom headers (X-Api-Key) without triggering a preflight the
// browser refuses to send the key on. They must pass the key as ?apikey=.
// Regression test: the CORS middleware must recognize the query-param form
// and emit Access-Control-Allow-Origin, otherwise the browser drops the
// response and the library appears empty.
func TestCORS_APIKeyInQueryParamSetsOrigin(t *testing.T) {
	s := &Server{}
	handler := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	req := httptest.NewRequest("GET", "/api/library?apikey=test-key", nil)
	req.Header.Set("Origin", "http://mobile.pwa.example")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://mobile.pwa.example" {
		t.Errorf("expected Allow-Origin to reflect request origin for apikey query param, got %q", got)
	}
}

// TestCORS_APIKeyInHeaderStillWorks — don't regress the header path.
func TestCORS_APIKeyInHeaderStillWorks(t *testing.T) {
	s := &Server{}
	handler := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	req := httptest.NewRequest("GET", "/api/library", nil)
	req.Header.Set("Origin", "http://client.example")
	req.Header.Set("X-Api-Key", "k")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://client.example" {
		t.Errorf("expected Allow-Origin for X-Api-Key header, got %q", got)
	}
}

// TestCORS_NoCredsNoOrigin — cross-origin unauthenticated requests must NOT
// receive an Allow-Origin (would break the same-origin+credentials protection).
func TestCORS_NoCredsNoOrigin(t *testing.T) {
	s := &Server{}
	handler := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	req := httptest.NewRequest("GET", "/api/library", nil)
	req.Header.Set("Origin", "http://attacker.example")
	req.Host = "librarr.local"
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("expected no Allow-Origin for unauthenticated cross-origin, got %q", got)
	}
}
