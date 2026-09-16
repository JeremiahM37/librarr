package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleOpenAPI confirms the embedded OpenAPI spec is well-formed JSON
// and that key fields are present. Catches regressions where a bad edit to
// openapi.json gets shipped.
func TestHandleOpenAPI(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/openapi.json", nil)
	rr := httptest.NewRecorder()
	s := &Server{}
	s.handleOpenAPI(rr, req)

	if rr.Code != 200 {
		t.Fatalf("HTTP %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var spec struct {
		OpenAPI    string                 `json:"openapi"`
		Info       map[string]interface{} `json:"info"`
		Paths      map[string]interface{} `json:"paths"`
		Components struct {
			Schemas         map[string]interface{} `json:"schemas"`
			SecuritySchemes map[string]interface{} `json:"securitySchemes"`
		} `json:"components"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &spec); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if !strings.HasPrefix(spec.OpenAPI, "3.") {
		t.Errorf("openapi field = %q, want 3.x", spec.OpenAPI)
	}
	if title, _ := spec.Info["title"].(string); !strings.Contains(title, "Librarr") {
		t.Errorf("info.title = %q, want to contain Librarr", title)
	}
	if len(spec.Paths) < 50 {
		t.Errorf("expected at least 50 documented paths, got %d", len(spec.Paths))
	}
	// Spot-check the major endpoints every AI agent will care about exist.
	for _, p := range []string{"/api/health", "/api/search", "/api/library", "/api/wishlist", "/torznab/api"} {
		if _, ok := spec.Paths[p]; !ok {
			t.Errorf("expected path %q in spec", p)
		}
	}
	if _, ok := spec.Components.SecuritySchemes["apiKey"]; !ok {
		t.Error("expected apiKey securityScheme")
	}
	for _, path := range []string{"/api/library/book/{id}", "/api/library/audiobook/{id}", "/api/library/manga/{id}"} {
		operations, ok := spec.Paths[path].(map[string]interface{})
		if !ok || operations["delete"] == nil {
			t.Errorf("missing library removal contract: %s", path)
		}
	}
}
