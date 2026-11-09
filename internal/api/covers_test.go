package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/covers"
)

// coverTestServer returns the library test server with a cover store in a
// temporary directory.
func coverTestServer(t *testing.T) *Server {
	t.Helper()
	s := libraryMediaTypeTestSetup(t)
	s.coverStore = covers.NewStore(filepath.Join(t.TempDir(), "covers"))
	return s
}

func TestLibraryCoverRequiresAuth(t *testing.T) {
	s := coverTestServer(t)
	s.cfg = &config.Config{APIKey: "k-123"}
	s.sessions = NewSessionStore()
	s.mux = http.NewServeMux()
	s.registerLibraryRoutes()
	h := s.Handler()

	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10, 'J', 'F', 'I', 'F', 0}
	file, err := s.coverStore.Save(1, jpeg)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.db.SetItemCover(1, file, time.Now()); err != nil {
		t.Fatalf("SetItemCover: %v", err)
	}

	do := func(method, path, body string, key bool) int {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if key {
			req.Header.Set("X-Api-Key", "k-123")
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if code := do("GET", "/api/library/1/cover", "", false); code != 401 {
		t.Errorf("without key = %d, want 401", code)
	}
	if code := do("GET", "/api/library/1/cover", "", true); code != 200 {
		t.Errorf("with key = %d, want 200", code)
	}
}

func saveItemCover(t *testing.T, s *Server, id int64, data []byte) string {
	t.Helper()
	file, err := s.coverStore.Save(id, data)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.db.SetItemCover(id, file, time.Now()); err != nil {
		t.Fatalf("SetItemCover: %v", err)
	}
	return file
}

var testJPEG = []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10, 'J', 'F', 'I', 'F', 0, 1, 2, 3}

func getCover(s *Server, id string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", "/api/library/"+id+"/cover", nil)
	req.SetPathValue("id", id)
	rr := httptest.NewRecorder()
	s.handleLibraryCover(rr, req)
	return rr
}

func TestServeLibraryCover(t *testing.T) {
	s := coverTestServer(t)
	saveItemCover(t, s, 1, testJPEG)
	rr := getCover(s, "1")
	if rr.Code != 200 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("Content-Type = %q", ct)
	}
	if v := rr.Header().Get("X-Content-Type-Options"); v != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", v)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "private, max-age=86400" {
		t.Errorf("Cache-Control = %q", cc)
	}
	if rr.Body.String() != string(testJPEG) {
		t.Errorf("body = %v", rr.Body.Bytes())
	}
}

func TestServeLibraryCoverMissing(t *testing.T) {
	s := coverTestServer(t)
	file := saveItemCover(t, s, 1, testJPEG)
	if err := s.coverStore.Remove(file); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	for name, id := range map[string]string{"unknown": "9999", "non-numeric": "abc", "no cover": "2", "file removed": "1"} {
		if rr := getCover(s, id); rr.Code != 404 {
			t.Errorf("%s: HTTP %d, want 404", name, rr.Code)
		}
	}
}

func TestLocalLibraryIncludesCoverURL(t *testing.T) {
	s := coverTestServer(t)
	req := httptest.NewRequest("GET", "/api/library", nil)
	rr := httptest.NewRecorder()
	s.handleLibrary(rr, req)
	var resp struct {
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil || len(resp.Items) != 1 {
		t.Fatalf("decode: %v %s", err, rr.Body.String())
	}
	if _, ok := resp.Items[0]["cover_url"]; ok {
		t.Errorf("cover_url present without a cover: %v", resp.Items[0])
	}
	saveItemCover(t, s, 1, testJPEG)
	rr = httptest.NewRecorder()
	s.handleLibrary(rr, req)
	resp.Items = nil
	json.Unmarshal(rr.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0]["cover_url"] != "/api/library/1/cover" {
		t.Errorf("items = %v", resp.Items)
	}
}

func TestOpenAPIDocumentsLibraryCover(t *testing.T) {
	rr := httptest.NewRecorder()
	(&Server{}).handleOpenAPI(rr, httptest.NewRequest("GET", "/api/openapi.json", nil))
	var spec struct {
		Paths map[string]map[string]interface{} `json:"paths"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Paths["/api/library/{id}/cover"]["get"] == nil {
		t.Error("missing GET /api/library/{id}/cover")
	}
}
