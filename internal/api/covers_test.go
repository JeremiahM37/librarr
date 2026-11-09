package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/metadata"
)

type apiCoverCache struct {
	mu sync.Mutex
	m  map[string]string
}

func (c *apiCoverCache) GetCoverLookup(key string) (string, bool, time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.m[key]
	return u, ok, time.Now(), nil
}

func (c *apiCoverCache) PutCoverLookup(key, url string, _ time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = url
	return nil
}

// coverTestServer returns the library test server with a resolver pointed at a
// stub Open Library search, and the number of requests the stub has received.
func coverTestServer(t *testing.T, maxConcurrent int, handler http.HandlerFunc) (*Server, *atomic.Int64) {
	t.Helper()
	s := libraryMediaTypeTestSetup(t)
	var hits atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(stub.Close)
	s.coverResolver = metadata.NewCoverResolver(metadata.CoverResolverOptions{
		Cache:         &apiCoverCache{m: map[string]string{}},
		HTTPClient:    stub.Client(),
		SearchURL:     func() string { return stub.URL + "/search.json" },
		CoverBaseURL:  func() string { return "https://covers.test" },
		MaxConcurrent: maxConcurrent,
	})
	return s, &hits
}

// olStub answers by the searched title: Dune matches, Broken fails, the rest miss.
func olStub(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	switch {
	case strings.Contains(q, "Broken"):
		http.Error(w, "boom", http.StatusInternalServerError)
	case strings.Contains(q, "Dune"):
		fmt.Fprint(w, `{"docs":[{"key":"/works/OL1W","title":"Dune","author_name":["Frank Herbert"],"cover_i":42}]}`)
	default:
		fmt.Fprint(w, `{"docs":[]}`)
	}
}

func postLookup(s *Server, ctx context.Context, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/api/covers/lookup", strings.NewReader(body)).WithContext(ctx)
	rr := httptest.NewRecorder()
	s.handleCoverLookup(rr, req)
	return rr
}

func decodeCovers(t *testing.T, rr *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	var resp struct {
		Covers map[string]string `json:"covers"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v\n%s", err, rr.Body.String())
	}
	if resp.Covers == nil {
		t.Fatalf("covers missing or null: %s", rr.Body.String())
	}
	return resp.Covers
}

func TestCoverLookupReturnsMatches(t *testing.T) {
	s, _ := coverTestServer(t, 3, olStub)
	rr := postLookup(s, context.Background(), `{"items":[
		{"id":"0","title":"Dune","author":"Frank Herbert","media_type":"ebook"},
		{"id":"1","title":"Nothing Here","author":"Nobody","media_type":"audiobook"},
		{"id":"2","title":"Broken","author":"Nobody","media_type":"ebook"}]}`)
	if rr.Code != 200 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	got := decodeCovers(t, rr)
	if len(got) != 1 || !strings.HasPrefix(got["0"], "https://") {
		t.Fatalf("covers = %v, want only id 0", got)
	}
}

func TestCoverLookupSkipsManga(t *testing.T) {
	s, hits := coverTestServer(t, 3, olStub)
	rr := postLookup(s, context.Background(), `{"items":[{"id":"0","title":"Dune","author":"Frank Herbert","media_type":"manga"}]}`)
	if rr.Code != 200 || len(decodeCovers(t, rr)) != 0 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("stub requests = %d, want 0", n)
	}
}

func TestCoverLookupValidation(t *testing.T) {
	s, _ := coverTestServer(t, 3, olStub)
	long := strings.Repeat("x", 301)
	var many []string
	for i := 0; i < 21; i++ {
		many = append(many, fmt.Sprintf(`{"id":"%d","title":"T","media_type":"ebook"}`, i))
	}
	bad := map[string]string{
		"too many items": `{"items":[` + strings.Join(many, ",") + `]}`,
		"long title":     `{"items":[{"id":"0","title":"` + long + `","media_type":"ebook"}]}`,
		"long author":    `{"items":[{"id":"0","title":"T","author":"` + long + `","media_type":"ebook"}]}`,
		"long id":        `{"items":[{"id":"` + strings.Repeat("i", 65) + `","title":"T","media_type":"ebook"}]}`,
		"malformed":      `{"items":[`,
	}
	for name, body := range bad {
		if rr := postLookup(s, context.Background(), body); rr.Code != 400 {
			t.Errorf("%s: HTTP %d, want 400", name, rr.Code)
		}
	}
	rr := postLookup(s, context.Background(), `{"items":[]}`)
	if rr.Code != 200 || len(decodeCovers(t, rr)) != 0 {
		t.Errorf("empty items: HTTP %d: %s", rr.Code, rr.Body.String())
	}
}

func TestCoverLookupStopsOnDisconnect(t *testing.T) {
	release := make(chan struct{})
	arrived := make(chan struct{}, 20)
	s, hits := coverTestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		fmt.Fprint(w, `{"docs":[]}`)
	})
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	var items []string
	for i := 0; i < 5; i++ {
		items = append(items, fmt.Sprintf(`{"id":"%d","title":"Book %d","author":"A","media_type":"ebook"}`, i, i))
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- postLookup(s, ctx, `{"items":[`+strings.Join(items, ",")+`]}`) }()

	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no lookup reached the stub")
	}
	cancel()
	select {
	case rr := <-done:
		if rr.Code != 200 || len(decodeCovers(t, rr)) != 0 {
			t.Errorf("HTTP %d: %s", rr.Code, rr.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after disconnect")
	}
	time.Sleep(100 * time.Millisecond)
	if n := hits.Load(); n != 1 {
		t.Errorf("stub requests = %d, want 1 (queued lookups should not run)", n)
	}
}

func TestCoverLookupRequiresAuth(t *testing.T) {
	s, _ := coverTestServer(t, 3, olStub)
	s.cfg = &config.Config{APIKey: "k-123"}
	s.sessions = NewSessionStore()
	s.mux = http.NewServeMux()
	s.registerLibraryRoutes()
	h := s.Handler()

	do := func(method, path, body string, key bool) int {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if key {
			req.Header.Set("X-Api-Key", "k-123")
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if code := do("POST", "/api/covers/lookup", `{"items":[]}`, false); code != 401 {
		t.Errorf("without key = %d, want 401", code)
	}
	if code := do("POST", "/api/covers/lookup", `{"items":[]}`, true); code != 200 {
		t.Errorf("with key = %d, want 200", code)
	}
}

func TestOpenAPIDocumentsCoverLookup(t *testing.T) {
	rr := httptest.NewRecorder()
	(&Server{}).handleOpenAPI(rr, httptest.NewRequest("GET", "/api/openapi.json", nil))
	var spec struct {
		Paths map[string]map[string]interface{} `json:"paths"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Paths["/api/covers/lookup"]["post"] == nil {
		t.Error("missing POST /api/covers/lookup")
	}
}

func TestPruneCoverLookupsDropsOldEntries(t *testing.T) {
	s, _ := coverTestServer(t, 3, olStub)
	if err := s.db.PutCoverLookup("old", "u", time.Now().Add(-31*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutCoverLookup("new", "u", time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.pruneCoverLookups(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, found, _, _ := s.db.GetCoverLookup("old"); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old entry not pruned")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if _, found, _, _ := s.db.GetCoverLookup("new"); !found {
		t.Error("new entry pruned")
	}
}
