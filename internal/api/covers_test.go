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
	s.cfg.CoverLookupEnabled = true
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
		MinInterval:   time.Nanosecond,
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

func decodeCover(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	var resp struct {
		CoverURL *string `json:"cover_url"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v\n%s", err, rr.Body.String())
	}
	if resp.CoverURL == nil {
		t.Fatalf("cover_url missing: %s", rr.Body.String())
	}
	return *resp.CoverURL
}

func TestCoverLookupReturnsMatches(t *testing.T) {
	s, _ := coverTestServer(t, 3, olStub)
	cases := []struct {
		body string
		hit  bool
	}{
		{`{"title":"Dune","author":"Frank Herbert","media_type":"ebook"}`, true},
		{`{"title":"Nothing Here","author":"Nobody","media_type":"audiobook"}`, false},
		{`{"title":"Broken","author":"Nobody","media_type":"ebook"}`, false},
	}
	for _, c := range cases {
		rr := postLookup(s, context.Background(), c.body)
		if rr.Code != 200 {
			t.Fatalf("%s: HTTP %d: %s", c.body, rr.Code, rr.Body.String())
		}
		got := decodeCover(t, rr)
		if c.hit != strings.HasPrefix(got, "https://") || (!c.hit && got != "") {
			t.Errorf("%s: cover_url = %q", c.body, got)
		}
	}
}

func TestCoverLookupSkipsManga(t *testing.T) {
	s, hits := coverTestServer(t, 3, olStub)
	rr := postLookup(s, context.Background(), `{"title":"Dune","author":"Frank Herbert","media_type":"manga"}`)
	if rr.Code != 200 || decodeCover(t, rr) != "" {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("stub requests = %d, want 0", n)
	}
}

func TestCoverLookupDisabled(t *testing.T) {
	s, hits := coverTestServer(t, 3, olStub)
	s.cfg.CoverLookupEnabled = false
	rr := postLookup(s, context.Background(), `{"title":"Dune","author":"Frank Herbert","media_type":"ebook"}`)
	if rr.Code != 200 || decodeCover(t, rr) != "" {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("stub requests = %d, want 0", n)
	}
}

func TestCoverLookupValidation(t *testing.T) {
	s, _ := coverTestServer(t, 3, olStub)
	long := strings.Repeat("x", 301)
	bad := map[string]string{
		"long title":  `{"title":"` + long + `","media_type":"ebook"}`,
		"long author": `{"title":"T","author":"` + long + `","media_type":"ebook"}`,
		"too large":   `{"title":"T","pad":"` + strings.Repeat("p", 5000) + `"}`,
		"malformed":   `{"title":`,
	}
	for name, body := range bad {
		if rr := postLookup(s, context.Background(), body); rr.Code != 400 {
			t.Errorf("%s: HTTP %d, want 400", name, rr.Code)
		}
	}
	rr := postLookup(s, context.Background(), `{"title":"","media_type":"ebook"}`)
	if rr.Code != 200 || decodeCover(t, rr) != "" {
		t.Errorf("empty title: HTTP %d: %s", rr.Code, rr.Body.String())
	}
}

// A client that gives up while its lookup is queued behind another must not
// send a request of its own.
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

	go postLookup(s, context.Background(), `{"title":"Holding","author":"A","media_type":"ebook"}`)
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no lookup reached the stub")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- postLookup(s, ctx, `{"title":"Queued","author":"A","media_type":"ebook"}`) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case rr := <-done:
		if rr.Code != 200 || decodeCover(t, rr) != "" {
			t.Errorf("HTTP %d: %s", rr.Code, rr.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after disconnect")
	}
	time.Sleep(100 * time.Millisecond)
	if n := hits.Load(); n != 1 {
		t.Errorf("stub requests = %d, want 1 (the queued lookup should not run)", n)
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
	if code := do("POST", "/api/covers/lookup", `{"title":""}`, false); code != 401 {
		t.Errorf("without key = %d, want 401", code)
	}
	if code := do("POST", "/api/covers/lookup", `{"title":""}`, true); code != 200 {
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
