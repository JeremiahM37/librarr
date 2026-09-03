package scheduler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/webhook"
)

func newTestDB(t *testing.T) *db.DB {
	t.Helper()
	dir := t.TempDir()
	d, err := db.New(dir + "/test.db")
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

type olDoc struct {
	Key   string   `json:"key,omitempty"`
	Title string   `json:"title"`
	Year  int      `json:"first_publish_year,omitempty"`
	Names []string `json:"author_name"`
}

// fakeOpenLibrary serves a mutable list of docs and counts requests.
type fakeOpenLibrary struct {
	srv   *httptest.Server
	docs  atomic.Value // []olDoc
	calls atomic.Int32
	code  atomic.Int32
	query atomic.Value // last raw query
}

func newFakeOpenLibrary(t *testing.T, docs []olDoc) *fakeOpenLibrary {
	t.Helper()
	f := &fakeOpenLibrary{}
	f.docs.Store(docs)
	f.code.Store(http.StatusOK)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		f.query.Store(r.URL.RawQuery)
		if r.URL.Path != "/search.json" {
			http.NotFound(w, r)
			return
		}
		if c := int(f.code.Load()); c != http.StatusOK {
			w.WriteHeader(c)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"docs": f.docs.Load()})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func newTestMonitor(t *testing.T, d *db.DB, ol *fakeOpenLibrary, enabled bool) *AuthorMonitor {
	t.Helper()
	cfg := &config.Config{AuthorMonitorEnabled: enabled}
	am := NewAuthorMonitor(cfg, d, webhook.NewSender())
	am.SetOpenLibraryURL(ol.srv.URL + "/")
	return am
}

func TestNewAuthorMonitor(t *testing.T) {
	cfg := &config.Config{AuthorMonitorEnabled: true}
	database := newTestDB(t)
	ws := webhook.NewSender()
	am := NewAuthorMonitor(cfg, database, ws)
	if am == nil || am.cfg != cfg || am.db != database || am.webhookSender != ws || am.client == nil {
		t.Fatal("monitor not wired")
	}
	if am.baseURL != DefaultOpenLibraryURL {
		t.Fatalf("default base URL = %q", am.baseURL)
	}
	if am.client.Timeout != 15*time.Second {
		t.Errorf("expected 15s timeout, got %v", am.client.Timeout)
	}
	am.SetOpenLibraryURL("")
	if am.baseURL != DefaultOpenLibraryURL {
		t.Fatal("empty URL must not clear the base URL")
	}
}

func TestCheckAuthors_DisabledDoesNothing(t *testing.T) {
	database := newTestDB(t)
	ol := newFakeOpenLibrary(t, []olDoc{{Key: "/works/OL1W", Title: "X", Names: []string{"Test Author"}}})
	database.AddMonitoredAuthor("Test Author", 1)
	am := newTestMonitor(t, database, ol, false)
	am.CheckAuthors()
	if ol.calls.Load() != 0 {
		t.Fatal("disabled monitor must not call Open Library")
	}
}

func TestSearchOpenLibrary_ParsesAndFilters(t *testing.T) {
	database := newTestDB(t)
	ol := newFakeOpenLibrary(t, []olDoc{
		{Key: "/works/OL1W", Title: "New Book", Year: 2026, Names: []string{"Brandon Sanderson"}},
		{Key: "/works/OL2W", Title: "Old Book", Year: 2020, Names: []string{"Brandon Sanderson"}},
		{Key: "/works/OL3W", Title: "Someone Else's", Year: 2025, Names: []string{"Other Person"}},
		{Key: "/works/OL1W", Title: "New Book (reissue)", Year: 2026, Names: []string{"Brandon Sanderson"}}, // dup key
		{Key: "", Title: "Keyless", Year: 2024, Names: []string{"Brandon Sanderson"}},
		{Key: "/works/OL9W", Title: "", Names: []string{"Brandon Sanderson"}}, // untitled
	})
	am := newTestMonitor(t, database, ol, true)

	works, err := am.searchOpenLibrary("Brandon Sanderson")
	if err != nil {
		t.Fatal(err)
	}
	if len(works) != 3 {
		t.Fatalf("expected 3 works (2 keyed + 1 keyless), got %d: %+v", len(works), works)
	}
	if works[0].Key != "/works/OL1W" || works[1].Key != "/works/OL2W" || works[2].Key != "title:keyless" {
		t.Fatalf("keys: %+v", works)
	}
	q, _ := ol.query.Load().(string)
	for _, want := range []string{"author=Brandon+Sanderson", "fields=", "sort=new"} {
		if !contains(q, want) {
			t.Errorf("query %q missing %q", q, want)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestSearchOpenLibrary_ErrorResponse(t *testing.T) {
	database := newTestDB(t)
	ol := newFakeOpenLibrary(t, nil)
	ol.code.Store(http.StatusInternalServerError)
	am := newTestMonitor(t, database, ol, true)
	if _, err := am.searchOpenLibrary("X"); err == nil {
		t.Fatal("expected error on 500")
	}
}

// TestCheckAuthor_BaselineThenActs is the monitor's contract: the first check
// records the catalogue silently; a later check adds only the genuinely new
// work to the wanted list, and a reissue (same work key) is ignored.
func TestCheckAuthor_BaselineThenActs(t *testing.T) {
	database := newTestDB(t)
	docs := []olDoc{
		{Key: "/works/OL1W", Title: "Ancillary Justice", Year: 2013, Names: []string{"Ann Leckie"}},
		{Key: "/works/OL2W", Title: "Ancillary Sword", Year: 2014, Names: []string{"Ann Leckie"}},
	}
	ol := newFakeOpenLibrary(t, docs)
	am := newTestMonitor(t, database, ol, true)
	id, _ := database.AddMonitoredAuthorWithOptions("Ann Leckie", 7, true)
	author, _ := database.GetMonitoredAuthor(id)

	// 1. Baseline.
	res := am.CheckAuthor(*author)
	if !res.Baseline || res.Seen != 2 || len(res.New) != 0 || res.Added != 0 || res.Error != "" {
		t.Fatalf("baseline result: %+v", res)
	}
	if items, _ := database.GetWishlist(); len(items) != 0 {
		t.Fatalf("baseline must not add wanted items, got %+v", items)
	}
	a, _ := database.GetMonitoredAuthor(id)
	if a.SeenWorks != 2 || a.LastBookFound != "Ancillary Sword" || a.LastChecked.IsZero() {
		t.Fatalf("after baseline: %+v", a)
	}

	// 2. Nothing new: no action.
	res = am.CheckAuthor(*a)
	if res.Baseline || len(res.New) != 0 || res.Added != 0 {
		t.Fatalf("unchanged catalogue produced %+v", res)
	}

	// 3. A reissue of an old work plus two genuinely new works, one of them
	// keyless and one that already sits on the wanted list by hand.
	_, _ = database.AddWishlistItem("Translation State", "Ann Leckie", "ebook")
	ol.docs.Store(append(docs,
		olDoc{Key: "/works/OL1W", Title: "Ancillary Justice (10th anniversary)", Year: 2023, Names: []string{"Ann Leckie"}},
		olDoc{Key: "/works/OL3W", Title: "Translation State", Year: 2023, Names: []string{"Ann Leckie"}},
		olDoc{Key: "/works/OL4W", Title: "Provenance", Year: 2017, Names: []string{"Ann Leckie"}},
	))
	res = am.CheckAuthor(*a)
	if res.Baseline || res.Error != "" {
		t.Fatalf("unexpected: %+v", res)
	}
	if len(res.New) != 2 || res.New[0] != "Translation State" || res.New[1] != "Provenance" {
		t.Fatalf("new works (newest first) = %v", res.New)
	}
	if res.Added != 1 {
		t.Fatalf("only Provenance should be added (Translation State already wanted), got %d", res.Added)
	}
	items, _ := database.GetWishlist()
	if len(items) != 2 {
		t.Fatalf("wanted list should hold 2 rows, got %+v", items)
	}
	var prov bool
	for _, it := range items {
		if it.Title == "Provenance" {
			prov = true
			if it.Author != "Ann Leckie" || !it.Monitored || it.Source != "author:"+itoa(id) || it.MediaType != "ebook" {
				t.Fatalf("auto-added row malformed: %+v", it)
			}
		}
	}
	if !prov {
		t.Fatal("Provenance not added")
	}

	// 4. Running again announces nothing: the new keys are now seen.
	res = am.CheckAuthor(*a)
	if len(res.New) != 0 || res.Added != 0 {
		t.Fatalf("second pass re-announced: %+v", res)
	}
	a, _ = database.GetMonitoredAuthor(id)
	if a.SeenWorks != 4 {
		t.Fatalf("seen works = %d, want 4", a.SeenWorks)
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestCheckAuthor_NotifyOnlyWhenAutoAddOff(t *testing.T) {
	database := newTestDB(t)
	ol := newFakeOpenLibrary(t, []olDoc{{Key: "/works/OL1W", Title: "First", Year: 2020, Names: []string{"Quiet Author"}}})
	am := newTestMonitor(t, database, ol, true)
	// An admin user receives in-app notifications.
	adminID, err := database.CreateUser("admin", "pw-hash", "admin")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := database.AddMonitoredAuthorWithOptions("Quiet Author", 7, false)
	author, _ := database.GetMonitoredAuthor(id)
	am.CheckAuthor(*author) // baseline

	ol.docs.Store([]olDoc{
		{Key: "/works/OL1W", Title: "First", Year: 2020, Names: []string{"Quiet Author"}},
		{Key: "/works/OL2W", Title: "Second", Year: 2026, Names: []string{"Quiet Author"}},
	})
	res := am.CheckAuthor(*author)
	if len(res.New) != 1 || res.Added != 0 {
		t.Fatalf("notify-only author: %+v", res)
	}
	if items, _ := database.GetWishlist(); len(items) != 0 {
		t.Fatal("auto_add=false must not add wanted rows")
	}
	notes, _ := database.GetNotifications(adminID, 10, 0)
	if len(notes) != 1 || notes[0].Type != "author_new_book" {
		t.Fatalf("expected one admin notification, got %+v", notes)
	}
}

func TestCheckAuthors_RespectsInterval(t *testing.T) {
	database := newTestDB(t)
	ol := newFakeOpenLibrary(t, []olDoc{{Key: "/works/OL1W", Title: "X", Names: []string{"A B"}}})
	am := newTestMonitor(t, database, ol, true)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	am.now = func() time.Time { return base }
	id, _ := database.AddMonitoredAuthor("A B", 7)

	am.CheckAuthors()
	if ol.calls.Load() != 1 {
		t.Fatalf("first run should check once, got %d", ol.calls.Load())
	}
	am.CheckAuthors()
	if ol.calls.Load() != 1 {
		t.Fatalf("not due yet, should not check again; got %d", ol.calls.Load())
	}
	// Advance a week (LastChecked was written with the real clock, so move
	// the fake clock relative to what was stored).
	a, _ := database.GetMonitoredAuthor(id)
	am.now = func() time.Time { return a.LastChecked.Add(8 * 24 * time.Hour) }
	am.CheckAuthors()
	if ol.calls.Load() != 2 {
		t.Fatalf("due again after the interval; got %d calls", ol.calls.Load())
	}
}

func TestCheckAuthor_ErrorKeepsState(t *testing.T) {
	database := newTestDB(t)
	ol := newFakeOpenLibrary(t, nil)
	ol.code.Store(http.StatusBadGateway)
	am := newTestMonitor(t, database, ol, true)
	id, _ := database.AddMonitoredAuthor("Err Author", 7)
	author, _ := database.GetMonitoredAuthor(id)
	res := am.CheckAuthor(*author)
	if res.Error == "" {
		t.Fatal("expected error surfaced")
	}
	a, _ := database.GetMonitoredAuthor(id)
	if a.SeenWorks != 0 || a.LastChecked.IsZero() {
		t.Fatalf("error should update last_checked but record nothing: %+v", a)
	}
	if _, err := am.CheckAuthorByID(9999); err == nil {
		t.Fatal("unknown author id should error")
	}
}
