package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
)

// libraryItemRouter serves the library routes through the real mux, so the
// tests also prove /api/library/{id} does not shadow the category listings.
func libraryItemRouter(t *testing.T, cfg *config.Config) (*Server, http.Handler) {
	t.Helper()
	s := libraryMediaTypeTestSetup(t)
	s.cfg = cfg
	s.sessions = NewSessionStore()
	s.mux = http.NewServeMux()
	s.registerLibraryRoutes()
	s.registerFeedRoutes()
	return s, s.Handler()
}

func serve(h http.Handler, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", path, nil))
	return rr
}

func TestLibraryItemReturnsItemWithTags(t *testing.T) {
	s, h := libraryItemRouter(t, &config.Config{})
	tagID, err := s.db.CreateTag("Favourites", "#ff0000")
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	if err := s.db.AddItemTag(1, tagID); err != nil {
		t.Fatalf("AddItemTag: %v", err)
	}

	rr := serve(h, "/api/library/1")
	if rr.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	var body struct {
		ID        int64  `json:"id"`
		Title     string `json:"title"`
		Author    string `json:"author"`
		MediaType string `json:"media_type"`
		Tags      []struct {
			Name string `json:"name"`
		} `json:"tags"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ID != 1 || body.Title != "An Ebook" || body.Author != "E. Bookworm" || body.MediaType != "ebook" {
		t.Errorf("item = %+v", body)
	}
	if len(body.Tags) != 1 || body.Tags[0].Name != "Favourites" {
		t.Errorf("tags = %+v, want [Favourites]", body.Tags)
	}
}

func TestLibraryItemUntaggedHasEmptyTags(t *testing.T) {
	_, h := libraryItemRouter(t, &config.Config{})
	rr := serve(h, "/api/library/2")
	var body map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(body["tags"]) != "[]" {
		t.Errorf("tags = %s, want []", body["tags"])
	}
}

func TestLibraryItemErrors(t *testing.T) {
	_, h := libraryItemRouter(t, &config.Config{})
	for path, want := range map[string]int{
		"/api/library/999": http.StatusNotFound,
		"/api/library/abc": http.StatusBadRequest,
	} {
		if rr := serve(h, path); rr.Code != want {
			t.Errorf("GET %s = %d, want %d", path, rr.Code, want)
		}
	}
}

func TestLibraryItemRouteKeepsCategoryListings(t *testing.T) {
	_, h := libraryItemRouter(t, &config.Config{})
	for _, path := range []string{"/api/library/audiobooks", "/api/library/manga"} {
		rr := serve(h, path)
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s = %d", path, rr.Code)
		}
		var body struct {
			Items []map[string]interface{} `json:"items"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil || body.Items == nil {
			t.Errorf("GET %s did not return a listing: %s", path, rr.Body.String())
		}
	}
}

func TestLibraryFileDownload(t *testing.T) {
	dir := t.TempDir()
	s, h := libraryItemRouter(t, &config.Config{EbookDir: dir, AudiobookDir: dir})

	book := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(book, []byte("epub bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	bookID, err := s.db.AddItem(&models.LibraryItem{Title: "On disk", FilePath: book, FileFormat: "epub", MediaType: "ebook"})
	if err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(dir, "Audiobook")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	folderID, err := s.db.AddItem(&models.LibraryItem{Title: "Folder", FilePath: folder, MediaType: "audiobook"})
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.epub")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	outsideID, err := s.db.AddItem(&models.LibraryItem{Title: "Outside", FilePath: outside, MediaType: "ebook"})
	if err != nil {
		t.Fatal(err)
	}

	id := func(n int64) string { return strconv.FormatInt(n, 10) }
	rr := serve(h, "/api/library/"+id(bookID)+"/file")
	if rr.Code != http.StatusOK || rr.Body.String() != "epub bytes" {
		t.Fatalf("file download = %d %q", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Disposition"); got != `attachment; filename="book.epub"` {
		t.Errorf("Content-Disposition = %q", got)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/epub+zip" {
		t.Errorf("Content-Type = %q", got)
	}
	// The OPDS acquisition link serves the same file.
	if rr := serve(h, "/opds/download/"+id(bookID)); rr.Code != http.StatusOK || rr.Body.String() != "epub bytes" {
		t.Errorf("OPDS download = %d %q", rr.Code, rr.Body.String())
	}
	if rr := serve(h, "/api/library/"+id(folderID)+"/file"); rr.Code != http.StatusNotFound {
		t.Errorf("directory download = %d, want 404", rr.Code)
	}
	if rr := serve(h, "/api/library/"+id(outsideID)+"/file"); rr.Code != http.StatusForbidden {
		t.Errorf("outside download = %d, want 403", rr.Code)
	}
	if rr := serve(h, "/api/library/1/file"); rr.Code != http.StatusNotFound {
		t.Errorf("missing file download = %d, want 404", rr.Code)
	}
}

// Rows listed from Audiobookshelf carry ABS ids; the ones Librarr also has a
// record of are linked to it so the UI can open the local detail view.
func TestABSListingLinksLocalRecords(t *testing.T) {
	abs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"results":[
			{"id":"li_known","media":{"metadata":{"title":"An Audiobook","authorName":"A. Reader"}}},
			{"id":"li_other","media":{"metadata":{"title":"Only on the shelf","authorName":"Someone"}}}
		],"total":2,"numPages":1}`))
	}))
	t.Cleanup(abs.Close)
	_, h := libraryItemRouter(t, &config.Config{ABSURL: abs.URL, ABSToken: "tok", ABSLibraryID: "lib"})

	rr := serve(h, "/api/library/audiobooks")
	if rr.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	var body struct {
		Items []struct {
			ID      string `json:"id"`
			LocalID int64  `json:"local_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := map[string]int64{}
	for _, it := range body.Items {
		got[it.ID] = it.LocalID
	}
	// Item 2 is the local audiobook in libraryMediaTypeTestSetup.
	if got["li_known"] != 2 || got["li_other"] != 0 || len(got) != 2 {
		t.Errorf("local ids = %v, want li_known=2, li_other unset", got)
	}
}

func TestKavitaSeriesLinkLocalRecordsByName(t *testing.T) {
	s := libraryMediaTypeTestSetup(t)
	items := s.kavitaSeriesToItems([]kavitaSeries{{ID: 7, Name: "Some Manga"}, {ID: 8, Name: "Another"}}, "http://kavita")
	if items[0]["local_id"] != int64(3) {
		t.Errorf("Some Manga local_id = %v, want 3", items[0]["local_id"])
	}
	if _, ok := items[1]["local_id"]; ok {
		t.Errorf("Another should not be linked: %v", items[1])
	}
}
