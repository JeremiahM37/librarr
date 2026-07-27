package api

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
)

// libraryMediaTypeTestSetup creates a server with three library items
// (one ebook, one audiobook, one manga) imported into the local DB.
// ABS and Kavita are deliberately left unconfigured so the local-DB
// fallback paths are exercised — this is the exact configuration in
// which the issue #49 reproducer ran.
func libraryMediaTypeTestSetup(t *testing.T) *Server {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	items := []models.LibraryItem{
		{Title: "An Ebook", Author: "E. Bookworm", FilePath: "/x/ebook.epub", FileFormat: "epub", MediaType: "ebook", Source: "manual_import"},
		{Title: "An Audiobook", Author: "A. Reader", FilePath: "/x/audio.m4b", FileFormat: "m4b", MediaType: "audiobook", Source: "manual_import"},
		{Title: "Some Manga", Author: "M. Artist", FilePath: "/x/manga.cbz", FileFormat: "cbz", MediaType: "manga", Source: "manual_import"},
	}
	for i := range items {
		if _, err := database.AddItem(&items[i]); err != nil {
			t.Fatalf("AddItem(%s): %v", items[i].Title, err)
		}
	}

	return &Server{
		cfg: &config.Config{}, // ABSURL, ABSToken, KavitaURL, etc. all empty
		db:  database,
	}
}

// decodeLibraryListBody pulls the items array out of a /api/library* response
// and returns the (title, media_type) pairs for assertion.
func decodeLibraryListBody(t *testing.T, body []byte) []struct{ Title, MediaType string } {
	t.Helper()
	var resp struct {
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, body)
	}
	out := make([]struct{ Title, MediaType string }, 0, len(resp.Items))
	for _, it := range resp.Items {
		title, _ := it["title"].(string)
		mt, _ := it["media_type"].(string)
		out = append(out, struct{ Title, MediaType string }{title, mt})
	}
	return out
}

// TestLibraryEbooks_ExcludesAudiobooksAndManga is the regression test for
// issue #49: a curl-imported library where everything ended up showing
// under the "Ebooks" tab. The UI's ebooks tab calls GET /api/library
// with no `type` filter; before the fix the handler returned every row
// in library_items regardless of media_type.
func TestLibraryEbooks_ExcludesAudiobooksAndManga(t *testing.T) {
	s := libraryMediaTypeTestSetup(t)

	req := httptest.NewRequest("GET", "/api/library", nil)
	rr := httptest.NewRecorder()
	s.handleLibrary(rr, req)

	if rr.Code != 200 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	items := decodeLibraryListBody(t, rr.Body.Bytes())
	if len(items) != 1 {
		t.Fatalf("expected exactly 1 item under default /api/library (ebooks only), got %d: %+v", len(items), items)
	}
	if items[0].MediaType != "ebook" {
		t.Errorf("expected ebook, got media_type=%q (title=%q)", items[0].MediaType, items[0].Title)
	}
}

// TestLibraryAudiobooks_FallsBackToLocalDB covers the second half of issue #49:
// audiobooks imported into the local DB were invisible because the audiobooks
// endpoint short-circuited to ABS and returned an empty list when ABS was
// unconfigured. With the fix it falls back to library_items filtered by
// media_type="audiobook".
func TestLibraryAudiobooks_FallsBackToLocalDB(t *testing.T) {
	s := libraryMediaTypeTestSetup(t)

	req := httptest.NewRequest("GET", "/api/library/audiobooks", nil)
	rr := httptest.NewRecorder()
	s.handleLibraryAudiobooks(rr, req)

	if rr.Code != 200 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	items := decodeLibraryListBody(t, rr.Body.Bytes())
	if len(items) != 1 {
		t.Fatalf("expected 1 audiobook from local DB, got %d: %+v", len(items), items)
	}
	if items[0].MediaType != "audiobook" {
		t.Errorf("expected audiobook, got media_type=%q (title=%q)", items[0].MediaType, items[0].Title)
	}
}

// TestLibraryManga_FallsBackToLocalDB — mirror of the audiobooks case for
// the manga endpoint, which short-circuited to Kavita.
func TestLibraryManga_FallsBackToLocalDB(t *testing.T) {
	s := libraryMediaTypeTestSetup(t)

	req := httptest.NewRequest("GET", "/api/library/manga", nil)
	rr := httptest.NewRecorder()
	s.handleLibraryManga(rr, req)

	if rr.Code != 200 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	items := decodeLibraryListBody(t, rr.Body.Bytes())
	if len(items) != 1 {
		t.Fatalf("expected 1 manga from local DB, got %d: %+v", len(items), items)
	}
	if items[0].MediaType != "manga" {
		t.Errorf("expected manga, got media_type=%q (title=%q)", items[0].MediaType, items[0].Title)
	}
}

// TestLibraryEbooks_ExplicitTypeOverrideStillWorks confirms the existing
// `?type=` query param keeps working — important because the OPDS feed and
// the wishlist/scheduler use it explicitly to ask for a specific media type.
func TestLibraryEbooks_ExplicitTypeOverrideStillWorks(t *testing.T) {
	s := libraryMediaTypeTestSetup(t)

	req := httptest.NewRequest("GET", "/api/library?type=audiobook", nil)
	rr := httptest.NewRecorder()
	s.handleLibrary(rr, req)

	if rr.Code != 200 {
		t.Fatalf("HTTP %d: %s", rr.Code, rr.Body.String())
	}
	items := decodeLibraryListBody(t, rr.Body.Bytes())
	if len(items) != 1 || items[0].MediaType != "audiobook" {
		t.Errorf("expected single audiobook for ?type=audiobook, got: %+v", items)
	}
}
