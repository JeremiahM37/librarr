package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/search"
)

// newOwnershipTestServer builds a server whose library already holds the book
// from issue #96, imported from Kavita exactly as the reporter described.
func newOwnershipTestServer(t *testing.T) *Server {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if _, err := database.AddItem(&models.LibraryItem{
		Title:      "4:50 from Paddington",
		Author:     "Agatha Christie",
		FilePath:   "/books/Agatha Christie/4-50 from Paddington.epub",
		FileFormat: "epub",
		MediaType:  "ebook",
		Source:     "kavita-existing",
	}); err != nil {
		t.Fatal(err)
	}

	// No download client and no download URLs anywhere in these tests: a
	// request that survives the library check must fail on the *next* guard,
	// which is what proves the check let it through without ever starting a
	// real transfer.
	cfg := &config.Config{}
	manager := download.NewManager(cfg, database, nil, nil, nil, nil, nil, search.NewHealthTracker(3, 300))
	return &Server{
		cfg:         cfg,
		db:          database,
		downloadMgr: manager,
		searchMgr:   search.NewManager(cfg, nil, search.NewHealthTracker(3, 300)),
	}
}

func postDownload(t *testing.T, server *Server, body map[string]interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/download", strings.NewReader(string(encoded)))
	rr := httptest.NewRecorder()
	server.handleDownload(rr, r)

	var response map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is not JSON: %s", rr.Body.String())
	}
	return rr, response
}

// The issue's exact repro: POST /api/download for a book already in the
// library returned 200 and downloaded a second copy.
func TestDownloadOfOwnedBookIsRejected(t *testing.T) {
	server := newOwnershipTestServer(t)

	rr, response := postDownload(t, server, map[string]interface{}{
		"title":  "Agatha Christie - 4.50 From Paddington",
		"author": "Agatha Christie",
		"source": "annas",
		"md5":    "3e8184fac9f9d2413af8260dbf240ac9",
	})

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rr.Code, rr.Body.String())
	}
	if response["success"] != false {
		t.Errorf("success = %v, want false", response["success"])
	}
	if response["in_library"] != true {
		t.Errorf("in_library = %v, want true", response["in_library"])
	}
	if response["code"] != "already_in_library" {
		t.Errorf("code = %v, want already_in_library", response["code"])
	}
	if id, ok := response["library_item_id"].(float64); !ok || id <= 0 {
		t.Errorf("library_item_id = %v, want the owned item's id", response["library_item_id"])
	}
	if response["library_title"] != "4:50 from Paddington" {
		t.Errorf("library_title = %v, want the title as stored", response["library_title"])
	}
	if response["job_id"] != nil {
		t.Errorf("a download job was started anyway: %s", rr.Body.String())
	}
}

// "Download anyway" has to work: wanting a second edition is legitimate, and
// the block would otherwise be a dead end.
func TestForceOverridesTheLibraryCheck(t *testing.T) {
	server := newOwnershipTestServer(t)

	// Carrying no download URL, the request falls through to the next guard —
	// reaching it at all is the proof that force skipped the library check.
	rr, response := postDownload(t, server, map[string]interface{}{
		"title":  "Agatha Christie - 4.50 From Paddington",
		"author": "Agatha Christie",
		"source": "annas",
		"force":  true,
	})

	if rr.Code == http.StatusConflict || response["code"] == "already_in_library" {
		t.Fatalf("force was ignored: %s", rr.Body.String())
	}
	if !strings.Contains(response["error"].(string), "No download source") {
		t.Fatalf("expected the request to reach the download guards, got: %s", rr.Body.String())
	}
}

func TestDownloadOfUnownedBookIsUnaffected(t *testing.T) {
	server := newOwnershipTestServer(t)

	rr, response := postDownload(t, server, map[string]interface{}{
		"title":  "Murder on the Orient Express",
		"author": "Agatha Christie",
		"source": "annas",
	})

	if rr.Code == http.StatusConflict {
		t.Fatalf("a different book by the same author was blocked: %s", rr.Body.String())
	}
	if response["in_library"] == true {
		t.Fatalf("unowned book reported as owned: %s", rr.Body.String())
	}
}

// Every path that can start a download must check, not just the one the web UI
// happens to use — the API is also driven by scripts and Torznab clients.
func TestEveryDownloadRouteChecksTheLibrary(t *testing.T) {
	body := map[string]interface{}{
		"title":  "Agatha Christie - 4.50 From Paddington",
		"author": "Agatha Christie",
		"source": "annas",
		"md5":    "3e8184fac9f9d2413af8260dbf240ac9",
	}

	routes := map[string]func(*Server) http.HandlerFunc{
		"/api/download":         func(s *Server) http.HandlerFunc { return s.handleDownload },
		"/api/download/annas":   func(s *Server) http.HandlerFunc { return s.handleDownloadAnnas },
		"/api/download/torrent": func(s *Server) http.HandlerFunc { return s.handleDownloadTorrent },
	}

	for route, handler := range routes {
		t.Run(route, func(t *testing.T) {
			server := newOwnershipTestServer(t)
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", route, strings.NewReader(string(encoded)))
			rr := httptest.NewRecorder()
			handler(server)(rr, r)

			if rr.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

// The audiobook route grabs a different media type, so an owned ebook must not
// block the audiobook of the same title.
func TestOwnedEbookDoesNotBlockTheAudiobook(t *testing.T) {
	server := newOwnershipTestServer(t)

	encoded, err := json.Marshal(map[string]interface{}{
		"title":     "4:50 from Paddington",
		"author":    "Agatha Christie",
		"source":    "audiobook",
		"info_hash": "0123456789abcdef0123456789abcdef01234567",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/download/audiobook", strings.NewReader(string(encoded)))
	rr := httptest.NewRecorder()
	server.handleDownloadAudiobook(rr, r)

	if rr.Code == http.StatusConflict {
		t.Fatalf("owned ebook blocked the audiobook edition: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "No torrent download client") {
		t.Fatalf("expected the request to reach the download guards, got: %s", rr.Body.String())
	}
}

func TestSearchResultsAreAnnotatedWithOwnership(t *testing.T) {
	server := newOwnershipTestServer(t)
	index := server.libraryIndex()
	if index.Len() == 0 {
		t.Fatal("ownership index is empty; the library row was not loaded")
	}

	results := annotateOwnership(index, []models.SearchResult{
		{Source: "annas", Title: "Agatha Christie - 4.50 From Paddington", Author: "Agatha Christie"},
		{Source: "annas", Title: "4.50 from Paddington (aka What Mrs. Mcgillicuddy Saw)"},
		{Source: "annas", Title: "Murder on the Orient Express", Author: "Agatha Christie"},
	}, "main")

	if !results[0].InLibrary || results[0].LibraryItemID == 0 {
		t.Errorf("author-prefixed result not flagged: %+v", results[0])
	}
	if results[0].LibraryTitle != "4:50 from Paddington" {
		t.Errorf("library_title = %q, want the title as stored", results[0].LibraryTitle)
	}
	if !results[1].InLibrary {
		t.Errorf("alternate-title result not flagged: %+v", results[1])
	}
	if results[2].InLibrary {
		t.Errorf("a different book was flagged as owned: %+v", results[2])
	}

	// The JSON contract the issue asked for: in_library is always present, so
	// a client can tell "not owned" from "server does not report ownership".
	encoded, err := json.Marshal(results[2])
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, present := decoded["in_library"]; !present {
		t.Errorf("in_library missing from an unowned result: %s", encoded)
	}
	if _, present := decoded["library_item_id"]; present {
		t.Errorf("library_item_id should be omitted when unowned: %s", encoded)
	}
}

func TestAnnotateOwnershipUsesTheSearchTabForMediaType(t *testing.T) {
	server := newOwnershipTestServer(t)
	index := server.libraryIndex()

	// The library holds the ebook. Audiobook-tab results carry no media type
	// of their own, so the tab is what keeps them off the ebook shelf.
	audiobookTab := annotateOwnership(index, []models.SearchResult{
		{Source: "audiobook", Title: "4:50 from Paddington", Author: "Agatha Christie"},
	}, "audiobook")
	if audiobookTab[0].InLibrary {
		t.Error("an owned ebook was reported as owned on the audiobook tab")
	}

	ebookTab := annotateOwnership(index, []models.SearchResult{
		{Source: "annas", Title: "4:50 from Paddington", Author: "Agatha Christie"},
	}, "main")
	if !ebookTab[0].InLibrary {
		t.Error("the owned ebook was not flagged on the ebook tab")
	}
}

func TestOwnershipIsInertWithoutALibrary(t *testing.T) {
	database, err := db.New(filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	server := &Server{cfg: &config.Config{}, db: database}

	results := annotateOwnership(server.libraryIndex(), []models.SearchResult{
		{Source: "annas", Title: "Anything At All", Author: "Someone"},
	}, "main")
	if results[0].InLibrary {
		t.Error("an empty library reported ownership")
	}

	rr := httptest.NewRecorder()
	if server.rejectIfInLibrary(rr, models.DownloadRequest{Title: "Anything At All"}) {
		t.Error("an empty library blocked a download")
	}
}
