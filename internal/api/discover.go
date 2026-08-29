package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/JeremiahM37/librarr/internal/models"
)

// handleDiscoverSearch handles GET /api/discover/search?q= — merged
// Google Books + Open Library search for the browse/discover UI.
func (s *Server) handleDiscoverSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{"results": []models.DiscoverResult{}})
		return
	}

	limit := 24
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 && l <= 50 {
		limit = l
	}

	results := s.discover.Search(r.Context(), query, limit)
	s.annotateDiscoverResults(results)

	writeJSON(w, http.StatusOK, map[string]interface{}{"results": results})
}

// handleDiscoverDetail handles GET /api/discover/book/{source}/{id}.
func (s *Server) handleDiscoverDetail(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	id := r.PathValue("id")
	if source == "" || id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "source and id are required"})
		return
	}
	if source != "google_books" && source != "open_library" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "unknown source"})
		return
	}

	result, err := s.discover.GetDetail(r.Context(), source, id)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{"error": "Failed to fetch book detail"})
		return
	}
	if result == nil || result.Title == "" {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"error": "Not found"})
		return
	}

	results := []models.DiscoverResult{*result}
	s.annotateDiscoverResults(results)

	writeJSON(w, http.StatusOK, results[0])
}

// annotateDiscoverResults marks each result as owned (already in the
// library) and/or requested (a request already exists), in place. Mirrors
// annotateOwnership's pattern in ownership.go, but discover results don't
// carry a MediaType the way indexer SearchResults do, so ownership is
// checked against the ebook shelf by default — matching what Discover
// actually lists today (books, not audiobooks/manga).
func (s *Server) annotateDiscoverResults(results []models.DiscoverResult) {
	idx := s.libraryIndex()
	for i := range results {
		if idx != nil {
			if match, ok := idx.Lookup(results[i].Title, results[i].Author, "ebook"); ok {
				results[i].InLibrary = true
				results[i].LibraryItemID = match.ID
				results[i].LibraryTitle = match.Title
			}
		}
		if s.db == nil {
			continue
		}
		if req, ok, err := s.db.FindActiveRequestByTitle(results[i].Title); err == nil && ok {
			results[i].Requested = true
			results[i].RequestID = req.ID
			results[i].RequestStatus = req.Status
		}
	}
}
