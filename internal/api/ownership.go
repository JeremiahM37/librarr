package api

import (
	"log/slog"

	"github.com/JeremiahM37/librarr/internal/library"
	"github.com/JeremiahM37/librarr/internal/models"
)

// mediaTypeForTab maps a search tab to the media type its results import as.
// Most sources leave media_type empty on the ebook tab, so the tab is the only
// thing that says which shelf a hit belongs to.
func mediaTypeForTab(tab string) string {
	switch tab {
	case "audiobook", "manga":
		return tab
	default:
		return "ebook"
	}
}

// libraryIndex builds the ownership index for one request. A failure here must
// not fail the search: an empty index simply reports nothing as owned, which
// leaves behaviour exactly as it was before ownership detection existed.
func (s *Server) libraryIndex() *library.Index {
	if s.db == nil {
		return nil
	}
	idx, err := s.db.LibraryMatchIndex()
	if err != nil {
		slog.Warn("library ownership index unavailable", "error", err)
		return nil
	}
	return idx
}

// annotateOwnership marks every result the user already owns. Results are
// annotated rather than filtered out — a user may legitimately want another
// edition, and silently dropping hits would look like a broken search.
func annotateOwnership(idx *library.Index, results []models.SearchResult, tab string) []models.SearchResult {
	if idx == nil || idx.Len() == 0 {
		return results
	}
	tabMediaType := mediaTypeForTab(tab)
	for i := range results {
		mediaType := results[i].MediaType
		if mediaType == "" {
			mediaType = tabMediaType
		}
		match, ok := idx.Lookup(results[i].Title, results[i].Author, mediaType)
		if !ok {
			continue
		}
		results[i].InLibrary = true
		results[i].LibraryItemID = match.ID
		results[i].LibraryTitle = match.Title
	}
	return results
}
