package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

// The audiobook and manga tabs list Librarr's own records, like the ebook
// tab. Items shelved in Audiobookshelf or Kavita carry links out to them.

func (s *Server) handleLibraryAudiobooks(w http.ResponseWriter, r *http.Request) {
	s.serveLocalLibraryByMediaType(w, r, "audiobook")
}

func (s *Server) handleLibraryManga(w http.ResponseWriter, r *http.Request) {
	s.serveLocalLibraryByMediaType(w, r, "manga")
}

// --- Delete library items ---

func (s *Server) handleDeleteBook(w http.ResponseWriter, r *http.Request) {
	s.deleteLibraryItem(w, r, "ebook")
}

// deleteLibraryItem removes a Librarr library record. Kavita and
// Audiobookshelf are left alone: what they shelve is managed there.
func (s *Server) deleteLibraryItem(w http.ResponseWriter, r *http.Request, mediaType string) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "Invalid library item ID"})
		return
	}
	item, err := s.db.GetItem(id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && item.MediaType != mediaType) {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"error": "Library item not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load library item", err)
		return
	}
	if err := s.db.DeleteItem(id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	username, _ := r.Context().Value(ctxUsername).(string)
	s.db.LogActivity(username, "library_remove", idStr, fmt.Sprintf("Removed library item %s", idStr))
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (s *Server) handleDeleteAudiobook(w http.ResponseWriter, r *http.Request) {
	s.deleteLibraryItem(w, r, "audiobook")
}

func (s *Server) handleDeleteManga(w http.ResponseWriter, r *http.Request) {
	s.deleteLibraryItem(w, r, "manga")
}
