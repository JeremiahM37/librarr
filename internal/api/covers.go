package api

import (
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

// handleLibraryCover serves the saved cover of a library item.
func (s *Server) handleLibraryCover(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	item, err := s.db.GetItem(id)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error("cover item lookup failed", "id", id, "error", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
		return
	}
	if item.CoverFile == "" {
		http.NotFound(w, r)
		return
	}
	f, contentType, err := s.coverStore.Open(item.CoverFile)
	if err != nil {
		slog.Debug("cover file unavailable", "id", id, "error", err)
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, max-age=86400")
	io.Copy(w, f)
}
