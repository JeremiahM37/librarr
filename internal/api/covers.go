package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/JeremiahM37/librarr/internal/metadata"
)

const (
	maxCoverLookupBody = 4 << 10
	maxCoverLookupText = 300 // bytes, for a title or author
)

type coverLookupRequest struct {
	Title     string `json:"title"`
	Author    string `json:"author"`
	MediaType string `json:"media_type"`
}

// handleCoverLookup resolves the cover URL of one search result. A miss, a
// failure or a non-book item answers with an empty cover_url.
func (s *Server) handleCoverLookup(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCoverLookupBody)
	var req coverLookupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	if len(req.Title) > maxCoverLookupText || len(req.Author) > maxCoverLookupText {
		writeError(w, http.StatusBadRequest, "Title or author too long", nil)
		return
	}

	url := ""
	if s.cfg.CoverLookupEnabled && (req.MediaType == "ebook" || req.MediaType == "audiobook") {
		found, err := s.coverResolver.Resolve(r.Context(), metadata.CoverKindBook, req.Title, req.Author)
		if err != nil {
			slog.Debug("cover lookup failed", "error", err)
		} else {
			url = found
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"cover_url": url})
}

const coverLookupMaxAge = 30 * 24 * time.Hour

// pruneCoverLookups drops month-old cached lookups once a day until ctx ends.
func (s *Server) pruneCoverLookups(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		if err := s.db.PruneCoverLookups(time.Now().Add(-coverLookupMaxAge)); err != nil {
			slog.Warn("prune cover lookups failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
