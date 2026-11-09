package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/metadata"
)

const (
	maxCoverLookupBody  = 64 << 10
	maxCoverLookupItems = 20
	maxCoverLookupText  = 300 // bytes, for a title or author
	maxCoverLookupID    = 64  // bytes
)

type coverLookupItem struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Author    string `json:"author"`
	MediaType string `json:"media_type"`
}

// handleCoverLookup resolves cover URLs for a batch of search results. Misses,
// failures and non-book items are left out of the response.
func (s *Server) handleCoverLookup(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCoverLookupBody)
	var req struct {
		Items []coverLookupItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	if len(req.Items) > maxCoverLookupItems {
		writeError(w, http.StatusBadRequest, "Too many items (max 20)", nil)
		return
	}
	for _, it := range req.Items {
		if len(it.ID) > maxCoverLookupID || len(it.Title) > maxCoverLookupText || len(it.Author) > maxCoverLookupText {
			writeError(w, http.StatusBadRequest, "Item field too long", nil)
			return
		}
	}

	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		result = map[string]string{}
	)
	for _, it := range req.Items {
		if it.MediaType != "ebook" && it.MediaType != "audiobook" {
			continue
		}
		wg.Add(1)
		go func(it coverLookupItem) {
			defer wg.Done()
			url, err := s.coverResolver.Resolve(r.Context(), metadata.CoverKindBook, it.Title, it.Author)
			if err != nil {
				slog.Debug("cover lookup failed", "error", err)
				return
			}
			if url == "" {
				return
			}
			mu.Lock()
			result[it.ID] = url
			mu.Unlock()
		}(it)
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, map[string]interface{}{"covers": result})
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
