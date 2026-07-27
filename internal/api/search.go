package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jamie75/librarr/internal/models"
)

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	s.handleSearchTab(w, r, "main", "Ebook search: %s")
}

func (s *Server) handleSearchAudiobooks(w http.ResponseWriter, r *http.Request) {
	s.handleSearchTab(w, r, "audiobook", "")
}

func (s *Server) handleSearchManga(w http.ResponseWriter, r *http.Request) {
	s.handleSearchTab(w, r, "manga", "")
}

func (s *Server) handleSearchTab(w http.ResponseWriter, r *http.Request, tab, activityFormat string) {
	query := truncateSearchQuery(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"results": []interface{}{},
			"error":   "No query provided",
		})
		return
	}
	if activityFormat != "" {
		username, _ := r.Context().Value(ctxUsername).(string)
		s.db.LogActivity(username, "search", query, fmt.Sprintf(activityFormat, query))
	}
	author := truncateSearchQuery(r.URL.Query().Get("author"))
	results, elapsed := s.searchMgr.SearchWithAuthor(r.Context(), tab, query, author)
	if results == nil {
		results = []models.SearchResult{}
	}

	resp := map[string]interface{}{
		"results":        results,
		"search_time_ms": elapsed,
		"sources":        s.searchMgr.SourceMeta(),
	}

	// Fetch metadata for the query from Open Library.
	if s.metadataClient != nil {
		meta, err := s.metadataClient.FetchMetadataCtx(r.Context(), query, author)
		if err == nil && meta != nil {
			resp["metadata"] = meta
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSearchStream(w http.ResponseWriter, r *http.Request) {
	s.handleSearchStreamTab(w, r, "main", "Ebook search: %s")
}

func (s *Server) handleSearchAudiobooksStream(w http.ResponseWriter, r *http.Request) {
	s.handleSearchStreamTab(w, r, "audiobook", "")
}

func (s *Server) handleSearchMangaStream(w http.ResponseWriter, r *http.Request) {
	s.handleSearchStreamTab(w, r, "manga", "")
}

func (s *Server) handleSearchStreamTab(w http.ResponseWriter, r *http.Request, tab, activityFormat string) {
	query := truncateSearchQuery(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"results": []interface{}{},
			"error":   "No query provided",
		})
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": "streaming unsupported"})
		return
	}
	if activityFormat != "" {
		username, _ := r.Context().Value(ctxUsername).(string)
		s.db.LogActivity(username, "search", query, fmt.Sprintf(activityFormat, query))
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	author := truncateSearchQuery(r.URL.Query().Get("author"))
	var allResults []models.SearchResult
	emit := func(event string, payload interface{}) bool {
		data, err := json.Marshal(payload)
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	if !emit("started", map[string]interface{}{
		"results": []models.SearchResult{},
		"sources": s.searchMgr.SourceMeta(),
	}) {
		return
	}

	for update := range s.searchMgr.SearchStream(r.Context(), tab, query, author) {
		if len(update.Results) > 0 {
			allResults = append(allResults, update.Results...)
		}
		results := s.searchMgr.ProcessResults(allResults, query, author)
		if results == nil {
			results = []models.SearchResult{}
		}
		payload := map[string]interface{}{
			"source":         update.Source,
			"source_done":    update.Done,
			"circuit_open":   update.CircuitOpen,
			"results":        results,
			"search_time_ms": update.ElapsedMilli,
			"sources":        s.searchMgr.SourceMeta(),
		}
		if update.Err != nil {
			payload["error"] = update.Err.Error()
		}
		if !emit("results", payload) {
			return
		}
	}

	finalResults := s.searchMgr.ProcessResults(allResults, query, author)
	if finalResults == nil {
		finalResults = []models.SearchResult{}
	}
	emit("complete", map[string]interface{}{
		"results": finalResults,
		"sources": s.searchMgr.SourceMeta(),
	})
}
