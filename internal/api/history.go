package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/jamie75/librarr/internal/db"
)

// handleAddHistory adds a new reading history entry.
func (s *Server) handleAddHistory(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false, "error": "Authentication required",
		})
		return
	}

	var req struct {
		BookTitle     string `json:"book_title"`
		Author        string `json:"author"`
		Format        string `json:"format"`
		StartedAt     string `json:"started_at"`
		FinishedAt    string `json:"finished_at"`
		Rating        *int   `json:"rating"`
		Notes         string `json:"notes"`
		LibraryItemID *int64 `json:"library_item_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON",
		})
		return
	}

	if req.BookTitle == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "book_title is required",
		})
		return
	}

	var startedAt, finishedAt *time.Time
	if req.StartedAt != "" {
		t, err := time.Parse(time.RFC3339, req.StartedAt)
		if err != nil {
			t, err = time.Parse("2006-01-02", req.StartedAt)
		}
		if err == nil {
			startedAt = &t
		}
	}
	if startedAt == nil {
		now := time.Now()
		startedAt = &now
	}

	if req.FinishedAt != "" {
		t, err := time.Parse(time.RFC3339, req.FinishedAt)
		if err != nil {
			t, err = time.Parse("2006-01-02", req.FinishedAt)
		}
		if err == nil {
			finishedAt = &t
		}
	}

	id, err := s.db.AddReadingHistory(userID, req.BookTitle, req.Author, req.Format, startedAt, finishedAt, req.Rating, req.Notes, req.LibraryItemID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to add history entry",
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"id":      id,
	})
}

// handleGetHistory lists reading history with pagination and filter.
func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false, "error": "Authentication required",
		})
		return
	}

	limit := 50
	offset := 0
	status := r.URL.Query().Get("status") // "reading", "finished", or "" for all

	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	entries, err := s.db.GetReadingHistory(userID, status, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to get history",
		})
		return
	}

	if entries == nil {
		entries = []db.ReadingHistoryEntry{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"history": entries,
	})
}

// handleUpdateHistory updates a reading history entry.
func (s *Server) handleUpdateHistory(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false, "error": "Authentication required",
		})
		return
	}

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}

	var req struct {
		FinishedAt *string `json:"finished_at"`
		Rating     *int    `json:"rating"`
		Notes      *string `json:"notes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON",
		})
		return
	}

	var finishedAt *time.Time
	if req.FinishedAt != nil && *req.FinishedAt != "" {
		t, err := time.Parse(time.RFC3339, *req.FinishedAt)
		if err != nil {
			t, err = time.Parse("2006-01-02", *req.FinishedAt)
		}
		if err == nil {
			finishedAt = &t
		}
	}

	if err := s.db.UpdateReadingHistory(id, userID, finishedAt, req.Rating, req.Notes); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": "Entry not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// handleDeleteHistory removes a reading history entry.
func (s *Server) handleDeleteHistory(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false, "error": "Authentication required",
		})
		return
	}

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}

	if err := s.db.DeleteReadingHistory(id, userID); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": "Entry not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// handleHistoryStats returns reading statistics.
func (s *Server) handleHistoryStats(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false, "error": "Authentication required",
		})
		return
	}

	stats, err := s.db.GetReadingStats(userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to get stats",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"stats":   stats,
	})
}
