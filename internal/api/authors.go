package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jamie75/librarr/internal/db"
)

func (s *Server) handleListMonitoredAuthors(w http.ResponseWriter, r *http.Request) {
	authors, err := s.db.GetMonitoredAuthors()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to get monitored authors",
		})
		return
	}
	if authors == nil {
		authors = []db.MonitoredAuthor{}
	}
	writeJSON(w, http.StatusOK, authors)
}

func (s *Server) handleAddMonitoredAuthor(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		CheckIntervalDays int    `json:"check_interval_days"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Author name is required",
		})
		return
	}
	if req.CheckIntervalDays <= 0 {
		req.CheckIntervalDays = s.cfg.AuthorCheckIntervalDays
		if req.CheckIntervalDays <= 0 {
			req.CheckIntervalDays = 7
		}
	}

	id, err := s.db.AddMonitoredAuthor(req.Name, req.CheckIntervalDays)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to add monitored author",
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"id":      id,
		"name":    req.Name,
	})
}

func (s *Server) handleDeleteMonitoredAuthor(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}
	if err := s.db.DeleteMonitoredAuthor(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}
