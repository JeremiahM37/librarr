package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/db"
)

func (s *Server) handleListMonitoredAuthors(w http.ResponseWriter, r *http.Request) {
	authors, err := s.db.GetMonitoredAuthors()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to list monitored authors",
		})
		return
	}
	if authors == nil {
		authors = []db.MonitoredAuthor{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"authors":  authors,
		"enabled":  s.cfg.AuthorMonitorEnabled,
		"auto_add": s.cfg.AuthorMonitorAutoAdd,
	})
}

func (s *Server) handleAddMonitoredAuthor(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		CheckIntervalDays int    `json:"check_interval_days"`
		AutoAdd           *bool  `json:"auto_add"`
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
	if len(req.Name) > 200 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Author name is too long",
		})
		return
	}
	if req.CheckIntervalDays <= 0 {
		req.CheckIntervalDays = s.cfg.AuthorCheckIntervalDays
		if req.CheckIntervalDays <= 0 {
			req.CheckIntervalDays = 7
		}
	}
	autoAdd := s.cfg.AuthorMonitorAutoAdd
	if req.AutoAdd != nil {
		autoAdd = *req.AutoAdd
	}

	id, err := s.db.AddMonitoredAuthorWithOptions(req.Name, req.CheckIntervalDays, autoAdd)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to add monitored author",
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success":  true,
		"id":       id,
		"name":     req.Name,
		"auto_add": autoAdd,
	})
}

// handleUpdateMonitoredAuthor changes an author's interval or auto-add flag.
func (s *Server) handleUpdateMonitoredAuthor(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid ID"})
		return
	}
	var req struct {
		CheckIntervalDays *int  `json:"check_interval_days"`
		AutoAdd           *bool `json:"auto_add"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid JSON"})
		return
	}
	if req.CheckIntervalDays != nil && *req.CheckIntervalDays < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "check_interval_days must be at least 1"})
		return
	}
	if err := s.db.UpdateMonitoredAuthor(id, req.CheckIntervalDays, req.AutoAdd); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	author, err := s.db.GetMonitoredAuthor(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "author": author})
}

// handleCheckMonitoredAuthor runs one author's check now and reports what it
// found. It is synchronous: the caller learns whether this was the baseline
// pass, which works were new, and how many were added to the wanted list.
func (s *Server) handleCheckMonitoredAuthor(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid ID"})
		return
	}
	if s.authorMonitor == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Author monitor not initialized"})
		return
	}
	res, err := s.authorMonitor.CheckAuthorByID(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	author, _ := s.db.GetMonitoredAuthor(id)
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "result": res, "author": author})
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
