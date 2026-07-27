package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jamie75/librarr/internal/db"
)

func (s *Server) handleGetBlocklist(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	entries, err := s.db.GetBlocklist(limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to get blocklist",
		})
		return
	}
	if entries == nil {
		entries = []db.BlocklistEntry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) handleAddBlocklistEntry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title       string `json:"title"`
		Source      string `json:"source"`
		DownloadURL string `json:"download_url"`
		InfoHash    string `json:"info_hash"`
		Reason      string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	if req.DownloadURL == "" && req.InfoHash == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "download_url or info_hash is required",
		})
		return
	}

	id, err := s.db.AddBlocklistEntry(req.Title, req.Source, req.DownloadURL, req.InfoHash, req.Reason)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to add blocklist entry",
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"id":      id,
	})
}

func (s *Server) handleDeleteBlocklistEntry(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}
	if err := s.db.DeleteBlocklistEntry(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (s *Server) handleClearBlocklist(w http.ResponseWriter, _ *http.Request) {
	if err := s.db.ClearBlocklist(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to clear blocklist",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}
