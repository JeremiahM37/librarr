package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jamie75/librarr/internal/db"
)

func (s *Server) handleGetReleaseProfiles(w http.ResponseWriter, r *http.Request) {
	profiles, err := s.db.GetReleaseProfiles()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to get release profiles",
		})
		return
	}
	if profiles == nil {
		profiles = []db.ReleaseProfile{}
	}
	writeJSON(w, http.StatusOK, profiles)
}

func (s *Server) handleCreateReleaseProfile(w http.ResponseWriter, r *http.Request) {
	var rp db.ReleaseProfile
	if err := json.NewDecoder(r.Body).Decode(&rp); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	if rp.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Name is required",
		})
		return
	}
	if rp.MustContain == nil {
		rp.MustContain = []string{}
	}
	if rp.MustNotContain == nil {
		rp.MustNotContain = []string{}
	}
	if rp.Preferred == nil {
		rp.Preferred = []db.PreferredWord{}
	}

	id, err := s.db.CreateReleaseProfile(&rp)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to create release profile",
		})
		return
	}
	rp.ID = id
	writeJSON(w, http.StatusCreated, rp)
}

func (s *Server) handleUpdateReleaseProfile(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}

	var rp db.ReleaseProfile
	if err := json.NewDecoder(r.Body).Decode(&rp); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	rp.ID = id

	if err := s.db.UpdateReleaseProfile(&rp); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to update release profile",
		})
		return
	}
	writeJSON(w, http.StatusOK, rp)
}

func (s *Server) handleDeleteReleaseProfile(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}
	if err := s.db.DeleteReleaseProfile(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}
