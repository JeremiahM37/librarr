package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jamie75/librarr/internal/db"
)

// Default format rankings.
var (
	defaultEbookRanking     = []string{"EPUB", "AZW3", "MOBI", "PDF", "CBZ", "CBR"}
	defaultAudiobookRanking = []string{"M4B", "MP3"}
)

func (s *Server) handleGetQualityProfiles(w http.ResponseWriter, r *http.Request) {
	profiles, err := s.db.GetQualityProfiles()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to get quality profiles",
		})
		return
	}
	if profiles == nil {
		profiles = []db.QualityProfile{}
	}
	writeJSON(w, http.StatusOK, profiles)
}

func (s *Server) handleGetDefaultQualityProfile(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ebook": map[string]interface{}{
			"name":           "Default Ebook",
			"format_ranking": defaultEbookRanking,
		},
		"audiobook": map[string]interface{}{
			"name":           "Default Audiobook",
			"format_ranking": defaultAudiobookRanking,
		},
		"auto_upgrade_enabled": s.cfg.AutoUpgradeEnabled,
	})
}

func (s *Server) handleCreateQualityProfile(w http.ResponseWriter, r *http.Request) {
	var qp db.QualityProfile
	if err := json.NewDecoder(r.Body).Decode(&qp); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	if qp.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Name is required",
		})
		return
	}
	if len(qp.FormatRanking) == 0 {
		qp.FormatRanking = defaultEbookRanking
	}

	id, err := s.db.CreateQualityProfile(&qp)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to create quality profile",
		})
		return
	}
	qp.ID = id
	writeJSON(w, http.StatusCreated, qp)
}

func (s *Server) handleUpdateQualityProfile(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}

	var qp db.QualityProfile
	if err := json.NewDecoder(r.Body).Decode(&qp); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON: " + err.Error(),
		})
		return
	}
	qp.ID = id

	if err := s.db.UpdateQualityProfile(&qp); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to update quality profile",
		})
		return
	}
	writeJSON(w, http.StatusOK, qp)
}

func (s *Server) handleDeleteQualityProfile(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid ID",
		})
		return
	}
	if err := s.db.DeleteQualityProfile(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// GetFormatRank returns the rank index of a format in the quality profile.
// Lower rank = better. Returns -1 if not found.
func GetFormatRank(format string, ranking []string) int {
	for i, f := range ranking {
		if equalFoldASCII(f, format) {
			return i
		}
	}
	return -1
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
