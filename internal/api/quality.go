package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/quality"
)

// Default format rankings, kept for older clients of /quality-profiles/default.
var (
	defaultEbookRanking     = quality.DefaultProfiles()[0].Ranking
	defaultAudiobookRanking = quality.DefaultProfiles()[1].Ranking
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

// handleGetDefaultQualityProfile reports the built-in profile per media type
// and the global upgrade switches. The ebook/audiobook keys predate
// per-media-type profiles and are kept for compatibility.
func (s *Server) handleGetDefaultQualityProfile(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]interface{}{
		"ebook": map[string]interface{}{
			"name":           "Default Ebook",
			"format_ranking": defaultEbookRanking,
		},
		"audiobook": map[string]interface{}{
			"name":           "Default Audiobook",
			"format_ranking": defaultAudiobookRanking,
		},
		"auto_upgrade_enabled":   s.cfg.AutoUpgradeEnabled,
		"upgrade_keep_old_files": s.cfg.UpgradeKeepOldFiles,
	}
	defaults := map[string]interface{}{}
	for _, mt := range []string{"ebook", "audiobook", "manga"} {
		if qp, err := s.db.GetDefaultQualityProfile(mt); err == nil {
			defaults[mt] = qp
			resp[mt] = map[string]interface{}{"name": qp.Name, "format_ranking": qp.FormatRanking, "id": qp.ID}
		}
	}
	resp["defaults"] = defaults
	writeJSON(w, http.StatusOK, resp)
}

// handleQualityFormats lists the formats the profile editor can offer.
func (s *Server) handleQualityFormats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"formats": quality.KnownFormats,
	})
}

// validateProfileRequest normalises and validates a profile payload, returning
// an HTTP-ready error message when it is unusable.
func validateProfileRequest(qp *db.QualityProfile) string {
	if qp.MediaType == "" {
		qp.MediaType = "ebook"
	}
	if _, ok := quality.KnownFormats[qp.MediaType]; !ok {
		return "media_type must be ebook, audiobook or manga"
	}
	if len(qp.Name) > 100 {
		return "Name is too long"
	}
	if len(qp.FormatRanking) > 32 {
		return "Too many formats in ranking"
	}
	if err := quality.Validate(qp.Profile()); err != nil {
		return err.Error()
	}
	return ""
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
		for _, p := range quality.DefaultProfiles() {
			if p.MediaType == qp.MediaType || (qp.MediaType == "" && p.MediaType == "ebook") {
				qp.FormatRanking = append([]string(nil), p.Ranking...)
			}
		}
	}
	qp.Builtin = false // only the seeder creates built-ins
	if msg := validateProfileRequest(&qp); msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": msg})
		return
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
	existing, err := s.db.GetQualityProfile(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "quality profile not found"})
		return
	}
	if existing.Builtin {
		qp.MediaType = existing.MediaType
	}
	if msg := validateProfileRequest(&qp); msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": msg})
		return
	}

	if err := s.db.UpdateQualityProfile(&qp); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to update quality profile",
		})
		return
	}
	saved, err := s.db.GetQualityProfile(id)
	if err != nil {
		writeJSON(w, http.StatusOK, qp)
		return
	}
	writeJSON(w, http.StatusOK, saved)
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
		status := http.StatusNotFound
		if errors.Is(err, db.ErrBuiltinProfile) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]interface{}{
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
