package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// handleSchedulerStatus returns the scheduler's current state.
func (s *Server) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"status":  map[string]interface{}{"enabled": false},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"status":  s.scheduler.Status(),
	})
}

// handleSchedulerRun triggers a scheduler pass. By default it returns at once
// and the pass runs in the background; with ?wait=1 it blocks until the pass
// finishes and returns its statistics.
func (s *Server) handleSchedulerRun(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Scheduler not initialized",
		})
		return
	}
	if r.URL.Query().Get("wait") == "1" {
		stats := s.scheduler.RunCtx(r.Context())
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Scheduler run complete",
			"stats":   stats,
			"status":  s.scheduler.Status(),
		})
		return
	}
	go s.scheduler.Run()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Scheduler run triggered",
	})
}

// handleSchedulerConfig updates scheduler configuration. Values are applied
// at once and persisted to settings.json so they survive a restart.
func (s *Server) handleSchedulerConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled          *bool `json:"enabled"`
		IntervalHours    *int  `json:"interval_hours"`
		AutoDownload     *bool `json:"auto_download"`
		MinScore         *int  `json:"min_score"`
		ItemDelaySeconds *int  `json:"item_delay_seconds"`
		AutoUpgrade      *bool `json:"auto_upgrade"`
		KeepOldFiles     *bool `json:"keep_old_files"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON",
		})
		return
	}
	if req.IntervalHours != nil && *req.IntervalHours < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "interval_hours must be at least 1"})
		return
	}
	if req.MinScore != nil && (*req.MinScore < 0 || *req.MinScore > 100) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "min_score must be between 0 and 100"})
		return
	}
	if req.ItemDelaySeconds != nil && (*req.ItemDelaySeconds < 0 || *req.ItemDelaySeconds > 3600) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "item_delay_seconds must be between 0 and 3600"})
		return
	}

	persist := map[string]interface{}{}
	if req.Enabled != nil {
		s.cfg.SchedulerEnabled = *req.Enabled
		persist["scheduler_enabled"] = *req.Enabled
	}
	if req.IntervalHours != nil {
		s.cfg.SchedulerIntervalHours = *req.IntervalHours
		persist["scheduler_interval_hours"] = *req.IntervalHours
	}
	if req.AutoDownload != nil {
		s.cfg.SchedulerAutoDownload = *req.AutoDownload
		persist["scheduler_auto_download"] = *req.AutoDownload
	}
	if req.MinScore != nil {
		s.cfg.SchedulerMinScore = *req.MinScore
		persist["scheduler_min_score"] = *req.MinScore
	}
	if req.ItemDelaySeconds != nil {
		s.cfg.SchedulerItemDelaySeconds = *req.ItemDelaySeconds
		persist["scheduler_item_delay_seconds"] = *req.ItemDelaySeconds
	}
	if req.AutoUpgrade != nil {
		s.cfg.AutoUpgradeEnabled = *req.AutoUpgrade
		persist["auto_upgrade_enabled"] = *req.AutoUpgrade
	}
	if req.KeepOldFiles != nil {
		s.cfg.UpgradeKeepOldFiles = *req.KeepOldFiles
		persist["upgrade_keep_old_files"] = *req.KeepOldFiles
	}
	if len(persist) > 0 {
		if err := s.persistSettings(persist); err != nil {
			slog.Warn("scheduler config applied but not persisted", "error", err)
		}
		username, _ := r.Context().Value(ctxUsername).(string)
		s.db.LogActivity(username, "settings_changed", "scheduler", "Scheduler settings updated")
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"config": map[string]interface{}{
			"enabled":            s.cfg.SchedulerEnabled,
			"interval_hours":     s.cfg.SchedulerIntervalHours,
			"auto_download":      s.cfg.SchedulerAutoDownload,
			"min_score":          s.cfg.SchedulerMinScore,
			"item_delay_seconds": s.cfg.SchedulerItemDelaySeconds,
			"auto_upgrade":       s.cfg.AutoUpgradeEnabled,
			"keep_old_files":     s.cfg.UpgradeKeepOldFiles,
		},
	})
}

// handleListSeries returns detected series with completion status.
func (s *Server) handleListSeries(w http.ResponseWriter, r *http.Request) {
	if s.seriesDetector == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"series":  []interface{}{},
		})
		return
	}

	series, err := s.seriesDetector.DetectSeries()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to detect series",
		})
		return
	}
	if series == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"series":  []interface{}{},
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"series":  series,
	})
}

// handleSeriesMissing returns missing books for a specific series.
func (s *Server) handleSeriesMissing(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Series name is required",
		})
		return
	}

	if s.seriesDetector == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"missing": []string{},
		})
		return
	}

	missing, err := s.seriesDetector.GetMissing(name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"series":  name,
		"missing": missing,
	})
}

// handleSearchMissingSeries searches for missing books in a series.
func (s *Server) handleSearchMissingSeries(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Series name is required",
		})
		return
	}

	if s.seriesDetector == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"results": []interface{}{},
		})
		return
	}

	results, err := s.seriesDetector.SearchMissing(name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"series":  name,
		"results": results,
	})
}
