package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"time"
)

// startTime is declared in health.go

func (s *Server) handleAdminDashboard(w http.ResponseWriter, _ *http.Request) {
	// Library stats.
	ebookCount, _ := s.db.CountItems("ebook")
	audiobookCount, _ := s.db.CountItems("audiobook")
	mangaCount, _ := s.db.CountItems("manga")

	// Active downloads.
	downloads := s.downloadMgr.GetDownloads()
	activeCount := 0
	for _, d := range downloads {
		if d.Status == "downloading" || d.Status == "queued" || d.Status == "searching" {
			activeCount++
		}
	}

	// Pending requests.
	pendingRequests, _ := s.db.CountRequests(0, "pending")

	// Sources health.
	sourceMeta := s.searchMgr.SourceMeta()
	enabledCount := 0
	var sourcesHealth []map[string]interface{}
	for _, src := range sourceMeta {
		if enabled, ok := src["enabled"].(bool); ok && enabled {
			enabledCount++
		}
		status := "ok"
		if health, ok := src["health"].(map[string]interface{}); ok {
			if state, ok := health["state"].(string); ok && state == "open" {
				status = "degraded"
			}
		}
		sourcesHealth = append(sourcesHealth, map[string]interface{}{
			"name":   src["name"],
			"label":  src["label"],
			"status": status,
		})
	}

	// Recent activity.
	recentActivity, _ := s.db.GetActivityLog("", "", 10, 0)
	if recentActivity == nil {
		recentActivity = nil
	}

	// Uptime.
	uptime := time.Since(startTime)
	uptimeStr := formatUptime(uptime)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"library_stats": map[string]int{
			"ebooks":     ebookCount,
			"audiobooks": audiobookCount,
			"manga":      mangaCount,
		},
		"active_downloads": activeCount,
		"pending_requests": pendingRequests,
		"sources_enabled":  enabledCount,
		"sources_health":   sourcesHealth,
		"recent_activity":  recentActivity,
		"system": map[string]string{
			"version":    Version,
			"uptime":     uptimeStr,
			"go_version": runtime.Version(),
		},
	})
}

func (s *Server) handleAdminActivity(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	action := r.URL.Query().Get("action")
	limit := queryInt(r, "limit", 50)
	offset := queryInt(r, "offset", 0)

	entries, err := s.db.GetActivityLog(user, action, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	total, _ := s.db.GetActivityLogCount(user, action)

	if entries == nil {
		entries = nil
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"entries": entries,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func (s *Server) handleAdminBulkRetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RequestIDs []string `json:"request_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}

	if len(req.RequestIDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "No request IDs provided",
		})
		return
	}

	results := make([]map[string]interface{}, 0, len(req.RequestIDs))
	for _, id := range req.RequestIDs {
		request, err := s.db.GetRequest(id)
		if err != nil {
			results = append(results, map[string]interface{}{
				"id":      id,
				"success": false,
				"error":   "Request not found",
			})
			continue
		}
		if request.Status != "failed" {
			results = append(results, map[string]interface{}{
				"id":      id,
				"success": false,
				"error":   fmt.Sprintf("Request is %s, not failed", request.Status),
			})
			continue
		}
		if err := s.db.UpdateRequestStatus(id, "pending"); err != nil {
			results = append(results, map[string]interface{}{
				"id":      id,
				"success": false,
				"error":   err.Error(),
			})
			continue
		}

		username, _ := r.Context().Value(ctxUsername).(string)
		s.db.LogActivity(username, "request_retry", request.Title, fmt.Sprintf("Bulk retry request %s", id))

		results = append(results, map[string]interface{}{
			"id":      id,
			"success": true,
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"results": results,
	})
}

func (s *Server) handleAdminBulkCancel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RequestIDs []string `json:"request_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}

	if len(req.RequestIDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "No request IDs provided",
		})
		return
	}

	results := make([]map[string]interface{}, 0, len(req.RequestIDs))
	for _, id := range req.RequestIDs {
		request, err := s.db.GetRequest(id)
		if err != nil {
			results = append(results, map[string]interface{}{
				"id":      id,
				"success": false,
				"error":   "Request not found",
			})
			continue
		}
		if request.Status == "completed" || request.Status == "cancelled" {
			results = append(results, map[string]interface{}{
				"id":      id,
				"success": false,
				"error":   fmt.Sprintf("Request is already %s", request.Status),
			})
			continue
		}
		if err := s.db.UpdateRequestStatus(id, "cancelled"); err != nil {
			results = append(results, map[string]interface{}{
				"id":      id,
				"success": false,
				"error":   err.Error(),
			})
			continue
		}

		username, _ := r.Context().Value(ctxUsername).(string)
		s.db.LogActivity(username, "request_cancel", request.Title, fmt.Sprintf("Bulk cancel request %s", id))

		results = append(results, map[string]interface{}{
			"id":      id,
			"success": true,
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"results": results,
	})
}

func (s *Server) handleAdminHealth(w http.ResponseWriter, _ *http.Request) {
	checks := make([]map[string]interface{}, 0)

	healthClient := &http.Client{Timeout: 10 * time.Second}

	// Prowlarr.
	if s.cfg.HasProwlarr() {
		status := "ok"
		detail := ""
		req, _ := http.NewRequest("GET", s.cfg.ProwlarrURL+"/api/v1/health", nil)
		req.Header.Set("X-Api-Key", s.cfg.ProwlarrAPIKey)
		resp, err := healthClient.Do(req)
		if err != nil {
			status = "error"
			detail = "Connection failed"
		} else {
			resp.Body.Close()
			if resp.StatusCode != 200 {
				status = "error"
				detail = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
		checks = append(checks, map[string]interface{}{
			"service": "prowlarr",
			"status":  status,
			"detail":  detail,
		})
	}

	// qBittorrent.
	if s.cfg.HasQBittorrent() {
		result := s.qb.Diagnose()
		status := "ok"
		detail := ""
		if success, ok := result["success"].(bool); !ok || !success {
			status = "error"
			if e, ok := result["error"].(string); ok {
				detail = e
			}
		}
		checks = append(checks, map[string]interface{}{
			"service": "qbittorrent",
			"status":  status,
			"detail":  detail,
		})
	}

	// SABnzbd.
	if s.cfg.HasSABnzbd() && s.sab != nil {
		result := s.sab.Diagnose()
		status := "ok"
		detail := ""
		if success, ok := result["success"].(bool); !ok || !success {
			status = "error"
			if e, ok := result["error"].(string); ok {
				detail = e
			}
		}
		checks = append(checks, map[string]interface{}{
			"service": "sabnzbd",
			"status":  status,
			"detail":  detail,
		})
	}

	// Audiobookshelf.
	if s.cfg.HasAudiobookshelf() {
		status := "ok"
		detail := ""
		req, _ := http.NewRequest("GET", s.cfg.ABSURL+"/api/libraries", nil)
		req.Header.Set("Authorization", "Bearer "+s.cfg.ABSToken)
		resp, err := healthClient.Do(req)
		if err != nil {
			status = "error"
			detail = "Connection failed"
		} else {
			resp.Body.Close()
			if resp.StatusCode != 200 {
				status = "error"
				detail = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
		checks = append(checks, map[string]interface{}{
			"service": "audiobookshelf",
			"status":  status,
			"detail":  detail,
		})
	}

	// Kavita.
	if s.cfg.HasKavita() {
		status := "ok"
		detail := ""
		resp, err := healthClient.Get(s.cfg.KavitaURL + "/api/health")
		if err != nil {
			status = "error"
			detail = "Connection failed"
		} else {
			resp.Body.Close()
			if resp.StatusCode != 200 {
				status = "error"
				detail = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
		checks = append(checks, map[string]interface{}{
			"service": "kavita",
			"status":  status,
			"detail":  detail,
		})
	}

	// Calibre.
	if s.cfg.HasCalibre() && s.cfg.CalibreURL != "" {
		status := "ok"
		detail := ""
		resp, err := healthClient.Get(s.cfg.CalibreURL)
		if err != nil {
			status = "error"
			detail = "Connection failed"
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				status = "error"
				detail = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
		checks = append(checks, map[string]interface{}{
			"service": "calibre",
			"status":  status,
			"detail":  detail,
		})
	}

	allOK := true
	for _, c := range checks {
		if c["status"] != "ok" {
			allOK = false
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"healthy": allOK,
		"checks":  checks,
	})
}

func formatUptime(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
