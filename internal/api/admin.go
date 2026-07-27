package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"time"
)

// startTime is declared in health.go

func (s *Server) handleAdminDashboard(w http.ResponseWriter, r *http.Request) {
	// Library stats.
	ebookCount, _ := s.library().CountLegacyItems(r.Context(), "ebook")
	audiobookCount, _ := s.library().CountLegacyItems(r.Context(), "audiobook")
	mangaCount, _ := s.library().CountLegacyItems(r.Context(), "manga")

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
	limit := queryBoundedInt(r, "limit", 50, 1, 500)
	offset := queryBoundedInt(r, "offset", 0, 0, 1_000_000)

	entries, err := s.db.GetActivityLog(user, action, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to load activity log", err)
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

// httpHealthCheck probes url with optional headers and returns a health
// check entry for the named service. statusOK decides whether a response
// status code counts as healthy.
func httpHealthCheck(client *http.Client, service, url string, headers map[string]string, statusOK func(int) bool) map[string]interface{} {
	status := "ok"
	detail := ""
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		status = "error"
		detail = "Invalid URL"
	} else {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			status = "error"
			detail = "Connection failed"
		} else {
			resp.Body.Close()
			if !statusOK(resp.StatusCode) {
				status = "error"
				detail = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
	}
	return map[string]interface{}{
		"service": service,
		"status":  status,
		"detail":  detail,
	}
}

// diagnoseHealthCheck converts a download-client Diagnose() result into a
// health check entry for the named service.
func diagnoseHealthCheck(service string, result map[string]interface{}) map[string]interface{} {
	status := "ok"
	detail := ""
	if success, ok := result["success"].(bool); !ok || !success {
		status = "error"
		if e, ok := result["error"].(string); ok {
			detail = e
		}
	}
	return map[string]interface{}{
		"service": service,
		"status":  status,
		"detail":  detail,
	}
}

func (s *Server) handleAdminHealth(w http.ResponseWriter, _ *http.Request) {
	checks := make([]map[string]interface{}, 0)

	healthClient := &http.Client{Timeout: 10 * time.Second}
	is200 := func(code int) bool { return code == 200 }

	// Prowlarr.
	if s.cfg.HasProwlarr() {
		checks = append(checks, httpHealthCheck(healthClient, "prowlarr",
			s.cfg.ProwlarrURL+"/api/v1/health",
			map[string]string{"X-Api-Key": s.cfg.ProwlarrAPIKey}, is200))
	}

	// qBittorrent.
	if s.cfg.HasQBittorrent() {
		checks = append(checks, diagnoseHealthCheck("qbittorrent", s.qb.Diagnose()))
	}

	// SABnzbd.
	if s.cfg.HasSABnzbd() && s.sab != nil {
		checks = append(checks, diagnoseHealthCheck("sabnzbd", s.sab.Diagnose()))
	}

	// Audiobookshelf.
	if s.cfg.HasAudiobookshelf() {
		checks = append(checks, httpHealthCheck(healthClient, "audiobookshelf",
			s.cfg.ABSURL+"/api/libraries",
			map[string]string{"Authorization": "Bearer " + s.cfg.ABSToken}, is200))
	}

	// Kavita.
	if s.cfg.HasKavita() {
		checks = append(checks, httpHealthCheck(healthClient, "kavita",
			s.cfg.KavitaURL+"/api/health", nil, is200))
	}

	// Calibre.
	if s.cfg.HasCalibre() && s.cfg.CalibreURL != "" {
		checks = append(checks, httpHealthCheck(healthClient, "calibre",
			s.cfg.CalibreURL, nil, func(code int) bool { return code < 400 }))
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
