package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const maskedValue = "--------"

// sensitiveKeys are settings keys that should be masked in GET responses.
var sensitiveKeys = map[string]bool{
	"prowlarr_api_key": true,
	"qb_pass":          true,
	"abs_token":        true,
	"kavita_pass":      true,
	"api_key":          true,
	"auth_password":    true,
	"komga_pass":       true,
	"sabnzbd_api_key":  true,
}

func (s *Server) handleGetSettings(w http.ResponseWriter, _ *http.Request) {
	settings := s.loadSettings()

	// Inject current config values as defaults.
	defaults := map[string]interface{}{
		"file_org_enabled":    s.cfg.FileOrgEnabled,
		"annas_archive_domain": s.cfg.AnnasArchiveDomain,
		"ebook_dir":           s.cfg.EbookDir,
		"audiobook_dir":       s.cfg.AudiobookDir,
		"manga_dir":           s.cfg.MangaDir,
		"incoming_dir":        s.cfg.IncomingDir,
		"rate_limit_enabled":  s.cfg.RateLimitEnabled,
		"metrics_enabled":     s.cfg.MetricsEnabled,
		"webnovel_enabled":    s.cfg.WebNovelEnabled,
		"mangadex_enabled":    s.cfg.MangaDexEnabled,
		"max_retries":         s.cfg.MaxRetries,
		"foreign_lang_filter": s.searchMgr.ForeignLangFilterEnabled(),
		"flibusta_enabled":    s.cfg.FlibustaEnabled,
		"flibusta_url":        s.cfg.FlibustaURL,
		"zlibrary_enabled":    s.cfg.ZLibraryEnabled,
		"remove_torrent_after_import": s.cfg.RemoveTorrentAfterImport,
	}

	// Merge defaults under settings (settings override).
	for k, v := range defaults {
		if _, exists := settings[k]; !exists {
			settings[k] = v
		}
	}

	// Mask sensitive values.
	for k := range sensitiveKeys {
		if v, ok := settings[k]; ok {
			if str, isStr := v.(string); isStr && str != "" {
				settings[k] = maskedValue
			}
		}
	}

	writeJSON(w, http.StatusOK, settings)
}

func (s *Server) handleSaveSettings(w http.ResponseWriter, r *http.Request) {
	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON",
		})
		return
	}

	if len(data) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "No data provided",
		})
		return
	}

	// Don't save masked values (user didn't change them).
	for k := range sensitiveKeys {
		if v, ok := data[k]; ok {
			if str, isStr := v.(string); isStr && str == maskedValue {
				delete(data, k)
			}
		}
	}

	// Load existing settings and merge.
	existing := s.loadSettings()
	for k, v := range data {
		existing[k] = v
	}

	// Write to file.
	jsonBytes, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	if err := os.WriteFile(s.cfg.SettingsFile, jsonBytes, 0600); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	username, _ := r.Context().Value(ctxUsername).(string)
	s.db.LogActivity(username, "settings_changed", "settings", "Settings updated")

	// Apply runtime-updatable settings immediately.
	if v, ok := data["foreign_lang_filter"]; ok {
		if b, ok := v.(bool); ok {
			s.searchMgr.SetForeignLangFilter(b)
			slog.Info("foreign language filter updated", "enabled", b)
		}
	}
	if v, ok := data["remove_torrent_after_import"]; ok {
		if b, ok := v.(bool); ok {
			s.cfg.RemoveTorrentAfterImport = b
			slog.Info("remove torrent after import updated", "enabled", b)
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (s *Server) loadSettings() map[string]interface{} {
	settings := make(map[string]interface{})
	data, err := os.ReadFile(s.cfg.SettingsFile)
	if err != nil {
		return settings
	}
	_ = json.Unmarshal(data, &settings)
	return settings
}

// validateTestURL checks that a URL provided for connection testing is safe
// (not targeting internal metadata services, localhost, etc.).
func validateTestURL(rawURL string) error {
	cfg := DefaultURLValidationConfig()
	return ValidateURLSafety(rawURL, cfg)
}

// handleTestProwlarr actually tests the Prowlarr API connection.
func (s *Server) handleTestProwlarr(w http.ResponseWriter, r *http.Request) {
	var data struct {
		URL    string `json:"url"`
		APIKey string `json:"api_key"`
	}
	json.NewDecoder(r.Body).Decode(&data)

	testURL := strings.TrimRight(data.URL, "/")
	apiKey := data.APIKey
	if testURL == "" {
		testURL = s.cfg.ProwlarrURL
	}
	if apiKey == "" || apiKey == maskedValue {
		apiKey = s.cfg.ProwlarrAPIKey
	}

	if testURL == "" || apiKey == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "Prowlarr URL and API key required",
		})
		return
	}

	if err := validateTestURL(testURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, _ := http.NewRequest("GET", testURL+"/api/v1/health", nil)
	req.Header.Set("X-Api-Key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "Connection failed",
		})
		return
	}
	resp.Body.Close()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": resp.StatusCode == 200,
		"status":  resp.StatusCode,
	})
}

// handleTestQBittorrent actually tests qBittorrent login.
func (s *Server) handleTestQBittorrent(w http.ResponseWriter, _ *http.Request) {
	result := s.qb.Diagnose()
	writeJSON(w, http.StatusOK, result)
}

// handleTestAudiobookshelf actually tests ABS API.
func (s *Server) handleTestAudiobookshelf(w http.ResponseWriter, _ *http.Request) {
	if !s.cfg.HasAudiobookshelf() {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "Audiobookshelf not configured",
		})
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, _ := http.NewRequest("GET", s.cfg.ABSURL+"/api/libraries", nil)
	req.Header.Set("Authorization", "Bearer "+s.cfg.ABSToken)

	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "Connection failed",
		})
		return
	}
	resp.Body.Close()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": resp.StatusCode == 200,
		"status":  resp.StatusCode,
	})
}

// handleTestKavita actually tests Kavita login.
func (s *Server) handleTestKavita(w http.ResponseWriter, _ *http.Request) {
	if !s.cfg.HasKavita() {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "Kavita not configured",
		})
		return
	}

	payload, _ := json.Marshal(map[string]string{
		"username": s.cfg.KavitaUser,
		"password": s.cfg.KavitaPass,
	})

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(
		s.cfg.KavitaURL+"/api/Account/login",
		"application/json",
		strings.NewReader(string(payload)),
	)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "Connection failed",
		})
		return
	}
	resp.Body.Close()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": resp.StatusCode == 200,
		"status":  resp.StatusCode,
	})
}

// handleTestSABnzbd tests SABnzbd API connection.
func (s *Server) handleTestSABnzbd(w http.ResponseWriter, _ *http.Request) {
	if s.sab == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": false, "error": "SABnzbd not configured",
		})
		return
	}
	result := s.sab.Diagnose()
	writeJSON(w, http.StatusOK, result)
}
