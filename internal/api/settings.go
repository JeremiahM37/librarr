package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/diagnostics"
	"github.com/jamie75/librarr/internal/netutil"
	"github.com/jamie75/librarr/internal/sources"
)

const maskedValue = "--------"

// sensitiveKeys are settings keys that should be masked in GET responses.
var sensitiveKeys = map[string]bool{
	"prowlarr_api_key":         true,
	"qb_pass":                  true,
	"abs_token":                true,
	"kavita_pass":              true,
	"api_key":                  true,
	"auth_password":            true,
	"komga_pass":               true,
	"sabnzbd_api_key":          true,
	"transmission_pass":        true,
	"annas_archive_secret_key": true,
	"annas_secret_key":         true,
}

func (s *Server) handleGetSettings(w http.ResponseWriter, _ *http.Request) {
	settings := s.loadSettings()

	// Inject current config values as defaults so the UI can render fields
	// even when nothing has been saved to settings.json yet.
	defaults := map[string]interface{}{
		"file_org_enabled":            s.cfg.FileOrgEnabled,
		"annas_archive_domain":        s.cfg.AnnasArchiveDomain,
		"annas_archive_secret_key":    s.cfg.AnnasArchiveSecretKey,
		"ebook_dir":                   s.cfg.EbookDir,
		"audiobook_dir":               s.cfg.AudiobookDir,
		"manga_dir":                   s.cfg.MangaDir,
		"incoming_dir":                s.cfg.IncomingDir,
		"rate_limit_enabled":          s.cfg.RateLimitEnabled,
		"metrics_enabled":             s.cfg.MetricsEnabled,
		"webnovel_enabled":            s.cfg.WebNovelEnabled,
		"mangadex_enabled":            s.cfg.MangaDexEnabled,
		"max_retries":                 s.cfg.MaxRetries,
		"foreign_lang_filter":         s.searchMgr.ForeignLangFilterEnabled(),
		"flibusta_enabled":            s.cfg.FlibustaEnabled,
		"flibusta_url":                s.cfg.FlibustaURL,
		"zlibrary_enabled":            s.cfg.ZLibraryEnabled,
		"remove_torrent_after_import": s.cfg.RemoveTorrentAfterImport,
		"wanted_monitor_enabled":      s.cfg.WantedMonitorEnabled,
		"wanted_search_interval":      s.cfg.WantedSearchIntervalMode(),
		"wanted_retry_failures":       s.cfg.WantedRetryFailures,
		"wanted_max_results_keep":     s.cfg.WantedMaxResultsKeep,

		// Integration URLs and credentials (sensitive ones are masked below).
		"qb_url":                 s.cfg.QBUrl,
		"qb_user":                s.cfg.QBUser,
		"qb_pass":                s.cfg.QBPass,
		"qb_save_path":           s.cfg.QBSavePath,
		"qb_category":            s.cfg.QBCategory,
		"qb_audiobook_save_path": s.cfg.QBAudiobookSavePath,
		"qb_audiobook_category":  s.cfg.QBAudiobookCategory,
		"qb_manga_save_path":     s.cfg.QBMangaSavePath,
		"qb_manga_category":      s.cfg.QBMangaCategory,
		"transmission_url":       s.cfg.TransmissionURL,
		"transmission_user":      s.cfg.TransmissionUser,
		"transmission_pass":      s.cfg.TransmissionPass,
		"torrent_client":         s.cfg.TorrentClient,
		"prowlarr_url":           s.cfg.ProwlarrURL,
		"prowlarr_api_key":       s.cfg.ProwlarrAPIKey,
		"sabnzbd_url":            s.cfg.SABnzbdURL,
		"sabnzbd_api_key":        s.cfg.SABnzbdAPIKey,
		"sabnzbd_category":       s.cfg.SABnzbdCategory,
		"abs_url":                s.cfg.ABSURL,
		"abs_token":              s.cfg.ABSToken,
		"kavita_url":             s.cfg.KavitaURL,
		"kavita_user":            s.cfg.KavitaUser,
		"kavita_pass":            s.cfg.KavitaPass,
		"komga_url":              s.cfg.KomgaURL,
		"komga_user":             s.cfg.KomgaUser,
		"komga_pass":             s.cfg.KomgaPass,
		"calibre_url":            s.cfg.CalibreURL,
		"calibre_library_path":   s.cfg.CalibreLibraryPath,
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
	if value, ok := data["annas_archive_domain"].(string); ok && value != "" {
		data["annas_archive_domain"] = sources.NormalizeDomain(value)
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
		// Clearing a string field deletes the override, so the env value (or
		// default) reapplies on next startup. Without this, settings.json
		// would hold "" and the UI would show "" while the runtime kept
		// using the env value — those two views would disagree.
		if str, isStr := v.(string); isStr && str == "" {
			delete(existing, k)
			continue
		}
		existing[k] = v
	}

	// Write to file. Server-side errors get logged with full context; the
	// HTTP response stays generic so we don't leak the on-disk file path or
	// underlying filesystem error to the browser.
	jsonBytes, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		slog.Error("settings marshal failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to save settings",
		})
		return
	}

	if err := os.WriteFile(s.cfg.SettingsFile, jsonBytes, 0600); err != nil {
		slog.Error("settings write failed", "path", s.cfg.SettingsFile, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to save settings",
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
	if v, ok := data["wanted_monitor_enabled"]; ok {
		if b, ok := v.(bool); ok {
			s.cfg.WantedMonitorEnabled = b
		}
	}
	if v, ok := data["wanted_retry_failures"]; ok {
		if b, ok := v.(bool); ok {
			s.cfg.WantedRetryFailures = b
		}
	}
	if v, ok := data["wanted_search_interval"].(string); ok && strings.TrimSpace(v) != "" {
		s.cfg.WantedSearchInterval = strings.TrimSpace(v)
	}
	if v, ok := data["wanted_max_results_keep"]; ok {
		switch n := v.(type) {
		case float64:
			if int(n) > 0 {
				s.cfg.WantedMaxResultsKeep = int(n)
			}
		case int:
			if n > 0 {
				s.cfg.WantedMaxResultsKeep = n
			}
		}
	}
	if v, ok := data["annas_archive_domain"].(string); ok && v != "" {
		s.cfg.AnnasArchiveDomain = sources.NormalizeDomain(v)
	}
	if v, ok := data["annas_archive_secret_key"].(string); ok && v != "" {
		s.cfg.AnnasArchiveSecretKey = v
		slog.Info("annas archive secret key updated")
	}
	s.applyStringSetting(data, "qb_save_path", &s.cfg.QBSavePath)
	s.applyStringSetting(data, "qb_category", &s.cfg.QBCategory)
	s.applyStringSetting(data, "qb_audiobook_save_path", &s.cfg.QBAudiobookSavePath)
	s.applyStringSetting(data, "qb_audiobook_category", &s.cfg.QBAudiobookCategory)
	s.applyStringSetting(data, "qb_manga_save_path", &s.cfg.QBMangaSavePath)
	s.applyStringSetting(data, "qb_manga_category", &s.cfg.QBMangaCategory)

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (s *Server) applyStringSetting(data map[string]interface{}, key string, target *string) {
	if target == nil {
		return
	}
	if value, ok := data[key].(string); ok && strings.TrimSpace(value) != "" {
		*target = value
	}
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

// validateTestURL checks integration test URLs (admin-only). Homelab services
// on LAN IPs and localhost are allowed; cloud metadata endpoints are not.
func validateTestURL(rawURL string) error {
	return netutil.ValidateIntegrationURL(rawURL)
}

// handleTestProwlarr runs staged Prowlarr diagnostics.
func (s *Server) handleTestProwlarr(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	_ = json.NewDecoder(r.Body).Decode(&data)

	testURL, hasURL := data["url"]
	apiKey, hasAPIKey := data["api_key"]
	if !hasURL {
		testURL = s.cfg.ProwlarrURL
	}
	if !hasAPIKey || apiKey == maskedValue {
		apiKey = s.cfg.ProwlarrAPIKey
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	result := diagnostics.DiagnoseProwlarr(ctx, diagnostics.ProwlarrConfig{
		URL:     strings.TrimRight(testURL, "/"),
		APIKey:  apiKey,
		Timeout: 5 * time.Second,
	})
	writeJSON(w, http.StatusOK, result)
}

// handleTestQBittorrent runs staged qBittorrent diagnostics.
func (s *Server) handleTestQBittorrent(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	_ = json.NewDecoder(r.Body).Decode(&data)

	testURL, hasURL := data["url"]
	username, hasUsername := data["username"]
	password, hasPassword := data["password"]
	if !hasURL {
		testURL = s.cfg.QBUrl
	}
	if !hasUsername {
		username = s.cfg.QBUser
	}
	if !hasPassword || password == maskedValue {
		password = s.cfg.QBPass
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	result := diagnostics.DiagnoseQBittorrent(ctx, diagnostics.QBittorrentConfig{
		URL:      strings.TrimRight(testURL, "/"),
		Username: username,
		Password: password,
		Timeout:  5 * time.Second,
	})
	writeJSON(w, http.StatusOK, result)
}

// handleTestTransmission tests the Transmission RPC connection.
func (s *Server) handleTestTransmission(w http.ResponseWriter, _ *http.Request) {
	result := s.transmission.Diagnose()
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

	if err := validateTestURL(s.cfg.ABSURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
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

	if err := validateTestURL(s.cfg.KavitaURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
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
