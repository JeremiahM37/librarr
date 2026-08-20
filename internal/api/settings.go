package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/netutil"
	"github.com/JeremiahM37/librarr/internal/sources"
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
		"import_mode":                 config.NormalizeImportMode(s.cfg.ImportMode),

		// Integration URLs and credentials (sensitive ones are masked below).
		"qb_url":                  s.cfg.QBUrl,
		"qb_user":                 s.cfg.QBUser,
		"qb_pass":                 s.cfg.QBPass,
		"transmission_url":        s.cfg.TransmissionURL,
		"transmission_user":       s.cfg.TransmissionUser,
		"transmission_pass":       s.cfg.TransmissionPass,
		"torrent_client":          s.cfg.TorrentClient,
		"prowlarr_url":            s.cfg.ProwlarrURL,
		"prowlarr_api_key":        s.cfg.ProwlarrAPIKey,
		"sabnzbd_url":             s.cfg.SABnzbdURL,
		"sabnzbd_api_key":         s.cfg.SABnzbdAPIKey,
		"sabnzbd_category":        s.cfg.SABnzbdCategory,
		"abs_url":                 s.cfg.ABSURL,
		"abs_token":               s.cfg.ABSToken,
		"kavita_url":              s.cfg.KavitaURL,
		"kavita_user":             s.cfg.KavitaUser,
		"kavita_pass":             s.cfg.KavitaPass,
		"kavita_ebook_library_id": s.cfg.KavitaEbookLibraryID,
		"kavita_manga_library_id": s.cfg.KavitaMangaLibraryID,
		"komga_url":               s.cfg.KomgaURL,
		"komga_user":              s.cfg.KomgaUser,
		"komga_pass":              s.cfg.KomgaPass,
		"calibre_url":             s.cfg.CalibreURL,
		"calibre_library_path":    s.cfg.CalibreLibraryPath,
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
	if value, ok := data["import_mode"].(string); ok && value != "" {
		data["import_mode"] = config.NormalizeImportMode(value)
	}
	normalizeSettingURLs(data)

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
	if v, ok := data["import_mode"].(string); ok && v != "" {
		mode := config.NormalizeImportMode(v)
		s.cfg.ImportMode = mode
		slog.Info("import mode updated", "mode", mode)
	}
	if v, ok := data["annas_archive_domain"].(string); ok && v != "" {
		s.cfg.AnnasArchiveDomain = sources.NormalizeDomain(v)
	}
	if v, ok := data["annas_archive_secret_key"].(string); ok && v != "" {
		s.cfg.AnnasArchiveSecretKey = v
		slog.Info("annas archive secret key updated")
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// normalizeSettingURLs repairs service base URLs in an incoming settings
// payload — mainly by adding the "http://" a user leaves off when they type
// "audiobookshelf:13378" into the Integrations form. Without a scheme every
// request built from that value dies with `unsupported protocol scheme ""`
// (issue #92). Normalizing on write means settings.json, the UI and the
// runtime config all agree; config.Load repairs the env layer separately.
func normalizeSettingURLs(data map[string]interface{}) {
	for _, key := range config.BaseURLSettingKeys {
		v, ok := data[key]
		if !ok {
			continue
		}
		str, isStr := v.(string)
		if !isStr {
			continue
		}
		data[key] = config.NormalizeBaseURL(str)
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
