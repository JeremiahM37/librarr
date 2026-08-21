package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/search"
)

// settingsTestServer builds the minimum Server needed for settings handler
// tests: an on-disk SettingsFile, a real in-memory DB (LogActivity needs one),
// and a search.Manager (ForeignLangFilterEnabled is read on GET).
func settingsTestServer(t *testing.T) (*Server, string) {
	t.Helper()

	dir := t.TempDir()
	settingsPath := filepath.Join(dir, "settings.json")

	cfg := &config.Config{
		SettingsFile: settingsPath,
		// Seed the env-layer values that the handler injects as defaults.
		ProwlarrURL:    "http://env-prowlarr:9696",
		ProwlarrAPIKey: "ENV_API_KEY",
		QBUrl:          "http://env-qbit:8080",
		QBUser:         "admin",
		QBPass:         "env-qb-pass",
	}

	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	health := search.NewHealthTracker(3, 300)
	searchMgr := search.NewManager(cfg, nil, health)

	return &Server{cfg: cfg, db: database, searchMgr: searchMgr}, settingsPath
}

func saveSettings(t *testing.T, s *Server, payload map[string]interface{}) {
	t.Helper()
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/settings", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUsername, "admin"))
	rr := httptest.NewRecorder()
	s.handleSaveSettings(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("save returned %d: %s", rr.Code, rr.Body.String())
	}
}

func readSettingsFile(t *testing.T, path string) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		// Treat missing file as empty — that's the post-clear state.
		return map[string]interface{}{}
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("settings.json malformed: %v", err)
	}
	return m
}

// TestSaveSettings_EmptyStringDeletesKey — regression guard for the bug found
// in E2E. Clearing a UI field must remove the override from settings.json so
// the env value reapplies; persisting "" would make /api/settings disagree
// with the runtime cfg.
func TestSaveSettings_EmptyStringDeletesKey(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{"prowlarr_url": "http://custom:9696"})
	if got := readSettingsFile(t, path)["prowlarr_url"]; got != "http://custom:9696" {
		t.Fatalf("setup: expected URL persisted, got %v", got)
	}

	saveSettings(t, s, map[string]interface{}{"prowlarr_url": ""})

	stored := readSettingsFile(t, path)
	if _, present := stored["prowlarr_url"]; present {
		t.Errorf("empty-string save should delete the key, but it is still present: %v", stored)
	}
}

func TestSaveSettings_NormalizesAnnasDomain(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{
		"annas_archive_domain": " https://Annas-Archive.GD/search/ ",
	})

	if got := readSettingsFile(t, path)["annas_archive_domain"]; got != "annas-archive.gd" {
		t.Errorf("expected normalized domain, got %v", got)
	}
}

// TestSaveSettings_FalseBoolPersists — the empty-string-delete rule must NOT
// drop legitimate falsy values like bool false. Toggles depend on this.
func TestSaveSettings_FalseBoolPersists(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{"remove_torrent_after_import": false})

	stored := readSettingsFile(t, path)
	v, present := stored["remove_torrent_after_import"]
	if !present {
		t.Fatal("bool false should be persisted, but key was deleted")
	}
	if b, ok := v.(bool); !ok || b != false {
		t.Errorf("expected false, got %v (%T)", v, v)
	}
}

// TestSaveSettings_MaskedSentinelPreservesRealValue — when a user saves a form
// without touching a sensitive field, the JS sends back the masked sentinel
// "--------". The handler must drop that key entirely so the previously-saved
// real value remains on disk. Without this, a single UI save could wipe every
// API key in settings.json.
func TestSaveSettings_MaskedSentinelPreservesRealValue(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{
		"prowlarr_url":     "http://saved:9696",
		"prowlarr_api_key": "REAL_SECRET_KEY",
	})

	// User edits only the URL and submits the form; API key field still holds
	// the "--------" mask the GET handler returned.
	saveSettings(t, s, map[string]interface{}{
		"prowlarr_url":     "http://updated:9696",
		"prowlarr_api_key": maskedValue,
	})

	stored := readSettingsFile(t, path)
	if got := stored["prowlarr_url"]; got != "http://updated:9696" {
		t.Errorf("URL should have updated, got %v", got)
	}
	if got := stored["prowlarr_api_key"]; got != "REAL_SECRET_KEY" {
		t.Errorf("real API key should have been preserved, got %v", got)
	}
}

// TestSaveSettings_WriteFailureDoesNotLeakPath — when the on-disk write fails
// (e.g. permission denied) the HTTP response must stay generic. Previously the
// raw err.Error() was returned, which included the absolute filesystem path
// of settings.json — useful to an attacker probing the deployment layout.
func TestSaveSettings_WriteFailureDoesNotLeakPath(t *testing.T) {
	s, path := settingsTestServer(t)

	// Make the file unwritable. WriteFile on a 0444 file returns
	// "permission denied" with the full path in the message.
	if err := os.WriteFile(path, []byte("{}"), 0444); err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer os.Chmod(path, 0600)

	body, _ := json.Marshal(map[string]interface{}{"prowlarr_url": "http://x:9696"})
	req := httptest.NewRequest(http.MethodPost, "/api/settings", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUsername, "admin"))
	rr := httptest.NewRecorder()
	s.handleSaveSettings(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on write failure, got %d: %s", rr.Code, rr.Body.String())
	}
	if bytes.Contains(rr.Body.Bytes(), []byte(path)) {
		t.Errorf("response leaks settings file path: %s", rr.Body.String())
	}
	if bytes.Contains(rr.Body.Bytes(), []byte("permission denied")) {
		t.Errorf("response leaks underlying OS error: %s", rr.Body.String())
	}
}

// TestGetSettings_MasksSensitiveValues — non-empty sensitive values must come
// back as the sentinel, never as plaintext. Empty values stay empty so the UI
// can distinguish "unset" from "set but hidden".
func TestGetSettings_MasksSensitiveValues(t *testing.T) {
	s, _ := settingsTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	rr := httptest.NewRecorder()
	s.handleGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET returned %d", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Prowlarr API key is set in env, must be masked.
	if got := resp["prowlarr_api_key"]; got != maskedValue {
		t.Errorf("prowlarr_api_key should be masked, got %v", got)
	}
	if got := resp["qb_pass"]; got != maskedValue {
		t.Errorf("qb_pass should be masked, got %v", got)
	}
	// Non-sensitive URL is exposed.
	if got := resp["prowlarr_url"]; got != "http://env-prowlarr:9696" {
		t.Errorf("URL should be exposed, got %v", got)
	}
	// Unset sensitive (no env, no file) stays empty — empty is distinguishable
	// from masked so the UI can show a placeholder.
	if got := resp["abs_token"]; got != "" {
		t.Errorf("unset abs_token should be empty string, got %v", got)
	}
}

// TestSaveSettings_NormalizesSchemeLessURLs — regression for issue #92. A URL
// typed without "http://" was persisted verbatim, and every request built from
// it failed with `unsupported protocol scheme ""` (library page stuck on
// "Failed to load library", "abs scan failed" in the logs). The save handler
// now repairs the value, so settings.json and the next GET both show the URL
// the runtime will actually use.
func TestSaveSettings_NormalizesSchemeLessURLs(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{
		"abs_url":      "audiobookshelf:13378",
		"abs_token":    "tok",
		"kavita_url":   "  kavita:5000/  ",
		"komga_url":    "https://komga.example.com",
		"prowlarr_url": "192.168.1.5:9696",
	})

	saved := readSettingsFile(t, path)
	for key, want := range map[string]string{
		"abs_url":      "http://audiobookshelf:13378",
		"kavita_url":   "http://kavita:5000",
		"komga_url":    "https://komga.example.com",
		"prowlarr_url": "http://192.168.1.5:9696",
	} {
		if got := saved[key]; got != want {
			t.Errorf("settings.json %s = %v, want %q", key, got, want)
		}
	}

	// Non-URL values must pass through untouched.
	if got := saved["abs_token"]; got != "tok" {
		t.Errorf("abs_token = %v, want tok", got)
	}
}

// TestSaveSettings_WhitespaceOnlyURLClearsOverride — a field wiped to spaces
// normalizes to "", which must take the same "delete the override" path as an
// empty string so the env value reapplies on restart.
func TestSaveSettings_WhitespaceOnlyURLClearsOverride(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{"prowlarr_url": "http://saved:9696"})
	if got := readSettingsFile(t, path)["prowlarr_url"]; got != "http://saved:9696" {
		t.Fatalf("setup failed: prowlarr_url = %v", got)
	}

	saveSettings(t, s, map[string]interface{}{"prowlarr_url": "   "})
	if _, exists := readSettingsFile(t, path)["prowlarr_url"]; exists {
		t.Error("whitespace-only URL should delete the override, not persist a value")
	}
}

// A bad import_mode must never reach settings.json or the runtime config: an
// unrecognized value falls back to "move" rather than disabling organization.
func TestSaveSettings_NormalizesImportMode(t *testing.T) {
	s, path := settingsTestServer(t)

	saveSettings(t, s, map[string]interface{}{"import_mode": " HardLink "})

	if got := readSettingsFile(t, path)["import_mode"]; got != config.ImportModeHardlink {
		t.Errorf("stored import_mode = %v, want %q", got, config.ImportModeHardlink)
	}
	if s.cfg.ImportMode != config.ImportModeHardlink {
		t.Errorf("runtime ImportMode = %q, want %q", s.cfg.ImportMode, config.ImportModeHardlink)
	}

	// An unrecognized value clears the override rather than picking a mode,
	// which returns the setting to automatic.
	saveSettings(t, s, map[string]interface{}{"import_mode": "hardlnik"})

	if _, present := readSettingsFile(t, path)["import_mode"]; present {
		t.Error("an unrecognized import_mode should clear the override")
	}
	if s.cfg.ImportMode != config.ImportModeAuto {
		t.Errorf("runtime ImportMode = %q, want automatic", s.cfg.ImportMode)
	}
}

// Choosing "Automatic" in the UI posts an empty value: the override must be
// dropped from settings.json AND from the running config, or the two disagree
// until the next restart.
func TestSaveSettings_EmptyImportModeRestoresAutomatic(t *testing.T) {
	s, path := settingsTestServer(t)
	s.cfg.RemoveTorrentAfterImport = false

	saveSettings(t, s, map[string]interface{}{"import_mode": config.ImportModeMove})
	if s.cfg.EffectiveImportMode() != config.ImportModeMove {
		t.Fatalf("setup: effective mode = %q", s.cfg.EffectiveImportMode())
	}

	saveSettings(t, s, map[string]interface{}{"import_mode": ""})

	if _, present := readSettingsFile(t, path)["import_mode"]; present {
		t.Error("empty import_mode should delete the override")
	}
	if s.cfg.ImportMode != config.ImportModeAuto {
		t.Errorf("runtime ImportMode = %q, want automatic", s.cfg.ImportMode)
	}
	if got := s.cfg.EffectiveImportMode(); got != config.ImportModeHardlink {
		t.Errorf("effective mode = %q, want %q now that torrents are kept", got, config.ImportModeHardlink)
	}
}

// The settings UI renders the select from import_mode and the "this is what
// will happen" line from effective_import_mode, so both have to be exposed —
// on Automatic they differ, and that difference is the whole point.
func TestGetSettings_ExposesImportModeAndItsEffect(t *testing.T) {
	s, _ := settingsTestServer(t)
	s.cfg.RemoveTorrentAfterImport = false // keep torrents, nothing else set

	body := getSettings(t, s)
	if got := body["import_mode"]; got != config.ImportModeAuto {
		t.Errorf("import_mode = %v, want automatic", got)
	}
	if got := body["effective_import_mode"]; got != config.ImportModeHardlink {
		t.Errorf("effective_import_mode = %v, want %q", got, config.ImportModeHardlink)
	}

	s.cfg.RemoveTorrentAfterImport = true
	body = getSettings(t, s)
	if got := body["effective_import_mode"]; got != config.ImportModeMove {
		t.Errorf("effective_import_mode = %v, want %q when torrents are removed", got, config.ImportModeMove)
	}
}

func getSettings(t *testing.T, s *Server) map[string]interface{} {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleGetSettings(rr, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET returned %d", rr.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}
