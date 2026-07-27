package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/search"
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
		ProwlarrURL:         "http://env-prowlarr:9696",
		ProwlarrAPIKey:      "ENV_API_KEY",
		QBUrl:               "http://env-qbit:8080",
		QBUser:              "admin",
		QBPass:              "env-qb-pass",
		QBSavePath:          "/env/ebooks",
		QBCategory:          "env-ebooks",
		QBAudiobookSavePath: "/env/audiobooks",
		QBAudiobookCategory: "env-audiobooks",
		QBMangaSavePath:     "/env/manga",
		QBMangaCategory:     "env-manga",
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

func TestHandleSettingsExposesQBittorrentSavePathDefaults(t *testing.T) {
	s, _ := settingsTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	rr := httptest.NewRecorder()
	s.handleGetSettings(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var settings map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if settings["qb_save_path"] != "/env/ebooks" || settings["incoming_dir"] == settings["qb_save_path"] {
		t.Fatalf("settings = %+v", settings)
	}
	if settings["qb_audiobook_save_path"] != "/env/audiobooks" || settings["qb_manga_save_path"] != "/env/manga" {
		t.Fatalf("qB media paths missing: %+v", settings)
	}
}

func TestHandleSaveSettingsAppliesQBittorrentSavePathsToRuntimeConfig(t *testing.T) {
	s, settingsPath := settingsTestServer(t)
	body, _ := json.Marshal(map[string]interface{}{
		"qb_save_path":           "/remote/ebooks",
		"qb_category":            "ebooks",
		"qb_audiobook_save_path": "/remote/audiobooks",
		"qb_audiobook_category":  "audio",
		"qb_manga_save_path":     "/remote/manga",
		"qb_manga_category":      "manga",
		"incoming_dir":           "/data/incoming",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/settings", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleSaveSettings(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if s.cfg.QBSavePath != "/remote/ebooks" || s.cfg.IncomingDir == s.cfg.QBSavePath {
		t.Fatalf("runtime paths: QBSavePath=%q IncomingDir=%q", s.cfg.QBSavePath, s.cfg.IncomingDir)
	}
	if s.cfg.QBAudiobookSavePath != "/remote/audiobooks" || s.cfg.QBMangaSavePath != "/remote/manga" {
		t.Fatalf("runtime media paths: audio=%q manga=%q", s.cfg.QBAudiobookSavePath, s.cfg.QBMangaSavePath)
	}
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"qb_save_path": "/remote/ebooks"`) {
		t.Fatalf("saved settings = %s", string(data))
	}
}

func TestHandleTestProwlarrReturnsStructuredDiagnostics(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/system/status" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("X-Api-Key") != "test-key" {
			t.Fatalf("missing api key")
		}
		_, _ = w.Write([]byte(`{"version":"2.0.5"}`))
	}))
	defer upstream.Close()
	s, _ := settingsTestServer(t)

	body, _ := json.Marshal(map[string]interface{}{"url": upstream.URL, "api_key": "test-key"})
	req := httptest.NewRequest(http.MethodPost, "/api/test/prowlarr", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleTestProwlarr(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Service string `json:"service"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
		Steps   []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Service != "prowlarr" || !resp.Success || resp.Status != "connected" {
		t.Fatalf("resp = %+v", resp)
	}
	if len(resp.Steps) == 0 {
		t.Fatal("missing diagnostic steps")
	}
}

func TestHandleTestProwlarrUsesPostedURLInsteadOfSavedConfig(t *testing.T) {
	saved := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"version":"saved-would-succeed"}`))
	}))
	defer saved.Close()
	posted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer posted.Close()

	s, _ := settingsTestServer(t)
	s.cfg.ProwlarrURL = saved.URL
	s.cfg.ProwlarrAPIKey = "saved-key"

	body, _ := json.Marshal(map[string]interface{}{"url": posted.URL, "api_key": maskedValue})
	req := httptest.NewRequest(http.MethodPost, "/api/test/prowlarr", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleTestProwlarr(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Success bool `json:"success"`
		Steps   []struct {
			Name    string `json:"name"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatalf("posted URL should have failed instead of falling back to saved URL: %+v", resp)
	}
	for _, step := range resp.Steps {
		if step.Name == "API Validation" && step.Status == "failed" && strings.Contains(step.Message, "404") {
			return
		}
	}
	t.Fatalf("missing failed API validation for posted URL: %+v", resp.Steps)
}

func TestHandleTestQBittorrentReturnsAuthenticationSuggestion(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Fails.", http.StatusForbidden)
	}))
	defer upstream.Close()
	s, _ := settingsTestServer(t)

	body, _ := json.Marshal(map[string]interface{}{"url": upstream.URL, "username": "admin", "password": "bad"})
	req := httptest.NewRequest(http.MethodPost, "/api/test/qbittorrent", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleTestQBittorrent(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Service string `json:"service"`
		Success bool   `json:"success"`
		Steps   []struct {
			Name       string `json:"name"`
			Status     string `json:"status"`
			Suggestion string `json:"suggestion"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Service != "qbittorrent" || resp.Success {
		t.Fatalf("resp = %+v", resp)
	}
	for _, step := range resp.Steps {
		if step.Name == "Authentication" && step.Status == "failed" && step.Suggestion != "" {
			return
		}
	}
	t.Fatalf("missing failed auth step with suggestion: %+v", resp.Steps)
}

func TestHandleTestQBittorrentUsesPostedUsernameInsteadOfSavedConfig(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("username") != "admin" {
				http.Error(w, "Fails.", http.StatusForbidden)
				return
			}
			http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "sid"})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/app/version":
			_, _ = w.Write([]byte("v5.0.0"))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	s, _ := settingsTestServer(t)
	s.cfg.QBUrl = upstream.URL
	s.cfg.QBUser = "admin"
	s.cfg.QBPass = "saved-password"

	body, _ := json.Marshal(map[string]interface{}{
		"url":      upstream.URL,
		"username": "wrong-user",
		"password": maskedValue,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/test/qbittorrent", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleTestQBittorrent(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Success bool `json:"success"`
		Steps   []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatalf("posted username should have failed instead of falling back to saved username: %+v", resp)
	}
	for _, step := range resp.Steps {
		if step.Name == "Authentication" && step.Status == "failed" {
			return
		}
	}
	t.Fatalf("missing failed auth step for posted username: %+v", resp.Steps)
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
