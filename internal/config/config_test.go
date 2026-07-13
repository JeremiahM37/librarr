package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any env vars that might interfere.
	for _, key := range []string{
		"LIBRARR_PORT", "LIBRARR_DB_PATH", "QB_URL", "QB_USER", "QB_PASS",
		"PROWLARR_URL", "PROWLARR_API_KEY", "FILE_ORG_ENABLED",
		"ANNAS_ARCHIVE_DOMAIN", "RATE_LIMIT_ENABLED", "METRICS_ENABLED",
		"MIN_TORRENT_SIZE_BYTES", "MAX_TORRENT_SIZE_BYTES",
		"AUTH_USERNAME", "AUTH_PASSWORD", "API_KEY",
		"OIDC_ENABLED", "OIDC_ISSUER", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET",
		"OIDC_PROXY_HEADERS_ENABLED",
	} {
		os.Unsetenv(key)
	}

	cfg := Load()

	t.Run("port default", func(t *testing.T) {
		if cfg.Port != 5050 {
			t.Errorf("expected port 5050, got %d", cfg.Port)
		}
	})

	t.Run("db path default", func(t *testing.T) {
		if cfg.DBPath != "/data/librarr.db" {
			t.Errorf("expected /data/librarr.db, got %s", cfg.DBPath)
		}
	})

	t.Run("qb user default", func(t *testing.T) {
		if cfg.QBUser != "admin" {
			t.Errorf("expected admin, got %s", cfg.QBUser)
		}
	})

	t.Run("file org enabled by default", func(t *testing.T) {
		if !cfg.FileOrgEnabled {
			t.Error("expected FileOrgEnabled=true by default")
		}
	})

	t.Run("annas archive domain default", func(t *testing.T) {
		if cfg.AnnasArchiveDomain != "annas-archive.gl" {
			t.Errorf("expected annas-archive.gl, got %s", cfg.AnnasArchiveDomain)
		}
	})

	t.Run("torrent size defaults", func(t *testing.T) {
		if cfg.MinTorrentSizeBytes != 10000 {
			t.Errorf("expected min 10000, got %d", cfg.MinTorrentSizeBytes)
		}
		if cfg.MaxTorrentSizeBytes != 2000000000 {
			t.Errorf("expected max 2000000000, got %d", cfg.MaxTorrentSizeBytes)
		}
	})

	t.Run("OIDC disabled by default", func(t *testing.T) {
		if cfg.OIDCEnabled {
			t.Error("expected OIDCEnabled=false by default")
		}
	})

	t.Run("OIDC proxy headers disabled by default", func(t *testing.T) {
		if cfg.OIDCProxyHeaders {
			t.Error("expected OIDCProxyHeaders=false by default")
		}
	})

	t.Run("rate limit enabled by default", func(t *testing.T) {
		if !cfg.RateLimitEnabled {
			t.Error("expected RateLimitEnabled=true by default")
		}
	})
}

func TestLoad_EnvOverrides(t *testing.T) {
	os.Setenv("LIBRARR_PORT", "8080")
	os.Setenv("LIBRARR_DB_PATH", "/tmp/test.db")
	os.Setenv("QB_URL", "http://localhost:9090")
	os.Setenv("FILE_ORG_ENABLED", "false")
	os.Setenv("ANNAS_ARCHIVE_DOMAIN", "https://annas-archive.org/search/")
	os.Setenv("MIN_TORRENT_SIZE_BYTES", "50000")
	os.Setenv("OIDC_PROXY_HEADERS_ENABLED", "true")
	defer func() {
		for _, key := range []string{
			"LIBRARR_PORT", "LIBRARR_DB_PATH", "QB_URL",
			"FILE_ORG_ENABLED", "ANNAS_ARCHIVE_DOMAIN", "MIN_TORRENT_SIZE_BYTES",
			"OIDC_PROXY_HEADERS_ENABLED",
		} {
			os.Unsetenv(key)
		}
	}()

	cfg := Load()

	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.DBPath != "/tmp/test.db" {
		t.Errorf("expected /tmp/test.db, got %s", cfg.DBPath)
	}
	if cfg.QBUrl != "http://localhost:9090" {
		t.Errorf("expected QB URL, got %s", cfg.QBUrl)
	}
	if cfg.FileOrgEnabled {
		t.Error("expected FileOrgEnabled=false with env override")
	}
	if cfg.AnnasArchiveDomain != "annas-archive.org" {
		t.Errorf("expected annas-archive.org, got %s", cfg.AnnasArchiveDomain)
	}
	if cfg.MinTorrentSizeBytes != 50000 {
		t.Errorf("expected 50000, got %d", cfg.MinTorrentSizeBytes)
	}
	if !cfg.OIDCProxyHeaders {
		t.Error("expected OIDCProxyHeaders=true with env override")
	}
}

func TestLoad_NormalizesSettingsFileDomain(t *testing.T) {
	dir := t.TempDir()
	settingsPath := dir + "/settings.json"
	if err := os.WriteFile(settingsPath, []byte(`{"annas_archive_domain":"https://Annas-Archive.GD/search/"}`), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	t.Setenv("SETTINGS_FILE", settingsPath)

	cfg := Load()
	if cfg.AnnasArchiveDomain != "annas-archive.gd" {
		t.Errorf("AnnasArchiveDomain = %q, want annas-archive.gd", cfg.AnnasArchiveDomain)
	}
}

func TestLoad_SettingsFileOverridesEnv(t *testing.T) {
	// Env says one thing; settings file should win.
	os.Setenv("PROWLARR_URL", "http://from-env:9696")
	os.Setenv("PROWLARR_API_KEY", "env-key")
	os.Setenv("REMOVE_TORRENT_AFTER_IMPORT", "true")
	defer func() {
		os.Unsetenv("PROWLARR_URL")
		os.Unsetenv("PROWLARR_API_KEY")
		os.Unsetenv("REMOVE_TORRENT_AFTER_IMPORT")
	}()

	dir := t.TempDir()
	settingsPath := dir + "/settings.json"
	body := `{
		"prowlarr_url": "http://from-file:9696",
		"prowlarr_api_key": "file-key",
		"remove_torrent_after_import": false
	}`
	if err := os.WriteFile(settingsPath, []byte(body), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	os.Setenv("SETTINGS_FILE", settingsPath)
	defer os.Unsetenv("SETTINGS_FILE")

	cfg := Load()

	if cfg.ProwlarrURL != "http://from-file:9696" {
		t.Errorf("settings file should override env URL, got %q", cfg.ProwlarrURL)
	}
	if cfg.ProwlarrAPIKey != "file-key" {
		t.Errorf("settings file should override env API key, got %q", cfg.ProwlarrAPIKey)
	}
	if cfg.RemoveTorrentAfterImport {
		t.Error("settings file should override env bool")
	}
}

func TestLoad_SettingsFileMissing_NoError(t *testing.T) {
	os.Setenv("SETTINGS_FILE", "/nonexistent/path/settings.json")
	os.Setenv("PROWLARR_URL", "http://env-only:9696")
	defer func() {
		os.Unsetenv("SETTINGS_FILE")
		os.Unsetenv("PROWLARR_URL")
	}()

	cfg := Load()
	if cfg.ProwlarrURL != "http://env-only:9696" {
		t.Errorf("missing settings file should not clobber env values, got %q", cfg.ProwlarrURL)
	}
}

func TestLoad_SettingsFileTypeMismatch_IgnoresBadValues(t *testing.T) {
	// A JSON number where the field expects a string, and a string where the
	// field expects a bool — neither should override the env value. Without
	// the type guards in applySettingsFileOverrides, the runtime cfg would
	// hold nonsense values that the rest of the app would then dereference.
	os.Setenv("PROWLARR_URL", "http://env-only:9696")
	os.Setenv("FILE_ORG_ENABLED", "true")
	defer func() {
		os.Unsetenv("PROWLARR_URL")
		os.Unsetenv("FILE_ORG_ENABLED")
	}()

	dir := t.TempDir()
	settingsPath := dir + "/settings.json"
	body := `{
		"prowlarr_url": 12345,
		"file_org_enabled": "not_a_bool"
	}`
	if err := os.WriteFile(settingsPath, []byte(body), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	os.Setenv("SETTINGS_FILE", settingsPath)
	defer os.Unsetenv("SETTINGS_FILE")

	cfg := Load()

	if cfg.ProwlarrURL != "http://env-only:9696" {
		t.Errorf("number value for string field should be ignored, got %q", cfg.ProwlarrURL)
	}
	if !cfg.FileOrgEnabled {
		t.Error("string value for bool field should be ignored, env true should persist")
	}
}

func TestLoad_SettingsFileMalformed_NoError(t *testing.T) {
	dir := t.TempDir()
	settingsPath := dir + "/settings.json"
	if err := os.WriteFile(settingsPath, []byte("{ this is not valid json"), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	os.Setenv("SETTINGS_FILE", settingsPath)
	os.Setenv("PROWLARR_URL", "http://env-only:9696")
	defer func() {
		os.Unsetenv("SETTINGS_FILE")
		os.Unsetenv("PROWLARR_URL")
	}()

	cfg := Load()
	if cfg.ProwlarrURL != "http://env-only:9696" {
		t.Errorf("malformed settings file should not clobber env values, got %q", cfg.ProwlarrURL)
	}
}

func TestGetEnvInt_InvalidFallback(t *testing.T) {
	os.Setenv("TEST_INT_INVALID", "not_a_number")
	defer os.Unsetenv("TEST_INT_INVALID")

	result := getEnvInt("TEST_INT_INVALID", 42)
	if result != 42 {
		t.Errorf("expected fallback 42, got %d", result)
	}
}

func TestGetEnvBool(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		fallback bool
		expected bool
	}{
		{"true string", "true", false, true},
		{"1 string", "1", false, true},
		{"yes string", "yes", false, true},
		{"false string", "false", true, false},
		{"0 string", "0", true, false},
		{"no string", "no", true, false},
		{"empty uses fallback true", "", false, false},
		{"unknown uses fallback", "maybe", false, false},
		{"unknown uses fallback true", "maybe", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("TEST_BOOL", tt.value)
			defer os.Unsetenv("TEST_BOOL")

			result := getEnvBool("TEST_BOOL", tt.fallback)
			if result != tt.expected {
				t.Errorf("getEnvBool(%q, %v) = %v, want %v", tt.value, tt.fallback, result, tt.expected)
			}
		})
	}
}

func TestGetEnvInt64(t *testing.T) {
	os.Setenv("TEST_INT64", "9999999999")
	defer os.Unsetenv("TEST_INT64")

	result := getEnvInt64("TEST_INT64", 0)
	if result != 9999999999 {
		t.Errorf("expected 9999999999, got %d", result)
	}
}

func TestGetEnvInt64_Invalid(t *testing.T) {
	os.Setenv("TEST_INT64_BAD", "abc")
	defer os.Unsetenv("TEST_INT64_BAD")

	result := getEnvInt64("TEST_INT64_BAD", 100)
	if result != 100 {
		t.Errorf("expected fallback 100, got %d", result)
	}
}

func TestHas_Methods(t *testing.T) {
	t.Run("HasQBittorrent", func(t *testing.T) {
		cfg := &Config{QBUrl: ""}
		if cfg.HasQBittorrent() {
			t.Error("expected false when QBUrl is empty")
		}
		cfg.QBUrl = "http://localhost:8080"
		if !cfg.HasQBittorrent() {
			t.Error("expected true when QBUrl is set")
		}
	})

	t.Run("HasProwlarr", func(t *testing.T) {
		cfg := &Config{ProwlarrURL: "http://p", ProwlarrAPIKey: ""}
		if cfg.HasProwlarr() {
			t.Error("expected false when API key is empty")
		}
		cfg.ProwlarrAPIKey = "abc"
		if !cfg.HasProwlarr() {
			t.Error("expected true when both are set")
		}
	})

	t.Run("HasAuth", func(t *testing.T) {
		cfg := &Config{AuthUsername: "user", AuthPassword: ""}
		if cfg.HasAuth() {
			t.Error("expected false when password is empty")
		}
		cfg.AuthPassword = "pass"
		if !cfg.HasAuth() {
			t.Error("expected true when both are set")
		}
	})

	t.Run("HasOIDC", func(t *testing.T) {
		cfg := &Config{OIDCEnabled: true, OIDCIssuer: "https://issuer", OIDCClientID: "id", OIDCClientSecret: ""}
		if cfg.HasOIDC() {
			t.Error("expected false when client secret is empty")
		}
		cfg.OIDCClientSecret = "secret"
		if !cfg.HasOIDC() {
			t.Error("expected true when all fields set")
		}
		cfg.OIDCEnabled = false
		if cfg.HasOIDC() {
			t.Error("expected false when disabled")
		}
	})

	t.Run("HasSABnzbd", func(t *testing.T) {
		cfg := &Config{SABnzbdURL: "http://sab", SABnzbdAPIKey: "key"}
		if !cfg.HasSABnzbd() {
			t.Error("expected true when both set")
		}
		cfg.SABnzbdAPIKey = ""
		if cfg.HasSABnzbd() {
			t.Error("expected false when key is empty")
		}
	})

	t.Run("HasAPIKey", func(t *testing.T) {
		cfg := &Config{APIKey: ""}
		if cfg.HasAPIKey() {
			t.Error("expected false when empty")
		}
		cfg.APIKey = "mykey"
		if !cfg.HasAPIKey() {
			t.Error("expected true when set")
		}
	})

	t.Run("HasAudiobookshelf", func(t *testing.T) {
		cfg := &Config{ABSURL: "http://abs", ABSToken: "tok"}
		if !cfg.HasAudiobookshelf() {
			t.Error("expected true")
		}
		cfg.ABSToken = ""
		if cfg.HasAudiobookshelf() {
			t.Error("expected false")
		}
	})

	t.Run("HasKavita", func(t *testing.T) {
		cfg := &Config{KavitaURL: "http://k", KavitaUser: "u", KavitaPass: "p"}
		if !cfg.HasKavita() {
			t.Error("expected true")
		}
		cfg.KavitaPass = ""
		if cfg.HasKavita() {
			t.Error("expected false")
		}
	})

	t.Run("HasCalibre", func(t *testing.T) {
		cfg := &Config{CalibreLibraryPath: "/lib"}
		if !cfg.HasCalibre() {
			t.Error("expected true")
		}
		cfg.CalibreLibraryPath = ""
		if cfg.HasCalibre() {
			t.Error("expected false")
		}
	})

	t.Run("HasKomga", func(t *testing.T) {
		cfg := &Config{KomgaURL: "http://k", KomgaUser: "u", KomgaPass: "p"}
		if !cfg.HasKomga() {
			t.Error("expected true")
		}
		cfg.KomgaUser = ""
		if cfg.HasKomga() {
			t.Error("expected false")
		}
	})
}

func TestRemoveTorrentAfterImportDefault(t *testing.T) {
	os.Unsetenv("REMOVE_TORRENT_AFTER_IMPORT")
	cfg := Load()
	if !cfg.RemoveTorrentAfterImport {
		t.Error("RemoveTorrentAfterImport should default to true (remove after import)")
	}
}

func TestRemoveTorrentAfterImportDisabled(t *testing.T) {
	os.Setenv("REMOVE_TORRENT_AFTER_IMPORT", "false")
	defer os.Unsetenv("REMOVE_TORRENT_AFTER_IMPORT")
	cfg := Load()
	if cfg.RemoveTorrentAfterImport {
		t.Error("RemoveTorrentAfterImport should be false when explicitly disabled")
	}
}

func TestActiveTorrentClient(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"none", Config{}, ""},
		{"qb only", Config{QBUrl: "http://qb"}, "qbittorrent"},
		{"transmission only", Config{TransmissionURL: "http://tr"}, "transmission"},
		{"both default prefers qb", Config{QBUrl: "http://qb", TransmissionURL: "http://tr"}, "qbittorrent"},
		{"explicit transmission", Config{QBUrl: "http://qb", TransmissionURL: "http://tr", TorrentClient: "transmission"}, "transmission"},
		{"explicit qb alias", Config{QBUrl: "http://qb", TransmissionURL: "http://tr", TorrentClient: "qbit"}, "qbittorrent"},
		{"explicit transmission but unconfigured falls back", Config{QBUrl: "http://qb", TorrentClient: "transmission"}, "qbittorrent"},
	}
	for _, c := range cases {
		if got := c.cfg.ActiveTorrentClient(); got != c.want {
			t.Errorf("%s: ActiveTorrentClient() = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestHasTorrentClient(t *testing.T) {
	if (&Config{}).HasTorrentClient() {
		t.Error("empty config should have no torrent client")
	}
	if !(&Config{TransmissionURL: "http://tr"}).HasTorrentClient() {
		t.Error("transmission-only config should report a torrent client")
	}
}
