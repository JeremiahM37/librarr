package config

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNormalizeBaseURL covers the shapes users actually paste into the
// Integrations form or an env var.
//
// Regression for issue #92: "audiobookshelf:13378" (no scheme) reaches
// net/http as a relative URL, and every request built from it fails with
// `unsupported protocol scheme ""`.
func TestNormalizeBaseURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty stays empty", "", ""},
		{"whitespace only stays empty", "   ", ""},
		{"already valid is untouched", "http://abs:13378", "http://abs:13378"},
		{"https is untouched", "https://abs.example.com", "https://abs.example.com"},
		{"scheme case preserved", "HTTP://abs:13378", "HTTP://abs:13378"},
		{"host:port gets http", "audiobookshelf:13378", "http://audiobookshelf:13378"},
		{"bare host gets http", "audiobookshelf", "http://audiobookshelf"},
		{"ip:port gets http", "192.168.1.5:13378", "http://192.168.1.5:13378"},
		{"host with path gets http", "abs.example.com/abs", "http://abs.example.com/abs"},
		{"surrounding whitespace trimmed", "  http://abs:13378  ", "http://abs:13378"},
		{"trailing slash trimmed", "http://abs:13378/", "http://abs:13378"},
		{"repeated trailing slashes trimmed", "http://abs:13378///", "http://abs:13378"},
		{"trailing slash trimmed after scheme added", "audiobookshelf:13378/", "http://audiobookshelf:13378"},
		{"subpath keeps its shape", "https://host/abs/", "https://host/abs"},
		{"non-http scheme left alone", "unix:///var/run/abs.sock", "unix:///var/run/abs.sock"},
		{"scheme-only input is not truncated", "http://", "http://"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeBaseURL(tc.in); got != tc.want {
				t.Errorf("NormalizeBaseURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeBaseURL_ProducesUsableRequests is the end-to-end half of the
// regression: net/http must accept the normalized value. Before the fix, the
// raw input produced `unsupported protocol scheme ""` at request time.
func TestNormalizeBaseURL_ProducesUsableRequests(t *testing.T) {
	for _, raw := range []string{"audiobookshelf:13378", "audiobookshelf", "192.168.1.5:13378"} {
		t.Run(raw, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, NormalizeBaseURL(raw)+"/api/libraries", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if req.URL.Scheme != "http" {
				t.Errorf("scheme = %q, want http", req.URL.Scheme)
			}
			if req.URL.Host == "" {
				t.Error("host is empty — request would fail with unsupported protocol scheme")
			}
		})
	}
}

// TestLoad_NormalizesEnvURLs proves the repair reaches the config the app
// actually runs on, for both config sources (env vars here, settings.json in
// the next test).
func TestLoad_NormalizesEnvURLs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SETTINGS_FILE", filepath.Join(dir, "settings.json"))
	t.Setenv("ABS_URL", "audiobookshelf:13378")
	t.Setenv("ABS_PUBLIC_URL", "abs.example.com/")
	t.Setenv("PROWLARR_URL", "prowlarr:9696")
	t.Setenv("QB_URL", "  qbittorrent:8080  ")
	t.Setenv("KAVITA_URL", "https://kavita.example.com")

	cfg := Load()

	for _, tc := range []struct {
		field string
		got   string
		want  string
	}{
		{"ABSURL", cfg.ABSURL, "http://audiobookshelf:13378"},
		{"ABSPublicURL", cfg.ABSPublicURL, "http://abs.example.com"},
		{"ProwlarrURL", cfg.ProwlarrURL, "http://prowlarr:9696"},
		{"QBUrl", cfg.QBUrl, "http://qbittorrent:8080"},
		{"KavitaURL", cfg.KavitaURL, "https://kavita.example.com"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.field, tc.got, tc.want)
		}
	}
}

// TestLoad_NormalizesSettingsFileURLs is the path the issue reporter hit:
// the URL was typed into the UI, so it arrives via settings.json rather than
// an env var. Every key in BaseURLSettingKeys is exercised, which also guards
// against the list drifting away from normalizeServiceURLs.
func TestLoad_NormalizesSettingsFileURLs(t *testing.T) {
	dir := t.TempDir()
	settingsPath := filepath.Join(dir, "settings.json")

	// Scheme-less value for every base-URL key the settings file supports.
	body := `{
	  "qb_url": "qbittorrent:8080",
	  "transmission_url": "transmission:9091",
	  "prowlarr_url": "prowlarr:9696",
	  "sabnzbd_url": "sabnzbd:8080",
	  "abs_url": "audiobookshelf:13378",
	  "abs_public_url": "abs.example.com",
	  "kavita_url": "kavita:5000",
	  "kavita_public_url": "kavita.example.com",
	  "komga_url": "komga:25600",
	  "calibre_url": "calibre-web:8083",
	  "flibusta_url": "flibusta.is"
	}`
	if err := os.WriteFile(settingsPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SETTINGS_FILE", settingsPath)

	cfg := Load()

	got := map[string]string{
		"qb_url":            cfg.QBUrl,
		"transmission_url":  cfg.TransmissionURL,
		"prowlarr_url":      cfg.ProwlarrURL,
		"sabnzbd_url":       cfg.SABnzbdURL,
		"abs_url":           cfg.ABSURL,
		"abs_public_url":    cfg.ABSPublicURL,
		"kavita_url":        cfg.KavitaURL,
		"kavita_public_url": cfg.KavitaPublicURL,
		"komga_url":         cfg.KomgaURL,
		"calibre_url":       cfg.CalibreURL,
		"flibusta_url":      cfg.FlibustaURL,
	}

	for _, key := range BaseURLSettingKeys {
		value, ok := got[key]
		if !ok {
			t.Fatalf("BaseURLSettingKeys contains %q but this test does not assert on it — "+
				"add the matching Config field so the list can't drift", key)
		}
		if !strings.HasPrefix(value, "http://") {
			t.Errorf("%s = %q, want an http:// prefix", key, value)
		}
	}
}
