package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDeriveSettingsFile covers the path-resolution rules for SETTINGS_FILE.
//
// Regression: a v1.1.0 deployment with LIBRARR_DB_PATH=/data/librarr/librarr.db
// silently failed every UI "Save Settings" click because the binary defaulted
// SettingsFile to /data/settings.json (a path inside a non-writable parent).
// Auto-deriving from the DB-path dir prevents that whole class of misconfig.
func TestDeriveSettingsFile(t *testing.T) {
	cases := []struct {
		name     string
		explicit string
		dbPath   string
		want     string
	}{
		{"explicit env wins over derived", "/custom/settings.json", "/data/librarr/librarr.db", "/custom/settings.json"},
		{"default db path -> /data/settings.json (historical)", "", "/data/librarr.db", "/data/settings.json"},
		{"db in subdir -> derives next to it", "", "/data/librarr/librarr.db", "/data/librarr/settings.json"},
		{"db at root of mount -> derives next to it", "", "/var/lib/librarr/db.sqlite", "/var/lib/librarr/settings.json"},
		{"relative db path -> falls back to /data", "", "librarr.db", "/data/settings.json"},
		{"empty db path -> /data fallback", "", "", "/data/settings.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveSettingsFile(tc.explicit, tc.dbPath)
			if got != tc.want {
				t.Errorf("deriveSettingsFile(%q, %q) = %q, want %q", tc.explicit, tc.dbPath, got, tc.want)
			}
		})
	}
}

// TestProbeSettingsFileWritable confirms the startup write probe does NOT
// crash on a non-writable path (must be tolerant — just logs a clear error).
func TestProbeSettingsFileWritable_NonWritable(t *testing.T) {
	// /proc is read-only on Linux; on darwin, /System. Find a path we know
	// won't be writable for tests.
	dir := t.TempDir()
	roDir := filepath.Join(dir, "read-only")
	if err := os.Mkdir(roDir, 0o555); err != nil { // r-xr-xr-x, no write
		t.Fatal(err)
	}
	cfg := &Config{SettingsFile: filepath.Join(roDir, "settings.json")}

	// Must not panic. The error is reported via slog (silenced by main_test.go).
	cfg.probeSettingsFileWritable()
}

// TestProbeSettingsFileWritable_Writable confirms the probe leaves no
// litter behind on the happy path.
func TestProbeSettingsFileWritable_Writable(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{SettingsFile: filepath.Join(dir, "settings.json")}
	cfg.probeSettingsFileWritable()

	// The probe file must be cleaned up.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "settings.json" {
			t.Errorf("probe left behind unexpected entry: %s", e.Name())
		}
	}
}

// TestProbeSettingsFileWritable_EmptyPath is a defensive guard — empty
// SettingsFile should be a no-op, not a crash.
func TestProbeSettingsFileWritable_EmptyPath(t *testing.T) {
	cfg := &Config{SettingsFile: ""}
	cfg.probeSettingsFileWritable() // must not panic
}
