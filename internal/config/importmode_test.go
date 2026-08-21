package config

import (
	"os"
	"testing"
)

func TestNormalizeImportMode(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ImportModeAuto},
		{"move", ImportModeMove},
		{"MOVE", ImportModeMove},
		{"  move  ", ImportModeMove},
		{"hardlink", ImportModeHardlink},
		{"Hardlink", ImportModeHardlink},
		{"hard_link", ImportModeHardlink},
		{"hard-link", ImportModeHardlink},
		{"link", ImportModeHardlink},
		{"copy", ImportModeCopy},
		{"COPY", ImportModeCopy},
		// A typo must not silently pick a mode nobody asked for; it lands on
		// the automatic default.
		{"hardlnik", ImportModeAuto},
		{"symlink", ImportModeAuto},
	}

	for _, tt := range tests {
		if got := NormalizeImportMode(tt.input); got != tt.want {
			t.Errorf("NormalizeImportMode(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestImportModeKeepsPayload(t *testing.T) {
	tests := map[string]bool{
		ImportModeAuto:     false,
		ImportModeMove:     false,
		"nonsense":         false,
		ImportModeHardlink: true,
		ImportModeCopy:     true,
	}
	for mode, want := range tests {
		if got := ImportModeKeepsPayload(mode); got != want {
			t.Errorf("ImportModeKeepsPayload(%q) = %v, want %v", mode, got, want)
		}
	}
}

// The single-knob contract: keeping torrents is on its own enough to keep them
// seedable, and an explicit mode always wins over that inference.
func TestEffectiveImportMode(t *testing.T) {
	tests := []struct {
		name         string
		configured   string
		removeAfter  bool
		want         string
		keepsPayload bool
	}{
		{"auto + remove torrents", ImportModeAuto, true, ImportModeMove, false},
		{"auto + keep torrents", ImportModeAuto, false, ImportModeHardlink, true},
		{"typo + keep torrents", "hardlnik", false, ImportModeHardlink, true},
		{"explicit move beats keep", ImportModeMove, false, ImportModeMove, false},
		{"explicit copy beats keep", ImportModeCopy, false, ImportModeCopy, true},
		{"explicit hardlink with removal", ImportModeHardlink, true, ImportModeHardlink, true},
		{"explicit copy with removal", ImportModeCopy, true, ImportModeCopy, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{ImportMode: tt.configured, RemoveTorrentAfterImport: tt.removeAfter}
			if got := cfg.EffectiveImportMode(); got != tt.want {
				t.Errorf("EffectiveImportMode() = %q, want %q", got, tt.want)
			}
			if got := cfg.KeepsPayload(); got != tt.keepsPayload {
				t.Errorf("KeepsPayload() = %v, want %v", got, tt.keepsPayload)
			}
		})
	}
}

// Nothing configured behaves exactly as librarr always has: torrents are
// removed after import, so imports move.
func TestLoad_ImportModeDefaultsToAutomaticMove(t *testing.T) {
	os.Unsetenv("IMPORT_MODE")
	os.Unsetenv("REMOVE_TORRENT_AFTER_IMPORT")
	os.Unsetenv("SETTINGS_FILE")
	cfg := Load()
	if cfg.ImportMode != ImportModeAuto {
		t.Errorf("ImportMode = %q, want automatic", cfg.ImportMode)
	}
	if got := cfg.EffectiveImportMode(); got != ImportModeMove {
		t.Errorf("EffectiveImportMode() = %q, want %q", got, ImportModeMove)
	}
}

// The one-setting path: keeping torrents is enough to make them seedable.
func TestLoad_KeepingTorrentsAloneEnablesHardlink(t *testing.T) {
	os.Unsetenv("IMPORT_MODE")
	os.Setenv("REMOVE_TORRENT_AFTER_IMPORT", "false")
	os.Unsetenv("SETTINGS_FILE")
	defer os.Unsetenv("REMOVE_TORRENT_AFTER_IMPORT")

	cfg := Load()
	if got := cfg.EffectiveImportMode(); got != ImportModeHardlink {
		t.Errorf("EffectiveImportMode() = %q, want %q", got, ImportModeHardlink)
	}
	if !cfg.KeepsPayload() {
		t.Error("KeepsPayload() = false; a kept torrent needs its payload")
	}
}

func TestLoad_ImportModeFromEnv(t *testing.T) {
	os.Setenv("IMPORT_MODE", "HardLink")
	defer os.Unsetenv("IMPORT_MODE")
	if cfg := Load(); cfg.ImportMode != ImportModeHardlink {
		t.Errorf("ImportMode = %q, want %q", cfg.ImportMode, ImportModeHardlink)
	}
}

func TestLoad_ImportModeInvalidEnvFallsBackToAutomatic(t *testing.T) {
	os.Setenv("IMPORT_MODE", "hardlnik")
	defer os.Unsetenv("IMPORT_MODE")
	if cfg := Load(); cfg.ImportMode != ImportModeAuto {
		t.Errorf("ImportMode = %q, want automatic for an unrecognized value", cfg.ImportMode)
	}
}

func TestLoad_ImportModeSettingsFileOverridesEnv(t *testing.T) {
	os.Setenv("IMPORT_MODE", "move")
	defer os.Unsetenv("IMPORT_MODE")

	settingsPath := t.TempDir() + "/settings.json"
	if err := os.WriteFile(settingsPath, []byte(`{"import_mode": "copy"}`), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	os.Setenv("SETTINGS_FILE", settingsPath)
	defer os.Unsetenv("SETTINGS_FILE")

	if cfg := Load(); cfg.ImportMode != ImportModeCopy {
		t.Errorf("ImportMode = %q, want %q from settings file", cfg.ImportMode, ImportModeCopy)
	}
}

func TestLoad_ImportModeSettingsFileNormalizesGarbage(t *testing.T) {
	os.Setenv("IMPORT_MODE", "hardlink")
	defer os.Unsetenv("IMPORT_MODE")

	settingsPath := t.TempDir() + "/settings.json"
	if err := os.WriteFile(settingsPath, []byte(`{"import_mode": "hardlinkk"}`), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	os.Setenv("SETTINGS_FILE", settingsPath)
	defer os.Unsetenv("SETTINGS_FILE")

	if cfg := Load(); cfg.ImportMode != ImportModeAuto {
		t.Errorf("ImportMode = %q, want automatic for an unrecognized settings value", cfg.ImportMode)
	}
}

// Clearing the override in the UI writes an empty value; that must return the
// mode to automatic rather than being ignored as "no key".
func TestLoad_ImportModeSettingsFileEmptyValueRestoresAutomatic(t *testing.T) {
	os.Setenv("IMPORT_MODE", "copy")
	defer os.Unsetenv("IMPORT_MODE")

	settingsPath := t.TempDir() + "/settings.json"
	if err := os.WriteFile(settingsPath, []byte(`{"import_mode": ""}`), 0600); err != nil {
		t.Fatalf("write settings file: %v", err)
	}
	os.Setenv("SETTINGS_FILE", settingsPath)
	defer os.Unsetenv("SETTINGS_FILE")

	if cfg := Load(); cfg.ImportMode != ImportModeAuto {
		t.Errorf("ImportMode = %q, want automatic", cfg.ImportMode)
	}
}
