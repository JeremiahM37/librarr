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
		{"", ImportModeMove},
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
		// A typo must not silently become something destructive or exotic;
		// it falls back to the historical behavior.
		{"hardlnik", ImportModeMove},
		{"symlink", ImportModeMove},
	}

	for _, tt := range tests {
		if got := NormalizeImportMode(tt.input); got != tt.want {
			t.Errorf("NormalizeImportMode(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestImportModeKeepsPayload(t *testing.T) {
	tests := map[string]bool{
		"":         false,
		"move":     false,
		"nonsense": false,
		"hardlink": true,
		"copy":     true,
	}
	for mode, want := range tests {
		if got := ImportModeKeepsPayload(mode); got != want {
			t.Errorf("ImportModeKeepsPayload(%q) = %v, want %v", mode, got, want)
		}
	}
}

func TestLoad_ImportModeDefaultsToMove(t *testing.T) {
	os.Unsetenv("IMPORT_MODE")
	if cfg := Load(); cfg.ImportMode != ImportModeMove {
		t.Errorf("ImportMode = %q, want %q", cfg.ImportMode, ImportModeMove)
	}
}

func TestLoad_ImportModeFromEnv(t *testing.T) {
	os.Setenv("IMPORT_MODE", "HardLink")
	defer os.Unsetenv("IMPORT_MODE")
	if cfg := Load(); cfg.ImportMode != ImportModeHardlink {
		t.Errorf("ImportMode = %q, want %q", cfg.ImportMode, ImportModeHardlink)
	}
}

func TestLoad_ImportModeInvalidEnvFallsBackToMove(t *testing.T) {
	os.Setenv("IMPORT_MODE", "hardlnik")
	defer os.Unsetenv("IMPORT_MODE")
	if cfg := Load(); cfg.ImportMode != ImportModeMove {
		t.Errorf("ImportMode = %q, want %q for an unrecognized value", cfg.ImportMode, ImportModeMove)
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

	if cfg := Load(); cfg.ImportMode != ImportModeMove {
		t.Errorf("ImportMode = %q, want %q for an unrecognized settings value", cfg.ImportMode, ImportModeMove)
	}
}
