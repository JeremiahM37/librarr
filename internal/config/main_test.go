package config

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/sources/sourcestest"
)

// TestMain prepares the package test environment. It does two things:
//
//  1. Silences slog — the settings-file write-probe intentionally emits ERROR
//     logs for non-writable paths, which would otherwise spam test output.
//
//  2. Pins the sources registry to an on-disk copy of the canonical fixture
//     via LIBRARR_SOURCES_PATH. config.Load() resolves the registry through
//     sources.Load(), which — with no path or URL set — falls through to an
//     HTTP fetch of the default registry URL. Pointing it at a local file
//     keeps every Load() call in these tests hermetic, so the suite passes in
//     sandboxed builds with no network access (issue #73). The production
//     binary still fetches the registry at runtime; only the tests are pinned.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(runWithPinnedSources(m))
}

// runWithPinnedSources writes the canonical registry fixture to a temp file,
// points LIBRARR_SOURCES_PATH at it for the duration of the run, and runs the
// suite. It is a separate function so the temp-dir cleanup defer executes
// before os.Exit in TestMain.
func runWithPinnedSources(m *testing.M) int {
	dir, err := os.MkdirTemp("", "librarr-sources-fixture")
	if err != nil {
		panic("config test setup: create temp dir for sources fixture: " + err.Error())
	}
	defer os.RemoveAll(dir)

	fixture := filepath.Join(dir, "sources.json")
	if err := os.WriteFile(fixture, sourcestest.CanonicalJSON(), 0o600); err != nil {
		panic("config test setup: write sources fixture: " + err.Error())
	}

	os.Setenv("LIBRARR_SOURCES_PATH", fixture)
	os.Unsetenv("LIBRARR_SOURCES_URL")

	return m.Run()
}
