package config

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// Silence slog during test runs — the new settings-file write-probe
// intentionally emits ERROR logs for non-writable paths, which would
// otherwise spam test output.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
