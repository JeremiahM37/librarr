package api

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Silence slog during test runs — handlers emit INFO/ERROR logs (database
// init, settings persistence) that look like noise in `go test -v` output
// but are not test failures.
//
// Hash passwords at the minimum bcrypt cost: at the default cost, hashing
// dominates the package's runtime under -race.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	bcryptCost = bcrypt.MinCost
	os.Exit(m.Run())
}
