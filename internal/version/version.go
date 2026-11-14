// Package version exposes the Librarr release version.
//
// The version is the single source of truth in the VERSION file in this
// directory, embedded at build time. This keeps the reported version correct
// for every build path (go build, go run, and the container image) without
// relying on -ldflags being passed. Bump the VERSION file when cutting a
// release.
package version

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var raw string

// Suffix is appended to the release version for unreleased builds, such as
// the canary image, which sets it with
// -ldflags "-X github.com/JeremiahM37/librarr/internal/version.Suffix=-canary+<sha>".
// It is empty for releases.
var Suffix string

// Version is the current Librarr release.
var Version = strings.TrimSpace(raw) + Suffix
