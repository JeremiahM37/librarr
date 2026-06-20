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

// Version is the current Librarr release.
var Version = strings.TrimSpace(raw)
