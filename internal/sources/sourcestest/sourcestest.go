// Package sourcestest is a test-only helper that exposes the canonical
// librarr-sources registry as Go bytes/types. The production binary does not
// embed the registry — Librarr fetches it at runtime — but tests across the
// repo need a populated registry without hitting the network.
//
// Import this only from test files.
package sourcestest

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/jamie75/librarr/internal/sources"
)

//go:embed sources.json
var canonicalJSON []byte

// CanonicalJSON returns the raw bytes of the canonical registry. Useful for
// tests that want to serve it over an httptest server.
func CanonicalJSON() []byte {
	out := make([]byte, len(canonicalJSON))
	copy(out, canonicalJSON)
	return out
}

// Registry returns the canonical registry decoded into a *sources.Registry.
func Registry() (*sources.Registry, error) {
	var r sources.Registry
	if err := json.Unmarshal(canonicalJSON, &r); err != nil {
		return nil, fmt.Errorf("decode canonical sources registry: %w", err)
	}
	return &r, nil
}
