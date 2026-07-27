//go:build live

// This file is excluded from the default build. It holds tests that hit the
// real network and are meant to run on a schedule (or manually), never as part
// of `go test ./...`. Run with:
//
//	go test -tags live ./internal/sources/ -run TestLive -v
package sources_test

import (
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/sources"
)

// TestLiveDefaultRegistry fetches the registry from the real default URL the
// production binary uses and asserts it is reachable and complete.
//
// Unlike TestCanonicalRegistryIsComplete (which validates the in-repo fixture),
// this catches drift in the live librarr-sources companion repo: the file
// moving or being removed, a non-2xx response, malformed JSON, or a schema
// change that leaves fields the code depends on empty. Any of those degrades
// production to an empty registry — "search returns no results" — with no other
// signal, so this canary runs on a schedule to surface it early.
//
// Empty path and URL force sources.Load down the default-URL fetch path, and a
// fresh temp cache dir guarantees a real network fetch rather than a cached hit.
func TestLiveDefaultRegistry(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "sources-cache.json")
	r := sources.Load("", "", cache)

	if r.Version <= 0 {
		t.Fatalf("live registry has no version (%d) — the default URL is likely unreachable, "+
			"returned a non-2xx, or served malformed JSON", r.Version)
	}

	// Mirror the field set from TestCanonicalRegistryIsComplete so live drift is
	// held to the same completeness bar as the fixture.
	checks := map[string]bool{
		"Annas.Domain":          r.Annas.Domain == "",
		"LibgenMirrors":         len(r.LibgenMirrors) == 0,
		"AudioBookBay.Mirrors":  len(r.AudioBookBay.Mirrors) == 0,
		"AudioBookBay.Trackers": len(r.AudioBookBay.Trackers) == 0,
		"WebNovels":             len(r.WebNovels) == 0,
		"Gutenberg.URL":         r.Gutenberg.URL == "",
		"MangaDex.APIURL":       r.MangaDex.APIURL == "",
		"MangaDex.UploadsURL":   r.MangaDex.UploadsURL == "",
		"MangaDex.WebURL":       r.MangaDex.WebURL == "",
		"OpenLibrary.SearchURL": r.OpenLibrary.SearchURL == "",
		"OpenLibrary.CoverURL":  r.OpenLibrary.CoverURL == "",
		"Librivox.URL":          r.Librivox.URL == "",
		"StandardEbooks.URL":    r.StandardEbooks.URL == "",
		"Nyaa.URL":              r.Nyaa.URL == "",
		"ThePirateBay.URL":      r.ThePirateBay.URL == "",
		"ZLibraryDefault":       r.ZLibraryDefault == "",
	}
	for field, empty := range checks {
		if empty {
			t.Errorf("live registry: %s is empty (schema drift in librarr-sources?)", field)
		}
	}
}
