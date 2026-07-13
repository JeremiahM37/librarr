// Package sources holds the runtime registry of indexer endpoints.
//
// The registry is loaded at startup from one of:
//
//  1. LIBRARR_SOURCES_PATH — local JSON file (takes precedence)
//  2. LIBRARR_SOURCES_URL  — HTTP(S) URL to a JSON file
//  3. The built-in default URL (the librarr-sources companion repo)
//  4. On-disk cache of the last successful fetch
//
// The production binary ships no embedded registry — Librarr matches the
// Prowlarr pattern of fetching its indexer definitions at runtime. The
// on-disk cache lets subsequent restarts work even if the network is down.
//
// Legacy per-source environment variables (ANNAS_ARCHIVE_DOMAIN, etc.) still
// take precedence over the registry value when they are set.
package sources

import (
	"net/url"
	"strings"
)

// Registry is the in-memory representation of the indexer registry.
type Registry struct {
	Version         int              `json:"version"`
	Annas           AnnasSpec        `json:"annas"`
	AudioBookBay    AudioBookBaySpec `json:"audiobookbay"`
	ThePirateBay    URLSpec          `json:"thepiratebay"`
	Gutenberg       URLSpec          `json:"gutenberg"`
	OpenLibrary     OpenLibrarySpec  `json:"openlibrary"`
	Librivox        URLSpec          `json:"librivox"`
	StandardEbooks  URLSpec          `json:"standardebooks"`
	MangaDex        MangaDexSpec     `json:"mangadex"`
	Nyaa            URLSpec          `json:"nyaa"`
	WebNovels       []WebNovelSite   `json:"webnovels"`
	LibgenMirrors   []string         `json:"libgen_mirrors"`
	ZLibraryDefault string           `json:"zlibrary_default"`
}

// AnnasSpec is the configurable endpoint for an Anna's-Archive-style direct-download driver.
type AnnasSpec struct {
	Domain string `json:"domain"`
}

// AudioBookBaySpec is the mirror + tracker list for an AudioBookBay-style scrape driver.
type AudioBookBaySpec struct {
	Mirrors  []string `json:"mirrors"`
	Trackers []string `json:"trackers"`
}

// URLSpec is a single-URL driver endpoint.
type URLSpec struct {
	URL string `json:"url"`
}

// OpenLibrarySpec splits search and cover endpoints for the Open Library driver.
type OpenLibrarySpec struct {
	SearchURL string `json:"search_url"`
	CoverURL  string `json:"cover_url"`
}

// MangaDexSpec carries the three independent endpoints used by the MangaDex driver.
type MangaDexSpec struct {
	APIURL     string `json:"api_url"`
	UploadsURL string `json:"uploads_url"`
	WebURL     string `json:"web_url"`
}

// WebNovelSite is one entry in the web-novel driver's site table.
type WebNovelSite struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	URL     string `json:"url"`
	BaseURL string `json:"base_url,omitempty"`
}

// WebNovel looks up a configured web-novel site by ID. Returns nil if absent.
func (r *Registry) WebNovel(id string) *WebNovelSite {
	for i := range r.WebNovels {
		if r.WebNovels[i].ID == id {
			return &r.WebNovels[i]
		}
	}
	return nil
}

// NormalizeDomain accepts either a hostname or URL and returns only the host.
// Source drivers that consume domain fields add their own scheme and paths.
func NormalizeDomain(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	if !strings.Contains(value, "://") {
		value = "//" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	return strings.ToLower(parsed.Host)
}

// Normalize canonicalizes host-only fields after any registry or env load.
func (r *Registry) Normalize() *Registry {
	r.Annas.Domain = NormalizeDomain(r.Annas.Domain)
	for i := range r.AudioBookBay.Mirrors {
		r.AudioBookBay.Mirrors[i] = NormalizeDomain(r.AudioBookBay.Mirrors[i])
	}
	return r
}
