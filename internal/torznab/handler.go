// Package torznab serves a Torznab-compatible API so external tools
// (e.g. Prowlarr) can query librarr as an indexer.
package torznab

import (
	"context"
	"crypto/subtle"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/search"
)

// Handler serves Torznab API endpoints.
type Handler struct {
	cfg     *config.Config
	manager *search.Manager
}

// NewHandler creates a new Torznab handler.
func NewHandler(cfg *config.Config, manager *search.Manager) *Handler {
	return &Handler{cfg: cfg, manager: manager}
}

// ServeHTTP handles GET /torznab/api requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Validate API key if configured.
	if h.cfg.TorznabAPIKey != "" {
		apikey := r.URL.Query().Get("apikey")
		if subtle.ConstantTimeCompare([]byte(apikey), []byte(h.cfg.TorznabAPIKey)) != 1 {
			h.writeError(w, "100", "Invalid API Key", http.StatusUnauthorized)
			return
		}
	}

	t := r.URL.Query().Get("t")
	switch t {
	case "caps":
		h.handleCaps(w, r)
	case "search":
		h.handleSearch(w, r, "main")
	case "book":
		h.handleSearch(w, r, "main")
	case "audio":
		h.handleSearch(w, r, "audiobook")
	case "tvsearch", "movie":
		// Not supported, but return empty results instead of error.
		h.writeEmptyResults(w)
	default:
		h.writeError(w, "202", fmt.Sprintf("No such function (%s)", t), http.StatusBadRequest)
	}
}

// ServeHTTPAlias handles the bare /api path that Prowlarr probes during
// indexer discovery (it expects a Torznab endpoint at /api, not
// /torznab/api). Same semantics as ServeHTTP — this just accepts the alias
// path. Mounted exactly on /api (not /api/...) so it doesn't shadow the
// main JSON API tree.
func (h *Handler) ServeHTTPAlias(w http.ResponseWriter, r *http.Request) {
	h.ServeHTTP(w, r)
}

func (h *Handler) handleCaps(w http.ResponseWriter, _ *http.Request) {
	caps := BuildCaps()
	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(caps)
}

func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request, tab string) {
	query := r.URL.Query().Get("q")
	if query == "" {
		// Try book-specific parameters.
		title := r.URL.Query().Get("title")
		author := r.URL.Query().Get("author")
		if title != "" {
			query = title
			if author != "" {
				query = title + " " + author
			}
		}
	}

	if query == "" {
		// Prowlarr polls `t=search` with no query during indexer discovery.
		// Returning an empty feed is valid Torznab but fails Prowlarr's
		// "Generic Torznab" validator (needs ≥1 item). Returning results
		// from all 13 sources would be slow (~10s) for a health probe.
		// Instead: emit a single placeholder item describing the endpoint.
		// - guid prefixed "librarr-placeholder-" so downstream *arr apps
		//   won't queue it for download.
		// - no MagnetURL / DownloadURL so there's nothing to grab.
		// - humans poking the URL still see a valid-looking feed.
		slog.Info("torznab empty search (health probe)", "tab", tab, "remote", r.RemoteAddr)
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		baseURL := fmt.Sprintf("%s://%s", scheme, r.Host)
		h.writeProbeResponse(w, baseURL)
		return
	}

	slog.Info("torznab search", "query", query, "tab", tab, "remote", r.RemoteAddr)

	results, _ := h.manager.Search(context.Background(), tab, query)

	// Determine the base URL for download links.
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	baseURL := fmt.Sprintf("%s://%s", scheme, r.Host)

	// Convert results to Torznab items.
	var items []models.TorznabItem
	for _, result := range results {
		items = append(items, ResultToItem(result, baseURL))
	}

	rss := models.TorznabRSS{
		Version: "2.0",
		Xmlns:   "http://torznab.com/schemas/2015/feed",
		Channel: models.TorznabChannel{
			Title:       "Librarr",
			Description: "Book search results from Librarr",
			Items:       items,
		},
	}

	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(rss)
}

// writeProbeResponse returns a single-item feed for empty search probes.
// The item is a labeled placeholder — downstream *arr apps should recognize
// the "librarr-placeholder-" guid and skip it. Humans hitting the URL see a
// valid feed with a clear explanation.
func (h *Handler) writeProbeResponse(w http.ResponseWriter, baseURL string) {
	// Prowlarr's Torznab validator rejects items missing any of:
	// - pubDate (RFC-1123)
	// - size > 0
	// - enclosure with length and type
	// - torznab:attr for category + seeders
	// All real indexer feeds include these. Without them Prowlarr says
	// "Indexer feed is not supported" or "no results returned" even with
	// a syntactically-valid item present. Shape the placeholder to match
	// a real result — the guid prefix stops downstream apps from grabbing it.
	probeURL := baseURL + "/torznab/api?t=caps"
	items := []models.TorznabItem{{
		Title:    "Librarr Torznab endpoint — pass ?q=<title> to search",
		GUID:     fmt.Sprintf("librarr-placeholder-%s", baseURL),
		Size:     1024,
		Link:     probeURL,
		Category: "7000",
		PubDate:  time.Now().UTC().Format(time.RFC1123Z),
		Enclosure: &models.TorznabEnclosure{
			URL:    probeURL,
			Length: 1024,
			Type:   "application/x-bittorrent",
		},
		Attrs: []models.TorznabAttr{
			{Name: "category", Value: "7000"},
			{Name: "seeders", Value: "0"},
			{Name: "peers", Value: "0"},
		},
	}}
	rss := models.TorznabRSS{
		Version: "2.0",
		Xmlns:   "http://torznab.com/schemas/2015/feed",
		Channel: models.TorznabChannel{
			Title:       "Librarr",
			Description: "Book search endpoint",
			Items:       items,
		},
	}
	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(rss)
}

func (h *Handler) writeEmptyResults(w http.ResponseWriter) {
	rss := models.TorznabRSS{
		Version: "2.0",
		Xmlns:   "http://torznab.com/schemas/2015/feed",
		Channel: models.TorznabChannel{
			Title:       "Librarr",
			Description: "No results",
		},
	}

	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(rss)
}

func (h *Handler) writeError(w http.ResponseWriter, code, description string, httpStatus int) {
	errResp := models.TorznabError{
		Code:        code,
		Description: description,
	}
	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(httpStatus)
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(errResp)
}
