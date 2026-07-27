package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// tpbDefaultTrackers are the trackers appended to magnet URLs so qBittorrent
// can find peers (info_hash alone has no trackers). Used when the runtime
// registry has no override.
var tpbDefaultTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://open.stealth.si:80/announce",
	"udp://exodus.desync.com:6969/announce",
	"udp://tracker.coppersurfer.tk:6969/announce",
}

// buildMagnet constructs a magnet URL with well-known trackers appended so
// the torrent can find peers without further metadata fetch.
func buildMagnet(infoHash, displayName string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "magnet:?xt=urn:btih:%s&dn=%s", infoHash, url.QueryEscape(displayName))
	for _, tr := range tpbDefaultTrackers {
		b.WriteString("&tr=")
		b.WriteString(url.QueryEscape(tr))
	}
	return b.String()
}

// ThePirateBay searches ThePirateBay via the apibay.org JSON API.
// It supports both ebook (cat 601) and audiobook (cat 102) categories.
type ThePirateBay struct {
	cfg    *config.Config
	client *http.Client
	tab    string // "main" or "audiobook"
}

// NewThePirateBay creates a new ThePirateBay searcher for the given tab.
func NewThePirateBay(cfg *config.Config, client *http.Client, tab string) *ThePirateBay {
	return &ThePirateBay{cfg: cfg, client: client, tab: tab}
}

// apiBase returns the runtime-configured API endpoint for this driver.
func (t *ThePirateBay) apiBase() string {
	return t.cfg.Sources.ThePirateBay.URL
}

func (t *ThePirateBay) Name() string {
	if t.tab == "audiobook" {
		return "tpb_audiobook"
	}
	return "tpb"
}

func (t *ThePirateBay) Label() string { return "ThePirateBay" }

func (t *ThePirateBay) Enabled() bool { return t.cfg.TPBEnabled }

func (t *ThePirateBay) SearchTab() string { return t.tab }

func (t *ThePirateBay) DownloadType() string { return "torrent" }

// categoriesForTab returns the TPB category number for the configured tab.
func (t *ThePirateBay) categoriesForTab() []string {
	switch t.tab {
	case "audiobook":
		return []string{"102"} // Audio books
	default:
		return []string{"601"} // E-books
	}
}

// tpbItem represents a single result from the apibay.org API.
type tpbItem struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	InfoHash string `json:"info_hash"`
	Leechers string `json:"leechers"`
	Seeders  string `json:"seeders"`
	Size     string `json:"size"` // size in bytes as string
	NumFiles string `json:"num_files"`
	Username string `json:"username"`
	Added    string `json:"added"`
	Category string `json:"category"`
}

func (t *ThePirateBay) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	categories := t.categoriesForTab()
	var allResults []models.SearchResult

	for _, cat := range categories {
		results, err := t.searchCategory(ctx, query, cat)
		if err != nil {
			slog.Warn("TPB search failed for category", "category", cat, "error", err)
			continue
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

func (t *ThePirateBay) searchCategory(ctx context.Context, query, category string) ([]models.SearchResult, error) {
	u, err := url.Parse(t.apiBase() + "/q.php")
	if err != nil {
		return nil, fmt.Errorf("parse TPB URL: %w", err)
	}
	q := u.Query()
	q.Set("q", query)
	q.Set("cat", category)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", t.cfg.UserAgent)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("TPB request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("TPB HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// apibay returns JSON array, but may return [0] on no results
	var items []tpbItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("parse TPB JSON: %w", err)
	}

	sourceName := t.Name()
	mediaType := "ebook"
	if t.tab == "audiobook" {
		mediaType = "audiobook"
	}

	var results []models.SearchResult
	seenHashes := make(map[string]bool)

	for _, item := range items {
		if len(results) >= 20 {
			break
		}

		name := strings.TrimSpace(item.Name)
		if name == "" || item.InfoHash == "" {
			continue
		}

		// Skip duplicates by info hash.
		if seenHashes[item.InfoHash] {
			continue
		}
		seenHashes[item.InfoHash] = true

		seeders, _ := strconv.Atoi(item.Seeders)
		if seeders < 1 {
			continue
		}

		// Skip suspicious titles.
		if IsSuspicious(name) {
			continue
		}

		sizeBytes, _ := strconv.ParseInt(item.Size, 10, 64)
		sizeHuman := HumanSize(sizeBytes)
		leechers, _ := strconv.Atoi(item.Leechers)

		magnetURL := buildMagnet(item.InfoHash, name)

		results = append(results, models.SearchResult{
			Source:      sourceName,
			Title:       name,
			DownloadURL: magnetURL,
			MagnetURL:   magnetURL,
			InfoHash:    item.InfoHash,
			Seeders:     seeders,
			Leechers:    leechers,
			Size:        sizeBytes,
			SizeHuman:   sizeHuman,
			Indexer:     "ThePirateBay",
			MediaType:   mediaType,
			Format:      extractFormatFromTitle(name),
		})
	}

	return results, nil
}
