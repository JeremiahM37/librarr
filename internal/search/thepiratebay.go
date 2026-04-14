package search

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
)

// ThePirateBay searches ThePirateBay via the apibay.org public API.
// No authentication required. Returns magnet links.
type ThePirateBay struct {
	cfg    *config.Config
	client *http.Client
	tab    string // "main" or "audiobook"
}

// NewThePirateBay creates a new ThePirateBay searcher for the given tab.
func NewThePirateBay(cfg *config.Config, client *http.Client, tab string) *ThePirateBay {
	return &ThePirateBay{cfg: cfg, client: client, tab: tab}
}

func (t *ThePirateBay) Name() string {
	switch t.tab {
	case "audiobook":
		return "tpb_audiobook"
	default:
		return "tpb"
	}
}

func (t *ThePirateBay) Label() string {
	switch t.tab {
	case "audiobook":
		return "ThePirateBay (Audiobooks)"
	default:
		return "ThePirateBay"
	}
}

func (t *ThePirateBay) Enabled() bool        { return t.cfg.TPBEnabled }
func (t *ThePirateBay) SearchTab() string     { return t.tab }
func (t *ThePirateBay) DownloadType() string  { return "torrent" }

// TPB API category IDs.
const (
	tpbCatEbooks    = "601" // E-books
	tpbCatAudiobooks = "102" // Audio books
)

func (t *ThePirateBay) categoriesForTab() string {
	switch t.tab {
	case "audiobook":
		return tpbCatAudiobooks
	default:
		return tpbCatEbooks
	}
}

// tpbItem represents a single result from the apibay.org API.
type tpbItem struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	InfoHash string `json:"info_hash"`
	Leechers int    `json:"leechers"`
	Seeders  int    `json:"seeders"`
	Size     string `json:"size"` // in bytes as string
	NumFiles string `json:"num_files"`
	Username string `json:"username"`
	Added    string `json:"added"` // unix timestamp
	Category string `json:"category"`
}

func (t *ThePirateBay) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	cat := t.categoriesForTab()
	apiURL := fmt.Sprintf("https://apibay.org/q.php?q=%s&cat=%s",
		url.QueryEscape(query), cat)

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", t.cfg.UserAgent)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tpb API request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("tpb API HTTP %d", resp.StatusCode)
	}

	var items []tpbItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode tpb response: %w", err)
	}

	sourceName := "tpb"
	if t.tab == "audiobook" {
		sourceName = "tpb_audiobook"
	}

	var results []models.SearchResult
	seenHashes := make(map[string]bool)

	for _, item := range items {
		// Skip items with no info hash or zero seeders.
		if item.InfoHash == "" || item.Seeders < 1 {
			continue
		}

		// Dedup by info hash.
		if seenHashes[item.InfoHash] {
			continue
		}
		seenHashes[item.InfoHash] = true

		// Skip items with suspicious names.
		name := strings.TrimSpace(item.Name)
		if name == "" || IsSuspicious(name) {
			continue
		}

		// Build magnet URL.
		magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s&dn=%s",
			item.InfoHash, url.QueryEscape(name))

		// Parse size.
		sizeInt, _ := strconv.ParseInt(item.Size, 10, 64)

		results = append(results, models.SearchResult{
			Source:     sourceName,
			Title:      name,
			Size:       sizeInt,
			SizeHuman:  HumanSize(sizeInt),
			Seeders:    item.Seeders,
			Leechers:   item.Leechers,
			InfoHash:   item.InfoHash,
			MagnetURL:  magnet,
			Indexer:    "ThePirateBay",
			GUID:       fmt.Sprintf("tpb-%s", item.ID),
		})
	}

	slog.Debug("tpb search completed", "tab", t.tab, "results", len(results))
	return results, nil
}
