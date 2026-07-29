package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// Prowlarr searches via the Prowlarr API for torrent indexers.
type Prowlarr struct {
	cfg    *config.Config
	client *http.Client
	tab    string // "main", "audiobook", or "manga"
}

// NewProwlarr creates a Prowlarr searcher for the given tab.
func NewProwlarr(cfg *config.Config, client *http.Client, tab string) *Prowlarr {
	return &Prowlarr{cfg: cfg, client: client, tab: tab}
}

func (p *Prowlarr) Name() string {
	switch p.tab {
	case "audiobook":
		return "prowlarr_audiobooks"
	case "manga":
		return "prowlarr_manga"
	default:
		return "prowlarr"
	}
}

func (p *Prowlarr) Label() string {
	switch p.tab {
	case "audiobook":
		return "Prowlarr (Audiobooks)"
	case "manga":
		return "Prowlarr (Manga)"
	default:
		return "Prowlarr"
	}
}

func (p *Prowlarr) Enabled() bool        { return p.cfg.HasProwlarr() }
func (p *Prowlarr) SearchTab() string    { return p.tab }
func (p *Prowlarr) DownloadType() string { return "torrent" }

func (p *Prowlarr) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	categories := p.categoriesForTab()
	var allResults []models.SearchResult
	seenHashes := make(map[string]bool)

	for _, search := range p.searchVariants(query, categories) {
		results, err := p.doSearch(ctx, search)
		if err != nil {
			slog.Warn("prowlarr search variant failed", "tab", p.tab, "error", err)
			continue
		}
		for _, r := range results {
			if r.InfoHash != "" {
				if seenHashes[r.InfoHash] {
					continue
				}
				seenHashes[r.InfoHash] = true
			}
			allResults = append(allResults, r)
		}
	}

	return allResults, nil
}

func (p *Prowlarr) categoriesForTab() []string {
	switch p.tab {
	case "audiobook":
		return []string{"3030"}
	case "manga":
		return []string{"7020", "7030"}
	default:
		return []string{"7000", "7020"}
	}
}

type prowlarrSearchParams struct {
	query      string
	categories []string
	limit      int
}

func (p *Prowlarr) searchVariants(query string, categories []string) []prowlarrSearchParams {
	switch p.tab {
	case "audiobook":
		return []prowlarrSearchParams{
			{query: query, categories: categories, limit: 50},
			{query: query + " audiobook", categories: nil, limit: 30},
		}
	case "manga":
		return []prowlarrSearchParams{
			{query: query, categories: categories, limit: 50},
			{query: query + " manga", categories: nil, limit: 30},
		}
	default:
		return []prowlarrSearchParams{
			{query: query, categories: categories, limit: 50},
		}
	}
}

func (p *Prowlarr) doSearch(ctx context.Context, params prowlarrSearchParams) ([]models.SearchResult, error) {
	url := fmt.Sprintf("%s/api/v1/search", p.cfg.ProwlarrURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("query", params.query)
	q.Set("type", "search")
	q.Set("limit", fmt.Sprintf("%d", params.limit))
	for _, cat := range params.categories {
		q.Add("categories", cat)
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Api-Key", p.cfg.ProwlarrAPIKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("prowlarr HTTP %d", resp.StatusCode)
	}

	// Read body first so we can diagnose HTML error pages (which happen when
	// the user's API key is wrong, a reverse proxy is intercepting the request,
	// or Prowlarr is starting up and serving a placeholder page).
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read prowlarr response: %w", err)
	}

	trimmed := strings.TrimSpace(string(bodyBytes))
	if strings.HasPrefix(trimmed, "<") {
		ct := resp.Header.Get("Content-Type")
		preview := trimmed
		if len(preview) > 200 {
			preview = preview[:200]
		}
		return nil, fmt.Errorf("prowlarr returned HTML instead of JSON (content-type=%q, likely wrong API key or reverse proxy issue): %s", ct, preview)
	}

	var items []prowlarrItem
	if err := json.Unmarshal(bodyBytes, &items); err != nil {
		return nil, fmt.Errorf("decode prowlarr response: %w", err)
	}

	var results []models.SearchResult
	for _, item := range items {
		source := "torrent"
		if p.tab == "audiobook" {
			source = "prowlarr_audiobooks"
		} else if p.tab == "manga" {
			source = "prowlarr_manga"
		}

		// Detect NZB vs torrent based on download URL.
		protocol := "torrent"
		if isNZBURL(item.DownloadURL) || item.Protocol == "usenet" {
			protocol = "nzb"
		}
		format := extractFormatFromTitle(item.Title)
		if format == "" {
			format = extractFormatFromTitle(item.FileType)
		}

		results = append(results, models.SearchResult{
			Source:           source,
			Title:            item.Title,
			Author:           item.Author(),
			Size:             int64(item.Size),
			SizeHuman:        HumanSize(int64(item.Size)),
			Seeders:          item.Seeders,
			Leechers:         item.Leechers,
			Grabs:            item.Grabs,
			Indexer:          item.Indexer,
			DownloadURL:      item.DownloadURL,
			MagnetURL:        item.MagnetURL,
			InfoHash:         item.InfoHash,
			GUID:             item.GUID,
			DownloadProtocol: protocol,
			Format:           format,
			Language:         firstProwlarrLanguage(item.Language, item.Languages),
			PublishDate:      item.PublishDate,
			Categories:       prowlarrCategories(item.Categories),
		})
	}
	slog.Info("prowlarr search response parsed",
		"tab", p.tab,
		"query", params.query,
		"raw_items", len(items),
		"normalized_results", len(results),
	)

	return results, nil
}

type prowlarrItem struct {
	Title       string              `json:"title"`
	AuthorName  string              `json:"author"`
	AuthorInfo  string              `json:"author_info"`
	FileType    string              `json:"filetype"`
	Size        prowlarrSizeBytes   `json:"size"`
	Seeders     int                 `json:"seeders"`
	Leechers    int                 `json:"leechers"`
	Grabs       int                 `json:"grabs"`
	Indexer     string              `json:"indexer"`
	DownloadURL string              `json:"downloadUrl"`
	MagnetURL   string              `json:"magnetUrl"`
	InfoHash    string              `json:"infoHash"`
	GUID        string              `json:"guid"`
	Protocol    string              `json:"protocol"`
	PublishDate string              `json:"publishDate"`
	Language    string              `json:"language"`
	Languages   []string            `json:"languages"`
	Categories  prowlarrCategoryIDs `json:"categories"`
}

func (p prowlarrItem) Author() string {
	if strings.TrimSpace(p.AuthorName) != "" {
		return strings.TrimSpace(p.AuthorName)
	}
	var authorInfo map[string]string
	if err := json.Unmarshal([]byte(p.AuthorInfo), &authorInfo); err == nil {
		for _, author := range authorInfo {
			if strings.TrimSpace(author) != "" {
				return strings.TrimSpace(author)
			}
		}
	}
	return ""
}

type prowlarrCategoryIDs []int

type prowlarrSizeBytes int64

func (s *prowlarrSizeBytes) UnmarshalJSON(data []byte) error {
	var number int64
	if err := json.Unmarshal(data, &number); err == nil {
		*s = prowlarrSizeBytes(number)
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*s = prowlarrSizeBytes(parseSizeBytes(text))
		return nil
	}
	return fmt.Errorf("unsupported prowlarr size shape")
}

func (c *prowlarrCategoryIDs) UnmarshalJSON(data []byte) error {
	var ints []int
	if err := json.Unmarshal(data, &ints); err == nil {
		*c = ints
		return nil
	}

	var objects []struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(data, &objects); err == nil {
		out := make([]int, 0, len(objects))
		for _, object := range objects {
			if object.ID != 0 {
				out = append(out, object.ID)
			}
		}
		*c = out
		return nil
	}

	var encoded string
	if err := json.Unmarshal(data, &encoded); err == nil {
		if strings.TrimSpace(encoded) == "" {
			*c = nil
			return nil
		}
		return c.UnmarshalJSON([]byte(encoded))
	}

	return fmt.Errorf("unsupported prowlarr categories shape")
}

func firstProwlarrLanguage(language string, languages []string) string {
	if len(languages) > 1 {
		return "multi"
	}
	if strings.TrimSpace(language) != "" {
		return normalizeProwlarrLanguage(language)
	}
	for _, candidate := range languages {
		if strings.TrimSpace(candidate) != "" {
			return normalizeProwlarrLanguage(candidate)
		}
	}
	return ""
}

func normalizeProwlarrLanguage(language string) string {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "en", "eng", "english":
		return "en"
	default:
		return strings.TrimSpace(language)
	}
}

func prowlarrCategories(categories []int) []string {
	out := make([]string, 0, len(categories))
	for _, category := range categories {
		out = append(out, fmt.Sprintf("%d", category))
	}
	return out
}

// isNZBURL checks if a URL points to an NZB download.
func isNZBURL(u string) bool {
	lower := strings.ToLower(u)
	return strings.HasSuffix(lower, ".nzb") ||
		strings.Contains(lower, "/nzb/") ||
		strings.Contains(lower, "nzb?") ||
		strings.Contains(lower, "&t=get&") // newznab API pattern
}

// HumanSize converts bytes to a human-readable string.
func HumanSize(b int64) string {
	if b == 0 {
		return "0 B"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(b)/float64(div), units[exp])
}
