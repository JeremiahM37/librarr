package search

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

var annasMetadataFormatRe = regexp.MustCompile(`(?i)·\s*([a-z0-9]+)\s*·`)

// AnnasArchive searches Anna's Archive by scraping HTML results.
type AnnasArchive struct {
	cfg    *config.Config
	client *http.Client
}

// NewAnnasArchive creates a new Anna's Archive searcher.
func NewAnnasArchive(cfg *config.Config, client *http.Client) *AnnasArchive {
	return &AnnasArchive{cfg: cfg, client: client}
}

func (a *AnnasArchive) Name() string         { return "annas" }
func (a *AnnasArchive) Label() string        { return "Anna's Archive" }
func (a *AnnasArchive) Enabled() bool        { return a.cfg.AnnasArchiveDomain != "" }
func (a *AnnasArchive) SearchTab() string    { return "main" }
func (a *AnnasArchive) DownloadType() string { return "direct" }

func (a *AnnasArchive) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	var results []models.SearchResult
	seenMD5 := make(map[string]bool)

	// Search multiple variations for better coverage.
	searches := []struct {
		q   string
		ext string
	}{
		{query, "epub"},
		{query, ""},
	}

	for _, s := range searches {
		res, err := a.doSearch(ctx, s.q, s.ext, seenMD5)
		if err != nil {
			slog.Warn("anna's archive search variant failed", "query", s.q, "ext", s.ext, "error", err)
			continue
		}
		results = append(results, res...)
	}

	// Sort by size descending.
	sort.Slice(results, func(i, j int) bool {
		return parseSizeBytes(results[i].SizeHuman) > parseSizeBytes(results[j].SizeHuman)
	})

	if len(results) > 50 {
		results = results[:50]
	}
	return results, nil
}

func (a *AnnasArchive) doSearch(ctx context.Context, query, ext string, seenMD5 map[string]bool) ([]models.SearchResult, error) {
	baseURL := fmt.Sprintf("https://%s/search", a.cfg.AnnasArchiveDomain)

	req, err := http.NewRequestWithContext(ctx, "GET", baseURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("q", query)
	if ext != "" {
		q.Set("ext", ext)
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", a.cfg.UserAgent)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d from annas-archive", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse HTML: %w", err)
	}

	var results []models.SearchResult
	md5Re := regexp.MustCompile(`/md5/([a-f0-9]+)`)

	// Collect sizes from metadata lines. Use HTML string search because goquery .Text()
	// on divs includes all child text making them too long to filter reliably.
	// The metadata format is: "English [en] · EPUB · 0.4MB · ..."  in divs with class "leading-[1.2]"
	htmlStr, _ := doc.Html()
	metaSizeRe := regexp.MustCompile(`leading-\[1\.2\][^>]*>[^<]*?(\d+[\.\d]*[KMG]i?B)`)
	sizeMatches := metaSizeRe.FindAllStringSubmatch(htmlStr, -1)
	var metadataSizes []string
	for _, m := range sizeMatches {
		if len(m) >= 2 {
			metadataSizes = append(metadataSizes, m[1])
		}
	}
	// Collect title links (the ones with text, not image wrappers).
	resultIdx := 0
	doc.Find("a[href*='/md5/']").Each(func(_ int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists {
			return
		}
		md5Match := md5Re.FindStringSubmatch(href)
		if len(md5Match) < 2 {
			return
		}
		md5 := md5Match[1]
		title := strings.TrimSpace(s.Text())
		if title == "" {
			return
		}
		if seenMD5[md5] {
			return
		}
		seenMD5[md5] = true

		// Match with metadata size by index (they appear in the same order as results).
		sizeHuman := ""
		if resultIdx < len(metadataSizes) {
			sizeHuman = metadataSizes[resultIdx]
		}
		format := ext
		metadataText := s.Closest("div.flex").Find("div.font-semibold").Last().Text()
		if match := annasMetadataFormatRe.FindStringSubmatch(metadataText); len(match) > 1 {
			format = strings.ToLower(match[1])
		} else if format == "" {
			format = extractFormatFromTitle(title)
		}
		resultIdx++

		// Extract author from "LastName, FirstName - Title" format.
		author := ""
		if idx := strings.Index(title, " - "); idx > 0 {
			candidate := title[:idx]
			if len(candidate) < 60 && strings.Contains(candidate, ",") {
				author = candidate
				title = strings.TrimSpace(title[idx+3:])
			}
		}

		results = append(results, models.SearchResult{
			Source:    "annas",
			Title:     title,
			Author:    author,
			SizeHuman: sizeHuman,
			Format:    format,
			MD5:       md5,
			URL:       fmt.Sprintf("https://%s/md5/%s", a.cfg.AnnasArchiveDomain, md5),
		})
	})

	return results, nil
}

func parseSizeBytes(s string) float64 {
	if s == "" {
		return 0
	}
	re := regexp.MustCompile(`([\d.]+)\s*(GIB|MIB|KIB|GB|MB|KB|B)`)
	m := re.FindStringSubmatch(strings.ToUpper(s))
	if len(m) < 3 {
		return 0
	}
	val, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	switch m[2] {
	case "GIB":
		return val * 1024 * 1024 * 1024
	case "MIB":
		return val * 1024 * 1024
	case "KIB":
		return val * 1024
	case "GB":
		return val * 1e9
	case "MB":
		return val * 1e6
	case "KB":
		return val * 1e3
	default:
		return val
	}
}
