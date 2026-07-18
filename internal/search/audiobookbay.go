package search

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/PuerkitoBio/goquery"
)

const abbBrowserUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

// AudioBookBay searches an AudioBookBay-style scrape source for audiobook torrents.
// Mirrors and trackers are loaded from the runtime sources registry.
type AudioBookBay struct {
	cfg    *config.Config
	client *http.Client
}

// NewAudioBookBay creates a new AudioBookBay searcher.
func NewAudioBookBay(cfg *config.Config, client *http.Client) *AudioBookBay {
	return &AudioBookBay{cfg: cfg, client: client}
}

func (a *AudioBookBay) domains() []string { return a.cfg.Sources.AudioBookBay.Mirrors }

func (a *AudioBookBay) Name() string         { return "audiobookbay" }
func (a *AudioBookBay) Label() string        { return "AudioBookBay" }
func (a *AudioBookBay) Enabled() bool        { return true }
func (a *AudioBookBay) SearchTab() string    { return "audiobook" }
func (a *AudioBookBay) DownloadType() string { return "torrent" }

func (a *AudioBookBay) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	for _, domain := range a.domains() {
		results, err := a.searchDomain(ctx, domain, query)
		if err != nil {
			slog.Warn("audiobookbay search failed on domain", "domain", domain, "error", err)
			continue
		}
		return results, nil
	}
	return nil, fmt.Errorf("all AudioBookBay domains failed")
}

func (a *AudioBookBay) searchDomain(ctx context.Context, domain, query string) ([]models.SearchResult, error) {
	searchURL := fmt.Sprintf("https://%s/", domain)

	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("s", query)
	q.Set("tt", "1")
	req.URL.RawQuery = q.Encode()
	setABBRequestHeaders(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("AudioBookBay HTTP %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse ABB HTML: %w", err)
	}

	var results []models.SearchResult

	// Parse search results: look for h2 > a links with /abss/ paths.
	titleRe := regexp.MustCompile(`<[^>]+>`)

	doc.Find("div.post").Each(func(_ int, post *goquery.Selection) {
		// Find the title link.
		link := post.Find("h2 a, .postTitle a")
		if link.Length() == 0 {
			return
		}

		href, exists := link.Attr("href")
		if !exists || !strings.Contains(href, "/") {
			return
		}

		title := strings.TrimSpace(titleRe.ReplaceAllString(link.Text(), ""))
		if title == "" {
			return
		}

		// Check language if present.
		infoText := post.Find(".postInfo").Text()
		if langIdx := strings.Index(strings.ToLower(infoText), "language:"); langIdx >= 0 {
			langStr := strings.TrimSpace(infoText[langIdx+9:])
			// Take first word as language.
			if spaceIdx := strings.IndexAny(langStr, " \t\n,"); spaceIdx > 0 {
				langStr = langStr[:spaceIdx]
			}
			langStr = strings.ToLower(strings.TrimSpace(langStr))
			if langStr != "" && langStr != "english" {
				return
			}
		}

		results = append(results, models.SearchResult{
			Source:    "audiobook",
			Title:     title,
			SizeHuman: "?",
			Seeders:   0,
			Leechers:  0,
			Indexer:   "AudioBookBay",
			AbbURL:    href,
		})
	})

	return results, nil
}

// ResolveABBMagnet fetches the detail page for an AudioBookBay result and extracts
// the magnet URI. mirrors and fallbackTrackers come from the runtime sources
// registry; if either is empty, the function returns an error rather than
// silently using a stale fallback.
func ResolveABBMagnet(ctx context.Context, client *http.Client, userAgent, abbPath string, mirrors, fallbackTrackers []string) (string, error) {
	if len(mirrors) == 0 {
		return "", fmt.Errorf("no AudioBookBay mirrors configured (registry not loaded?)")
	}
	if len(fallbackTrackers) == 0 {
		return "", fmt.Errorf("no AudioBookBay fallback trackers configured (registry not loaded?)")
	}
	_ = userAgent // retained for API compatibility; ABB now uses a browser-like header set.

	infoHashRe := regexp.MustCompile(`(?i)Info\s*Hash:[^\r\n]*?(?:<td[^>]*>)?\s*([0-9a-fA-F]{40})`)
	hashRe := regexp.MustCompile(`(?i)\b[0-9a-f]{40}\b`)
	trackerRe := regexp.MustCompile(`<td>((?:udp|http)://[^<]+)</td>`)
	titleRe := regexp.MustCompile(`<h1[^>]*>(.*?)</h1>`)

	for _, domain := range mirrors {
		abbURL := fmt.Sprintf("https://%s%s", domain, abbPath)

		req, err := http.NewRequestWithContext(ctx, "GET", abbURL, nil)
		if err != nil {
			continue
		}
		setABBRequestHeaders(req)

		resp, err := client.Do(req)
		if err != nil {
			continue
		}

		doc, err := goquery.NewDocumentFromReader(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		htmlContent, _ := doc.Html()
		infoHash := extractABBInfoHash(doc, hashRe)
		if infoHash == "" {
			hashMatch := infoHashRe.FindStringSubmatch(htmlContent)
			if len(hashMatch) < 2 {
				continue
			}
			infoHash = hashMatch[1]
		}

		// Extract trackers.
		trackers := trackerRe.FindAllStringSubmatch(htmlContent, -1)
		var trList []string
		for _, m := range trackers {
			trList = append(trList, m[1])
		}
		if len(trList) == 0 {
			trList = fallbackTrackers
		}

		// Build tracker params.
		var trParams []string
		for _, t := range trList {
			trParams = append(trParams, "tr="+url.QueryEscape(t))
		}

		// Extract display name.
		dn := ""
		titleMatch := titleRe.FindStringSubmatch(htmlContent)
		if len(titleMatch) >= 2 {
			cleanTitle := regexp.MustCompile(`<[^>]+>`).ReplaceAllString(titleMatch[1], "")
			dn = url.QueryEscape(strings.TrimSpace(cleanTitle))
		}

		magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s", infoHash)
		if dn != "" {
			magnet += "&dn=" + dn
		}
		if len(trParams) > 0 {
			magnet += "&" + strings.Join(trParams, "&")
		}

		return magnet, nil
	}
	return "", fmt.Errorf("failed to resolve ABB magnet from all domains")
}

func extractABBInfoHash(doc *goquery.Document, hashRe *regexp.Regexp) string {
	var infoHash string

	doc.Find("tr").EachWithBreak(func(_ int, row *goquery.Selection) bool {
		cells := row.Find("td")
		if cells.Length() < 2 {
			return true
		}

		label := strings.ToLower(strings.TrimSpace(cells.First().Text()))
		if !strings.Contains(label, "info hash") {
			return true
		}

		value := strings.TrimSpace(cells.Eq(1).Text())
		if match := hashRe.FindString(value); match != "" {
			infoHash = match
			return false
		}
		return true
	})

	return infoHash
}

func setABBRequestHeaders(req *http.Request) {
	req.Header.Set("User-Agent", abbBrowserUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Accept-Encoding", "identity")
}
