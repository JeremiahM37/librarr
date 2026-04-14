package search

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
)

// Flibusta searches the Flibusta OPDS catalog for Russian-language books.
type Flibusta struct {
	cfg    *config.Config
	client *http.Client
}

// NewFlibusta creates a new Flibusta OPDS searcher.
func NewFlibusta(cfg *config.Config, client *http.Client) *Flibusta {
	return &Flibusta{cfg: cfg, client: client}
}

func (f *Flibusta) Name() string        { return "flibusta" }
func (f *Flibusta) Label() string        { return "Flibusta" }
func (f *Flibusta) Enabled() bool        { return f.cfg.FlibustaEnabled && f.cfg.FlibustaURL != "" }
func (f *Flibusta) SearchTab() string    { return "main" }
func (f *Flibusta) DownloadType() string { return "direct" }

var (
	flEntryRe   = regexp.MustCompile(`(?s)<entry>(.*?)</entry>`)
	flTitleRe   = regexp.MustCompile(`(?s)<title[^>]*>(.*?)</title>`)
	flAuthorRe  = regexp.MustCompile(`(?s)<author[^>]*>.*?<name>(.*?)</name>`)
	flLinkRe    = regexp.MustCompile(`<link[^>]*rel="http://opds-spec\.org/acquisition"[^>]*href="([^"]+)"[^>]*type="([^"]+)"[^>]*/?>`)
	flLinkRe2   = regexp.MustCompile(`<link[^>]*type="([^"]+)"[^>]*href="([^"]+)"[^>]*rel="http://opds-spec\.org/acquisition"[^>]*/?>`)
	flCoverRe   = regexp.MustCompile(`<link[^>]*rel="http://opds-spec\.org/image"[^>]*href="([^"]+)"`)
	flCoverRe2  = regexp.MustCompile(`<link[^>]*href="([^"]+)"[^>]*rel="http://opds-spec\.org/image"`)
	flTagRe     = regexp.MustCompile(`<[^>]+>`)
	flContentRe = regexp.MustCompile(`(?s)<content[^>]*>(.*?)</content>`)
	flSizeRe    = regexp.MustCompile(`(\d+[\.\d]*\s*[КKМMG]?[iI]?[БBb]?)`)
	flBookIDRe  = regexp.MustCompile(`/b/(\d+)`)
)

// formatPriority defines the preferred download format order.
var formatPriority = map[string]int{
	"application/epub+zip":             1,
	"application/epub":                 2,
	"application/fb2+zip":              3,
	"application/fb2":                  4,
	"application/x-mobipocket-ebook":   5,
	"application/pdf":                  6,
	"text/html":                        7,
	"text/plain":                       8,
}

func (f *Flibusta) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	baseURL := strings.TrimRight(f.cfg.FlibustaURL, "/")
	searchURL := fmt.Sprintf("%s/opds/search?searchType=books&searchTerm=%s",
		baseURL, url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.cfg.UserAgent)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("flibusta request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("flibusta HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("flibusta read body: %w", err)
	}
	content := string(body)

	entries := flEntryRe.FindAllStringSubmatch(content, -1)

	var results []models.SearchResult
	for _, entry := range entries {
		if len(results) >= 20 {
			break
		}
		entryText := entry[1]

		// Title
		titleMatch := flTitleRe.FindStringSubmatch(entryText)
		if titleMatch == nil {
			continue
		}
		title := strings.TrimSpace(flTagRe.ReplaceAllString(titleMatch[1], ""))
		if title == "" {
			continue
		}

		// Author
		author := ""
		if authorMatch := flAuthorRe.FindStringSubmatch(entryText); authorMatch != nil {
			author = strings.TrimSpace(flTagRe.ReplaceAllString(authorMatch[1], ""))
		}

		// Book ID from entry (needed for SourceID and download links).
		bookID := ""
		if idMatch := flBookIDRe.FindStringSubmatch(entryText); idMatch != nil {
			bookID = idMatch[1]
		}

		// Cover URL.
		coverURL := ""
		if coverMatch := flCoverRe.FindStringSubmatch(entryText); coverMatch != nil {
			coverURL = f.resolveURL(coverMatch[1])
		} else if coverMatch := flCoverRe2.FindStringSubmatch(entryText); coverMatch != nil {
			coverURL = f.resolveURL(coverMatch[1])
		}

		// Find best download link (prefer epub).
		bestHref, bestFormat := f.bestAcquisitionLink(entryText, bookID)

		// Extract size from <content> if available.
		sizeHuman := ""
		if contentMatch := flContentRe.FindStringSubmatch(entryText); contentMatch != nil {
			sizeMatch := flSizeRe.FindStringSubmatch(flTagRe.ReplaceAllString(contentMatch[1], " "))
			if sizeMatch != nil {
				sizeHuman = sizeMatch[1]
			}
		}

		// Derive format from MIME type.
		format := formatFromMIME(bestFormat)

		results = append(results, models.SearchResult{
			Source:      "flibusta",
			Title:       title,
			Author:      author,
			SourceID:    fmt.Sprintf("flibusta-%s", bookID),
			CoverURL:    coverURL,
			URL:         bestHref,
			DownloadURL: bestHref,
			SizeHuman:   sizeHuman,
			Format:      format,
		})
	}

	slog.Debug("flibusta search complete", "query", query, "results", len(results))
	return results, nil
}

// bestAcquisitionLink finds the best download link from OPDS acquisition links.
// Prefers epub, then fb2, then mobi, then other formats.
func (f *Flibusta) bestAcquisitionLink(entryText, bookID string) (href, mime string) {
	type link struct {
		href string
		mime string
	}
	var links []link

	// Match pattern: href before type
	for _, m := range flLinkRe.FindAllStringSubmatch(entryText, -1) {
		if len(m) >= 3 {
			links = append(links, link{href: m[1], mime: m[2]})
		}
	}
	// Match pattern: type before href
	for _, m := range flLinkRe2.FindAllStringSubmatch(entryText, -1) {
		if len(m) >= 3 {
			links = append(links, link{href: m[2], mime: m[1]})
		}
	}

	if len(links) == 0 {
		// Fallback: construct download URL from book ID.
		if bookID != "" {
			return f.resolveURL(fmt.Sprintf("/b/%s/epub", bookID)), "application/epub"
		}
		return "", ""
	}

	// Pick the link with the highest format priority (lowest number).
	best := links[0]
	bestPri := 99
	for _, l := range links {
		if pri, ok := formatPriority[l.mime]; ok && pri < bestPri {
			bestPri = pri
			best = l
		}
	}

	return f.resolveURL(best.href), best.mime
}

// resolveURL converts a relative URL to absolute using the Flibusta base URL.
func (f *Flibusta) resolveURL(href string) string {
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href
	}
	baseURL := strings.TrimRight(f.cfg.FlibustaURL, "/")
	return baseURL + href
}

// formatFromMIME returns a short format string from a MIME type.
func formatFromMIME(mime string) string {
	m := strings.ToLower(mime)
	switch {
	case strings.Contains(m, "epub"):
		return "EPUB"
	case strings.Contains(m, "fb2"):
		return "FB2"
	case strings.Contains(m, "mobipocket") || strings.Contains(m, "mobi"):
		return "MOBI"
	case strings.Contains(m, "pdf"):
		return "PDF"
	case strings.Contains(m, "html"):
		return "HTML"
	case strings.Contains(m, "plain") || strings.Contains(m, "txt"):
		return "TXT"
	default:
		return ""
	}
}
