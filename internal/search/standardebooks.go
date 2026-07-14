package search

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
)

// StandardEbooks searches the Standard Ebooks OPDS catalog.
type StandardEbooks struct {
	cfg    *config.Config
	client *http.Client
}

func NewStandardEbooks(cfg *config.Config, client *http.Client) *StandardEbooks {
	return &StandardEbooks{cfg: cfg, client: client}
}

func (s *StandardEbooks) Name() string         { return "standardebooks" }
func (s *StandardEbooks) Label() string        { return "Standard Ebooks" }
func (s *StandardEbooks) Enabled() bool        { return true }
func (s *StandardEbooks) SearchTab() string    { return "main" }
func (s *StandardEbooks) DownloadType() string { return "direct" }

var (
	seEntryRe  = regexp.MustCompile(`(?s)<entry>(.*?)</entry>`)
	seTitleRe  = regexp.MustCompile(`(?s)<title[^>]*>(.*?)</title>`)
	seAuthorRe = regexp.MustCompile(`(?s)<author[^>]*>.*?<name>(.*?)</name>`)
	seIDRe     = regexp.MustCompile(`<id>(.*?)</id>`)
	seCoverRe  = regexp.MustCompile(`rel="http://opds-spec\.org/image"[^>]*href="([^"]+)"`)
	seTagRe    = regexp.MustCompile(`<[^>]+>`)
)

func (s *StandardEbooks) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	// Use the search-enabled OPDS endpoint — /feeds/opds/all?query= returns Atom XML.
	// Base URL comes from the runtime sources registry.
	base := s.cfg.Sources.StandardEbooks.URL
	u := fmt.Sprintf("%s/feeds/opds/all?query=%s", base, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Librarr/2.0)")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Standard Ebooks HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	content := string(body)

	entries := seEntryRe.FindAllStringSubmatch(content, -1)

	// Build query word set.
	qWords := extractWords(strings.ToLower(query))

	var results []models.SearchResult
	for _, entry := range entries {
		if len(results) >= 15 {
			break
		}
		entryText := entry[1]

		titleMatch := seTitleRe.FindStringSubmatch(entryText)
		idMatch := seIDRe.FindStringSubmatch(entryText)
		if titleMatch == nil || idMatch == nil {
			continue
		}

		title := strings.TrimSpace(seTagRe.ReplaceAllString(titleMatch[1], ""))
		bookID := strings.TrimSpace(idMatch[1])

		author := ""
		if authorMatch := seAuthorRe.FindStringSubmatch(entryText); authorMatch != nil {
			author = strings.TrimSpace(seTagRe.ReplaceAllString(authorMatch[1], ""))
		}

		coverURL := ""
		if coverMatch := seCoverRe.FindStringSubmatch(entryText); coverMatch != nil {
			coverURL = coverMatch[1]
		}

		// Relevance check: query words must overlap with title+author.
		combined := strings.ToLower(title + " " + author)
		combinedWords := extractWords(combined)
		overlap := 0
		for w := range qWords {
			if combinedWords[w] {
				overlap++
			}
		}
		if len(qWords) > 0 && overlap == 0 {
			continue
		}

		// Derive EPUB URL using the configured base.
		seURL := bookID
		if !strings.HasPrefix(seURL, "http") {
			seURL = base + bookID
		}
		path := strings.TrimPrefix(seURL, base+"/ebooks/")
		epubURL := fmt.Sprintf("%s/ebooks/%s/downloads/%s.epub",
			base, path, strings.ReplaceAll(path, "/", "_"))

		results = append(results, models.SearchResult{
			Source:      "standardebooks",
			Title:       title,
			Author:      author,
			SizeHuman:   "~1 MB",
			CoverURL:    coverURL,
			SourceID:    fmt.Sprintf("standardebooks-%s", path),
			EpubURL:     epubURL,
			DownloadURL: epubURL,
			Format:      "epub",
		})
	}

	return results, nil
}
