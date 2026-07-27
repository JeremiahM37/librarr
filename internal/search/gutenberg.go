package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// Gutenberg searches a Gutendex-compatible public-domain ebook catalog.
// Endpoint comes from the runtime sources registry.
type Gutenberg struct {
	cfg    *config.Config
	client *http.Client
}

// NewGutenberg creates a Gutenberg searcher.
func NewGutenberg(cfg *config.Config, client *http.Client) *Gutenberg {
	return &Gutenberg{cfg: cfg, client: client}
}

func (g *Gutenberg) Name() string         { return "gutenberg" }
func (g *Gutenberg) Label() string        { return "Project Gutenberg" }
func (g *Gutenberg) Enabled() bool        { return true }
func (g *Gutenberg) SearchTab() string    { return "main" }
func (g *Gutenberg) DownloadType() string { return "direct" }

func (g *Gutenberg) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", g.cfg.Sources.Gutenberg.URL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("search", query)
	q.Set("languages", "en")
	q.Set("mime_type", "application/epub+zip")
	req.URL.RawQuery = q.Encode()

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("gutendex HTTP %d", resp.StatusCode)
	}

	var data gutendexResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var results []models.SearchResult
	for _, book := range data.Results {
		if len(results) >= 10 {
			break
		}

		epubURL, ok := book.Formats["application/epub+zip"]
		if !ok || epubURL == "" {
			continue
		}

		author := ""
		if len(book.Authors) > 0 {
			author = book.Authors[0].Name
			// Flip "Last, First" to "First Last".
			if parts := strings.SplitN(author, ", ", 2); len(parts) == 2 {
				author = parts[1] + " " + parts[0]
			}
		}

		coverURL, _ := book.Formats["image/jpeg"]

		results = append(results, models.SearchResult{
			Source:        "gutenberg",
			Title:         book.Title,
			Author:        author,
			SourceID:      fmt.Sprintf("gutenberg-%d", book.ID),
			GutenbergID:   book.ID,
			EpubURL:       epubURL,
			Format:        "epub",
			CoverURL:      coverURL,
			SizeHuman:     "Public Domain",
			DownloadCount: book.DownloadCount,
		})
	}

	return results, nil
}

type gutendexResponse struct {
	Results []gutendexBook `json:"results"`
}

type gutendexBook struct {
	ID            int               `json:"id"`
	Title         string            `json:"title"`
	Authors       []gutendexAuthor  `json:"authors"`
	Formats       map[string]string `json:"formats"`
	DownloadCount int               `json:"download_count"`
}

type gutendexAuthor struct {
	Name string `json:"name"`
}
