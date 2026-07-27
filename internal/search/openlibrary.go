package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// OpenLibrary searches a public-domain catalog over Open Library's JSON API.
// Search and cover endpoints come from the runtime sources registry.
type OpenLibrary struct {
	cfg    *config.Config
	client *http.Client
}

// NewOpenLibrary creates an Open Library searcher.
func NewOpenLibrary(cfg *config.Config, client *http.Client) *OpenLibrary {
	return &OpenLibrary{cfg: cfg, client: client}
}

func (o *OpenLibrary) Name() string         { return "openlibrary" }
func (o *OpenLibrary) Label() string        { return "Open Library" }
func (o *OpenLibrary) Enabled() bool        { return true }
func (o *OpenLibrary) SearchTab() string    { return "main" }
func (o *OpenLibrary) DownloadType() string { return "direct" }

func (o *OpenLibrary) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", o.cfg.Sources.OpenLibrary.SearchURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("q", query)
	q.Set("fields", "key,title,author_name,ebook_access,ia,first_publish_year,cover_i")
	q.Set("limit", "15")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "Librarr/2.0 (book download manager; github.com/jamie75/librarr)")

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("open library HTTP %d", resp.StatusCode)
	}

	var data olResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var results []models.SearchResult
	for _, doc := range data.Docs {
		// Only include public domain books with Internet Archive identifiers.
		if doc.EbookAccess != "public" {
			continue
		}
		if len(doc.IA) == 0 {
			continue
		}

		author := ""
		if len(doc.AuthorName) > 0 {
			author = doc.AuthorName[0]
		}

		sizeHuman := "Public Domain"
		if doc.FirstPublishYear > 0 {
			sizeHuman = fmt.Sprintf("Public Domain (%d)", doc.FirstPublishYear)
		}

		coverURL := ""
		if doc.CoverI > 0 {
			coverURL = fmt.Sprintf("%s/b/id/%d-M.jpg", o.cfg.Sources.OpenLibrary.CoverURL, doc.CoverI)
		}

		iaIDs := doc.IA
		if len(iaIDs) > 5 {
			iaIDs = iaIDs[:5]
		}

		results = append(results, models.SearchResult{
			Source:    "openlibrary",
			Title:     doc.Title,
			Author:    author,
			SourceID:  fmt.Sprintf("ol-%s", doc.Key),
			IAIDs:     iaIDs,
			CoverURL:  coverURL,
			SizeHuman: sizeHuman,
		})
	}

	return results, nil
}

type olResponse struct {
	Docs []olDoc `json:"docs"`
}

type olDoc struct {
	Key              string   `json:"key"`
	Title            string   `json:"title"`
	AuthorName       []string `json:"author_name"`
	EbookAccess      string   `json:"ebook_access"`
	IA               []string `json:"ia"`
	FirstPublishYear int      `json:"first_publish_year"`
	CoverI           int      `json:"cover_i"`
}
