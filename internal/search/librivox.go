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

// Librivox searches a Librivox-style public-domain audiobook feed. Endpoint
// comes from the runtime sources registry.
type Librivox struct {
	cfg    *config.Config
	client *http.Client
}

func NewLibrivox(cfg *config.Config, client *http.Client) *Librivox {
	return &Librivox{cfg: cfg, client: client}
}

func (l *Librivox) Name() string         { return "librivox" }
func (l *Librivox) Label() string        { return "Librivox" }
func (l *Librivox) Enabled() bool        { return true }
func (l *Librivox) SearchTab() string    { return "audiobook" }
func (l *Librivox) DownloadType() string { return "direct" }

type librivoxResponse struct {
	Books []librivoxBook `json:"books"`
}

type librivoxBook struct {
	ID            string           `json:"id"`
	Title         string           `json:"title"`
	Authors       []librivoxAuthor `json:"authors"`
	TotalTime     string           `json:"totaltime"`
	NumSections   string           `json:"num_sections"`
	URLZipFile    string           `json:"url_zip_file"`
	URLLibrivox   string           `json:"url_librivox"`
	CoverArtThumb string           `json:"coverart_thumbnail"`
	CoverArtJPG   string           `json:"coverart_jpg"`
}

type librivoxAuthor struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func (l *Librivox) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	var results []models.SearchResult

	for _, field := range []string{"title", "author"} {
		req, err := http.NewRequestWithContext(ctx, "GET", l.cfg.Sources.Librivox.URL, nil)
		if err != nil {
			continue
		}

		q := req.URL.Query()
		q.Set(field, query)
		q.Set("format", "json")
		q.Set("extended", "1")
		q.Set("coverart", "1")
		q.Set("limit", "15")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("User-Agent", "Librarr/2.0 (book download manager)")

		resp, err := l.client.Do(req)
		if err != nil {
			continue
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}

		var data librivoxResponse
		err = json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()
		if err != nil || len(data.Books) == 0 {
			continue
		}

		for _, book := range data.Books {
			if book.URLZipFile == "" {
				continue
			}

			var authorParts []string
			for _, a := range book.Authors {
				name := strings.TrimSpace(a.FirstName + " " + a.LastName)
				if name != "" {
					authorParts = append(authorParts, name)
				}
			}
			author := strings.Join(authorParts, ", ")

			cover := book.CoverArtThumb
			if cover == "" {
				cover = book.CoverArtJPG
			}

			sizeHuman := book.TotalTime
			if sizeHuman == "" {
				sizeHuman = "Public Domain"
			}

			results = append(results, models.SearchResult{
				Source:      "librivox",
				Title:       book.Title,
				Author:      author,
				SourceID:    fmt.Sprintf("librivox-%s", book.ID),
				DownloadURL: book.URLZipFile,
				URL:         book.URLLibrivox,
				CoverURL:    cover,
				SizeHuman:   sizeHuman,
				MediaType:   "audiobook",
				Format:      "zip",
			})
		}

		// If title search got results, skip author search.
		if len(results) > 0 {
			break
		}
	}

	return results, nil
}
