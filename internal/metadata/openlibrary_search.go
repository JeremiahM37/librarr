package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/JeremiahM37/librarr/internal/models"
)

// SearchMulti searches Open Library and returns up to limit browsable
// results, unlike FetchMetadata/searchOL which pick a single best match for
// enriching one already-known title/author pair. This is what backs the
// Discover UI's Open-Library side - added alongside the existing Client
// rather than changing its enrichment behavior.
func (c *Client) SearchMulti(ctx context.Context, query string, limit int) ([]models.DiscoverResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", olSearchAPI, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("q", query)
	q.Set("fields", "key,title,author_name,first_publish_year,cover_i,isbn,publisher,language,number_of_pages_median,subject")
	q.Set("limit", fmt.Sprintf("%d", limit))
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "Librarr/2.0 (book download manager; github.com/JeremiahM37/librarr)")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var data olSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	results := make([]models.DiscoverResult, 0, len(data.Docs))
	for _, doc := range data.Docs {
		if doc.Title == "" {
			continue
		}
		r := models.DiscoverResult{
			Source:   "open_library",
			SourceID: doc.Key,
			Title:    doc.Title,
		}
		if len(doc.AuthorName) > 0 {
			r.Author = doc.AuthorName[0]
		}
		if doc.CoverI > 0 {
			r.CoverURL = fmt.Sprintf("https://covers.openlibrary.org/b/id/%d-M.jpg", doc.CoverI)
		}
		if len(doc.ISBN) > 0 {
			r.ISBN = doc.ISBN[0]
		}
		if doc.NumberOfPagesMedian > 0 {
			r.PageCount = doc.NumberOfPagesMedian
		}
		if doc.FirstPublishYear > 0 {
			r.PublishedDate = fmt.Sprintf("%d", doc.FirstPublishYear)
		}
		results = append(results, r)
	}
	return results, nil
}

// GetWork fetches full detail (description included) for one Open Library
// work, given the key returned in a SearchMulti result's SourceID (e.g.
// "/works/OL12345W"). Reuses enrichFromWork's parsing rather than
// duplicating it.
func (c *Client) GetWork(ctx context.Context, workKey string) (*models.DiscoverResult, error) {
	meta := &BookMetadata{OLID: workKey}
	c.enrichFromWork(ctx, workKey, meta)

	return &models.DiscoverResult{
		Source:      "open_library",
		SourceID:    workKey,
		Title:       meta.Title,
		Author:      meta.Author,
		CoverURL:    meta.CoverURL,
		Description: meta.Description,
		ISBN:        meta.ISBN,
		PageCount:   meta.PageCount,
	}, nil
}
