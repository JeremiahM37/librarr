package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

const googleBooksAPI = "https://www.googleapis.com/books/v1/volumes"

// GoogleBooksClient searches the Google Books API for browsable results.
// Distinct from Client (Open Library): this is search-oriented (multiple
// results for a free-text query, for browsing), not enrichment-oriented
// (one best match for an already-known title/author). No API key is
// required for basic volume search - GOOGLE_BOOKS_API_KEY is an optional
// quota increase, not a gate; a client with an empty key still works.
type GoogleBooksClient struct {
	httpClient *http.Client
	apiKey     string

	mu    sync.RWMutex
	cache map[string]gbCacheEntry
}

type gbCacheEntry struct {
	results   []models.DiscoverResult
	fetchedAt time.Time
}

// NewGoogleBooksClient creates a client. apiKey may be empty.
func NewGoogleBooksClient(httpClient *http.Client, apiKey string) *GoogleBooksClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &GoogleBooksClient{
		httpClient: httpClient,
		apiKey:     apiKey,
		cache:      make(map[string]gbCacheEntry),
	}
}

// Search returns up to maxResults browsable volumes matching query.
func (c *GoogleBooksClient) Search(ctx context.Context, query string, maxResults int) ([]models.DiscoverResult, error) {
	cacheKey := "search:" + strings.ToLower(strings.TrimSpace(query))
	if cached, ok := c.fromCache(cacheKey); ok {
		return cached, nil
	}

	req, err := http.NewRequestWithContext(ctx, "GET", googleBooksAPI, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("q", query)
	q.Set("maxResults", fmt.Sprintf("%d", maxResults))
	if c.apiKey != "" {
		q.Set("key", c.apiKey)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("google books HTTP %d", resp.StatusCode)
	}

	var data gbSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	results := make([]models.DiscoverResult, 0, len(data.Items))
	for _, item := range data.Items {
		results = append(results, item.toDiscoverResult())
	}

	c.toCache(cacheKey, results)
	return results, nil
}

// GetVolume fetches full detail for one volume by its Google Books ID.
func (c *GoogleBooksClient) GetVolume(ctx context.Context, id string) (*models.DiscoverResult, error) {
	cacheKey := "volume:" + id
	if cached, ok := c.fromCache(cacheKey); ok && len(cached) == 1 {
		r := cached[0]
		return &r, nil
	}

	req, err := http.NewRequestWithContext(ctx, "GET", googleBooksAPI+"/"+id, nil)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		q := req.URL.Query()
		q.Set("key", c.apiKey)
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("google books HTTP %d", resp.StatusCode)
	}

	var item gbVolume
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return nil, err
	}

	result := item.toDiscoverResult()
	c.toCache(cacheKey, []models.DiscoverResult{result})
	return &result, nil
}

func (c *GoogleBooksClient) fromCache(key string) ([]models.DiscoverResult, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.cache[key]
	if !ok || time.Since(entry.fetchedAt) >= cacheTTL {
		return nil, false
	}
	return entry.results, true
}

func (c *GoogleBooksClient) toCache(key string, results []models.DiscoverResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = gbCacheEntry{results: results, fetchedAt: time.Now()}
}

// Google Books API response types.

type gbSearchResponse struct {
	Items []gbVolume `json:"items"`
}

type gbVolume struct {
	ID         string `json:"id"`
	VolumeInfo struct {
		Title               string   `json:"title"`
		Authors             []string `json:"authors"`
		Description         string   `json:"description"`
		PublishedDate       string   `json:"publishedDate"`
		PageCount           int      `json:"pageCount"`
		Categories          []string `json:"categories"`
		IndustryIdentifiers []struct {
			Type       string `json:"type"`
			Identifier string `json:"identifier"`
		} `json:"industryIdentifiers"`
		ImageLinks struct {
			Thumbnail      string `json:"thumbnail"`
			SmallThumbnail string `json:"smallThumbnail"`
		} `json:"imageLinks"`
	} `json:"volumeInfo"`
}

func (v gbVolume) toDiscoverResult() models.DiscoverResult {
	r := models.DiscoverResult{
		Source:        "google_books",
		SourceID:      v.ID,
		Title:         v.VolumeInfo.Title,
		Description:   v.VolumeInfo.Description,
		PublishedDate: v.VolumeInfo.PublishedDate,
		PageCount:     v.VolumeInfo.PageCount,
		Categories:    v.VolumeInfo.Categories,
	}
	if len(v.VolumeInfo.Authors) > 0 {
		r.Author = strings.Join(v.VolumeInfo.Authors, ", ")
	}
	// Prefer ISBN-13, fall back to ISBN-10.
	for _, id := range v.VolumeInfo.IndustryIdentifiers {
		if id.Type == "ISBN_13" {
			r.ISBN = id.Identifier
			break
		}
		if id.Type == "ISBN_10" && r.ISBN == "" {
			r.ISBN = id.Identifier
		}
	}
	// Thumbnail over SmallThumbnail when both are present - same "prefer the
	// better one, fall back to what's there" pattern as the ISBN pick above.
	if v.VolumeInfo.ImageLinks.Thumbnail != "" {
		r.CoverURL = v.VolumeInfo.ImageLinks.Thumbnail
	} else if v.VolumeInfo.ImageLinks.SmallThumbnail != "" {
		r.CoverURL = v.VolumeInfo.ImageLinks.SmallThumbnail
	}
	return r
}
