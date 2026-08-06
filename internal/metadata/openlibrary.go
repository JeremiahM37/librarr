// Package metadata fetches book metadata from Open Library.
package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// BookMetadata holds enriched book information from Open Library.
type BookMetadata struct {
	Title       string `json:"title"`
	Author      string `json:"author"`
	CoverURL    string `json:"cover_url"`
	Description string `json:"description"`
	Year        string `json:"year"`
	Series      string `json:"series"`
	SeriesPos   string `json:"series_position"`
	ISBN        string `json:"isbn"`
	PageCount   int    `json:"page_count"`
	Publisher   string `json:"publisher"`
	Language    string `json:"language"`
	OLID        string `json:"olid"`
}

const (
	olSearchAPI = "https://openlibrary.org/search.json"
	olWorksAPI  = "https://openlibrary.org/works/"
	cacheTTL    = 30 * time.Minute
)

type cacheEntry struct {
	data      *BookMetadata
	fetchedAt time.Time
}

// Client fetches metadata from Open Library with in-memory caching.
type Client struct {
	httpClient *http.Client
	mu         sync.RWMutex
	cache      map[string]cacheEntry
}

// NewClient creates a metadata client.
func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{
		httpClient: httpClient,
		cache:      make(map[string]cacheEntry),
	}
}

// FetchMetadata looks up a book by title and author, returning enriched metadata.
func (c *Client) FetchMetadata(title, author string) (*BookMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.FetchMetadataCtx(ctx, title, author)
}

// FetchMetadataCtx looks up a book with a caller-provided context.
func (c *Client) FetchMetadataCtx(ctx context.Context, title, author string) (*BookMetadata, error) {
	cacheKey := strings.ToLower(strings.TrimSpace(title) + "|" + strings.TrimSpace(author))

	// Check cache.
	c.mu.RLock()
	if entry, ok := c.cache[cacheKey]; ok && time.Since(entry.fetchedAt) < cacheTTL {
		c.mu.RUnlock()
		return entry.data, nil
	}
	c.mu.RUnlock()

	// Search Open Library.
	doc, err := c.searchOL(ctx, title, author)
	if err != nil {
		return nil, fmt.Errorf("open library search: %w", err)
	}
	if doc == nil {
		return nil, nil
	}

	meta := &BookMetadata{
		Title: doc.Title,
		OLID:  doc.Key,
		Year:  fmt.Sprintf("%d", doc.FirstPublishYear),
	}
	if doc.FirstPublishYear == 0 {
		meta.Year = ""
	}
	if len(doc.AuthorName) > 0 {
		meta.Author = doc.AuthorName[0]
	}
	if doc.CoverI > 0 {
		meta.CoverURL = fmt.Sprintf("https://covers.openlibrary.org/b/id/%d-M.jpg", doc.CoverI)
	}
	if len(doc.ISBN) > 0 {
		meta.ISBN = doc.ISBN[0]
	}
	if len(doc.Publisher) > 0 {
		meta.Publisher = doc.Publisher[0]
	}
	if len(doc.Language) > 0 {
		meta.Language = doc.Language[0]
	}
	if doc.NumberOfPagesMedian > 0 {
		meta.PageCount = doc.NumberOfPagesMedian
	}

	// Try to fetch work details for description and series.
	if doc.Key != "" {
		c.enrichFromWork(ctx, doc.Key, meta)
	}

	// Cache the result.
	c.mu.Lock()
	c.cache[cacheKey] = cacheEntry{data: meta, fetchedAt: time.Now()}
	c.mu.Unlock()

	return meta, nil
}

// searchOL searches the Open Library search API and returns the best matching doc.
func (c *Client) searchOL(ctx context.Context, title, author string) (*olSearchDoc, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", olSearchAPI, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("title", title)
	if author != "" {
		q.Set("author", author)
	}
	q.Set("fields", "key,title,author_name,first_publish_year,cover_i,isbn,publisher,language,number_of_pages_median")
	// Wider than strictly needed: the volume a series query is after is often
	// ranked below one or more box sets, and pickBestDoc has to be able to see it.
	q.Set("limit", "10")
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
	return pickBestDoc(title, author, data.Docs), nil
}

// enrichFromWork fetches the Works API for description and series info.
func (c *Client) enrichFromWork(ctx context.Context, workKey string, meta *BookMetadata) {
	// workKey is like "/works/OL12345W"
	url := "https://openlibrary.org" + workKey + ".json"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "Librarr/2.0 (book download manager; github.com/JeremiahM37/librarr)")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Debug("metadata work fetch failed", "key", workKey, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return
	}

	var work olWork
	if err := json.NewDecoder(resp.Body).Decode(&work); err != nil {
		return
	}

	// Description can be a string or an object with "value" key.
	meta.Description = work.descriptionText()

	// Extract series from subjects or links.
	for _, link := range work.Links {
		lower := strings.ToLower(link.Title)
		if strings.Contains(lower, "series") {
			meta.Series = link.Title
			break
		}
	}

	// Try to find series in subjects.
	if meta.Series == "" {
		for _, subj := range work.Subjects {
			lower := strings.ToLower(subj)
			if strings.Contains(lower, "series") || strings.Contains(lower, "#") {
				meta.Series = subj
				break
			}
		}
	}

	// Cover URL from OLID if we didn't get one from search.
	if meta.CoverURL == "" && meta.OLID != "" {
		// Extract work ID from key like "/works/OL12345W".
		parts := strings.Split(meta.OLID, "/")
		if len(parts) > 0 {
			olid := parts[len(parts)-1]
			meta.CoverURL = fmt.Sprintf("https://covers.openlibrary.org/b/olid/%s-M.jpg", olid)
		}
	}
}

// Open Library API response types.

type olSearchResponse struct {
	Docs []olSearchDoc `json:"docs"`
}

type olSearchDoc struct {
	Key                 string   `json:"key"`
	Title               string   `json:"title"`
	AuthorName          []string `json:"author_name"`
	FirstPublishYear    int      `json:"first_publish_year"`
	CoverI              int      `json:"cover_i"`
	ISBN                []string `json:"isbn"`
	Publisher           []string `json:"publisher"`
	Language            []string `json:"language"`
	NumberOfPagesMedian int      `json:"number_of_pages_median"`
}

type olWork struct {
	Description interface{} `json:"description"`
	Subjects    []string    `json:"subjects"`
	Links       []olLink    `json:"links"`
}

type olLink struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

func (w *olWork) descriptionText() string {
	if w.Description == nil {
		return ""
	}
	switch v := w.Description.(type) {
	case string:
		return truncate(v, 500)
	case map[string]interface{}:
		if val, ok := v["value"].(string); ok {
			return truncate(val, 500)
		}
	}
	return ""
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
