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
	Title       string   `json:"title"`
	Author      string   `json:"author"`
	CoverURL    string   `json:"cover_url"`
	Description string   `json:"description"`
	Year        string   `json:"year"`
	Series      string   `json:"series"`
	SeriesPos   string   `json:"series_position"`
	ISBN        string   `json:"isbn"`
	PageCount   int      `json:"page_count"`
	Publisher   string   `json:"publisher"`
	Language    string   `json:"language"`
	OLID        string   `json:"olid"`
	Subjects    []string `json:"subjects,omitempty"`
	Provider    string   `json:"provider,omitempty"`
	Confidence  int      `json:"confidence,omitempty"`
	MatchReason string   `json:"match_reason,omitempty"`
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
	searchURL  string
	workBase   string
	coverBase  string
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
		searchURL:  olSearchAPI,
		workBase:   "https://openlibrary.org",
		coverBase:  "https://covers.openlibrary.org",
		cache:      make(map[string]cacheEntry),
	}
}

// NewClientForTest creates an Open Library client pointed at local test
// servers. Production callers should use NewClient.
func NewClientForTest(httpClient *http.Client, searchURL, workBase, coverBase string) *Client {
	c := NewClient(httpClient)
	if strings.TrimSpace(searchURL) != "" {
		c.searchURL = searchURL
	}
	if strings.TrimSpace(workBase) != "" {
		c.workBase = strings.TrimRight(workBase, "/")
	}
	if strings.TrimSpace(coverBase) != "" {
		c.coverBase = strings.TrimRight(coverBase, "/")
	}
	return c
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

type MatchQuery struct {
	Title  string
	Author string
	ISBN   string
	Limit  int
}

func (c *Client) SearchMatches(ctx context.Context, query MatchQuery) ([]BookMetadata, error) {
	limit := query.Limit
	if limit <= 0 || limit > 10 {
		limit = 5
	}
	req, err := http.NewRequestWithContext(ctx, "GET", c.searchURL, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	if strings.TrimSpace(query.ISBN) != "" {
		q.Set("isbn", strings.TrimSpace(query.ISBN))
	} else {
		q.Set("title", strings.TrimSpace(query.Title))
		if strings.TrimSpace(query.Author) != "" {
			q.Set("author", strings.TrimSpace(query.Author))
		}
	}
	q.Set("fields", "key,title,subtitle,author_name,first_publish_year,cover_i,isbn,publisher,language,number_of_pages_median,subject")
	q.Set("limit", fmt.Sprint(limit))
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "Librarr/2.0 (book download manager; github.com/jamie75/librarr)")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var data olSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	matches := make([]BookMetadata, 0, len(data.Docs))
	for _, doc := range data.Docs {
		meta := c.metadataFromDoc(ctx, doc)
		meta.Provider = "openlibrary"
		meta.Confidence, meta.MatchReason = scoreOpenLibraryMatch(query, doc)
		matches = append(matches, meta)
	}
	return matches, nil
}

func (c *Client) metadataFromDoc(ctx context.Context, doc olSearchDoc) BookMetadata {
	meta := BookMetadata{
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
		meta.CoverURL = fmt.Sprintf("%s/b/id/%d-M.jpg", strings.TrimRight(c.coverBase, "/"), doc.CoverI)
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
	meta.Subjects = append(meta.Subjects, doc.Subject...)
	if doc.Key != "" {
		c.enrichFromWork(ctx, doc.Key, &meta)
	}
	return meta
}

// searchOL searches the Open Library search API and returns the best matching doc.
func (c *Client) searchOL(ctx context.Context, title, author string) (*olSearchDoc, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.searchURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("title", title)
	if author != "" {
		q.Set("author", author)
	}
	q.Set("fields", "key,title,author_name,first_publish_year,cover_i,isbn,publisher,language,number_of_pages_median")
	q.Set("limit", "3")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "Librarr/2.0 (book download manager; github.com/jamie75/librarr)")

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
	if len(data.Docs) == 0 {
		return nil, nil
	}

	return &data.Docs[0], nil
}

// enrichFromWork fetches the Works API for description and series info.
func (c *Client) enrichFromWork(ctx context.Context, workKey string, meta *BookMetadata) {
	// workKey is like "/works/OL12345W"
	url := strings.TrimRight(c.workBase, "/") + workKey + ".json"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "Librarr/2.0 (book download manager; github.com/jamie75/librarr)")

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
	meta.Subjects = append(meta.Subjects, work.Subjects...)

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
			meta.CoverURL = fmt.Sprintf("%s/b/olid/%s-M.jpg", strings.TrimRight(c.coverBase, "/"), olid)
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
	Subject             []string `json:"subject"`
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

func scoreOpenLibraryMatch(query MatchQuery, doc olSearchDoc) (int, string) {
	if strings.TrimSpace(query.ISBN) != "" {
		want := cleanID(query.ISBN)
		for _, isbn := range doc.ISBN {
			if cleanID(isbn) == want && want != "" {
				return 98, "ISBN exact match"
			}
		}
	}
	titleMatch := normalizeText(doc.Title) == normalizeText(query.Title)
	authorMatch := false
	if strings.TrimSpace(query.Author) == "" {
		authorMatch = true
	} else {
		wantAuthor := normalizeText(query.Author)
		for _, author := range doc.AuthorName {
			if normalizeText(author) == wantAuthor {
				authorMatch = true
				break
			}
		}
	}
	switch {
	case titleMatch && authorMatch:
		return 92, "Exact title and author match"
	case titleMatch:
		return 74, "Exact title match"
	case authorMatch && containsNormalized(doc.Title, query.Title):
		return 62, "Conservative fuzzy title and author match"
	default:
		return 35, "Provider candidate"
	}
}

func cleanID(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		if (r >= '0' && r <= '9') || r == 'X' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func normalizeText(value string) string {
	replacer := strings.NewReplacer("-", " ", ":", " ", "_", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(value))), " ")
}

func containsNormalized(haystack, needle string) bool {
	haystack = normalizeText(haystack)
	needle = normalizeText(needle)
	return needle != "" && strings.Contains(haystack, needle)
}
