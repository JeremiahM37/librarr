package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
)

// ZLibrary searches the Z-Library external API for ebooks.
// Requires email + password authentication; daily download limits apply.
type ZLibrary struct {
	cfg    *config.Config
	client *http.Client

	mu          sync.Mutex
	loggedIn    bool
	sessionTS   time.Time
	personalURL string
}

// NewZLibrary creates a new Z-Library searcher.
func NewZLibrary(cfg *config.Config, client *http.Client) *ZLibrary {
	// Z-Library needs cookie persistence for session management.
	jar, _ := cookiejar.New(nil)
	zlClient := &http.Client{
		Timeout:       client.Timeout,
		CheckRedirect: client.CheckRedirect,
		Jar:           jar,
	}
	if rt := client.Transport; rt != nil {
		zlClient.Transport = rt
	}

	return &ZLibrary{
		cfg:    cfg,
		client: zlClient,
	}
}

func (z *ZLibrary) Name() string        { return "zlibrary" }
func (z *ZLibrary) Label() string        { return "Z-Library" }
func (z *ZLibrary) Enabled() bool        { return z.cfg.ZLibraryEnabled && z.cfg.ZLibraryEmail != "" && z.cfg.ZLibraryPassword != "" }
func (z *ZLibrary) SearchTab() string    { return "main" }
func (z *ZLibrary) DownloadType() string { return "direct" }

// apiBase returns the Z-Library API base URL.
func (z *ZLibrary) apiBase() string {
	if z.cfg.ZLibraryURL != "" {
		return strings.TrimRight(z.cfg.ZLibraryURL, "/")
	}
	return "https://z-lib.org"
}

// --- Z-Library API response types ---

type zlLoginResponse struct {
	Success bool `json:"success"`
	User    struct {
		ID    int    `json:"id"`
		Email string `json:"email"`
	} `json:"user"`
	Message string `json:"message,omitempty"`
}

type zlSearchResponse struct {
	Success    bool     `json:"success"`
	Result     []zlBook `json:"result"`
	Pagination struct {
		Page  int `json:"page"`
		Total int `json:"total"`
	} `json:"pagination"`
	Error string `json:"error,omitempty"`
}

type zlBook struct {
	ID          int    `json:"id"`
	Hash        string `json:"hash"`
	Title       string `json:"title"`
	Author      string `json:"author"`
	Extension   string `json:"extension"`
	Filesize    int64  `json:"filesize"`
	Language    string `json:"language"`
	Year        string `json:"year"`
	Pages       int    `json:"pages"`
	Cover       string `json:"cover"`
	Description string `json:"description,omitempty"`
}

type zlDomainsResponse struct {
	Success bool     `json:"success"`
	Domains []string `json:"domains"`
}

// login authenticates with Z-Library and stores the session.
func (z *ZLibrary) login(ctx context.Context) error {
	z.mu.Lock()
	defer z.mu.Unlock()

	// Re-use session if still fresh (login lasts ~1 hour).
	if z.loggedIn && time.Since(z.sessionTS) < 55*time.Minute {
		return nil
	}

	baseURL := z.apiBase()
	loginURL := fmt.Sprintf("%s/eapi/user/login", baseURL)

	payload := map[string]string{
		"email":    z.cfg.ZLibraryEmail,
		"password": z.cfg.ZLibraryPassword,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("zlibrary marshal login: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", loginURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("zlibrary login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", z.cfg.UserAgent)

	resp, err := z.client.Do(req)
	if err != nil {
		return fmt.Errorf("zlibrary login: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("zlibrary login HTTP %d", resp.StatusCode)
	}

	var loginResp zlLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("zlibrary decode login: %w", err)
	}

	if !loginResp.Success {
		return fmt.Errorf("zlibrary login failed: %s", loginResp.Message)
	}

	z.loggedIn = true
	z.sessionTS = time.Now()
	slog.Info("z-library login successful", "email", z.cfg.ZLibraryEmail)

	// Resolve personal download domain.
	if err := z.resolveDomains(ctx); err != nil {
		slog.Warn("z-library domain resolution failed, using default", "error", err)
		z.personalURL = baseURL
	}

	return nil
}

// resolveDomains fetches the personal download domain list.
func (z *ZLibrary) resolveDomains(ctx context.Context) error {
	baseURL := z.apiBase()
	domainsURL := fmt.Sprintf("%s/eapi/info/domains", baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", domainsURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", z.cfg.UserAgent)

	resp, err := z.client.Do(req)
	if err != nil {
		return fmt.Errorf("zlibrary domains: %w", err)
	}
	defer resp.Body.Close()

	var domainsResp zlDomainsResponse
	if err := json.NewDecoder(resp.Body).Decode(&domainsResp); err != nil {
		return fmt.Errorf("zlibrary decode domains: %w", err)
	}

	if domainsResp.Success && len(domainsResp.Domains) > 0 {
		z.personalURL = "https://" + domainsResp.Domains[0]
		slog.Debug("z-library personal domain resolved", "domain", domainsResp.Domains[0])
	}

	return nil
}

// Search performs a search against the Z-Library API.
func (z *ZLibrary) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	// Ensure we're logged in.
	if err := z.login(ctx); err != nil {
		return nil, fmt.Errorf("zlibrary auth: %w", err)
	}

	baseURL := z.apiBase()
	searchURL := fmt.Sprintf("%s/eapi/book/search", baseURL)

	payload := map[string]interface{}{
		"message": query,
		"limit":   15,
		"page":    1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("zlibrary marshal search: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", searchURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("zlibrary search request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", z.cfg.UserAgent)

	resp, err := z.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("zlibrary search: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("zlibrary read search: %w", err)
	}

	// Handle 401 — session expired; retry once.
	if resp.StatusCode == 401 {
		z.mu.Lock()
		z.loggedIn = false
		z.mu.Unlock()
		slog.Info("z-library session expired, re-logging in")

		if err := z.login(ctx); err != nil {
			return nil, fmt.Errorf("zlibrary re-auth: %w", err)
		}

		// Retry search.
		req2, err := http.NewRequestWithContext(ctx, "POST", searchURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req2.Header.Set("Content-Type", "application/json")
		req2.Header.Set("User-Agent", z.cfg.UserAgent)

		resp2, err := z.client.Do(req2)
		if err != nil {
			return nil, fmt.Errorf("zlibrary search retry: %w", err)
		}
		defer resp2.Body.Close()
		respBody, err = io.ReadAll(resp2.Body)
		if err != nil {
			return nil, err
		}
		if resp2.StatusCode != 200 {
			return nil, fmt.Errorf("zlibrary search retry HTTP %d", resp2.StatusCode)
		}
	} else if resp.StatusCode != 200 {
		snippet := string(respBody)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("zlibrary search HTTP %d: %s", resp.StatusCode, snippet)
	}

	var searchResp zlSearchResponse
	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return nil, fmt.Errorf("zlibrary decode search: %w", err)
	}

	if !searchResp.Success {
		return nil, fmt.Errorf("zlibrary search failed: %s", searchResp.Error)
	}

	var results []models.SearchResult
	for _, book := range searchResp.Result {
		sizeHuman := formatZLSize(book.Filesize)
		format := strings.ToUpper(book.Extension)

		// Construct download URL using personal domain.
		downloadURL := ""
		if z.personalURL != "" && book.Hash != "" {
			downloadURL = fmt.Sprintf("%s/dl/%d/%s", z.personalURL, book.ID, book.Hash)
		} else if z.personalURL != "" {
			downloadURL = fmt.Sprintf("%s/dl/%d", z.personalURL, book.ID)
		}

		coverURL := book.Cover
		if coverURL != "" && !strings.HasPrefix(coverURL, "http") {
			coverURL = baseURL + coverURL
		}

		results = append(results, models.SearchResult{
			Source:      "zlibrary",
			Title:       book.Title,
			Author:      book.Author,
			SourceID:    fmt.Sprintf("zlibrary-%d", book.ID),
			CoverURL:    coverURL,
			URL:         downloadURL,
			DownloadURL: downloadURL,
			SizeHuman:   sizeHuman,
			Format:      format,
		})
	}

	slog.Debug("z-library search complete", "query", query, "results", len(results))
	return results, nil
}

// formatZLSize converts bytes to a human-readable size string.
func formatZLSize(b int64) string {
	if b <= 0 {
		return ""
	}
	switch {
	case b >= 1e9:
		return fmt.Sprintf("%.1f GB", float64(b)/1e9)
	case b >= 1e6:
		return fmt.Sprintf("%.1f MB", float64(b)/1e6)
	case b >= 1e3:
		return fmt.Sprintf("%.1f KB", float64(b)/1e3)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
