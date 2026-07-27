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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/zlibraryparse"
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

func (z *ZLibrary) Name() string  { return "zlibrary" }
func (z *ZLibrary) Label() string { return "Z-Library" }
func (z *ZLibrary) Enabled() bool {
	return z.cfg.ZLibraryEnabled && z.cfg.ZLibraryEmail != "" && z.cfg.ZLibraryPassword != ""
}
func (z *ZLibrary) SearchTab() string    { return "main" }
func (z *ZLibrary) DownloadType() string { return "direct" }

// apiBase returns the API base URL — user-supplied ZLibraryURL takes
// precedence; otherwise we fall back to the value in the runtime sources
// registry.
func (z *ZLibrary) apiBase() string {
	if z.cfg.ZLibraryURL != "" {
		return strings.TrimRight(z.cfg.ZLibraryURL, "/")
	}
	return strings.TrimRight(z.cfg.Sources.ZLibraryDefault, "/")
}

// --- Z-Library API response types ---

type zlRPCLoginResponse struct {
	Errors   []string `json:"errors"`
	Response struct {
		UserID  int    `json:"user_id"`
		UserKey string `json:"user_key"`
	} `json:"response"`
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
	loginURL := fmt.Sprintf("%s/rpc.php", baseURL)

	form := url.Values{}
	form.Set("isModal", "true")
	form.Set("email", z.cfg.ZLibraryEmail)
	form.Set("password", z.cfg.ZLibraryPassword)
	form.Set("site_mode", "books")
	form.Set("action", "login")
	form.Set("redirectUrl", baseURL+"/")
	form.Set("gg_json_mode", "1")

	req, err := http.NewRequestWithContext(ctx, "POST", loginURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("zlibrary login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("User-Agent", z.cfg.UserAgent)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := z.client.Do(req)
	if err != nil {
		return fmt.Errorf("zlibrary login: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("zlibrary login HTTP %d", resp.StatusCode)
	}

	var loginResp zlRPCLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("zlibrary decode login: %w", err)
	}

	if len(loginResp.Errors) > 0 {
		return fmt.Errorf("zlibrary login failed: %s", strings.Join(loginResp.Errors, "; "))
	}
	if loginResp.Response.UserID == 0 || loginResp.Response.UserKey == "" {
		return fmt.Errorf("zlibrary login failed: missing session credentials")
	}

	z.loggedIn = true
	z.sessionTS = time.Now()
	slog.Debug("z-library login successful")

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

	form := url.Values{}
	form.Set("message", query)
	form.Set("limit", "15")
	form.Set("page", "1")
	body := []byte(form.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", searchURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("zlibrary search request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
		req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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

	books, err := zlibraryparse.BooksFromJSON(respBody)
	if err != nil {
		return nil, fmt.Errorf("zlibrary decode search: %w", err)
	}

	var results []models.SearchResult
	for _, book := range books {
		sizeHuman := formatZLSize(book.Filesize)
		format := strings.ToUpper(book.Extension)
		sourceID := fmt.Sprintf("zlibrary-%d", book.ID)
		if book.Hash != "" {
			sourceID = fmt.Sprintf("%s-%s", sourceID, book.Hash)
		}

		// Construct download URL using personal domain.
		downloadURL := ""
		if book.DL != "" {
			downloadURL = zlibraryparse.AbsoluteURL(baseURL, book.DL)
		} else if z.personalURL != "" && book.Hash != "" {
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
			SourceID:    sourceID,
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
