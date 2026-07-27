package search

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// BookTracker searches the BookTracker.org Russian book/audiobook tracker.
// It requires cookie-based authentication and HTML scraping.
type BookTracker struct {
	cfg        *config.Config
	authClient *http.Client
	tab        string // "main" or "audiobook"

	mu        sync.Mutex
	loggedIn  bool
	loginTime time.Time
}

// NewBookTracker creates a new BookTracker searcher for the given tab.
func NewBookTracker(cfg *config.Config, _ *http.Client, tab string) *BookTracker {
	jar, _ := cookiejar.New(nil)
	return &BookTracker{
		cfg: cfg,
		authClient: &http.Client{
			Timeout: 30 * time.Second,
			Jar:     jar,
		},
		tab: tab,
	}
}

func (b *BookTracker) Name() string {
	if b.tab == "audiobook" {
		return "booktracker_audiobook"
	}
	return "booktracker"
}

func (b *BookTracker) Label() string { return "BookTracker" }

func (b *BookTracker) Enabled() bool {
	return b.cfg.BookTrackerEnabled && b.cfg.BookTrackerURL != "" &&
		b.cfg.BookTrackerUser != "" && b.cfg.BookTrackerPass != ""
}

func (b *BookTracker) SearchTab() string { return b.tab }

func (b *BookTracker) DownloadType() string { return "torrent" }

var (
	btTopicLinkRe = regexp.MustCompile(`viewtopic\.php\?t=(\d+)`)
	btSizeRe      = regexp.MustCompile(`(\d+[\.,]?\d*)\s*(Г[Бб]|М[Бб]|К[Бб]|G[Bb]|M[Bb]|K[Bb])`)
	btSeedRe      = regexp.MustCompile(`(\d+)`)
	btFormatRe    = regexp.MustCompile(`(?i)\b(epub|pdf|fb2|mobi|djvu|mp3|m4b|ogg|flac|aac)\b`)
	btAuthorRe    = regexp.MustCompile(`^(.+?)\s*-\s*`)
)

// forumIDForTab returns the BookTracker forum ID for the configured tab.
func (b *BookTracker) forumIDForTab() string {
	switch b.tab {
	case "audiobook":
		return "24" // Аудиокниги
	default:
		return "56" // Книги
	}
}

// login authenticates to BookTracker and caches the session for 30 minutes.
func (b *BookTracker) login() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.loggedIn && time.Since(b.loginTime) < 30*time.Minute {
		return nil
	}

	loginURL := strings.TrimRight(b.cfg.BookTrackerURL, "/") + "/login.php"
	form := url.Values{
		"username":  {b.cfg.BookTrackerUser},
		"password":  {b.cfg.BookTrackerPass},
		"autologin": {"1"},
		"login":     {"1"},
	}

	resp, err := b.authClient.PostForm(loginURL, form)
	if err != nil {
		return fmt.Errorf("BookTracker login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 302 {
		return fmt.Errorf("BookTracker login HTTP %d", resp.StatusCode)
	}

	// Verify we got an authenticated session cookie. phpBB sets
	// *_sid cookies for anonymous visitors too, so we require one of the
	// persistent-login cookies that only appear after a real login.
	// Known names: phpbb2mysql_data, phpbb3_data, bb_data, bb_t (autologin token).
	parsedURL, _ := url.Parse(b.cfg.BookTrackerURL)
	cookies := b.authClient.Jar.Cookies(parsedURL)
	hasSession := false
	for _, c := range cookies {
		n := strings.ToLower(c.Name)
		if strings.Contains(n, "_data") || strings.Contains(n, "bb_t") ||
			strings.HasSuffix(n, "_u") || strings.HasSuffix(n, "_k") {
			if c.Value != "" && c.Value != "0" && c.Value != "a%3A0%3A%7B%7D" {
				hasSession = true
				break
			}
		}
	}

	if !hasSession {
		return fmt.Errorf("BookTracker login failed: no authenticated session cookie received (check credentials)")
	}

	b.loggedIn = true
	b.loginTime = time.Now()
	slog.Debug("BookTracker login successful")
	return nil
}

// reauth clears session state and re-authenticates.
func (b *BookTracker) reauth() error {
	b.mu.Lock()
	b.loggedIn = false
	b.mu.Unlock()
	return b.login()
}

func (b *BookTracker) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	if err := b.login(); err != nil {
		return nil, fmt.Errorf("BookTracker login: %w", err)
	}

	results, err := b.doSearch(ctx, query)
	if err != nil {
		// On auth failure, try re-auth once.
		if strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "403") {
			slog.Warn("BookTracker search got auth error, re-authenticating", "error", err)
			if reErr := b.reauth(); reErr != nil {
				return nil, fmt.Errorf("BookTracker re-auth: %w", reErr)
			}
			return b.doSearch(ctx, query)
		}
		return nil, err
	}
	return results, nil
}

func (b *BookTracker) doSearch(ctx context.Context, query string) ([]models.SearchResult, error) {
	baseURL := strings.TrimRight(b.cfg.BookTrackerURL, "/")
	searchURL := fmt.Sprintf("%s/search.php?search_forum=%s&search_keywords=%s&show_results=topics",
		baseURL, b.forumIDForTab(), url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", b.cfg.UserAgent)

	resp, err := b.authClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("BookTracker search request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return nil, fmt.Errorf("BookTracker HTTP %d (auth expired)", resp.StatusCode)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("BookTracker HTTP %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("BookTracker parse HTML: %w", err)
	}

	return b.parseSearchResults(doc, baseURL), nil
}

func (b *BookTracker) parseSearchResults(doc *goquery.Document, baseURL string) []models.SearchResult {
	sourceName := b.Name()
	mediaType := "ebook"
	if b.tab == "audiobook" {
		mediaType = "audiobook"
	}

	var results []models.SearchResult
	seenTopics := make(map[string]bool)

	doc.Find("tr:has(a.topictitle)").Each(func(i int, s *goquery.Selection) {
		if len(results) >= 20 {
			return
		}

		link := s.Find("a.topictitle").First()
		href, exists := link.Attr("href")
		if !exists {
			return
		}

		// Extract topic ID.
		matches := btTopicLinkRe.FindStringSubmatch(href)
		if len(matches) < 2 {
			return
		}
		topicID := matches[1]

		if seenTopics[topicID] {
			return
		}
		seenTopics[topicID] = true

		title := strings.TrimSpace(link.Text())
		if title == "" {
			return
		}

		if IsSuspicious(title) {
			return
		}

		// Extract author from title (format: "Author - Title [details]").
		author := ""
		if m := btAuthorRe.FindStringSubmatch(title); len(m) > 1 {
			author = strings.TrimSpace(m[1])
		}

		// Extract format.
		format := ""
		if m := btFormatRe.FindStringSubmatch(title); len(m) > 1 {
			format = strings.ToLower(m[1])
		}

		// Extract size from row text.
		rowText := s.Text()
		var sizeHuman string
		if m := btSizeRe.FindStringSubmatch(rowText); len(m) > 1 {
			sizeHuman = m[1] + " " + m[2]
		}

		// Extract seeders only from cells explicitly tagged as seed columns.
		// A looser fallback (scan every <td> for a number) mis-reads year
		// and file-count cells as seeder counts, so we leave seeders at 0
		// when the known selectors don't match rather than guess.
		seeders := 0
		seedEl := s.Find("td.seedLeach, td.leechseed, .seedmed, .seed, .leech, b.seedmed, span.seedmed")
		if seedEl.Length() > 0 {
			seedText := strings.TrimSpace(seedEl.First().Text())
			if m := btSeedRe.FindStringSubmatch(seedText); len(m) > 1 {
				seeders, _ = strconv.Atoi(m[1])
			}
		}

		viewURL := baseURL + "/viewtopic.php?t=" + topicID
		downloadURL := baseURL + "/download.php?t=" + topicID

		results = append(results, models.SearchResult{
			Source:      sourceName,
			Title:       title,
			Author:      author,
			DownloadURL: downloadURL,
			URL:         viewURL,
			Seeders:     seeders,
			SizeHuman:   sizeHuman,
			Format:      format,
			Indexer:     "BookTracker",
			MediaType:   mediaType,
		})
	})

	return results
}
