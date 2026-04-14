package search

import (
	"context"
	"fmt"
	"io"
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
	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
)

// BookTracker searches booktracker.org — a Russian book/audiobook torrent tracker.
// Requires username/password for authentication. Returns torrent download URLs.
type BookTracker struct {
	cfg        *config.Config
	authClient *http.Client // client with cookie jar for authenticated requests
	tab        string       // "main" or "audiobook"

	mu        sync.Mutex
	loggedIn  bool
	loginTime time.Time
}

// NewBookTracker creates a new BookTracker searcher for the given tab.
func NewBookTracker(cfg *config.Config, client *http.Client, tab string) *BookTracker {
	jar, _ := cookiejar.New(nil)
	authClient := &http.Client{
		Timeout: client.Timeout,
		Jar:     jar,
	}
	return &BookTracker{
		cfg:        cfg,
		authClient: authClient,
		tab:        tab,
	}
}

func (b *BookTracker) Name() string {
	switch b.tab {
	case "audiobook":
		return "booktracker_audiobook"
	default:
		return "booktracker"
	}
}

func (b *BookTracker) Label() string {
	switch b.tab {
	case "audiobook":
		return "BookTracker (Audiobooks)"
	default:
		return "BookTracker"
	}
}

func (b *BookTracker) Enabled() bool {
	return b.cfg.BookTrackerEnabled && b.cfg.BookTrackerURL != "" &&
		b.cfg.BookTrackerUser != "" && b.cfg.BookTrackerPass != ""
}
func (b *BookTracker) SearchTab() string     { return b.tab }
func (b *BookTracker) DownloadType() string  { return "torrent" }

// login authenticates to BookTracker and stores session cookies.
// Cookie jar persists cookies across requests on the authClient.
func (b *BookTracker) login(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Reuse session if logged in within the last 30 minutes.
	if b.loggedIn && time.Since(b.loginTime) < 30*time.Minute {
		return nil
	}

	loginURL := fmt.Sprintf("%s/login.php", b.cfg.BookTrackerURL)

	form := url.Values{}
	form.Set("username", b.cfg.BookTrackerUser)
	form.Set("password", b.cfg.BookTrackerPass)
	form.Set("autologin", "1")
	form.Set("login", "Login")

	req, err := http.NewRequestWithContext(ctx, "POST", loginURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("booktracker login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", b.cfg.UserAgent)

	resp, err := b.authClient.Do(req)
	if err != nil {
		return fmt.Errorf("booktracker login: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 302 {
		return fmt.Errorf("booktracker login HTTP %d", resp.StatusCode)
	}

	// Check that we got session cookies.
	btURL, _ := url.Parse(b.cfg.BookTrackerURL)
	if btURL != nil && len(b.authClient.Jar.Cookies(btURL)) == 0 {
		slog.Warn("booktracker login: no session cookies received, auth may have failed")
	}

	b.loggedIn = true
	b.loginTime = time.Now()
	slog.Info("booktracker login successful")
	return nil
}

// BookTracker search forum IDs.
const (
	btForumEbooks     = 56 // Книги
	btForumAudiobooks = 24 // Аудиокниги
)

func (b *BookTracker) forumIDForTab() int {
	switch b.tab {
	case "audiobook":
		return btForumAudiobooks
	default:
		return btForumEbooks
	}
}

func (b *BookTracker) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	// Login first.
	if err := b.login(ctx); err != nil {
		return nil, fmt.Errorf("booktracker auth: %w", err)
	}

	forumID := b.forumIDForTab()
	searchURL := fmt.Sprintf("%s/search.php", b.cfg.BookTrackerURL)

	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("search_author", "")
	q.Set("search_forum", strconv.Itoa(forumID))
	q.Set("search_keywords", query)
	q.Set("search_terms", "all")   // all words
	q.Set("search_fields", "all")  // title + body
	q.Set("sort_dir", "DESC")
	q.Set("show_results", "topics")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", b.cfg.UserAgent)

	resp, err := b.authClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("booktracker search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Session may have expired — retry once.
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			b.mu.Lock()
			b.loggedIn = false
			b.mu.Unlock()
			if err := b.login(ctx); err != nil {
				return nil, fmt.Errorf("booktracker re-auth: %w", err)
			}
			// Retry search.
			req2, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
			if err != nil {
				return nil, err
			}
			req2.URL.RawQuery = q.Encode()
			req2.Header.Set("User-Agent", b.cfg.UserAgent)

			resp2, err := b.authClient.Do(req2)
			if err != nil {
				return nil, fmt.Errorf("booktracker retry search: %w", err)
			}
			defer resp2.Body.Close()
			if resp2.StatusCode != 200 {
				return nil, fmt.Errorf("booktracker search HTTP %d after re-auth", resp2.StatusCode)
			}
			return b.parseSearchResults(resp2.Body)
		}
		return nil, fmt.Errorf("booktracker search HTTP %d", resp.StatusCode)
	}

	return b.parseSearchResults(resp.Body)
}

var (
	btTopicLinkRe = regexp.MustCompile(`viewtopic\.php\?t=(\d+)`)
	btSizeRe     = regexp.MustCompile(`(?i)([\d.]+)\s*(GB|MB|KB|B)`)
	btSeedRe     = regexp.MustCompile(`<td[^>]*class="[^"]*seed[^"]*"[^>]*>\s*(\d+)`)
	btFormatRe   = regexp.MustCompile(`\[(epub|pdf|fb2|mobi|djvu|mp3|m4b|ogg|flac|aac)\b`)
	btAuthorRe   = regexp.MustCompile(`^([^-]+)\s*-\s*`)
)

func (b *BookTracker) parseSearchResults(body io.Reader) ([]models.SearchResult, error) {
	doc, err := goquery.NewDocumentFromReader(body)
	if err != nil {
		return nil, fmt.Errorf("booktracker parse HTML: %w", err)
	}

	sourceName := "booktracker"
	if b.tab == "audiobook" {
		sourceName = "booktracker_audiobook"
	}

	var results []models.SearchResult
	seenTopics := make(map[string]bool)

	// Find topic rows in search results.
	doc.Find("tr:has(a.topictitle)").Each(func(_ int, row *goquery.Selection) {
		link := row.Find("a.topictitle")
		if link.Length() == 0 {
			return
		}

		href, exists := link.Attr("href")
		if !exists {
			return
		}

		// Extract topic ID.
		topicMatch := btTopicLinkRe.FindStringSubmatch(href)
		if len(topicMatch) < 2 {
			return
		}
		topicID := topicMatch[1]

		if seenTopics[topicID] {
			return
		}
		seenTopics[topicID] = true

		title := strings.TrimSpace(link.Text())
		if title == "" {
			return
		}

		// Skip suspicious titles.
		if IsSuspicious(title) {
			return
		}

		// Extract author from "Author - Title" format common on BookTracker.
		author := ""
		if authorMatch := btAuthorRe.FindStringSubmatch(title); len(authorMatch) >= 2 {
			candidate := strings.TrimSpace(authorMatch[1])
			if len(candidate) < 80 {
				author = candidate
			}
		}

		// Extract format from title brackets like [epub], [mp3].
		format := ""
		if formatMatch := btFormatRe.FindStringSubmatch(title); len(formatMatch) >= 2 {
			format = strings.ToLower(formatMatch[1])
		}

		// Extract size from row text.
		sizeHuman := ""
		rowText := row.Text()
		if sizeMatch := btSizeRe.FindStringSubmatch(rowText); len(sizeMatch) >= 3 {
			sizeHuman = sizeMatch[1] + " " + sizeMatch[2]
		}

		// Extract seeders.
		seeders := 0
		if seedMatch := btSeedRe.FindStringSubmatch(rowText); len(seedMatch) >= 2 {
			seeders, _ = strconv.Atoi(seedMatch[1])
		}

		// Build torrent download URL from topic ID.
		// BookTracker uses: download.php?t={topicID}
		downloadURL := fmt.Sprintf("%s/download.php?t=%s", b.cfg.BookTrackerURL, topicID)

		// Build topic page URL for reference.
		topicURL := fmt.Sprintf("%s/viewtopic.php?t=%s", b.cfg.BookTrackerURL, topicID)

		results = append(results, models.SearchResult{
			Source:      sourceName,
			Title:       title,
			Author:      author,
			SizeHuman:   sizeHuman,
			Seeders:     seeders,
			DownloadURL: downloadURL,
			URL:         topicURL,
			SourceID:    topicID,
			Format:      format,
			Indexer:     "BookTracker",
			GUID:        fmt.Sprintf("bt-%s", topicID),
		})
	})

	slog.Debug("booktracker search completed", "tab", b.tab, "results", len(results))
	return results, nil
}
