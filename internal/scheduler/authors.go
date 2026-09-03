package scheduler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/webhook"
)

// DefaultOpenLibraryURL is the Open Library origin the author monitor queries.
const DefaultOpenLibraryURL = "https://openlibrary.org"

// AuthorMonitor periodically checks Open Library for new works by monitored
// authors and, per author, either adds them to the wanted list or only
// notifies.
//
// Works are tracked by Open Library work key, not by title or year: a
// reissued edition of an old book keeps its work key, so it does not fire,
// and two books released between checks are both seen. The first check of an
// author records the existing catalogue as a baseline without announcing it —
// monitoring means "tell me what comes next", not "list everything so far".
type AuthorMonitor struct {
	cfg           *config.Config
	db            *db.DB
	webhookSender *webhook.Sender
	client        *http.Client
	baseURL       string
	now           func() time.Time
}

// AuthorCheckResult summarises one author check.
type AuthorCheckResult struct {
	AuthorID  int64    `json:"author_id"`
	Author    string   `json:"author"`
	Baseline  bool     `json:"baseline"` // first check: catalogue recorded, nothing announced
	Seen      int      `json:"seen"`     // works returned by Open Library
	New       []string `json:"new"`      // titles not seen before
	Added     int      `json:"added"`    // wanted rows created
	Error     string   `json:"error,omitempty"`
	CheckedAt string   `json:"checked_at"`
}

// NewAuthorMonitor creates a new author monitor.
func NewAuthorMonitor(cfg *config.Config, database *db.DB, ws *webhook.Sender) *AuthorMonitor {
	return &AuthorMonitor{
		cfg:           cfg,
		db:            database,
		webhookSender: ws,
		client:        &http.Client{Timeout: 15 * time.Second},
		baseURL:       DefaultOpenLibraryURL,
		now:           time.Now,
	}
}

// SetOpenLibraryURL points the monitor at another Open Library origin
// (used by tests and by deployments with a local mirror).
func (am *AuthorMonitor) SetOpenLibraryURL(u string) {
	if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
		am.baseURL = u
	}
}

// CheckAuthors checks every monitored author whose interval has elapsed.
func (am *AuthorMonitor) CheckAuthors() {
	if !am.cfg.AuthorMonitorEnabled {
		return
	}

	authors, err := am.db.GetMonitoredAuthors()
	if err != nil {
		slog.Error("failed to get monitored authors", "error", err)
		return
	}

	now := am.now()
	for _, author := range authors {
		interval := time.Duration(author.CheckIntervalDays) * 24 * time.Hour
		if !author.LastChecked.IsZero() && now.Sub(author.LastChecked) < interval {
			continue
		}
		slog.Info("checking monitored author", "author", author.Name)
		am.CheckAuthor(author)
	}
}

// CheckAuthorByID checks one author now, regardless of its interval.
func (am *AuthorMonitor) CheckAuthorByID(id int64) (AuthorCheckResult, error) {
	author, err := am.db.GetMonitoredAuthor(id)
	if err != nil {
		return AuthorCheckResult{}, err
	}
	return am.CheckAuthor(*author), nil
}

// CheckAuthor fetches the author's works and acts on the ones not seen before.
func (am *AuthorMonitor) CheckAuthor(author db.MonitoredAuthor) AuthorCheckResult {
	res := AuthorCheckResult{AuthorID: author.ID, Author: author.Name, CheckedAt: am.now().Format(time.RFC3339)}

	works, err := am.searchOpenLibrary(author.Name)
	if err != nil {
		slog.Warn("failed to search Open Library for author", "author", author.Name, "error", err)
		res.Error = err.Error()
		_ = am.db.UpdateMonitoredAuthorCheck(author.ID, author.LastBookFound)
		return res
	}
	res.Seen = len(works)
	if len(works) == 0 {
		_ = am.db.UpdateMonitoredAuthorCheck(author.ID, author.LastBookFound)
		return res
	}

	seen, err := am.db.SeenWorkKeys(author.ID)
	if err != nil {
		res.Error = err.Error()
		return res
	}

	// Newest work, for the display-only watermark.
	newest := works[0]
	for _, w := range works[1:] {
		if w.Year > newest.Year {
			newest = w
		}
	}

	var fresh []openLibraryWork
	for _, w := range works {
		if !seen[w.Key] {
			fresh = append(fresh, w)
		}
	}
	toRecord := make([]db.SeenWork, 0, len(fresh))
	for _, w := range fresh {
		toRecord = append(toRecord, db.SeenWork{WorkKey: w.Key, Title: w.Title, Year: w.Year})
	}

	if len(seen) == 0 {
		// First look at this author: record the catalogue, announce nothing.
		res.Baseline = true
		if err := am.db.AddSeenWorks(author.ID, toRecord); err != nil {
			res.Error = err.Error()
		}
		_ = am.db.UpdateMonitoredAuthorCheck(author.ID, newest.Title)
		slog.Info("author monitor: baseline recorded", "author", author.Name, "works", len(toRecord))
		return res
	}

	// Announce newest first.
	sort.SliceStable(fresh, func(i, j int) bool { return fresh[i].Year > fresh[j].Year })
	for _, w := range fresh {
		res.New = append(res.New, w.Title)
		added := false
		if author.AutoAdd {
			added = am.addToWanted(author, w)
		}
		if added {
			res.Added++
		}
		am.announce(author, w, added)
	}
	if err := am.db.AddSeenWorks(author.ID, toRecord); err != nil {
		res.Error = err.Error()
	}
	_ = am.db.UpdateMonitoredAuthorCheck(author.ID, newest.Title)
	return res
}

// addToWanted creates a monitored wanted row for a new work unless one with
// the same title and author already exists.
func (am *AuthorMonitor) addToWanted(author db.MonitoredAuthor, w openLibraryWork) bool {
	existing, err := am.db.GetWishlist()
	if err == nil {
		for _, it := range existing {
			if strings.EqualFold(strings.TrimSpace(it.Title), strings.TrimSpace(w.Title)) &&
				(it.Author == "" || strings.EqualFold(it.Author, author.Name)) {
				return false
			}
		}
	}
	_, err = am.db.AddWishlistItemWithOptions(models.WishlistItem{
		Title:     w.Title,
		Author:    author.Name,
		MediaType: "ebook",
		Monitored: true,
		Source:    fmt.Sprintf("author:%d", author.ID),
	})
	if err != nil {
		slog.Warn("author monitor: failed to add wanted item", "author", author.Name, "title", w.Title, "error", err)
		return false
	}
	slog.Info("author monitor: new work added to wanted list", "author", author.Name, "title", w.Title, "year", w.Year)
	return true
}

func (am *AuthorMonitor) announce(author db.MonitoredAuthor, w openLibraryWork, added bool) {
	msg := fmt.Sprintf("%s (%d)", w.Title, w.Year)
	if w.Year == 0 {
		msg = w.Title
	}
	if added {
		msg += " — added to wanted list"
	}
	slog.Info("new book found for monitored author", "author", author.Name, "title", w.Title, "year", w.Year, "added", added)

	if am.webhookSender != nil {
		am.webhookSender.Send(webhook.Payload{
			Event:   webhook.EventInfo,
			Title:   fmt.Sprintf("New book by %s", author.Name),
			Message: msg,
			Status:  "info",
			Extra: map[string]interface{}{
				"author":   author.Name,
				"title":    w.Title,
				"year":     w.Year,
				"work_key": w.Key,
				"added":    added,
			},
		})
	}

	users, _ := am.db.ListUsers()
	for _, u := range users {
		if u.Role == "admin" {
			_, _ = am.db.CreateNotification(&models.Notification{
				UserID:    u.ID,
				Type:      "author_new_book",
				Title:     fmt.Sprintf("New book by %s", author.Name),
				Message:   msg,
				CreatedAt: am.now(),
			})
		}
	}
}

// openLibraryWork represents a work returned by Open Library search.
type openLibraryWork struct {
	Key   string `json:"key"`
	Title string `json:"title"`
	Year  int    `json:"first_publish_year"`
}

func (am *AuthorMonitor) searchOpenLibrary(authorName string) ([]openLibraryWork, error) {
	searchURL := fmt.Sprintf("%s/search.json?author=%s&sort=new&limit=50&fields=key,title,author_name,first_publish_year",
		am.baseURL, url.QueryEscape(authorName))

	req, err := http.NewRequest(http.MethodGet, searchURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Librarr (book download manager; github.com/JeremiahM37/librarr)")
	resp, err := am.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Open Library returned status %d", resp.StatusCode)
	}

	var result struct {
		Docs []struct {
			Key              string   `json:"key"`
			Title            string   `json:"title"`
			FirstPublishYear int      `json:"first_publish_year"`
			AuthorName       []string `json:"author_name"`
		} `json:"docs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	want := strings.ToLower(strings.TrimSpace(authorName))
	var works []openLibraryWork
	seenKeys := make(map[string]bool)
	for _, doc := range result.Docs {
		if doc.Title == "" {
			continue
		}
		authorMatch := false
		for _, a := range doc.AuthorName {
			al := strings.ToLower(a)
			if al == want || strings.Contains(al, want) {
				authorMatch = true
				break
			}
		}
		if !authorMatch {
			continue
		}
		key := doc.Key
		if key == "" {
			// No work key: fall back to a title-derived key so the work is
			// still tracked rather than announced on every check.
			key = "title:" + strings.ToLower(strings.TrimSpace(doc.Title))
		}
		if seenKeys[key] {
			continue
		}
		seenKeys[key] = true
		works = append(works, openLibraryWork{Key: key, Title: doc.Title, Year: doc.FirstPublishYear})
	}

	return works, nil
}
