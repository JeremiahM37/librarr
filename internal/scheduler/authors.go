package scheduler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/webhook"
)

// AuthorMonitor periodically checks for new books by monitored authors.
type AuthorMonitor struct {
	cfg           *config.Config
	db            *db.DB
	webhookSender *webhook.Sender
	client        *http.Client
}

// NewAuthorMonitor creates a new author monitor.
func NewAuthorMonitor(cfg *config.Config, database *db.DB, ws *webhook.Sender) *AuthorMonitor {
	return &AuthorMonitor{
		cfg:           cfg,
		db:            database,
		webhookSender: ws,
		client:        &http.Client{Timeout: 15 * time.Second},
	}
}

// CheckAuthors checks all monitored authors for new books.
func (am *AuthorMonitor) CheckAuthors() {
	if !am.cfg.AuthorMonitorEnabled {
		return
	}

	authors, err := am.db.GetMonitoredAuthors()
	if err != nil {
		slog.Error("failed to get monitored authors", "error", err)
		return
	}

	now := time.Now()
	for _, author := range authors {
		interval := time.Duration(author.CheckIntervalDays) * 24 * time.Hour
		if !author.LastChecked.IsZero() && now.Sub(author.LastChecked) < interval {
			continue
		}

		slog.Info("checking monitored author", "author", author.Name)
		am.checkAuthor(author)
	}
}

func (am *AuthorMonitor) checkAuthor(author db.MonitoredAuthor) {
	books, err := am.searchOpenLibrary(author.Name)
	if err != nil {
		slog.Warn("failed to search Open Library for author", "author", author.Name, "error", err)
		am.db.UpdateMonitoredAuthorCheck(author.ID, author.LastBookFound)
		return
	}

	if len(books) == 0 {
		am.db.UpdateMonitoredAuthorCheck(author.ID, author.LastBookFound)
		return
	}

	// Find newest book.
	newest := books[0]
	for _, b := range books[1:] {
		if b.Year > newest.Year {
			newest = b
		}
	}

	if newest.Title != author.LastBookFound && newest.Title != "" {
		slog.Info("new book found for monitored author",
			"author", author.Name,
			"title", newest.Title,
			"year", newest.Year,
		)

		// Send notification.
		if am.webhookSender != nil {
			am.webhookSender.Send(webhook.Payload{
				Event:   webhook.EventInfo,
				Title:   fmt.Sprintf("New book by %s", author.Name),
				Message: fmt.Sprintf("Found: %s (%d)", newest.Title, newest.Year),
				Status:  "info",
				Extra: map[string]interface{}{
					"author": author.Name,
					"title":  newest.Title,
					"year":   newest.Year,
				},
			})
		}

		// Create in-app notification for all admin users.
		users, _ := am.db.ListUsers()
		for _, u := range users {
			if u.Role == "admin" {
				am.db.CreateNotification(&models.Notification{
					UserID:    u.ID,
					Type:      "author_new_book",
					Title:     fmt.Sprintf("New book by %s", author.Name),
					Message:   fmt.Sprintf("%s (%d)", newest.Title, newest.Year),
					CreatedAt: time.Now(),
				})
			}
		}
	}

	am.db.UpdateMonitoredAuthorCheck(author.ID, newest.Title)
}

// openLibraryWork represents a work returned by Open Library search.
type openLibraryWork struct {
	Title string `json:"title"`
	Year  int    `json:"first_publish_year"`
}

func (am *AuthorMonitor) searchOpenLibrary(authorName string) ([]openLibraryWork, error) {
	searchURL := fmt.Sprintf("https://openlibrary.org/search.json?author=%s&sort=new&limit=5",
		url.QueryEscape(authorName))

	resp, err := am.client.Get(searchURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Open Library returned status %d", resp.StatusCode)
	}

	var result struct {
		Docs []struct {
			Title            string   `json:"title"`
			FirstPublishYear int      `json:"first_publish_year"`
			AuthorName       []string `json:"author_name"`
		} `json:"docs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var works []openLibraryWork
	for _, doc := range result.Docs {
		// Verify author match.
		authorMatch := false
		for _, a := range doc.AuthorName {
			if strings.EqualFold(a, authorName) {
				authorMatch = true
				break
			}
			// Also check partial match.
			if strings.Contains(strings.ToLower(a), strings.ToLower(authorName)) {
				authorMatch = true
				break
			}
		}
		if !authorMatch {
			continue
		}
		works = append(works, openLibraryWork{
			Title: doc.Title,
			Year:  doc.FirstPublishYear,
		})
	}

	return works, nil
}
