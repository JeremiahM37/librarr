package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/search"
	"github.com/jamie75/librarr/internal/webhook"
)

// SeriesInfo holds detected series data.
type SeriesInfo struct {
	ID           int64     `json:"id"`
	SeriesName   string    `json:"series_name"`
	KnownTotal   int       `json:"known_total"`
	OwnedCount   int       `json:"owned_count"`
	OwnedBooks   []string  `json:"owned_books,omitempty"`
	MissingBooks []string  `json:"missing_books,omitempty"`
	LastChecked  time.Time `json:"last_checked"`
}

// SeriesDetector analyzes the library for series patterns.
type SeriesDetector struct {
	db            *db.DB
	searchMgr     *search.Manager
	webhookSender *webhook.Sender
	httpClient    *http.Client
}

// NewSeriesDetector creates a new series detector.
func NewSeriesDetector(database *db.DB, searchMgr *search.Manager, ws *webhook.Sender) *SeriesDetector {
	return &SeriesDetector{
		db:            database,
		searchMgr:     searchMgr,
		webhookSender: ws,
		httpClient:    &http.Client{Timeout: 15 * time.Second},
	}
}

// Common patterns for detecting series in titles.
var seriesPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^(.+?)[\s,:-]+(?:book|vol\.?|volume|#)\s*(\d+)`),
	regexp.MustCompile(`(?i)^(.+?)\s*\((\d+)\)`),
	regexp.MustCompile(`(?i)^(.+?)\s+(\d+)$`),
}

// DetectedSeries represents a series found in the library.
type DetectedSeries struct {
	Name       string
	OwnedBooks map[int]string // book number -> title
}

// DetectSeries scans the library for series patterns and returns detected series.
func (d *SeriesDetector) DetectSeries() ([]SeriesInfo, error) {
	items, err := d.db.GetItems("", 100000, 0)
	if err != nil {
		return nil, err
	}

	// Group items by detected series name.
	seriesMap := make(map[string]*DetectedSeries)

	for _, item := range items {
		for _, pat := range seriesPatterns {
			matches := pat.FindStringSubmatch(item.Title)
			if len(matches) >= 3 {
				seriesName := strings.TrimSpace(matches[1])
				bookNum, err := strconv.Atoi(matches[2])
				if err != nil || bookNum < 1 || bookNum > 100 {
					continue
				}

				key := strings.ToLower(seriesName)
				if _, ok := seriesMap[key]; !ok {
					seriesMap[key] = &DetectedSeries{
						Name:       seriesName,
						OwnedBooks: make(map[int]string),
					}
				}
				seriesMap[key].OwnedBooks[bookNum] = item.Title
				break // Use first matching pattern
			}
		}
	}

	// Build series info with missing book detection.
	var result []SeriesInfo
	for _, series := range seriesMap {
		if len(series.OwnedBooks) < 2 {
			continue // Need at least 2 books to detect a series
		}

		// Find max book number.
		maxNum := 0
		for num := range series.OwnedBooks {
			if num > maxNum {
				maxNum = num
			}
		}

		// Try to get total from Open Library.
		total := maxNum
		olTotal := d.getOpenLibrarySeriesTotal(series.Name)
		if olTotal > total {
			total = olTotal
		}

		var owned []string
		var missing []string
		for i := 1; i <= total; i++ {
			if title, ok := series.OwnedBooks[i]; ok {
				owned = append(owned, title)
			} else {
				missing = append(missing, fmt.Sprintf("%s Book %d", series.Name, i))
			}
		}

		info := SeriesInfo{
			SeriesName:   series.Name,
			KnownTotal:   total,
			OwnedCount:   len(series.OwnedBooks),
			OwnedBooks:   owned,
			MissingBooks: missing,
			LastChecked:  time.Now(),
		}

		// Save/update in DB.
		id, _ := d.db.UpsertSeriesTracking(info.SeriesName, info.KnownTotal, info.OwnedCount)
		info.ID = id

		result = append(result, info)
	}

	return result, nil
}

// GetMissing returns missing books for a named series.
func (d *SeriesDetector) GetMissing(seriesName string) ([]string, error) {
	allSeries, err := d.DetectSeries()
	if err != nil {
		return nil, err
	}

	for _, s := range allSeries {
		if strings.EqualFold(s.SeriesName, seriesName) {
			return s.MissingBooks, nil
		}
	}

	return nil, fmt.Errorf("series not found: %s", seriesName)
}

// SearchMissing searches for missing books in a series.
func (d *SeriesDetector) SearchMissing(seriesName string) ([]models.SearchResult, error) {
	missing, err := d.GetMissing(seriesName)
	if err != nil {
		return nil, err
	}

	var allResults []models.SearchResult
	for _, title := range missing {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		results, _ := d.searchMgr.Search(ctx, "main", title)
		cancel()

		if len(results) > 0 {
			// Take only the best result for each missing book.
			best := results[0]
			if best.Score >= 50 {
				allResults = append(allResults, best)
			}
		}

		time.Sleep(3 * time.Second)
	}

	// Send webhook if missing books found.
	if d.webhookSender != nil && len(allResults) > 0 {
		d.webhookSender.Send(webhook.Payload{
			Event:   webhook.EventSeriesMissing,
			Title:   "Series: " + seriesName,
			Message: fmt.Sprintf("Found %d searchable missing books", len(allResults)),
			Status:  "info",
		})
	}

	return allResults, nil
}

// getOpenLibrarySeriesTotal tries to find total book count from Open Library.
func (d *SeriesDetector) getOpenLibrarySeriesTotal(seriesName string) int {
	u := fmt.Sprintf("https://openlibrary.org/search.json?q=%s&limit=5", url.QueryEscape(seriesName))
	req, err := http.NewRequestWithContext(context.Background(), "GET", u, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("User-Agent", "Librarr/2.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0
	}

	var data struct {
		NumFound int `json:"numFound"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0
	}

	// Cap at reasonable numbers.
	if data.NumFound > 50 {
		return 0 // Too many results, not a useful series count
	}
	return data.NumFound
}

// ScanForScheduler is called by the scheduler to check all tracked series for missing books.
func (d *SeriesDetector) ScanForScheduler() {
	series, err := d.DetectSeries()
	if err != nil {
		slog.Error("series scan failed", "error", err)
		return
	}

	for _, s := range series {
		if len(s.MissingBooks) > 0 {
			slog.Info("series incomplete",
				"series", s.SeriesName,
				"owned", s.OwnedCount,
				"total", s.KnownTotal,
				"missing", len(s.MissingBooks),
			)
		}
	}
}
