package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
)

func newWantedMonitorDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "wanted-monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func TestWantedMonitorSearchOneUpdatesWantedBookAndHistory(t *testing.T) {
	database := newWantedMonitorDB(t)
	defer database.Close()

	book, err := database.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}

	var apiKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey = r.Header.Get("X-Api-Key")
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"title":       "The Martian EPUB",
			"downloadUrl": "http://example.test/download",
			"protocol":    "torrent",
			"seeders":     15,
			"infoHash":    "abc123",
		}})
	}))
	defer server.Close()

	cfg := &config.Config{
		ProwlarrURL:          server.URL,
		ProwlarrAPIKey:       "prowlarr-key",
		WantedMaxResultsKeep: 10,
	}
	monitor := NewWantedMonitor(cfg, database, nil, server.Client())

	updated, err := monitor.SearchOne(context.Background(), book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if apiKey != "prowlarr-key" {
		t.Fatalf("api key = %q, want prowlarr-key", apiKey)
	}
	if updated.Status != "found" {
		t.Fatalf("status = %q, want found", updated.Status)
	}
	if updated.LastResultCount != 1 {
		t.Fatalf("last result count = %d, want 1", updated.LastResultCount)
	}
	if updated.LastMatchTitle == "" || updated.LastSearch == nil {
		t.Fatalf("updated wanted book missing search fields: %+v", updated)
	}
	releases, err := database.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 {
		t.Fatalf("stored releases len = %d, want 1", len(releases))
	}
	if releases[0].Title != "The Martian EPUB" || releases[0].Score <= 0 || releases[0].DownloadURL == "" {
		t.Fatalf("stored release = %+v", releases[0])
	}

	history, err := database.ListWantedSearchHistory(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	if !history[0].Success || history[0].BestMatchTitle == "" {
		t.Fatalf("history entry = %+v", history[0])
	}
}

func TestWantedMonitorReplacesStoredReleasesOnMultipleSearches(t *testing.T) {
	database := newWantedMonitorDB(t)
	defer database.Close()

	book, err := database.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}

	call := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		title := "The Martian EPUB"
		if call > 1 {
			title = "The Martian MOBI"
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"title":       title,
			"downloadUrl": "http://example.test/download",
			"protocol":    "torrent",
			"seeders":     15,
			"infoHash":    title,
			"publishDate": "2026-07-27T12:00:00Z",
			"grabs":       3,
			"categories":  []int{7020},
		}})
	}))
	defer server.Close()

	cfg := &config.Config{
		ProwlarrURL:          server.URL,
		ProwlarrAPIKey:       "prowlarr-key",
		WantedMaxResultsKeep: 10,
	}
	monitor := NewWantedMonitor(cfg, database, nil, server.Client())
	if _, err := monitor.SearchOne(context.Background(), book.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := monitor.SearchOne(context.Background(), book.ID); err != nil {
		t.Fatal(err)
	}
	releases, err := database.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 || releases[0].Title != "The Martian MOBI" {
		t.Fatalf("releases after replacement = %+v", releases)
	}
	if releases[0].Grabs != 3 || len(releases[0].Categories) != 1 || releases[0].Categories[0] != "7020" {
		t.Fatalf("metadata was not retained: %+v", releases[0])
	}
}

func TestWantedMonitorFailedSearchKeepsStoredOriginRelease(t *testing.T) {
	database := newWantedMonitorDB(t)
	defer database.Close()

	book, err := database.CreateWantedBook(models.WantedBook{
		Title:           "American Marxism",
		Author:          "Mark R. Levin",
		MediaType:       "ebook",
		PreferredFormat: "epub",
		Monitored:       true,
		Status:          "found",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ReplaceWantedReleases(book.ID, []models.WantedRelease{{
		Title:       "American Marxism by Mark R Levin [ENG / EPUB PDF]",
		Indexer:     "MyAnonamouse",
		Protocol:    "torrent",
		Format:      "epub",
		Language:    "en",
		DownloadURL: "https://prowlarr.example/download/123",
		Score:       100,
		SearchTime:  time.Now().UTC(),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.MarkWantedOriginFound(book.ID, "American Marxism by Mark R Levin [ENG / EPUB PDF]", 100); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "prowlarr down", http.StatusBadGateway)
	}))
	defer server.Close()

	cfg := &config.Config{
		ProwlarrURL:          server.URL,
		ProwlarrAPIKey:       "prowlarr-key",
		WantedMaxResultsKeep: 10,
	}
	monitor := NewWantedMonitor(cfg, database, nil, server.Client())
	if _, err := monitor.SearchOne(context.Background(), book.ID); err != nil {
		t.Fatal(err)
	}
	releases, err := database.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 || releases[0].Title != "American Marxism by Mark R Levin [ENG / EPUB PDF]" || releases[0].DownloadURL == "" {
		t.Fatalf("origin release was not preserved after failed search: %+v", releases)
	}
	updated, err := database.GetWantedBook(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != "found" || updated.LastResultCount != 1 || updated.LastMatchTitle == "" {
		t.Fatalf("origin found state was not preserved after empty/failed search: %+v", updated)
	}
}

func TestWantedSearchQueriesUseCanonicalMetadata(t *testing.T) {
	item := models.WantedBook{
		Title:              "Rebel Prince: The Power, Passion and Defiance of Prince Charles",
		Author:             "Tom Bower",
		ISBN:               "9780000000000",
		ASIN:               "B000TEST",
		OriginReleaseTitle: "Rebel Prince, the Power, Passion and Defiance of Prince Charles by Tom Bower [ENG / MOBI]",
	}
	queries := wantedSearchQueries(item)
	want := []string{
		"9780000000000",
		"Rebel Prince The Power Passion and Defiance of Prince Charles Tom Bower",
		"Rebel Prince The Power Passion and Defiance of Prince Charles",
		"B000TEST",
	}
	if strings.Join(queries, "|") != strings.Join(want, "|") {
		t.Fatalf("queries = %#v, want %#v", queries, want)
	}
	for _, query := range queries {
		if strings.Contains(query, "[ENG / MOBI]") || strings.Contains(query, " by Tom Bower ") {
			t.Fatalf("query used raw release title: %q", query)
		}
	}
}

func TestWantedMonitorStillFindsOriginalReleaseResult(t *testing.T) {
	database := newWantedMonitorDB(t)
	defer database.Close()

	book, err := database.CreateWantedBook(models.WantedBook{
		Title:              "Rebel Prince: The Power, Passion and Defiance of Prince Charles",
		Author:             "Tom Bower",
		MediaType:          "ebook",
		PreferredFormat:    "mobi",
		OriginReleaseTitle: "Rebel Prince, the Power, Passion and Defiance of Prince Charles by Tom Bower [ENG / MOBI]",
		Monitored:          true,
		Status:             "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}

	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"title":       "Rebel Prince, the Power, Passion and Defiance of Prince Charles by Tom Bower [ENG / MOBI]",
			"downloadUrl": "http://example.test/download",
			"protocol":    "torrent",
			"seeders":     7,
			"infoHash":    "rebel-prince",
		}})
	}))
	defer server.Close()

	cfg := &config.Config{
		ProwlarrURL:          server.URL,
		ProwlarrAPIKey:       "prowlarr-key",
		WantedMaxResultsKeep: 10,
	}
	monitor := NewWantedMonitor(cfg, database, nil, server.Client())
	updated, err := monitor.SearchOne(context.Background(), book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != "found" || updated.LastResultCount != 1 {
		t.Fatalf("updated = %+v", updated)
	}
	if len(queries) == 0 || strings.Contains(queries[0], "[ENG / MOBI]") {
		t.Fatalf("queries = %#v", queries)
	}
}

func TestWantedMonitorSearchAllSkipsFailedItemsWhenRetryDisabled(t *testing.T) {
	database := newWantedMonitorDB(t)
	defer database.Close()

	first, err := database.CreateWantedBook(models.WantedBook{
		Title:     "Project Hail Mary",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.CreateWantedBook(models.WantedBook{
		Title:     "Artemis",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.UpdateWantedSearch(first.ID, db.WantedSearchUpdate{
		Status:      "wanted",
		LastSearch:  time.Now().UTC(),
		LastSuccess: false,
		LastError:   "temporary failure",
	})
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"title":       "Artemis EPUB",
			"downloadUrl": "http://example.test/download",
			"protocol":    "torrent",
			"seeders":     10,
			"infoHash":    "hash-artemis",
		}})
	}))
	defer server.Close()

	cfg := &config.Config{
		ProwlarrURL:          server.URL,
		ProwlarrAPIKey:       "prowlarr-key",
		WantedRetryFailures:  false,
		WantedMaxResultsKeep: 10,
	}
	monitor := NewWantedMonitor(cfg, database, nil, server.Client())

	summary, err := monitor.SearchAll(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Searched != 1 {
		t.Fatalf("searched = %d, want 1", summary.Searched)
	}

	refetchedFirst, err := database.GetWantedBook(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if refetchedFirst.LastError != "temporary failure" {
		t.Fatalf("first book last error = %q, want preserved failure", refetchedFirst.LastError)
	}
	refetchedSecond, err := database.GetWantedBook(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if refetchedSecond.Status != "found" {
		t.Fatalf("second status = %q, want found", refetchedSecond.Status)
	}
}
