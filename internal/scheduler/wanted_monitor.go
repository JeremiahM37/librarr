package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/search"
	wantedmeta "github.com/jamie75/librarr/internal/wanted"
)

type WantedMonitor struct {
	cfg       *config.Config
	db        *db.DB
	searchMgr *search.Manager
	client    *http.Client

	mu      sync.Mutex
	running bool
}

type WantedSearchSummary struct {
	Searched   int                  `json:"searched"`
	Updated    []*models.WantedBook `json:"updated,omitempty"`
	StartedAt  time.Time            `json:"started_at"`
	FinishedAt time.Time            `json:"finished_at"`
}

func NewWantedMonitor(cfg *config.Config, database *db.DB, searchMgr *search.Manager, client *http.Client) *WantedMonitor {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &WantedMonitor{
		cfg:       cfg,
		db:        database,
		searchMgr: searchMgr,
		client:    client,
	}
}

func (m *WantedMonitor) Start(ctx context.Context) {
	if m == nil || m.cfg == nil || !m.cfg.WantedMonitorEnabled {
		slog.Info("wanted monitor disabled")
		return
	}
	interval, ok := m.cfg.WantedSearchIntervalDuration()
	if !ok {
		slog.Info("wanted monitor configured for manual-only mode")
		return
	}
	slog.Info("wanted monitor started", "interval", interval, "retry_failures", m.cfg.WantedRetryFailures)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("wanted monitor stopped")
			return
		case <-ticker.C:
			runCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
			_, err := m.SearchAll(runCtx, false)
			cancel()
			if err != nil {
				slog.Warn("wanted monitor run failed", "error", err)
			}
		}
	}
}

func (m *WantedMonitor) SearchAll(ctx context.Context, includeFailures bool) (*WantedSearchSummary, error) {
	if !m.beginRun() {
		return nil, fmt.Errorf("wanted monitor already running")
	}
	defer m.endRun()

	items, err := m.db.ListMonitoredWantedBooks()
	if err != nil {
		return nil, err
	}
	summary := &WantedSearchSummary{StartedAt: time.Now().UTC()}
	for _, item := range items {
		if !includeFailures && !m.cfg.WantedRetryFailures && item.LastSearch != nil && !item.LastSuccess && strings.TrimSpace(item.LastError) != "" {
			continue
		}
		updated, err := m.searchOne(ctx, item)
		if err != nil {
			return nil, err
		}
		summary.Searched++
		summary.Updated = append(summary.Updated, updated)
	}
	summary.FinishedAt = time.Now().UTC()
	return summary, nil
}

func (m *WantedMonitor) SearchOne(ctx context.Context, id int64) (*models.WantedBook, error) {
	if !m.beginRun() {
		return nil, fmt.Errorf("wanted monitor already running")
	}
	defer m.endRun()

	item, err := m.db.GetWantedBook(id)
	if err != nil {
		return nil, err
	}
	return m.searchOne(ctx, *item)
}

func (m *WantedMonitor) beginRun() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return false
	}
	m.running = true
	return true
}

func (m *WantedMonitor) endRun() {
	m.mu.Lock()
	m.running = false
	m.mu.Unlock()
}

func (m *WantedMonitor) searchOne(ctx context.Context, item models.WantedBook) (*models.WantedBook, error) {
	now := time.Now().UTC()
	searchUpdate := db.WantedSearchUpdate{
		Status:      "searching",
		LastSearch:  now,
		LastSuccess: false,
	}
	if _, err := m.db.UpdateWantedSearch(item.ID, searchUpdate); err != nil {
		return nil, err
	}

	queries := wantedSearchQueries(item)
	results, queryUsed, err := m.runSearchQueries(ctx, item, queries)
	if err != nil {
		searchUpdate.Status = strings.TrimSpace(strings.ToLower(item.Status))
		if searchUpdate.Status == "" || searchUpdate.Status == "searching" {
			searchUpdate.Status = "wanted"
		}
		searchUpdate.LastResultCount = item.LastResultCount
		searchUpdate.LastError = sanitizeWantedError(err)
		searchUpdate.BestMatchScore = item.BestMatchScore
		searchUpdate.LastMatchTitle = item.LastMatchTitle
		searchUpdate.Query = queryUsed
		updated, updateErr := m.db.UpdateWantedSearch(item.ID, searchUpdate)
		if updateErr != nil {
			return nil, updateErr
		}
		_ = m.db.AddWantedSearchHistory(item.ID, searchUpdate)
		_ = m.db.PruneWantedSearchHistory(m.cfg.WantedMaxResultsKeep)
		return updated, nil
	}

	status := "missing"
	lastMatchTitle := ""
	bestScore := 0.0
	resultCount := len(results)
	if len(results) > 0 {
		status = "found"
		lastMatchTitle = results[0].Title
		bestScore = results[0].Score
	} else if item.LastResultCount > 0 || item.LastMatchTitle != "" {
		status = strings.TrimSpace(strings.ToLower(item.Status))
		if status == "" || status == "searching" || status == "wanted" {
			status = "found"
		}
		resultCount = item.LastResultCount
		lastMatchTitle = item.LastMatchTitle
		bestScore = item.BestMatchScore
	}
	searchUpdate = db.WantedSearchUpdate{
		Status:          status,
		LastSearch:      now,
		LastResultCount: resultCount,
		LastSuccess:     true,
		LastError:       "",
		BestMatchScore:  bestScore,
		LastMatchTitle:  lastMatchTitle,
		Query:           queryUsed,
	}
	if len(results) > 0 {
		if err := m.db.ReplaceWantedReleases(item.ID, wantedReleasesFromResults(item.ID, results, queryUsed, now)); err != nil {
			return nil, err
		}
	}
	updated, err := m.db.UpdateWantedSearch(item.ID, searchUpdate)
	if err != nil {
		return nil, err
	}
	if err := m.db.AddWantedSearchHistory(item.ID, searchUpdate); err != nil {
		return nil, err
	}
	if err := m.db.PruneWantedSearchHistory(m.cfg.WantedMaxResultsKeep); err != nil {
		slog.Warn("wanted monitor prune failed", "error", err)
	}
	return updated, nil
}

func wantedReleasesFromResults(wantedBookID int64, results []models.SearchResult, query string, searchedAt time.Time) []models.WantedRelease {
	releases := make([]models.WantedRelease, 0, len(results))
	for _, result := range results {
		protocol := strings.TrimSpace(strings.ToLower(result.DownloadProtocol))
		if protocol == "" {
			protocol = strings.TrimSpace(strings.ToLower(result.Source))
		}
		if protocol == "" {
			protocol = "torrent"
		}
		sizeHuman := result.SizeHuman
		if sizeHuman == "" && result.Size > 0 {
			sizeHuman = search.HumanSize(result.Size)
		}
		releases = append(releases, models.WantedRelease{
			WantedBookID: wantedBookID,
			Title:        result.Title,
			GUID:         firstNonBlankString(result.GUID, result.InfoHash, result.SourceID),
			Indexer:      result.Indexer,
			Protocol:     protocol,
			PublishDate:  result.PublishDate,
			Size:         result.Size,
			SizeHuman:    sizeHuman,
			Seeders:      result.Seeders,
			Leechers:     result.Leechers,
			Grabs:        result.Grabs,
			Language:     result.Language,
			Format:       result.Format,
			DownloadURL:  firstNonBlankString(result.DownloadURL, result.MagnetURL, result.URL),
			Categories:   append([]string(nil), result.Categories...),
			Score:        result.Score,
			SearchQuery:  query,
			SearchTime:   searchedAt,
		})
	}
	return releases
}

func firstNonBlankString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (m *WantedMonitor) runSearchQueries(ctx context.Context, item models.WantedBook, queries []string) ([]models.SearchResult, string, error) {
	if len(queries) == 0 {
		return nil, "", fmt.Errorf("wanted book has no usable search terms")
	}
	tab := wantedSearchTab(item.MediaType)
	searcher := search.NewProwlarr(m.cfg, m.client, tab)
	if !searcher.Enabled() {
		return nil, "", fmt.Errorf("Prowlarr is not configured")
	}

	seen := map[string]bool{}
	var all []models.SearchResult
	queryUsed := queries[0]
	for _, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		queryUsed = query
		results, err := searcher.Search(ctx, query)
		if err != nil {
			return nil, query, err
		}
		for _, result := range results {
			key := strings.Join([]string{
				strings.ToLower(strings.TrimSpace(result.InfoHash)),
				strings.ToLower(strings.TrimSpace(result.GUID)),
				strings.ToLower(strings.TrimSpace(result.Title)),
			}, "\x00")
			if seen[key] {
				continue
			}
			seen[key] = true
			all = append(all, result)
		}
		if len(all) > 0 && (normalizeSearchIdentifier(item.ISBN) != "" || normalizeSearchIdentifier(item.ASIN) != "") {
			break
		}
	}
	if m.searchMgr != nil {
		all = m.searchMgr.ProcessResults(all, item.Title, item.Author)
	} else {
		all = search.ScoreResults(all, item.Title, item.Author)
	}
	return all, queryUsed, nil
}

func wantedSearchQueries(item models.WantedBook) []string {
	var queries []string
	if isbn := normalizeSearchIdentifier(item.ISBN); isbn != "" {
		queries = append(queries, isbn)
	}
	title := wantedmeta.CleanSearchPhrase(item.Title)
	author := wantedmeta.CleanSearchPhrase(item.Author)
	if title != "" && author != "" {
		queries = append(queries, title+" "+author)
	}
	if title != "" {
		queries = append(queries, title)
	}
	if asin := normalizeSearchIdentifier(item.ASIN); asin != "" {
		queries = append(queries, asin)
	}
	return uniqueSearchQueries(queries)
}

func uniqueSearchQueries(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, query := range in {
		normalized := strings.ToLower(strings.TrimSpace(query))
		if normalized == "" || seen[normalized] {
			continue
		}
		seen[normalized] = true
		out = append(out, strings.TrimSpace(query))
	}
	return out
}

func wantedSearchTab(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "audiobook":
		return "audiobook"
	case "manga":
		return "manga"
	default:
		return "main"
	}
}

func normalizeSearchIdentifier(value string) string {
	replacer := strings.NewReplacer("-", "", " ", "")
	return replacer.Replace(strings.TrimSpace(value))
}

func sanitizeWantedError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if message == "" {
		return "search failed"
	}
	message = strings.ReplaceAll(message, "\n", " ")
	if len(message) > 240 {
		message = message[:240]
	}
	return message
}
