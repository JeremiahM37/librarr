// Package search implements librarr's search sources (Anna's Archive,
// Prowlarr, Project Gutenberg, and others) plus result filtering,
// scoring, and source health tracking.
package search

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// Searcher is the interface that all search sources implement.
type Searcher interface {
	Name() string
	Label() string
	Enabled() bool
	Search(ctx context.Context, query string) ([]models.SearchResult, error)
	// SearchTab returns which search tab this source serves: "main", "audiobook", or "manga".
	SearchTab() string
	// DownloadType returns "direct" or "torrent".
	DownloadType() string
}

// Manager coordinates searches across multiple sources with circuit breaker support.
type Manager struct {
	cfg     *config.Config
	sources []Searcher
	health  *HealthTracker

	filterMu          sync.RWMutex
	foreignLangFilter bool // runtime-configurable via UI; initialised from config
}

// SourceSearchUpdate reports one source's contribution to a streaming search.
type SourceSearchUpdate struct {
	Source       string
	Results      []models.SearchResult
	Err          error
	CircuitOpen  bool
	Done         bool
	ElapsedMilli int64
}

// ResultProcessingStats reports how many upstream results survived local
// post-processing. It is diagnostic data for Discover responses and logs.
type ResultProcessingStats struct {
	UpstreamResults  int            `json:"upstream_results"`
	FilteredResults  int            `json:"filtered_results"`
	ReturnedResults  int            `json:"returned_results"`
	UpstreamBySource map[string]int `json:"upstream_by_source,omitempty"`
	FilteredBySource map[string]int `json:"filtered_by_source,omitempty"`
	ReturnedBySource map[string]int `json:"returned_by_source,omitempty"`
}

// NewManager creates a search manager with the given sources.
func NewManager(cfg *config.Config, sources []Searcher, health *HealthTracker) *Manager {
	return &Manager{
		cfg:               cfg,
		sources:           sources,
		health:            health,
		foreignLangFilter: cfg.ForeignLangFilter,
	}
}

// SetForeignLangFilter updates the foreign-language filter at runtime.
func (m *Manager) SetForeignLangFilter(enabled bool) {
	m.filterMu.Lock()
	defer m.filterMu.Unlock()
	m.foreignLangFilter = enabled
}

// ForeignLangFilterEnabled returns the current foreign-language filter state.
func (m *Manager) ForeignLangFilterEnabled() bool {
	m.filterMu.RLock()
	defer m.filterMu.RUnlock()
	return m.foreignLangFilter
}

// Search runs a query against all enabled sources for the given tab, returning combined results.
// Use SearchWithAuthor for scored results.
func (m *Manager) Search(ctx context.Context, tab, query string) ([]models.SearchResult, int64) {
	return m.SearchWithAuthor(ctx, tab, query, "")
}

// SearchWithAuthor runs a query and scores results using the provided author hint.
func (m *Manager) SearchWithAuthor(ctx context.Context, tab, query, author string) ([]models.SearchResult, int64) {
	results, elapsed := m.searchRaw(ctx, tab, query)
	results = m.processResults(results, query, author)
	return results, elapsed
}

// DiscoverSearchWithAuthor runs a free-form Discover query. Unlike Wanted
// monitor matching, Discover must allow broad title, author, and ISBN searches.
func (m *Manager) DiscoverSearchWithAuthor(ctx context.Context, tab, query, author string) ([]models.SearchResult, int64, ResultProcessingStats) {
	results, elapsed := m.searchRaw(ctx, tab, query)
	results, stats := m.ProcessDiscoverResults(results, query, author)
	return results, elapsed, stats
}

func (m *Manager) searchRaw(ctx context.Context, tab, query string) ([]models.SearchResult, int64) {
	start := time.Now()
	var (
		mu      sync.Mutex
		results []models.SearchResult
		wg      sync.WaitGroup
	)

	for _, s := range m.sources {
		if !s.Enabled() || s.SearchTab() != tab {
			continue
		}
		if !m.health.CanSearch(s.Name()) {
			slog.Warn("source circuit open, skipping", "source", s.Name())
			continue
		}

		wg.Add(1)
		go func(src Searcher) {
			defer wg.Done()

			searchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			res, err := src.Search(searchCtx, query)
			if err != nil {
				if ctx.Err() != nil {
					slog.Debug("search canceled by client", "source", src.Name())
					return
				}
				slog.Error("search failed", "source", src.Name(), "error", err)
				m.health.RecordFailure(src.Name(), err.Error(), "search")
				return
			}

			m.health.RecordSuccess(src.Name(), "search")

			// Set source field on all results.
			for i := range res {
				if res[i].Source == "" {
					res[i].Source = src.Name()
				}
			}

			mu.Lock()
			results = append(results, res...)
			mu.Unlock()
		}(s)
	}

	wg.Wait()

	elapsed := time.Since(start).Milliseconds()
	return results, elapsed
}

// SearchStream runs a query and emits an update when each source finishes.
func (m *Manager) SearchStream(ctx context.Context, tab, query, author string) <-chan SourceSearchUpdate {
	start := time.Now()
	updates := make(chan SourceSearchUpdate, len(m.sources))

	go func() {
		defer close(updates)
		var wg sync.WaitGroup
		sendUpdate := func(update SourceSearchUpdate) bool {
			select {
			case updates <- update:
				return true
			case <-ctx.Done():
				return false
			}
		}

		for _, s := range m.sources {
			if !s.Enabled() || s.SearchTab() != tab {
				continue
			}
			if !m.health.CanSearch(s.Name()) {
				slog.Warn("source circuit open, skipping", "source", s.Name())
				if !sendUpdate(SourceSearchUpdate{
					Source:       s.Name(),
					CircuitOpen:  true,
					Done:         true,
					ElapsedMilli: time.Since(start).Milliseconds(),
				}) {
					return
				}
				continue
			}

			wg.Add(1)
			go func(src Searcher) {
				defer wg.Done()

				searchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				res, err := src.Search(searchCtx, query)
				if err != nil {
					if ctx.Err() != nil {
						slog.Debug("search canceled by client", "source", src.Name())
						return
					}
					slog.Error("search failed", "source", src.Name(), "error", err)
					m.health.RecordFailure(src.Name(), err.Error(), "search")
					sendUpdate(SourceSearchUpdate{
						Source:       src.Name(),
						Err:          err,
						Done:         true,
						ElapsedMilli: time.Since(start).Milliseconds(),
					})
					return
				}

				m.health.RecordSuccess(src.Name(), "search")
				for i := range res {
					if res[i].Source == "" {
						res[i].Source = src.Name()
					}
				}
				sendUpdate(SourceSearchUpdate{
					Source:       src.Name(),
					Results:      res,
					Done:         true,
					ElapsedMilli: time.Since(start).Milliseconds(),
				})
			}(s)
		}
		wg.Wait()
	}()

	return updates
}

// ProcessResults applies canonical-title post-processing used by Wanted and
// automation paths that are searching for one known book.
func (m *Manager) ProcessResults(results []models.SearchResult, query, author string) []models.SearchResult {
	return m.processResults(results, query, author)
}

// ProcessDiscoverResults applies shared safety, language, size, duplicate, and
// scoring behavior without canonical-title filtering. That keeps free-form
// Discover searches separate from Wanted identity matching.
func (m *Manager) ProcessDiscoverResults(results []models.SearchResult, query, author string) ([]models.SearchResult, ResultProcessingStats) {
	stats := ResultProcessingStats{
		UpstreamResults:  len(results),
		UpstreamBySource: countResultsBySource(results),
	}
	results = FilterDiscoverResults(results, m.ForeignLangFilterEnabled())
	results = FilterAndSortResults(results, query, m.cfg.MinTorrentSizeBytes, m.cfg.MaxTorrentSizeBytes)
	stats.ReturnedResults = len(results)
	stats.FilteredResults = stats.UpstreamResults - stats.ReturnedResults
	stats.ReturnedBySource = countResultsBySource(results)
	stats.FilteredBySource = subtractSourceCounts(stats.UpstreamBySource, stats.ReturnedBySource)
	results = ScoreResults(results, query, author)
	sortByScoreDesc(results)
	return results, stats
}

func (m *Manager) processResults(results []models.SearchResult, query, author string) []models.SearchResult {
	results = FilterResults(results, query, m.ForeignLangFilterEnabled())
	results = FilterAndSortResults(results, query, m.cfg.MinTorrentSizeBytes, m.cfg.MaxTorrentSizeBytes)
	results = ScoreResults(results, query, author)
	sortByScoreDesc(results)
	return results
}

func sortByScoreDesc(results []models.SearchResult) {
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

func countResultsBySource(results []models.SearchResult) map[string]int {
	counts := make(map[string]int)
	for _, result := range results {
		counts[diagnosticSourceName(result)]++
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}

func subtractSourceCounts(before, after map[string]int) map[string]int {
	out := make(map[string]int)
	for source, count := range before {
		if delta := count - after[source]; delta > 0 {
			out[source] = delta
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func diagnosticSourceName(result models.SearchResult) string {
	if result.Source == "torrent" && strings.TrimSpace(result.Indexer) != "" {
		return "prowlarr"
	}
	if strings.TrimSpace(result.Source) != "" {
		return result.Source
	}
	return "unknown"
}

// GetSources returns all registered sources.
func (m *Manager) GetSources() []Searcher {
	return m.sources
}

// GetSource returns a source by name or nil.
func (m *Manager) GetSource(name string) Searcher {
	for _, s := range m.sources {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

// SourceMeta returns metadata about all sources for API responses.
func (m *Manager) SourceMeta() []map[string]interface{} {
	var meta []map[string]interface{}
	snapshot := m.health.Snapshot()
	for _, s := range m.sources {
		entry := map[string]interface{}{
			"name":          s.Name(),
			"label":         s.Label(),
			"enabled":       s.Enabled(),
			"search_tab":    s.SearchTab(),
			"download_type": s.DownloadType(),
		}
		if h, ok := snapshot[s.Name()]; ok {
			entry["health"] = h
		}
		meta = append(meta, entry)
	}
	return meta
}

// FilterResults removes irrelevant, foreign-language, or suspicious results.
// Sources that naturally return non-English content (Flibusta, Z-Library) bypass
// the foreign-language filter automatically — their results are always preserved.
// The foreignLangFilter flag only affects sources that primarily return English content.
func FilterResults(results []models.SearchResult, query string, foreignLangFilter bool) []models.SearchResult {
	var filtered []models.SearchResult
	for _, r := range results {
		if foreignLangFilter && isForeignTitle(r.Title) && !isMultilangSource(r.Source) {
			continue
		}
		if !titleRelevant(r.Title, query) {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

// FilterDiscoverResults applies language filtering without requiring title
// relevance. Discover accepts broad free-form searches, including author-only
// queries where valid release titles may not contain the query text.
func FilterDiscoverResults(results []models.SearchResult, foreignLangFilter bool) []models.SearchResult {
	var filtered []models.SearchResult
	for _, r := range results {
		if foreignLangFilter && isForeignTitle(r.Title) && !isMultilangSource(r.Source) {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

// isMultilangSource returns true for sources that naturally return non-English content.
// Results from these sources bypass the foreign-language filter automatically.
func isMultilangSource(source string) bool {
	switch source {
	case "flibusta", "zlibrary", "booktracker", "booktracker_audiobook":
		return true
	}
	return false
}

var foreignKeywords = map[string]bool{
	"norwegian": true, "deutsch": true, "german": true, "french": true,
	"francais": true, "spanish": true, "espanol": true, "italian": true,
	"italiano": true, "portuguese": true, "russian": true, "chinese": true,
	"korean": true, "arabic": true, "hindi": true, "turkish": true,
	"polish": true, "dutch": true, "swedish": true, "danish": true,
	"finnish": true, "czech": true, "hungarian": true, "romanian": true,
	"thai": true, "vietnamese": true, "indonesian": true, "malay": true,
}

func isForeignTitle(title string) bool {
	lower := strings.ToLower(title)
	for kw := range foreignKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}

	// Check for high proportion of non-Latin characters.
	var nonLatin, total int
	for _, ch := range title {
		if unicode.IsLetter(ch) {
			total++
			if !unicode.In(ch, unicode.Latin) {
				nonLatin++
			}
		}
	}
	if total > 0 && float64(nonLatin)/float64(total) > 0.3 {
		return true
	}
	return false
}

var wordRe = regexp.MustCompile(`\w+`)

var stopwords = map[string]bool{
	"the": true, "a": true, "an": true, "of": true, "in": true,
	"on": true, "at": true, "to": true, "for": true, "and": true,
	"or": true, "is": true, "it": true, "by": true,
}

func titleRelevant(title, query string) bool {
	if query == "" {
		return true
	}
	tLower := strings.ToLower(title)
	qLower := strings.ToLower(query)

	// Direct substring match.
	if strings.Contains(tLower, qLower) || strings.Contains(qLower, tLower) {
		return true
	}

	// Word overlap check.
	qWords := extractWords(qLower)
	tWords := extractWords(tLower)
	if len(qWords) == 0 {
		return true
	}

	overlap := 0
	for w := range qWords {
		if tWords[w] {
			overlap++
		}
	}

	return float64(overlap)/float64(len(qWords)) >= 0.5
}

func extractWords(s string) map[string]bool {
	words := make(map[string]bool)
	for _, w := range wordRe.FindAllString(s, -1) {
		w = strings.ToLower(w)
		if !stopwords[w] && len(w) > 1 {
			words[w] = true
		}
	}
	return words
}
