package search

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
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
}

// NewManager creates a search manager with the given sources.
func NewManager(cfg *config.Config, sources []Searcher, health *HealthTracker) *Manager {
	return &Manager{
		cfg:     cfg,
		sources: sources,
		health:  health,
	}
}

// Search runs a query against all enabled sources for the given tab, returning combined results.
// Use SearchWithAuthor for scored results.
func (m *Manager) Search(ctx context.Context, tab, query string) ([]models.SearchResult, int64) {
	return m.SearchWithAuthor(ctx, tab, query, "")
}

// SearchWithAuthor runs a query and scores results using the provided author hint.
func (m *Manager) SearchWithAuthor(ctx context.Context, tab, query, author string) ([]models.SearchResult, int64) {
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

	// Apply relevance and foreign-language filtering.
	results = FilterResults(results, query, m.cfg.ForeignLangFilter)
	// Apply suspicious keyword, seed, size, dedup, and sorting filters.
	results = FilterAndSortResults(results, query, m.cfg.MinTorrentSizeBytes, m.cfg.MaxTorrentSizeBytes)

	// Score all results.
	results = ScoreResults(results, query, author)

	// Re-sort by score (highest first), preserving filter order as tiebreaker.
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	elapsed := time.Since(start).Milliseconds()
	return results, elapsed
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
// When foreignLangFilter is true, results with non-English titles are removed
// (unless they come from multilang sources like BookTracker, Flibusta, Z-Library).
// When foreignLangFilter is false, all titles pass regardless of language.
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

// isMultilangSource returns true for sources that inherently serve multilingual
// content and should bypass the foreign-language filter.
func isMultilangSource(source string) bool {
	switch source {
	case "booktracker", "booktracker_audiobook", "flibusta", "zlibrary":
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

var wordRe = regexp.MustCompile(`[\pL\pN]+`)

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
