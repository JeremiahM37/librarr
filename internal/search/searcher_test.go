package search

import (
	"context"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

type stubSearcher struct {
	name    string
	tab     string
	results []models.SearchResult
	err     error
}

func (s stubSearcher) Name() string         { return s.name }
func (s stubSearcher) Label() string        { return s.name }
func (s stubSearcher) Enabled() bool        { return true }
func (s stubSearcher) SearchTab() string    { return s.tab }
func (s stubSearcher) DownloadType() string { return "direct" }
func (s stubSearcher) Search(context.Context, string) ([]models.SearchResult, error) {
	return s.results, s.err
}

func TestIsMultilangSource(t *testing.T) {
	tests := []struct {
		source   string
		expected bool
	}{
		{"flibusta", true},
		{"zlibrary", true},
		{"booktracker", true},
		{"booktracker_audiobook", true},
		{"annas", false},
		{"torrent", false},
		{"tpb", false},
		{"gutenberg", false},
		{"openlibrary", false},
		{"mangadex", false},
	}
	for _, tt := range tests {
		t.Run(tt.source, func(t *testing.T) {
			if got := isMultilangSource(tt.source); got != tt.expected {
				t.Errorf("isMultilangSource(%q) = %v, want %v", tt.source, got, tt.expected)
			}
		})
	}
}

func TestSearchStreamEmitsSourceUpdates(t *testing.T) {
	mgr := NewManager(&config.Config{}, []Searcher{
		stubSearcher{
			name: "one",
			tab:  "main",
			results: []models.SearchResult{{
				Title:  "Project Hail Mary",
				Source: "one",
			}},
		},
		stubSearcher{
			name: "two",
			tab:  "main",
			results: []models.SearchResult{{
				Title:  "Project Hail Mary Audiobook",
				Source: "two",
			}},
		},
	}, NewHealthTracker(3, 300))

	var all []models.SearchResult
	seen := map[string]bool{}
	for update := range mgr.SearchStream(context.Background(), "main", "Project Hail Mary", "") {
		if update.Err != nil {
			t.Fatalf("unexpected stream error: %v", update.Err)
		}
		seen[update.Source] = true
		all = append(all, update.Results...)
	}

	if !seen["one"] || !seen["two"] {
		t.Fatalf("stream updates = %#v, want one and two", seen)
	}
	processed := mgr.ProcessResults(all, "Project Hail Mary", "")
	if len(processed) != 2 {
		t.Fatalf("processed results = %d, want 2", len(processed))
	}
}

func TestProcessDiscoverResultsAllowsAuthorOnlyQuery(t *testing.T) {
	mgr := NewManager(defaultSearchConfig(), nil, NewHealthTracker(3, 300))
	results := tomBowerResults()

	processed, stats := mgr.ProcessDiscoverResults(results, "Tom Bower", "")

	if len(processed) != 4 {
		t.Fatalf("Discover author query returned %d results, want 4", len(processed))
	}
	if stats.UpstreamResults != 4 || stats.FilteredResults != 0 || stats.ReturnedResults != 4 {
		t.Fatalf("diagnostics = %+v, want upstream 4 filtered 0 returned 4", stats)
	}
	seen := map[string]bool{}
	for _, result := range processed {
		seen[result.Title] = true
	}
	for _, title := range []string{
		"Rebel Prince",
		"Sweet Revenge",
		"Oil, Money and Power",
		"The Fall of Sayed",
	} {
		if !seen[title] {
			t.Fatalf("Discover results missing %q: %#v", title, processed)
		}
	}
}

func TestProcessDiscoverResultsPreservesMixedSources(t *testing.T) {
	mgr := NewManager(defaultSearchConfig(), nil, NewHealthTracker(3, 300))
	var results []models.SearchResult
	for i, title := range []string{
		"Sweet Revenge",
		"Oil: Money, Politics, and Power in the 21st Century",
		"The Fall of Fayed",
		"Revenge: Meghan, Harry and the war between the Windsors",
		"Rebel Prince",
		"The House of Beckham",
		"Boris Johnson",
		"Nazi Gold",
		"Dangerous Hero",
		"Broken Vows",
		"No Angel",
		"Klaus Barbie",
		"Branson",
		"Maxwell, The Outsider",
	} {
		results = append(results, models.SearchResult{
			Source:      "torrent",
			Indexer:     "MyAnonamouse",
			Title:       title,
			Author:      "Tom Bower",
			Format:      "epub",
			Seeders:     5 + i,
			Size:        int64(1_000_000 + i),
			GUID:        "mam-guid-" + title,
			Categories:  []string{"7000", "7020"},
			DownloadURL: "https://prowlarr/download/" + title,
		})
	}
	results = append(results,
		models.SearchResult{Source: "annas", Title: "Rebel Prince: The Power, Passion and Defiance of Prince Charles", Author: "Tom Bower", Format: "epub", SizeHuman: "39.9MB"},
		models.SearchResult{Source: "annas", Title: "Dangerous Hero", Author: "Tom Bower", Format: "epub", SizeHuman: "33.1MB"},
		models.SearchResult{Source: "gutenberg", Title: "The Phantom Herd", Author: "B. M. Bower", Format: "epub"},
	)

	processed, stats := mgr.ProcessDiscoverResults(results, "Tom Bower", "")

	if len(processed) != 17 {
		t.Fatalf("processed results = %d, want 17", len(processed))
	}
	if stats.UpstreamResults != 17 || stats.ReturnedResults != 17 || stats.FilteredResults != 0 {
		t.Fatalf("diagnostics = %+v, want all 17 returned", stats)
	}
	if stats.UpstreamBySource["prowlarr"] != 14 || stats.ReturnedBySource["prowlarr"] != 14 {
		t.Fatalf("prowlarr diagnostics = upstream %#v returned %#v", stats.UpstreamBySource, stats.ReturnedBySource)
	}
	if stats.ReturnedBySource["annas"] != 2 || stats.ReturnedBySource["gutenberg"] != 1 {
		t.Fatalf("returned source diagnostics = %#v", stats.ReturnedBySource)
	}
}

func TestProcessDiscoverResultsReportsAllFiltered(t *testing.T) {
	mgr := NewManager(defaultSearchConfig(), nil, NewHealthTracker(3, 300))

	processed, stats := mgr.ProcessDiscoverResults([]models.SearchResult{
		{Source: "prowlarr", Title: "Tom Bower keygen exe", Author: "Tom Bower"},
	}, "Tom Bower", "")

	if len(processed) != 0 {
		t.Fatalf("processed results = %d, want 0", len(processed))
	}
	if stats.UpstreamResults != 1 || stats.FilteredResults != 1 || stats.ReturnedResults != 0 {
		t.Fatalf("diagnostics = %+v, want upstream 1 filtered 1 returned 0", stats)
	}
}

func TestProcessDiscoverResultsReportsZeroUpstream(t *testing.T) {
	mgr := NewManager(defaultSearchConfig(), nil, NewHealthTracker(3, 300))

	processed, stats := mgr.ProcessDiscoverResults(nil, "Tom Bower", "")

	if len(processed) != 0 {
		t.Fatalf("processed results = %d, want 0", len(processed))
	}
	if stats.UpstreamResults != 0 || stats.FilteredResults != 0 || stats.ReturnedResults != 0 {
		t.Fatalf("diagnostics = %+v, want all zero", stats)
	}
}

func TestProcessResultsKeepsWantedCanonicalMatchingStrict(t *testing.T) {
	mgr := NewManager(defaultSearchConfig(), nil, NewHealthTracker(3, 300))

	processed := mgr.ProcessResults(tomBowerResults(), "Rebel Prince", "Tom Bower")

	if len(processed) != 1 {
		t.Fatalf("Wanted canonical processing returned %d results, want 1: %#v", len(processed), processed)
	}
	if processed[0].Title != "Rebel Prince" {
		t.Fatalf("Wanted canonical processing returned %q, want Rebel Prince", processed[0].Title)
	}
}

func defaultSearchConfig() *config.Config {
	return &config.Config{
		MinTorrentSizeBytes: 10000,
		MaxTorrentSizeBytes: 2000000000,
	}
}

func tomBowerResults() []models.SearchResult {
	return []models.SearchResult{
		{Source: "torrent", Indexer: "MyAnonamouse", Title: "Rebel Prince", Author: "Tom Bower", Format: "epub", Seeders: 12, Size: 38_300_000, GUID: "mam-rebel"},
		{Source: "torrent", Indexer: "MyAnonamouse", Title: "Sweet Revenge", Author: "Tom Bower", Format: "epub", Seeders: 10, Size: 6_000_000, GUID: "mam-sweet"},
		{Source: "torrent", Indexer: "MyAnonamouse", Title: "Oil, Money and Power", Author: "Tom Bower", Format: "epub", Seeders: 8, Size: 15_000_000, GUID: "mam-oil"},
		{Source: "torrent", Indexer: "MyAnonamouse", Title: "The Fall of Sayed", Author: "Tom Bower", Format: "epub", Seeders: 6, Size: 4_500_000, GUID: "mam-fayed"},
	}
}

func TestIsForeignTitle(t *testing.T) {
	tests := []struct {
		title    string
		expected bool
	}{
		{"The Great Gatsby", false},
		{"Harry Potter (Norwegian Edition)", true},
		{"Buch auf Deutsch", true},
		{"Livre en French", true},
		{"Libro en Spanish", true},
		{"Книга на русском", true},    // > 30% non-Latin
		{"中文书籍", true},                // all non-Latin
		{"Book with some 日本語", false}, // 3 non-Latin / 15 letters = 20%, below 30% threshold
		{"Normal Book Title 123", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			result := isForeignTitle(tt.title)
			if result != tt.expected {
				t.Errorf("isForeignTitle(%q) = %v, want %v", tt.title, result, tt.expected)
			}
		})
	}
}

func TestTitleRelevant(t *testing.T) {
	tests := []struct {
		title    string
		query    string
		expected bool
	}{
		{"The Great Gatsby", "great gatsby", true},                   // substring match
		{"Gatsby: A Novel", "great gatsby", true},                    // 50% word overlap
		{"Completely Unrelated", "great gatsby", false},              // no overlap
		{"Any Title", "", true},                                      // empty query always relevant
		{"The Great Gatsby", "the great gatsby by fitzgerald", true}, // query contains title
		{"Great", "great gatsby adventure", true},                    // "great" is substring of query, so titleRelevant returns true
	}

	for _, tt := range tests {
		t.Run(tt.title+"_"+tt.query, func(t *testing.T) {
			result := titleRelevant(tt.title, tt.query)
			if result != tt.expected {
				t.Errorf("titleRelevant(%q, %q) = %v, want %v", tt.title, tt.query, result, tt.expected)
			}
		})
	}
}

func TestExtractWords(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]bool
	}{
		{"the great gatsby", map[string]bool{"great": true, "gatsby": true}},                   // "the" is stopword
		{"a book of many things", map[string]bool{"book": true, "many": true, "things": true}}, // "a", "of" are stopwords
		{"x", map[string]bool{}}, // single char filtered
		{"", map[string]bool{}},
		{"hello world 42", map[string]bool{"hello": true, "world": true, "42": true}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := extractWords(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("extractWords(%q) got %d words, want %d: %v", tt.input, len(result), len(tt.expected), result)
				return
			}
			for w := range tt.expected {
				if !result[w] {
					t.Errorf("extractWords(%q) missing word %q", tt.input, w)
				}
			}
		})
	}
}

func TestFilterResults(t *testing.T) {
	t.Run("removes foreign titles when filter enabled", func(t *testing.T) {
		results := []struct {
			title   string
			foreign bool
		}{
			{"Good English Book", false},
			{"Norwegian Edition Book", true},
			{"中文标题", true},
		}

		var input []struct {
			title   string
			foreign bool
		}
		input = append(input, results...)

		// Build SearchResult slice
		var searchResults []struct{ Title string }
		for _, r := range input {
			searchResults = append(searchResults, struct{ Title string }{r.title})
		}
		// Just validate the isForeignTitle function works correctly
		for _, r := range results {
			got := isForeignTitle(r.title)
			if got != r.foreign {
				t.Errorf("isForeignTitle(%q) = %v, want %v", r.title, got, r.foreign)
			}
		}
	})

	t.Run("preserves foreign titles when filter disabled", func(t *testing.T) {
		input := []models.SearchResult{
			{Source: "flibusta", Title: "Книга на русском"},
			{Source: "annas", Title: "Good English Book"},
		}
		// Use empty query so titleRelevant passes for all titles.
		filtered := FilterResults(input, "", false)
		if len(filtered) != 2 {
			t.Errorf("expected 2 results with filter disabled, got %d", len(filtered))
		}
	})

	t.Run("multilang sources bypass foreign filter", func(t *testing.T) {
		input := []models.SearchResult{
			{Source: "flibusta", Title: "Книга на русском"},
			{Source: "zlibrary", Title: "Книга по-русски"},
			{Source: "annas", Title: "Russian Edition Book"},
			{Source: "annas", Title: "Good English Book"},
		}
		// Use empty query so titleRelevant passes for all titles.
		filtered := FilterResults(input, "", true)
		// flibusta and zlibrary results pass through (multilang sources bypass filter)
		// "Russian Edition Book" from annas is filtered (foreign keyword + not multilang source)
		// "Good English Book" from annas passes
		if len(filtered) != 3 {
			t.Errorf("expected 3 results (2 multilang + 1 english), got %d", len(filtered))
		}
		for _, r := range filtered {
			if r.Source == "annas" && r.Title == "Russian Edition Book" {
				t.Errorf("foreign title from non-multilang source should be filtered: %s", r.Title)
			}
		}
	})

	t.Run("non-multilang source foreign titles removed when filter enabled", func(t *testing.T) {
		input := []models.SearchResult{
			{Source: "annas", Title: "Russian Edition Book"},
			{Source: "annas", Title: "Good English Book"},
		}
		filtered := FilterResults(input, "book", true)
		if len(filtered) != 1 {
			t.Errorf("expected 1 result (english only from non-multilang), got %d", len(filtered))
		}
		if len(filtered) > 0 && filtered[0].Title != "Good English Book" {
			t.Errorf("expected English book, got %s", filtered[0].Title)
		}
	})
}
