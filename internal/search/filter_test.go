package search

import (
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
)

func TestIsSuspicious(t *testing.T) {
	tests := []struct {
		title    string
		expected bool
	}{
		{"The Great Gatsby", false},
		{"Harry Potter and the Sorcerer's Stone", false},
		{"BookTitle.exe crack version", true},
		{"Book keygen included", true},
		{"Great Book MSI installer", true},
		{"warez collection", true},
		{"devcoursesweb bundle", true},
		{"Book trainer pack", true},
		{"Patch Only for Software v2", true},
		{"activator tool", true},
		{"serial number generator", true},
		{"nulled premium", true},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			result := IsSuspicious(tt.title)
			if result != tt.expected {
				t.Errorf("IsSuspicious(%q) = %v, want %v", tt.title, result, tt.expected)
			}
		})
	}
}

func TestNormalizeForDedup(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"The Great Gatsby", "thegreatgatsby"},
		{"  Hello   World!  ", "helloworld"},
		{"TEST-123_abc", "test123abc"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeForDedup(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeForDedup(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}

	t.Run("truncation at 60 chars", func(t *testing.T) {
		longTitle := "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
		result := normalizeForDedup(longTitle)
		if len(result) != 60 {
			t.Errorf("expected length 60, got %d", len(result))
		}
	})
}

func TestTitleRelevanceScore(t *testing.T) {
	tests := []struct {
		title    string
		query    string
		expected int
	}{
		{"The Great Gatsby", "The Great Gatsby", 3},                  // exact substring
		{"The Great Gatsby - F. Scott Fitzgerald", "great gatsby", 3}, // substring match
		{"Great Adventures of Gatsby", "great gatsby", 2},            // all words present
		{"Great Adventures", "great gatsby", 0},                       // 1 of 2 is not > half
		{"Unrelated Book Title", "great gatsby", 0},                   // no match
		{"", "great gatsby", 0},
		{"Some Book", "", 3}, // empty query is substring of anything
	}

	for _, tt := range tests {
		t.Run(tt.title+"_"+tt.query, func(t *testing.T) {
			result := titleRelevanceScore(tt.title, tt.query)
			if result != tt.expected {
				t.Errorf("titleRelevanceScore(%q, %q) = %d, want %d", tt.title, tt.query, result, tt.expected)
			}
		})
	}
}

func TestFilterAndSortResults(t *testing.T) {
	t.Run("filters suspicious titles", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "annas", Title: "Good Book"},
			{Source: "annas", Title: "Bad keygen exe"},
		}
		filtered := FilterAndSortResults(results, "book", 10000, 2000000000)
		if len(filtered) != 1 {
			t.Fatalf("expected 1 result, got %d", len(filtered))
		}
		if filtered[0].Title != "Good Book" {
			t.Errorf("expected Good Book, got %s", filtered[0].Title)
		}
	})

	t.Run("filters zero-seeder torrents", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "torrent", Title: "Good Torrent", Seeders: 5, Size: 50000},
			{Source: "torrent", Title: "Dead Torrent", Seeders: 0, Size: 50000},
		}
		filtered := FilterAndSortResults(results, "torrent", 10000, 2000000000)
		if len(filtered) != 1 {
			t.Fatalf("expected 1 result, got %d", len(filtered))
		}
	})

	t.Run("filters torrents outside size bounds", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "torrent", Title: "Too Small", Seeders: 5, Size: 100},
			{Source: "torrent", Title: "Just Right", Seeders: 5, Size: 500000},
			{Source: "torrent", Title: "Too Big", Seeders: 5, Size: 5000000000},
		}
		filtered := FilterAndSortResults(results, "test", 10000, 2000000000)
		if len(filtered) != 1 {
			t.Fatalf("expected 1 result, got %d", len(filtered))
		}
		if filtered[0].Title != "Just Right" {
			t.Errorf("expected Just Right, got %s", filtered[0].Title)
		}
	})

	t.Run("deduplicates torrents keeping highest seeders", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "torrent", Title: "Same Book", Seeders: 3, Size: 50000},
			{Source: "torrent", Title: "Same Book", Seeders: 10, Size: 50000},
		}
		filtered := FilterAndSortResults(results, "same book", 10000, 2000000000)
		if len(filtered) != 1 {
			t.Fatalf("expected 1 result after dedup, got %d", len(filtered))
		}
		if filtered[0].Seeders != 10 {
			t.Errorf("expected 10 seeders (highest), got %d", filtered[0].Seeders)
		}
	})

	t.Run("sorts by relevance then source priority", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "gutenberg", Title: "Other Book"},
			{Source: "annas", Title: "The Great Gatsby"},
		}
		filtered := FilterAndSortResults(results, "The Great Gatsby", 10000, 2000000000)
		if len(filtered) < 2 {
			t.Fatalf("expected 2 results, got %d", len(filtered))
		}
		// "The Great Gatsby" should rank first (exact match, relevance 3)
		if filtered[0].Title != "The Great Gatsby" {
			t.Errorf("expected exact match first, got %s", filtered[0].Title)
		}
	})

	t.Run("ABB with abb_url keeps zero seeders", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "audiobook", Title: "Audiobook Title", Seeders: 0, AbbURL: "/some/path"},
		}
		filtered := FilterAndSortResults(results, "audiobook", 10000, 2000000000)
		if len(filtered) != 1 {
			t.Errorf("expected ABB with abb_url to pass filter, got %d results", len(filtered))
		}
	})

	t.Run("ABB without abb_url and zero seeders filtered", func(t *testing.T) {
		results := []models.SearchResult{
			{Source: "audiobook", Title: "Dead Audiobook", Seeders: 0, AbbURL: ""},
		}
		filtered := FilterAndSortResults(results, "audiobook", 10000, 2000000000)
		if len(filtered) != 0 {
			t.Errorf("expected ABB without abb_url and 0 seeders to be filtered, got %d", len(filtered))
		}
	})
}

func TestSourcePriority(t *testing.T) {
	tests := []struct {
		result   models.SearchResult
		expected int
	}{
		{models.SearchResult{Source: "annas"}, 0},
		{models.SearchResult{Source: "annas_manga"}, 0},
		{models.SearchResult{Source: "torrent", Seeders: 5}, 1},
		{models.SearchResult{Source: "torrent", Seeders: 0}, 2},
		{models.SearchResult{Source: "gutenberg"}, 3},
		{models.SearchResult{Source: "openlibrary"}, 3},
		{models.SearchResult{Source: "mangadex"}, 2},
		{models.SearchResult{Source: "webnovel"}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.result.Source, func(t *testing.T) {
			result := sourcePriority(tt.result)
			if result != tt.expected {
				t.Errorf("sourcePriority(%s) = %d, want %d", tt.result.Source, result, tt.expected)
			}
		})
	}
}

func TestParseSizeBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"1.5 GB", 1.5e9},
		{"500 MB", 500e6},
		{"10 KB", 10e3},
		{"100 B", 100},
		{"", 0},
		{"unknown", 0},
		{"1.5GB", 1.5e9},
		{"500MB", 500e6},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSizeBytes(tt.input)
			if result != tt.expected {
				t.Errorf("parseSizeBytes(%q) = %f, want %f", tt.input, result, tt.expected)
			}
		})
	}
}
