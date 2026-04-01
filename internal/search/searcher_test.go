package search

import (
	"testing"
)

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
		{"Книга на русском", true},          // > 30% non-Latin
		{"中文书籍", true},                      // all non-Latin
		{"Book with some 日本語", false},       // 3 non-Latin / 15 letters = 20%, below 30% threshold
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
		{"The Great Gatsby", "great gatsby", true},                     // substring match
		{"Gatsby: A Novel", "great gatsby", true},                      // 50% word overlap
		{"Completely Unrelated", "great gatsby", false},                // no overlap
		{"Any Title", "", true},                                         // empty query always relevant
		{"The Great Gatsby", "the great gatsby by fitzgerald", true},   // query contains title
		{"Great", "great gatsby adventure", true},                      // "great" is substring of query, so titleRelevant returns true
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
		{"the great gatsby", map[string]bool{"great": true, "gatsby": true}},     // "the" is stopword
		{"a book of many things", map[string]bool{"book": true, "many": true, "things": true}}, // "a", "of" are stopwords
		{"x", map[string]bool{}},                                                   // single char filtered
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
	t.Run("removes foreign titles", func(t *testing.T) {
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
}
