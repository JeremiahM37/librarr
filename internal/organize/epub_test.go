package organize

import (
	"testing"
)

func TestWordOverlap(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		actual   string
		want     float64
	}{
		{"exact match", "The Great Gatsby", "The Great Gatsby", 1.0},
		{"case insensitive", "the great gatsby", "THE GREAT GATSBY", 1.0},
		{"partial match", "The Great Gatsby", "Great Stories", 0.5},  // "great" matches, "gatsby" doesn't -> 1/2 = 0.5
		{"no match", "The Great Gatsby", "Harry Potter", 0.0},
		{"empty expected", "", "Some Title", 1.0},                     // empty expected = 1.0
		{"stopwords ignored", "The Book of Everything", "Everything Goes", 0.5}, // "book" + "everything" -> only "everything" matches -> 1/2 = 0.5
		{"all stopwords", "the a an of", "different words", 1.0},      // no significant words in expected -> 1.0
		{"subtitle ignored", "Dune", "Dune: Part One", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wordOverlap(tt.expected, tt.actual)
			if got != tt.want {
				t.Errorf("wordOverlap(%q, %q) = %f, want %f", tt.expected, tt.actual, got, tt.want)
			}
		})
	}
}

func TestExtractSignificantWords(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]bool
	}{
		{"The Great Gatsby", map[string]bool{"great": true, "gatsby": true}},
		{"a book", map[string]bool{"book": true}},
		{"", map[string]bool{}},
		{"I x y", map[string]bool{}}, // all single chars or stopwords
		{"hello-world test", map[string]bool{"hello": true, "world": true, "test": true}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := extractSignificantWords(tt.input)
			if len(got) != len(tt.expected) {
				t.Errorf("extractSignificantWords(%q) = %v, want %v", tt.input, got, tt.expected)
				return
			}
			for w := range tt.expected {
				if !got[w] {
					t.Errorf("missing word %q in result", w)
				}
			}
		})
	}
}

func TestVerifyEPUBTitle_WordOverlapLogic(t *testing.T) {
	// We can't easily test the full VerifyEPUBTitle without real EPUB files,
	// but we can test the underlying logic.

	t.Run("high overlap passes", func(t *testing.T) {
		overlap := wordOverlap("The Great Gatsby", "Great Gatsby Novel")
		if overlap < 0.8 {
			t.Errorf("expected high overlap for similar titles, got %f", overlap)
		}
	})

	t.Run("low overlap fails", func(t *testing.T) {
		overlap := wordOverlap("The Great Gatsby", "Harry Potter")
		if overlap >= 0.5 {
			t.Errorf("expected low overlap for different titles, got %f", overlap)
		}
	})

	t.Run("empty title always passes", func(t *testing.T) {
		overlap := wordOverlap("", "Any Title")
		if overlap != 1.0 {
			t.Errorf("expected 1.0 for empty expected, got %f", overlap)
		}
	})
}
