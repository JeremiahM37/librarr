package organize

import (
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{"normal name", "John Smith", 80, "John Smith"},
		{"removes unsafe chars", `Book: "Title" <1>`, 80, "Book Title 1"},
		{"collapses whitespace", "  Too   Many   Spaces  ", 80, "Too Many Spaces"},
		{"truncates long names", "A Very Long Author Name That Exceeds The Limit", 20, "A Very Long Author N"},
		{"removes trailing dots", "Name...", 80, "Name"},
		{"empty becomes Unknown", "", 80, "Unknown"},
		{"only dots becomes Unknown", "...", 80, "Unknown"},
		{"pipe removed", "Author | Publisher", 80, "Author Publisher"},
		{"question mark removed", "What?", 80, "What"},
		{"asterisk removed", "Star*Wars", 80, "StarWars"},
		{"backslash removed", `Path\Name`, 80, "PathName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizePath(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("sanitizePath(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
			}
		})
	}
}

func TestCleanSeriesTitle(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"strips epub extension", "One Piece.epub", "One Piece"},
		{"strips cbz extension", "Naruto Vol 1.cbz", "Naruto"},
		{"strips cbr extension", "Manga.cbr", "Manga"},
		{"strips brackets", "Title [Digital] [2024]", "Title"},
		{"strips volume info", "One Piece Vol 1-100", "One Piece"},
		{"strips volume with dot", "One Piece Vol.5", "One Piece"},
		{"strips paren tags", "Title (Digital)", "Title"},
		{"strips range", "Series 1-50", "Series"},
		{"empty becomes Unknown", "", "Unknown"},
		{"complex cleanup", "[Group] Manga Series Vol 1 (Digital).cbz", "Manga Series"},
		{"strips trailing dash", "Title -", "Title"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanSeriesTitle(tt.input)
			if result != tt.expected {
				t.Errorf("cleanSeriesTitle(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestOrganizer_DisabledDoesNothing(t *testing.T) {
	cfg := &config.Config{FileOrgEnabled: false}
	o := NewOrganizer(cfg)

	if o.cfg.FileOrgEnabled {
		t.Error("expected FileOrgEnabled to be false")
	}
}
