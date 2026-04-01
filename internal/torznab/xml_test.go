package torznab

import (
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
)

func TestResultToItem(t *testing.T) {
	baseURL := "http://localhost:5050"

	t.Run("torrent result with magnet", func(t *testing.T) {
		r := models.SearchResult{
			Source:    "torrent",
			Title:     "Test Book Torrent",
			MagnetURL: "magnet:?xt=urn:btih:abc123",
			Size:      1000000,
			Seeders:   5,
			Leechers:  2,
			InfoHash:  "abc123",
			GUID:      "guid-1",
		}

		item := ResultToItem(r, baseURL)
		if item.Title != "Test Book Torrent" {
			t.Errorf("expected title, got %s", item.Title)
		}
		if item.GUID != "guid-1" {
			t.Errorf("expected GUID guid-1, got %s", item.GUID)
		}
		if item.Link != "magnet:?xt=urn:btih:abc123" {
			t.Errorf("expected magnet link, got %s", item.Link)
		}
		if item.Enclosure == nil {
			t.Fatal("expected enclosure")
		}
		if item.Enclosure.URL != "magnet:?xt=urn:btih:abc123" {
			t.Errorf("expected magnet in enclosure, got %s", item.Enclosure.URL)
		}
		if item.Category != "Books/Ebook" {
			t.Errorf("expected Books/Ebook, got %s", item.Category)
		}
	})

	t.Run("audiobook result", func(t *testing.T) {
		r := models.SearchResult{
			Source:      "audiobook",
			Title:       "Audio Book",
			DownloadURL: "http://example.com/download",
			Size:        5000000,
		}

		item := ResultToItem(r, baseURL)
		if item.Category != "Audio/Audiobook" {
			t.Errorf("expected Audio/Audiobook, got %s", item.Category)
		}
	})

	t.Run("manga result", func(t *testing.T) {
		r := models.SearchResult{
			Source:    "prowlarr_manga",
			Title:     "Manga Title",
			MediaType: "manga",
		}

		item := ResultToItem(r, baseURL)
		if item.Category != "Books/Comics" {
			t.Errorf("expected Books/Comics, got %s", item.Category)
		}
	})

	t.Run("annas result with MD5", func(t *testing.T) {
		r := models.SearchResult{
			Source: "annas",
			Title:  "Direct Download Book",
			MD5:    "abc123def456",
		}

		item := ResultToItem(r, baseURL)
		if item.Link != "http://localhost:5050/api/download/nzb/abc123def456" {
			t.Errorf("expected API download link, got %s", item.Link)
		}
		if item.GUID != "abc123def456" {
			t.Errorf("expected MD5 as GUID, got %s", item.GUID)
		}
	})

	t.Run("gutenberg result with epub URL", func(t *testing.T) {
		r := models.SearchResult{
			Source:   "gutenberg",
			Title:    "Public Domain Book",
			EpubURL:  "https://gutenberg.org/ebooks/123.epub",
			SourceID: "gutenberg-123",
		}

		item := ResultToItem(r, baseURL)
		if item.Link != "https://gutenberg.org/ebooks/123.epub" {
			t.Errorf("expected epub URL link, got %s", item.Link)
		}
		if item.GUID != "gutenberg-123" {
			t.Errorf("expected source ID as GUID, got %s", item.GUID)
		}
	})

	t.Run("result with info hash only", func(t *testing.T) {
		r := models.SearchResult{
			Source:   "torrent",
			Title:    "Hash Only",
			InfoHash: "deadbeef1234",
			Size:     500000,
		}

		item := ResultToItem(r, baseURL)
		if item.GUID != "deadbeef1234" {
			t.Errorf("expected info hash as GUID, got %s", item.GUID)
		}
		if item.Link == "" {
			t.Error("expected magnet link from info hash")
		}
	})

	t.Run("result with no identifiers generates GUID", func(t *testing.T) {
		r := models.SearchResult{
			Source: "webnovel",
			Title:  "Web Novel Title",
		}

		item := ResultToItem(r, baseURL)
		if item.GUID == "" {
			t.Error("expected auto-generated GUID")
		}
	})

	t.Run("seeders and leechers as attrs", func(t *testing.T) {
		r := models.SearchResult{
			Source:   "torrent",
			Title:    "With Peers",
			Seeders:  10,
			Leechers: 5,
			Size:     1000,
			InfoHash: "abc",
		}

		item := ResultToItem(r, baseURL)
		foundSeeders := false
		foundPeers := false
		for _, attr := range item.Attrs {
			if attr.Name == "seeders" && attr.Value == "10" {
				foundSeeders = true
			}
			if attr.Name == "peers" && attr.Value == "5" {
				foundPeers = true
			}
		}
		if !foundSeeders {
			t.Error("expected seeders attribute")
		}
		if !foundPeers {
			t.Error("expected peers attribute")
		}
	})
}

func TestCategoryName(t *testing.T) {
	tests := []struct {
		id       string
		expected string
	}{
		{"7000", "Books"},
		{"7020", "Books/Ebook"},
		{"7030", "Books/Comics"},
		{"7040", "Books/Magazines"},
		{"7050", "Books/Technical"},
		{"3030", "Audio/Audiobook"},
		{"9999", "Books"},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			result := categoryName(tt.id)
			if result != tt.expected {
				t.Errorf("categoryName(%q) = %q, want %q", tt.id, result, tt.expected)
			}
		})
	}
}
