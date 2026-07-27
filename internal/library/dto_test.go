package library

import (
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

func TestLegacyItemToDomainAndBack(t *testing.T) {
	added := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	item := models.LibraryItem{
		ID:           42,
		Title:        "The Guardian's Path",
		Author:       "Carla Jablonski",
		FilePath:     "/books/guardian.mobi",
		OriginalPath: "/downloads/guardian.mobi",
		FileSize:     1234,
		FileFormat:   "mobi",
		MediaType:    "ebook",
		Source:       "torrent",
		SourceID:     "source-42",
		ContentHash:  "abc123",
		AddedAt:      added,
	}

	book := LegacyItemToBook(item)
	if book.ID != item.ID || book.Title != item.Title || book.MediaType != MediaTypeEbook {
		t.Fatalf("book = %+v", book)
	}
	if len(book.Contributors) != 1 || book.Contributors[0].Name != item.Author {
		t.Fatalf("contributors = %+v", book.Contributors)
	}
	if book.PreferredEdition == nil || book.PreferredEdition.Title != item.Title {
		t.Fatalf("preferred edition = %+v", book.PreferredEdition)
	}

	file := LegacyItemToFile(item)
	if file.ID != item.ID || file.Format != item.FileFormat || file.Path != item.FilePath || file.SourceID != item.SourceID {
		t.Fatalf("file = %+v", file)
	}

	roundTrip := ToLegacyLibraryItem(book, file)
	if roundTrip.Title != item.Title || roundTrip.Author != item.Author || roundTrip.FilePath != item.FilePath || roundTrip.FileFormat != item.FileFormat {
		t.Fatalf("round trip = %+v", roundTrip)
	}
}

func TestLegacyItemToFileFallsBackToExtension(t *testing.T) {
	file := LegacyItemToFile(models.LibraryItem{FilePath: "/books/dune.EPUB", MediaType: "ebook"})
	if file.Format != "epub" {
		t.Fatalf("format = %q, want epub", file.Format)
	}
}
