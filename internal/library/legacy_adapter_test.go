package library

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

type fakeLegacyStore struct {
	items []models.LibraryItem
}

func (s fakeLegacyStore) GetItems(mediaType string, _, _ int) ([]models.LibraryItem, error) {
	if mediaType == "" {
		return s.items, nil
	}
	var filtered []models.LibraryItem
	for _, item := range s.items {
		if item.MediaType == mediaType {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func (s fakeLegacyStore) FindByTitle(title string) ([]models.LibraryItem, error) {
	var matches []models.LibraryItem
	for _, item := range s.items {
		if NormalizeKey(item.Title) == NormalizeKey(title) {
			matches = append(matches, item)
		}
	}
	return matches, nil
}

func (s fakeLegacyStore) HasSourceID(sourceID string) bool {
	for _, item := range s.items {
		if item.SourceID == sourceID {
			return true
		}
	}
	return false
}

func TestLegacyLibraryRepositoryFindsBookAndFiles(t *testing.T) {
	repo := NewLegacyLibraryRepositoryWithStore(fakeLegacyStore{items: []models.LibraryItem{legacyTestItem()}})
	ctx := context.Background()

	book, err := repo.FindBook(ctx, BookQuery{Title: "The Guardian's Path", Author: "Carla Jablonski", MediaType: MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	if book.ID != 7 || book.Title != "The Guardian's Path" {
		t.Fatalf("book = %+v", book)
	}

	files, err := repo.GetBookFiles(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Format != "epub" || files[0].Path != "/books/guardian.epub" {
		t.Fatalf("files = %+v", files)
	}

	file, err := repo.FindFileBySourceID(ctx, "source-7")
	if err != nil {
		t.Fatal(err)
	}
	if file.BookID != book.ID || file.SourceID != "source-7" {
		t.Fatalf("file by source = %+v", file)
	}
}

func TestLegacyLibraryRepositoryReadOnlyWrites(t *testing.T) {
	repo := NewLegacyLibraryRepositoryWithStore(fakeLegacyStore{})
	if _, err := repo.SaveBook(context.Background(), Book{Title: "New"}); !errors.Is(err, ErrReadOnlyRepository) {
		t.Fatalf("SaveBook error = %v", err)
	}
	if _, err := repo.AttachFile(context.Background(), BookFile{EditionID: 1, Format: "epub"}); !errors.Is(err, ErrReadOnlyRepository) {
		t.Fatalf("AttachFile error = %v", err)
	}
}

func TestLegacyLibraryRepositoryNotFoundAndCancellation(t *testing.T) {
	repo := NewLegacyLibraryRepositoryWithStore(fakeLegacyStore{})
	if _, err := repo.GetBook(context.Background(), 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBook error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.GetBook(ctx, 1); err == nil {
		t.Fatal("expected canceled context error")
	}
}

func legacyTestItem() models.LibraryItem {
	return models.LibraryItem{
		ID:           7,
		Title:        "The Guardian's Path",
		Author:       "Carla Jablonski",
		FilePath:     "/books/guardian.epub",
		OriginalPath: "/downloads/guardian.epub",
		FileSize:     123,
		FileFormat:   "epub",
		MediaType:    "ebook",
		Source:       "torrent",
		SourceID:     "source-7",
		ContentHash:  "hash-7",
		AddedAt:      time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
	}
}
