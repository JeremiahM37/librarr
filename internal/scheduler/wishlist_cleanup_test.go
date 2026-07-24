package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

func TestConservativeWishlistMatches(t *testing.T) {
	candidates := []cleanupCandidate{
		{Title: "The Martian", Author: "Andy Weir", MediaType: "ebook", Source: "local-db"},
		{Title: "The Martian", Author: "Another Author", MediaType: "audiobook", Source: "local-db"},
	}

	matches := conservativeWishlistMatches(models.WishlistItem{
		Title:     "the martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
	}, candidates)
	if len(matches) != 1 {
		t.Fatalf("expected one conservative match, got %d", len(matches))
	}

	matches = conservativeWishlistMatches(models.WishlistItem{
		Title:     "The Martian",
		Author:    "Not Andy Weir",
		MediaType: "ebook",
	}, candidates)
	if len(matches) != 0 {
		t.Fatalf("expected author mismatch to be skipped, got %d", len(matches))
	}

	matches = conservativeWishlistMatches(models.WishlistItem{
		Title:     "The Martian",
		Author:    "Another Author",
		MediaType: "ebook",
	}, candidates)
	if len(matches) != 0 {
		t.Fatalf("expected media type mismatch to be skipped, got %d", len(matches))
	}

	matches = conservativeWishlistMatches(models.WishlistItem{
		Title:     "Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
	}, candidates)
	if len(matches) != 0 {
		t.Fatalf("expected leading article difference to be skipped, got %d", len(matches))
	}

	matches = conservativeWishlistMatches(models.WishlistItem{
		Title:     "Coauthored Book",
		Author:    "Jane Writer",
		MediaType: "audiobook",
	}, []cleanupCandidate{
		{Title: "Coauthored Book", Author: "John Narrator AND Jane Writer", MediaType: "audiobook", Source: "audiobookshelf"},
	})
	if len(matches) != 1 {
		t.Fatalf("expected case-insensitive coauthor match, got %d", len(matches))
	}

	matches = conservativeWishlistMatches(models.WishlistItem{
		Title:     "The Seven Husbands of Evelyn Hugo",
		Author:    "Taylor Jenkins Reid",
		MediaType: "audiobook",
	}, []cleanupCandidate{
		{Title: "The Seven Husbands of Evelyn Hugo (Unabridged)", Author: "Taylor Jenkins Reid", MediaType: "audiobook", Source: "audiobookshelf"},
	})
	if len(matches) != 1 {
		t.Fatalf("expected unabridged edition marker to be ignored, got %d", len(matches))
	}

	matches = conservativeWishlistMatches(models.WishlistItem{
		Title:     "The Seven Husbands of Evelyn Hugo",
		Author:    "Not Taylor Jenkins Reid",
		MediaType: "audiobook",
	}, []cleanupCandidate{
		{Title: "The Seven Husbands of Evelyn Hugo (Unabridged)", Author: "Taylor Jenkins Reid", MediaType: "audiobook", Source: "audiobookshelf"},
	})
	if len(matches) != 0 {
		t.Fatalf("expected edition marker match to still require author agreement, got %d", len(matches))
	}
}

func TestConservativeWishlistMatches_AmbiguousDistinctAuthors(t *testing.T) {
	candidates := []cleanupCandidate{
		{Title: "The Stand", Author: "Stephen King", MediaType: "ebook", Source: "local-db"},
		{Title: "The Stand", Author: "A Different Author", MediaType: "ebook", Source: "local-db"},
	}

	matches := conservativeWishlistMatches(models.WishlistItem{
		Title:     "The Stand",
		MediaType: "ebook",
	}, candidates)
	if len(matches) != 2 {
		t.Fatalf("expected ambiguous title-only match to keep both candidates, got %d", len(matches))
	}
}

func TestWishlistCleanerRunDeletesOnlyUniqueConservativeMatches(t *testing.T) {
	database := newWishlistCleanupTestDB(t)
	addWishlist(t, database, "The Martian", "Andy Weir", "ebook")
	addWishlist(t, database, "The Stand", "", "ebook")
	addWishlist(t, database, "Project Hail Mary", "Wrong Author", "ebook")
	addLibraryItem(t, database, "The Martian", "Andy Weir", "ebook")
	addLibraryItem(t, database, "The Stand", "Stephen King", "ebook")
	addLibraryItem(t, database, "The Stand", "A Different Author", "ebook")
	addLibraryItem(t, database, "Project Hail Mary", "Andy Weir", "ebook")

	cleaner := NewWishlistCleaner(&config.Config{
		WishlistCleanupEnabled:       true,
		WishlistCleanupIntervalHours: 12,
		WishlistCleanupDryRun:        false,
	}, database)

	result := cleaner.Run(context.Background())
	if result.Deleted != 1 {
		t.Fatalf("expected exactly one deletion, got %+v", result)
	}
	if result.Ambiguous != 1 {
		t.Fatalf("expected one ambiguous skip, got %+v", result)
	}

	wishlist, err := database.GetWishlist()
	if err != nil {
		t.Fatalf("GetWishlist: %v", err)
	}
	if len(wishlist) != 2 {
		t.Fatalf("expected two wishlist items to remain, got %d: %+v", len(wishlist), wishlist)
	}
	for _, item := range wishlist {
		if item.Title == "The Martian" {
			t.Fatalf("expected matched item to be deleted: %+v", wishlist)
		}
	}
}

func TestWishlistCleanerRunDryRunDoesNotDelete(t *testing.T) {
	database := newWishlistCleanupTestDB(t)
	addWishlist(t, database, "The Martian", "Andy Weir", "ebook")
	addLibraryItem(t, database, "The Martian", "Andy Weir", "ebook")

	cleaner := NewWishlistCleaner(&config.Config{WishlistCleanupDryRun: true}, database)
	result := cleaner.Run(context.Background())
	if result.DryRun != 1 || result.Deleted != 0 {
		t.Fatalf("expected dry-run match without deletion, got %+v", result)
	}

	wishlist, err := database.GetWishlist()
	if err != nil {
		t.Fatalf("GetWishlist: %v", err)
	}
	if len(wishlist) != 1 {
		t.Fatalf("expected dry-run to keep wishlist item, got %d", len(wishlist))
	}
}

func TestWishlistCleanerLoadsAudiobookshelfCandidates(t *testing.T) {
	var sawAuth bool
	abs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/libraries/audio-lib/items" {
			t.Fatalf("unexpected ABS path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") == "Bearer token" {
			sawAuth = true
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"media": map[string]interface{}{
						"metadata": map[string]interface{}{
							"title":      "Dungeon Crawler Carl",
							"authorName": "Matt Dinniman",
						},
					},
				},
			},
			"total":    1,
			"numPages": 1,
		})
	}))
	defer abs.Close()

	database := newWishlistCleanupTestDB(t)
	addWishlist(t, database, "Dungeon Crawler Carl", "Matt Dinniman", "audiobook")

	cleaner := NewWishlistCleaner(&config.Config{
		ABSURL:                       abs.URL,
		ABSToken:                     "token",
		ABSLibraryID:                 "audio-lib",
		WishlistCleanupDryRun:        false,
		WishlistCleanupEnabled:       true,
		SchedulerIntervalHours:       12,
		SchedulerAutoDownload:        false,
		SchedulerEnabled:             false,
		SchedulerMinScore:            70,
		AuthorCheckIntervalDays:      7,
		WishlistCleanupIntervalHours: 12,
	}, database)

	result := cleaner.Run(context.Background())
	if !sawAuth {
		t.Fatal("expected ABS request to include bearer token")
	}
	if result.Deleted != 1 {
		t.Fatalf("expected ABS match to delete wishlist item, got %+v", result)
	}
}

func TestWishlistCleanerSkipsDeleteWhenAudiobookshelfFails(t *testing.T) {
	abs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusBadGateway)
	}))
	defer abs.Close()

	database := newWishlistCleanupTestDB(t)
	addWishlist(t, database, "Dungeon Crawler Carl", "Matt Dinniman", "audiobook")

	cleaner := NewWishlistCleaner(&config.Config{
		ABSURL:                       abs.URL,
		ABSToken:                     "token",
		ABSLibraryID:                 "audio-lib",
		WishlistCleanupDryRun:        false,
		WishlistCleanupEnabled:       true,
		WishlistCleanupIntervalHours: 12,
	}, database)

	result := cleaner.Run(context.Background())
	if result.Deleted != 0 {
		t.Fatalf("expected no deletion when ABS lookup fails, got %+v", result)
	}
	wishlist, err := database.GetWishlist()
	if err != nil {
		t.Fatalf("GetWishlist: %v", err)
	}
	if len(wishlist) != 1 {
		t.Fatalf("expected wishlist item to remain, got %d", len(wishlist))
	}
}

func newWishlistCleanupTestDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "librarr.db"))
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func addWishlist(t *testing.T, database *db.DB, title, author, mediaType string) {
	t.Helper()
	if _, err := database.AddWishlistItem(title, author, mediaType); err != nil {
		t.Fatalf("AddWishlistItem: %v", err)
	}
}

func addLibraryItem(t *testing.T, database *db.DB, title, author, mediaType string) {
	t.Helper()
	_, err := database.AddItem(&models.LibraryItem{
		Title:      title,
		Author:     author,
		MediaType:  mediaType,
		FilePath:   "/library/" + title + "/" + author,
		FileFormat: "epub",
		Source:     "test",
	})
	if err != nil {
		t.Fatalf("AddItem: %v", err)
	}
}
