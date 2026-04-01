package torznab

import (
	"testing"
)

func TestBuildCaps(t *testing.T) {
	caps := BuildCaps()

	if caps.Server.Title != "Librarr" {
		t.Errorf("expected server title Librarr, got %s", caps.Server.Title)
	}

	if caps.Limits.Max != 100 {
		t.Errorf("expected max 100, got %d", caps.Limits.Max)
	}
	if caps.Limits.Default != 50 {
		t.Errorf("expected default 50, got %d", caps.Limits.Default)
	}

	t.Run("search capabilities", func(t *testing.T) {
		if caps.Searching.Search.Available != "yes" {
			t.Errorf("expected search available, got %s", caps.Searching.Search.Available)
		}
		if caps.Searching.BookSearch.Available != "yes" {
			t.Error("expected book search available")
		}
		if caps.Searching.AudioSearch.Available != "yes" {
			t.Error("expected audio search available")
		}
		if caps.Searching.BookSearch.SupportedParams != "q,author,title" {
			t.Errorf("expected book search params q,author,title, got %s", caps.Searching.BookSearch.SupportedParams)
		}
	})

	t.Run("categories", func(t *testing.T) {
		cats := caps.Categories.Categories
		if len(cats) != 2 {
			t.Fatalf("expected 2 categories, got %d", len(cats))
		}

		// Books category
		books := cats[0]
		if books.ID != "7000" {
			t.Errorf("expected books ID 7000, got %s", books.ID)
		}
		if books.Name != "Books" {
			t.Errorf("expected name Books, got %s", books.Name)
		}
		if len(books.Subs) != 4 {
			t.Errorf("expected 4 book subcategories, got %d", len(books.Subs))
		}

		// Verify subcategory IDs
		expectedSubs := map[string]string{
			"7020": "Books/Ebook",
			"7030": "Books/Comics",
			"7040": "Books/Magazines",
			"7050": "Books/Technical",
		}
		for _, sub := range books.Subs {
			expected, ok := expectedSubs[sub.ID]
			if !ok {
				t.Errorf("unexpected subcategory ID: %s", sub.ID)
			}
			if sub.Name != expected {
				t.Errorf("subcategory %s: expected name %s, got %s", sub.ID, expected, sub.Name)
			}
		}

		// Audio category
		audio := cats[1]
		if audio.ID != "3000" {
			t.Errorf("expected audio ID 3000, got %s", audio.ID)
		}
		if len(audio.Subs) != 1 {
			t.Errorf("expected 1 audio subcategory, got %d", len(audio.Subs))
		}
		if audio.Subs[0].ID != "3030" {
			t.Errorf("expected audiobook sub ID 3030, got %s", audio.Subs[0].ID)
		}
	})
}
