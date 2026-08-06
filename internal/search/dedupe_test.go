package search

import (
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
)

func TestCollapseDuplicateCopies(t *testing.T) {
	annas := func(md5, title, size string) models.SearchResult {
		return models.SearchResult{
			Source: "annas", Title: title, Author: "Rick Riordan",
			Publisher: "Disney Hyperion", Format: "epub", Language: "en",
			Year: "2012", SizeHuman: size, MD5: md5,
		}
	}

	t.Run("rows differing only by hash merge into one", func(t *testing.T) {
		got := CollapseDuplicateCopies([]models.SearchResult{
			annas("aaa", "The Mark of Athena", "1.3MB"),
			annas("bbb", "The Mark of Athena", "1.3MB"),
			annas("ccc", "The Mark of Athena", "1.3MB"),
		})
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
		if got[0].Copies != 3 {
			t.Errorf("Copies = %d, want 3", got[0].Copies)
		}
		if got[0].MD5 != "aaa" {
			t.Errorf("survivor MD5 = %q, want the first (best-scoring) one", got[0].MD5)
		}
	})

	t.Run("a different size is a different book to the reader", func(t *testing.T) {
		// The whole point of showing size: 1.3MB is the single volume, 9.1MB is
		// the omnibus. Merging them would hide the one the user wanted.
		got := CollapseDuplicateCopies([]models.SearchResult{
			annas("aaa", "The Mark of Athena", "1.3MB"),
			annas("bbb", "The Mark of Athena", "9.1MB"),
		})
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
	})

	t.Run("every displayed field keeps rows apart", func(t *testing.T) {
		base := annas("aaa", "The Mark of Athena", "1.3MB")
		variants := map[string]func(models.SearchResult) models.SearchResult{
			"title":     func(r models.SearchResult) models.SearchResult { r.Title = "The House of Hades"; return r },
			"author":    func(r models.SearchResult) models.SearchResult { r.Author = "Someone Else"; return r },
			"publisher": func(r models.SearchResult) models.SearchResult { r.Publisher = "Thorndike Press"; return r },
			"format":    func(r models.SearchResult) models.SearchResult { r.Format = "pdf"; return r },
			"language":  func(r models.SearchResult) models.SearchResult { r.Language = "de"; return r },
			"year":      func(r models.SearchResult) models.SearchResult { r.Year = "2014"; return r },
			"source":    func(r models.SearchResult) models.SearchResult { r.Source = "zlibrary"; return r },
			"mediatype": func(r models.SearchResult) models.SearchResult { r.MediaType = "audiobook"; return r },
		}
		for name, mutate := range variants {
			t.Run(name, func(t *testing.T) {
				other := mutate(base)
				other.MD5 = "bbb"
				if got := CollapseDuplicateCopies([]models.SearchResult{base, other}); len(got) != 2 {
					t.Errorf("differing %s merged: len = %d, want 2", name, len(got))
				}
			})
		}
	})

	t.Run("punctuation and casing do not keep copies apart", func(t *testing.T) {
		a := annas("aaa", "The Mark of Athena", "1.3MB")
		b := annas("bbb", "the mark of athena!", "1.3 MB")
		if got := CollapseDuplicateCopies([]models.SearchResult{a, b}); len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
	})

	t.Run("hashless results are never merged", func(t *testing.T) {
		// Two torrent rows for one release differ by seeders and indexer, which
		// the key does not cover — so they must pass through untouched.
		torrent := models.SearchResult{
			Source: "prowlarr", Title: "The Mark of Athena", Format: "epub",
			SizeHuman: "1.3MB", Seeders: 12, Indexer: "Indexer A",
		}
		other := torrent
		other.Seeders = 3
		other.Indexer = "Indexer B"
		got := CollapseDuplicateCopies([]models.SearchResult{torrent, other})
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got[0].Copies != 0 || got[1].Copies != 0 {
			t.Errorf("Copies set on unmerged rows: %d, %d", got[0].Copies, got[1].Copies)
		}
	})

	t.Run("order and singletons are preserved", func(t *testing.T) {
		in := []models.SearchResult{
			annas("aaa", "First", "1MB"),
			annas("bbb", "Second", "2MB"),
			annas("ccc", "First", "1MB"),
			annas("ddd", "Third", "3MB"),
		}
		got := CollapseDuplicateCopies(in)
		want := []string{"First", "Second", "Third"}
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d", len(got), len(want))
		}
		for i, title := range want {
			if got[i].Title != title {
				t.Errorf("got[%d].Title = %q, want %q", i, got[i].Title, title)
			}
		}
		if got[0].Copies != 2 {
			t.Errorf("First Copies = %d, want 2", got[0].Copies)
		}
		if got[1].Copies != 0 {
			t.Errorf("Second Copies = %d, want 0 (nothing merged)", got[1].Copies)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		if got := CollapseDuplicateCopies(nil); len(got) != 0 {
			t.Errorf("len = %d, want 0", len(got))
		}
	})
}
