package api

import (
	"testing"
	"time"
)

func TestParseShelfCSVHeader(t *testing.T) {
	t.Run("goodreads header", func(t *testing.T) {
		cols, err := parseShelfCSVHeader([]string{
			"Book Id", "Title", "Author", "Author l-f", "My Rating",
			"ISBN", "Exclusive Shelf", "Date Read",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cols.title != 1 || cols.author != 2 || cols.rating != 4 || cols.isbn != 5 || cols.shelf != 6 || cols.dateRead != 7 {
			t.Errorf("wrong columns: %+v", cols)
		}
	})

	t.Run("goodreads author l-f fallback", func(t *testing.T) {
		cols, err := parseShelfCSVHeader([]string{"Title", "Author l-f"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cols.author != 1 {
			t.Errorf("author = %d, want 1", cols.author)
		}
	})

	t.Run("storygraph header", func(t *testing.T) {
		cols, err := parseShelfCSVHeader([]string{
			"Title", "Authors", "ISBN13", "Read Status", "Star Rating", "Last Date Read",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cols.shelf != 3 || cols.rating != 4 || cols.dateRead != 5 {
			t.Errorf("wrong columns: %+v", cols)
		}
	})

	t.Run("missing title", func(t *testing.T) {
		if _, err := parseShelfCSVHeader([]string{"Author", "ISBN"}); err == nil {
			t.Error("expected error for missing Title column")
		}
	})
}

func TestParseShelfCSVRow(t *testing.T) {
	cols := shelfCSVColumns{title: 0, author: 1, shelf: 2, rating: 3, dateRead: 4, isbn: -1}

	t.Run("full row", func(t *testing.T) {
		row, ok := parseShelfCSVRow(cols, []string{"Dune", "Herbert, Frank", "Read", "5", "2024/01/15"})
		if !ok {
			t.Fatal("expected row to parse")
		}
		if row.Title != "Dune" {
			t.Errorf("Title = %q", row.Title)
		}
		if row.Author != "Frank Herbert" {
			t.Errorf("Author = %q, want normalized 'Frank Herbert'", row.Author)
		}
		if row.Shelf != "read" {
			t.Errorf("Shelf = %q", row.Shelf)
		}
		if row.Rating != 5 {
			t.Errorf("Rating = %d", row.Rating)
		}
		want := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		if row.DateRead == nil || !row.DateRead.Equal(want) {
			t.Errorf("DateRead = %v, want %v", row.DateRead, want)
		}
	})

	t.Run("empty title skipped", func(t *testing.T) {
		if _, ok := parseShelfCSVRow(cols, []string{"  ", "Someone"}); ok {
			t.Error("expected row with empty title to be skipped")
		}
	})

	t.Run("short record tolerated", func(t *testing.T) {
		row, ok := parseShelfCSVRow(cols, []string{"Only Title"})
		if !ok {
			t.Fatal("expected row to parse")
		}
		if row.Author != "" || row.Shelf != "" || row.Rating != 0 || row.DateRead != nil {
			t.Errorf("expected zero values for missing fields, got %+v", row)
		}
	})
}

func TestNormalizeAuthorName(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Herbert, Frank", "Frank Herbert"},
		{"Frank Herbert", "Frank Herbert"},
		{"", ""},
		{"Good Omens, Terry Pratchett and Neil Gaiman", "Good Omens, Terry Pratchett and Neil Gaiman"},
	}
	for _, c := range cases {
		if got := normalizeAuthorName(c.in); got != c.want {
			t.Errorf("normalizeAuthorName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseReadDate(t *testing.T) {
	cases := []struct {
		in   string
		want *time.Time
	}{
		{"", nil},
		{"not a date", nil},
	}
	for _, c := range cases {
		if got := parseReadDate(c.in); got != nil {
			t.Errorf("parseReadDate(%q) = %v, want nil", c.in, got)
		}
		_ = c.want
	}

	formats := map[string]string{
		"2024/01/15":   "slash",
		"2024-01-15":   "dash",
		"01/15/2024":   "us",
		"Jan 15, 2024": "written",
	}
	want := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	for in, name := range formats {
		got := parseReadDate(in)
		if got == nil || !got.Equal(want) {
			t.Errorf("parseReadDate(%q) [%s] = %v, want %v", in, name, got, want)
		}
	}
}
