package zlibraryparse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestBooksFromJSON(t *testing.T) {
	t.Run("standard books array with key variants", func(t *testing.T) {
		books, err := BooksFromJSON(fixture(t, "search_books.json"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(books) != 2 {
			t.Fatalf("got %d books, want 2", len(books))
		}

		b := books[0]
		if b.ID != 42 || b.Hash != "abc123" || b.Title != "The Go Programming Language" {
			t.Errorf("book 0 core fields wrong: %+v", b)
		}
		if b.Extension != "epub" || b.Filesize != 2500000 || b.Pages != 380 {
			t.Errorf("book 0 detail fields wrong: %+v", b)
		}
		if b.DL != "/dl/2222/token42" {
			t.Errorf("book 0 DL = %q", b.DL)
		}

		// Second book uses alternate key names and string-typed values.
		b = books[1]
		if b.Hash != "def456" {
			t.Errorf("book 1 Hash = %q, want def456 (from book_hash)", b.Hash)
		}
		if b.Title != "Learning Go" {
			t.Errorf("book 1 Title = %q, want Learning Go (from name)", b.Title)
		}
		if b.Author != "Jon Bodner" {
			t.Errorf("book 1 Author = %q, want Jon Bodner (from authors array)", b.Author)
		}
		if b.Extension != "pdf" {
			t.Errorf("book 1 Extension = %q, want pdf (from format)", b.Extension)
		}
		if b.Filesize != 12345 {
			t.Errorf("book 1 Filesize = %d, want 12345 (comma-separated string)", b.Filesize)
		}
		if b.DL != "https://z.example.org/dl/7/tok7" {
			t.Errorf("book 1 DL = %q", b.DL)
		}
	})

	t.Run("books nested under result.data.items", func(t *testing.T) {
		books, err := BooksFromJSON(fixture(t, "search_nested_result.json"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(books) != 1 {
			t.Fatalf("got %d books, want 1", len(books))
		}
		b := books[0]
		if b.ID != 99 || b.Hash != "cafebabe" || b.Title != "Nested Response Book" {
			t.Errorf("nested book fields wrong: %+v", b)
		}
		if b.Author != "Deep Author" {
			t.Errorf("Author = %q, want Deep Author (from nested object)", b.Author)
		}
		if b.Filesize != 1048576 {
			t.Errorf("Filesize = %d, want 1048576 (from sizeBytes)", b.Filesize)
		}
	})

	t.Run("error response surfaces messages", func(t *testing.T) {
		_, err := BooksFromJSON(fixture(t, "search_error.json"))
		if err == nil {
			t.Fatal("expected error for success=0 response")
		}
		if !strings.Contains(err.Error(), "invalid session") {
			t.Errorf("error %q should contain the server message", err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		if _, err := BooksFromJSON([]byte("<html>not json</html>")); err == nil {
			t.Error("expected error for non-JSON body")
		}
	})

	t.Run("non-object json", func(t *testing.T) {
		if _, err := BooksFromJSON([]byte(`[1,2,3]`)); err == nil {
			t.Error("expected error for array body")
		}
	})

	t.Run("empty object yields no books, no error", func(t *testing.T) {
		books, err := BooksFromJSON([]byte(`{}`))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(books) != 0 {
			t.Errorf("got %d books, want 0", len(books))
		}
	})
}

func TestDetailDownloadFromJSON(t *testing.T) {
	t.Run("nested book object", func(t *testing.T) {
		dl, err := DetailDownloadFromJSON(fixture(t, "detail_book.json"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dl != "https://z.example.org/dl/42/token" {
			t.Errorf("dl = %q", dl)
		}
	})

	t.Run("flat download_url key", func(t *testing.T) {
		dl, err := DetailDownloadFromJSON(fixture(t, "detail_flat.json"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dl != "/dl/direct/tokflat" {
			t.Errorf("dl = %q", dl)
		}
	})

	t.Run("error response", func(t *testing.T) {
		if _, err := DetailDownloadFromJSON(fixture(t, "search_error.json")); err == nil {
			t.Error("expected error for success=0 response")
		}
	})

	t.Run("no link present", func(t *testing.T) {
		dl, err := DetailDownloadFromJSON([]byte(`{"success": true, "book": {"id": 1}}`))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dl != "" {
			t.Errorf("dl = %q, want empty", dl)
		}
	})
}

func TestFindDownloadLinkInHTML(t *testing.T) {
	t.Run("relative link resolved and unescaped", func(t *testing.T) {
		got := FindDownloadLinkInHTML("https://z.example.org", fixture(t, "book_page.html"))
		want := "https://z.example.org/dl/1234567/a1b2c3&source=book"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("page without download link", func(t *testing.T) {
		if got := FindDownloadLinkInHTML("https://z.example.org", fixture(t, "book_page_no_link.html")); got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})

	t.Run("absolute link kept as-is", func(t *testing.T) {
		body := []byte(`<a href="https://cdn.example.org/dl/55/tok">Download</a>`)
		got := FindDownloadLinkInHTML("https://z.example.org", body)
		if got != "https://cdn.example.org/dl/55/tok" {
			t.Errorf("got %q", got)
		}
	})
}

func TestJSONTruthy(t *testing.T) {
	cases := []struct {
		in   any
		def  bool
		want bool
	}{
		{nil, true, true},
		{nil, false, false},
		{true, false, true},
		{false, true, false},
		{float64(1), false, true},
		{float64(0), true, false},
		{"1", false, true},
		{"true", false, true},
		{"ok", false, true},
		{"success", false, true},
		{"0", true, false},
		{"no", true, false},
		{[]any{}, true, true},
	}
	for _, c := range cases {
		if got := JSONTruthy(c.in, c.def); got != c.want {
			t.Errorf("JSONTruthy(%v, %v) = %v, want %v", c.in, c.def, got, c.want)
		}
	}
}

func TestAbsoluteURL(t *testing.T) {
	cases := []struct{ base, path, want string }{
		{"https://z.example.org", "/dl/1/tok", "https://z.example.org/dl/1/tok"},
		{"https://z.example.org/", "dl/1/tok", "https://z.example.org/dl/1/tok"},
		{"https://z.example.org", "https://other.example.org/dl", "https://other.example.org/dl"},
		{"https://z.example.org", "", ""},
	}
	for _, c := range cases {
		if got := AbsoluteURL(c.base, c.path); got != c.want {
			t.Errorf("AbsoluteURL(%q, %q) = %q, want %q", c.base, c.path, got, c.want)
		}
	}
}

func TestErrorMessage(t *testing.T) {
	cases := []struct {
		obj  map[string]any
		want string
	}{
		{map[string]any{"error": "bad key"}, "bad key"},
		{map[string]any{"message": "nope"}, "nope"},
		{map[string]any{"errors": []any{"a", "b"}}, "a; b"},
		{map[string]any{}, "unknown error"},
	}
	for _, c := range cases {
		if got := ErrorMessage(c.obj); got != c.want {
			t.Errorf("ErrorMessage(%v) = %q, want %q", c.obj, got, c.want)
		}
	}
}
