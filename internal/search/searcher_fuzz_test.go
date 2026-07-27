package search

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jamie75/librarr/internal/models"
)

func FuzzTitleRelevant(f *testing.F) {
	seeds := []struct{ title, query string }{
		{"", ""},
		{"The Great Gatsby", "gatsby"},
		{"作者", "book"},
		{strings.Repeat("x", 5000), "x"},
		{"'; DROP TABLE--", "drop"},
		{"%00\x00hidden", "hidden"},
	}
	for _, s := range seeds {
		f.Add(s.title, s.query)
	}

	f.Fuzz(func(t *testing.T, title, query string) {
		if utf8.ValidString(title) && utf8.ValidString(query) {
			_ = titleRelevant(title, query)
		}
	})
}

func FuzzFilterResults(f *testing.F) {
	f.Add("Test Book", "test", true)
	f.Add("Norwegian Edition", "book", true)
	f.Add("中文", "query", false)

	f.Fuzz(func(t *testing.T, title, query string, foreignFilter bool) {
		if !utf8.ValidString(title) || !utf8.ValidString(query) {
			return
		}
		input := []models.SearchResult{{
			Source: "annas",
			Title:  title,
			Format: "epub",
		}}
		_ = FilterResults(input, query, foreignFilter)
	})
}

func FuzzIsForeignTitle(f *testing.F) {
	f.Add("Normal English Title")
	f.Add("Deutsch Ausgabe")
	f.Add(string([]byte{0xff, 0xfe}))

	f.Fuzz(func(t *testing.T, title string) {
		if utf8.ValidString(title) {
			_ = isForeignTitle(title)
		}
	})
}
