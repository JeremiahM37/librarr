package wanted

import (
	"testing"

	"github.com/jamie75/librarr/internal/models"
)

func TestNormalizeBookProwlarrReleaseExample(t *testing.T) {
	raw := "Rebel Prince, the Power, Passion and Defiance of Prince Charles by Tom Bower [ENG / MOBI]"
	result := NormalizeBook(models.WantedBook{
		Title:              raw,
		Source:             "torrent",
		OriginSource:       "prowlarr",
		OriginReleaseTitle: raw,
		MediaType:          "ebook",
	})

	got := result.Normalized
	if got.Title != "Rebel Prince: The Power, Passion and Defiance of Prince Charles" {
		t.Fatalf("title = %q", got.Title)
	}
	if got.Author != "Tom Bower" {
		t.Fatalf("author = %q", got.Author)
	}
	if got.Language != "en" {
		t.Fatalf("language = %q", got.Language)
	}
	if got.PreferredFormat != "mobi" {
		t.Fatalf("preferred format = %q", got.PreferredFormat)
	}
	if got.OriginReleaseTitle != raw {
		t.Fatalf("origin release title = %q", got.OriginReleaseTitle)
	}
	if result.Confidence != ConfidenceHigh {
		t.Fatalf("confidence = %q", result.Confidence)
	}
}

func TestNormalizeBookStructuredFieldsWin(t *testing.T) {
	result := NormalizeBook(models.WantedBook{
		Title:              "Clean Title",
		Author:             "Known Author",
		Language:           "fr",
		PreferredFormat:    "epub",
		OriginReleaseTitle: "Noisy Release by Other Person [ENG / MOBI]",
		MediaType:          "ebook",
	})
	if result.Normalized.Title != "Clean Title" || result.Normalized.Author != "Known Author" {
		t.Fatalf("structured title/author overwritten: %+v", result.Normalized)
	}
	if result.Normalized.Language != "fr" || result.Normalized.PreferredFormat != "epub" {
		t.Fatalf("structured language/format overwritten: %+v", result.Normalized)
	}
}

func TestNormalizeBookUncertainParsingDoesNotInventAuthor(t *testing.T) {
	result := NormalizeBook(models.WantedBook{
		Title:              "Collected Essays Volume One [EPUB]",
		OriginReleaseTitle: "Collected Essays Volume One [EPUB]",
		Source:             "torrent",
		MediaType:          "ebook",
	})
	if result.Normalized.Author != "" {
		t.Fatalf("author = %q, want empty", result.Normalized.Author)
	}
	if result.Normalized.Title != "Collected Essays Volume One" {
		t.Fatalf("title = %q", result.Normalized.Title)
	}
	if result.Confidence != ConfidenceLow {
		t.Fatalf("confidence = %q", result.Confidence)
	}
}

func TestNormalizeBookAuthorDashTitlePattern(t *testing.T) {
	result := NormalizeBook(models.WantedBook{
		Title:              "Mark R. Levin - Ameritopia: The Unmaking of America (mobi)",
		OriginReleaseTitle: "Mark R. Levin - Ameritopia: The Unmaking of America (mobi)",
		Source:             "torrent",
		MediaType:          "ebook",
	})
	if result.Normalized.Title != "Ameritopia: The Unmaking of America" {
		t.Fatalf("title = %q", result.Normalized.Title)
	}
	if result.Normalized.Author != "Mark R. Levin" {
		t.Fatalf("author = %q", result.Normalized.Author)
	}
	if result.Normalized.PreferredFormat != "mobi" {
		t.Fatalf("format = %q", result.Normalized.PreferredFormat)
	}
}

func TestNormalizeBookTitleByAuthorPattern(t *testing.T) {
	result := NormalizeBook(models.WantedBook{
		Title:              "Men in Black: How the Supreme Court is Destroying America by Mark R Levin [EN / EPUB]",
		OriginReleaseTitle: "Men in Black: How the Supreme Court is Destroying America by Mark R Levin [EN / EPUB]",
		Source:             "torrent",
		MediaType:          "ebook",
	})
	if result.Normalized.Title != "Men in Black: How the Supreme Court is Destroying America" {
		t.Fatalf("title = %q", result.Normalized.Title)
	}
	if result.Normalized.Author != "Mark R. Levin" {
		t.Fatalf("author = %q", result.Normalized.Author)
	}
}

func TestCleanSearchPhraseDropsReleaseTokens(t *testing.T) {
	got := CleanSearchPhrase("Rebel Prince: The Power, Passion and Defiance of Prince Charles [ENG / MOBI]")
	want := "Rebel Prince The Power Passion and Defiance of Prince Charles"
	if got != want {
		t.Fatalf("clean search phrase = %q, want %q", got, want)
	}
}
