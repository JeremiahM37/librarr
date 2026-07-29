package organize

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
)

func TestAudiobookScannerDisabledInNormalizedModeDoesNotInsertLegacyRows(t *testing.T) {
	root := t.TempDir()
	audioRoot := filepath.Join(root, "audiobooks")
	writeTestFile(t, filepath.Join(audioRoot, "Stephen King", "11.22.63 - Part.m4b"), "audio")
	database := newScannerTestDB(t)

	scanner := NewAudiobookScanner(&config.Config{
		LibraryRepositoryMode: "normalized",
		ImportEngine:          "v2",
		AudiobookDir:          audioRoot,
	}, database, nil)

	enabled, reason := scanner.Enabled()
	if enabled {
		t.Fatalf("scanner enabled in normalized mode, reason=%q", reason)
	}

	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		scanner.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("normalized scanner did not return promptly")
	}

	if count, err := database.CountItems(""); err != nil || count != 0 {
		t.Fatalf("legacy rows after normalized startup scanner: count=%d err=%v", count, err)
	}
}

func TestAudiobookScannerLegacyModeParsesAuthorDirectoryAndSingleFileTitle(t *testing.T) {
	root := t.TempDir()
	audioRoot := filepath.Join(root, "audiobooks")
	bookPath := filepath.Join(audioRoot, "Stephen King", "11.22.63 - Part.m4b")
	writeTestFile(t, bookPath, "audio")
	database := newScannerTestDB(t)

	scanner := NewAudiobookScanner(&config.Config{
		LibraryRepositoryMode: "legacy",
		AudiobookDir:          audioRoot,
	}, database, nil)

	scanner.scan()

	items, err := database.GetItems("audiobook", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if items[0].Author != "Stephen King" {
		t.Fatalf("author = %q, want Stephen King", items[0].Author)
	}
	if items[0].Title != "11.22.63" {
		t.Fatalf("title = %q, want 11.22.63", items[0].Title)
	}
	if items[0].FilePath != bookPath {
		t.Fatalf("file path = %q, want %q", items[0].FilePath, bookPath)
	}
}

func TestParseAudiobookScanPathHandlesNestedAndMalformedPaths(t *testing.T) {
	root := filepath.Join("books", "audiobooks")
	title, author := parseAudiobookScanPath(root, filepath.Join(root, "Stephen King", "Series", "Book Folder"), []string{
		filepath.Join(root, "Stephen King", "Series", "Book Folder", "track01.m4b"),
		filepath.Join(root, "Stephen King", "Series", "Book Folder", "track02.m4b"),
	})
	if author != "Stephen King" {
		t.Fatalf("author = %q, want Stephen King", author)
	}
	if title != "Book Folder" {
		t.Fatalf("title = %q, want Book Folder", title)
	}

	title, author = parseAudiobookScanPath(root, filepath.Join("outside", "Author"), []string{filepath.Join("outside", "Author", "Title - Part 01.m4b")})
	if author != "" {
		t.Fatalf("outside author = %q, want empty", author)
	}
	if title != "Title" {
		t.Fatalf("outside title = %q, want Title", title)
	}
}

func TestAudiobookScannerLegacyModeKeepsDifferentTitlesDistinct(t *testing.T) {
	root := t.TempDir()
	audioRoot := filepath.Join(root, "audiobooks")
	writeTestFile(t, filepath.Join(audioRoot, "Stephen King", "11.22.63 - Part.m4b"), "audio-one")
	writeTestFile(t, filepath.Join(audioRoot, "Stephen King", "The Stand - Part.m4b"), "audio-two")
	database := newScannerTestDB(t)

	scanner := NewAudiobookScanner(&config.Config{
		LibraryRepositoryMode: "legacy",
		AudiobookDir:          audioRoot,
	}, database, nil)

	scanner.scan()

	items, err := database.GetItems("audiobook", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	titles := map[string]bool{}
	for _, item := range items {
		titles[item.Title] = true
		if item.Author != "Stephen King" {
			t.Fatalf("author = %q, want Stephen King", item.Author)
		}
	}
	if !titles["11.22.63"] || !titles["The Stand"] {
		t.Fatalf("titles = %+v", titles)
	}
}

func newScannerTestDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "librarr.db"))
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}
