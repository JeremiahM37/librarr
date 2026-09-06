package organize

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

func TestAudiobookScannerSkipsTorrentTrackedPaths(t *testing.T) {
	tests := []struct {
		name string
		path func(root, audioPath string) string
	}{
		{
			name: "file import",
			path: func(_, audioPath string) string { return audioPath },
		},
		{
			name: "directory import",
			path: func(root, _ string) string { return filepath.Join(root, "Author", "Book") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			bookDir := filepath.Join(root, "Author", "Book")
			if err := os.MkdirAll(bookDir, 0755); err != nil {
				t.Fatal(err)
			}
			audioPath := filepath.Join(bookDir, "Book.m4b")
			if err := os.WriteFile(audioPath, []byte("audiobook"), 0644); err != nil {
				t.Fatal(err)
			}

			database, err := db.New(filepath.Join(root, "library.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()

			trackedPath := tt.path(root, audioPath)
			if _, err := database.AddItem(&models.LibraryItem{
				Title:     "Book",
				Author:    "Author",
				FilePath:  trackedPath,
				MediaType: "audiobook",
				Source:    "torrent",
				SourceID:  "torrent-hash",
			}); err != nil {
				t.Fatal(err)
			}

			scanner := NewAudiobookScanner(&config.Config{AudiobookDir: root}, database, nil)
			scanner.scan()

			scanEvents, err := database.GetActivityLogCount("", "scan_import")
			if err != nil {
				t.Fatal(err)
			}
			if scanEvents != 0 {
				t.Fatalf("scan_import events = %d, want 0", scanEvents)
			}

			itemCount, err := database.CountItems("audiobook")
			if err != nil {
				t.Fatal(err)
			}
			if itemCount != 1 {
				t.Fatalf("audiobook library items = %d, want 1", itemCount)
			}
		})
	}
}

// A library row naming the audiobook root must not mark the whole tree as
// tracked. No import records the root, but a restored library export carries
// its file paths verbatim, and treating that row as an ancestor match would
// silently stop the scanner from ever importing again.
func TestAudiobookScannerIgnoresRootLevelTrackedPath(t *testing.T) {
	root := t.TempDir()
	for _, book := range []string{"BookA", "BookB"} {
		bookDir := filepath.Join(root, "Author", book)
		if err := os.MkdirAll(bookDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bookDir, "Book.m4b"), []byte(book), 0644); err != nil {
			t.Fatal(err)
		}
	}

	database, err := db.New(filepath.Join(root, "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if _, err := database.AddItem(&models.LibraryItem{
		Title:     "root row",
		FilePath:  root,
		MediaType: "audiobook",
		Source:    "import",
		SourceID:  "restored-root",
	}); err != nil {
		t.Fatal(err)
	}

	NewAudiobookScanner(&config.Config{AudiobookDir: root}, database, nil).scan()

	itemCount, err := database.CountItems("audiobook")
	if err != nil {
		t.Fatal(err)
	}
	if itemCount != 3 {
		t.Fatalf("audiobook library items = %d, want 3 (the root row plus both books)", itemCount)
	}
}
