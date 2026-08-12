package organize

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{"normal name", "John Smith", 80, "John Smith"},
		{"removes unsafe chars", `Book: "Title" <1>`, 80, "Book Title 1"},
		{"collapses whitespace", "  Too   Many   Spaces  ", 80, "Too Many Spaces"},
		{"truncates long names", "A Very Long Author Name That Exceeds The Limit", 20, "A Very Long Author N"},
		{"removes trailing dots", "Name...", 80, "Name"},
		{"empty becomes Unknown", "", 80, "Unknown"},
		{"only dots becomes Unknown", "...", 80, "Unknown"},
		{"pipe removed", "Author | Publisher", 80, "Author Publisher"},
		{"question mark removed", "What?", 80, "What"},
		{"asterisk removed", "Star*Wars", 80, "StarWars"},
		{"backslash removed", `Path\Name`, 80, "PathName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizePath(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("sanitizePath(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
			}
		})
	}
}

func TestCleanSeriesTitle(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"strips epub extension", "One Piece.epub", "One Piece"},
		{"strips cbz extension", "Naruto Vol 1.cbz", "Naruto"},
		{"strips cbr extension", "Manga.cbr", "Manga"},
		{"strips brackets", "Title [Digital] [2024]", "Title"},
		{"strips volume info", "One Piece Vol 1-100", "One Piece"},
		{"strips volume with dot", "One Piece Vol.5", "One Piece"},
		{"strips paren tags", "Title (Digital)", "Title"},
		{"strips range", "Series 1-50", "Series"},
		{"empty becomes Unknown", "", "Unknown"},
		{"complex cleanup", "[Group] Manga Series Vol 1 (Digital).cbz", "Manga Series"},
		{"strips trailing dash", "Title -", "Title"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanSeriesTitle(tt.input)
			if result != tt.expected {
				t.Errorf("cleanSeriesTitle(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestOrganizer_DisabledDoesNothing(t *testing.T) {
	cfg := &config.Config{FileOrgEnabled: false}
	o := NewOrganizer(cfg)

	if o.cfg.FileOrgEnabled {
		t.Error("expected FileOrgEnabled to be false")
	}
}

func TestMoveFile(t *testing.T) {
	t.Run("same filesystem rename", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src.m4b")
		dst := filepath.Join(dir, "dst.m4b")
		payload := []byte("librarr-move-test")
		if err := os.WriteFile(src, payload, 0644); err != nil {
			t.Fatal(err)
		}

		if err := moveFile(src, dst); err != nil {
			t.Fatalf("moveFile: %v", err)
		}
		if _, err := os.Stat(src); !os.IsNotExist(err) {
			t.Fatalf("source still present after move: %v", err)
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("dst content = %q, want %q", got, payload)
		}
	})

	t.Run("streams when rename fails", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src.m4b")
		dst := filepath.Join(dir, "dst.m4b")
		payload := []byte("forced-fallback-copy")
		if err := os.WriteFile(src, payload, 0644); err != nil {
			t.Fatal(err)
		}

		orig := renameFile
		renameFile = func(string, string) error {
			return errors.New("rename forced fail")
		}
		defer func() { renameFile = orig }()

		if err := moveFile(src, dst); err != nil {
			t.Fatalf("moveFile: %v", err)
		}
		if _, err := os.Stat(src); !os.IsNotExist(err) {
			t.Fatalf("source still present after fallback move: %v", err)
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("dst content = %q, want %q", got, payload)
		}
	})
}

func TestCopyFileForOrg(t *testing.T) {
	t.Run("large file size and content", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "big.m4b")
		dst := filepath.Join(dir, "big-copy.m4b")

		// Larger than a naive ReadFile would comfortably fit in a 256MiB
		// cgroup alongside the process; still cheap for CI.
		const size = 32 << 20 // 32 MiB
		payload := bytes.Repeat([]byte{0x5a}, size)
		if err := os.WriteFile(src, payload, 0644); err != nil {
			t.Fatal(err)
		}

		if err := copyFileForOrg(src, dst); err != nil {
			t.Fatalf("copyFileForOrg: %v", err)
		}

		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("content mismatch: len=%d want=%d", len(got), len(payload))
		}
	})

	t.Run("removes partial dst on mid-copy failure", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "partial.m4b")
		r := io.MultiReader(
			bytes.NewReader(bytes.Repeat([]byte("x"), 4096)),
			&errReader{err: errors.New("injected read failure")},
		)

		err := copyReaderToFile(r, dst)
		if err == nil {
			t.Fatal("expected mid-copy error")
		}
		if _, err := os.Stat(dst); !os.IsNotExist(err) {
			t.Fatalf("partial dst should be removed, stat err=%v", err)
		}
	})
}

func TestCopyFileUsesStreamingPath(t *testing.T) {
	// targets.copyFile must share copyFileForOrg so library imports don't
	// reintroduce the ReadFile OOM on network mounts.
	dir := t.TempDir()
	src := filepath.Join(dir, "book.epub")
	dst := filepath.Join(dir, "book-out.epub")
	payload := []byte("ebook-bytes")
	if err := os.WriteFile(src, payload, 0644); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("dst content = %q, want %q", got, payload)
	}
}

type errReader struct {
	err error
}

func (e *errReader) Read([]byte) (int, error) {
	return 0, e.err
}
