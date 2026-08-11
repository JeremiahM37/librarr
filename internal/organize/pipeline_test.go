package organize

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

// errCrossDevice stands in for EXDEV so tests can force moveFile's copy fallback.
var errCrossDevice = syscall.EXDEV

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

func TestMoveFileSameFilesystem(t *testing.T) {
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
	if string(got) != string(payload) {
		t.Fatalf("dst content = %q, want %q", got, payload)
	}
}

func TestMoveFileStreamsWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.m4b")
	dst := filepath.Join(dir, "dst.m4b")
	payload := []byte("forced-fallback-copy")
	if err := os.WriteFile(src, payload, 0644); err != nil {
		t.Fatal(err)
	}

	orig := renameFile
	renameFile = func(string, string) error {
		return &os.LinkError{Op: "rename", Old: src, New: dst, Err: errCrossDevice}
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
	if string(got) != string(payload) {
		t.Fatalf("dst content = %q, want %q", got, payload)
	}
}

func TestCopyFileForOrgStreamsLargeFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "big.m4b")
	dst := filepath.Join(dir, "big-copy.m4b")

	// Large enough that a ReadFile fallback would be obviously wrong under a
	// small container mem_limit; still cheap for CI.
	const size = 32 << 20 // 32 MiB
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	written := 0
	for written < size {
		n, err := f.Write(chunk)
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		written += n
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := copyFileForOrg(src, dst); err != nil {
		t.Fatalf("copyFileForOrg: %v", err)
	}

	srcInfo, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	dstInfo, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if srcInfo.Size() != dstInfo.Size() {
		t.Fatalf("size mismatch: src=%d dst=%d", srcInfo.Size(), dstInfo.Size())
	}
}

func TestCopyFileForOrgCleansPartialOnFailure(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "missing-src.m4b")
	dst := filepath.Join(dir, "should-not-remain.m4b")

	err := copyFileForOrg(src, dst)
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("partial dst should be removed, stat err=%v", err)
	}
}
