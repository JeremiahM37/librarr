package download

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectFileExtension(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		expected string
	}{
		{
			name:     "PDF magic bytes",
			content:  append([]byte("%PDF-1.6\n"), make([]byte, 100)...),
			expected: ".pdf",
		},
		{
			name:     "ZIP/EPUB magic bytes (PK\\x03\\x04)",
			content:  append([]byte{0x50, 0x4B, 0x03, 0x04}, make([]byte, 100)...),
			expected: ".epub",
		},
		{
			name:     "ZIP/EPUB magic bytes (PK\\x05\\x06 empty archive)",
			content:  append([]byte{0x50, 0x4B, 0x05, 0x06}, make([]byte, 100)...),
			expected: ".epub",
		},
		{
			name:     "RAR/CBR magic bytes",
			content:  append([]byte("Rar!"), make([]byte, 100)...),
			expected: ".cbr",
		},
		{
			name:     "MOBI BOOK header",
			content:  append([]byte("BOOK"), make([]byte, 100)...),
			expected: ".mobi",
		},
		{
			name:     "Unrecognized format",
			content:  append([]byte("randomdata"), make([]byte, 100)...),
			expected: "",
		},
		{
			name:     "Too small to detect",
			content:  []byte("xx"),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.bin")
			if err := os.WriteFile(path, tt.content, 0644); err != nil {
				t.Fatalf("write test file: %v", err)
			}

			ext, err := detectFileExtension(path)
			if err != nil {
				t.Fatalf("detectFileExtension: %v", err)
			}
			if ext != tt.expected {
				t.Errorf("got %q, expected %q", ext, tt.expected)
			}
		})
	}
}

func TestDetectFileExtension_MissingFile(t *testing.T) {
	_, err := detectFileExtension("/nonexistent/file.bin")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

// TestPDFSavedAsEPUBScenario simulates issue #8: server returns a PDF file
// but the Content-Type header says it's an EPUB (or octet-stream defaulting
// to EPUB). Our fix should detect the PDF and rename the file.
func TestPDFSavedAsEPUBScenario(t *testing.T) {
	dir := t.TempDir()

	// Write a file that looks like a PDF but is saved with .epub extension.
	epubPath := filepath.Join(dir, "book.epub")
	pdfContent := append([]byte("%PDF-1.4\n%"), make([]byte, 2000)...)
	if err := os.WriteFile(epubPath, pdfContent, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	ext, err := detectFileExtension(epubPath)
	if err != nil {
		t.Fatalf("detectFileExtension: %v", err)
	}
	if ext != ".pdf" {
		t.Errorf("detection failed: got %q, expected %q", ext, ".pdf")
	}
}
