package organize

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractEbookMetadataFilenameFallbacks(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"The Guardian's Path.epub", "Author Name - A Book.mobi", "A Book.azw3", "A Book.pdf"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("not embedded metadata"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	got := ExtractEbookMetadata(filepath.Join(dir, "Author Name - A Book.mobi"))
	if got.Title != "A Book" || got.Author != "Author Name" {
		t.Fatalf("filename metadata = %+v, want title/author from filename", got)
	}
	got = ExtractEbookMetadata(filepath.Join(dir, "A Book.azw3"))
	if got.Title != "A Book" {
		t.Fatalf("AZW3 filename title = %q, want A Book", got.Title)
	}
}

func TestExtractEbookMetadataMOBIFilenameTitleAuthor(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name       string
		wantTitle  string
		wantAuthor string
	}{
		{
			name:       "Ameritopia-The Unmaking of America - Mark R. Levin.mobi",
			wantTitle:  "Ameritopia-The Unmaking of America",
			wantAuthor: "Mark R. Levin",
		},
		{
			name:       "Disney - [Prince of Persia- The Sands of Time] - The Guardian's Path - Carla Jablonski (retail) (epub).mobi",
			wantTitle:  "The Guardian's Path",
			wantAuthor: "Carla Jablonski",
		},
		{
			name:       "Disney - [Prince of Persia- The Sands of Time] - To Right a Wrong - Carla Jablonski (retail) (epub).mobi",
			wantTitle:  "To Right a Wrong",
			wantAuthor: "Carla Jablonski",
		},
	}

	for _, tc := range cases {
		path := filepath.Join(dir, tc.name)
		if err := os.WriteFile(path, []byte("not embedded metadata"), 0644); err != nil {
			t.Fatal(err)
		}
		got := ExtractEbookMetadata(path)
		if got.Title != tc.wantTitle || got.Author != tc.wantAuthor {
			t.Fatalf("%q metadata = %+v, want title %q author %q", tc.name, got, tc.wantTitle, tc.wantAuthor)
		}
	}
}

func TestExtractEbookMetadataEPUBFilenameFallbackUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Disney - [Prince of Persia- The Sands of Time] - The Guardian's Path - Carla Jablonski (retail) (epub).epub")
	if err := os.WriteFile(path, []byte("not an epub"), 0644); err != nil {
		t.Fatal(err)
	}

	got := ExtractEbookMetadata(path)
	if got.Title != "- The Guardian's Path - Carla Jablonski (retail) (epub)" || got.Author != "Disney" {
		t.Fatalf("EPUB fallback metadata = %+v, want existing generic filename behavior", got)
	}
}

func TestExtractEbookMetadataPrefersEPUBMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Torrent Name.epub")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zipWriter := zip.NewWriter(file)
	opf, err := zipWriter.Create("content.opf")
	if err != nil {
		t.Fatal(err)
	}
	_, err = opf.Write([]byte(`<package><metadata><dc:title xmlns:dc="x">The Guardian's Path</dc:title><dc:creator xmlns:dc="x">Doe, Jane</dc:creator></metadata></package>`))
	if err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	got := ExtractEbookMetadata(path)
	if got.Title != "The Guardian's Path" || got.Author != "Jane Doe" {
		t.Fatalf("embedded metadata = %+v, want EPUB metadata", got)
	}
}

func TestExtractEbookMetadataPDFInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Torrent Name.pdf")
	if err := os.WriteFile(path, []byte("%PDF-1.7 /Title (The Guardian's Path) /Author (Jane Doe)"), 0644); err != nil {
		t.Fatal(err)
	}
	got := ExtractEbookMetadata(path)
	if got.Title != "The Guardian's Path" || got.Author != "Jane Doe" {
		t.Fatalf("PDF metadata = %+v, want PDF info values", got)
	}
}
