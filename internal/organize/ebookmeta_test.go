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

// writeTestEPUB writes an EPUB whose OPF carries the given title and author.
func writeTestEPUB(t *testing.T, path, title, author string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zipWriter := zip.NewWriter(file)
	opf, err := zipWriter.Create("content.opf")
	if err != nil {
		t.Fatal(err)
	}
	_, err = opf.Write([]byte(`<package><metadata><dc:title xmlns:dc="x">` + title + `</dc:title><dc:creator xmlns:dc="x">` + author + `</dc:creator></metadata></package>`))
	if err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

// Trackers such as MyAnonamouse often name files "Title - Author", so a MOBI
// beside an EPUB must take the EPUB's metadata instead of reading the
// filename as "Author - Title".
func TestExtractEbookMetadataSetSharesEmbeddedMetadataAcrossFormats(t *testing.T) {
	dir := t.TempDir()
	epub := filepath.Join(dir, "Red Rising - Pierce Brown.epub")
	mobi := filepath.Join(dir, "Red Rising - Pierce Brown.mobi")
	writeTestEPUB(t, epub, "Red Rising", "Pierce Brown")
	if err := os.WriteFile(mobi, []byte("no embedded metadata"), 0644); err != nil {
		t.Fatal(err)
	}

	got := ExtractEbookMetadataSet([]string{epub, mobi})
	want := EbookMetadata{Title: "Red Rising", Author: "Pierce Brown"}
	if got[epub] != want || got[mobi] != want {
		t.Fatalf("metadata = %+v, want both formats as %+v", got, want)
	}
}

// Different books in one download (a pack) must not borrow each other's
// metadata.
func TestExtractEbookMetadataSetKeepsDifferentBooksApart(t *testing.T) {
	dir := t.TempDir()
	epub := filepath.Join(dir, "Book One.epub")
	mobi := filepath.Join(dir, "Other Author - Book Two.mobi")
	writeTestEPUB(t, epub, "Book One", "Jane Doe")
	if err := os.WriteFile(mobi, []byte("no embedded metadata"), 0644); err != nil {
		t.Fatal(err)
	}

	got := ExtractEbookMetadataSet([]string{epub, mobi})
	if want := (EbookMetadata{Title: "Book Two", Author: "Other Author"}); got[mobi] != want {
		t.Fatalf("pack MOBI metadata = %+v, want filename values %+v", got[mobi], want)
	}
}
