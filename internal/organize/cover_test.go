package organize

import (
	"archive/zip"
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractEPUBCoverFromManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "book.epub")
	writeCoverEPUB(t, path, testPNG(t), "image/png")

	cover, err := ExtractEPUBCover(path)
	if err != nil {
		t.Fatalf("ExtractEPUBCover: %v", err)
	}
	if cover == nil {
		t.Fatal("expected cover")
	}
	if cover.MimeType != "image/jpeg" {
		t.Fatalf("mime = %q, want image/jpeg", cover.MimeType)
	}
	if cover.Width != 1 || cover.Height != 1 {
		t.Fatalf("size = %dx%d, want 1x1", cover.Width, cover.Height)
	}
	if len(cover.Data) == 0 {
		t.Fatal("expected cover data")
	}
}

func testPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{R: 0xff, A: 0xff})
	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestExtractEPUBCoverMissingReturnsNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "book.epub")
	writeCoverEPUB(t, path, nil, "")

	cover, err := ExtractEPUBCover(path)
	if err != nil {
		t.Fatalf("ExtractEPUBCover: %v", err)
	}
	if cover != nil {
		t.Fatalf("cover = %+v, want nil", cover)
	}
}

func TestExtractEPUBCoverRejectsCorruptImage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "book.epub")
	writeCoverEPUB(t, path, []byte("<html>not image</html>"), "image/png")

	cover, err := ExtractEPUBCover(path)
	if err == nil {
		t.Fatalf("expected corrupt artwork error, got cover %+v", cover)
	}
}

func writeCoverEPUB(t *testing.T, target string, cover []byte, mediaType string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(target)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	container, err := zw.Create("META-INF/container.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(container, `<container><rootfiles><rootfile full-path="OPS/content.opf"/></rootfiles></container>`); err != nil {
		t.Fatal(err)
	}
	opf, err := zw.Create("OPS/content.opf")
	if err != nil {
		t.Fatal(err)
	}
	manifest := ""
	meta := ""
	if cover != nil {
		meta = `<meta name="cover" content="cover-image"/>`
		manifest = fmt.Sprintf(`<manifest><item id="cover-image" href="images/cover.png" media-type="%s"/></manifest>`, mediaType)
	}
	if _, err := fmt.Fprintf(opf, `<package><metadata><title>Book</title><creator>Author</creator>%s</metadata>%s</package>`, meta, manifest); err != nil {
		t.Fatal(err)
	}
	if cover != nil {
		img, err := zw.Create("OPS/images/cover.png")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := img.Write(cover); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}
