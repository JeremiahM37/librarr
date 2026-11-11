package organize

import (
	"archive/zip"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// writeZip writes the named entries into a zip at path.
func writeZip(t *testing.T, path string, entries map[string][]byte) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(file)
	for name, data := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func manifestOPF(meta, items string) []byte {
	return []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata>` + meta + `</metadata><manifest>` + items + `</manifest></package>`)
}

func TestExtractEPUBCoverMetaName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	img := []byte("jpeg-bytes")
	writeZip(t, path, map[string][]byte{
		"content.opf":       manifestOPF(`<meta name="cover" content="cover"/>`, `<item id="cover" href="images/00012.jpeg"/>`),
		"images/00012.jpeg": img,
	})
	got, err := ExtractEPUBCover(path, 1<<20)
	if err != nil || !bytes.Equal(got, img) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestExtractEPUBCoverMetaImageName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	img := []byte("jpg")
	writeZip(t, path, map[string][]byte{
		"OEBPS/content.opf":      manifestOPF(`<meta name="cover-image" content="cover.jpg"/>`, `<item id="cover.jpg" href="Images/cover.jpg"/>`),
		"OEBPS/Images/cover.jpg": img,
	})
	got, err := ExtractEPUBCover(path, 1<<20)
	if err != nil || !bytes.Equal(got, img) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestExtractEPUBCoverProperties(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	img := []byte("png")
	writeZip(t, path, map[string][]byte{
		"content.opf": manifestOPF(`<meta name="cover" content="old"/>`,
			`<item id="old" href="old.jpg"/><item id="n" href="c.png" properties="cover-image"/>`),
		"old.jpg": []byte("old"),
		"c.png":   img,
	})
	got, err := ExtractEPUBCover(path, 1<<20)
	if err != nil || !bytes.Equal(got, img) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestExtractEPUBCoverEncodedHref(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	img := []byte("x")
	writeZip(t, path, map[string][]byte{
		"content.opf":  manifestOPF(`<meta name="cover" content="c"/>`, `<item id="c" href="my%20cover.jpg"/>`),
		"my cover.jpg": img,
	})
	got, err := ExtractEPUBCover(path, 1<<20)
	if err != nil || !bytes.Equal(got, img) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestExtractEPUBCoverNone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	writeZip(t, path, map[string][]byte{
		"content.opf": manifestOPF(``, `<item id="c" href="c.jpg"/>`),
		"c.jpg":       []byte("x"),
	})
	if _, err := ExtractEPUBCover(path, 1<<20); err == nil {
		t.Fatal("want error")
	}
}

func TestExtractEPUBCoverDanglingHref(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	writeZip(t, path, map[string][]byte{
		"content.opf": manifestOPF(`<meta name="cover" content="c"/>`, `<item id="c" href="../../etc/passwd"/>`),
	})
	if _, err := ExtractEPUBCover(path, 1<<20); err == nil {
		t.Fatal("want error")
	}
}

func TestExtractEPUBCoverTooLarge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	writeZip(t, path, map[string][]byte{
		"content.opf": manifestOPF(`<meta name="cover" content="c"/>`, `<item id="c" href="big.jpg"/>`),
		"big.jpg":     make([]byte, 2<<20),
	})
	if _, err := ExtractEPUBCover(path, 1<<20); !errors.Is(err, ErrCoverTooLarge) {
		t.Fatalf("err = %v, want ErrCoverTooLarge", err)
	}
}

func TestExtractEPUBCoverCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.epub")
	if err := os.WriteFile(path, []byte("not a zip"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ExtractEPUBCover(path, 1<<20); err == nil {
		t.Fatal("want error")
	}
}
