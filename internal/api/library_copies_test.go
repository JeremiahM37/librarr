package api

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/organize"
)

// libraryCopyEPUB builds a minimal real EPUB whose metadata names an author.
func libraryCopyEPUB(t *testing.T, title, author string) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	for _, f := range []struct{ name, body string }{
		{"mimetype", "application/epub+zip"},
		{"META-INF/container.xml", `<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="c.opf" media-type="application/oebps-package+xml"/></rootfiles></container>`},
		{"c.opf", `<?xml version="1.0"?><package xmlns="http://www.idpf.org/2007/opf" version="2.0"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>` + title + `</dc:title><dc:creator>` + author + `</dc:creator></metadata></package>`},
	} {
		w, err := z.Create(f.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(f.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// filesUnder lists every regular file below root, relative to it.
func filesUnder(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, _ := filepath.Rel(root, p)
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func libraryCopyServer(t *testing.T) (s *Server, kavitaEbooks, kavitaManga, incoming string) {
	t.Helper()
	base := t.TempDir()
	incoming = filepath.Join(base, "incoming")
	s = manualImportTestServer(t, map[string]string{
		"ebook":    filepath.Join(base, "ebooks"),
		"incoming": incoming,
	})
	kavitaEbooks = filepath.Join(base, "kavita-ebooks")
	kavitaManga = filepath.Join(base, "kavita-manga")
	s.cfg.FileOrgEnabled = true
	s.cfg.MangaDir = filepath.Join(base, "manga")
	s.cfg.KavitaLibraryPath = kavitaEbooks
	s.cfg.KavitaMangaLibraryPath = kavitaManga
	s.organizer = organize.NewOrganizer(s.cfg)
	s.targets = organize.NewLibraryTargets(s.cfg)
	return s, kavitaEbooks, kavitaManga, incoming
}

// An upload names no author, so the organizer reads it from the EPUB. Kavita
// must get one copy, under that author rather than a second one under "Unknown".
func TestUpload_ReachesKavitaOnce(t *testing.T) {
	s, kavita, _, _ := libraryCopyServer(t)

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", "book.epub")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(libraryCopyEPUB(t, "Some Book", "Real Author")); err != nil {
		t.Fatal(err)
	}
	form.WriteField("media_type", "ebook")
	form.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/upload", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	rr := httptest.NewRecorder()
	s.handleUpload(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
	}

	got := filesUnder(t, kavita)
	if len(got) != 1 || !strings.HasPrefix(got[0], filepath.Join("Real Author", "book")+string(filepath.Separator)) {
		t.Fatalf("Kavita library holds %v, want one copy under Real Author/book", got)
	}
}

// Manual import only ever called the organizer, so it must still reach Kavita
// now that the organizer no longer copies there.
func TestManualImport_ReachesKavitaOnce(t *testing.T) {
	for _, tc := range []struct {
		name, file, mediaType, title, wantDir string
	}{
		{"ebook", "book.epub", "ebook", "Some Book", filepath.Join("Real Author", "Some Book")},
		{"manga", "series.cbz", "manga", "Some Series v01 (2020)", "Some Series"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, kavitaEbooks, kavitaManga, incoming := libraryCopyServer(t)
			src := filepath.Join(incoming, tc.file)
			if err := os.WriteFile(src, libraryCopyEPUB(t, "Some Book", "Real Author"), 0o644); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(map[string]interface{}{
				"files": []map[string]string{{"path": src, "media_type": tc.mediaType, "title": tc.title}},
			})
			rr := httptest.NewRecorder()
			s.handleImportFiles(rr, httptest.NewRequest(http.MethodPost, "/api/import/manual", bytes.NewReader(payload)))
			if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"imported":1`) {
				t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
			}

			root := kavitaEbooks
			if tc.mediaType == "manga" {
				root = kavitaManga
			}
			want := filepath.Join(tc.wantDir, tc.file)
			if got := filesUnder(t, root); len(got) != 1 || got[0] != want {
				t.Fatalf("Kavita library holds %v, want [%s]", got, want)
			}
		})
	}
}
