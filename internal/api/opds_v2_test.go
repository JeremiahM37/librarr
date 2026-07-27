package api

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
)

type parsedOPDSFeed struct {
	XMLName xml.Name `xml:"feed"`
	Title   string   `xml:"title"`
	Links   []struct {
		Rel  string `xml:"rel,attr"`
		Href string `xml:"href,attr"`
		Type string `xml:"type,attr"`
	} `xml:"link"`
	Entries []parsedOPDSEntry `xml:"entry"`
}

type parsedOPDSEntry struct {
	Title   string `xml:"title"`
	ID      string `xml:"id"`
	Authors []struct {
		Name string `xml:"name"`
	} `xml:"author"`
	Links []struct {
		Rel   string `xml:"rel,attr"`
		Href  string `xml:"href,attr"`
		Type  string `xml:"type,attr"`
		Title string `xml:"title,attr"`
	} `xml:"link"`
}

type opdsTestServer struct {
	server    *Server
	bookID    int64
	epubID    int64
	pdfID     int64
	missingID int64
}

func TestOPDSBasicAuthentication(t *testing.T) {
	ts := newOPDSTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/opds", nil)
	rr := httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized || rr.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("unauth status=%d headers=%v body=%s", rr.Code, rr.Header(), rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/opds", nil)
	req.SetBasicAuth("brooke", "wrong")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("bad password status=%d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/opds", nil)
	req.SetBasicAuth("disabled", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("disabled user status=%d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/opds", nil)
	req.SetBasicAuth("brooke", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid auth status=%d body=%s", rr.Code, rr.Body.String())
	}
	var feed parsedOPDSFeed
	if err := xml.Unmarshal(rr.Body.Bytes(), &feed); err != nil {
		t.Fatalf("root XML: %v\n%s", err, rr.Body.String())
	}
	if feed.Title != "Librarr" || len(feed.Entries) < 4 {
		t.Fatalf("feed = %+v", feed)
	}
}

func TestOPDSBooksFeedIncludesCoversAndMultipleAcquisitions(t *testing.T) {
	ts := newOPDSTestServer(t)
	req := opdsRequest(http.MethodGet, "/opds/books", "brooke", "password")
	rr := httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var feed parsedOPDSFeed
	if err := xml.Unmarshal(rr.Body.Bytes(), &feed); err != nil {
		t.Fatalf("books XML: %v\n%s", err, rr.Body.String())
	}
	entry := findOPDSEntry(feed, "A & B <Book>")
	if entry == nil {
		t.Fatalf("book entry missing: %+v", feed.Entries)
	}
	var sawCover, sawEPUB, sawPDF bool
	for _, link := range entry.Links {
		if link.Rel == "http://opds-spec.org/image" && strings.Contains(link.Href, fmt.Sprintf("/opds/cover/%d", ts.bookID)) {
			sawCover = true
		}
		if link.Rel == "http://opds-spec.org/acquisition" && strings.Contains(link.Href, fmt.Sprintf("/opds/download/%d", ts.epubID)) && link.Type == "application/epub+zip" {
			sawEPUB = true
		}
		if link.Rel == "http://opds-spec.org/acquisition" && strings.Contains(link.Href, fmt.Sprintf("/opds/download/%d", ts.pdfID)) && link.Type == "application/pdf" {
			sawPDF = true
		}
	}
	if !sawCover || !sawEPUB || !sawPDF {
		t.Fatalf("links = %+v", entry.Links)
	}
}

func TestOPDSSearchAuthorsPaginationAndForwardedHost(t *testing.T) {
	ts := newOPDSTestServer(t)
	setTrustedProxies([]string{"192.0.2.10"})
	t.Cleanup(func() { setTrustedProxies(nil) })

	req := opdsRequest(http.MethodGet, "/opds/search?q=Casey", "brooke", "password")
	req.RemoteAddr = "192.0.2.10:12345"
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "books.example.test")
	rr := httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("search status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "https://books.example.test/opds/download/") {
		t.Fatalf("forwarded absolute URL missing:\n%s", rr.Body.String())
	}
	var feed parsedOPDSFeed
	if err := xml.Unmarshal(rr.Body.Bytes(), &feed); err != nil {
		t.Fatal(err)
	}
	if findOPDSEntry(feed, "A & B <Book>") == nil {
		t.Fatalf("search did not find author result: %+v", feed.Entries)
	}

	req = opdsRequest(http.MethodGet, "/opds/authors", "brooke", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "/opds/authors/casey-writer") {
		t.Fatalf("authors status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = opdsRequest(http.MethodGet, "/opds/books?page=1", "brooke", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `rel="next"`) {
		t.Fatalf("pagination body=%s", rr.Body.String())
	}
}

func TestOPDSDownloadsAndCovers(t *testing.T) {
	ts := newOPDSTestServer(t)
	req := opdsRequest(http.MethodGet, fmt.Sprintf("/opds/download/%d", ts.epubID), "brooke", "password")
	rr := httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("epub status=%d body=%s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/epub+zip") {
		t.Fatalf("epub content-type=%q", ct)
	}
	if !strings.Contains(rr.Header().Get("Content-Disposition"), "casey.epub") {
		t.Fatalf("content-disposition=%q", rr.Header().Get("Content-Disposition"))
	}

	req = opdsRequest(http.MethodGet, fmt.Sprintf("/opds/download/%d", ts.pdfID), "brooke", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.HasPrefix(rr.Header().Get("Content-Type"), "application/pdf") {
		t.Fatalf("pdf status=%d ct=%q", rr.Code, rr.Header().Get("Content-Type"))
	}

	req = opdsRequest(http.MethodGet, fmt.Sprintf("/opds/download/%d", ts.missingID), "brooke", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d", rr.Code)
	}

	req = opdsRequest(http.MethodGet, fmt.Sprintf("/opds/cover/%d", ts.bookID), "brooke", "password")
	rr = httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.HasPrefix(rr.Header().Get("Content-Type"), "image/png") {
		t.Fatalf("cover status=%d ct=%q", rr.Code, rr.Header().Get("Content-Type"))
	}
}

func TestOPDSDownloadRejectsPathOutsideLibraryRoots(t *testing.T) {
	ts := newOPDSTestServer(t)
	outside := filepath.Join(t.TempDir(), "outside.epub")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	book, edition := createOPDSBook(t, ts.server.libraryService, "Outside", "Writer")
	file, err := ts.server.libraryService.AttachFile(context.Background(), library.BookFile{
		EditionID: edition.ID,
		MediaType: library.MediaTypeEbook,
		Format:    "epub",
		Path:      outside,
	})
	if err != nil {
		t.Fatal(err)
	}
	if book.ID == 0 {
		t.Fatal("book not created")
	}

	req := opdsRequest(http.MethodGet, fmt.Sprintf("/opds/download/%d", file.ID), "brooke", "password")
	rr := httptest.NewRecorder()
	ts.server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("outside path status=%d", rr.Code)
	}
}

func newOPDSTestServer(t *testing.T) opdsTestServer {
	t.Helper()
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "opds.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	seedCompletedEmptyBackfill(t, database)
	cfg := &config.Config{
		LibraryRepositoryMode: "normalized",
		DBPath:                filepath.Join(dir, "opds.db"),
		EbookDir:              filepath.Join(dir, "ebooks"),
		AudiobookDir:          filepath.Join(dir, "audiobooks"),
		MangaDir:              filepath.Join(dir, "manga"),
		IncomingDir:           filepath.Join(dir, "incoming"),
	}
	for _, root := range []string{cfg.EbookDir, cfg.AudiobookDir, cfg.MangaDir, cfg.IncomingDir} {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, database)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := hashPassword("password")
	if _, err := database.CreateUser("brooke", pass, "user"); err != nil {
		t.Fatal(err)
	}
	disabledID, err := database.CreateUser("disabled", pass, "user")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetUserEnabled(disabledID, false); err != nil {
		t.Fatal(err)
	}
	server := &Server{
		cfg:            cfg,
		db:             database,
		libraryService: selection.LibraryService,
		mux:            http.NewServeMux(),
		sessions:       NewSessionStore(),
	}
	server.registerFeedRoutes()

	book, edition := createOPDSBook(t, selection.LibraryService, "A & B <Book>", "Casey Writer")
	epubPath := filepath.Join(cfg.EbookDir, "casey.epub")
	pdfPath := filepath.Join(cfg.EbookDir, "casey.pdf")
	missingPath := filepath.Join(cfg.EbookDir, "missing.epub")
	if err := os.WriteFile(epubPath, []byte("epub bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pdfPath, []byte("%PDF-1.7"), 0600); err != nil {
		t.Fatal(err)
	}
	epub, err := selection.LibraryService.AttachFile(context.Background(), library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: epubPath, Size: 10, Managed: true})
	if err != nil {
		t.Fatal(err)
	}
	pdf, err := selection.LibraryService.AttachFile(context.Background(), library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "pdf", Path: pdfPath, Size: 8, Managed: true})
	if err != nil {
		t.Fatal(err)
	}
	missing, err := selection.LibraryService.AttachFile(context.Background(), library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: missingPath, Size: 1, Managed: true})
	if err != nil {
		t.Fatal(err)
	}
	coverPath := filepath.Join(dir, "cover.png")
	if err := os.WriteFile(coverPath, []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachCover(context.Background(), library.Cover{BookID: book.ID, LocalPath: coverPath, MimeType: "image/png", Source: "test", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < opdsPageSize; i++ {
		createOPDSBook(t, selection.LibraryService, fmt.Sprintf("Paged %02d", i), "Other Writer")
	}

	return opdsTestServer{server: server, bookID: book.ID, epubID: epub.ID, pdfID: pdf.ID, missingID: missing.ID}
}

func createOPDSBook(t *testing.T, svc *library.LibraryService, title, author string) (*library.Book, *library.Edition) {
	t.Helper()
	book, err := svc.CreateBook(context.Background(), library.Book{Title: title, SortTitle: library.NormalizeKey(title), MediaType: library.MediaTypeEbook, Status: library.BookStatusOwned})
	if err != nil {
		t.Fatal(err)
	}
	edition, err := svc.CreateEdition(context.Background(), library.Edition{BookID: book.ID, Title: title})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachContributor(context.Background(), edition.ID, library.Contributor{Name: author, Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	return book, edition
}

func opdsRequest(method, target, username, password string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	req.SetBasicAuth(username, password)
	return req
}

func findOPDSEntry(feed parsedOPDSFeed, title string) *parsedOPDSEntry {
	for i := range feed.Entries {
		if feed.Entries[i].Title == title {
			return &feed.Entries[i]
		}
	}
	return nil
}
