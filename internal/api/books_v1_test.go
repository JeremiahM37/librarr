package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

func TestV1BooksListSupportsPaginationSortSearchAndFormat(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/books?media_type=ebook&sort=author&order=asc&limit=1&offset=1", nil)
	rr := httptest.NewRecorder()
	s.handleV1Books(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var body struct {
		Items      []v1BookSummary `json:"items"`
		Pagination struct {
			Limit  int `json:"limit"`
			Offset int `json:"offset"`
			Total  int `json:"total"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Pagination.Total != 2 || body.Pagination.Limit != 1 || body.Pagination.Offset != 1 {
		t.Fatalf("pagination = %+v", body.Pagination)
	}
	if len(body.Items) != 1 || body.Items[0].ID != ids.darkMatterID {
		t.Fatalf("items = %+v", body.Items)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/books?media_type=ebook&search=andy&format=mobi", nil)
	rr = httptest.NewRecorder()
	s.handleV1Books(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Items[0].ID != ids.projectHailMaryID {
		t.Fatalf("filtered items = %+v", body.Items)
	}
	if body.Items[0].PrimaryAuthor == nil || body.Items[0].PrimaryAuthor.Name != "Andy Weir" {
		t.Fatalf("primary author = %+v", body.Items[0].PrimaryAuthor)
	}
}

func TestV1BooksListGroupsMultipleFormatsUnderOneBook(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/books?media_type=ebook&search=project", nil)
	rr := httptest.NewRecorder()
	s.handleV1Books(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var body v1BookListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Items[0].ID != ids.projectHailMaryID {
		t.Fatalf("items = %+v", body.Items)
	}
	if body.Items[0].FileCount != 2 {
		t.Fatalf("file_count = %d", body.Items[0].FileCount)
	}
	if got, want := strings.Join(body.Items[0].Formats, ","), "epub,mobi"; got != want {
		t.Fatalf("formats = %q, want %q", got, want)
	}
}

func TestV1BookMergeMatchingMergesTitleAndAuthorPunctuationVariants(t *testing.T) {
	s, _, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()
	ctx := context.Background()

	targetBook, targetEdition := createAPIScanBook(t, s.libraryService, "Men in Black: How the Supreme Court is Destroying America", "Mark R. Levin")
	sourceBook, sourceEdition := createAPIScanBook(t, s.libraryService, "Men in Black- How the Supreme Court is Destroying America", "Mark R Levin")
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:    targetEdition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "epub",
		Path:         "/books/mark-r-levin/men-in-black.epub",
		OriginalPath: "/incoming/men-in-black.epub",
		ContentHash:  "hash-men-in-black-epub",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:    sourceEdition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "mobi",
		Path:         "/incoming/Mark R Levin - Men in Black- How the Supreme Court is Destroying America.mobi",
		OriginalPath: "/incoming/Mark R Levin - Men in Black- How the Supreme Court is Destroying America.mobi",
		ContentHash:  "hash-men-in-black-mobi",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/books/%d/merge-matching", sourceBook.ID), nil)
	req.SetPathValue("id", fmt.Sprint(sourceBook.ID))
	rr := httptest.NewRecorder()
	s.handleV1BookMergeMatching(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var response v1BookMergeMatchingResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Success || response.TargetBookID != targetBook.ID || response.MergedCount != 1 {
		t.Fatalf("response = %+v", response)
	}
	if _, err := s.libraryService.GetBook(ctx, sourceBook.ID); !errors.Is(err, library.ErrBookNotFound) {
		t.Fatalf("source book error = %v", err)
	}
	files, err := s.libraryService.GetBookFiles(ctx, targetBook.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
}

func TestV1NestedEbookPathRepairDryRunAndExecution(t *testing.T) {
	s, cleanup := newNestedEbookRepairServer(t)
	defer cleanup()
	ctx := context.Background()
	legacyRoot := filepath.Join(s.cfg.EbookDir, filepath.Base(s.cfg.EbookDir))
	sourcePath := filepath.Join(legacyRoot, "Mark R. Levin", "Men in Black.epub")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0700); err != nil {
		t.Fatal(err)
	}
	content := []byte("ebook bytes")
	if err := os.WriteFile(sourcePath, content, 0600); err != nil {
		t.Fatal(err)
	}
	book, edition := createAPIScanBook(t, s.libraryService, "Men in Black", "Mark R. Levin")
	file, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:    edition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "epub",
		Path:         sourcePath,
		OriginalPath: sourcePath,
		Size:         int64(len(content)),
		ContentHash:  sha256ForTest(t, content),
		SourceType:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	correctPath := filepath.Join(s.cfg.EbookDir, "Mark R. Levin", "Men in Black.epub")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr := httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairPreview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview status = %d body=%s", rr.Code, rr.Body.String())
	}
	var preview nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.TotalAffectedFiles != 1 || preview.Summary[nestedEbookRepairReady] != 1 {
		t.Fatalf("preview = %+v", preview)
	}
	if preview.Entries[0].DestinationPath != correctPath {
		t.Fatalf("destination = %q, want %q", preview.Entries[0].DestinationPath, correctPath)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr = httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairRun(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("run status = %d body=%s", rr.Code, rr.Body.String())
	}
	var result nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Executed || result.Summary[nestedEbookRepairMoved] != 1 || !result.LegacyRootRemoved {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(sourcePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source stat err = %v, want missing", err)
	}
	if got, err := os.ReadFile(correctPath); err != nil || string(got) != string(content) {
		t.Fatalf("destination content = %q err=%v", got, err)
	}
	updated, err := s.libraryService.GetFile(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Path != correctPath || updated.BookID != book.ID {
		t.Fatalf("updated file = %+v", updated)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/files", book.ID), nil)
	req.SetPathValue("id", fmt.Sprint(book.ID))
	rr = httptest.NewRecorder()
	s.handleV1BookFiles(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), correctPath) {
		t.Fatalf("files status = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/opds/download/%d", file.ID), nil)
	req.SetPathValue("id", fmt.Sprint(file.ID))
	rr = httptest.NewRecorder()
	s.handleOPDSDownload(rr, req)
	if rr.Code != http.StatusOK || rr.Body.String() != string(content) {
		t.Fatalf("opds status = %d body=%q", rr.Code, rr.Body.String())
	}
}

func TestV1NestedEbookPathRepairReportsCollisionMissingAndSymlinkEscape(t *testing.T) {
	s, cleanup := newNestedEbookRepairServer(t)
	defer cleanup()
	ctx := context.Background()
	legacyRoot := filepath.Join(s.cfg.EbookDir, filepath.Base(s.cfg.EbookDir))
	_, edition := createAPIScanBook(t, s.libraryService, "Repair Cases", "Casey")

	collisionSource := filepath.Join(legacyRoot, "Casey", "Collision.epub")
	collisionDest := filepath.Join(s.cfg.EbookDir, "Casey", "Collision.epub")
	writeTestFile(t, collisionSource, []byte("source"))
	writeTestFile(t, collisionDest, []byte("different"))
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: collisionSource, ContentHash: sha256ForTest(t, []byte("source"))}); err != nil {
		t.Fatal(err)
	}

	missingSource := filepath.Join(legacyRoot, "Casey", "Missing.epub")
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: missingSource}); err != nil {
		t.Fatal(err)
	}

	outside := filepath.Join(t.TempDir(), "outside.epub")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	symlinkSource := filepath.Join(legacyRoot, "Casey", "Escape.epub")
	if err := os.MkdirAll(filepath.Dir(symlinkSource), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, symlinkSource); err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: symlinkSource}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr := httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairPreview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview status = %d body=%s", rr.Code, rr.Body.String())
	}
	var preview nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.Summary[nestedEbookRepairCollision] != 1 || preview.Summary[nestedEbookRepairMissing] != 1 || preview.Summary[nestedEbookRepairUnsafe] != 1 {
		t.Fatalf("summary = %+v entries=%+v", preview.Summary, preview.Entries)
	}
}

func TestV1NestedEbookPathRepairPreservesUnknownNonEmptyDirectoriesAndRequiresAdmin(t *testing.T) {
	s, cleanup := newNestedEbookRepairServer(t)
	defer cleanup()
	ctx := context.Background()
	legacyRoot := filepath.Join(s.cfg.EbookDir, filepath.Base(s.cfg.EbookDir))
	sourcePath := filepath.Join(legacyRoot, "Known", "Known.epub")
	unknownPath := filepath.Join(legacyRoot, "Unknown", "orphan.epub")
	writeTestFile(t, sourcePath, []byte("known"))
	writeTestFile(t, unknownPath, []byte("orphan"))
	_, edition := createAPIScanBook(t, s.libraryService, "Known", "Known")
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: sourcePath, ContentHash: sha256ForTest(t, []byte("known"))}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr := httptest.NewRecorder()
	requireAdmin(s.handleV1NestedEbookPathRepairRun)(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status without admin = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/library/repairs/nested-ebook-paths", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	rr = httptest.NewRecorder()
	requireAdmin(s.handleV1NestedEbookPathRepairRun)(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status with admin = %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := os.Stat(unknownPath); err != nil {
		t.Fatalf("unknown orphan should be preserved: %v", err)
	}
	if _, err := os.Stat(legacyRoot); err != nil {
		t.Fatalf("legacy root should remain because unknown file exists: %v", err)
	}
}

func TestV1NestedEbookPathRepairReconcilesDiskOnlyAndZeroReady(t *testing.T) {
	s, cleanup := newNestedEbookRepairServer(t)
	defer cleanup()
	legacyRoot := filepath.Join(s.cfg.EbookDir, filepath.Base(s.cfg.EbookDir))
	orphan := filepath.Join(legacyRoot, "Orphans", "Nested Only.epub")
	writeTestFile(t, orphan, []byte("orphan"))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr := httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairPreview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview status = %d body=%s", rr.Code, rr.Body.String())
	}
	var preview nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.FilesFoundOnDisk != 1 || preview.Summary[nestedEbookRepairReady] != 0 || preview.Reconciliation[nestedEbookClassUncataloged] != 1 {
		t.Fatalf("preview summary=%+v reconciliation=%+v files=%d entries=%+v", preview.Summary, preview.Reconciliation, preview.FilesFoundOnDisk, preview.Entries)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr = httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairRun(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("run status = %d body=%s", rr.Code, rr.Body.String())
	}
	var result nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Executed || result.Summary[nestedEbookRepairMoved] != 0 || result.Reconciliation[nestedEbookClassUncataloged] != 1 {
		t.Fatalf("result summary=%+v reconciliation=%+v", result.Summary, result.Reconciliation)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("orphan should not move on zero-ready repair: %v", err)
	}
}

func TestV1NestedEbookPathRepairClassifiesLegacyOnlyAndUnmanagedIncoming(t *testing.T) {
	s, cleanup := newNestedEbookRepairServer(t)
	defer cleanup()
	ctx := context.Background()
	legacyRoot := filepath.Join(s.cfg.EbookDir, filepath.Base(s.cfg.EbookDir))

	legacyOnly := filepath.Join(legacyRoot, "Legacy", "Legacy Only.epub")
	legacyContent := []byte("legacy-only")
	writeTestFile(t, legacyOnly, legacyContent)
	if _, err := s.db.SQLDB().Exec(`INSERT INTO library_items (title, author, file_path, original_path, file_size, file_format, media_type, source, source_id, metadata, content_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"Legacy Only", "Writer", legacyOnly, legacyOnly, len(legacyContent), "epub", "ebook", "test", "legacy-only", "{}", sha256ForTest(t, legacyContent)); err != nil {
		t.Fatal(err)
	}

	nestedIncomingCopy := filepath.Join(legacyRoot, "Incoming", "Incoming Backed.epub")
	incomingPath := filepath.Join(s.cfg.IncomingDir, "Incoming Backed.epub")
	incomingContent := []byte("incoming-backed")
	writeTestFile(t, nestedIncomingCopy, incomingContent)
	writeTestFile(t, incomingPath, incomingContent)
	_, edition := createAPIScanBook(t, s.libraryService, "Incoming Backed", "Writer")
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:    edition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "epub",
		Path:         incomingPath,
		OriginalPath: incomingPath,
		ContentHash:  sha256ForTest(t, incomingContent),
		Managed:      false,
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr := httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairPreview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview status = %d body=%s", rr.Code, rr.Body.String())
	}
	var preview nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.Reconciliation[nestedEbookClassCatalogedLegacyOnly] != 1 || preview.Reconciliation[nestedEbookClassCatalogedUnmanaged] != 1 || preview.Summary[nestedEbookRepairReady] != 0 {
		t.Fatalf("summary=%+v reconciliation=%+v entries=%+v", preview.Summary, preview.Reconciliation, preview.Entries)
	}
}

func TestV1NestedEbookPathRepairClassifiesDuplicatePhysicalCopy(t *testing.T) {
	s, cleanup := newNestedEbookRepairServer(t)
	defer cleanup()
	legacyRoot := filepath.Join(s.cfg.EbookDir, filepath.Base(s.cfg.EbookDir))
	nestedPath := filepath.Join(legacyRoot, "Mark Twain", "Biography.epub")
	correctPath := filepath.Join(s.cfg.EbookDir, "Mark Twain", "Biography.epub")
	content := []byte("same-book")
	writeTestFile(t, nestedPath, content)
	writeTestFile(t, correctPath, content)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/repairs/nested-ebook-paths", nil)
	rr := httptest.NewRecorder()
	s.handleV1NestedEbookPathRepairPreview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview status = %d body=%s", rr.Code, rr.Body.String())
	}
	var preview nestedEbookRepairResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.FilesFoundOnDisk != 1 || preview.CorrectRootFiles != 1 || preview.Reconciliation[nestedEbookClassDuplicatePhysical] != 1 {
		t.Fatalf("preview = %+v", preview)
	}
}

func TestV1BookDetailFilesAndEditions(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr := httptest.NewRecorder()
	s.handleV1Book(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var detail v1BookDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Title != "Project Hail Mary" {
		t.Fatalf("detail = %+v", detail)
	}
	if detail.Series.Name == nil || *detail.Series.Name != "The Ryland Grace Files" {
		t.Fatalf("series = %+v", detail.Series)
	}
	if detail.Cover.URL == nil || *detail.Cover.URL != fmt.Sprintf("/api/v1/books/%d/cover", ids.projectHailMaryID) {
		t.Fatalf("cover = %+v", detail.Cover)
	}
	if len(detail.Editions) != 1 || detail.Editions[0].FileCount != 2 {
		t.Fatalf("editions = %+v", detail.Editions)
	}
	if len(detail.Formats) != 2 || detail.FileCount != 2 {
		t.Fatalf("formats=%v file_count=%d", detail.Formats, detail.FileCount)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/files", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr = httptest.NewRecorder()
	s.handleV1BookFiles(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("files status = %d body=%s", rr.Code, rr.Body.String())
	}
	var files struct {
		Items []v1FileSummary `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &files); err != nil {
		t.Fatal(err)
	}
	if len(files.Items) != 2 {
		t.Fatalf("files = %+v", files.Items)
	}
	if files.Items[0].ContentHash == "" {
		t.Fatalf("expected content hash to be exposed consistently with existing project behavior")
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/editions", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr = httptest.NewRecorder()
	s.handleV1BookEditions(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("editions status = %d body=%s", rr.Code, rr.Body.String())
	}
	var editions struct {
		Items []v1EditionSummary `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &editions); err != nil {
		t.Fatal(err)
	}
	if len(editions.Items) != 1 || editions.Items[0].Title != "Project Hail Mary" {
		t.Fatalf("edition items = %+v", editions.Items)
	}
}

func TestV1BooksRejectInvalidParamsAndMissingBooks(t *testing.T) {
	s, _, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	for _, path := range []string{
		"/api/v1/books?media_type=invalid",
		"/api/v1/books?sort=bad",
		"/api/v1/books?order=sideways",
		"/api/v1/books/not-a-number",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		switch {
		case path == "/api/v1/books/not-a-number":
			req.SetPathValue("id", "not-a-number")
			s.handleV1Book(rr, req)
		default:
			s.handleV1Books(rr, req)
		}
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d body=%s", path, rr.Code, rr.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/books/9999", nil)
	req.SetPathValue("id", "9999")
	rr := httptest.NewRecorder()
	s.handleV1Book(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestV1BooksRejectLegacyModeAndCompatibilityEndpointRemainsAvailable(t *testing.T) {
	d, err := db.New(filepath.Join(t.TempDir(), "legacy-books.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if _, err := d.SQLDB().Exec(`INSERT INTO library_items (title, author, file_path, file_format, media_type, source_id)
		VALUES ('Legacy Book', 'Legacy Author', '/books/legacy.epub', 'epub', 'ebook', 'legacy-1')`); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{LibraryRepositoryMode: "legacy"}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: cfg, db: d, libraryService: selection.LibraryService}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/books", nil)
	rr := httptest.NewRecorder()
	s.handleV1Books(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var errorBody map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &errorBody); err != nil {
		t.Fatal(err)
	}
	if errorBody["error"] != normalizedReadAPIError {
		t.Fatalf("error body = %+v", errorBody)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/library", nil)
	rr = httptest.NewRecorder()
	s.handleLibrary(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("compat status = %d body=%s", rr.Code, rr.Body.String())
	}
	var compat struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &compat); err != nil {
		t.Fatal(err)
	}
	if compat.Total != 1 || len(compat.Items) != 1 || compat.Items[0]["title"] != "Legacy Book" {
		t.Fatalf("compat = %+v", compat)
	}
}

func TestV1BooksSupportAudiobooksAndMangaAndSummary(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/books?media_type=audiobook", nil)
	rr := httptest.NewRecorder()
	s.handleV1Books(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("audiobook status = %d body=%s", rr.Code, rr.Body.String())
	}
	var listBody struct {
		Items []v1BookSummary `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	if len(listBody.Items) != 1 || listBody.Items[0].ID != ids.audiobookID || listBody.Items[0].MediaType != "audiobook" {
		t.Fatalf("audiobook items = %+v", listBody.Items)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/books?media_type=manga", nil)
	rr = httptest.NewRecorder()
	s.handleV1Books(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("manga status = %d body=%s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	if len(listBody.Items) != 1 || listBody.Items[0].ID != ids.mangaID || listBody.Items[0].MediaType != "manga" {
		t.Fatalf("manga items = %+v", listBody.Items)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/library/summary", nil)
	rr = httptest.NewRecorder()
	s.handleV1LibrarySummary(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary status = %d body=%s", rr.Code, rr.Body.String())
	}
	var summary map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary["total_books"].(float64) != 4 || summary["authors"].(float64) != 2 || summary["ebooks"].(float64) != 2 || summary["audiobooks"].(float64) != 1 || summary["manga"].(float64) != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	formats := summary["format_distribution"].(map[string]any)
	if formats["epub"].(float64) < 2 || formats["mobi"].(float64) < 1 || formats["mp3"].(float64) < 1 || formats["cbz"].(float64) < 1 {
		t.Fatalf("format distribution = %+v", formats)
	}
}

func TestV1BookCoverSuccessAndNotFound(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/cover", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr := httptest.NewRecorder()
	s.handleV1BookCover(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("cover status = %d body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); got == "" || !strings.HasPrefix(got, "image/") {
		t.Fatalf("content-type = %q", got)
	}
	if rr.Body.Len() == 0 {
		t.Fatal("expected image body")
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/cover", ids.darkMatterID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.darkMatterID))
	rr = httptest.NewRecorder()
	s.handleV1BookCover(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("cover missing status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestV1BookMetadataAndProvenanceHandlers(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/metadata", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr := httptest.NewRecorder()
	s.handleV1BookMetadata(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var body struct {
		BookID int64                             `json:"book_id"`
		Fields map[string]v1MetadataFieldSummary `json:"fields"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.BookID != ids.projectHailMaryID || body.Fields["title"].Value != "Project Hail Mary" {
		t.Fatalf("metadata body = %+v", body)
	}

	req = httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/v1/books/%d/metadata", ids.projectHailMaryID), strings.NewReader(`{"fields":{"title":"Project Hail Mary (Edited)","edition_title":"Signed Edition","subtitle":"A Novel","description":"Edited description","publisher":"Ballantine","publication_date":"2022","language":"fr","genres":["Science Fiction","Space Opera"]}}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr = httptest.NewRecorder()
	s.handleV1BookMetadataPatch(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("patch status = %d body=%s", rr.Code, rr.Body.String())
	}
	var patchBody struct {
		Metadata struct {
			Fields map[string]v1MetadataFieldSummary `json:"fields"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &patchBody); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]string{
		"title":            "Project Hail Mary (Edited)",
		"edition_title":    "Signed Edition",
		"subtitle":         "A Novel",
		"description":      "Edited description",
		"publisher":        "Ballantine",
		"publication_date": "2022",
		"language":         "fr",
		"genres":           "Science Fiction, Space Opera",
	} {
		got := patchBody.Metadata.Fields[field]
		if got.Value != want || !got.ManualOverride {
			t.Fatalf("field %s = %+v, want %q manual override", field, got, want)
		}
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d/provenance", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr = httptest.NewRecorder()
	s.handleV1BookProvenance(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("provenance status = %d body=%s", rr.Code, rr.Body.String())
	}

	var provenance struct {
		Fields map[string][]v1MetadataEvidenceSummary `json:"fields"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &provenance); err != nil {
		t.Fatal(err)
	}
	if len(provenance.Fields["title"]) == 0 || !provenance.Fields["title"][0].Selected {
		t.Fatalf("provenance = %+v", provenance)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/books/%d", ids.projectHailMaryID), nil)
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr = httptest.NewRecorder()
	s.handleV1Book(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%s", rr.Code, rr.Body.String())
	}
	var detail v1BookDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Title != "Project Hail Mary (Edited)" {
		t.Fatalf("detail title after patch = %+v", detail)
	}
}

func TestV1BookMetadataRejectsInvalidPatchBody(t *testing.T) {
	s, ids, cleanup := newNormalizedBooksAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/v1/books/%d/metadata", ids.projectHailMaryID), strings.NewReader(`{"fields":{"unknown":"value"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", fmt.Sprint(ids.projectHailMaryID))
	rr := httptest.NewRecorder()
	s.handleV1BookMetadataPatch(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestV1BookDeleteRemoveOnlyLeavesFiles(t *testing.T) {
	s, bookID, paths, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected file to remain at %s: %v", path, err)
		}
	}
	if _, err := s.libraryService.GetBook(context.Background(), bookID); !errors.Is(err, library.ErrBookNotFound) {
		t.Fatalf("GetBook after remove = %v, want not found", err)
	}
}

func TestV1BookDeleteRequiresAdminMiddleware(t *testing.T) {
	s, bookID, _, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d?delete_files=true", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	requireAdmin(s.handleV1BookDelete)(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status without admin = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d", bookID), nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr = httptest.NewRecorder()
	requireAdmin(s.handleV1BookDelete)(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status with admin = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestV1BookDeleteWithFilesDeletesManagedFiles(t *testing.T) {
	s, bookID, paths, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d?delete_files=true", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	var body v1BookDeleteResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Success || body.DeletedFiles != 2 || body.FailedFiles != 0 {
		t.Fatalf("body = %+v", body)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("file %s still exists or stat error=%v", path, err)
		}
	}
	if _, err := s.libraryService.GetBook(context.Background(), bookID); !errors.Is(err, library.ErrBookNotFound) {
		t.Fatalf("GetBook after delete = %v, want not found", err)
	}
}

func TestV1BookDeleteRejectsPathsOutsideLibraryRoots(t *testing.T) {
	s, bookID, _, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	outside := filepath.Join(t.TempDir(), "outside.epub")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	edition, err := s.libraryService.FindEdition(context.Background(), bookID, "Delete Me")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachFile(context.Background(), library.BookFile{
		EditionID:   edition.ID,
		MediaType:   library.MediaTypeEbook,
		Format:      "pdf",
		Path:        outside,
		ContentHash: "hash-outside",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d?delete_files=true", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := s.libraryService.GetBook(context.Background(), bookID); err != nil {
		t.Fatalf("book should remain after rejected delete: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("outside file should remain: %v", err)
	}
}

func TestV1BookDeleteRejectsSymlinkEscape(t *testing.T) {
	s, bookID, _, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	outside := filepath.Join(t.TempDir(), "outside.epub")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(s.cfg.EbookDir, "escape.epub")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	edition, err := s.libraryService.FindEdition(context.Background(), bookID, "Delete Me")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachFile(context.Background(), library.BookFile{
		EditionID:   edition.ID,
		MediaType:   library.MediaTypeEbook,
		Format:      "azw3",
		Path:        link,
		ContentHash: "hash-link",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d?delete_files=true", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := s.libraryService.GetBook(context.Background(), bookID); err != nil {
		t.Fatalf("book should remain after rejected delete: %v", err)
	}
}

func TestV1BookDeleteHandlesMissingFilesAsKnownOutcome(t *testing.T) {
	s, bookID, paths, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()
	if err := os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d?delete_files=true", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body v1BookDeleteResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.DeletedFiles != 1 || body.MissingFiles != 1 || body.FailedFiles != 0 {
		t.Fatalf("body = %+v", body)
	}
}

func TestV1BookDeletePartialFilesystemFailureKeepsCatalog(t *testing.T) {
	s, bookID, _, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	blockedDir := filepath.Join(s.cfg.EbookDir, "blocked.epub")
	if err := os.MkdirAll(filepath.Join(blockedDir, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	edition, err := s.libraryService.FindEdition(context.Background(), bookID, "Delete Me")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachFile(context.Background(), library.BookFile{
		EditionID:   edition.ID,
		MediaType:   library.MediaTypeEbook,
		Format:      "pdf",
		Path:        blockedDir,
		ContentHash: "hash-blocked",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d?delete_files=true", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body v1BookDeleteResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.FailedFiles != 1 || len(body.Files) == 0 || body.Files[len(body.Files)-1].Error == "" {
		t.Fatalf("body = %+v", body)
	}
	if strings.Contains(body.Files[len(body.Files)-1].Error, blockedDir) {
		t.Fatalf("file error leaked path: %+v", body.Files[len(body.Files)-1])
	}
	if _, err := s.libraryService.GetBook(context.Background(), bookID); err != nil {
		t.Fatalf("book should remain after partial failure: %v", err)
	}
}

func TestV1BookDeleteDoesNotRemoveSharedCoverCacheFile(t *testing.T) {
	s, bookID, _, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	coverPath := filepath.Join(s.coverCache.Dir(), "books", "shared-cover.png")
	if err := os.MkdirAll(filepath.Dir(coverPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(coverPath, []byte{0x89, 0x50, 0x4e, 0x47, 1, 2, 3}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachCover(context.Background(), library.Cover{BookID: bookID, LocalPath: coverPath, MimeType: "image/png", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}
	other, err := s.libraryService.CreateBook(context.Background(), library.Book{Title: "Other", MediaType: library.MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachCover(context.Background(), library.Cover{BookID: other.ID, LocalPath: coverPath, MimeType: "image/png", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := os.Stat(coverPath); err != nil {
		t.Fatalf("shared cover cache file should remain: %v", err)
	}
}

func TestV1BookDeleteRemovesUnreferencedCoverCacheFile(t *testing.T) {
	s, bookID, _, cleanup := newNormalizedDeleteBookAPIServer(t)
	defer cleanup()

	coverPath := filepath.Join(s.coverCache.Dir(), "books", "delete-cover.png")
	if err := os.MkdirAll(filepath.Dir(coverPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(coverPath, []byte{0x89, 0x50, 0x4e, 0x47, 1, 2, 3}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachCover(context.Background(), library.Cover{BookID: bookID, LocalPath: coverPath, MimeType: "image/png", IsPrimary: true}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/books/%d", bookID), nil)
	req.SetPathValue("id", fmt.Sprint(bookID))
	rr := httptest.NewRecorder()
	s.handleV1BookDelete(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := os.Stat(coverPath); !os.IsNotExist(err) {
		t.Fatalf("cover cache file should be removed, stat err=%v", err)
	}
}

type normalizedBookAPIIDs struct {
	projectHailMaryID int64
	darkMatterID      int64
	audiobookID       int64
	mangaID           int64
}

func newNormalizedBooksAPIServer(t *testing.T) (*Server, normalizedBookAPIIDs, func()) {
	t.Helper()
	d, err := db.New(filepath.Join(t.TempDir(), "books-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	seedCompletedEmptyBackfill(t, d)

	cfg := &config.Config{LibraryRepositoryMode: "normalized"}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	repo, ok := selection.Repository.(*library.NormalizedRepository)
	if !ok {
		t.Fatalf("repository type = %T", selection.Repository)
	}

	ctx := context.Background()
	firstBook, err := selection.LibraryService.CreateBook(ctx, library.Book{
		Title:     "Project Hail Mary",
		SortTitle: "Project Hail Mary",
		MediaType: library.MediaTypeEbook,
		Status:    library.BookStatusOwned,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstEdition, err := selection.LibraryService.CreateEdition(ctx, library.Edition{
		BookID: firstBook.ID,
		Title:  "Project Hail Mary",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := selection.LibraryService.AttachContributor(ctx, firstEdition.ID, library.Contributor{Name: "Andy Weir", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AddIdentifier(ctx, library.Identifier{
		Scope:    library.IdentifierScopeBook,
		Provider: "isbn13",
		Value:    "9780593135204",
		Source:   fmt.Sprint(firstBook.ID),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachBook(ctx, firstBook.ID, library.BookSeries{
		Series:          library.Series{Title: "The Ryland Grace Files"},
		Position:        1,
		DisplayPosition: "1",
	}); err != nil {
		t.Fatal(err)
	}
	coverPath := filepath.Join(t.TempDir(), "project-hail-mary-cover.png")
	if err := os.WriteFile(coverPath, []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AttachCover(ctx, library.Cover{
		BookID:    firstBook.ID,
		Source:    "test",
		LocalPath: coverPath,
		MimeType:  "image/png",
		IsPrimary: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:    firstEdition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "epub",
		Path:         "/books/project-hail-mary.epub",
		OriginalPath: "/incoming/project-hail-mary.epub",
		Size:         100,
		ContentHash:  "hash-epub",
		SourceID:     "source-epub",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:    firstEdition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "mobi",
		Path:         "/books/project-hail-mary.mobi",
		OriginalPath: "/incoming/project-hail-mary.mobi",
		Size:         120,
		ContentHash:  "hash-mobi",
		SourceID:     "source-mobi",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}

	secondBook, err := selection.LibraryService.CreateBook(ctx, library.Book{
		Title:     "Dark Matter",
		SortTitle: "Dark Matter",
		MediaType: library.MediaTypeEbook,
		Status:    library.BookStatusOwned,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondEdition, err := selection.LibraryService.CreateEdition(ctx, library.Edition{
		BookID: secondBook.ID,
		Title:  "Dark Matter",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := selection.LibraryService.AttachContributor(ctx, secondEdition.ID, library.Contributor{Name: "Blake Crouch", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:    secondEdition.ID,
		MediaType:    library.MediaTypeEbook,
		Format:       "epub",
		Path:         "/books/dark-matter.epub",
		OriginalPath: "/incoming/dark-matter.epub",
		Size:         90,
		ContentHash:  "hash-dark-matter",
		SourceID:     "source-dark-matter",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}

	audiobookBook, err := selection.LibraryService.CreateBook(ctx, library.Book{
		Title:     "Project Hail Mary Audio",
		SortTitle: "Project Hail Mary Audio",
		MediaType: library.MediaTypeAudiobook,
		Status:    library.BookStatusOwned,
	})
	if err != nil {
		t.Fatal(err)
	}
	audiobookEdition, err := selection.LibraryService.CreateEdition(ctx, library.Edition{
		BookID: audiobookBook.ID,
		Title:  "Project Hail Mary Audio",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := selection.LibraryService.AttachContributor(ctx, audiobookEdition.ID, library.Contributor{Name: "Ray Porter", Roles: []library.ContributorRole{library.RoleNarrator}}); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:    audiobookEdition.ID,
		MediaType:    library.MediaTypeAudiobook,
		Format:       "mp3",
		Path:         "/books/project-hail-mary-audio.mp3",
		OriginalPath: "/incoming/project-hail-mary-audio.mp3",
		Size:         1000,
		ContentHash:  "hash-audio",
		SourceID:     "source-audio",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}

	mangaBook, err := selection.LibraryService.CreateBook(ctx, library.Book{
		Title:     "Yotsuba",
		SortTitle: "Yotsuba",
		MediaType: library.MediaTypeManga,
		Status:    library.BookStatusOwned,
	})
	if err != nil {
		t.Fatal(err)
	}
	mangaEdition, err := selection.LibraryService.CreateEdition(ctx, library.Edition{
		BookID: mangaBook.ID,
		Title:  "Yotsuba",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:    mangaEdition.ID,
		MediaType:    library.MediaTypeManga,
		Format:       "cbz",
		Path:         "/books/yotsuba.cbz",
		OriginalPath: "/incoming/yotsuba.cbz",
		Size:         700,
		ContentHash:  "hash-manga",
		SourceID:     "source-manga",
		SourceType:   "test",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := d.SQLDB().Exec(`UPDATE books SET created_at = '2026-07-20 10:00:00', updated_at = '2026-07-20 10:00:00' WHERE id = ?`, firstBook.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQLDB().Exec(`UPDATE books SET created_at = '2026-07-21 10:00:00', updated_at = '2026-07-21 10:00:00' WHERE id = ?`, secondBook.ID); err != nil {
		t.Fatal(err)
	}

	return &Server{cfg: cfg, db: d, libraryService: selection.LibraryService}, normalizedBookAPIIDs{
		projectHailMaryID: firstBook.ID,
		darkMatterID:      secondBook.ID,
		audiobookID:       audiobookBook.ID,
		mangaID:           mangaBook.ID,
	}, func() { _ = d.Close() }
}

func newNestedEbookRepairServer(t *testing.T) (*Server, func()) {
	t.Helper()
	base := t.TempDir()
	d, err := db.New(filepath.Join(base, "repair.db"))
	if err != nil {
		t.Fatal(err)
	}
	seedCompletedEmptyBackfill(t, d)
	cfg := &config.Config{
		LibraryRepositoryMode: "normalized",
		DBPath:                filepath.Join(base, "repair.db"),
		EbookDir:              filepath.Join(base, "ebooks"),
		AudiobookDir:          filepath.Join(base, "audiobooks"),
		MangaDir:              filepath.Join(base, "manga"),
		IncomingDir:           filepath.Join(base, "incoming"),
	}
	for _, root := range []string{cfg.EbookDir, cfg.AudiobookDir, cfg.MangaDir, cfg.IncomingDir} {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	return &Server{
		cfg:            cfg,
		db:             d,
		libraryService: selection.LibraryService,
		mux:            http.NewServeMux(),
		sessions:       NewSessionStore(),
	}, func() { _ = d.Close() }
}

func writeTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
}

func sha256ForTest(t *testing.T, content []byte) string {
	t.Helper()
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func newNormalizedDeleteBookAPIServer(t *testing.T) (*Server, int64, []string, func()) {
	t.Helper()
	base := t.TempDir()
	d, err := db.New(filepath.Join(base, "delete-books-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	seedCompletedEmptyBackfill(t, d)

	cfg := &config.Config{
		LibraryRepositoryMode: "normalized",
		EbookDir:              filepath.Join(base, "ebooks"),
		AudiobookDir:          filepath.Join(base, "audiobooks"),
		MangaDir:              filepath.Join(base, "manga"),
	}
	for _, root := range []string{cfg.EbookDir, cfg.AudiobookDir, cfg.MangaDir} {
		if err := os.MkdirAll(root, 0755); err != nil {
			t.Fatal(err)
		}
	}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: cfg, db: d, libraryService: selection.LibraryService, coverCache: library.NewCoverCache(filepath.Join(base, "covers"))}

	ctx := context.Background()
	book, err := selection.LibraryService.CreateBook(ctx, library.Book{Title: "Delete Me", MediaType: library.MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	edition, err := selection.LibraryService.CreateEdition(ctx, library.Edition{BookID: book.ID, Title: "Delete Me"})
	if err != nil {
		t.Fatal(err)
	}
	if err := selection.LibraryService.AttachContributor(ctx, edition.ID, library.Contributor{Name: "Delete Author", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	epub := filepath.Join(cfg.EbookDir, "Delete Me.epub")
	mobi := filepath.Join(cfg.EbookDir, "Delete Me.mobi")
	for _, path := range []string{epub, mobi} {
		if err := os.WriteFile(path, []byte(filepath.Base(path)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:   edition.ID,
		MediaType:   library.MediaTypeEbook,
		Format:      "epub",
		Path:        epub,
		ContentHash: "hash-delete-epub",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := selection.LibraryService.AttachFile(ctx, library.BookFile{
		EditionID:   edition.ID,
		MediaType:   library.MediaTypeEbook,
		Format:      "mobi",
		Path:        mobi,
		ContentHash: "hash-delete-mobi",
	}); err != nil {
		t.Fatal(err)
	}
	return s, book.ID, []string{epub, mobi}, func() { _ = d.Close() }
}
