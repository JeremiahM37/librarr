package api

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	libraryscanner "github.com/jamie75/librarr/internal/library/scanner"
)

func TestV1LibraryScanRequiresAuthentication(t *testing.T) {
	s, cleanup := newLibraryScanAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestV1LibraryScanUsesSavedSettingsAndReturnsResults(t *testing.T) {
	s, cleanup := newLibraryScanAPIServer(t)
	defer cleanup()
	savedRoot := filepath.Join(t.TempDir(), "saved-ebooks")
	if err := os.MkdirAll(savedRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(savedRoot, "Saved Book.epub"), []byte("bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	settings := map[string]interface{}{
		"ebook_dir":     savedRoot,
		"audiobook_dir": filepath.Join(t.TempDir(), "missing-audio"),
		"manga_dir":     filepath.Join(t.TempDir(), "missing-manga"),
	}
	data, _ := json.Marshal(settings)
	if err := os.WriteFile(s.cfg.SettingsFile, data, 0600); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan", nil)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var started struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}

	job := waitAPIScanJob(t, s, started.JobID)
	if job.Status != libraryscanner.StatusCompleted {
		t.Fatalf("job = %+v", job)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/library/scan/"+started.JobID+"/results", nil)
	req.SetPathValue("job_id", started.JobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("results status = %d body=%s", rr.Code, rr.Body.String())
	}
	var result libraryscanner.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Totals.ReadyToImport != 1 || result.Candidates[0].Path != filepath.Join(savedRoot, "Saved Book.epub") {
		t.Fatalf("result = %+v", result)
	}
}

func TestV1LibraryScanUnknownJob(t *testing.T) {
	s, cleanup := newLibraryScanAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/scan/nope", nil)
	req.SetPathValue("job_id", "nope")
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestV1LibraryScanPrettyResults(t *testing.T) {
	s, cleanup := newLibraryScanAPIServer(t)
	defer cleanup()
	root := filepath.Join(t.TempDir(), "ebooks")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "Debug Book.epub"), []byte("bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	settings := map[string]interface{}{
		"ebook_dir":     root,
		"audiobook_dir": filepath.Join(t.TempDir(), "missing-audio"),
		"manga_dir":     filepath.Join(t.TempDir(), "missing-manga"),
	}
	data, _ := json.Marshal(settings)
	if err := os.WriteFile(s.cfg.SettingsFile, data, 0600); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan", nil)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var started struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	waitAPIScanJob(t, s, started.JobID)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/library/scan/"+started.JobID+"/results?pretty=1", nil)
	req.SetPathValue("job_id", started.JobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "text/html; charset=UTF-8" {
		t.Fatalf("content type = %q", ct)
	}
	if body := rr.Body.String(); !strings.Contains(body, "Classification") || !strings.Contains(body, "Debug Book") {
		t.Fatalf("body = %s", body)
	}
}

func TestV1LibraryImportSelectedUpdatesScanReview(t *testing.T) {
	engine := &sequenceImportEngine{results: []*libraryimport.EngineResult{{InsertedCount: 1}}}
	s, cleanup := newLibraryScanAPIServerWithEngine(t, engine)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"Import Me.epub"})
	result := getAPIScanResult(t, s, scanJobID)

	importJobID := startAPILibraryImport(t, s, map[string]interface{}{
		"scan_job_id":   scanJobID,
		"candidate_ids": []string{result.Candidates[0].ID},
		"all_ready":     false,
	})
	importJob := waitAPIImportJob(t, s, importJobID)
	if importJob.Status != libraryImportStatusCompleted {
		t.Fatalf("job = %+v", importJob)
	}
	importResult := getAPIImportResult(t, s, importJobID)
	if importResult.Summary["imported"] != 1 || len(engine.requests) != 1 {
		t.Fatalf("result=%+v requests=%d", importResult, len(engine.requests))
	}
	updated := getAPIScanResult(t, s, scanJobID)
	if updated.Totals.AlreadyImported != 1 || updated.Totals.ReadyToImport != 0 {
		t.Fatalf("updated scan = %+v", updated.Totals)
	}
}

func TestV1LibraryImportAllReadyExcludesDuplicates(t *testing.T) {
	engine := &sequenceImportEngine{results: []*libraryimport.EngineResult{{InsertedCount: 1}}}
	s, cleanup := newLibraryScanAPIServerWithEngine(t, engine)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"Ready.epub", "Duplicate.epub"})
	result := getAPIScanResult(t, s, scanJobID)
	var duplicateID string
	for _, candidate := range result.Candidates {
		if filepath.Base(candidate.Path) == "Duplicate.epub" {
			duplicateID = candidate.ID
		}
	}
	if duplicateID == "" {
		t.Fatalf("duplicate candidate missing: %+v", result.Candidates)
	}
	s.libraryScanner.UpdateCandidates(scanJobID, []libraryscanner.CandidateUpdate{{
		ID:                   duplicateID,
		Classification:       libraryscanner.ClassificationDuplicate,
		ClassificationReason: "test duplicate",
	}})

	importJobID := startAPILibraryImport(t, s, map[string]interface{}{
		"scan_job_id": scanJobID,
		"all_ready":   true,
	})
	waitAPIImportJob(t, s, importJobID)
	if len(engine.requests) != 1 || filepath.Base(engine.requests[0].RootPath) != "Ready.epub" {
		t.Fatalf("requests = %+v", engine.requests)
	}
}

func TestV1LibraryScanResolveManualReviewCandidate(t *testing.T) {
	s, cleanup := newLibraryScanAPIServer(t)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"Review.epub"})
	result := getAPIScanResult(t, s, scanJobID)
	candidateID := result.Candidates[0].ID
	s.libraryScanner.UpdateCandidates(scanJobID, []libraryscanner.CandidateUpdate{{
		ID:                   candidateID,
		Classification:       libraryscanner.ClassificationManualReview,
		ClassificationReason: "Existing title match did not cleanly agree on author",
	}})

	body, _ := json.Marshal(map[string]interface{}{
		"id":     candidateID,
		"action": "edit_metadata",
		"title":  "Resolved Title",
		"author": "Resolved Author",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan/"+scanJobID+"/resolve", bytes.NewReader(body))
	req.SetPathValue("job_id", scanJobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resolve status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resolved libraryscanner.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &resolved); err != nil {
		t.Fatal(err)
	}
	got := resolved.Candidates[0]
	if got.Classification != libraryscanner.ClassificationNew || got.Title != "Resolved Title" || got.Author != "Resolved Author" || got.Metadata.Source != "manual_edit" {
		t.Fatalf("resolved candidate = %+v", got)
	}
	if resolved.Totals.ReadyToImport != 1 || resolved.Totals.ManualReview != 0 {
		t.Fatalf("totals = %+v", resolved.Totals)
	}
}

func TestV1LibraryScanResolveMetadataEditorFields(t *testing.T) {
	s, cleanup := newLibraryScanAPIServer(t)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"Review.epub"})
	result := getAPIScanResult(t, s, scanJobID)
	candidateID := result.Candidates[0].ID
	s.libraryScanner.UpdateCandidates(scanJobID, []libraryscanner.CandidateUpdate{{
		ID:                   candidateID,
		Classification:       libraryscanner.ClassificationManualReview,
		ClassificationReason: "Needs metadata review",
	}})

	body, _ := json.Marshal(map[string]interface{}{
		"id":               candidateID,
		"action":           "edit_metadata",
		"title":            "The Guardian's Path",
		"subtitle":         "A Prince of Persia Novel",
		"author":           "Carla Jablonski",
		"series":           "Prince of Persia",
		"series_number":    "1",
		"publisher":        "Disney",
		"publication_year": "2004",
		"isbn":             "978-1234567890",
		"language":         "en",
		"description":      "Adventure novel",
		"tags":             []string{"fantasy", "tie-in"},
		"library":          "ebook",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan/"+scanJobID+"/resolve", bytes.NewReader(body))
	req.SetPathValue("job_id", scanJobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resolve status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resolved libraryscanner.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &resolved); err != nil {
		t.Fatal(err)
	}
	got := resolved.Candidates[0]
	if got.Classification != libraryscanner.ClassificationNew || got.Metadata.Source != "manual_edit" || got.Metadata.Confidence != library.ConfidenceHigh {
		t.Fatalf("resolved candidate = %+v", got)
	}
	if got.Title != "The Guardian's Path" || got.Author != "Carla Jablonski" || got.Metadata.Subtitle != "A Prince of Persia Novel" ||
		got.Metadata.Series != "Prince of Persia" || got.Metadata.SeriesNumber != "1" || got.Metadata.Publisher != "Disney" ||
		got.Metadata.PublicationYear != "2004" || got.Metadata.ISBN != "9781234567890" || got.Metadata.Language != "en" ||
		got.Metadata.Description != "Adventure novel" || got.Metadata.Library != "ebook" || len(got.Metadata.Tags) != 2 {
		t.Fatalf("metadata = %+v", got.Metadata)
	}
	if !strings.Contains(got.DestinationPath, "Carla Jablonski - The Guardian's Path.epub") {
		t.Fatalf("destination = %q", got.DestinationPath)
	}
}

func TestV1LibraryScanResultsBackfillExistingBookCover(t *testing.T) {
	s, _, cleanup := newNormalizedLibraryScanAPIServer(t)
	defer cleanup()
	ctx := context.Background()
	ebookPath := filepath.Join(s.cfg.EbookDir, "The Guardian's Path.epub")
	writeAPIEPUBWithCover(t, ebookPath, "The Guardian's Path", "Carla Jablonski")
	book, edition := createAPIScanBook(t, s.libraryService, "The Guardian's Path", "Carla Jablonski")
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:   edition.ID,
		MediaType:   library.MediaTypeEbook,
		Format:      "epub",
		Path:        ebookPath,
		ContentHash: "already-imported",
		SourceType:  "test",
	}); err != nil {
		t.Fatal(err)
	}

	scanJobID := startAPIScanWithCurrentRoots(t, s)
	result := getAPIScanResult(t, s, scanJobID)
	candidate := result.Candidates[0]
	if candidate.Classification != libraryscanner.ClassificationAlreadyImported {
		t.Fatalf("classification = %s candidate=%+v", candidate.Classification, candidate)
	}
	if candidate.CoverURL == "" {
		t.Fatalf("expected scan cover URL, candidate = %+v", candidate)
	}

	cover, err := s.libraryService.GetPrimaryCover(ctx, book.ID)
	if err != nil {
		t.Fatalf("expected backfilled cover: %v", err)
	}
	if cover.LocalPath == "" {
		t.Fatalf("cover = %+v", cover)
	}
	if _, err := os.Stat(cover.LocalPath); err != nil {
		t.Fatalf("cover file missing: %v", err)
	}
}

func TestV1LibraryScanUseSuggestedAttachesCoverToExistingBook(t *testing.T) {
	s, dbPath, cleanup := newNormalizedLibraryScanAPIServer(t)
	defer cleanup()
	ctx := context.Background()
	ebookPath := filepath.Join(s.cfg.EbookDir, "Conflict.epub")
	writeAPIEPUBWithCover(t, ebookPath, "The Guardian's Path", "Carla Jablonski")
	book, _ := createAPIScanBook(t, s.libraryService, "The Guardian's Path", "Different Author")

	scanJobID := startAPIScanWithCurrentRoots(t, s)
	result := getAPIScanResult(t, s, scanJobID)
	candidate := result.Candidates[0]
	if candidate.Classification != libraryscanner.ClassificationManualReview {
		t.Fatalf("classification = %s candidate=%+v", candidate.Classification, candidate)
	}
	if candidate.CoverURL == "" {
		t.Fatalf("expected manual review cover URL, candidate = %+v", candidate)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"id":     candidate.ID,
		"action": "use_suggested",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan/"+scanJobID+"/resolve", bytes.NewReader(body))
	req.SetPathValue("job_id", scanJobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resolve status = %d body=%s", rr.Code, rr.Body.String())
	}

	cover, err := s.libraryService.GetPrimaryCover(ctx, book.ID)
	if err != nil {
		t.Fatalf("expected attached cover: %v", err)
	}
	if cover.LocalPath == "" {
		t.Fatalf("cover = %+v", cover)
	}
	count, err := s.libraryService.CountListedBooks(ctx, library.ListBooksQuery{MediaType: library.MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("book count = %d, want 1", count)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/books?media_type=ebook", nil)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("books status = %d body=%s", rr.Code, rr.Body.String())
	}
	var list v1BookListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || !list.Items[0].Cover.Available || list.Items[0].Cover.URL == nil {
		t.Fatalf("list items = %+v", list.Items)
	}

	reopened, err := db.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	repo, err := library.NewNormalizedRepository(reopened.SQLDB())
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := library.NewLibraryService(library.ServiceOptions{
		BookRepository:        repo,
		EditionRepository:     repo,
		FileRepository:        repo,
		MetadataRepository:    repo,
		SeriesRepository:      repo,
		ContributorRepository: repo,
		IdentifierRepository:  repo,
		CoverRepository:       repo,
		TransactionManager:    repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := restarted.GetPrimaryCover(ctx, book.ID)
	if err != nil {
		t.Fatalf("expected cover after restart: %v", err)
	}
	if persisted.LocalPath != cover.LocalPath {
		t.Fatalf("persisted cover path = %q, want %q", persisted.LocalPath, cover.LocalPath)
	}
}

func TestV1LibraryScanUseSuggestedDoesNotOverwriteExistingCover(t *testing.T) {
	s, _, cleanup := newNormalizedLibraryScanAPIServer(t)
	defer cleanup()
	ctx := context.Background()
	ebookPath := filepath.Join(s.cfg.EbookDir, "Conflict.epub")
	writeAPIEPUBWithCover(t, ebookPath, "The Guardian's Path", "Carla Jablonski")
	book, _ := createAPIScanBook(t, s.libraryService, "The Guardian's Path", "Different Author")
	existingCoverPath := filepath.Join(t.TempDir(), "existing-cover.png")
	if err := os.WriteFile(existingCoverPath, []byte{0x89, 0x50, 0x4e, 0x47, 1, 2, 3}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachCover(ctx, library.Cover{
		BookID:    book.ID,
		Source:    "existing",
		LocalPath: existingCoverPath,
		MimeType:  "image/png",
		IsPrimary: true,
	}); err != nil {
		t.Fatal(err)
	}

	scanJobID := startAPIScanWithCurrentRoots(t, s)
	result := getAPIScanResult(t, s, scanJobID)
	candidate := result.Candidates[0]
	body, _ := json.Marshal(map[string]interface{}{
		"id":     candidate.ID,
		"action": "use_suggested",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan/"+scanJobID+"/resolve", bytes.NewReader(body))
	req.SetPathValue("job_id", scanJobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resolve status = %d body=%s", rr.Code, rr.Body.String())
	}
	cover, err := s.libraryService.GetPrimaryCover(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cover.LocalPath != existingCoverPath {
		t.Fatalf("cover path = %q, want existing %q", cover.LocalPath, existingCoverPath)
	}
}

func TestV1LibraryScanMergeMatchingBooksRepairsColonDashDuplicate(t *testing.T) {
	s, _, cleanup := newNormalizedLibraryScanAPIServer(t)
	defer cleanup()
	ctx := context.Background()
	ebookPath := filepath.Join(s.cfg.EbookDir, "Ameritopia-The Unmaking of America - Mark R. Levin.mobi")
	if err := os.WriteFile(ebookPath, []byte("mobi bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	targetBook, targetEdition := createAPIScanBook(t, s.libraryService, "Ameritopia: The Unmaking of America", "Mark R. Levin")
	sourceBook, sourceEdition := createAPIScanBook(t, s.libraryService, "Ameritopia-The Unmaking of America", "Mark R Levin")
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:  targetEdition.ID,
		MediaType:  library.MediaTypeEbook,
		Format:     "epub",
		Path:       filepath.Join(s.cfg.EbookDir, "Ameritopia The Unmaking of America.epub"),
		SourceType: "test",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.libraryService.AttachFile(ctx, library.BookFile{
		EditionID:  sourceEdition.ID,
		MediaType:  library.MediaTypeEbook,
		Format:     "mobi",
		Path:       ebookPath,
		SourceType: "test",
	}); err != nil {
		t.Fatal(err)
	}

	scanJobID := startAPIScanWithCurrentRoots(t, s)
	result := getAPIScanResult(t, s, scanJobID)
	if len(result.Candidates) != 1 {
		t.Fatalf("candidates = %+v", result.Candidates)
	}
	candidate := result.Candidates[0]
	if candidate.Classification != libraryscanner.ClassificationManualReview || !strings.Contains(candidate.ClassificationReason, "Multiple existing books") {
		t.Fatalf("candidate = %+v", candidate)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"id":     candidate.ID,
		"action": "merge_matching_books",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan/"+scanJobID+"/resolve", bytes.NewReader(body))
	req.SetPathValue("job_id", scanJobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("merge status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resolved libraryscanner.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &resolved); err != nil {
		t.Fatal(err)
	}
	got := resolved.Candidates[0]
	if got.Classification != libraryscanner.ClassificationAlreadyImported || got.ExistingBookID != targetBook.ID {
		t.Fatalf("resolved candidate = %+v", got)
	}
	if got.ManualReview != nil {
		t.Fatalf("manual review was not cleared: %+v", got.ManualReview)
	}
	if strings.Contains(filepath.ToSlash(got.DestinationPath), "/ebooks/ebooks/") {
		t.Fatalf("destination contains duplicated ebooks segment: %q", got.DestinationPath)
	}
	if _, err := s.libraryService.GetBook(ctx, sourceBook.ID); !errors.Is(err, library.ErrNotFound) {
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

func TestV1LibraryImportUsesEditedMetadataOverride(t *testing.T) {
	engine := &sequenceImportEngine{results: []*libraryimport.EngineResult{{InsertedCount: 1}}}
	s, cleanup := newLibraryScanAPIServerWithEngine(t, engine)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"Review.epub"})
	result := getAPIScanResult(t, s, scanJobID)
	candidateID := result.Candidates[0].ID
	s.libraryScanner.UpdateCandidates(scanJobID, []libraryscanner.CandidateUpdate{{
		ID:             candidateID,
		Classification: libraryscanner.ClassificationManualReview,
	}})
	resolvePayload, _ := json.Marshal(map[string]interface{}{
		"id":               candidateID,
		"action":           "edit_metadata",
		"title":            "The Guardian's Path",
		"author":           "Carla Jablonski",
		"publisher":        "Disney",
		"publication_year": "2004",
		"isbn":             "9781234567890",
		"language":         "en",
		"tags":             []string{"fantasy"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan/"+scanJobID+"/resolve", bytes.NewReader(resolvePayload))
	req.SetPathValue("job_id", scanJobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resolve status = %d body=%s", rr.Code, rr.Body.String())
	}

	importJobID := startAPILibraryImport(t, s, map[string]interface{}{
		"scan_job_id":   scanJobID,
		"candidate_ids": []string{candidateID},
	})
	waitAPIImportJob(t, s, importJobID)
	if len(engine.requests) != 1 {
		t.Fatalf("requests = %d", len(engine.requests))
	}
	request := engine.requests[0]
	if request.TitleHint != "The Guardian's Path" || request.AuthorHint != "Carla Jablonski" {
		t.Fatalf("hints = %q / %q", request.TitleHint, request.AuthorHint)
	}
	if request.MetadataOverride.SelectedTitle != "The Guardian's Path" || request.MetadataOverride.SelectedAuthor != "Carla Jablonski" ||
		request.MetadataOverride.Publisher != "Disney" || request.MetadataOverride.PublicationYear != "2004" ||
		request.MetadataOverride.ISBN != "9781234567890" || request.MetadataOverride.Language != "en" ||
		len(request.MetadataOverride.Tags) != 1 {
		t.Fatalf("override = %+v", request.MetadataOverride)
	}
}

func TestV1LibraryImportPartialFailureContinues(t *testing.T) {
	engine := &sequenceImportEngine{
		results: []*libraryimport.EngineResult{{InsertedCount: 1}},
		errors:  []error{nil, os.ErrNotExist},
	}
	s, cleanup := newLibraryScanAPIServerWithEngine(t, engine)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"Good.epub", "Missing.epub"})

	importJobID := startAPILibraryImport(t, s, map[string]interface{}{
		"scan_job_id": scanJobID,
		"all_ready":   true,
	})
	waitAPIImportJob(t, s, importJobID)
	result := getAPIImportResult(t, s, importJobID)
	if result.Summary["imported"] != 1 || result.Summary["failed"] != 1 || len(engine.requests) != 2 {
		t.Fatalf("result=%+v requests=%d", result, len(engine.requests))
	}
	updated := getAPIScanResult(t, s, scanJobID)
	if updated.Totals.AlreadyImported != 1 || updated.Totals.ReadyToImport != 1 {
		t.Fatalf("updated totals = %+v", updated.Totals)
	}
}

func TestV1LibraryImportRejectsConcurrentJob(t *testing.T) {
	engine := &blockingImportEngine{started: make(chan struct{}), release: make(chan struct{})}
	s, cleanup := newLibraryScanAPIServerWithEngine(t, engine)
	defer cleanup()
	scanJobID := startCompletedAPIScan(t, s, []string{"One.epub"})

	startAPILibraryImport(t, s, map[string]interface{}{"scan_job_id": scanJobID, "all_ready": true})
	<-engine.started
	body, _ := json.Marshal(map[string]interface{}{"scan_job_id": scanJobID, "all_ready": true})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/import", bytes.NewReader(body))
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	close(engine.release)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func newLibraryScanAPIServer(t *testing.T) (*Server, func()) {
	return newLibraryScanAPIServerWithEngine(t, nil)
}

func newLibraryScanAPIServerWithEngine(t *testing.T, engine libraryimport.ImportEngine) (*Server, func()) {
	t.Helper()
	d, err := db.New(filepath.Join(t.TempDir(), "scan-api.db"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := library.NewLegacyLibraryService(d)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		APIKey:       "test-api-key",
		SettingsFile: filepath.Join(t.TempDir(), "settings.json"),
		EbookDir:     filepath.Join(t.TempDir(), "default-ebooks"),
		AudiobookDir: filepath.Join(t.TempDir(), "default-audio"),
		MangaDir:     filepath.Join(t.TempDir(), "default-manga"),
	}
	s := &Server{
		cfg:            cfg,
		db:             d,
		mux:            http.NewServeMux(),
		sessions:       NewSessionStore(),
		libraryService: service,
		importEngine:   engine,
		libraryScanner: libraryscanner.NewManager(service),
	}
	s.registerLibraryRoutes()
	return s, func() { d.Close() }
}

func newNormalizedLibraryScanAPIServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "scan-normalized.db")
	d, err := db.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	seedCompletedEmptyBackfill(t, d)
	cfg := &config.Config{
		APIKey:                "test-api-key",
		LibraryRepositoryMode: "normalized",
		SettingsFile:          filepath.Join(t.TempDir(), "settings.json"),
		EbookDir:              filepath.Join(t.TempDir(), "ebooks"),
		AudiobookDir:          filepath.Join(t.TempDir(), "audio"),
		MangaDir:              filepath.Join(t.TempDir(), "manga"),
	}
	for _, dir := range []string{cfg.EbookDir, cfg.AudiobookDir, cfg.MangaDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	settings := map[string]interface{}{
		"ebook_dir":     cfg.EbookDir,
		"audiobook_dir": cfg.AudiobookDir,
		"manga_dir":     cfg.MangaDir,
	}
	data, _ := json.Marshal(settings)
	if err := os.WriteFile(cfg.SettingsFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	coverCache := library.NewCoverCache(filepath.Join(t.TempDir(), "covers"))
	s := &Server{
		cfg:            cfg,
		db:             d,
		mux:            http.NewServeMux(),
		sessions:       NewSessionStore(),
		libraryService: selection.LibraryService,
		coverCache:     coverCache,
		libraryScanner: libraryscanner.NewManager(selection.LibraryService, libraryscanner.WithCoverCache(coverCache)),
	}
	s.registerLibraryRoutes()
	return s, dbPath, func() { d.Close() }
}

func createAPIScanBook(t *testing.T, svc *library.LibraryService, title, author string) (*library.Book, *library.Edition) {
	t.Helper()
	ctx := context.Background()
	book, err := svc.CreateBook(ctx, library.Book{Title: title, SortTitle: title, MediaType: library.MediaTypeEbook, Status: library.BookStatusOwned})
	if err != nil {
		t.Fatal(err)
	}
	edition, err := svc.CreateEdition(ctx, library.Edition{BookID: book.ID, Title: title})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(author) != "" {
		if err := svc.AttachContributor(ctx, edition.ID, library.Contributor{Name: author, Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
			t.Fatal(err)
		}
	}
	return book, edition
}

func startAPIScanWithCurrentRoots(t *testing.T, s *Server) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan", nil)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("scan status = %d body=%s", rr.Code, rr.Body.String())
	}
	var started struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	waitAPIScanJob(t, s, started.JobID)
	return started.JobID
}

func writeAPIEPUBWithCover(t *testing.T, path, title, author string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	container, err := zw.Create("META-INF/container.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := container.Write([]byte(`<container><rootfiles><rootfile full-path="OPS/content.opf"/></rootfiles></container>`)); err != nil {
		t.Fatal(err)
	}
	opf, err := zw.Create("OPS/content.opf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opf.Write([]byte(`<package><metadata><title>` + title + `</title><creator>` + author + `</creator><meta name="cover" content="cover-image"/></metadata><manifest><item id="cover-image" href="images/cover.png" media-type="image/png"/></manifest></package>`)); err != nil {
		t.Fatal(err)
	}
	img, err := zw.Create("OPS/images/cover.png")
	if err != nil {
		t.Fatal(err)
	}
	png, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=")
	if _, err := img.Write(png); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitAPIScanJob(t *testing.T, s *Server, jobID string) *libraryscanner.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/library/scan/"+jobID, nil)
		req.SetPathValue("job_id", jobID)
		req.Header.Set("X-Api-Key", s.cfg.APIKey)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("job status = %d body=%s", rr.Code, rr.Body.String())
		}
		var job libraryscanner.Job
		if err := json.Unmarshal(rr.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status == libraryscanner.StatusCompleted || job.Status == libraryscanner.StatusFailed || job.Status == libraryscanner.StatusCancelled {
			return &job
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", jobID)
	return nil
}

func startCompletedAPIScan(t *testing.T, s *Server, filenames []string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "ebooks")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, filename := range filenames {
		if err := os.WriteFile(filepath.Join(root, filename), []byte(filename), 0600); err != nil {
			t.Fatal(err)
		}
	}
	settings := map[string]interface{}{
		"ebook_dir":     root,
		"audiobook_dir": filepath.Join(t.TempDir(), "missing-audio"),
		"manga_dir":     filepath.Join(t.TempDir(), "missing-manga"),
	}
	data, _ := json.Marshal(settings)
	if err := os.WriteFile(s.cfg.SettingsFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/scan", nil)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("scan status = %d body=%s", rr.Code, rr.Body.String())
	}
	var started struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	waitAPIScanJob(t, s, started.JobID)
	return started.JobID
}

func getAPIScanResult(t *testing.T, s *Server, jobID string) libraryscanner.Result {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/scan/"+jobID+"/results", nil)
	req.SetPathValue("job_id", jobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("scan result status = %d body=%s", rr.Code, rr.Body.String())
	}
	var result libraryscanner.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func startAPILibraryImport(t *testing.T, s *Server, payload map[string]interface{}) string {
	t.Helper()
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/library/import", bytes.NewReader(body))
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("import status = %d body=%s", rr.Code, rr.Body.String())
	}
	var started struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	return started.JobID
}

func waitAPIImportJob(t *testing.T, s *Server, jobID string) *libraryImportJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/library/import/"+jobID, nil)
		req.SetPathValue("job_id", jobID)
		req.Header.Set("X-Api-Key", s.cfg.APIKey)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("import job status = %d body=%s", rr.Code, rr.Body.String())
		}
		var job libraryImportJob
		if err := json.Unmarshal(rr.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status == libraryImportStatusCompleted || job.Status == libraryImportStatusFailed {
			return &job
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for import %s", jobID)
	return nil
}

func getAPIImportResult(t *testing.T, s *Server, jobID string) libraryImportResult {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/library/import/"+jobID+"/results", nil)
	req.SetPathValue("job_id", jobID)
	req.Header.Set("X-Api-Key", s.cfg.APIKey)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("import result status = %d body=%s", rr.Code, rr.Body.String())
	}
	var result libraryImportResult
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

type sequenceImportEngine struct {
	requests []libraryimport.ImportRequest
	results  []*libraryimport.EngineResult
	errors   []error
}

func (e *sequenceImportEngine) Import(_ context.Context, request libraryimport.ImportRequest) (*libraryimport.EngineResult, error) {
	e.requests = append(e.requests, request)
	idx := len(e.requests) - 1
	if idx < len(e.errors) && e.errors[idx] != nil {
		return nil, e.errors[idx]
	}
	if idx < len(e.results) && e.results[idx] != nil {
		return e.results[idx], nil
	}
	return &libraryimport.EngineResult{InsertedCount: 1}, nil
}

type blockingImportEngine struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingImportEngine) Import(_ context.Context, request libraryimport.ImportRequest) (*libraryimport.EngineResult, error) {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return &libraryimport.EngineResult{InsertedCount: 1}, nil
}
