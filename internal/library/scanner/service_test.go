package scanner

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
)

func TestScannerRecursiveScanningAndReviewPayload(t *testing.T) {
	roots := testRoots(t)
	writeFile(t, filepath.Join(roots.EbookDir, "nested", "Project Hail Mary.EPUB"), "epub bytes")
	writeFile(t, filepath.Join(roots.EbookDir, "notes.txt"), "ignore me")
	writeFile(t, filepath.Join(roots.AudiobookDir, "Audio Book.MP3"), "audio bytes")
	writeFile(t, filepath.Join(roots.MangaDir, "Volume 1.PDF"), "%PDF-1.7 /Title (Manga Volume) /Author (Artist)")

	manager := NewManager(newFakeCatalog())
	job := runScan(t, manager, roots)
	result := job.Result
	if result == nil {
		t.Fatal("missing result")
	}
	if result.Totals.Found != 4 || result.Totals.ReadyToImport != 3 || result.Totals.Unsupported != 1 {
		t.Fatalf("totals = %+v", result.Totals)
	}
	byName := candidatesByFilename(result.Candidates)
	if byName["Project Hail Mary.EPUB"].Format != "epub" || byName["Project Hail Mary.EPUB"].MediaType != library.MediaTypeEbook {
		t.Fatalf("ebook candidate = %+v", byName["Project Hail Mary.EPUB"])
	}
	if byName["Audio Book.MP3"].Format != "mp3" || byName["Audio Book.MP3"].MediaType != library.MediaTypeAudiobook {
		t.Fatalf("audio candidate = %+v", byName["Audio Book.MP3"])
	}
	if byName["Volume 1.PDF"].MediaType != library.MediaTypeManga {
		t.Fatalf("pdf should be classified by manga root: %+v", byName["Volume 1.PDF"])
	}
	if byName["notes.txt"].Classification != ClassificationUnsupported {
		t.Fatalf("unsupported = %+v", byName["notes.txt"])
	}
}

func TestScannerAddsEmbeddedEPUBCoverURL(t *testing.T) {
	roots := testRoots(t)
	writeEPUBWithCover(t, filepath.Join(roots.EbookDir, "Covered.epub"), "Covered", "Jane Doe")
	manager := NewManager(newFakeCatalog(), WithCoverCache(library.NewCoverCache(filepath.Join(t.TempDir(), "covers"))))

	job := runScan(t, manager, roots)
	if job.Result == nil || len(job.Result.Candidates) == 0 {
		t.Fatal("missing scan result")
	}
	candidate := candidatesByFilename(job.Result.Candidates)["Covered.epub"]
	if candidate.CoverURL == "" {
		t.Fatalf("expected cover URL, candidate = %+v", candidate)
	}
	if candidate.CoverPath == "" {
		t.Fatal("expected cached cover path")
	}
	if _, err := os.Stat(candidate.CoverPath); err != nil {
		t.Fatalf("expected cached cover file: %v", err)
	}
}

func TestScannerMissingFolderRecordsWarningAndContinues(t *testing.T) {
	roots := testRoots(t)
	missing := filepath.Join(t.TempDir(), "missing")
	roots.EbookDir = missing
	writeFile(t, filepath.Join(roots.AudiobookDir, "Audio.m4b"), "audio")

	job := runScan(t, NewManager(newFakeCatalog()), roots)
	if job.Result == nil {
		t.Fatal("missing result")
	}
	if job.Result.Totals.ReadyToImport != 1 {
		t.Fatalf("totals = %+v", job.Result.Totals)
	}
	if len(job.Result.Warnings) == 0 || job.Result.Warnings[0].Message != "Folder not found" {
		t.Fatalf("warnings = %+v", job.Result.Warnings)
	}
}

func TestScannerUnreadableFile(t *testing.T) {
	roots := testRoots(t)
	broken := filepath.Join(roots.EbookDir, "Broken.epub")
	if err := os.Symlink(filepath.Join(roots.EbookDir, "missing-target.epub"), broken); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	job := runScan(t, NewManager(newFakeCatalog()), roots)
	if job.Result.Totals.Unreadable != 1 {
		t.Fatalf("totals = %+v candidates=%+v", job.Result.Totals, job.Result.Candidates)
	}
	if job.Result.Candidates[0].Classification != ClassificationUnreadable || job.Result.Candidates[0].Error == "" {
		t.Fatalf("candidate = %+v", job.Result.Candidates[0])
	}
}

func TestScannerEmbeddedMetadataAndFilenameFallback(t *testing.T) {
	roots := testRoots(t)
	writeEPUB(t, filepath.Join(roots.EbookDir, "Torrent Name.epub"), "The Guardian's Path", "Carla Jablonski")
	writeFile(t, filepath.Join(roots.EbookDir, "Carla Jablonski - To Right a Wrong.mobi"), "mobi bytes")

	job := runScan(t, NewManager(newFakeCatalog()), roots)
	byName := candidatesByFilename(job.Result.Candidates)

	embedded := byName["Torrent Name.epub"]
	if embedded.Title != "The Guardian's Path" || embedded.Author != "Carla Jablonski" || embedded.Metadata.Source != "embedded_metadata" {
		t.Fatalf("embedded = %+v", embedded)
	}
	fallback := byName["Carla Jablonski - To Right a Wrong.mobi"]
	if fallback.Title != "To Right a Wrong" || fallback.Author != "Carla Jablonski" || fallback.Metadata.Source != "filename_fallback" {
		t.Fatalf("fallback = %+v", fallback)
	}
	if fallback.DestinationPath != fallback.Path {
		t.Fatalf("destination path = %q, want %q", fallback.DestinationPath, fallback.Path)
	}
}

func TestScannerAudiobookPathFallbackUsesAuthorDirectoryAndFilenameTitle(t *testing.T) {
	roots := testRoots(t)
	writeFile(t, filepath.Join(roots.AudiobookDir, "Stephen King", "11.22.63 - Part.m4b"), "audio bytes")

	job := runScan(t, NewManager(newFakeCatalog()), roots)
	candidate := candidatesByFilename(job.Result.Candidates)["11.22.63 - Part.m4b"]
	if candidate.MediaType != library.MediaTypeAudiobook {
		t.Fatalf("candidate = %+v", candidate)
	}
	if candidate.Author != "Stephen King" {
		t.Fatalf("author = %q, want Stephen King", candidate.Author)
	}
	if candidate.Title != "11.22.63" {
		t.Fatalf("title = %q, want 11.22.63", candidate.Title)
	}
	if candidate.Metadata.Source != "filename_fallback" {
		t.Fatalf("metadata source = %q", candidate.Metadata.Source)
	}
}

func TestPreviewDestinationDoesNotDuplicateLibrarySegment(t *testing.T) {
	candidate := Candidate{
		Title:           "The Guardian's Path",
		Author:          "Carla Jablonski",
		Format:          "epub",
		Path:            filepath.Join("/books", "ebooks", "Disney", "The Guardian's Path.epub"),
		DestinationPath: filepath.Join("/books", "ebooks", "ebooks", "The Guardian's Path.epub"),
	}

	got := previewDestination(candidate)
	want := filepath.Join("/books", "ebooks", "Carla Jablonski - The Guardian's Path.epub")
	if got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
	if strings.Count(filepath.ToSlash(got), "/ebooks/") != 1 {
		t.Fatalf("destination contains duplicated ebooks segment: %q", got)
	}
}

func TestApplyPlanDoesNotExposeDuplicatedLibrarySegment(t *testing.T) {
	candidate := Candidate{
		Title:     "Ameritopia-The Unmaking of America",
		Author:    "Mark R. Levin",
		Format:    "mobi",
		MediaType: library.MediaTypeEbook,
		Path:      filepath.Join("/books", "ebooks", "Ameritopia-The Unmaking of America.mobi"),
	}
	plan := libraryimport.ImportPlan{
		Candidate: libraryimport.ImportCandidate{
			Path:      candidate.Path,
			MediaType: library.MediaTypeEbook,
			Format:    "mobi",
			Metadata: libraryimport.CandidateMetadata{
				SelectedTitle:  candidate.Title,
				SelectedAuthor: candidate.Author,
			},
		},
		Book: libraryimport.ResolvedBook{Action: libraryimport.BookActionNeedsManualReview},
		File: libraryimport.FileDecision{
			Action:   libraryimport.FileActionNeedsManualReview,
			Proposed: &library.BookFile{Path: filepath.Join("/books", "ebooks", "ebooks", candidate.Title, candidate.Author, candidate.Filename)},
		},
		Disposition: libraryimport.DispositionNeedsManualReview,
	}

	applyPlan(&candidate, plan)

	if strings.Contains(filepath.ToSlash(candidate.DestinationPath), "/ebooks/ebooks/") {
		t.Fatalf("destination contains duplicated ebooks segment: %q", candidate.DestinationPath)
	}
	if candidate.ManualReview == nil || strings.Contains(filepath.ToSlash(candidate.ManualReview.SuggestedDestination), "/ebooks/ebooks/") {
		t.Fatalf("manual review destination = %+v", candidate.ManualReview)
	}
}

func TestScannerAlreadyImportedAndDuplicateAreDistinct(t *testing.T) {
	roots := testRoots(t)
	existingPath := filepath.Join(roots.EbookDir, "Existing.epub")
	duplicatePath := filepath.Join(roots.EbookDir, "Duplicate.epub")
	writeFile(t, existingPath, "same existing")
	writeFile(t, duplicatePath, "same duplicate")
	hash, err := fileSHA256(duplicatePath)
	if err != nil {
		t.Fatal(err)
	}
	catalog := newFakeCatalog()
	catalog.filesByPath[existingPath] = library.BookFile{ID: 1, Path: existingPath, Format: "epub", MediaType: library.MediaTypeEbook}
	catalog.filesByHash[hash] = []library.BookFile{{ID: 2, Path: "/other/Duplicate.epub", ContentHash: hash, Format: "epub", MediaType: library.MediaTypeEbook}}

	job := runScan(t, NewManager(catalog), roots)
	byName := candidatesByFilename(job.Result.Candidates)
	if byName["Existing.epub"].Classification != ClassificationAlreadyImported {
		t.Fatalf("existing = %+v", byName["Existing.epub"])
	}
	if byName["Existing.epub"].ExistingPath != existingPath {
		t.Fatalf("existing path = %+v", byName["Existing.epub"])
	}
	if byName["Duplicate.epub"].Classification != ClassificationDuplicate || byName["Duplicate.epub"].ExistingPath != "/other/Duplicate.epub" {
		t.Fatalf("duplicate = %+v", byName["Duplicate.epub"])
	}
	if byName["Duplicate.epub"].Duplicate == nil || byName["Duplicate.epub"].Duplicate.Reason != "Identical hash" {
		t.Fatalf("duplicate details = %+v", byName["Duplicate.epub"].Duplicate)
	}
}

func TestScannerManualReviewDetails(t *testing.T) {
	roots := testRoots(t)
	writeFile(t, filepath.Join(roots.EbookDir, "New Author - Shared Title.mobi"), "mobi bytes")
	catalog := newFakeCatalog()
	catalog.booksByID[1] = library.Book{
		ID:        1,
		Title:     "Shared Title",
		MediaType: library.MediaTypeEbook,
		Contributors: []library.Contributor{{
			Name:  "Existing Author",
			Roles: []library.ContributorRole{library.RoleAuthor},
		}},
	}

	job := runScan(t, NewManager(catalog), roots)
	candidate := candidatesByFilename(job.Result.Candidates)["New Author - Shared Title.mobi"]
	if candidate.Classification != ClassificationManualReview {
		t.Fatalf("classification = %s candidate=%+v", candidate.Classification, candidate)
	}
	if job.Result.Totals.ManualReview != 1 || job.Result.Totals.ReadyToImport != 0 {
		t.Fatalf("totals = %+v", job.Result.Totals)
	}
	if candidate.ManualReview == nil || candidate.ManualReview.Reason == "" || candidate.ManualReview.MetadataSource != "filename_fallback" {
		t.Fatalf("manual review = %+v", candidate.ManualReview)
	}
}

func TestScannerUsesFilenameAuthorWhenEmbeddedAuthorLooksPublisher(t *testing.T) {
	roots := testRoots(t)
	filename := "Disney - [Prince of Persia- The Sands of Time] - The Guardian's Path - Carla Jablonski (retail) (epub).epub"
	writeEPUB(t, filepath.Join(roots.EbookDir, filename), "The Guardian's Path", "Disney Book Group")
	catalog := newFakeCatalog()
	catalog.booksByID[1] = library.Book{
		ID:        1,
		Title:     "The Guardian's Path",
		MediaType: library.MediaTypeEbook,
		Contributors: []library.Contributor{{
			Name:  "Carla Jablonski",
			Roles: []library.ContributorRole{library.RoleAuthor},
		}},
	}

	job := runScan(t, NewManager(catalog), roots)
	candidate := candidatesByFilename(job.Result.Candidates)[filename]
	if candidate.Classification == ClassificationManualReview {
		t.Fatalf("candidate should not need manual review: %+v", candidate)
	}
	if candidate.Author != "Carla Jablonski" || candidate.Metadata.Author != "Carla Jablonski" {
		t.Fatalf("candidate author = %q metadata=%+v", candidate.Author, candidate.Metadata)
	}
	if candidate.Metadata.Source != "embedded_metadata" || candidate.Metadata.Confidence != library.ConfidenceHigh {
		t.Fatalf("metadata source/confidence = %+v", candidate.Metadata)
	}
}

func TestScannerConcurrentScanRejected(t *testing.T) {
	roots := testRoots(t)
	for i := 0; i < 200; i++ {
		writeFile(t, filepath.Join(roots.EbookDir, fmt.Sprintf("Book %03d.epub", i)), "bytes")
	}
	catalog := newFakeCatalog()
	catalog.searchDelay = 2 * time.Millisecond
	manager := NewManager(catalog)

	first, err := manager.Start(context.Background(), roots)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Start(context.Background(), roots)
	var active *ActiveJobError
	if !errors.As(err, &active) || active.JobID != first.ID {
		t.Fatalf("expected active job error for %s, got %v", first.ID, err)
	}
	waitJob(t, manager, first.ID)
}

func TestScannerLargeLibrary(t *testing.T) {
	roots := testRoots(t)
	for i := 0; i < 5000; i++ {
		writeFile(t, filepath.Join(roots.EbookDir, fmt.Sprintf("Book %04d.txt", i)), "unsupported")
	}
	job := runScan(t, NewManager(newFakeCatalog()), roots)
	if job.Result.Totals.Found != 5000 || job.Result.Totals.Unsupported != 5000 {
		t.Fatalf("totals = %+v", job.Result.Totals)
	}
	if job.Progress.FilesDiscovered != 5000 || job.Progress.CandidatesReady != 5000 {
		t.Fatalf("progress = %+v", job.Progress)
	}
}

func TestScannerDoesNotWriteLibraryData(t *testing.T) {
	roots := testRoots(t)
	writeFile(t, filepath.Join(roots.EbookDir, "New Book.epub"), "bytes")
	catalog := newFakeCatalog()
	runScan(t, NewManager(catalog), roots)
	if catalog.writeCalls != 0 {
		t.Fatalf("write calls = %d", catalog.writeCalls)
	}
}

func testRoots(t *testing.T) Roots {
	t.Helper()
	base := t.TempDir()
	roots := Roots{
		EbookDir:     filepath.Join(base, "ebooks"),
		AudiobookDir: filepath.Join(base, "audiobooks"),
		MangaDir:     filepath.Join(base, "manga"),
	}
	for _, dir := range []string{roots.EbookDir, roots.AudiobookDir, roots.MangaDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	return roots
}

func runScan(t *testing.T, manager *Manager, roots Roots) *Job {
	t.Helper()
	job, err := manager.Start(context.Background(), roots)
	if err != nil {
		t.Fatal(err)
	}
	return waitJob(t, manager, job.ID)
}

func waitJob(t *testing.T, manager *Manager, jobID string) *Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, ok := manager.Get(jobID)
		if !ok {
			t.Fatalf("job %s not found", jobID)
		}
		if job.Status == StatusCompleted || job.Status == StatusFailed || job.Status == StatusCancelled {
			if job.Status != StatusCompleted {
				t.Fatalf("job status = %s error=%s", job.Status, job.Error)
			}
			return job
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %s", jobID)
	return nil
}

func candidatesByFilename(candidates []Candidate) map[string]Candidate {
	out := map[string]Candidate{}
	for _, candidate := range candidates {
		out[candidate.Filename] = candidate
	}
	return out
}

func writeFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeEPUB(t *testing.T, path, title, author string) {
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
	w, err := zw.Create("content.opf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(w, `<package><metadata><title>%s</title><creator>%s</creator></metadata></package>`, title, author); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeEPUBWithCover(t *testing.T, path, title, author string) {
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
	if _, err := fmt.Fprint(container, `<container><rootfiles><rootfile full-path="OPS/content.opf"/></rootfiles></container>`); err != nil {
		t.Fatal(err)
	}
	opf, err := zw.Create("OPS/content.opf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(opf, `<package><metadata><title>%s</title><creator>%s</creator><meta name="cover" content="cover-image"/></metadata><manifest><item id="cover-image" href="images/cover.png" media-type="image/png"/></manifest></package>`, title, author); err != nil {
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

type fakeCatalog struct {
	booksByID   map[int64]library.Book
	filesByPath map[string]library.BookFile
	filesByHash map[string][]library.BookFile
	searchDelay time.Duration
	writeCalls  int
}

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{
		booksByID:   map[int64]library.Book{},
		filesByPath: map[string]library.BookFile{},
		filesByHash: map[string][]library.BookFile{},
	}
}

func (c *fakeCatalog) FindBookByIdentifier(context.Context, library.Identifier) (*library.Book, error) {
	return nil, library.ErrNotFound
}

func (c *fakeCatalog) SearchBooks(ctx context.Context, query library.BookQuery) ([]library.Book, error) {
	if c.searchDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.searchDelay):
		}
	}
	var books []library.Book
	for _, book := range c.booksByID {
		if library.NormalizeKey(book.Title) == library.NormalizeKey(query.Title) && (query.MediaType == "" || book.MediaType == query.MediaType) {
			books = append(books, book)
		}
	}
	return books, nil
}

func (c *fakeCatalog) GetBook(_ context.Context, id int64) (*library.Book, error) {
	book, ok := c.booksByID[id]
	if !ok {
		return nil, library.ErrNotFound
	}
	return &book, nil
}

func (c *fakeCatalog) FindEdition(context.Context, int64, string) (*library.Edition, error) {
	return nil, library.ErrNotFound
}

func (c *fakeCatalog) ListBookEditions(context.Context, int64) ([]library.Edition, error) {
	return nil, nil
}

func (c *fakeCatalog) GetEdition(context.Context, int64) (*library.Edition, error) {
	return nil, library.ErrNotFound
}

func (c *fakeCatalog) GetEditionContributors(context.Context, int64) ([]library.Contributor, error) {
	return nil, nil
}

func (c *fakeCatalog) GetBookFiles(context.Context, int64) ([]library.BookFile, error) {
	return nil, nil
}

func (c *fakeCatalog) FindFileByPath(_ context.Context, path string) (*library.BookFile, error) {
	file, ok := c.filesByPath[path]
	if !ok {
		return nil, library.ErrNotFound
	}
	return &file, nil
}

func (c *fakeCatalog) FindFilesByContentHash(_ context.Context, hash string) ([]library.BookFile, error) {
	files := c.filesByHash[hash]
	if len(files) == 0 {
		return nil, library.ErrNotFound
	}
	return files, nil
}
