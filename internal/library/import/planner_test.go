package libraryimport

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/library"
)

func TestImportPlannerEPUBAndMOBISameBook(t *testing.T) {
	dir := t.TempDir()
	epubPath := filepath.Join(dir, "torrent-1.epub")
	writeEPUB(t, epubPath, "The Guardian's Path", "Jane Doe")
	mobiPath := filepath.Join(dir, "Jane Doe - The Guardian's Path.mobi")
	writeFile(t, mobiPath, "same-book-mobi")

	planner := NewImportPlanner(newFakeCatalog())
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeEbook},
		RootPath: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plans) != 2 {
		t.Fatalf("plans = %d, want 2", len(result.Plans))
	}
	if result.Counts[string(DispositionCreateNewBook)] != 1 {
		t.Fatalf("create_new_book count = %d, want 1", result.Counts[string(DispositionCreateNewBook)])
	}
	if result.Counts[string(DispositionAttachNewFormat)] != 1 {
		t.Fatalf("attach_new_format count = %d, want 1", result.Counts[string(DispositionAttachNewFormat)])
	}
}

func TestImportPlannerSameTitleDifferentAuthorNeedsManualReview(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Someone Else - Dune.azw3")
	writeFile(t, path, "azw3")

	catalog := newFakeCatalog()
	catalog.addBookWithEditionAndFile("Dune", "Frank Herbert", library.MediaTypeEbook, "epub", "/books/dune.epub", "hash-dune")

	planner := NewImportPlanner(catalog)
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := result.Plans[0]
	if plan.Disposition != DispositionNeedsManualReview {
		t.Fatalf("disposition = %s, want %s", plan.Disposition, DispositionNeedsManualReview)
	}
}

func TestImportPlannerSameAuthorDifferentTitleCreatesNewBook(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Jane Doe - Second Book.azw3")
	writeFile(t, path, "azw3")

	catalog := newFakeCatalog()
	catalog.addBookWithEditionAndFile("First Book", "Jane Doe", library.MediaTypeEbook, "epub", "/books/first.epub", "hash-first")

	planner := NewImportPlanner(catalog)
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Plans[0].Disposition; got != DispositionCreateNewBook {
		t.Fatalf("disposition = %s, want %s", got, DispositionCreateNewBook)
	}
}

func TestImportPlannerMissingMetadataFallsBackToFilename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Unknown Book.azw3")
	writeFile(t, path, "azw3")

	planner := NewImportPlanner(newFakeCatalog())
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := result.Plans[0].Candidate.Metadata
	if meta.SelectedTitle != "Unknown Book" {
		t.Fatalf("selected title = %q, want %q", meta.SelectedTitle, "Unknown Book")
	}
	if meta.EmbeddedTitle != "" {
		t.Fatalf("embedded title = %q, want empty", meta.EmbeddedTitle)
	}
}

func TestImportPlannerEmbeddedMetadataWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Torrent Name.epub")
	writeEPUB(t, path, "The Guardian's Path", "Jane Doe")

	planner := NewImportPlanner(newFakeCatalog())
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := result.Plans[0].Candidate.Metadata
	if meta.SelectedTitle != "The Guardian's Path" || meta.SelectedAuthor != "Jane Doe" {
		t.Fatalf("selected metadata = %+v", meta)
	}
	if meta.FilenameTitle != "Torrent Name" {
		t.Fatalf("filename title = %q, want %q", meta.FilenameTitle, "Torrent Name")
	}
}

func TestImportPlannerMOBIFilenameFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Disney - [Prince of Persia- The Sands of Time] - The Guardian's Path - Carla Jablonski (retail) (epub).mobi")
	writeFile(t, path, "mobi")

	planner := NewImportPlanner(newFakeCatalog())
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := result.Plans[0].Candidate.Metadata
	if meta.SelectedTitle != "The Guardian's Path" || meta.SelectedAuthor != "Carla Jablonski" {
		t.Fatalf("selected metadata = %+v", meta)
	}
}

func TestImportPlannerComicArchives(t *testing.T) {
	dir := t.TempDir()
	cbz := filepath.Join(dir, "Saga Vol 1.cbz")
	cbr := filepath.Join(dir, "Saga Vol 2.cbr")
	writeFile(t, cbz, "cbz")
	writeFile(t, cbr, "cbr")

	planner := NewImportPlanner(newFakeCatalog())
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeComic},
		RootPath: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plans) != 2 {
		t.Fatalf("plans = %d, want 2", len(result.Plans))
	}
	for _, plan := range result.Plans {
		if plan.Candidate.MediaType != library.MediaTypeComic {
			t.Fatalf("candidate media type = %s, want comic", plan.Candidate.MediaType)
		}
		if plan.Candidate.Metadata.SelectedTitle == "" {
			t.Fatal("comic plan missing selected title")
		}
	}
}

func TestImportPlannerAudiobookDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "The Guardian's Path")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "Jane Doe - The Guardian's Path.mp3"), "audio")

	planner := NewImportPlanner(newFakeCatalog())
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeAudiobook},
		RootPath: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(result.Plans))
	}
	plan := result.Plans[0]
	if !plan.Candidate.IsDirectory {
		t.Fatal("audiobook candidate should be a directory")
	}
	if plan.Candidate.Metadata.SelectedTitle != "The Guardian's Path" || plan.Candidate.Metadata.SelectedAuthor != "Jane Doe" {
		t.Fatalf("audiobook metadata = %+v", plan.Candidate.Metadata)
	}
}

func TestImportPlannerDuplicateContentHashIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Jane Doe - Duplicate.mobi")
	writeFile(t, path, "identical-bytes")

	hash := fileHashForTest(t, path)
	catalog := newFakeCatalog()
	catalog.addBookWithEditionAndFile("Duplicate", "Jane Doe", library.MediaTypeEbook, "epub", "/books/duplicate.epub", hash)

	planner := NewImportPlanner(catalog)
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Plans[0].Disposition; got != DispositionIgnoreDuplicate {
		t.Fatalf("disposition = %s, want %s", got, DispositionIgnoreDuplicate)
	}
}

func TestImportPlannerDuplicateFormatConflicts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Jane Doe - Duplicate.epub")
	writeFile(t, path, "new-edition-same-format")

	catalog := newFakeCatalog()
	catalog.addBookWithEditionAndFile("Duplicate", "Jane Doe", library.MediaTypeEbook, "epub", "/books/duplicate.epub", "other-hash")

	planner := NewImportPlanner(catalog)
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Plans[0].Disposition; got != DispositionConflict {
		t.Fatalf("disposition = %s, want %s", got, DispositionConflict)
	}
}

func TestImportPlannerNewFormatExistingBook(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Jane Doe - Existing Book.mobi")
	writeFile(t, path, "mobi")

	catalog := newFakeCatalog()
	catalog.addBookWithEditionAndFile("Existing Book", "Jane Doe", library.MediaTypeEbook, "epub", "/books/existing.epub", "hash-1")

	planner := NewImportPlanner(catalog)
	result, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := result.Plans[0]
	if plan.Disposition != DispositionAttachNewFormat {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.File.Action != FileActionAttachNewFormat {
		t.Fatalf("plan = %+v", plan)
	}
}

type fakeCatalog struct {
	nextBookID    int64
	nextEditionID int64
	nextFileID    int64

	books               map[int64]library.Book
	editions            map[int64]library.Edition
	editionContributors map[int64][]library.Contributor
	files               map[int64][]library.BookFile
	filesByPath         map[string]library.BookFile
	filesByHash         map[string][]library.BookFile
	booksByIdentifier   map[string]int64
}

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{
		nextBookID:          1,
		nextEditionID:       1,
		nextFileID:          1,
		books:               map[int64]library.Book{},
		editions:            map[int64]library.Edition{},
		editionContributors: map[int64][]library.Contributor{},
		files:               map[int64][]library.BookFile{},
		filesByPath:         map[string]library.BookFile{},
		filesByHash:         map[string][]library.BookFile{},
		booksByIdentifier:   map[string]int64{},
	}
}

func (c *fakeCatalog) addBookWithEditionAndFile(title, author string, mediaType library.MediaType, format, path, hash string) {
	bookID := c.nextBookID
	c.nextBookID++
	book := library.Book{
		ID:           bookID,
		Title:        title,
		SortTitle:    library.NormalizeKey(title),
		MediaType:    mediaType,
		Contributors: []library.Contributor{{ID: bookID, Name: author, SortName: library.NormalizeKey(author)}},
	}
	c.books[bookID] = book

	editionID := c.nextEditionID
	c.nextEditionID++
	edition := library.Edition{
		ID:       editionID,
		BookID:   bookID,
		Title:    title,
		Language: "",
	}
	c.editions[editionID] = edition
	c.editionContributors[editionID] = book.Contributors

	fileID := c.nextFileID
	c.nextFileID++
	file := library.BookFile{
		ID:           fileID,
		BookID:       bookID,
		EditionID:    editionID,
		MediaType:    mediaType,
		Format:       format,
		Path:         path,
		OriginalPath: path,
		ContentHash:  hash,
	}
	c.files[bookID] = append(c.files[bookID], file)
	c.filesByPath[path] = file
	if hash != "" {
		c.filesByHash[hash] = append(c.filesByHash[hash], file)
	}
}

func (c *fakeCatalog) FindBookByIdentifier(context.Context, library.Identifier) (*library.Book, error) {
	return nil, library.ErrNotFound
}

func (c *fakeCatalog) SearchBooks(_ context.Context, query library.BookQuery) ([]library.Book, error) {
	var books []library.Book
	for _, book := range c.books {
		if query.MediaType != "" && book.MediaType != query.MediaType {
			continue
		}
		if strings.EqualFold(library.NormalizeKey(book.Title), library.NormalizeKey(query.Title)) {
			books = append(books, book)
		}
	}
	if len(books) == 0 {
		return nil, library.ErrNotFound
	}
	return books, nil
}

func (c *fakeCatalog) GetBook(_ context.Context, id int64) (*library.Book, error) {
	book, ok := c.books[id]
	if !ok {
		return nil, library.ErrNotFound
	}
	return &book, nil
}

func (c *fakeCatalog) FindEdition(_ context.Context, bookID int64, title string) (*library.Edition, error) {
	for _, edition := range c.editions {
		if edition.BookID == bookID && strings.EqualFold(library.NormalizeKey(edition.Title), library.NormalizeKey(title)) {
			copy := edition
			return &copy, nil
		}
	}
	return nil, library.ErrNotFound
}

func (c *fakeCatalog) ListBookEditions(_ context.Context, bookID int64) ([]library.Edition, error) {
	var editions []library.Edition
	for _, edition := range c.editions {
		if edition.BookID == bookID {
			editions = append(editions, edition)
		}
	}
	if len(editions) == 0 {
		return nil, library.ErrNotFound
	}
	return editions, nil
}

func (c *fakeCatalog) GetEdition(_ context.Context, id int64) (*library.Edition, error) {
	edition, ok := c.editions[id]
	if !ok {
		return nil, library.ErrNotFound
	}
	return &edition, nil
}

func (c *fakeCatalog) GetEditionContributors(_ context.Context, editionID int64) ([]library.Contributor, error) {
	contributors := c.editionContributors[editionID]
	if len(contributors) == 0 {
		return nil, library.ErrNotFound
	}
	return contributors, nil
}

func (c *fakeCatalog) GetBookFiles(_ context.Context, bookID int64) ([]library.BookFile, error) {
	files := c.files[bookID]
	if len(files) == 0 {
		return nil, library.ErrNotFound
	}
	return files, nil
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

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
}

func writeEPUB(t *testing.T, path, title, author string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	opf, err := zw.Create("content.opf")
	if err != nil {
		t.Fatal(err)
	}
	_, err = opf.Write([]byte(`<package><metadata><dc:title xmlns:dc="x">` + title + `</dc:title><dc:creator xmlns:dc="x">` + author + `</dc:creator></metadata></package>`))
	if err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func fileHashForTest(t *testing.T, path string) string {
	t.Helper()
	hash, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
