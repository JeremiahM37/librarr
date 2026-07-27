package libraryimport

import (
	"context"
	"path/filepath"
	"testing"

	librarrdb "github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
)

var _ RepositoryWriter = (*library.LibraryService)(nil)
var _ Catalog = (*library.LibraryService)(nil)

func TestImportExecutorNewBook(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	dir := t.TempDir()
	path := filepath.Join(dir, "Torrent Name.epub")
	writeEPUB(t, path, "The Guardian's Path", "Jane Doe")

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Results) != 1 || summary.Results[0].Status != ExecutionStatusSuccess {
		t.Fatalf("summary = %+v", summary)
	}
	books, err := service.ListBooks(context.Background(), library.ListBooksQuery{MediaType: library.MediaTypeEbook, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 {
		t.Fatalf("books = %+v", books)
	}
	files, err := service.GetBookFiles(context.Background(), books[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Format != "epub" {
		t.Fatalf("files = %+v", files)
	}
}

func TestImportExecutorExistingBookAndNewFormat(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	existingBook, err := service.CreateBook(context.Background(), library.Book{Title: "Existing Book", MediaType: library.MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	existingEdition, err := service.CreateEdition(context.Background(), library.Edition{BookID: existingBook.ID, Title: "Existing Book"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AttachContributor(context.Background(), existingEdition.ID, library.Contributor{Name: "Jane Doe", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AttachFile(context.Background(), library.BookFile{EditionID: existingEdition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: "/books/existing.epub", OriginalPath: "/books/existing.epub", ContentHash: "hash-epub"}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "Jane Doe - Existing Book.mobi")
	writeFile(t, path, "mobi-bytes")

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if planned.Plans[0].Disposition != DispositionAttachNewFormat {
		t.Fatalf("plan = %+v", planned.Plans[0])
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusSuccess {
		t.Fatalf("summary = %+v", summary)
	}
	files, err := service.GetBookFiles(context.Background(), existingBook.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
}

func TestImportExecutorReusesExistingBookForColonDashTitleVariant(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	existingBook, err := service.CreateBook(context.Background(), library.Book{Title: "Ameritopia: The Unmaking of America", MediaType: library.MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	existingEdition, err := service.CreateEdition(context.Background(), library.Edition{BookID: existingBook.ID, Title: "Ameritopia: The Unmaking of America"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AttachContributor(context.Background(), existingEdition.ID, library.Contributor{Name: "Mark R. Levin", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AttachFile(context.Background(), library.BookFile{EditionID: existingEdition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: "/books/ameritopia.epub", OriginalPath: "/books/ameritopia.epub", ContentHash: "hash-ameritopia-epub"}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "Ameritopia-The Unmaking of America - Mark R. Levin.mobi")
	writeFile(t, path, "mobi-bytes")

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if planned.Plans[0].Disposition != DispositionAttachNewFormat {
		t.Fatalf("plan = %+v", planned.Plans[0])
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusSuccess || summary.Results[0].BookID != existingBook.ID {
		t.Fatalf("summary = %+v", summary)
	}
	books, err := service.ListBooks(context.Background(), library.ListBooksQuery{MediaType: library.MediaTypeEbook, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 {
		t.Fatalf("books = %+v", books)
	}
	files, err := service.GetBookFiles(context.Background(), existingBook.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
}

func TestImportExecutorReusesExistingBookForAuthorPunctuationVariant(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	existingBook, err := service.CreateBook(context.Background(), library.Book{Title: "Men in Black: How the Supreme Court is Destroying America", MediaType: library.MediaTypeEbook})
	if err != nil {
		t.Fatal(err)
	}
	existingEdition, err := service.CreateEdition(context.Background(), library.Edition{BookID: existingBook.ID, Title: "Men in Black: How the Supreme Court is Destroying America"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AttachContributor(context.Background(), existingEdition.ID, library.Contributor{Name: "Mark R. Levin", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AttachFile(context.Background(), library.BookFile{EditionID: existingEdition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: "/books/men-in-black.epub", OriginalPath: "/books/men-in-black.epub", ContentHash: "hash-men-in-black-epub"}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "Mark R Levin - Men in Black- How the Supreme Court is Destroying America.mobi")
	writeFile(t, path, "mobi-bytes")

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if planned.Plans[0].Disposition != DispositionAttachNewFormat {
		t.Fatalf("plan = %+v", planned.Plans[0])
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusSuccess || summary.Results[0].BookID != existingBook.ID {
		t.Fatalf("summary = %+v", summary)
	}
	books, err := service.ListBooks(context.Background(), library.ListBooksQuery{MediaType: library.MediaTypeEbook, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 {
		t.Fatalf("books = %+v", books)
	}
	files, err := service.GetBookFiles(context.Background(), existingBook.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
}

func TestImportExecutorExistingEdition(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	existingBook, _ := service.CreateBook(context.Background(), library.Book{Title: "The Guardian's Path", MediaType: library.MediaTypeEbook})
	existingEdition, _ := service.CreateEdition(context.Background(), library.Edition{BookID: existingBook.ID, Title: "The Guardian's Path"})
	if err := service.AttachContributor(context.Background(), existingEdition.ID, library.Contributor{Name: "Jane Doe", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "The Guardian's Path.pdf")
	writeFile(t, path, "%PDF-1.7 /Title (The Guardian's Path) /Author (Jane Doe)")

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if planned.Plans[0].Edition.Action != EditionActionReuse {
		t.Fatalf("plan = %+v", planned.Plans[0])
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].EditionID != existingEdition.ID {
		t.Fatalf("result = %+v", summary.Results[0])
	}
}

func TestImportExecutorDuplicateHash(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	book, _ := service.CreateBook(context.Background(), library.Book{Title: "Duplicate", MediaType: library.MediaTypeEbook})
	edition, _ := service.CreateEdition(context.Background(), library.Edition{BookID: book.ID, Title: "Duplicate"})
	if err := service.AttachContributor(context.Background(), edition.ID, library.Contributor{Name: "Jane Doe", Roles: []library.ContributorRole{library.RoleAuthor}}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "Jane Doe - Duplicate.mobi")
	writeFile(t, path, "same-content")
	hash := fileHashForTest(t, path)

	if _, err := service.AttachFile(context.Background(), library.BookFile{EditionID: edition.ID, MediaType: library.MediaTypeEbook, Format: "epub", Path: "/books/duplicate.epub", OriginalPath: "/books/duplicate.epub", ContentHash: hash}); err != nil {
		t.Fatal(err)
	}

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusDuplicate {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestImportExecutorSamePlanTwiceIsIdempotent(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	dir := t.TempDir()
	path := filepath.Join(dir, "Torrent Name.epub")
	writeEPUB(t, path, "The Guardian's Path", "Jane Doe")

	planner := NewImportPlanner(service)
	planned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeEbook},
		RootPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor := NewImportExecutor(service)

	first, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(context.Background(), ExecutionContext{}, planned.Plans)
	if err != nil {
		t.Fatal(err)
	}
	if first.Results[0].Status != ExecutionStatusSuccess || second.Results[0].Status != ExecutionStatusDuplicate {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	books, err := service.ListBooks(context.Background(), library.ListBooksQuery{MediaType: library.MediaTypeEbook, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 {
		t.Fatalf("books = %+v", books)
	}
	files, err := service.GetBookFiles(context.Background(), books[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %+v", files)
	}
}

func TestImportExecutorRollbackOnFailure(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	plan := ImportPlan{
		Candidate: ImportCandidate{
			Path:         "/incoming/broken.epub",
			RelativePath: "broken.epub",
			MediaType:    library.MediaTypeEbook,
			Metadata: CandidateMetadata{
				SelectedTitle:  "Broken Book",
				SelectedAuthor: "Jane Doe",
			},
		},
		Book: ResolvedBook{
			Action:   BookActionCreate,
			Proposed: &library.Book{Title: "Broken Book", MediaType: library.MediaTypeEbook},
		},
		Edition: ResolvedEdition{
			Action:   EditionActionCreate,
			Proposed: &library.Edition{Title: "Broken Book"},
		},
		Contributors: []ResolvedContributor{{
			Action:   ContributorActionCreate,
			Proposed: &library.Contributor{Name: "Jane Doe", Roles: []library.ContributorRole{library.RoleAuthor}},
			Role:     library.RoleAuthor,
		}},
		File: FileDecision{
			Action:   FileActionCreate,
			Proposed: &library.BookFile{Path: "/incoming/broken.epub"},
		},
		Disposition: DispositionCreateNewBook,
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, []ImportPlan{plan})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusRolledBack {
		t.Fatalf("summary = %+v", summary)
	}
	books, err := service.ListBooks(context.Background(), library.ListBooksQuery{MediaType: library.MediaTypeEbook, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 0 {
		t.Fatalf("books after rollback = %+v", books)
	}
}

func TestImportExecutorPartialFailure(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	validDir := t.TempDir()
	validPath := filepath.Join(validDir, "valid.epub")
	writeEPUB(t, validPath, "Valid Book", "Jane Doe")

	planner := NewImportPlanner(service)
	validPlanned, err := planner.Plan(context.Background(), PlanningContext{
		Source:   library.ImportSource{Name: "torrent", MediaType: library.MediaTypeEbook},
		RootPath: validPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	invalidPlan := ImportPlan{
		Candidate: ImportCandidate{
			Path:         "/incoming/invalid.epub",
			RelativePath: "invalid.epub",
			MediaType:    library.MediaTypeEbook,
			Metadata:     CandidateMetadata{SelectedTitle: "Invalid Book", SelectedAuthor: "Jane Doe"},
		},
		Book:        ResolvedBook{Action: BookActionCreate, Proposed: &library.Book{Title: "Invalid Book", MediaType: library.MediaTypeEbook}},
		Edition:     ResolvedEdition{Action: EditionActionCreate, Proposed: &library.Edition{Title: "Invalid Book"}},
		File:        FileDecision{Action: FileActionCreate, Proposed: &library.BookFile{}},
		Disposition: DispositionCreateNewBook,
	}

	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, []ImportPlan{validPlanned.Plans[0], invalidPlan})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusSuccess || summary.Results[1].Status != ExecutionStatusRolledBack {
		t.Fatalf("summary = %+v", summary)
	}
	books, err := service.ListBooks(context.Background(), library.ListBooksQuery{MediaType: library.MediaTypeEbook, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 || books[0].Title != "Valid Book" {
		t.Fatalf("books = %+v", books)
	}
}

func TestImportExecutorManualReviewSkipped(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()

	plan := ImportPlan{
		Candidate:   ImportCandidate{MediaType: library.MediaTypeEbook},
		Disposition: DispositionNeedsManualReview,
	}
	executor := NewImportExecutor(service)
	summary, err := executor.Execute(context.Background(), ExecutionContext{}, []ImportPlan{plan})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Results[0].Status != ExecutionStatusSkipped {
		t.Fatalf("summary = %+v", summary)
	}
}

func newNormalizedLibraryService(t *testing.T) (*library.LibraryService, func()) {
	t.Helper()
	database, err := librarrdb.New(filepath.Join(t.TempDir(), "executor.db"))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := library.NewNormalizedRepository(database.SQLDB())
	if err != nil {
		t.Fatal(err)
	}
	svc, err := library.NewLibraryService(library.ServiceOptions{
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
	return svc, func() { _ = database.Close() }
}
