package library

import (
	"context"
	"errors"
	"fmt"

	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
)

type TransactionManager interface {
	WithinTransaction(context.Context, func(context.Context) error) error
}

type NoopTransactionManager struct{}

func (NoopTransactionManager) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx)
}

type LegacyCompatibilityRepository interface {
	ListLegacyItems(context.Context, string, int, int) ([]models.LibraryItem, error)
	CountLegacyItems(context.Context, string) (int, error)
	LegacyStats(context.Context) (map[string]interface{}, error)
	GetLegacyItem(context.Context, int64) (models.LibraryItem, error)
	ImportLegacyItem(context.Context, models.LibraryItem) (int64, error)
	DeleteLegacyItem(context.Context, int64) error
	DeleteLegacyItemBySourceID(context.Context, string) error
	HasLegacySourceID(context.Context, string) (bool, error)
}

type ServiceOptions struct {
	BookRepository        BookRepository
	EditionRepository     EditionRepository
	FileRepository        FileRepository
	MetadataRepository    MetadataRepository
	SeriesRepository      SeriesRepository
	ContributorRepository ContributorRepository
	IdentifierRepository  IdentifierRepository
	CoverRepository       CoverRepository
	MatchingService       MatchingService
	FileService           FileService
	TransactionManager    TransactionManager
	LegacyCompatibility   LegacyCompatibilityRepository
}

type LibraryService struct {
	books        BookRepository
	editions     EditionRepository
	files        FileRepository
	metadata     MetadataRepository
	metadataOps  *MetadataEngine
	series       SeriesRepository
	contributors ContributorRepository
	identifiers  IdentifierRepository
	covers       CoverRepository
	matcher      MatchingService
	fileService  FileService
	transactions TransactionManager
	legacy       LegacyCompatibilityRepository
}

func NewLibraryService(opts ServiceOptions) (*LibraryService, error) {
	if opts.BookRepository == nil {
		return nil, fmt.Errorf("book repository is required")
	}
	if opts.FileRepository == nil {
		return nil, fmt.Errorf("file repository is required")
	}
	if opts.SeriesRepository == nil {
		return nil, fmt.Errorf("series repository is required")
	}
	if opts.MetadataRepository == nil {
		return nil, fmt.Errorf("metadata repository is required")
	}
	if opts.ContributorRepository == nil {
		return nil, fmt.Errorf("contributor repository is required")
	}
	if opts.IdentifierRepository == nil {
		return nil, fmt.Errorf("identifier repository is required")
	}
	if opts.CoverRepository == nil {
		return nil, fmt.Errorf("cover repository is required")
	}
	if opts.TransactionManager == nil {
		opts.TransactionManager = NoopTransactionManager{}
	}
	metadataOps, err := NewMetadataEngine(opts.MetadataRepository)
	if err != nil {
		return nil, err
	}
	return &LibraryService{
		books:        opts.BookRepository,
		editions:     opts.EditionRepository,
		files:        opts.FileRepository,
		metadata:     opts.MetadataRepository,
		metadataOps:  metadataOps,
		series:       opts.SeriesRepository,
		contributors: opts.ContributorRepository,
		identifiers:  opts.IdentifierRepository,
		covers:       opts.CoverRepository,
		matcher:      opts.MatchingService,
		fileService:  opts.FileService,
		transactions: opts.TransactionManager,
		legacy:       opts.LegacyCompatibility,
	}, nil
}

func NewLegacyLibraryService(database *db.DB) (*LibraryService, error) {
	repo := NewLegacyLibraryRepository(database)
	return NewLibraryService(ServiceOptions{
		BookRepository:        repo,
		FileRepository:        repo,
		MetadataRepository:    repo,
		SeriesRepository:      repo,
		ContributorRepository: repo,
		IdentifierRepository:  repo,
		CoverRepository:       repo,
		LegacyCompatibility:   repo,
	})
}

func (s *LibraryService) FindBook(ctx context.Context, query BookQuery) (*Book, error) {
	book, err := s.books.FindBook(ctx, query)
	return book, translateLibraryError(err)
}

func (s *LibraryService) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	if s.transactions == nil {
		return NoopTransactionManager{}.WithinTransaction(ctx, fn)
	}
	return s.transactions.WithinTransaction(ctx, fn)
}

func (s *LibraryService) CreateBook(ctx context.Context, book Book) (*Book, error) {
	created, err := s.books.CreateBook(ctx, book)
	return created, translateLibraryError(err)
}

func (s *LibraryService) SaveBook(ctx context.Context, book Book) (*Book, error) {
	saved, err := s.books.SaveBook(ctx, book)
	return saved, translateLibraryError(err)
}

func (s *LibraryService) FindBookByID(ctx context.Context, id int64) (*Book, error) {
	book, err := s.books.GetBook(ctx, id)
	return book, translateLibraryError(err)
}

func (s *LibraryService) GetBook(ctx context.Context, id int64) (*Book, error) {
	return s.FindBookByID(ctx, id)
}

func (s *LibraryService) FindBookByIdentifier(ctx context.Context, identifier Identifier) (*Book, error) {
	if err := identifier.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidIdentifier, err)
	}
	book, err := s.books.FindBookByIdentifier(ctx, identifier)
	return book, translateLibraryError(err)
}

func (s *LibraryService) SearchBooks(ctx context.Context, query BookQuery) ([]Book, error) {
	books, err := s.books.SearchBooks(ctx, query)
	return books, translateLibraryError(err)
}

func (s *LibraryService) ListBooks(ctx context.Context, query ListBooksQuery) ([]Book, error) {
	books, err := s.books.ListBooks(ctx, query)
	return books, translateLibraryError(err)
}

func (s *LibraryService) ListBookReadModels(ctx context.Context, query ListBooksQuery) ([]BookReadModel, error) {
	items, err := s.books.ListBookReadModels(ctx, query)
	return items, translateLibraryError(err)
}

func (s *LibraryService) CountListedBooks(ctx context.Context, query ListBooksQuery) (int, error) {
	count, err := s.books.CountListedBooks(ctx, query)
	return count, translateLibraryError(err)
}

func (s *LibraryService) GetLibrarySummary(ctx context.Context) (LibrarySummary, error) {
	summary, err := s.books.GetLibrarySummary(ctx)
	return summary, translateLibraryError(err)
}

func (s *LibraryService) ListRecent(ctx context.Context, mediaType MediaType, limit, offset int) ([]Book, error) {
	books, err := s.books.RecentBooks(ctx, ListBooksQuery{MediaType: mediaType, Limit: limit, Offset: offset})
	return books, translateLibraryError(err)
}

func (s *LibraryService) GetBookFiles(ctx context.Context, bookID int64) ([]BookFile, error) {
	files, err := s.files.GetBookFiles(ctx, bookID)
	return files, translateLibraryError(err)
}

func (s *LibraryService) GetFile(ctx context.Context, id int64) (*BookFile, error) {
	file, err := s.files.GetFile(ctx, id)
	return file, translateLibraryError(err)
}

func (s *LibraryService) FindFileByPath(ctx context.Context, path string) (*BookFile, error) {
	file, err := s.files.FindFileByPath(ctx, path)
	return file, translateLibraryError(err)
}

func (s *LibraryService) FindFileBySourceID(ctx context.Context, sourceID string) (*BookFile, error) {
	file, err := s.files.FindFileBySourceID(ctx, sourceID)
	return file, translateLibraryError(err)
}

func (s *LibraryService) FindFilesByContentHash(ctx context.Context, hash string) ([]BookFile, error) {
	files, err := s.files.FindFilesByContentHash(ctx, hash)
	return files, translateLibraryError(err)
}

func (s *LibraryService) MoveFile(ctx context.Context, fileID int64, path string) (*BookFile, error) {
	file, err := s.files.MoveFile(ctx, fileID, path)
	return file, translateLibraryError(err)
}

func (s *LibraryService) CreateEdition(ctx context.Context, edition Edition) (*Edition, error) {
	if s.editions == nil {
		return nil, ErrUnsupportedOperation
	}
	created, err := s.editions.CreateEdition(ctx, edition)
	return created, translateLibraryError(err)
}

func (s *LibraryService) GetEdition(ctx context.Context, id int64) (*Edition, error) {
	if s.editions == nil {
		return nil, ErrUnsupportedOperation
	}
	edition, err := s.editions.GetEdition(ctx, id)
	return edition, translateLibraryError(err)
}

func (s *LibraryService) UpdateEdition(ctx context.Context, edition Edition) (*Edition, error) {
	if s.editions == nil {
		return nil, ErrUnsupportedOperation
	}
	updated, err := s.editions.UpdateEdition(ctx, edition)
	return updated, translateLibraryError(err)
}

func (s *LibraryService) GetPrimaryCover(ctx context.Context, bookID int64) (*Cover, error) {
	cover, err := s.covers.GetPrimaryCover(ctx, bookID)
	return cover, translateLibraryError(err)
}

func (s *LibraryService) AttachCover(ctx context.Context, cover Cover) (*Cover, error) {
	attached, err := s.covers.AttachCover(ctx, cover)
	return attached, translateLibraryError(err)
}

func (s *LibraryService) FindEdition(ctx context.Context, bookID int64, title string) (*Edition, error) {
	if s.editions == nil {
		return nil, ErrUnsupportedOperation
	}
	edition, err := s.editions.FindEdition(ctx, bookID, title)
	return edition, translateLibraryError(err)
}

func (s *LibraryService) ListBookEditions(ctx context.Context, bookID int64) ([]Edition, error) {
	if s.editions == nil {
		return nil, ErrUnsupportedOperation
	}
	editions, err := s.editions.ListBookEditions(ctx, bookID)
	return editions, translateLibraryError(err)
}

func (s *LibraryService) GetSeries(ctx context.Context, name string) (*Series, error) {
	series, err := s.series.GetSeries(ctx, name)
	return series, translateLibraryError(err)
}

func (s *LibraryService) AttachBookToSeries(ctx context.Context, bookID int64, series BookSeries) error {
	return translateLibraryError(s.series.AttachBookToSeries(ctx, bookID, series))
}

func (s *LibraryService) MergeContributor(ctx context.Context, contributor Contributor) (*Contributor, error) {
	merged, err := s.contributors.MergeContributor(ctx, contributor)
	return merged, translateLibraryError(err)
}

func (s *LibraryService) GetEditionContributors(ctx context.Context, editionID int64) ([]Contributor, error) {
	contributors, err := s.contributors.GetEditionContributors(ctx, editionID)
	return contributors, translateLibraryError(err)
}

func (s *LibraryService) AttachContributor(ctx context.Context, editionID int64, contributor Contributor) error {
	return translateLibraryError(s.contributors.AttachContributor(ctx, editionID, contributor))
}

func (s *LibraryService) DetachContributor(ctx context.Context, editionID, contributorID int64, role ContributorRole) error {
	return translateLibraryError(s.contributors.DetachContributor(ctx, editionID, contributorID, role))
}

func (s *LibraryService) AddIdentifier(ctx context.Context, identifier Identifier) (*Identifier, error) {
	added, err := s.identifiers.AddIdentifier(ctx, identifier)
	return added, translateLibraryError(err)
}

func (s *LibraryService) FindIdentifierMatches(ctx context.Context, identifier Identifier) ([]IdentifierMatch, error) {
	matches, err := s.identifiers.FindByIdentifier(ctx, identifier)
	return matches, translateLibraryError(err)
}

func (s *LibraryService) SaveEmbeddedMetadata(ctx context.Context, fileID int64, metadata map[string]string) error {
	return translateLibraryError(s.metadata.SaveEmbeddedMetadata(ctx, fileID, metadata))
}

func (s *LibraryService) GetBookMetadata(ctx context.Context, bookID int64) (*BookMetadata, error) {
	if s.metadataOps == nil {
		return nil, ErrUnsupportedOperation
	}
	metadata, err := s.metadataOps.GetBookMetadata(ctx, bookID)
	return metadata, translateLibraryError(err)
}

func (s *LibraryService) GetBookProvenance(ctx context.Context, bookID int64) (*BookMetadataProvenance, error) {
	if s.metadataOps == nil {
		return nil, ErrUnsupportedOperation
	}
	provenance, err := s.metadataOps.GetBookProvenance(ctx, bookID)
	return provenance, translateLibraryError(err)
}

func (s *LibraryService) PatchBookMetadata(ctx context.Context, bookID int64, patch BookMetadataPatch) (*BookMetadata, error) {
	if s.metadataOps == nil {
		return nil, ErrUnsupportedOperation
	}
	metadata, err := s.metadataOps.PatchBookMetadata(ctx, bookID, patch)
	return metadata, translateLibraryError(err)
}

func (s *LibraryService) ApplyBookMetadataSource(ctx context.Context, update MetadataUpdate) (*BookMetadata, error) {
	if s.metadataOps == nil {
		return nil, ErrUnsupportedOperation
	}
	metadata, err := s.metadataOps.ApplyBookMetadataSource(ctx, update)
	return metadata, translateLibraryError(err)
}

func (s *LibraryService) DeleteBook(ctx context.Context, id int64) error {
	return translateLibraryError(s.books.DeleteBook(ctx, id))
}

type bookMerger interface {
	MergeBooks(context.Context, int64, int64) (*Book, error)
}

func (s *LibraryService) MergeBooks(ctx context.Context, sourceID, targetID int64) (*Book, error) {
	if sourceID == 0 || targetID == 0 {
		return nil, fmt.Errorf("%w: source and target book ids are required", ErrInvalidDomainObject)
	}
	if sourceID == targetID {
		return nil, fmt.Errorf("%w: source and target book ids must be different", ErrInvalidDomainObject)
	}
	merger, ok := s.books.(bookMerger)
	if !ok {
		return nil, ErrUnsupportedOperation
	}
	merged, err := merger.MergeBooks(ctx, sourceID, targetID)
	return merged, translateLibraryError(err)
}

func (s *LibraryService) RefreshBook(context.Context, int64) error {
	return ErrUnsupportedOperation
}

func (s *LibraryService) AttachFile(ctx context.Context, file BookFile) (*BookFile, error) {
	attached, err := s.files.AttachFile(ctx, file)
	return attached, translateLibraryError(err)
}

func (s *LibraryService) DetachFile(ctx context.Context, fileID int64) error {
	return translateLibraryError(s.files.DetachFile(ctx, fileID))
}

func (s *LibraryService) ImportCandidate(context.Context, ImportCandidate) (ImportResult, error) {
	return ImportResult{}, ErrUnsupportedOperation
}

func (s *LibraryService) ListLegacyItems(ctx context.Context, mediaType string, limit, offset int) ([]models.LibraryItem, error) {
	if s.legacy == nil {
		return nil, ErrUnsupportedOperation
	}
	items, err := s.legacy.ListLegacyItems(ctx, mediaType, limit, offset)
	return items, translateLibraryError(err)
}

func (s *LibraryService) CountLegacyItems(ctx context.Context, mediaType string) (int, error) {
	if s.legacy == nil {
		return 0, ErrUnsupportedOperation
	}
	count, err := s.legacy.CountLegacyItems(ctx, mediaType)
	return count, translateLibraryError(err)
}

func (s *LibraryService) LegacyStats(ctx context.Context) (map[string]interface{}, error) {
	if s.legacy == nil {
		return nil, ErrUnsupportedOperation
	}
	stats, err := s.legacy.LegacyStats(ctx)
	return stats, translateLibraryError(err)
}

func (s *LibraryService) GetLegacyItem(ctx context.Context, id int64) (models.LibraryItem, error) {
	if s.legacy == nil {
		return models.LibraryItem{}, ErrUnsupportedOperation
	}
	item, err := s.legacy.GetLegacyItem(ctx, id)
	return item, translateLibraryError(err)
}

func (s *LibraryService) ImportLegacyItem(ctx context.Context, item models.LibraryItem) (int64, error) {
	if s.legacy == nil {
		return 0, ErrUnsupportedOperation
	}
	id, err := s.legacy.ImportLegacyItem(ctx, item)
	return id, translateLibraryError(err)
}

func (s *LibraryService) DeleteLegacyItem(ctx context.Context, id int64) error {
	if s.legacy == nil {
		return ErrUnsupportedOperation
	}
	return translateLibraryError(s.legacy.DeleteLegacyItem(ctx, id))
}

func (s *LibraryService) DeleteLegacyItemBySourceID(ctx context.Context, sourceID string) error {
	if s.legacy == nil {
		return ErrUnsupportedOperation
	}
	return translateLibraryError(s.legacy.DeleteLegacyItemBySourceID(ctx, sourceID))
}

func (s *LibraryService) HasLegacySourceID(ctx context.Context, sourceID string) (bool, error) {
	if s.legacy == nil {
		return false, ErrUnsupportedOperation
	}
	ok, err := s.legacy.HasLegacySourceID(ctx, sourceID)
	return ok, translateLibraryError(err)
}

func translateLibraryError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return ErrBookNotFound
	}
	if errors.Is(err, ErrReadOnlyRepository) {
		return ErrRepositoryReadOnly
	}
	return err
}
