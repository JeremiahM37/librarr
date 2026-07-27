package library

import (
	"context"
	"strings"

	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
)

type legacyStore interface {
	GetItems(mediaType string, limit, offset int) ([]models.LibraryItem, error)
	FindByTitle(title string) ([]models.LibraryItem, error)
	HasSourceID(sourceID string) bool
}

type legacyCountStore interface {
	CountItems(mediaType string) (int, error)
}

type legacyStatsStore interface {
	GetStats() (map[string]interface{}, error)
}

type legacyDeleteStore interface {
	DeleteItem(id int64) error
	DeleteItemBySourceID(sourceID string) error
}

type legacyImportStore interface {
	AddItem(*models.LibraryItem) (int64, error)
}

type LegacyLibraryRepository struct {
	store legacyStore
}

func NewLegacyLibraryRepository(database *db.DB) *LegacyLibraryRepository {
	return &LegacyLibraryRepository{store: database}
}

func NewLegacyLibraryRepositoryWithStore(store legacyStore) *LegacyLibraryRepository {
	return &LegacyLibraryRepository{store: store}
}

func (r *LegacyLibraryRepository) FindBook(ctx context.Context, query BookQuery) (*Book, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := r.store.FindByTitle(query.Title)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if query.MediaType != "" && MediaType(item.MediaType) != query.MediaType {
			continue
		}
		if query.Author != "" && !strings.EqualFold(strings.TrimSpace(item.Author), strings.TrimSpace(query.Author)) {
			continue
		}
		book := LegacyItemToBook(item)
		return &book, nil
	}
	return nil, ErrNotFound
}

func (r *LegacyLibraryRepository) FindBookByIdentifier(ctx context.Context, identifier Identifier) (*Book, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(identifier.Value) == "" || !r.store.HasSourceID(identifier.Value) {
		return nil, ErrNotFound
	}
	file, err := r.FindFileBySourceID(ctx, identifier.Value)
	if err != nil {
		return nil, err
	}
	book, err := r.GetBook(ctx, file.BookID)
	if err != nil {
		return nil, err
	}
	return book, nil
}

func (r *LegacyLibraryRepository) GetBook(ctx context.Context, id int64) (*Book, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := r.findLegacyItem(ctx, func(item models.LibraryItem) bool {
		return item.ID == id
	})
	if err != nil {
		return nil, err
	}
	book := LegacyItemToBook(item)
	return &book, nil
}

func (r *LegacyLibraryRepository) CreateBook(context.Context, Book) (*Book, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) UpdateBook(context.Context, Book) (*Book, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) ListBooks(ctx context.Context, query ListBooksQuery) ([]Book, error) {
	items, err := r.ListLegacyItems(ctx, string(query.MediaType), query.Limit, query.Offset)
	if err != nil {
		return nil, err
	}
	books := make([]Book, 0, len(items))
	for _, item := range items {
		books = append(books, LegacyItemToBook(item))
	}
	return books, nil
}

func (r *LegacyLibraryRepository) ListBookReadModels(ctx context.Context, query ListBooksQuery) ([]BookReadModel, error) {
	books, err := r.ListBooks(ctx, query)
	if err != nil {
		return nil, err
	}
	items := make([]BookReadModel, 0, len(books))
	for _, book := range books {
		bookCopy := book
		items = append(items, BookReadModel{Book: bookCopy})
	}
	return items, nil
}

func (r *LegacyLibraryRepository) CountListedBooks(ctx context.Context, query ListBooksQuery) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if strings.TrimSpace(query.Search) != "" || strings.TrimSpace(query.Format) != "" {
		books, err := r.ListBooks(ctx, ListBooksQuery{
			MediaType: query.MediaType,
			Search:    query.Search,
			Format:    query.Format,
			Limit:     100000,
		})
		if err != nil {
			return 0, err
		}
		return len(books), nil
	}
	return r.CountBooks(ctx, BookQuery{Title: query.Search, MediaType: query.MediaType})
}

func (r *LegacyLibraryRepository) GetLibrarySummary(ctx context.Context) (LibrarySummary, error) {
	if err := ctx.Err(); err != nil {
		return LibrarySummary{}, err
	}
	return LibrarySummary{}, ErrUnsupportedOperation
}

func (r *LegacyLibraryRepository) SearchBooks(ctx context.Context, query BookQuery) ([]Book, error) {
	if strings.TrimSpace(query.Title) == "" {
		return r.ListBooks(ctx, ListBooksQuery{MediaType: query.MediaType, Limit: 100000})
	}
	items, err := r.store.FindByTitle(query.Title)
	if err != nil {
		return nil, err
	}
	books := make([]Book, 0, len(items))
	for _, item := range items {
		if query.MediaType != "" && MediaType(item.MediaType) != query.MediaType {
			continue
		}
		if query.Author != "" && !strings.EqualFold(strings.TrimSpace(item.Author), strings.TrimSpace(query.Author)) {
			continue
		}
		books = append(books, LegacyItemToBook(item))
	}
	return books, nil
}

func (r *LegacyLibraryRepository) CountBooks(ctx context.Context, query BookQuery) (int, error) {
	books, err := r.SearchBooks(ctx, query)
	if err != nil {
		return 0, err
	}
	return len(books), nil
}

func (r *LegacyLibraryRepository) RecentBooks(ctx context.Context, query ListBooksQuery) ([]Book, error) {
	return r.ListBooks(ctx, query)
}

func (r *LegacyLibraryRepository) SaveBook(context.Context, Book) (*Book, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) DeleteBook(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, ok := r.store.(legacyDeleteStore)
	if !ok {
		return ErrUnsupportedOperation
	}
	return store.DeleteItem(id)
}

func (r *LegacyLibraryRepository) GetBookFiles(ctx context.Context, bookID int64) ([]BookFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := r.findLegacyItem(ctx, func(item models.LibraryItem) bool {
		return item.ID == bookID
	})
	if err != nil {
		return nil, err
	}
	return []BookFile{LegacyItemToFile(item)}, nil
}

func (r *LegacyLibraryRepository) GetFile(ctx context.Context, id int64) (*BookFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := r.findLegacyItem(ctx, func(item models.LibraryItem) bool {
		return item.ID == id
	})
	if err != nil {
		return nil, err
	}
	file := LegacyItemToFile(item)
	return &file, nil
}

func (r *LegacyLibraryRepository) FindFileByPath(ctx context.Context, path string) (*BookFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := r.findLegacyItem(ctx, func(item models.LibraryItem) bool {
		return item.FilePath == path || item.OriginalPath == path
	})
	if err != nil {
		return nil, err
	}
	file := LegacyItemToFile(item)
	return &file, nil
}

func (r *LegacyLibraryRepository) FindByPath(ctx context.Context, path string) (*BookFile, error) {
	return r.FindFileByPath(ctx, path)
}

func (r *LegacyLibraryRepository) FindFileBySourceID(ctx context.Context, sourceID string) (*BookFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := r.findLegacyItem(ctx, func(item models.LibraryItem) bool {
		return item.SourceID == sourceID
	})
	if err != nil {
		return nil, err
	}
	file := LegacyItemToFile(item)
	return &file, nil
}

func (r *LegacyLibraryRepository) FindFilesByContentHash(ctx context.Context, hash string) ([]BookFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := r.allLegacyItems()
	if err != nil {
		return nil, err
	}
	files := make([]BookFile, 0)
	for _, item := range items {
		if item.ContentHash == hash {
			files = append(files, LegacyItemToFile(item))
		}
	}
	if len(files) == 0 {
		return nil, ErrNotFound
	}
	return files, nil
}

func (r *LegacyLibraryRepository) ListFiles(ctx context.Context, editionID int64) ([]BookFile, error) {
	return r.GetBookFiles(ctx, editionID)
}

func (r *LegacyLibraryRepository) AttachFile(context.Context, BookFile) (*BookFile, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) DetachFile(context.Context, int64) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) MoveFile(context.Context, int64, string) (*BookFile, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) DeleteFile(context.Context, int64) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) ValidateManagedFile(ctx context.Context, fileID int64) error {
	_, err := r.GetFile(ctx, fileID)
	return err
}

func (r *LegacyLibraryRepository) GetSeries(context.Context, string) (*Series, error) {
	return nil, ErrNotFound
}

func (r *LegacyLibraryRepository) FindSeries(context.Context, string) ([]Series, error) {
	return []Series{}, nil
}

func (r *LegacyLibraryRepository) AttachBookToSeries(context.Context, int64, BookSeries) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) AttachBook(ctx context.Context, bookID int64, series BookSeries) error {
	return r.AttachBookToSeries(ctx, bookID, series)
}

func (r *LegacyLibraryRepository) DetachBook(context.Context, int64, int64) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) SeriesPosition(context.Context, int64, int64) (BookSeries, error) {
	return BookSeries{}, ErrNotFound
}

func (r *LegacyLibraryRepository) ListSeriesBooks(context.Context, int64) ([]Book, error) {
	return []Book{}, nil
}

func (r *LegacyLibraryRepository) MergeContributor(context.Context, Contributor) (*Contributor, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) GetEditionContributors(ctx context.Context, editionID int64) ([]Contributor, error) {
	book, err := r.GetBook(ctx, editionID)
	if err != nil {
		return nil, err
	}
	return book.Contributors, nil
}

func (r *LegacyLibraryRepository) AttachContributor(context.Context, int64, Contributor) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) DetachContributor(context.Context, int64, int64, ContributorRole) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) AddCover(context.Context, Cover) (*Cover, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) AttachCover(ctx context.Context, cover Cover) (*Cover, error) {
	return r.AddCover(ctx, cover)
}

func (r *LegacyLibraryRepository) ReplaceCover(context.Context, Cover) (*Cover, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) RemoveCover(context.Context, int64) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) GetPrimaryCover(context.Context, int64) (*Cover, error) {
	return nil, ErrNotFound
}

func (r *LegacyLibraryRepository) PrimaryCover(ctx context.Context, bookID int64) (*Cover, error) {
	return r.GetPrimaryCover(ctx, bookID)
}

func (r *LegacyLibraryRepository) AddIdentifier(context.Context, Identifier) (*Identifier, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) FindByIdentifier(ctx context.Context, identifier Identifier) ([]IdentifierMatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := r.FindFileBySourceID(ctx, identifier.Value)
	if err != nil {
		return nil, err
	}
	return []IdentifierMatch{{
		BookID:     file.BookID,
		EditionID:  file.EditionID,
		Identifier: identifier,
	}}, nil
}

func (r *LegacyLibraryRepository) SaveEmbeddedMetadata(context.Context, int64, map[string]string) error {
	return ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) GetBookMetadata(context.Context, int64) (*BookMetadata, error) {
	return nil, ErrUnsupportedOperation
}

func (r *LegacyLibraryRepository) GetBookProvenance(context.Context, int64) (*BookMetadataProvenance, error) {
	return nil, ErrUnsupportedOperation
}

func (r *LegacyLibraryRepository) PatchBookMetadata(context.Context, int64, BookMetadataPatch) (*BookMetadata, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) ApplyBookMetadataSource(context.Context, MetadataUpdate) (*BookMetadata, error) {
	return nil, ErrReadOnlyRepository
}

func (r *LegacyLibraryRepository) findLegacyItem(ctx context.Context, match func(models.LibraryItem) bool) (models.LibraryItem, error) {
	if err := ctx.Err(); err != nil {
		return models.LibraryItem{}, err
	}
	items, err := r.allLegacyItems()
	if err != nil {
		return models.LibraryItem{}, err
	}
	for _, item := range items {
		if match(item) {
			return item, nil
		}
	}
	return models.LibraryItem{}, ErrNotFound
}

func (r *LegacyLibraryRepository) allLegacyItems() ([]models.LibraryItem, error) {
	items, err := r.store.GetItems("", 100000, 0)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []models.LibraryItem{}, nil
	}
	return items, nil
}

func (r *LegacyLibraryRepository) ListLegacyItems(ctx context.Context, mediaType string, limit, offset int) ([]models.LibraryItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := r.store.GetItems(mediaType, limit, offset)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []models.LibraryItem{}, nil
	}
	return items, nil
}

func (r *LegacyLibraryRepository) CountLegacyItems(ctx context.Context, mediaType string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store, ok := r.store.(legacyCountStore)
	if !ok {
		items, err := r.store.GetItems(mediaType, 100000, 0)
		if err != nil {
			return 0, err
		}
		return len(items), nil
	}
	return store.CountItems(mediaType)
}

func (r *LegacyLibraryRepository) LegacyStats(ctx context.Context) (map[string]interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store, ok := r.store.(legacyStatsStore); ok {
		return store.GetStats()
	}
	ebookCount, _ := r.CountLegacyItems(ctx, "ebook")
	audiobookCount, _ := r.CountLegacyItems(ctx, "audiobook")
	mangaCount, _ := r.CountLegacyItems(ctx, "manga")
	totalCount, _ := r.CountLegacyItems(ctx, "")
	return map[string]interface{}{
		"total_items": totalCount,
		"ebooks":      ebookCount,
		"audiobooks":  audiobookCount,
		"manga":       mangaCount,
	}, nil
}

func (r *LegacyLibraryRepository) GetLegacyItem(ctx context.Context, id int64) (models.LibraryItem, error) {
	return r.findLegacyItem(ctx, func(item models.LibraryItem) bool {
		return item.ID == id
	})
}

func (r *LegacyLibraryRepository) ImportLegacyItem(ctx context.Context, item models.LibraryItem) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store, ok := r.store.(legacyImportStore)
	if !ok {
		return 0, ErrUnsupportedOperation
	}
	item.ID = 0
	return store.AddItem(&item)
}

func (r *LegacyLibraryRepository) DeleteLegacyItem(ctx context.Context, id int64) error {
	return r.DeleteBook(ctx, id)
}

func (r *LegacyLibraryRepository) DeleteLegacyItemBySourceID(ctx context.Context, sourceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, ok := r.store.(legacyDeleteStore)
	if !ok {
		return ErrUnsupportedOperation
	}
	return store.DeleteItemBySourceID(sourceID)
}

func (r *LegacyLibraryRepository) HasLegacySourceID(ctx context.Context, sourceID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return r.store.HasSourceID(sourceID), nil
}

var _ BookRepository = (*LegacyLibraryRepository)(nil)
var _ FileRepository = (*LegacyLibraryRepository)(nil)
var _ SeriesRepository = (*LegacyLibraryRepository)(nil)
var _ ContributorRepository = (*LegacyLibraryRepository)(nil)
var _ CoverRepository = (*LegacyLibraryRepository)(nil)
var _ IdentifierRepository = (*LegacyLibraryRepository)(nil)
