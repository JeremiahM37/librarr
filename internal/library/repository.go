package library

import "context"

type BookRepository interface {
	CreateBook(context.Context, Book) (*Book, error)
	UpdateBook(context.Context, Book) (*Book, error)
	FindBook(context.Context, BookQuery) (*Book, error)
	FindBookByIdentifier(context.Context, Identifier) (*Book, error)
	GetBook(context.Context, int64) (*Book, error)
	ListBooks(context.Context, ListBooksQuery) ([]Book, error)
	ListBookReadModels(context.Context, ListBooksQuery) ([]BookReadModel, error)
	CountListedBooks(context.Context, ListBooksQuery) (int, error)
	GetLibrarySummary(context.Context) (LibrarySummary, error)
	SearchBooks(context.Context, BookQuery) ([]Book, error)
	CountBooks(context.Context, BookQuery) (int, error)
	RecentBooks(context.Context, ListBooksQuery) ([]Book, error)
	SaveBook(context.Context, Book) (*Book, error)
	DeleteBook(context.Context, int64) error
}

type FileRepository interface {
	GetBookFiles(context.Context, int64) ([]BookFile, error)
	GetFile(context.Context, int64) (*BookFile, error)
	FindFileByPath(context.Context, string) (*BookFile, error)
	FindByPath(context.Context, string) (*BookFile, error)
	FindFileBySourceID(context.Context, string) (*BookFile, error)
	FindFilesByContentHash(context.Context, string) ([]BookFile, error)
	ListFiles(context.Context, int64) ([]BookFile, error)
	AttachFile(context.Context, BookFile) (*BookFile, error)
	DetachFile(context.Context, int64) error
	MoveFile(context.Context, int64, string) (*BookFile, error)
	DeleteFile(context.Context, int64) error
	ValidateManagedFile(context.Context, int64) error
}

type SeriesRepository interface {
	GetSeries(context.Context, string) (*Series, error)
	FindSeries(context.Context, string) ([]Series, error)
	AttachBookToSeries(context.Context, int64, BookSeries) error
	AttachBook(context.Context, int64, BookSeries) error
	DetachBook(context.Context, int64, int64) error
	SeriesPosition(context.Context, int64, int64) (BookSeries, error)
	ListSeriesBooks(context.Context, int64) ([]Book, error)
}

type ContributorRepository interface {
	MergeContributor(context.Context, Contributor) (*Contributor, error)
	GetEditionContributors(context.Context, int64) ([]Contributor, error)
	AttachContributor(context.Context, int64, Contributor) error
	DetachContributor(context.Context, int64, int64, ContributorRole) error
}

type MetadataRepository interface {
	SaveEmbeddedMetadata(context.Context, int64, map[string]string) error
	GetBookMetadata(context.Context, int64) (*BookMetadata, error)
	GetBookProvenance(context.Context, int64) (*BookMetadataProvenance, error)
	PatchBookMetadata(context.Context, int64, BookMetadataPatch) (*BookMetadata, error)
	ApplyBookMetadataSource(context.Context, MetadataUpdate) (*BookMetadata, error)
}

type CoverRepository interface {
	AddCover(context.Context, Cover) (*Cover, error)
	AttachCover(context.Context, Cover) (*Cover, error)
	ReplaceCover(context.Context, Cover) (*Cover, error)
	RemoveCover(context.Context, int64) error
	GetPrimaryCover(context.Context, int64) (*Cover, error)
	PrimaryCover(context.Context, int64) (*Cover, error)
}

type IdentifierRepository interface {
	AddIdentifier(context.Context, Identifier) (*Identifier, error)
	FindByIdentifier(context.Context, Identifier) ([]IdentifierMatch, error)
}

type LibraryRepository interface {
	BookRepository
	FileRepository
	SeriesRepository
	ContributorRepository
	CoverRepository
	IdentifierRepository
	MetadataRepository
}

type IdentifierMatch struct {
	BookID     int64
	EditionID  int64
	Identifier Identifier
}

type ListBooksQuery struct {
	MediaType MediaType
	Search    string
	Format    string
	Sort      string
	Order     string
	Limit     int
	Offset    int
}

type BookReadModel struct {
	Book          Book
	PrimaryAuthor *Contributor
	Contributors  []Contributor
	Identifiers   []Identifier
	Series        []BookSeries
	Formats       []string
	EditionCount  int
	FileCount     int
	LocalCover    *Cover
}

type LibrarySummary struct {
	TotalBooks       int
	TotalEditions    int
	TotalFiles       int
	EbookCount       int
	AudiobookCount   int
	MangaCount       int
	RecentAddedCount int
	FormatCounts     map[string]int
}

type EditionRepository interface {
	CreateEdition(context.Context, Edition) (*Edition, error)
	GetEdition(context.Context, int64) (*Edition, error)
	FindEdition(context.Context, int64, string) (*Edition, error)
	ListBookEditions(context.Context, int64) ([]Edition, error)
	UpdateEdition(context.Context, Edition) (*Edition, error)
	DeleteEdition(context.Context, int64) error
}
