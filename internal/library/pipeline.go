package library

import "context"

type ImportSource struct {
	Name       string
	SourceID   string
	SourceType string
	MediaType  MediaType
}

type ImportCandidate struct {
	Source       ImportSource
	Path         string
	RelativePath string
	Format       string
	Size         int64
	Hash         string
}

type ExtractedMetadata struct {
	Title       string
	Author      string
	Language    string
	Identifiers []Identifier
	Embedded    map[string]string
	Evidence    []Evidence
}

type DiscoveredFile struct {
	Candidate ImportCandidate
	Metadata  ExtractedMetadata
}

type ImportPlan struct {
	Book     Book
	Edition  Edition
	File     BookFile
	Evidence []Evidence
}

type ImportResult struct {
	BookID    int64
	EditionID int64
	FileID    int64
	Inserted  bool
	Reason    string
}

type FileDiscoverer interface {
	Discover(context.Context, ImportSource, string) ([]ImportCandidate, error)
}

type MetadataExtractor interface {
	ExtractMetadata(context.Context, ImportCandidate) (ExtractedMetadata, error)
}

type FileOrganizer interface {
	Organize(context.Context, ImportPlan) (BookFile, error)
}

type ImportNotifier interface {
	NotifyImport(context.Context, ImportResult) error
}

type ImportPipeline interface {
	Discover(context.Context, ImportSource, string) ([]ImportCandidate, error)
	ExtractMetadata(context.Context, ImportCandidate) (ExtractedMetadata, error)
	MatchBook(context.Context, DiscoveredFile) (MatchResult[Book], error)
	MatchEdition(context.Context, Book, DiscoveredFile) (MatchResult[Edition], error)
	Organize(context.Context, ImportPlan) (BookFile, error)
	PersistFile(context.Context, BookFile) (ImportResult, error)
	EnrichMetadata(context.Context, ImportResult) error
	Notify(context.Context, ImportResult) error
}
