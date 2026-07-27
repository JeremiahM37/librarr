package libraryimport

import (
	"context"
	"time"

	"github.com/jamie75/librarr/internal/library"
)

type RepositoryWriter interface {
	WithinTransaction(context.Context, func(context.Context) error) error
	FindBookByID(context.Context, int64) (*library.Book, error)
	SearchBooks(context.Context, library.BookQuery) ([]library.Book, error)
	FindBookByIdentifier(context.Context, library.Identifier) (*library.Book, error)
	FindIdentifierMatches(context.Context, library.Identifier) ([]library.IdentifierMatch, error)
	CreateBook(context.Context, library.Book) (*library.Book, error)
	GetEdition(context.Context, int64) (*library.Edition, error)
	FindEdition(context.Context, int64, string) (*library.Edition, error)
	CreateEdition(context.Context, library.Edition) (*library.Edition, error)
	GetEditionContributors(context.Context, int64) ([]library.Contributor, error)
	MergeContributor(context.Context, library.Contributor) (*library.Contributor, error)
	AttachContributor(context.Context, int64, library.Contributor) error
	GetBookFiles(context.Context, int64) ([]library.BookFile, error)
	FindFileByPath(context.Context, string) (*library.BookFile, error)
	FindFileBySourceID(context.Context, string) (*library.BookFile, error)
	FindFilesByContentHash(context.Context, string) ([]library.BookFile, error)
	AttachFile(context.Context, library.BookFile) (*library.BookFile, error)
	AddIdentifier(context.Context, library.Identifier) (*library.Identifier, error)
	SaveEmbeddedMetadata(context.Context, int64, map[string]string) error
}

type ExecutionContext struct {
	Now time.Time `json:"now,omitempty"`
}

type ExecutionStatus string

const (
	ExecutionStatusSuccess    ExecutionStatus = "success"
	ExecutionStatusSkipped    ExecutionStatus = "skipped"
	ExecutionStatusConflict   ExecutionStatus = "conflict"
	ExecutionStatusDuplicate  ExecutionStatus = "duplicate"
	ExecutionStatusRolledBack ExecutionStatus = "rolled_back"
)

type ExecutionError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

func (e *ExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Stage == "" {
		return e.Message
	}
	return e.Stage + ": " + e.Message
}

type ExecutionTransaction struct {
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	RolledBack bool      `json:"rolled_back,omitempty"`
}

type ExecutionResult struct {
	Plan        ImportPlan           `json:"plan"`
	Status      ExecutionStatus      `json:"status"`
	Reason      string               `json:"reason,omitempty"`
	BookID      int64                `json:"book_id,omitempty"`
	EditionID   int64                `json:"edition_id,omitempty"`
	FileID      int64                `json:"file_id,omitempty"`
	Transaction ExecutionTransaction `json:"transaction,omitempty"`
	Error       *ExecutionError      `json:"error,omitempty"`
}

type ExecutionSummary struct {
	Results []ExecutionResult `json:"results"`
	Counts  map[string]int    `json:"counts,omitempty"`
}
