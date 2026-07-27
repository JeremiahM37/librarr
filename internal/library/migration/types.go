package migration

import (
	"context"
	"time"

	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/models"
)

const Version = "librarr-2-backfill-v1"

type Options struct {
	DryRun          bool
	ResumeFromRunID int64
	StopAfterRows   int
}

type LegacyStore interface {
	ListLegacyItems(context.Context) ([]models.LibraryItem, error)
	CountLegacyItems(context.Context) (int, error)
}

type StateStore interface {
	StartRun(context.Context, Options) (int64, error)
	FinishRun(context.Context, int64, Report) error
	GetMapping(context.Context, int64) (*Mapping, error)
	SaveMapping(context.Context, int64, Mapping) error
	ValidationSnapshot(context.Context) (ValidationSnapshot, error)
}

type Repository interface {
	library.LibraryRepository
	library.EditionRepository
	library.TransactionManager
}

type Mapping struct {
	LegacyItemID int64
	BookID       int64
	EditionID    int64
	FileID       int64
	Status       string
	Reason       string
}

type Plan struct {
	LegacyRows int
	DryRun     bool
	Warnings   []string
}

type ValidationSnapshot struct {
	LegacyItems           int
	CompletedMappings     int
	Files                 int
	OrphanFiles           int
	DuplicateFilePaths    int
	DuplicateIdentifiers  int
	MissingMappedBooks    int
	MissingMappedEditions int
	MissingMappedFiles    int
}

type ValidationResult struct {
	OK       bool
	Errors   []string
	Warnings []string
	Snapshot ValidationSnapshot
}

type RowResult struct {
	LegacyItemID int64  `json:"legacy_item_id"`
	BookID       int64  `json:"book_id,omitempty"`
	EditionID    int64  `json:"edition_id,omitempty"`
	FileID       int64  `json:"file_id,omitempty"`
	Action       string `json:"action"`
	Reason       string `json:"reason,omitempty"`
	Error        string `json:"error,omitempty"`
}

type Report struct {
	RunID               int64            `json:"run_id"`
	Version             string           `json:"version"`
	DryRun              bool             `json:"dry_run"`
	StartedAt           time.Time        `json:"started_at"`
	FinishedAt          time.Time        `json:"finished_at"`
	Elapsed             time.Duration    `json:"elapsed"`
	LegacyRowsTotal     int              `json:"legacy_rows_total"`
	RowsProcessed       int              `json:"rows_processed"`
	RowsCompleted       int              `json:"rows_completed"`
	RowsSkipped         int              `json:"rows_skipped"`
	RowsFailed          int              `json:"rows_failed"`
	BooksCreated        int              `json:"books_created"`
	BooksReused         int              `json:"books_reused"`
	EditionsCreated     int              `json:"editions_created"`
	EditionsReused      int              `json:"editions_reused"`
	FilesCreated        int              `json:"files_created"`
	FilesReused         int              `json:"files_reused"`
	ContributorsCreated int              `json:"contributors_created"`
	ContributorsReused  int              `json:"contributors_reused"`
	IdentifiersCreated  int              `json:"identifiers_created"`
	CoversCreated       int              `json:"covers_created"`
	DuplicatesMerged    int              `json:"duplicates_merged"`
	Warnings            []string         `json:"warnings"`
	Errors              []string         `json:"errors"`
	Rows                []RowResult      `json:"rows"`
	Validation          ValidationResult `json:"validation"`
}
