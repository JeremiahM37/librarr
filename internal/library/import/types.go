package libraryimport

import (
	"context"
	"time"

	"github.com/jamie75/librarr/internal/library"
)

type PlanningContext struct {
	Source           library.ImportSource `json:"source"`
	RootPath         string               `json:"root_path"`
	OriginalPath     string               `json:"original_path,omitempty"`
	TitleHint        string               `json:"title_hint,omitempty"`
	AuthorHint       string               `json:"author_hint,omitempty"`
	MetadataOverride CandidateMetadata    `json:"metadata_override,omitempty"`
	Now              time.Time            `json:"now,omitempty"`
}

type PlanningEvidence struct {
	Signal      string             `json:"signal"`
	Value       string             `json:"value,omitempty"`
	Source      string             `json:"source,omitempty"`
	Confidence  library.Confidence `json:"confidence,omitempty"`
	Explanation string             `json:"explanation,omitempty"`
}

type CandidateMetadata struct {
	EmbeddedTitle   string               `json:"embedded_title,omitempty"`
	EmbeddedAuthor  string               `json:"embedded_author,omitempty"`
	FilenameTitle   string               `json:"filename_title,omitempty"`
	FilenameAuthor  string               `json:"filename_author,omitempty"`
	SelectedTitle   string               `json:"selected_title,omitempty"`
	SelectedAuthor  string               `json:"selected_author,omitempty"`
	Subtitle        string               `json:"subtitle,omitempty"`
	Series          string               `json:"series,omitempty"`
	SeriesNumber    string               `json:"series_number,omitempty"`
	Publisher       string               `json:"publisher,omitempty"`
	PublicationYear string               `json:"publication_year,omitempty"`
	ISBN            string               `json:"isbn,omitempty"`
	Language        string               `json:"language,omitempty"`
	Description     string               `json:"description,omitempty"`
	Tags            []string             `json:"tags,omitempty"`
	Library         string               `json:"library,omitempty"`
	Identifiers     []library.Identifier `json:"identifiers,omitempty"`
}

type ImportCandidate struct {
	Path             string             `json:"path"`
	RelativePath     string             `json:"relative_path"`
	OriginalPath     string             `json:"original_path,omitempty"`
	TitleHint        string             `json:"title_hint,omitempty"`
	AuthorHint       string             `json:"author_hint,omitempty"`
	MetadataOverride CandidateMetadata  `json:"metadata_override,omitempty"`
	MediaType        library.MediaType  `json:"media_type"`
	Format           string             `json:"format"`
	Size             int64              `json:"size"`
	ContentHash      string             `json:"content_hash,omitempty"`
	IsDirectory      bool               `json:"is_directory,omitempty"`
	Metadata         CandidateMetadata  `json:"metadata"`
	Evidence         []PlanningEvidence `json:"evidence,omitempty"`
}

type BookAction string

const (
	BookActionCreate            BookAction = "create"
	BookActionReuse             BookAction = "reuse"
	BookActionConflict          BookAction = "conflict"
	BookActionNeedsManualReview BookAction = "needs_manual_review"
)

type EditionAction string

const (
	EditionActionCreate            EditionAction = "create"
	EditionActionReuse             EditionAction = "reuse"
	EditionActionConflict          EditionAction = "conflict"
	EditionActionNeedsManualReview EditionAction = "needs_manual_review"
)

type ContributorAction string

const (
	ContributorActionCreate ContributorAction = "create"
	ContributorActionReuse  ContributorAction = "reuse"
)

type FileAction string

const (
	FileActionCreate            FileAction = "create"
	FileActionAttachNewFormat   FileAction = "attach_new_format"
	FileActionIgnoreDuplicate   FileAction = "ignore_duplicate_file"
	FileActionConflict          FileAction = "conflict"
	FileActionNeedsManualReview FileAction = "needs_manual_review"
)

type PlanDisposition string

const (
	DispositionCreateNewBook     PlanDisposition = "create_new_book"
	DispositionCreateEdition     PlanDisposition = "create_edition"
	DispositionAttachNewFormat   PlanDisposition = "attach_new_format"
	DispositionIgnoreDuplicate   PlanDisposition = "ignore_duplicate_file"
	DispositionConflict          PlanDisposition = "conflict"
	DispositionNeedsManualReview PlanDisposition = "needs_manual_review"
)

type ResolvedBook struct {
	Action     BookAction         `json:"action"`
	Existing   *library.Book      `json:"existing,omitempty"`
	Proposed   *library.Book      `json:"proposed,omitempty"`
	Confidence library.Confidence `json:"confidence,omitempty"`
	Evidence   []PlanningEvidence `json:"evidence,omitempty"`
}

type ResolvedEdition struct {
	Action     EditionAction      `json:"action"`
	Existing   *library.Edition   `json:"existing,omitempty"`
	Proposed   *library.Edition   `json:"proposed,omitempty"`
	Confidence library.Confidence `json:"confidence,omitempty"`
	Evidence   []PlanningEvidence `json:"evidence,omitempty"`
}

type ResolvedContributor struct {
	Action     ContributorAction       `json:"action"`
	Existing   *library.Contributor    `json:"existing,omitempty"`
	Proposed   *library.Contributor    `json:"proposed,omitempty"`
	Role       library.ContributorRole `json:"role"`
	Confidence library.Confidence      `json:"confidence,omitempty"`
	Evidence   []PlanningEvidence      `json:"evidence,omitempty"`
}

type FileDecision struct {
	Action     FileAction         `json:"action"`
	Existing   *library.BookFile  `json:"existing,omitempty"`
	Proposed   *library.BookFile  `json:"proposed,omitempty"`
	Confidence library.Confidence `json:"confidence,omitempty"`
	Evidence   []PlanningEvidence `json:"evidence,omitempty"`
}

type ImportPlan struct {
	Candidate    ImportCandidate       `json:"candidate"`
	Book         ResolvedBook          `json:"book"`
	Edition      ResolvedEdition       `json:"edition"`
	Contributors []ResolvedContributor `json:"contributors,omitempty"`
	File         FileDecision          `json:"file"`
	Disposition  PlanDisposition       `json:"disposition"`
	Evidence     []PlanningEvidence    `json:"evidence,omitempty"`
}

type ImportResult struct {
	Plans  []ImportPlan   `json:"plans"`
	Counts map[string]int `json:"counts,omitempty"`
}

type Catalog interface {
	FindBookByIdentifier(context.Context, library.Identifier) (*library.Book, error)
	SearchBooks(context.Context, library.BookQuery) ([]library.Book, error)
	GetBook(context.Context, int64) (*library.Book, error)
	FindEdition(context.Context, int64, string) (*library.Edition, error)
	ListBookEditions(context.Context, int64) ([]library.Edition, error)
	GetEdition(context.Context, int64) (*library.Edition, error)
	GetEditionContributors(context.Context, int64) ([]library.Contributor, error)
	GetBookFiles(context.Context, int64) ([]library.BookFile, error)
	FindFileByPath(context.Context, string) (*library.BookFile, error)
	FindFilesByContentHash(context.Context, string) ([]library.BookFile, error)
}
