package scanner

import (
	"time"

	"github.com/jamie75/librarr/internal/library"
)

type Status string

const (
	StatusPending            Status = "pending"
	StatusScanning           Status = "scanning"
	StatusProcessingMetadata Status = "processing_metadata"
	StatusClassifying        Status = "classifying"
	StatusCompleted          Status = "completed"
	StatusFailed             Status = "failed"
	StatusCancelled          Status = "cancelled"
)

type Classification string

const (
	ClassificationNew             Classification = "new"
	ClassificationManualReview    Classification = "manual_review"
	ClassificationAlreadyImported Classification = "already_imported"
	ClassificationDuplicate       Classification = "duplicate"
	ClassificationUnsupported     Classification = "unsupported"
	ClassificationUnreadable      Classification = "unreadable"
)

type Roots struct {
	EbookDir     string `json:"ebook_dir"`
	AudiobookDir string `json:"audiobook_dir"`
	MangaDir     string `json:"manga_dir"`
}

type Warning struct {
	MediaType library.MediaType `json:"media_type,omitempty"`
	Path      string            `json:"path,omitempty"`
	Message   string            `json:"message"`
}

type Progress struct {
	Status             Status            `json:"status"`
	CurrentMediaType   library.MediaType `json:"current_media_type,omitempty"`
	CurrentPath        string            `json:"current_path,omitempty"`
	CurrentPhase       string            `json:"current_phase,omitempty"`
	DirectoriesScanned int               `json:"directories_scanned"`
	FilesDiscovered    int               `json:"files_discovered"`
	FilesProcessed     int               `json:"files_processed"`
	CandidatesReady    int               `json:"candidates_ready"`
	Warnings           []Warning         `json:"warnings,omitempty"`
	StartedAt          time.Time         `json:"started_at"`
	CompletedAt        *time.Time        `json:"completed_at,omitempty"`
}

type Metadata struct {
	Title           string             `json:"title,omitempty"`
	Subtitle        string             `json:"subtitle,omitempty"`
	Author          string             `json:"author,omitempty"`
	Series          string             `json:"series,omitempty"`
	Volume          string             `json:"volume,omitempty"`
	SeriesNumber    string             `json:"series_number,omitempty"`
	Publisher       string             `json:"publisher,omitempty"`
	PublicationYear string             `json:"publication_year,omitempty"`
	ISBN            string             `json:"isbn,omitempty"`
	Language        string             `json:"language,omitempty"`
	Description     string             `json:"description,omitempty"`
	Tags            []string           `json:"tags,omitempty"`
	Library         string             `json:"library,omitempty"`
	Source          string             `json:"source,omitempty"`
	Confidence      library.Confidence `json:"confidence,omitempty"`
}

type ManualReviewDetails struct {
	Reason               string             `json:"reason,omitempty"`
	PlannerDisposition   string             `json:"planner_disposition,omitempty"`
	SuggestedDestination string             `json:"suggested_destination,omitempty"`
	MetadataSource       string             `json:"metadata_source,omitempty"`
	Confidence           library.Confidence `json:"confidence,omitempty"`
}

type DuplicateDetails struct {
	Reason         string `json:"reason,omitempty"`
	Signal         string `json:"signal,omitempty"`
	ExistingTitle  string `json:"existing_title,omitempty"`
	ExistingAuthor string `json:"existing_author,omitempty"`
	ExistingFormat string `json:"existing_format,omitempty"`
	ExistingPath   string `json:"existing_path,omitempty"`
}

type Candidate struct {
	ID                   string               `json:"id"`
	MediaType            library.MediaType    `json:"media_type"`
	Format               string               `json:"format"`
	Path                 string               `json:"path"`
	RelativePath         string               `json:"relative_path"`
	Filename             string               `json:"filename"`
	Size                 int64                `json:"size"`
	ModifiedAt           time.Time            `json:"modified_at"`
	Title                string               `json:"title,omitempty"`
	Author               string               `json:"author,omitempty"`
	Series               string               `json:"series,omitempty"`
	Volume               string               `json:"volume,omitempty"`
	CoverURL             string               `json:"cover_url,omitempty"`
	CoverPath            string               `json:"-"`
	Metadata             Metadata             `json:"metadata"`
	Classification       Classification       `json:"classification"`
	ClassificationReason string               `json:"classification_reason,omitempty"`
	ExistingPath         string               `json:"existing_path,omitempty"`
	ExistingBookID       int64                `json:"existing_book_id,omitempty"`
	ExistingFileID       int64                `json:"existing_file_id,omitempty"`
	DestinationPath      string               `json:"destination_path,omitempty"`
	ManualReview         *ManualReviewDetails `json:"manual_review,omitempty"`
	Duplicate            *DuplicateDetails    `json:"duplicate,omitempty"`
	Error                string               `json:"error,omitempty"`
}

type CandidateUpdate struct {
	ID                   string         `json:"id"`
	Classification       Classification `json:"classification"`
	ClassificationReason string         `json:"classification_reason,omitempty"`
	ExistingPath         string         `json:"existing_path,omitempty"`
	ExistingBookID       int64          `json:"existing_book_id,omitempty"`
	ExistingFileID       int64          `json:"existing_file_id,omitempty"`
	Error                string         `json:"error,omitempty"`
}

type CandidateResolution struct {
	ID              string   `json:"id"`
	Action          string   `json:"action"`
	Title           string   `json:"title,omitempty"`
	Subtitle        string   `json:"subtitle,omitempty"`
	Author          string   `json:"author,omitempty"`
	Series          string   `json:"series,omitempty"`
	SeriesNumber    string   `json:"series_number,omitempty"`
	Publisher       string   `json:"publisher,omitempty"`
	PublicationYear string   `json:"publication_year,omitempty"`
	ISBN            string   `json:"isbn,omitempty"`
	Language        string   `json:"language,omitempty"`
	Description     string   `json:"description,omitempty"`
	Tags            []string `json:"tags,omitempty"`
	Library         string   `json:"library,omitempty"`
}

type Totals struct {
	Found           int `json:"found"`
	ReadyToImport   int `json:"ready_to_import"`
	ManualReview    int `json:"manual_review"`
	AlreadyImported int `json:"already_imported"`
	Duplicates      int `json:"duplicates"`
	Unsupported     int `json:"unsupported"`
	Unreadable      int `json:"unreadable"`
}

type Result struct {
	JobID       string      `json:"job_id"`
	Status      Status      `json:"status"`
	Totals      Totals      `json:"totals"`
	Candidates  []Candidate `json:"candidates"`
	Warnings    []Warning   `json:"warnings,omitempty"`
	StartedAt   time.Time   `json:"started_at"`
	CompletedAt *time.Time  `json:"completed_at,omitempty"`
}

type Job struct {
	ID          string     `json:"id"`
	Status      Status     `json:"status"`
	Progress    Progress   `json:"progress"`
	Error       string     `json:"error,omitempty"`
	Result      *Result    `json:"result,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}
