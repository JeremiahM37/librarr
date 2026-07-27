// Package models defines the shared data types used across librarr.
package models

import "time"

// SearchResult represents a single search result from any source.
type SearchResult struct {
	Source           string `json:"source"`
	Title            string `json:"title"`
	Author           string `json:"author,omitempty"`
	Size             int64  `json:"size,omitempty"`
	SizeHuman        string `json:"size_human,omitempty"`
	Seeders          int    `json:"seeders,omitempty"`
	Leechers         int    `json:"leechers,omitempty"`
	Indexer          string `json:"indexer,omitempty"`
	DownloadURL      string `json:"download_url,omitempty"`
	MagnetURL        string `json:"magnet_url,omitempty"`
	InfoHash         string `json:"info_hash,omitempty"`
	GUID             string `json:"guid,omitempty"`
	MD5              string `json:"md5,omitempty"`
	URL              string `json:"url,omitempty"`
	SourceID         string `json:"source_id,omitempty"`
	CoverURL         string `json:"cover_url,omitempty"`
	Format           string `json:"format,omitempty"`
	MediaType        string `json:"media_type,omitempty"`        // ebook, audiobook, manga
	DownloadProtocol string `json:"download_protocol,omitempty"` // "torrent" or "nzb"

	// Scoring fields (populated by scorer).
	Score          float64         `json:"score,omitempty"`
	ScoreBreakdown *ScoreBreakdown `json:"score_breakdown,omitempty"`

	// Open Library specific
	IAIDs []string `json:"ia_ids,omitempty"`

	// Gutenberg specific
	GutenbergID int    `json:"gutenberg_id,omitempty"`
	EpubURL     string `json:"epub_url,omitempty"`

	// AudioBookBay specific
	AbbURL string `json:"abb_url,omitempty"`

	// Download count (for Gutenberg/OL)
	DownloadCount int `json:"download_count,omitempty"`
}

// ScoreBreakdown provides a detailed breakdown of a search result's confidence score.
type ScoreBreakdown struct {
	TitleMatch  float64 `json:"title_match"`
	AuthorMatch float64 `json:"author_match"`
	FormatScore float64 `json:"format_score"`
	SeederScore float64 `json:"seeder_score"`
	SizeScore   float64 `json:"size_score"`
	Total       float64 `json:"total"`
	Confidence  string  `json:"confidence"`
}

// StatusTransition records a job status change.
type StatusTransition struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Detail    string `json:"detail,omitempty"`
	Timestamp string `json:"timestamp"`
}

// DownloadJob represents a background download job.
type DownloadJob struct {
	ID            string             `json:"job_id"`
	Title         string             `json:"title"`
	Source        string             `json:"source"`
	Status        string             `json:"status"` // queued, searching, downloading, importing, completed, error, dead_letter, retry_wait
	Detail        string             `json:"detail,omitempty"`
	Error         string             `json:"error,omitempty"`
	URL           string             `json:"url,omitempty"`
	MD5           string             `json:"md5,omitempty"`
	SourceID      string             `json:"source_id,omitempty"`
	MediaType     string             `json:"media_type,omitempty"`
	RetryCount    int                `json:"retry_count"`
	MaxRetries    int                `json:"max_retries"`
	CreatedAt     time.Time          `json:"created_at"`
	UpdatedAt     time.Time          `json:"updated_at"`
	StatusHistory []StatusTransition `json:"status_history,omitempty"`
}

// LibraryItem represents a tracked book in the library.
type LibraryItem struct {
	ID           int64     `json:"id"`
	Title        string    `json:"title"`
	Author       string    `json:"author"`
	FilePath     string    `json:"file_path"`
	OriginalPath string    `json:"original_path"`
	FileSize     int64     `json:"file_size"`
	FileFormat   string    `json:"file_format"`
	MediaType    string    `json:"media_type"`
	Source       string    `json:"source"`
	SourceID     string    `json:"source_id"`
	ContentHash  string    `json:"-"`
	Metadata     string    `json:"metadata"`
	AddedAt      time.Time `json:"added_at"`
}

// ActivityEvent represents an entry in the activity log.
type ActivityEvent struct {
	ID            int64     `json:"id"`
	EventType     string    `json:"event_type"`
	Title         string    `json:"title"`
	Detail        string    `json:"detail"`
	LibraryItemID *int64    `json:"library_item_id,omitempty"`
	JobID         string    `json:"job_id"`
	Timestamp     time.Time `json:"timestamp"`
}

// NZBJob tracks an NZB submitted to SABnzbd so the completion watcher can
// import it with the right media type once SABnzbd finishes. SABnzbd uses a
// single category, so the media type can't be recovered from its history —
// it is recorded here at submit time and cleared once imported.
type NZBJob struct {
	NzoID     string    `json:"nzo_id"`
	Title     string    `json:"title"`
	MediaType string    `json:"media_type"`
	Imported  bool      `json:"imported"`
	CreatedAt time.Time `json:"created_at"`
}

// WishlistItem represents a user's wish for a book/audiobook/manga.
type WishlistItem struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	Author    string    `json:"author"`
	MediaType string    `json:"media_type"`
	AddedAt   time.Time `json:"added_at"`
}

// ActivityEntry represents an entry in the enhanced activity log.
type ActivityEntry struct {
	ID        int64     `json:"id"`
	User      string    `json:"user"`
	Action    string    `json:"action"`
	Target    string    `json:"target"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

// UploadRecord represents a tracked file upload.
type UploadRecord struct {
	ID           int64     `json:"id"`
	User         string    `json:"user"`
	Filename     string    `json:"filename"`
	OriginalName string    `json:"original_name"`
	FileType     string    `json:"file_type"`
	FileSize     int64     `json:"file_size"`
	OrganizedTo  string    `json:"organized_to"`
	Status       string    `json:"status"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// User represents a registered user.
type User struct {
	ID           int64     `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"` // "admin" or "user"
	TOTPSecret   string    `json:"-"`
	TOTPEnabled  bool      `json:"totp_enabled"`
	CreatedAt    time.Time `json:"created_at"`
	LastLogin    time.Time `json:"last_login,omitempty"`
}

// DownloadRequest is the payload for the POST /api/download endpoint.
type DownloadRequest struct {
	Source           string `json:"source"`
	Title            string `json:"title"`
	Author           string `json:"author,omitempty"`
	SourceID         string `json:"source_id,omitempty"`
	DownloadURL      string `json:"download_url,omitempty"`
	MagnetURL        string `json:"magnet_url,omitempty"`
	InfoHash         string `json:"info_hash,omitempty"`
	GUID             string `json:"guid,omitempty"`
	MD5              string `json:"md5,omitempty"`
	URL              string `json:"url,omitempty"`
	AbbURL           string `json:"abb_url,omitempty"`
	Force            bool   `json:"force,omitempty"`
	MediaType        string `json:"media_type,omitempty"`
	DownloadProtocol string `json:"download_protocol,omitempty"`
}

// DownloadStatus is an entry in the GET /api/downloads response.
type DownloadStatus struct {
	Source     string  `json:"source"`
	Title      string  `json:"title"`
	Status     string  `json:"status"`
	Progress   float64 `json:"progress,omitempty"`
	Size       string  `json:"size,omitempty"`
	Speed      string  `json:"speed,omitempty"`
	Hash       string  `json:"hash,omitempty"`
	JobID      string  `json:"job_id,omitempty"`
	Error      string  `json:"error,omitempty"`
	Detail     string  `json:"detail,omitempty"`
	RetryCount int     `json:"retry_count,omitempty"`
	MaxRetries int     `json:"max_retries,omitempty"`
}
