// Package models defines the shared data types used across librarr.
package models

import "time"

// SearchResult represents a single search result from any source.
type SearchResult struct {
	Source           string   `json:"source"`
	Title            string   `json:"title"`
	Author           string   `json:"author,omitempty"`
	Size             int64    `json:"size,omitempty"`
	SizeHuman        string   `json:"size_human,omitempty"`
	Seeders          int      `json:"seeders,omitempty"`
	Leechers         int      `json:"leechers,omitempty"`
	Grabs            int      `json:"grabs,omitempty"`
	Indexer          string   `json:"indexer,omitempty"`
	DownloadURL      string   `json:"download_url,omitempty"`
	MagnetURL        string   `json:"magnet_url,omitempty"`
	InfoHash         string   `json:"info_hash,omitempty"`
	GUID             string   `json:"guid,omitempty"`
	MD5              string   `json:"md5,omitempty"`
	URL              string   `json:"url,omitempty"`
	SourceID         string   `json:"source_id,omitempty"`
	CoverURL         string   `json:"cover_url,omitempty"`
	Format           string   `json:"format,omitempty"`
	Language         string   `json:"language,omitempty"`
	MediaType        string   `json:"media_type,omitempty"`        // ebook, audiobook, manga
	DownloadProtocol string   `json:"download_protocol,omitempty"` // "torrent" or "nzb"
	PublishDate      string   `json:"publish_date,omitempty"`
	Categories       []string `json:"categories,omitempty"`

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

// WishlistItem represents a user's wish for a book/audiobook/manga.
type WishlistItem struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	Author    string    `json:"author"`
	MediaType string    `json:"media_type"`
	AddedAt   time.Time `json:"added_at"`
}

// WantedBook represents a normalized wanted/discover record.
type WantedBook struct {
	ID                   int64      `json:"id"`
	Title                string     `json:"title"`
	Author               string     `json:"author"`
	ISBN                 string     `json:"isbn,omitempty"`
	ASIN                 string     `json:"asin,omitempty"`
	Series               string     `json:"series,omitempty"`
	Publisher            string     `json:"publisher,omitempty"`
	Language             string     `json:"language,omitempty"`
	CoverURL             string     `json:"cover_url,omitempty"`
	Description          string     `json:"description,omitempty"`
	Source               string     `json:"source,omitempty"`
	MediaType            string     `json:"media_type,omitempty"`
	PreferredFormat      string     `json:"preferred_format,omitempty"`
	OriginSource         string     `json:"origin_source,omitempty"`
	OriginReleaseTitle   string     `json:"origin_release_title,omitempty"`
	OriginIndexer        string     `json:"origin_indexer,omitempty"`
	SourceID             string     `json:"source_id,omitempty"`
	Monitored            bool       `json:"monitored"`
	Status               string     `json:"status"`
	LastSearch           *time.Time `json:"last_search,omitempty"`
	LastResultCount      int        `json:"last_result_count"`
	LastSuccess          bool       `json:"last_success"`
	LastError            string     `json:"last_error,omitempty"`
	BestMatchScore       float64    `json:"best_match_score"`
	LastMatchTitle       string     `json:"last_match_title,omitempty"`
	SelectedReleaseID    int64      `json:"selected_release_id,omitempty"`
	SelectedReleaseTitle string     `json:"selected_release_title,omitempty"`
	DownloadJobID        string     `json:"download_job_id,omitempty"`
	DownloadClient       string     `json:"download_client,omitempty"`
	DownloadHash         string     `json:"download_hash,omitempty"`
	DownloadStartedAt    *time.Time `json:"download_started_at,omitempty"`
	DownloadError        string     `json:"download_error,omitempty"`
	AddedAt              time.Time  `json:"added_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

// WantedSearchHistory represents one monitored search attempt for a wanted book.
type WantedSearchHistory struct {
	ID             int64     `json:"id"`
	WantedBookID   int64     `json:"wanted_book_id"`
	Title          string    `json:"title,omitempty"`
	Author         string    `json:"author,omitempty"`
	Query          string    `json:"query,omitempty"`
	Status         string    `json:"status"`
	ResultCount    int       `json:"result_count"`
	Success        bool      `json:"success"`
	Error          string    `json:"error,omitempty"`
	BestMatchScore float64   `json:"best_match_score"`
	BestMatchTitle string    `json:"best_match_title,omitempty"`
	SearchedAt     time.Time `json:"searched_at"`
}

// WantedRelease represents one stored release from the latest successful search.
type WantedRelease struct {
	ID                int64     `json:"id"`
	WantedBookID      int64     `json:"wanted_book_id"`
	Title             string    `json:"title"`
	GUID              string    `json:"guid,omitempty"`
	Indexer           string    `json:"indexer,omitempty"`
	Protocol          string    `json:"protocol,omitempty"`
	PublishDate       string    `json:"publish_date,omitempty"`
	Size              int64     `json:"size,omitempty"`
	SizeHuman         string    `json:"size_human,omitempty"`
	Seeders           int       `json:"seeders"`
	Leechers          int       `json:"leechers"`
	Grabs             int       `json:"grabs"`
	Language          string    `json:"language,omitempty"`
	Format            string    `json:"format,omitempty"`
	DownloadURL       string    `json:"-"`
	DownloadAvailable bool      `json:"download_available,omitempty"`
	Selected          bool      `json:"selected,omitempty"`
	Categories        []string  `json:"categories,omitempty"`
	Score             float64   `json:"score"`
	SearchQuery       string    `json:"search_query,omitempty"`
	SearchTime        time.Time `json:"search_time"`
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
	Enabled      bool      `json:"enabled"`
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
