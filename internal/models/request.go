package models

import "time"

// Request represents a user book request.
type Request struct {
	ID               string    `json:"id"`
	UserID           int64     `json:"user_id"`
	Username         string    `json:"username"`
	Title            string    `json:"title"`
	Author           string    `json:"author,omitempty"`
	BookType         string    `json:"book_type"` // ebook, audiobook, manga
	Status           string    `json:"status"`    // pending, approved, searching, downloading, processing, completed, failed, cancelled
	CoverURL         string    `json:"cover_url,omitempty"`
	Description      string    `json:"description,omitempty"`
	ISBN             string    `json:"isbn,omitempty"`
	Source           string    `json:"source,omitempty"` // discovery origin: google_books, open_library, or empty for a plain manual request
	Year             string    `json:"year,omitempty"`
	SeriesName       string    `json:"series_name,omitempty"`
	SeriesPosition   string    `json:"series_position,omitempty"`
	SearchQuery      string    `json:"search_query,omitempty"`
	SelectedResultID string    `json:"selected_result_id,omitempty"`
	DownloadID       string    `json:"download_id,omitempty"`
	AttentionNote    string    `json:"attention_note,omitempty"`
	AutoApproved     bool      `json:"auto_approved"`
	RetryCount       int       `json:"retry_count"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Notification represents an in-app notification for a user.
type Notification struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	Type      string    `json:"type"` // request_completed, request_failed, request_approved, request_attention
	Title     string    `json:"title"`
	Message   string    `json:"message,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
	Read      bool      `json:"read"`
	CreatedAt time.Time `json:"created_at"`
}
