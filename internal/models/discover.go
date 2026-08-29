package models

// DiscoverResult is a browsable book from an external metadata catalog
// (Google Books, Open Library) - what powers the Discover/browse UI. This
// is deliberately a different type from SearchResult: a discover result is
// a catalog entry for someone deciding what to request, not a downloadable
// release from an indexer, and forcing it into SearchResult's shape would
// mean carrying a dozen download-specific fields (Size, Seeders, InfoHash,
// DownloadProtocol...) that never apply here.
type DiscoverResult struct {
	Source        string   `json:"source"` // "google_books" or "open_library"
	SourceID      string   `json:"source_id"`
	Title         string   `json:"title"`
	Author        string   `json:"author,omitempty"`
	CoverURL      string   `json:"cover_url,omitempty"`
	Description   string   `json:"description,omitempty"`
	ISBN          string   `json:"isbn,omitempty"`
	PageCount     int      `json:"page_count,omitempty"`
	Categories    []string `json:"categories,omitempty"`
	PublishedDate string   `json:"published_date,omitempty"` // as reported by the source; not always a full date

	// Ownership, resolved against library_items at response time. Mirrors
	// SearchResult's InLibrary/LibraryItemID/LibraryTitle convention - see
	// annotateOwnership in internal/api/ownership.go.
	InLibrary     bool   `json:"in_library"`
	LibraryItemID int64  `json:"library_item_id,omitempty"`
	LibraryTitle  string `json:"library_title,omitempty"`

	// Requested, resolved against the requests table at response time, so
	// browse cards can show "already requested" instead of letting someone
	// submit a duplicate.
	Requested     bool   `json:"requested"`
	RequestID     string `json:"request_id,omitempty"`
	RequestStatus string `json:"request_status,omitempty"`
}
