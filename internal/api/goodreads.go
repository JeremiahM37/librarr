package api

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) handleImportGoodreads(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Failed to parse form: " + err.Error(),
		})
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "No file uploaded",
		})
		return
	}
	defer file.Close()

	result := s.parseAndImportCSV(file, "goodreads")
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleImportStoryGraph(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Failed to parse form: " + err.Error(),
		})
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "No file uploaded",
		})
		return
	}
	defer file.Close()

	result := s.parseAndImportCSV(file, "storygraph")
	writeJSON(w, http.StatusOK, result)
}

// shelfCSVColumns holds the column indexes of interest in a Goodreads or
// StoryGraph export. An index of -1 means the column is absent.
type shelfCSVColumns struct {
	title    int
	author   int
	shelf    int
	rating   int
	dateRead int
	isbn     int
}

// parseShelfCSVHeader maps a Goodreads/StoryGraph CSV header row to column
// indexes, accepting the naming variants used by both services.
func parseShelfCSVHeader(header []string) (shelfCSVColumns, error) {
	colMap := make(map[string]int)
	for i, h := range header {
		colMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	pick := func(names ...string) int {
		for _, n := range names {
			if idx, ok := colMap[n]; ok {
				return idx
			}
		}
		return -1
	}

	cols := shelfCSVColumns{
		title: pick("title"),
		// Goodreads uses "Author l-f" (last, first) as a fallback.
		author: pick("author", "author l-f"),
		// StoryGraph uses "Read Status".
		shelf:    pick("exclusive shelf", "bookshelves", "read status"),
		rating:   pick("my rating", "star rating"),
		dateRead: pick("date read", "last date read"),
		isbn:     pick("isbn", "isbn13"),
	}
	if cols.title < 0 {
		return cols, fmt.Errorf("CSV must have a 'Title' column")
	}
	return cols, nil
}

// shelfCSVRow is one parsed book row from a Goodreads/StoryGraph export.
type shelfCSVRow struct {
	Title    string
	Author   string
	Shelf    string
	Rating   int
	DateRead *time.Time
}

// parseShelfCSVRow extracts a book row using the given column indexes.
// It returns false for rows that should be skipped (missing title).
func parseShelfCSVRow(cols shelfCSVColumns, record []string) (shelfCSVRow, bool) {
	field := func(idx int) string {
		if idx >= 0 && idx < len(record) {
			return strings.TrimSpace(record[idx])
		}
		return ""
	}

	row := shelfCSVRow{Title: field(cols.title)}
	if row.Title == "" {
		return row, false
	}

	row.Author = normalizeAuthorName(field(cols.author))
	row.Shelf = strings.ToLower(field(cols.shelf))
	if r, err := strconv.Atoi(field(cols.rating)); err == nil {
		row.Rating = r
	}
	row.DateRead = parseReadDate(field(cols.dateRead))
	return row, true
}

// normalizeAuthorName converts Goodreads' "Last, First" form to "First Last".
func normalizeAuthorName(author string) string {
	if strings.Contains(author, ",") && !strings.Contains(author, " and ") {
		parts := strings.SplitN(author, ",", 2)
		if len(parts) == 2 {
			return strings.TrimSpace(parts[1]) + " " + strings.TrimSpace(parts[0])
		}
	}
	return author
}

// parseReadDate parses the date formats used by Goodreads and StoryGraph
// exports, returning nil if the value is empty or unrecognized.
func parseReadDate(dateStr string) *time.Time {
	if dateStr == "" {
		return nil
	}
	for _, layout := range []string{"2006/01/02", "2006-01-02", "01/02/2006", "Jan 2, 2006"} {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return &t
		}
	}
	return nil
}

func (s *Server) parseAndImportCSV(file io.Reader, source string) map[string]interface{} {
	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return map[string]interface{}{
			"success": false, "error": "Failed to read CSV header: " + err.Error(),
		}
	}

	cols, err := parseShelfCSVHeader(header)
	if err != nil {
		return map[string]interface{}{
			"success": false, "error": err.Error(),
		}
	}

	importedWishlist := 0
	importedHistory := 0
	skipped := 0

	// Get existing wishlist to deduplicate.
	existingWishlist, _ := s.db.GetWishlist()
	wishlistTitles := make(map[string]bool)
	for _, w := range existingWishlist {
		wishlistTitles[strings.ToLower(w.Title)] = true
	}

	// Get user ID for history (use first admin or ID 0).
	var userID int64
	users, _ := s.db.ListUsers()
	for _, u := range users {
		if u.Role == "admin" {
			userID = u.ID
			break
		}
	}

	addToWishlist := func(row shelfCSVRow) {
		if wishlistTitles[strings.ToLower(row.Title)] {
			skipped++
			return
		}
		if _, err := s.db.AddWishlistItem(row.Title, row.Author, "ebook"); err != nil {
			skipped++
			return
		}
		wishlistTitles[strings.ToLower(row.Title)] = true
		importedWishlist++
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		row, ok := parseShelfCSVRow(cols, record)
		if !ok {
			continue
		}

		switch {
		case row.Shelf == "to-read" || row.Shelf == "want to read":
			addToWishlist(row)

		case row.Shelf == "read" || row.Shelf == "finished":
			// Add to reading history.
			var ratingPtr *int
			if row.Rating > 0 {
				ratingPtr = &row.Rating
			}
			finishedAt := row.DateRead
			if finishedAt == nil {
				now := time.Now()
				finishedAt = &now
			}
			if _, err := s.db.AddReadingHistory(userID, row.Title, row.Author, "", nil, finishedAt, ratingPtr, "", nil); err != nil {
				skipped++
				continue
			}
			importedHistory++

		case row.Shelf == "currently-reading" || row.Shelf == "currently reading":
			// Add to reading history as started.
			now := time.Now()
			if _, err := s.db.AddReadingHistory(userID, row.Title, row.Author, "", &now, nil, nil, "", nil); err != nil {
				skipped++
				continue
			}
			importedHistory++

		default:
			// Unknown shelf, add to wishlist by default.
			addToWishlist(row)
		}
	}

	return map[string]interface{}{
		"success":           true,
		"source":            source,
		"imported_wishlist": importedWishlist,
		"imported_history":  importedHistory,
		"skipped":           skipped,
	}
}
