package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ReadingHistoryEntry represents a reading history record.
type ReadingHistoryEntry struct {
	ID            int64      `json:"id"`
	UserID        int64      `json:"user_id"`
	BookTitle     string     `json:"book_title"`
	Author        string     `json:"author"`
	Format        string     `json:"format"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Rating        *int       `json:"rating,omitempty"`
	Notes         string     `json:"notes"`
	LibraryItemID *int64     `json:"library_item_id,omitempty"`
	Status        string     `json:"status"`
}

// --- Reading History ---

// AddReadingHistory inserts a reading history entry.
func (d *DB) AddReadingHistory(userID int64, bookTitle, author, format string, startedAt, finishedAt *time.Time, rating *int, notes string, libraryItemID *int64) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var startedAtVal, finishedAtVal sql.NullFloat64
	if startedAt != nil {
		startedAtVal = sql.NullFloat64{Float64: float64(startedAt.Unix()), Valid: true}
	}
	if finishedAt != nil {
		finishedAtVal = sql.NullFloat64{Float64: float64(finishedAt.Unix()), Valid: true}
	}

	var ratingVal sql.NullInt64
	if rating != nil {
		ratingVal = sql.NullInt64{Int64: int64(*rating), Valid: true}
	}

	result, err := d.db.Exec(
		`INSERT INTO reading_history (user_id, book_title, author, format, started_at, finished_at, rating, notes, library_item_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		userID, bookTitle, author, format, startedAtVal, finishedAtVal, ratingVal, notes, libraryItemID,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetReadingHistory returns paginated reading history for a user.
func (d *DB) GetReadingHistory(userID int64, status string, limit, offset int) ([]ReadingHistoryEntry, error) {
	query := `SELECT id, user_id, book_title, author, format, started_at, finished_at, rating, notes, library_item_id
		FROM reading_history WHERE user_id = ?`
	args := []interface{}{userID}

	switch status {
	case "reading":
		query += " AND finished_at IS NULL"
	case "finished":
		query += " AND finished_at IS NOT NULL"
	}

	query += " ORDER BY COALESCE(finished_at, started_at) DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ReadingHistoryEntry
	for rows.Next() {
		var e ReadingHistoryEntry
		var startedAt, finishedAt sql.NullFloat64
		var rating sql.NullInt64
		var libraryItemID sql.NullInt64

		if err := rows.Scan(&e.ID, &e.UserID, &e.BookTitle, &e.Author, &e.Format, &startedAt, &finishedAt, &rating, &e.Notes, &libraryItemID); err != nil {
			continue
		}

		if startedAt.Valid {
			t := time.Unix(int64(startedAt.Float64), 0)
			e.StartedAt = &t
		}
		if finishedAt.Valid {
			t := time.Unix(int64(finishedAt.Float64), 0)
			e.FinishedAt = &t
			e.Status = "finished"
		} else {
			e.Status = "reading"
		}
		if rating.Valid {
			r := int(rating.Int64)
			e.Rating = &r
		}
		if libraryItemID.Valid {
			lid := libraryItemID.Int64
			e.LibraryItemID = &lid
		}

		entries = append(entries, e)
	}
	return entries, nil
}

// UpdateReadingHistory updates a history entry (finish date, rating, notes).
func (d *DB) UpdateReadingHistory(id, userID int64, finishedAt *time.Time, rating *int, notes *string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Build dynamic update.
	sets := []string{}
	args := []interface{}{}

	if finishedAt != nil {
		sets = append(sets, "finished_at = ?")
		args = append(args, float64(finishedAt.Unix()))
	}
	if rating != nil {
		sets = append(sets, "rating = ?")
		args = append(args, *rating)
	}
	if notes != nil {
		sets = append(sets, "notes = ?")
		args = append(args, *notes)
	}

	if len(sets) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE reading_history SET %s WHERE id = ? AND user_id = ?",
		strings.Join(sets, ", "))
	args = append(args, id, userID)

	result, err := d.db.Exec(query, args...)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("entry not found")
	}
	return nil
}

// DeleteReadingHistory removes a history entry by ID.
func (d *DB) DeleteReadingHistory(id, userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM reading_history WHERE id = ? AND user_id = ?", id, userID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("entry not found")
	}
	return nil
}

// GetReadingStats returns reading statistics for a user.
func (d *DB) GetReadingStats(userID int64) (map[string]interface{}, error) {
	stats := map[string]interface{}{}

	// Total finished.
	var totalFinished int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NOT NULL", userID).Scan(&totalFinished)
	stats["total_finished"] = totalFinished

	// Currently reading.
	var currentlyReading int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NULL", userID).Scan(&currentlyReading)
	stats["currently_reading"] = currentlyReading

	// Average rating.
	var avgRating sql.NullFloat64
	d.db.QueryRow("SELECT AVG(rating) FROM reading_history WHERE user_id = ? AND rating IS NOT NULL", userID).Scan(&avgRating)
	if avgRating.Valid {
		stats["avg_rating"] = float64(int(avgRating.Float64*10)) / 10
	} else {
		stats["avg_rating"] = 0
	}

	// Books this month.
	monthStart := time.Now().AddDate(0, 0, -time.Now().Day()+1)
	var booksThisMonth int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NOT NULL AND finished_at >= ?",
		userID, float64(monthStart.Unix())).Scan(&booksThisMonth)
	stats["books_this_month"] = booksThisMonth

	// Books this year.
	yearStart := time.Date(time.Now().Year(), 1, 1, 0, 0, 0, 0, time.Local)
	var booksThisYear int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NOT NULL AND finished_at >= ?",
		userID, float64(yearStart.Unix())).Scan(&booksThisYear)
	stats["books_this_year"] = booksThisYear

	return stats, nil
}
