package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

// --- Requests ---

// CreateRequest inserts a new book request.
func (d *DB) CreateRequest(req *models.Request) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO requests (id, user_id, username, title, author, book_type, status, cover_url, description, year, series_name, series_position, search_query, selected_result_id, download_id, attention_note, auto_approved, retry_count, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ID, req.UserID, req.Username, req.Title, req.Author, req.BookType,
		req.Status, req.CoverURL, req.Description, req.Year,
		req.SeriesName, req.SeriesPosition, req.SearchQuery,
		req.SelectedResultID, req.DownloadID, req.AttentionNote,
		boolToInt(req.AutoApproved), req.RetryCount,
		float64(req.CreatedAt.Unix()), float64(req.UpdatedAt.Unix()),
	)
	return err
}

// GetRequest retrieves a request by ID.
func (d *DB) GetRequest(id string) (*models.Request, error) {
	row := d.db.QueryRow(
		`SELECT id, user_id, username, title, author, book_type, status, cover_url, description, year, series_name, series_position, search_query, selected_result_id, download_id, attention_note, auto_approved, retry_count, created_at, updated_at
		 FROM requests WHERE id = ?`, id,
	)
	return scanRequest(row)
}

// ListRequests returns requests filtered by optional user ID and status.
// If userID is 0, all requests are returned (admin view).
func (d *DB) ListRequests(userID int64, status string, limit, offset int) ([]models.Request, error) {
	query := "SELECT id, user_id, username, title, author, book_type, status, cover_url, description, year, series_name, series_position, search_query, selected_result_id, download_id, attention_note, auto_approved, retry_count, created_at, updated_at FROM requests"
	var args []interface{}
	var conditions []string

	if userID > 0 {
		conditions = append(conditions, "user_id = ?")
		args = append(args, userID)
	}
	if status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, status)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var requests []models.Request
	for rows.Next() {
		req, err := scanRequestFromRows(rows)
		if err != nil {
			continue
		}
		requests = append(requests, *req)
	}
	return requests, nil
}

// CountRequests returns the number of requests matching the filters.
func (d *DB) CountRequests(userID int64, status string) (int, error) {
	query := "SELECT COUNT(*) FROM requests"
	var args []interface{}
	var conditions []string

	if userID > 0 {
		conditions = append(conditions, "user_id = ?")
		args = append(args, userID)
	}
	if status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, status)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int
	err := d.db.QueryRow(query, args...).Scan(&count)
	return count, err
}

// UpdateRequestStatus updates the status and optional fields of a request.
func (d *DB) UpdateRequestStatus(id, status string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE requests SET status = ?, updated_at = ? WHERE id = ?`,
		status, float64(time.Now().Unix()), id,
	)
	return err
}

// UpdateRequest updates mutable fields on a request.
func (d *DB) UpdateRequest(req *models.Request) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE requests SET status = ?, search_query = ?, selected_result_id = ?, download_id = ?, attention_note = ?, retry_count = ?, updated_at = ?
		 WHERE id = ?`,
		req.Status, req.SearchQuery, req.SelectedResultID, req.DownloadID,
		req.AttentionNote, req.RetryCount, float64(time.Now().Unix()), req.ID,
	)
	return err
}

// DeleteRequest removes a request by ID.
func (d *DB) DeleteRequest(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM requests WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("request not found")
	}
	return nil
}

func scanRequest(row *sql.Row) (*models.Request, error) {
	var req models.Request
	var createdAt, updatedAt float64
	var author, coverURL, description, year, seriesName, seriesPosition sql.NullString
	var searchQuery, selectedResultID, downloadID, attentionNote sql.NullString
	var autoApproved int

	err := row.Scan(
		&req.ID, &req.UserID, &req.Username, &req.Title, &author,
		&req.BookType, &req.Status, &coverURL, &description, &year,
		&seriesName, &seriesPosition, &searchQuery, &selectedResultID,
		&downloadID, &attentionNote, &autoApproved, &req.RetryCount,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}

	req.Author = nullStr(author)
	req.CoverURL = nullStr(coverURL)
	req.Description = nullStr(description)
	req.Year = nullStr(year)
	req.SeriesName = nullStr(seriesName)
	req.SeriesPosition = nullStr(seriesPosition)
	req.SearchQuery = nullStr(searchQuery)
	req.SelectedResultID = nullStr(selectedResultID)
	req.DownloadID = nullStr(downloadID)
	req.AttentionNote = nullStr(attentionNote)
	req.AutoApproved = autoApproved == 1
	req.CreatedAt = time.Unix(int64(createdAt), 0)
	req.UpdatedAt = time.Unix(int64(updatedAt), 0)
	return &req, nil
}

func scanRequestFromRows(rows *sql.Rows) (*models.Request, error) {
	var req models.Request
	var createdAt, updatedAt float64
	var author, coverURL, description, year, seriesName, seriesPosition sql.NullString
	var searchQuery, selectedResultID, downloadID, attentionNote sql.NullString
	var autoApproved int

	err := rows.Scan(
		&req.ID, &req.UserID, &req.Username, &req.Title, &author,
		&req.BookType, &req.Status, &coverURL, &description, &year,
		&seriesName, &seriesPosition, &searchQuery, &selectedResultID,
		&downloadID, &attentionNote, &autoApproved, &req.RetryCount,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}

	req.Author = nullStr(author)
	req.CoverURL = nullStr(coverURL)
	req.Description = nullStr(description)
	req.Year = nullStr(year)
	req.SeriesName = nullStr(seriesName)
	req.SeriesPosition = nullStr(seriesPosition)
	req.SearchQuery = nullStr(searchQuery)
	req.SelectedResultID = nullStr(selectedResultID)
	req.DownloadID = nullStr(downloadID)
	req.AttentionNote = nullStr(attentionNote)
	req.AutoApproved = autoApproved == 1
	req.CreatedAt = time.Unix(int64(createdAt), 0)
	req.UpdatedAt = time.Unix(int64(updatedAt), 0)
	return &req, nil
}

func nullStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
