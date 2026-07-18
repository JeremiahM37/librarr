package db

import (
	"database/sql"
	"log/slog"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// --- Activity Log ---

// LogEvent appends an event to the activity log.
func (d *DB) LogEvent(eventType, title, detail string, libraryItemID *int64, jobID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO activity_log (event_type, title, detail, library_item_id, job_id) VALUES (?, ?, ?, ?, ?)`,
		eventType, title, detail, libraryItemID, jobID,
	)
	return err
}

// GetActivity returns recent activity, newest first.
func (d *DB) GetActivity(limit, offset int) ([]models.ActivityEvent, error) {
	rows, err := d.db.Query(
		"SELECT id, event_type, title, detail, library_item_id, job_id, timestamp FROM activity_log ORDER BY timestamp DESC LIMIT ? OFFSET ?",
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.ActivityEvent
	for rows.Next() {
		var e models.ActivityEvent
		var ts float64
		if err := rows.Scan(&e.ID, &e.EventType, &e.Title, &e.Detail, &e.LibraryItemID, &e.JobID, &ts); err != nil {
			continue
		}
		e.Timestamp = time.Unix(int64(ts), 0)
		events = append(events, e)
	}
	return events, nil
}

// CountActivity returns the total number of activity events.
func (d *DB) CountActivity() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM activity_log").Scan(&count)
	return count, err
}

// LogActivity appends a user-attributed event to the activity log.
func (d *DB) LogActivity(user, action, target, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO activity_log (event_type, title, detail, user, job_id) VALUES (?, ?, ?, ?, '')`,
		action, target, detail, user,
	)
	if err != nil {
		slog.Warn("LogActivity failed", "error", err)
	}
}

// GetActivityLog returns paginated activity entries with optional filters.
func (d *DB) GetActivityLog(user, action string, limit, offset int) ([]models.ActivityEntry, error) {
	query := "SELECT id, event_type, title, detail, user, timestamp FROM activity_log"
	var args []interface{}
	var conditions []string

	if user != "" {
		conditions = append(conditions, "user = ?")
		args = append(args, user)
	}
	if action != "" {
		conditions = append(conditions, "event_type = ?")
		args = append(args, action)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []models.ActivityEntry
	for rows.Next() {
		var e models.ActivityEntry
		var ts float64
		var userStr sql.NullString
		if err := rows.Scan(&e.ID, &e.Action, &e.Target, &e.Detail, &userStr, &ts); err != nil {
			continue
		}
		if userStr.Valid {
			e.User = userStr.String
		}
		e.CreatedAt = time.Unix(int64(ts), 0)
		entries = append(entries, e)
	}
	return entries, nil
}

// GetActivityLogCount returns the total number of activity entries matching filters.
func (d *DB) GetActivityLogCount(user, action string) (int, error) {
	query := "SELECT COUNT(*) FROM activity_log"
	var args []interface{}
	var conditions []string

	if user != "" {
		conditions = append(conditions, "user = ?")
		args = append(args, user)
	}
	if action != "" {
		conditions = append(conditions, "event_type = ?")
		args = append(args, action)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int
	err := d.db.QueryRow(query, args...).Scan(&count)
	return count, err
}
