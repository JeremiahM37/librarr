package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

// --- Notifications ---

// CreateNotification inserts a new notification.
func (d *DB) CreateNotification(n *models.Notification) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`INSERT INTO notifications (user_id, type, title, message, request_id, read, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, ?)`,
		n.UserID, n.Type, n.Title, n.Message, n.RequestID,
		float64(n.CreatedAt.Unix()),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetNotifications returns notifications for a user, newest first.
func (d *DB) GetNotifications(userID int64, limit, offset int) ([]models.Notification, error) {
	rows, err := d.db.Query(
		`SELECT id, user_id, type, title, message, request_id, read, created_at
		 FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		userID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifications []models.Notification
	for rows.Next() {
		n, err := scanNotificationFromRows(rows)
		if err != nil {
			continue
		}
		notifications = append(notifications, *n)
	}
	return notifications, nil
}

// CountUnreadNotifications returns the number of unread notifications for a user.
func (d *DB) CountUnreadNotifications(userID int64) (int, error) {
	var count int
	err := d.db.QueryRow(
		`SELECT COUNT(*) FROM notifications WHERE user_id = ? AND read = 0`, userID,
	).Scan(&count)
	return count, err
}

// MarkNotificationRead marks a single notification as read.
func (d *DB) MarkNotificationRead(id int64, userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`UPDATE notifications SET read = 1 WHERE id = ? AND user_id = ?`, id, userID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("notification not found")
	}
	return nil
}

// MarkAllNotificationsRead marks all notifications as read for a user.
func (d *DB) MarkAllNotificationsRead(userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0`, userID,
	)
	return err
}

// DeleteNotification removes a notification by ID (must belong to user).
func (d *DB) DeleteNotification(id int64, userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`DELETE FROM notifications WHERE id = ? AND user_id = ?`, id, userID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("notification not found")
	}
	return nil
}

func scanNotificationFromRows(rows *sql.Rows) (*models.Notification, error) {
	var n models.Notification
	var createdAt float64
	var readInt int
	var message, requestID sql.NullString

	err := rows.Scan(&n.ID, &n.UserID, &n.Type, &n.Title, &message, &requestID, &readInt, &createdAt)
	if err != nil {
		return nil, err
	}
	n.Message = nullStr(message)
	n.RequestID = nullStr(requestID)
	n.Read = readInt == 1
	n.CreatedAt = time.Unix(int64(createdAt), 0)
	return &n, nil
}
