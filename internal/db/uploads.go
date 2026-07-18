package db

import (
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// --- Uploads ---

// SaveUpload records a file upload.
func (d *DB) SaveUpload(user, filename, originalName, fileType string, fileSize int64, organizedTo, status, errMsg string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`INSERT INTO uploads (user, filename, original_name, file_type, file_size, organized_to, status, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		user, filename, originalName, fileType, fileSize, organizedTo, status, errMsg,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetUploads returns recent uploads.
func (d *DB) GetUploads(limit, offset int) ([]models.UploadRecord, error) {
	rows, err := d.db.Query(
		`SELECT id, user, filename, original_name, file_type, file_size, organized_to, status, error, created_at
		 FROM uploads ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uploads []models.UploadRecord
	for rows.Next() {
		var u models.UploadRecord
		var ts float64
		if err := rows.Scan(&u.ID, &u.User, &u.Filename, &u.OriginalName, &u.FileType, &u.FileSize, &u.OrganizedTo, &u.Status, &u.Error, &ts); err != nil {
			continue
		}
		u.CreatedAt = time.Unix(int64(ts), 0)
		uploads = append(uploads, u)
	}
	return uploads, nil
}
