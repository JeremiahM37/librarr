package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// --- Library Items ---

// AddItem records a successfully processed book.
func (d *DB) AddItem(item *models.LibraryItem) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	metadata := item.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	result, err := d.db.Exec(
		`INSERT INTO library_items (title, author, file_path, original_path, file_size, file_format, media_type, source, source_id, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.Title, item.Author, item.FilePath, item.OriginalPath,
		item.FileSize, item.FileFormat, item.MediaType,
		item.Source, item.SourceID, metadata,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// HasSourceID checks if a source_id already exists.
func (d *DB) HasSourceID(sourceID string) bool {
	if sourceID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var exists int
	err := d.db.QueryRow("SELECT 1 FROM library_items WHERE source_id = ?", sourceID).Scan(&exists)
	return err == nil
}

// FindByTitle performs a case-insensitive title lookup.
func (d *DB) FindByTitle(title string) ([]models.LibraryItem, error) {
	rows, err := d.db.Query("SELECT * FROM library_items WHERE title = ? COLLATE NOCASE", title)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}

// GetItems returns a paginated list of library items, newest first.
func (d *DB) GetItems(mediaType string, limit, offset int) ([]models.LibraryItem, error) {
	var rows *sql.Rows
	var err error
	if mediaType != "" {
		rows, err = d.db.Query(
			"SELECT * FROM library_items WHERE media_type = ? ORDER BY added_at DESC LIMIT ? OFFSET ?",
			mediaType, limit, offset,
		)
	} else {
		rows, err = d.db.Query(
			"SELECT * FROM library_items ORDER BY added_at DESC LIMIT ? OFFSET ?",
			limit, offset,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}

// CountItems counts library items, optionally filtered by media type.
func (d *DB) CountItems(mediaType string) (int, error) {
	var count int
	var err error
	if mediaType != "" {
		err = d.db.QueryRow("SELECT COUNT(*) FROM library_items WHERE media_type = ?", mediaType).Scan(&count)
	} else {
		err = d.db.QueryRow("SELECT COUNT(*) FROM library_items").Scan(&count)
	}
	return count, err
}
func (d *DB) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	ebookCount, _ := d.CountItems("ebook")
	audiobookCount, _ := d.CountItems("audiobook")
	mangaCount, _ := d.CountItems("manga")
	totalCount, _ := d.CountItems("")
	activityCount, _ := d.CountActivity()

	stats["total_items"] = totalCount
	stats["ebooks"] = ebookCount
	stats["audiobooks"] = audiobookCount
	stats["manga"] = mangaCount
	stats["activity_events"] = activityCount

	return stats, nil
}

// DeleteItem removes a library item by ID.
func (d *DB) DeleteItem(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM library_items WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("item not found")
	}
	return nil
}

// DeleteItemBySourceID removes a library item by its source_id field.
func (d *DB) DeleteItemBySourceID(sourceID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("DELETE FROM library_items WHERE source_id = ?", sourceID)
	return err
}
func scanLibraryItems(rows *sql.Rows) ([]models.LibraryItem, error) {
	var items []models.LibraryItem
	for rows.Next() {
		var item models.LibraryItem
		var ts float64
		var metadataStr string
		if err := rows.Scan(
			&item.ID, &item.Title, &item.Author, &item.FilePath,
			&item.OriginalPath, &item.FileSize, &item.FileFormat,
			&item.MediaType, &item.Source, &item.SourceID,
			&metadataStr, &ts,
		); err != nil {
			continue
		}
		item.AddedAt = time.Unix(int64(ts), 0)
		item.Metadata = metadataStr
		items = append(items, item)
	}
	return items, nil
}
func ItemToJSON(item models.LibraryItem) map[string]interface{} {
	m := map[string]interface{}{
		"id":            item.ID,
		"title":         item.Title,
		"author":        item.Author,
		"file_path":     item.FilePath,
		"original_path": item.OriginalPath,
		"file_size":     item.FileSize,
		"file_format":   item.FileFormat,
		"media_type":    item.MediaType,
		"source":        item.Source,
		"source_id":     item.SourceID,
		"added_at":      item.AddedAt.Format(time.RFC3339),
	}
	if item.Metadata != "" && item.Metadata != "{}" {
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(item.Metadata), &meta); err == nil {
			m["metadata"] = meta
		}
	}
	return m
}
