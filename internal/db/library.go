package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// --- Library Items ---

const libraryItemColumns = `id, title, author, file_path, original_path, file_size,
	file_format, media_type, source, source_id, metadata, content_hash, added_at`

// AddItemOutcome describes whether AddItem inserted a new row or reused an
// existing file-level identity.
type AddItemOutcome struct {
	ID             int64
	Inserted       bool
	Reason         string
	ContentHash    string
	NormalizedPath string
	ExistingID     int64
}

// AddItem records a successfully processed book.
func (d *DB) AddItem(item *models.LibraryItem) (int64, error) {
	outcome, err := d.AddItemWithOutcome(item)
	return outcome.ID, err
}

// AddItemWithOutcome inserts a library item idempotently. Exact canonical
// destination paths are always unique. Content hashes are also unique within
// the same media type and file format, which prevents a moved or re-imported
// file from creating a second row without merging EPUB and MOBI records.
// The DB mutex serializes the identity check and insert, so concurrent imports
// within Librarr cannot race between those operations.
func (d *DB) AddItemWithOutcome(item *models.LibraryItem) (AddItemOutcome, error) {
	if item == nil {
		return AddItemOutcome{}, fmt.Errorf("library item is nil")
	}

	normalizedPath := NormalizeLibraryPath(item.FilePath)
	contentHash := strings.ToLower(strings.TrimSpace(item.ContentHash))
	if contentHash == "" {
		contentHash = hashLibraryFile(item.FilePath)
	}
	item.ContentHash = contentHash

	d.mu.Lock()
	defer d.mu.Unlock()

	existing, err := d.findDuplicateLibraryItemLocked(item, normalizedPath, contentHash)
	if err != nil {
		return AddItemOutcome{}, err
	}
	if existing.ID != 0 {
		slog.Info("library import skipped", "reason", existing.Reason, "existing_record_id", existing.ID, "destination_path", item.FilePath, "content_hash", contentHash)
		return AddItemOutcome{
			ID:             existing.ID,
			Inserted:       false,
			Reason:         existing.Reason,
			ContentHash:    contentHash,
			NormalizedPath: normalizedPath,
			ExistingID:     existing.ID,
		}, nil
	}

	metadata := item.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	result, err := d.db.Exec(
		`INSERT INTO library_items (title, author, file_path, original_path, file_size, file_format, media_type, source, source_id, metadata, content_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.Title, item.Author, item.FilePath, item.OriginalPath,
		item.FileSize, item.FileFormat, item.MediaType,
		item.Source, item.SourceID, metadata, contentHash,
	)
	if err != nil {
		return AddItemOutcome{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return AddItemOutcome{}, err
	}
	return AddItemOutcome{
		ID:             id,
		Inserted:       true,
		Reason:         "new_library_item",
		ContentHash:    contentHash,
		NormalizedPath: normalizedPath,
	}, nil
}

type duplicateLibraryItem struct {
	ID     int64
	Reason string
}

func (d *DB) findDuplicateLibraryItemLocked(item *models.LibraryItem, normalizedPath, contentHash string) (duplicateLibraryItem, error) {
	rows, err := d.db.Query("SELECT id, file_path, content_hash, file_format, media_type FROM library_items")
	if err != nil {
		return duplicateLibraryItem{}, err
	}
	defer rows.Close()

	candidateFormat := effectiveLibraryFormat(item.FileFormat, item.FilePath)
	for rows.Next() {
		var id int64
		var filePath, existingHash, fileFormat, mediaType string
		if err := rows.Scan(&id, &filePath, &existingHash, &fileFormat, &mediaType); err != nil {
			return duplicateLibraryItem{}, err
		}
		if normalizedPath != "" && normalizedPath == NormalizeLibraryPath(filePath) {
			return duplicateLibraryItem{ID: id, Reason: "duplicate_destination_path"}, nil
		}

		if contentHash == "" || mediaType != item.MediaType {
			continue
		}
		if existingHash == "" {
			existingHash = hashLibraryFile(filePath)
		}
		if existingHash == contentHash && effectiveLibraryFormat(fileFormat, filePath) == candidateFormat {
			return duplicateLibraryItem{ID: id, Reason: "duplicate_content_hash"}, nil
		}
	}
	return duplicateLibraryItem{}, rows.Err()
}

// NormalizeLibraryPath returns a stable absolute path for identity checks.
// Symlinks are resolved when the target exists; case is intentionally not
// folded because Linux library filesystems are case-sensitive.
func NormalizeLibraryPath(filePath string) string {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return ""
	}
	cleaned, err := filepath.Abs(filepath.Clean(filePath))
	if err != nil {
		cleaned = filepath.Clean(filePath)
	}
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(cleaned)
}

func hashLibraryFile(filePath string) string {
	if filePath == "" {
		return ""
	}
	info, err := os.Stat(filePath)
	if err != nil || !info.Mode().IsRegular() {
		return ""
	}
	f, err := os.Open(filePath)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (d *DB) backfillLibraryContentHashes() error {
	rows, err := d.db.Query("SELECT id, file_path, content_hash FROM library_items WHERE content_hash = ''")
	if err != nil {
		return err
	}

	type libraryFile struct {
		id   int64
		path string
	}
	var files []libraryFile
	for rows.Next() {
		var file libraryFile
		var contentHash string
		if err := rows.Scan(&file.id, &file.path, &contentHash); err != nil {
			return err
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, file := range files {
		hash := hashLibraryFile(file.path)
		if hash == "" {
			continue
		}
		if _, err := d.db.Exec("UPDATE library_items SET content_hash = ? WHERE id = ? AND content_hash = ''", hash, file.id); err != nil {
			return err
		}
	}
	return nil
}

func effectiveLibraryFormat(fileFormat, filePath string) string {
	if format := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(fileFormat)), "."); format != "" {
		return format
	}
	return strings.TrimPrefix(strings.ToLower(filepath.Ext(filePath)), ".")
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
	rows, err := d.db.Query("SELECT "+libraryItemColumns+" FROM library_items WHERE title = ? COLLATE NOCASE", title)
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
			"SELECT "+libraryItemColumns+" FROM library_items WHERE media_type = ? ORDER BY added_at DESC LIMIT ? OFFSET ?",
			mediaType, limit, offset,
		)
	} else {
		rows, err = d.db.Query(
			"SELECT "+libraryItemColumns+" FROM library_items ORDER BY added_at DESC LIMIT ? OFFSET ?",
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
			&metadataStr, &item.ContentHash, &ts,
		); err != nil {
			return nil, err
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
