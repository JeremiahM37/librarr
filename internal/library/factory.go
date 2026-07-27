package library

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jamie75/librarr/internal/models"
)

type RepositoryMode int

const (
	LegacyRepositoryMode RepositoryMode = iota
	NormalizedRepositoryMode
)

func NewRepository(mode RepositoryMode, db *sql.DB) (LibraryRepository, error) {
	switch mode {
	case NormalizedRepositoryMode:
		return NewNormalizedRepository(db)
	case LegacyRepositoryMode:
		if db == nil {
			return nil, fmt.Errorf("legacy repository database is required")
		}
		return NewLegacyLibraryRepositoryWithStore(sqlLegacyStore{db: db}), nil
	default:
		return nil, fmt.Errorf("%w: unknown repository mode %d", ErrUnsupportedOperation, mode)
	}
}

type sqlLegacyStore struct {
	db *sql.DB
}

func (s sqlLegacyStore) GetItems(mediaType string, limit, offset int) ([]models.LibraryItem, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows *sql.Rows
	var err error
	if mediaType == "" {
		rows, err = s.db.Query(`SELECT id, title, author, file_path, original_path, file_size, file_format,
			media_type, source, source_id, metadata, content_hash, added_at FROM library_items
			ORDER BY added_at DESC LIMIT ? OFFSET ?`, limit, offset)
	} else {
		rows, err = s.db.Query(`SELECT id, title, author, file_path, original_path, file_size, file_format,
			media_type, source, source_id, metadata, content_hash, added_at FROM library_items
			WHERE media_type = ? ORDER BY added_at DESC LIMIT ? OFFSET ?`, mediaType, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLegacyItems(rows)
}

func (s sqlLegacyStore) FindByTitle(title string) ([]models.LibraryItem, error) {
	rows, err := s.db.Query(`SELECT id, title, author, file_path, original_path, file_size, file_format,
		media_type, source, source_id, metadata, content_hash, added_at FROM library_items
		WHERE title = ? COLLATE NOCASE`, title)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLegacyItems(rows)
}

func (s sqlLegacyStore) HasSourceID(sourceID string) bool {
	if sourceID == "" {
		return false
	}
	var exists int
	return s.db.QueryRow(`SELECT 1 FROM library_items WHERE source_id = ?`, sourceID).Scan(&exists) == nil
}

func (s sqlLegacyStore) CountItems(mediaType string) (int, error) {
	var count int
	var err error
	if mediaType == "" {
		err = s.db.QueryRow(`SELECT COUNT(*) FROM library_items`).Scan(&count)
	} else {
		err = s.db.QueryRow(`SELECT COUNT(*) FROM library_items WHERE media_type = ?`, mediaType).Scan(&count)
	}
	return count, err
}

func (s sqlLegacyStore) GetStats() (map[string]interface{}, error) {
	ebookCount, _ := s.CountItems("ebook")
	audiobookCount, _ := s.CountItems("audiobook")
	mangaCount, _ := s.CountItems("manga")
	totalCount, _ := s.CountItems("")
	return map[string]interface{}{
		"total_items": totalCount,
		"ebooks":      ebookCount,
		"audiobooks":  audiobookCount,
		"manga":       mangaCount,
	}, nil
}

func (s sqlLegacyStore) AddItem(item *models.LibraryItem) (int64, error) {
	if item == nil {
		return 0, fmt.Errorf("library item is nil")
	}
	metadata := item.Metadata
	if metadata == "" {
		metadata = "{}"
	}
	res, err := s.db.Exec(`INSERT INTO library_items
		(title, author, file_path, original_path, file_size, file_format, media_type, source, source_id, metadata, content_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.Title, item.Author, item.FilePath, item.OriginalPath, item.FileSize, item.FileFormat,
		item.MediaType, item.Source, item.SourceID, metadata, item.ContentHash)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s sqlLegacyStore) DeleteItem(id int64) error {
	res, err := s.db.Exec(`DELETE FROM library_items WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s sqlLegacyStore) DeleteItemBySourceID(sourceID string) error {
	_, err := s.db.Exec(`DELETE FROM library_items WHERE source_id = ?`, sourceID)
	return err
}

func scanLegacyItems(rows *sql.Rows) ([]models.LibraryItem, error) {
	var items []models.LibraryItem
	for rows.Next() {
		var item models.LibraryItem
		var added float64
		if err := rows.Scan(&item.ID, &item.Title, &item.Author, &item.FilePath, &item.OriginalPath,
			&item.FileSize, &item.FileFormat, &item.MediaType, &item.Source, &item.SourceID,
			&item.Metadata, &item.ContentHash, &added); err != nil {
			return nil, err
		}
		item.AddedAt = time.Unix(int64(added), 0)
		items = append(items, item)
	}
	return items, rows.Err()
}
