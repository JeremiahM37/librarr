package db

import (
	"fmt"

	"github.com/jamie75/librarr/internal/models"
)

// --- Tags ---

// Tag represents a tag record.
type Tag struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
}

func (d *DB) CreateTag(name, color string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec("INSERT INTO tags (name, color) VALUES (?, ?)", name, color)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetTags() ([]Tag, error) {
	rows, err := d.db.Query("SELECT id, name, color FROM tags ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.ID, &t.Name, &t.Color); err != nil {
			continue
		}
		tags = append(tags, t)
	}
	return tags, nil
}

func (d *DB) DeleteTag(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Delete tag assignments first.
	d.db.Exec("DELETE FROM item_tags WHERE tag_id = ?", id)
	result, err := d.db.Exec("DELETE FROM tags WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("tag not found")
	}
	return nil
}

func (d *DB) AddItemTag(itemID, tagID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("INSERT OR IGNORE INTO item_tags (item_id, tag_id) VALUES (?, ?)", itemID, tagID)
	return err
}

func (d *DB) RemoveItemTag(itemID, tagID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("DELETE FROM item_tags WHERE item_id = ? AND tag_id = ?", itemID, tagID)
	return err
}

func (d *DB) GetItemTags(itemID int64) ([]Tag, error) {
	rows, err := d.db.Query(
		"SELECT t.id, t.name, t.color FROM tags t JOIN item_tags it ON t.id = it.tag_id WHERE it.item_id = ? ORDER BY t.name",
		itemID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.ID, &t.Name, &t.Color); err != nil {
			continue
		}
		tags = append(tags, t)
	}
	return tags, nil
}

func (d *DB) GetItemsByTag(tagID int64, limit, offset int) ([]models.LibraryItem, error) {
	rows, err := d.db.Query(
		`SELECT li.id, li.title, li.author, li.file_path, li.original_path, li.file_size, li.file_format,
		        li.media_type, li.source, li.source_id, li.metadata, li.content_hash, li.added_at
		 FROM library_items li JOIN item_tags it ON li.id = it.item_id
		 WHERE it.tag_id = ? ORDER BY li.added_at DESC LIMIT ? OFFSET ?`,
		tagID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}
