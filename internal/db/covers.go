package db

import (
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

func unixSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// ItemsNeedingCover returns items of the given media types with no cover that
// have not been checked since checkedBefore, newest first.
func (d *DB) ItemsNeedingCover(mediaTypes []string, checkedBefore time.Time, limit int) ([]models.LibraryItem, error) {
	if len(mediaTypes) == 0 {
		return nil, nil
	}
	args := []interface{}{unixSeconds(checkedBefore)}
	for _, mt := range mediaTypes {
		args = append(args, mt)
	}
	args = append(args, limit)
	rows, err := d.db.Query(
		"SELECT "+libraryItemColumns+" FROM library_items WHERE cover_file = '' AND cover_checked_at < ? AND media_type IN (?"+
			strings.Repeat(",?", len(mediaTypes)-1)+") ORDER BY added_at DESC, id DESC LIMIT ?",
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}

// SetItemCover records the cover file for an item (empty when none was found)
// and when the lookup happened.
func (d *DB) SetItemCover(id int64, file string, checkedAt time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE library_items SET cover_file = ?, cover_checked_at = ? WHERE id = ?", file, unixSeconds(checkedAt), id)
	return err
}

// CoverFiles maps item IDs to their cover file names, for items that have one.
func (d *DB) CoverFiles() (map[int64]string, error) {
	rows, err := d.db.Query("SELECT id, cover_file FROM library_items WHERE cover_file != ''")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	files := map[int64]string{}
	for rows.Next() {
		var id int64
		var file string
		if err := rows.Scan(&id, &file); err != nil {
			return nil, err
		}
		files[id] = file
	}
	return files, rows.Err()
}

// ClearItemCover forgets an item's cover and resets its check time so it is
// looked up again.
func (d *DB) ClearItemCover(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE library_items SET cover_file = '', cover_checked_at = 0 WHERE id = ?", id)
	return err
}
