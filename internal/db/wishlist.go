package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// --- Wishlist (the wanted list) ---
//
// A wishlist row is a persistent, monitored entity: it stays after a grab and
// remembers which library file satisfies it, so the scheduler can upgrade it
// later. The columns are additive on top of the original (title, author,
// media_type) so exports, imports and older clients keep working.

const wishlistColumns = `w.id, w.title, w.author, w.media_type, w.added_at,
	w.monitored, w.quality_profile_id, w.library_item_id, w.active_job_id,
	w.last_searched, w.last_result, w.source,
	COALESCE(li.file_format, ''), COALESCE(li.file_path, '')`

const wishlistFrom = ` FROM wishlist w LEFT JOIN library_items li ON li.id = w.library_item_id `

// AddWishlistItem adds a monitored item using the default profile.
func (d *DB) AddWishlistItem(title, author, mediaType string) (int64, error) {
	return d.AddWishlistItemWithOptions(models.WishlistItem{
		Title: title, Author: author, MediaType: mediaType, Monitored: true, Source: "manual",
	})
}

// AddWishlistItemWithOptions adds an item with an explicit profile, monitored
// flag and provenance. An empty media type means ebook; an empty source means
// manual.
func (d *DB) AddWishlistItemWithOptions(item models.WishlistItem) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if item.MediaType == "" {
		item.MediaType = "ebook"
	}
	if item.Source == "" {
		item.Source = "manual"
	}
	result, err := d.db.Exec(
		`INSERT INTO wishlist (title, author, media_type, monitored, quality_profile_id, source) VALUES (?, ?, ?, ?, ?, ?)`,
		item.Title, item.Author, item.MediaType, boolToInt(item.Monitored), item.QualityProfileID, item.Source,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetWishlist returns all wishlist items, newest first, with the format and
// path of the library file currently satisfying each one.
func (d *DB) GetWishlist() ([]models.WishlistItem, error) {
	rows, err := d.db.Query("SELECT " + wishlistColumns + wishlistFrom + " ORDER BY w.added_at DESC, w.id DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWishlistItems(rows)
}

// GetWishlistItem returns one item by ID, or sql.ErrNoRows.
func (d *DB) GetWishlistItem(id int64) (*models.WishlistItem, error) {
	rows, err := d.db.Query("SELECT "+wishlistColumns+wishlistFrom+" WHERE w.id = ?", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanWishlistItems(rows)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, sql.ErrNoRows
	}
	return &items[0], nil
}

// FindWishlistByActiveJob returns the item whose in-flight grab is jobRef (a
// download job ID or "torrent:<infohash>"), or nil when none is.
func (d *DB) FindWishlistByActiveJob(jobRef string) (*models.WishlistItem, error) {
	if jobRef == "" {
		return nil, nil
	}
	rows, err := d.db.Query("SELECT "+wishlistColumns+wishlistFrom+" WHERE w.active_job_id = ? LIMIT 1", jobRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanWishlistItems(rows)
	if err != nil || len(items) == 0 {
		return nil, err
	}
	return &items[0], nil
}

func scanWishlistItems(rows *sql.Rows) ([]models.WishlistItem, error) {
	var items []models.WishlistItem
	for rows.Next() {
		var item models.WishlistItem
		var added, searched float64
		var monitored int
		if err := rows.Scan(&item.ID, &item.Title, &item.Author, &item.MediaType, &added,
			&monitored, &item.QualityProfileID, &item.LibraryItemID, &item.ActiveJobID,
			&searched, &item.LastResult, &item.Source,
			&item.CurrentFormat, &item.CurrentPath); err != nil {
			return nil, err
		}
		item.AddedAt = time.Unix(int64(added), 0)
		item.Monitored = monitored != 0
		if searched > 0 {
			item.LastSearched = time.Unix(int64(searched), 0)
		}
		// A dangling link (the library row was deleted) reads as "no file",
		// which is exactly what the scheduler should act on.
		if item.LibraryItemID != 0 && item.CurrentPath == "" && item.CurrentFormat == "" {
			item.LibraryItemID = 0
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// UpdateWishlistItem changes the user-editable fields: monitored flag and
// quality profile. Nil pointers leave a field untouched.
func (d *DB) UpdateWishlistItem(id int64, monitored *bool, qualityProfileID *int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if monitored == nil && qualityProfileID == nil {
		return nil
	}
	query := "UPDATE wishlist SET "
	var args []interface{}
	if monitored != nil {
		query += "monitored = ?"
		args = append(args, boolToInt(*monitored))
	}
	if qualityProfileID != nil {
		if len(args) > 0 {
			query += ", "
		}
		query += "quality_profile_id = ?"
		args = append(args, *qualityProfileID)
	}
	query += " WHERE id = ?"
	args = append(args, id)
	result, err := d.db.Exec(query, args...)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return fmt.Errorf("wishlist item not found")
	}
	return nil
}

// RecordWishlistSearch stores what the scheduler decided on its last pass.
func (d *DB) RecordWishlistSearch(id int64, result string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(result) > 500 {
		result = result[:500]
	}
	_, err := d.db.Exec("UPDATE wishlist SET last_searched = ?, last_result = ? WHERE id = ?",
		float64(time.Now().Unix()), result, id)
	return err
}

// SetWishlistActiveJob marks a grab as in flight (or clears it with "").
func (d *DB) SetWishlistActiveJob(id int64, jobRef string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE wishlist SET active_job_id = ? WHERE id = ?", jobRef, id)
	return err
}

// SatisfyWishlistItem links a wanted row to the library item that now
// fulfils it and clears the in-flight marker. It returns the previously
// linked library item ID (0 if none) so the caller can retire a superseded
// file after an upgrade.
func (d *DB) SatisfyWishlistItem(id, libraryItemID int64) (previous int64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.db.QueryRow("SELECT library_item_id FROM wishlist WHERE id = ?", id).Scan(&previous); err != nil {
		return 0, err
	}
	_, err = d.db.Exec("UPDATE wishlist SET library_item_id = ?, active_job_id = '' WHERE id = ?", libraryItemID, id)
	if previous == libraryItemID {
		previous = 0
	}
	return previous, err
}

// LinkWishlistItem attaches an existing library file to a wanted row without
// touching the in-flight marker — used by reconciliation when a file arrived
// by some other route (manual download, an old library, a torrent librarr
// could not tie to the row).
func (d *DB) LinkWishlistItem(id, libraryItemID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE wishlist SET library_item_id = ? WHERE id = ?", libraryItemID, id)
	return err
}

// ClearWishlistLink detaches a wanted row from a library item that no longer
// exists, sending it back to "missing".
func (d *DB) ClearWishlistLink(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE wishlist SET library_item_id = 0 WHERE id = ?", id)
	return err
}

// UnlinkLibraryItemFromWishlist detaches every wanted row pointing at a
// library item that is being deleted, so those rows become wanted again
// instead of silently pointing at nothing.
func (d *DB) UnlinkLibraryItemFromWishlist(libraryItemID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE wishlist SET library_item_id = 0 WHERE library_item_id = ?", libraryItemID)
	return err
}

// DeleteWishlistItem removes a wishlist item by ID.
func (d *DB) DeleteWishlistItem(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM wishlist WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("wishlist item not found")
	}
	return nil
}
