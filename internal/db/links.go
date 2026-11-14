package db

import (
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// LinkCandidate is the part of a library item the link sync matches on.
type LinkCandidate struct {
	ID        int64
	FilePath  string
	MediaType string
}

// LinkCandidates returns every library item's path and media type.
func (d *DB) LinkCandidates() ([]LinkCandidate, error) {
	rows, err := d.db.Query("SELECT id, file_path, media_type FROM library_items")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LinkCandidate
	for rows.Next() {
		var c LinkCandidate
		if err := rows.Scan(&c.ID, &c.FilePath, &c.MediaType); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ReplaceLibraryLinks makes links the complete set stored for service: items
// it leaves out lose their link to that service.
func (d *DB) ReplaceLibraryLinks(service string, links []models.LibraryLink, syncedAt time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("DELETE FROM library_links WHERE service = ?", service); err != nil {
		return err
	}
	// The join skips items deleted since the sync listed them.
	stmt, err := tx.Prepare(`INSERT INTO library_links (item_id, service, external_id, url_path, cover_path, synced_at)
		SELECT id, ?, ?, ?, ?, ? FROM library_items WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, l := range links {
		if _, err := stmt.Exec(service, l.ExternalID, l.URLPath, l.CoverPath, unixSeconds(syncedAt), l.ItemID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LibraryLinks returns the stored links of the given items, by item ID.
func (d *DB) LibraryLinks(ids []int64) (map[int64][]models.LibraryLink, error) {
	out := make(map[int64][]models.LibraryLink, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := d.db.Query(
		"SELECT item_id, service, external_id, url_path, cover_path FROM library_links WHERE item_id IN (?"+
			strings.Repeat(",?", len(ids)-1)+") ORDER BY service", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var l models.LibraryLink
		if err := rows.Scan(&l.ItemID, &l.Service, &l.ExternalID, &l.URLPath, &l.CoverPath); err != nil {
			return nil, err
		}
		out[l.ItemID] = append(out[l.ItemID], l)
	}
	return out, rows.Err()
}
