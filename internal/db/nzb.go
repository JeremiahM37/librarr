package db

import (
	"github.com/JeremiahM37/librarr/internal/models"
)

// --- NZB Jobs ---
//
// SABnzbd exposes a single category, so a completed download's history entry
// doesn't tell us which library it belongs to. We record the media type when
// the NZB is submitted and clear the row once the watcher imports it.

// RecordNZBJob remembers an NZB submitted to SABnzbd. It is idempotent: a
// repeated nzo_id (e.g. a retried submit) is ignored rather than resurrecting
// an already-imported job.
func (d *DB) RecordNZBJob(nzoID, title, mediaType string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if mediaType == "" {
		mediaType = "ebook"
	}
	_, err := d.db.Exec(
		`INSERT OR IGNORE INTO nzb_jobs (nzo_id, title, media_type) VALUES (?, ?, ?)`,
		nzoID, title, mediaType,
	)
	return err
}

// PendingNZBJobs returns NZB jobs that have not yet been imported.
func (d *DB) PendingNZBJobs() ([]models.NZBJob, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rows, err := d.db.Query(
		`SELECT nzo_id, title, media_type FROM nzb_jobs WHERE imported = 0`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.NZBJob
	for rows.Next() {
		var j models.NZBJob
		if err := rows.Scan(&j.NzoID, &j.Title, &j.MediaType); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// MarkNZBJobImported flags an NZB job as imported so it is not processed again.
func (d *DB) MarkNZBJobImported(nzoID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE nzb_jobs SET imported = 1 WHERE nzo_id = ?`, nzoID)
	return err
}
