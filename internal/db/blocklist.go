package db

import (
	"fmt"
	"time"
)

// --- Blocklist ---

// BlocklistEntry represents a blocklisted download.
type BlocklistEntry struct {
	ID          int64     `json:"id"`
	Title       string    `json:"title"`
	Source      string    `json:"source"`
	DownloadURL string    `json:"download_url"`
	InfoHash    string    `json:"info_hash"`
	Reason      string    `json:"reason"`
	CreatedAt   time.Time `json:"created_at"`
}

func (d *DB) AddBlocklistEntry(title, source, downloadURL, infoHash, reason string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		`INSERT INTO blocklist (title, source, download_url, info_hash, reason) VALUES (?, ?, ?, ?, ?)`,
		title, source, downloadURL, infoHash, reason,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetBlocklist(limit, offset int) ([]BlocklistEntry, error) {
	rows, err := d.db.Query(
		"SELECT id, title, source, download_url, info_hash, reason, created_at FROM blocklist ORDER BY created_at DESC LIMIT ? OFFSET ?",
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []BlocklistEntry
	for rows.Next() {
		var e BlocklistEntry
		var ts float64
		if err := rows.Scan(&e.ID, &e.Title, &e.Source, &e.DownloadURL, &e.InfoHash, &e.Reason, &ts); err != nil {
			continue
		}
		e.CreatedAt = time.Unix(int64(ts), 0)
		entries = append(entries, e)
	}
	return entries, nil
}

func (d *DB) DeleteBlocklistEntry(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec("DELETE FROM blocklist WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("blocklist entry not found")
	}
	return nil
}

func (d *DB) ClearBlocklist() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("DELETE FROM blocklist")
	return err
}

func (d *DB) IsBlocklisted(downloadURL, infoHash string) bool {
	if downloadURL != "" {
		var exists int
		if err := d.db.QueryRow("SELECT 1 FROM blocklist WHERE download_url = ?", downloadURL).Scan(&exists); err == nil {
			return true
		}
	}
	if infoHash != "" {
		var exists int
		if err := d.db.QueryRow("SELECT 1 FROM blocklist WHERE info_hash = ?", infoHash).Scan(&exists); err == nil {
			return true
		}
	}
	return false
}
