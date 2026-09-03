package db

import (
	"fmt"
	"time"
)

// --- Monitored Authors ---

// MonitoredAuthor represents a monitored author record.
type MonitoredAuthor struct {
	ID                int64     `json:"id"`
	Name              string    `json:"name"`
	LastChecked       time.Time `json:"last_checked"`
	LastBookFound     string    `json:"last_book_found"`
	CheckIntervalDays int       `json:"check_interval_days"`
	// AutoAdd puts newly discovered works on the wanted list instead of
	// only notifying about them.
	AutoAdd bool `json:"auto_add"`
	// SeenWorks is the number of works already recorded for the author; zero
	// means the next check will take a baseline instead of announcing the
	// entire back catalogue as new.
	SeenWorks int `json:"seen_works"`
}

// SeenWork is one Open Library work the monitor has already recorded.
type SeenWork struct {
	WorkKey string `json:"work_key"`
	Title   string `json:"title"`
	Year    int    `json:"year"`
}

func (d *DB) AddMonitoredAuthor(name string, intervalDays int) (int64, error) {
	return d.AddMonitoredAuthorWithOptions(name, intervalDays, true)
}

// AddMonitoredAuthorWithOptions adds an author with an explicit auto-add flag.
func (d *DB) AddMonitoredAuthorWithOptions(name string, intervalDays int, autoAdd bool) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if intervalDays < 1 {
		intervalDays = 7
	}
	result, err := d.db.Exec(
		"INSERT INTO monitored_authors (name, check_interval_days, auto_add) VALUES (?, ?, ?)",
		name, intervalDays, boolToInt(autoAdd),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

const monitoredAuthorColumns = `a.id, a.name, a.last_checked, a.last_book_found, a.check_interval_days, a.auto_add,
	(SELECT COUNT(*) FROM author_seen_works s WHERE s.author_id = a.id)`

func (d *DB) GetMonitoredAuthors() ([]MonitoredAuthor, error) {
	rows, err := d.db.Query("SELECT " + monitoredAuthorColumns + " FROM monitored_authors a ORDER BY a.name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var authors []MonitoredAuthor
	for rows.Next() {
		var a MonitoredAuthor
		var lastChecked float64
		var autoAdd int
		if err := rows.Scan(&a.ID, &a.Name, &lastChecked, &a.LastBookFound, &a.CheckIntervalDays, &autoAdd, &a.SeenWorks); err != nil {
			return nil, err
		}
		if lastChecked > 0 {
			a.LastChecked = time.Unix(int64(lastChecked), 0)
		}
		a.AutoAdd = autoAdd != 0
		authors = append(authors, a)
	}
	return authors, rows.Err()
}

// GetMonitoredAuthor returns one author by ID.
func (d *DB) GetMonitoredAuthor(id int64) (*MonitoredAuthor, error) {
	authors, err := d.GetMonitoredAuthors()
	if err != nil {
		return nil, err
	}
	for i := range authors {
		if authors[i].ID == id {
			return &authors[i], nil
		}
	}
	return nil, fmt.Errorf("monitored author not found")
}

// UpdateMonitoredAuthor changes the editable settings. Nil leaves a field as is.
func (d *DB) UpdateMonitoredAuthor(id int64, intervalDays *int, autoAdd *bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if intervalDays == nil && autoAdd == nil {
		return nil
	}
	query := "UPDATE monitored_authors SET "
	var args []interface{}
	if intervalDays != nil {
		days := *intervalDays
		if days < 1 {
			days = 1
		}
		query += "check_interval_days = ?"
		args = append(args, days)
	}
	if autoAdd != nil {
		if len(args) > 0 {
			query += ", "
		}
		query += "auto_add = ?"
		args = append(args, boolToInt(*autoAdd))
	}
	query += " WHERE id = ?"
	args = append(args, id)
	result, err := d.db.Exec(query, args...)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return fmt.Errorf("monitored author not found")
	}
	return nil
}

func (d *DB) DeleteMonitoredAuthor(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Foreign keys are not enforced by default in SQLite, so clear the seen
	// works explicitly rather than relying on ON DELETE CASCADE.
	if _, err := d.db.Exec("DELETE FROM author_seen_works WHERE author_id = ?", id); err != nil {
		return err
	}
	result, err := d.db.Exec("DELETE FROM monitored_authors WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("monitored author not found")
	}
	return nil
}

func (d *DB) UpdateMonitoredAuthorCheck(id int64, lastBookFound string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(
		"UPDATE monitored_authors SET last_checked = ?, last_book_found = ? WHERE id = ?",
		float64(time.Now().Unix()), lastBookFound, id,
	)
	return err
}

// SeenWorkKeys returns the set of work keys already recorded for an author.
func (d *DB) SeenWorkKeys(authorID int64) (map[string]bool, error) {
	rows, err := d.db.Query("SELECT work_key FROM author_seen_works WHERE author_id = ?", authorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make(map[string]bool)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys[k] = true
	}
	return keys, rows.Err()
}

// AddSeenWorks records works for an author; keys already present are ignored.
func (d *DB) AddSeenWorks(authorID int64, works []SeenWork) error {
	if len(works) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	for _, w := range works {
		if w.WorkKey == "" {
			continue
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO author_seen_works (author_id, work_key, title, year) VALUES (?, ?, ?, ?)",
			authorID, w.WorkKey, w.Title, w.Year); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
