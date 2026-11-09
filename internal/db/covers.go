package db

import (
	"database/sql"
	"time"
)

func unixSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// GetCoverLookup returns a cached cover search. found is true for a cached
// miss too, in which case url is empty.
func (d *DB) GetCoverLookup(key string) (url string, found bool, fetchedAt time.Time, err error) {
	var ts float64
	err = d.db.QueryRow("SELECT url, fetched_at FROM cover_lookups WHERE key = ?", key).Scan(&url, &ts)
	if err == sql.ErrNoRows {
		return "", false, time.Time{}, nil
	}
	if err != nil {
		return "", false, time.Time{}, err
	}
	return url, true, time.Unix(0, int64(ts*1e9)), nil
}

// PutCoverLookup caches a cover search result; an empty url records a miss.
func (d *DB) PutCoverLookup(key, url string, fetchedAt time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(
		"INSERT OR REPLACE INTO cover_lookups (key, url, fetched_at) VALUES (?, ?, ?)",
		key, url, unixSeconds(fetchedAt),
	)
	return err
}

// PruneCoverLookups drops cached searches fetched before the given time.
func (d *DB) PruneCoverLookups(before time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("DELETE FROM cover_lookups WHERE fetched_at < ?", unixSeconds(before))
	return err
}
