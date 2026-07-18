package db

import (
	"time"
)

// --- Series Tracking ---

// UpsertSeriesTracking creates or updates a series tracking entry.
func (d *DB) UpsertSeriesTracking(seriesName string, knownTotal, ownedCount int) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`INSERT INTO series_tracking (series_name, known_total, owned_count, last_checked)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(series_name) DO UPDATE SET known_total = ?, owned_count = ?, last_checked = ?`,
		seriesName, knownTotal, ownedCount, float64(time.Now().Unix()),
		knownTotal, ownedCount, float64(time.Now().Unix()),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetSeriesTracking returns all tracked series.
func (d *DB) GetSeriesTracking() ([]map[string]interface{}, error) {
	rows, err := d.db.Query("SELECT id, series_name, known_total, owned_count, last_checked FROM series_tracking ORDER BY series_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var id int64
		var name string
		var total, owned int
		var lastChecked float64
		if err := rows.Scan(&id, &name, &total, &owned, &lastChecked); err != nil {
			continue
		}
		result = append(result, map[string]interface{}{
			"id":           id,
			"series_name":  name,
			"known_total":  total,
			"owned_count":  owned,
			"last_checked": time.Unix(int64(lastChecked), 0).Format(time.RFC3339),
		})
	}
	return result, nil
}
