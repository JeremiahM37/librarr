package db

import (
	"encoding/json"
	"fmt"
)

// --- Release Profiles ---

// PreferredWord represents a word+score pair in a release profile.
type PreferredWord struct {
	Word  string `json:"word"`
	Score int    `json:"score"`
}

// ReleaseProfile represents a release profile record.
type ReleaseProfile struct {
	ID             int64           `json:"id"`
	Name           string          `json:"name"`
	MustContain    []string        `json:"must_contain"`
	MustNotContain []string        `json:"must_not_contain"`
	Preferred      []PreferredWord `json:"preferred"`
	Enabled        bool            `json:"enabled"`
}

func (d *DB) CreateReleaseProfile(rp *ReleaseProfile) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	mcJSON, _ := json.Marshal(rp.MustContain)
	mncJSON, _ := json.Marshal(rp.MustNotContain)
	prefJSON, _ := json.Marshal(rp.Preferred)
	result, err := d.db.Exec(
		`INSERT INTO release_profiles (name, must_contain, must_not_contain, preferred, enabled) VALUES (?, ?, ?, ?, ?)`,
		rp.Name, string(mcJSON), string(mncJSON), string(prefJSON), boolToInt(rp.Enabled),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetReleaseProfiles() ([]ReleaseProfile, error) {
	rows, err := d.db.Query("SELECT id, name, must_contain, must_not_contain, preferred, enabled FROM release_profiles ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var profiles []ReleaseProfile
	for rows.Next() {
		var rp ReleaseProfile
		var mcJSON, mncJSON, prefJSON string
		var enabled int
		if err := rows.Scan(&rp.ID, &rp.Name, &mcJSON, &mncJSON, &prefJSON, &enabled); err != nil {
			continue
		}
		json.Unmarshal([]byte(mcJSON), &rp.MustContain)
		json.Unmarshal([]byte(mncJSON), &rp.MustNotContain)
		json.Unmarshal([]byte(prefJSON), &rp.Preferred)
		rp.Enabled = enabled != 0
		profiles = append(profiles, rp)
	}
	return profiles, nil
}

func (d *DB) GetReleaseProfile(id int64) (*ReleaseProfile, error) {
	var rp ReleaseProfile
	var mcJSON, mncJSON, prefJSON string
	var enabled int
	err := d.db.QueryRow(
		"SELECT id, name, must_contain, must_not_contain, preferred, enabled FROM release_profiles WHERE id = ?", id,
	).Scan(&rp.ID, &rp.Name, &mcJSON, &mncJSON, &prefJSON, &enabled)
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(mcJSON), &rp.MustContain)
	json.Unmarshal([]byte(mncJSON), &rp.MustNotContain)
	json.Unmarshal([]byte(prefJSON), &rp.Preferred)
	rp.Enabled = enabled != 0
	return &rp, nil
}

func (d *DB) UpdateReleaseProfile(rp *ReleaseProfile) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	mcJSON, _ := json.Marshal(rp.MustContain)
	mncJSON, _ := json.Marshal(rp.MustNotContain)
	prefJSON, _ := json.Marshal(rp.Preferred)
	_, err := d.db.Exec(
		`UPDATE release_profiles SET name=?, must_contain=?, must_not_contain=?, preferred=?, enabled=? WHERE id=?`,
		rp.Name, string(mcJSON), string(mncJSON), string(prefJSON), boolToInt(rp.Enabled), rp.ID,
	)
	return err
}

func (d *DB) DeleteReleaseProfile(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec("DELETE FROM release_profiles WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("release profile not found")
	}
	return nil
}
