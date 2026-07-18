package db

import (
	"encoding/json"
	"fmt"
)

// --- Quality Profiles ---

// QualityProfile represents a quality profile record.
type QualityProfile struct {
	ID               int64    `json:"id"`
	Name             string   `json:"name"`
	FormatRanking    []string `json:"format_ranking"`
	PreferredSizeMin int64    `json:"preferred_size_min"`
	PreferredSizeMax int64    `json:"preferred_size_max"`
	UpgradeAllowed   bool     `json:"upgrade_allowed"`
	CutoffFormat     string   `json:"cutoff_format"`
}

func (d *DB) CreateQualityProfile(qp *QualityProfile) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rankingJSON, _ := json.Marshal(qp.FormatRanking)
	result, err := d.db.Exec(
		`INSERT INTO quality_profiles (name, format_ranking, preferred_size_min, preferred_size_max, upgrade_allowed, cutoff_format)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		qp.Name, string(rankingJSON), qp.PreferredSizeMin, qp.PreferredSizeMax, boolToInt(qp.UpgradeAllowed), qp.CutoffFormat,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetQualityProfiles() ([]QualityProfile, error) {
	rows, err := d.db.Query("SELECT id, name, format_ranking, preferred_size_min, preferred_size_max, upgrade_allowed, cutoff_format FROM quality_profiles ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var profiles []QualityProfile
	for rows.Next() {
		var qp QualityProfile
		var rankJSON string
		var upgrade int
		if err := rows.Scan(&qp.ID, &qp.Name, &rankJSON, &qp.PreferredSizeMin, &qp.PreferredSizeMax, &upgrade, &qp.CutoffFormat); err != nil {
			continue
		}
		json.Unmarshal([]byte(rankJSON), &qp.FormatRanking)
		qp.UpgradeAllowed = upgrade != 0
		profiles = append(profiles, qp)
	}
	return profiles, nil
}

func (d *DB) GetQualityProfile(id int64) (*QualityProfile, error) {
	var qp QualityProfile
	var rankJSON string
	var upgrade int
	err := d.db.QueryRow(
		"SELECT id, name, format_ranking, preferred_size_min, preferred_size_max, upgrade_allowed, cutoff_format FROM quality_profiles WHERE id = ?", id,
	).Scan(&qp.ID, &qp.Name, &rankJSON, &qp.PreferredSizeMin, &qp.PreferredSizeMax, &upgrade, &qp.CutoffFormat)
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(rankJSON), &qp.FormatRanking)
	qp.UpgradeAllowed = upgrade != 0
	return &qp, nil
}

func (d *DB) UpdateQualityProfile(qp *QualityProfile) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	rankingJSON, _ := json.Marshal(qp.FormatRanking)
	_, err := d.db.Exec(
		`UPDATE quality_profiles SET name=?, format_ranking=?, preferred_size_min=?, preferred_size_max=?, upgrade_allowed=?, cutoff_format=? WHERE id=?`,
		qp.Name, string(rankingJSON), qp.PreferredSizeMin, qp.PreferredSizeMax, boolToInt(qp.UpgradeAllowed), qp.CutoffFormat, qp.ID,
	)
	return err
}

func (d *DB) DeleteQualityProfile(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec("DELETE FROM quality_profiles WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("quality profile not found")
	}
	return nil
}
