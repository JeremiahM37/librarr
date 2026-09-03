package db

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/JeremiahM37/librarr/internal/quality"
)

// --- Quality Profiles ---

// QualityProfile represents a quality profile record.
type QualityProfile struct {
	ID               int64    `json:"id"`
	Name             string   `json:"name"`
	MediaType        string   `json:"media_type"`
	FormatRanking    []string `json:"format_ranking"`
	PreferredSizeMin int64    `json:"preferred_size_min"`
	PreferredSizeMax int64    `json:"preferred_size_max"`
	UpgradeAllowed   bool     `json:"upgrade_allowed"`
	CutoffFormat     string   `json:"cutoff_format"`
	// Builtin profiles are seeded per media type and cannot be deleted; they
	// are what an item with no explicit profile uses.
	Builtin bool `json:"builtin"`
}

// Profile converts the record into the decision engine's view of it.
func (qp QualityProfile) Profile() quality.Profile {
	return quality.Profile{
		ID:               qp.ID,
		Name:             qp.Name,
		MediaType:        qp.MediaType,
		Ranking:          qp.FormatRanking,
		Cutoff:           qp.CutoffFormat,
		UpgradesAllowed:  qp.UpgradeAllowed,
		PreferredSizeMin: qp.PreferredSizeMin,
		PreferredSizeMax: qp.PreferredSizeMax,
	}
}

const qualityProfileColumns = "id, name, media_type, format_ranking, preferred_size_min, preferred_size_max, upgrade_allowed, cutoff_format, builtin"

func normalizeProfile(qp *QualityProfile) {
	if qp.MediaType == "" {
		qp.MediaType = "ebook"
	}
	for i, f := range qp.FormatRanking {
		qp.FormatRanking[i] = quality.Normalize(f)
	}
	qp.CutoffFormat = quality.Normalize(qp.CutoffFormat)
}

func (d *DB) CreateQualityProfile(qp *QualityProfile) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.createQualityProfileLocked(qp)
}

func (d *DB) createQualityProfileLocked(qp *QualityProfile) (int64, error) {
	normalizeProfile(qp)
	rankingJSON, _ := json.Marshal(qp.FormatRanking)
	result, err := d.db.Exec(
		`INSERT INTO quality_profiles (name, media_type, format_ranking, preferred_size_min, preferred_size_max, upgrade_allowed, cutoff_format, builtin)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		qp.Name, qp.MediaType, string(rankingJSON), qp.PreferredSizeMin, qp.PreferredSizeMax, boolToInt(qp.UpgradeAllowed), qp.CutoffFormat, boolToInt(qp.Builtin),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetQualityProfiles() ([]QualityProfile, error) {
	rows, err := d.db.Query("SELECT " + qualityProfileColumns + " FROM quality_profiles ORDER BY builtin DESC, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var profiles []QualityProfile
	for rows.Next() {
		qp, err := scanQualityProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, qp)
	}
	return profiles, rows.Err()
}

func (d *DB) GetQualityProfile(id int64) (*QualityProfile, error) {
	rows, err := d.db.Query("SELECT "+qualityProfileColumns+" FROM quality_profiles WHERE id = ?", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	qp, err := scanQualityProfile(rows)
	if err != nil {
		return nil, err
	}
	return &qp, nil
}

// GetDefaultQualityProfile returns the built-in profile for a media type.
func (d *DB) GetDefaultQualityProfile(mediaType string) (*QualityProfile, error) {
	if mediaType == "" {
		mediaType = "ebook"
	}
	rows, err := d.db.Query("SELECT "+qualityProfileColumns+" FROM quality_profiles WHERE builtin = 1 AND media_type = ? ORDER BY id LIMIT 1", mediaType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	qp, err := scanQualityProfile(rows)
	if err != nil {
		return nil, err
	}
	return &qp, nil
}

// ResolveQualityProfile returns the profile a wanted item should use: its own
// when set and still present, otherwise the built-in default for its media
// type, otherwise the package default so a decision is always possible.
func (d *DB) ResolveQualityProfile(profileID int64, mediaType string) QualityProfile {
	if profileID != 0 {
		if qp, err := d.GetQualityProfile(profileID); err == nil {
			return *qp
		}
	}
	if qp, err := d.GetDefaultQualityProfile(mediaType); err == nil {
		return *qp
	}
	for _, p := range quality.DefaultProfiles() {
		if p.MediaType == mediaType || (mediaType == "" && p.MediaType == "ebook") {
			return QualityProfile{Name: p.Name, MediaType: p.MediaType, FormatRanking: p.Ranking, CutoffFormat: p.Cutoff, UpgradeAllowed: p.UpgradesAllowed, Builtin: true}
		}
	}
	p := quality.DefaultProfiles()[0]
	return QualityProfile{Name: p.Name, MediaType: p.MediaType, FormatRanking: p.Ranking, CutoffFormat: p.Cutoff, UpgradeAllowed: p.UpgradesAllowed, Builtin: true}
}

type qpScanner interface {
	Scan(dest ...any) error
}

func scanQualityProfile(row qpScanner) (QualityProfile, error) {
	var qp QualityProfile
	var rankJSON string
	var upgrade, builtin int
	if err := row.Scan(&qp.ID, &qp.Name, &qp.MediaType, &rankJSON, &qp.PreferredSizeMin, &qp.PreferredSizeMax, &upgrade, &qp.CutoffFormat, &builtin); err != nil {
		return qp, err
	}
	_ = json.Unmarshal([]byte(rankJSON), &qp.FormatRanking)
	if qp.FormatRanking == nil {
		qp.FormatRanking = []string{}
	}
	qp.UpgradeAllowed = upgrade != 0
	qp.Builtin = builtin != 0
	return qp, nil
}

// UpdateQualityProfile rewrites the editable fields. The builtin flag and
// media type of a built-in profile are preserved: the defaults stay one per
// media type no matter what a client sends.
func (d *DB) UpdateQualityProfile(qp *QualityProfile) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var builtin int
	var mediaType string
	if err := d.db.QueryRow("SELECT builtin, media_type FROM quality_profiles WHERE id = ?", qp.ID).Scan(&builtin, &mediaType); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("quality profile not found")
		}
		return err
	}
	if builtin != 0 {
		qp.Builtin = true
		qp.MediaType = mediaType
	}
	normalizeProfile(qp)
	rankingJSON, _ := json.Marshal(qp.FormatRanking)
	_, err := d.db.Exec(
		`UPDATE quality_profiles SET name=?, media_type=?, format_ranking=?, preferred_size_min=?, preferred_size_max=?, upgrade_allowed=?, cutoff_format=? WHERE id=?`,
		qp.Name, qp.MediaType, string(rankingJSON), qp.PreferredSizeMin, qp.PreferredSizeMax, boolToInt(qp.UpgradeAllowed), qp.CutoffFormat, qp.ID,
	)
	return err
}

// ErrBuiltinProfile is returned when deleting a built-in profile.
var ErrBuiltinProfile = fmt.Errorf("built-in quality profiles cannot be deleted")

// DeleteQualityProfile removes a custom profile. Wanted items that used it
// fall back to the built-in default for their media type.
func (d *DB) DeleteQualityProfile(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var builtin int
	if err := d.db.QueryRow("SELECT builtin FROM quality_profiles WHERE id = ?", id).Scan(&builtin); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("quality profile not found")
		}
		return err
	}
	if builtin != 0 {
		return ErrBuiltinProfile
	}
	if _, err := d.db.Exec("UPDATE wishlist SET quality_profile_id = 0 WHERE quality_profile_id = ?", id); err != nil {
		return err
	}
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

// seedDefaultQualityProfiles inserts the built-in profile for any media type
// that lacks one. It runs on every start and is idempotent, so a database
// created before profiles were per-media-type gains its defaults on upgrade
// while user edits to existing defaults are left alone.
func (d *DB) seedDefaultQualityProfiles() error {
	for _, p := range quality.DefaultProfiles() {
		var n int
		if err := d.db.QueryRow("SELECT COUNT(*) FROM quality_profiles WHERE builtin = 1 AND media_type = ?", p.MediaType).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		qp := &QualityProfile{
			Name:           p.Name,
			MediaType:      p.MediaType,
			FormatRanking:  append([]string(nil), p.Ranking...),
			CutoffFormat:   p.Cutoff,
			UpgradeAllowed: p.UpgradesAllowed,
			Builtin:        true,
		}
		if _, err := d.createQualityProfileLocked(qp); err != nil {
			return err
		}
	}
	return nil
}
