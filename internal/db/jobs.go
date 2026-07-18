package db

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// --- Download Jobs ---

// SaveJob persists a download job.
func (d *DB) SaveJob(job *models.DownloadJob) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	historyJSON, _ := json.Marshal(job.StatusHistory)
	if historyJSON == nil {
		historyJSON = []byte("[]")
	}

	_, err := d.db.Exec(
		`INSERT OR REPLACE INTO download_jobs (id, title, source, status, detail, error, url, md5, source_id, media_type, retry_count, max_retries, status_history, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.Title, job.Source, job.Status, job.Detail, job.Error,
		job.URL, job.MD5, job.SourceID, job.MediaType,
		job.RetryCount, job.MaxRetries, string(historyJSON),
		float64(job.CreatedAt.Unix()), float64(job.UpdatedAt.Unix()),
	)
	return err
}

// UpdateJobStatus updates the status and detail of a job.
func (d *DB) UpdateJobStatus(id, status, detail, errMsg string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE download_jobs SET status = ?, detail = ?, error = ?, updated_at = ? WHERE id = ?`,
		status, detail, errMsg, float64(time.Now().Unix()), id,
	)
	return err
}

// GetJob retrieves a download job by ID.
func (d *DB) GetJob(id string) (*models.DownloadJob, error) {
	row := d.db.QueryRow("SELECT id, title, source, status, detail, error, url, md5, source_id, media_type, retry_count, max_retries, status_history, created_at, updated_at FROM download_jobs WHERE id = ?", id)
	return scanJob(row)
}

// GetJobs returns all download jobs.
func (d *DB) GetJobs() ([]models.DownloadJob, error) {
	rows, err := d.db.Query("SELECT id, title, source, status, detail, error, url, md5, source_id, media_type, retry_count, max_retries, status_history, created_at, updated_at FROM download_jobs ORDER BY created_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.DownloadJob
	for rows.Next() {
		j, err := scanJobFromRows(rows)
		if err != nil {
			continue
		}
		jobs = append(jobs, *j)
	}
	return jobs, nil
}

// DeleteJob removes a download job.
func (d *DB) DeleteJob(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec("DELETE FROM download_jobs WHERE id = ?", id)
	return err
}

// ClearFinishedJobs removes completed, error, and dead_letter jobs.
func (d *DB) ClearFinishedJobs() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM download_jobs WHERE status IN ('completed', 'error', 'dead_letter')")
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// GetStats returns collection statistics.
func scanJob(row *sql.Row) (*models.DownloadJob, error) {
	var j models.DownloadJob
	var createdAt, updatedAt float64
	var historyJSON string
	err := row.Scan(&j.ID, &j.Title, &j.Source, &j.Status, &j.Detail, &j.Error,
		&j.URL, &j.MD5, &j.SourceID, &j.MediaType,
		&j.RetryCount, &j.MaxRetries, &historyJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	j.CreatedAt = time.Unix(int64(createdAt), 0)
	j.UpdatedAt = time.Unix(int64(updatedAt), 0)
	if historyJSON != "" {
		_ = json.Unmarshal([]byte(historyJSON), &j.StatusHistory)
	}
	return &j, nil
}

func scanJobFromRows(rows *sql.Rows) (*models.DownloadJob, error) {
	var j models.DownloadJob
	var createdAt, updatedAt float64
	var historyJSON string
	err := rows.Scan(&j.ID, &j.Title, &j.Source, &j.Status, &j.Detail, &j.Error,
		&j.URL, &j.MD5, &j.SourceID, &j.MediaType,
		&j.RetryCount, &j.MaxRetries, &historyJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	j.CreatedAt = time.Unix(int64(createdAt), 0)
	j.UpdatedAt = time.Unix(int64(updatedAt), 0)
	if historyJSON != "" {
		_ = json.Unmarshal([]byte(historyJSON), &j.StatusHistory)
	}
	return &j, nil
}
