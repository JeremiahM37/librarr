package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"

	_ "modernc.org/sqlite"
)

// DB wraps a SQLite database for library tracking and download jobs.
type DB struct {
	db   *sql.DB
	mu   sync.Mutex
	path string
}

// New opens (or creates) the SQLite database at the given path.
func New(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=10000")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	d := &DB{db: db, path: path}
	if err := d.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	slog.Info("database initialized", "path", path)
	return d, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS library_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			author TEXT NOT NULL DEFAULT '',
			file_path TEXT NOT NULL DEFAULT '',
			original_path TEXT NOT NULL DEFAULT '',
			file_size INTEGER NOT NULL DEFAULT 0,
			file_format TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			source TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			metadata TEXT NOT NULL DEFAULT '{}',
			added_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS activity_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_type TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT '',
			library_item_id INTEGER,
			job_id TEXT NOT NULL DEFAULT '',
			timestamp REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS download_jobs (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'queued',
			detail TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			url TEXT NOT NULL DEFAULT '',
			md5 TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 2,
			status_history TEXT NOT NULL DEFAULT '[]',
			created_at REAL NOT NULL DEFAULT (strftime('%s','now')),
			updated_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS wishlist (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL DEFAULT '',
			author TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT 'ebook',
			added_at REAL NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_library_items_source_id ON library_items(source_id)`,
		`CREATE INDEX IF NOT EXISTS idx_library_items_media_type ON library_items(media_type)`,
		`CREATE INDEX IF NOT EXISTS idx_activity_log_timestamp ON activity_log(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs(status)`,
	}

	// Users table for multi-user auth.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT UNIQUE NOT NULL,
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'user',
		totp_secret TEXT,
		totp_enabled INTEGER DEFAULT 0,
		created_at REAL NOT NULL DEFAULT (strftime('%s','now')),
		last_login REAL
	)`)
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS backup_codes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		code_hash TEXT NOT NULL,
		used INTEGER DEFAULT 0,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`)

	// Additive migrations — add columns that may not exist yet.
	addColumns := []string{
		`ALTER TABLE download_jobs ADD COLUMN status_history TEXT NOT NULL DEFAULT '[]'`,
	}
	for _, stmt := range addColumns {
		// Ignore "duplicate column" errors.
		d.db.Exec(stmt)
	}

	for _, m := range migrations {
		if _, err := d.db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}
	return nil
}

// --- Users ---

// CreateUser inserts a new user. Returns the new user ID.
func (d *DB) CreateUser(username, passwordHash, role string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)`,
		username, passwordHash, role, float64(time.Now().Unix()),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetUser retrieves a user by ID.
func (d *DB) GetUser(id int64) (*models.User, error) {
	row := d.db.QueryRow(
		`SELECT id, username, password_hash, role, totp_secret, totp_enabled, created_at, last_login FROM users WHERE id = ?`, id,
	)
	return scanUser(row)
}

// GetUserByUsername retrieves a user by username (case-insensitive).
func (d *DB) GetUserByUsername(username string) (*models.User, error) {
	row := d.db.QueryRow(
		`SELECT id, username, password_hash, role, totp_secret, totp_enabled, created_at, last_login FROM users WHERE username = ? COLLATE NOCASE`, username,
	)
	return scanUser(row)
}

// ListUsers returns all users.
func (d *DB) ListUsers() ([]models.User, error) {
	rows, err := d.db.Query(
		`SELECT id, username, password_hash, role, totp_secret, totp_enabled, created_at, last_login FROM users ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		u, err := scanUserFromRows(rows)
		if err != nil {
			continue
		}
		users = append(users, *u)
	}
	return users, nil
}

// CountUsers returns the total number of users.
func (d *DB) CountUsers() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	return count, err
}

// UpdateUser updates a user's username and role.
func (d *DB) UpdateUser(id int64, username, role string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE users SET username = ?, role = ? WHERE id = ?`, username, role, id)
	return err
}

// UpdateUserPassword updates only the password hash.
func (d *DB) UpdateUserPassword(id int64, passwordHash string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE users SET password_hash = ? WHERE id = ?`, passwordHash, id)
	return err
}

// DeleteUser removes a user by ID.
func (d *DB) DeleteUser(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM users WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("user not found")
	}
	return nil
}

// SetTOTPSecret stores the TOTP secret for a user (does not enable it yet).
func (d *DB) SetTOTPSecret(userID int64, secret string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE users SET totp_secret = ? WHERE id = ?`, secret, userID)
	return err
}

// EnableTOTP enables TOTP for a user.
func (d *DB) EnableTOTP(userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE users SET totp_enabled = 1 WHERE id = ?`, userID)
	return err
}

// DisableTOTP disables TOTP and clears the secret.
func (d *DB) DisableTOTP(userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE users SET totp_enabled = 0, totp_secret = NULL WHERE id = ?`, userID)
	if err != nil {
		return err
	}
	// Also delete backup codes.
	_, err = d.db.Exec(`DELETE FROM backup_codes WHERE user_id = ?`, userID)
	return err
}

// UpdateLastLogin updates the last_login timestamp for a user.
func (d *DB) UpdateLastLogin(userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`UPDATE users SET last_login = ? WHERE id = ?`, float64(time.Now().Unix()), userID)
	return err
}

// SaveBackupCodes stores hashed backup codes for a user.
func (d *DB) SaveBackupCodes(userID int64, codeHashes []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Delete old codes first.
	d.db.Exec(`DELETE FROM backup_codes WHERE user_id = ?`, userID)

	for _, hash := range codeHashes {
		_, err := d.db.Exec(`INSERT INTO backup_codes (user_id, code_hash) VALUES (?, ?)`, userID, hash)
		if err != nil {
			return err
		}
	}
	return nil
}

// UseBackupCode checks if a backup code matches any unused code for the user. If found, marks it used.
func (d *DB) UseBackupCode(userID int64, codeHash string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var id int64
	err := d.db.QueryRow(`SELECT id FROM backup_codes WHERE user_id = ? AND code_hash = ? AND used = 0`, userID, codeHash).Scan(&id)
	if err != nil {
		return false, nil
	}
	_, err = d.db.Exec(`UPDATE backup_codes SET used = 1 WHERE id = ?`, id)
	return err == nil, err
}

func scanUser(row *sql.Row) (*models.User, error) {
	var u models.User
	var createdAt float64
	var lastLogin sql.NullFloat64
	var totpSecret sql.NullString

	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &totpSecret, &u.TOTPEnabled, &createdAt, &lastLogin)
	if err != nil {
		return nil, err
	}
	u.CreatedAt = time.Unix(int64(createdAt), 0)
	if lastLogin.Valid {
		u.LastLogin = time.Unix(int64(lastLogin.Float64), 0)
	}
	if totpSecret.Valid {
		u.TOTPSecret = totpSecret.String
	}
	return &u, nil
}

func scanUserFromRows(rows *sql.Rows) (*models.User, error) {
	var u models.User
	var createdAt float64
	var lastLogin sql.NullFloat64
	var totpSecret sql.NullString

	err := rows.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &totpSecret, &u.TOTPEnabled, &createdAt, &lastLogin)
	if err != nil {
		return nil, err
	}
	u.CreatedAt = time.Unix(int64(createdAt), 0)
	if lastLogin.Valid {
		u.LastLogin = time.Unix(int64(lastLogin.Float64), 0)
	}
	if totpSecret.Valid {
		u.TOTPSecret = totpSecret.String
	}
	return &u, nil
}

// --- Library Items ---

// AddItem records a successfully processed book.
func (d *DB) AddItem(item *models.LibraryItem) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	metadata := item.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	result, err := d.db.Exec(
		`INSERT INTO library_items (title, author, file_path, original_path, file_size, file_format, media_type, source, source_id, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.Title, item.Author, item.FilePath, item.OriginalPath,
		item.FileSize, item.FileFormat, item.MediaType,
		item.Source, item.SourceID, metadata,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// HasSourceID checks if a source_id already exists.
func (d *DB) HasSourceID(sourceID string) bool {
	if sourceID == "" {
		return false
	}
	var exists int
	err := d.db.QueryRow("SELECT 1 FROM library_items WHERE source_id = ?", sourceID).Scan(&exists)
	return err == nil
}

// FindByTitle performs a case-insensitive title lookup.
func (d *DB) FindByTitle(title string) ([]models.LibraryItem, error) {
	rows, err := d.db.Query("SELECT * FROM library_items WHERE title = ? COLLATE NOCASE", title)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}

// GetItems returns a paginated list of library items, newest first.
func (d *DB) GetItems(mediaType string, limit, offset int) ([]models.LibraryItem, error) {
	var rows *sql.Rows
	var err error
	if mediaType != "" {
		rows, err = d.db.Query(
			"SELECT * FROM library_items WHERE media_type = ? ORDER BY added_at DESC LIMIT ? OFFSET ?",
			mediaType, limit, offset,
		)
	} else {
		rows, err = d.db.Query(
			"SELECT * FROM library_items ORDER BY added_at DESC LIMIT ? OFFSET ?",
			limit, offset,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}

// CountItems counts library items, optionally filtered by media type.
func (d *DB) CountItems(mediaType string) (int, error) {
	var count int
	var err error
	if mediaType != "" {
		err = d.db.QueryRow("SELECT COUNT(*) FROM library_items WHERE media_type = ?", mediaType).Scan(&count)
	} else {
		err = d.db.QueryRow("SELECT COUNT(*) FROM library_items").Scan(&count)
	}
	return count, err
}

// --- Activity Log ---

// LogEvent appends an event to the activity log.
func (d *DB) LogEvent(eventType, title, detail string, libraryItemID *int64, jobID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO activity_log (event_type, title, detail, library_item_id, job_id) VALUES (?, ?, ?, ?, ?)`,
		eventType, title, detail, libraryItemID, jobID,
	)
	return err
}

// GetActivity returns recent activity, newest first.
func (d *DB) GetActivity(limit, offset int) ([]models.ActivityEvent, error) {
	rows, err := d.db.Query(
		"SELECT id, event_type, title, detail, library_item_id, job_id, timestamp FROM activity_log ORDER BY timestamp DESC LIMIT ? OFFSET ?",
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.ActivityEvent
	for rows.Next() {
		var e models.ActivityEvent
		var ts float64
		if err := rows.Scan(&e.ID, &e.EventType, &e.Title, &e.Detail, &e.LibraryItemID, &e.JobID, &ts); err != nil {
			continue
		}
		e.Timestamp = time.Unix(int64(ts), 0)
		events = append(events, e)
	}
	return events, nil
}

// CountActivity returns the total number of activity events.
func (d *DB) CountActivity() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM activity_log").Scan(&count)
	return count, err
}

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
		`INSERT OR REPLACE INTO download_jobs (id, title, source, status, detail, error, url, md5, media_type, retry_count, max_retries, status_history, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.Title, job.Source, job.Status, job.Detail, job.Error,
		job.URL, job.MD5, job.MediaType,
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
	row := d.db.QueryRow("SELECT id, title, source, status, detail, error, url, md5, media_type, retry_count, max_retries, status_history, created_at, updated_at FROM download_jobs WHERE id = ?", id)
	return scanJob(row)
}

// GetJobs returns all download jobs.
func (d *DB) GetJobs() ([]models.DownloadJob, error) {
	rows, err := d.db.Query("SELECT id, title, source, status, detail, error, url, md5, media_type, retry_count, max_retries, status_history, created_at, updated_at FROM download_jobs ORDER BY created_at DESC")
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
func (d *DB) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	ebookCount, _ := d.CountItems("ebook")
	audiobookCount, _ := d.CountItems("audiobook")
	mangaCount, _ := d.CountItems("manga")
	totalCount, _ := d.CountItems("")
	activityCount, _ := d.CountActivity()

	stats["total_items"] = totalCount
	stats["ebooks"] = ebookCount
	stats["audiobooks"] = audiobookCount
	stats["manga"] = mangaCount
	stats["activity_events"] = activityCount

	return stats, nil
}

// DeleteItem removes a library item by ID.
func (d *DB) DeleteItem(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM library_items WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("item not found")
	}
	return nil
}

// --- Wishlist ---

// AddWishlistItem adds an item to the wishlist.
func (d *DB) AddWishlistItem(title, author, mediaType string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if mediaType == "" {
		mediaType = "ebook"
	}

	result, err := d.db.Exec(
		`INSERT INTO wishlist (title, author, media_type) VALUES (?, ?, ?)`,
		title, author, mediaType,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetWishlist returns all wishlist items.
func (d *DB) GetWishlist() ([]models.WishlistItem, error) {
	rows, err := d.db.Query("SELECT id, title, author, media_type, added_at FROM wishlist ORDER BY added_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []models.WishlistItem
	for rows.Next() {
		var item models.WishlistItem
		var ts float64
		if err := rows.Scan(&item.ID, &item.Title, &item.Author, &item.MediaType, &ts); err != nil {
			continue
		}
		item.AddedAt = time.Unix(int64(ts), 0)
		items = append(items, item)
	}
	return items, nil
}

// DeleteWishlistItem removes a wishlist item by ID.
func (d *DB) DeleteWishlistItem(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM wishlist WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("wishlist item not found")
	}
	return nil
}

func scanLibraryItems(rows *sql.Rows) ([]models.LibraryItem, error) {
	var items []models.LibraryItem
	for rows.Next() {
		var item models.LibraryItem
		var ts float64
		var metadataStr string
		if err := rows.Scan(
			&item.ID, &item.Title, &item.Author, &item.FilePath,
			&item.OriginalPath, &item.FileSize, &item.FileFormat,
			&item.MediaType, &item.Source, &item.SourceID,
			&metadataStr, &ts,
		); err != nil {
			continue
		}
		item.AddedAt = time.Unix(int64(ts), 0)
		item.Metadata = metadataStr
		items = append(items, item)
	}
	return items, nil
}

func scanJob(row *sql.Row) (*models.DownloadJob, error) {
	var j models.DownloadJob
	var createdAt, updatedAt float64
	var historyJSON string
	err := row.Scan(&j.ID, &j.Title, &j.Source, &j.Status, &j.Detail, &j.Error,
		&j.URL, &j.MD5, &j.MediaType,
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
		&j.URL, &j.MD5, &j.MediaType,
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

// ItemToJSON converts a LibraryItem to a JSON-friendly map.
func ItemToJSON(item models.LibraryItem) map[string]interface{} {
	m := map[string]interface{}{
		"id":            item.ID,
		"title":         item.Title,
		"author":        item.Author,
		"file_path":     item.FilePath,
		"original_path": item.OriginalPath,
		"file_size":     item.FileSize,
		"file_format":   item.FileFormat,
		"media_type":    item.MediaType,
		"source":        item.Source,
		"source_id":     item.SourceID,
		"added_at":      item.AddedAt.Format(time.RFC3339),
	}
	if item.Metadata != "" && item.Metadata != "{}" {
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(item.Metadata), &meta); err == nil {
			m["metadata"] = meta
		}
	}
	return m
}
