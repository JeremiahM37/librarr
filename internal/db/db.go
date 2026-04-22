package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/webhook"

	_ "modernc.org/sqlite"
)

// ReadingHistoryEntry represents a reading history record.
type ReadingHistoryEntry struct {
	ID            int64      `json:"id"`
	UserID        int64      `json:"user_id"`
	BookTitle     string     `json:"book_title"`
	Author        string     `json:"author"`
	Format        string     `json:"format"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Rating        *int       `json:"rating,omitempty"`
	Notes         string     `json:"notes"`
	LibraryItemID *int64     `json:"library_item_id,omitempty"`
	Status        string     `json:"status"`
}

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

	// Requests table for book request workflow.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS requests (
		id TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		username TEXT NOT NULL,
		title TEXT NOT NULL,
		author TEXT,
		book_type TEXT NOT NULL DEFAULT 'ebook',
		status TEXT NOT NULL DEFAULT 'pending',
		cover_url TEXT,
		description TEXT,
		year TEXT,
		series_name TEXT,
		series_position TEXT,
		search_query TEXT,
		selected_result_id TEXT,
		download_id TEXT,
		attention_note TEXT,
		auto_approved INTEGER DEFAULT 0,
		retry_count INTEGER DEFAULT 0,
		created_at REAL NOT NULL,
		updated_at REAL NOT NULL
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_requests_user_id ON requests(user_id)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status)`)

	// Notifications table for in-app notifications.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS notifications (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		type TEXT NOT NULL,
		title TEXT NOT NULL,
		message TEXT,
		request_id TEXT,
		read INTEGER DEFAULT 0,
		created_at REAL NOT NULL
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_notifications_read ON notifications(user_id, read)`)

	// Uploads table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS uploads (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user TEXT NOT NULL DEFAULT '',
		filename TEXT NOT NULL DEFAULT '',
		original_name TEXT NOT NULL DEFAULT '',
		file_type TEXT NOT NULL DEFAULT '',
		file_size INTEGER NOT NULL DEFAULT 0,
		organized_to TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending',
		error TEXT NOT NULL DEFAULT '',
		created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_uploads_created ON uploads(created_at)`)

	// Webhook configs table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS webhook_configs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		url TEXT NOT NULL,
		type TEXT NOT NULL DEFAULT 'generic',
		enabled INTEGER NOT NULL DEFAULT 1,
		events TEXT NOT NULL DEFAULT '*'
	)`)

	// Reading history table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS reading_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL DEFAULT 0,
		book_title TEXT NOT NULL DEFAULT '',
		author TEXT NOT NULL DEFAULT '',
		format TEXT NOT NULL DEFAULT '',
		started_at REAL,
		finished_at REAL,
		rating INTEGER,
		notes TEXT NOT NULL DEFAULT '',
		library_item_id INTEGER,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_reading_history_user ON reading_history(user_id)`)

	// Series tracking table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS series_tracking (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		series_name TEXT NOT NULL UNIQUE,
		known_total INTEGER NOT NULL DEFAULT 0,
		owned_count INTEGER NOT NULL DEFAULT 0,
		last_checked REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)

	// Quality profiles table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS quality_profiles (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		format_ranking TEXT NOT NULL DEFAULT '[]',
		preferred_size_min INTEGER NOT NULL DEFAULT 0,
		preferred_size_max INTEGER NOT NULL DEFAULT 0,
		upgrade_allowed INTEGER NOT NULL DEFAULT 0,
		cutoff_format TEXT NOT NULL DEFAULT ''
	)`)

	// Blocklist table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS blocklist (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT NOT NULL DEFAULT '',
		source TEXT NOT NULL DEFAULT '',
		download_url TEXT NOT NULL DEFAULT '',
		info_hash TEXT NOT NULL DEFAULT '',
		reason TEXT NOT NULL DEFAULT '',
		created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_blocklist_info_hash ON blocklist(info_hash)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_blocklist_download_url ON blocklist(download_url)`)

	// Release profiles table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS release_profiles (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		must_contain TEXT NOT NULL DEFAULT '[]',
		must_not_contain TEXT NOT NULL DEFAULT '[]',
		preferred TEXT NOT NULL DEFAULT '[]',
		enabled INTEGER NOT NULL DEFAULT 1
	)`)

	// Tags tables.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS tags (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		color TEXT NOT NULL DEFAULT '#6366f1'
	)`)
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS item_tags (
		item_id INTEGER NOT NULL,
		tag_id INTEGER NOT NULL,
		PRIMARY KEY (item_id, tag_id),
		FOREIGN KEY (item_id) REFERENCES library_items(id) ON DELETE CASCADE,
		FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_item_tags_tag ON item_tags(tag_id)`)

	// Monitored authors table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS monitored_authors (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		last_checked REAL NOT NULL DEFAULT 0,
		last_book_found TEXT NOT NULL DEFAULT '',
		check_interval_days INTEGER NOT NULL DEFAULT 7
	)`)

	// Sessions table for persisting user sessions across restarts.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS sessions (
		token TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		username TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL DEFAULT '',
		expires_at REAL NOT NULL,
		created_at REAL NOT NULL DEFAULT (strftime('%s','now')),
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)`)

	// Runtime settings table.
	migrations = append(migrations, `CREATE TABLE IF NOT EXISTS app_settings (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL DEFAULT '',
		updated_by TEXT NOT NULL DEFAULT '',
		updated_at REAL NOT NULL DEFAULT (strftime('%s','now'))
	)`)
	migrations = append(migrations, `CREATE INDEX IF NOT EXISTS idx_app_settings_updated_at ON app_settings(updated_at)`)

	// Additive migrations — add columns that may not exist yet.
	addColumns := []string{
		`ALTER TABLE download_jobs ADD COLUMN status_history TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE activity_log ADD COLUMN user TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE reading_history ADD COLUMN status TEXT NOT NULL DEFAULT ''`,
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

// SetUserPassword updates a user's password hash by a string user ID.
func (d *DB) SetUserPassword(userID, passwordHash string) error {
	id, err := strconv.ParseInt(strings.TrimSpace(userID), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid user ID: %w", err)
	}
	return d.UpdateUserPassword(id, passwordHash)
}

// GetUserIDByEmail returns a user ID for the given email/username input.
// The current users schema does not require an email column, so this performs
// a case-insensitive username lookup as a compatible fallback.
func (d *DB) GetUserIDByEmail(email string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var userID int64
	err := d.db.QueryRow(`SELECT id FROM users WHERE username = ? COLLATE NOCASE`, strings.TrimSpace(email)).Scan(&userID)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(userID, 10), nil
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

// InvalidateAllUserSessions removes all active sessions for a user.
func (d *DB) InvalidateAllUserSessions(userID string) error {
	id, err := strconv.ParseInt(strings.TrimSpace(userID), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid user ID: %w", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err = d.db.Exec(`DELETE FROM sessions WHERE user_id = ?`, id)
	return err
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

// --- Sessions ---

// CreateSession inserts a new session token into the database.
func (d *DB) CreateSession(token string, userID int64, username, role string, expiresAt int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO sessions (token, user_id, username, role, expires_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		token, userID, username, role, expiresAt, float64(time.Now().Unix()),
	)
	return err
}

// GetSession retrieves a session by token. Returns userID, username, role, expiresAt, found, and error.
func (d *DB) GetSession(token string) (int64, string, string, int64, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	row := d.db.QueryRow(
		`SELECT user_id, username, role, expires_at FROM sessions WHERE token = ?`,
		token,
	)

	var userID int64
	var username, role string
	var expiresAt int64

	err := row.Scan(&userID, &username, &role, &expiresAt)
	if err == sql.ErrNoRows {
		return 0, "", "", 0, false, nil
	}
	if err != nil {
		return 0, "", "", 0, false, err
	}

	return userID, username, role, expiresAt, true, nil
}

// DeleteSession removes a session token from the database.
func (d *DB) DeleteSession(token string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`DELETE FROM sessions WHERE token = ?`, token)
	return err
}

// DeleteExpiredSessions removes all expired sessions from the database.
func (d *DB) DeleteExpiredSessions(now int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`DELETE FROM sessions WHERE expires_at <= ?`, now)
	return err
}

// HasAnySettings returns whether at least one runtime setting exists.
func (d *DB) HasAnySettings() (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var count int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM app_settings`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetAllSettings returns all runtime settings as key/value strings.
func (d *DB) GetAllSettings() (map[string]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rows, err := d.db.Query(`SELECT key, value FROM app_settings`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	settings := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		settings[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return settings, nil
}

// BulkSetSettings upserts multiple settings in a single transaction.
func (d *DB) BulkSetSettings(settings map[string]interface{}, updatedBy string, sensitiveKeys map[string]bool) error {
	_ = sensitiveKeys // caller already handles masking semantics before persistence.

	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO app_settings (key, value, updated_by, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			updated_by = excluded.updated_by,
			updated_at = excluded.updated_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := float64(time.Now().Unix())
	for key, raw := range settings {
		value, err := settingValueToString(raw)
		if err != nil {
			return fmt.Errorf("serialize setting %q: %w", key, err)
		}
		if _, err := stmt.Exec(key, value, updatedBy, now); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func settingValueToString(v interface{}) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", nil
	case string:
		return t, nil
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
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

// LogActivity appends a user-attributed event to the activity log.
func (d *DB) LogActivity(user, action, target, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO activity_log (event_type, title, detail, user, job_id) VALUES (?, ?, ?, ?, '')`,
		action, target, detail, user,
	)
	if err != nil {
		slog.Warn("LogActivity failed", "error", err)
	}
}

// GetActivityLog returns paginated activity entries with optional filters.
func (d *DB) GetActivityLog(user, action string, limit, offset int) ([]models.ActivityEntry, error) {
	query := "SELECT id, event_type, title, detail, user, timestamp FROM activity_log"
	var args []interface{}
	var conditions []string

	if user != "" {
		conditions = append(conditions, "user = ?")
		args = append(args, user)
	}
	if action != "" {
		conditions = append(conditions, "event_type = ?")
		args = append(args, action)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []models.ActivityEntry
	for rows.Next() {
		var e models.ActivityEntry
		var ts float64
		var userStr sql.NullString
		if err := rows.Scan(&e.ID, &e.Action, &e.Target, &e.Detail, &userStr, &ts); err != nil {
			continue
		}
		if userStr.Valid {
			e.User = userStr.String
		}
		e.CreatedAt = time.Unix(int64(ts), 0)
		entries = append(entries, e)
	}
	return entries, nil
}

// GetActivityLogCount returns the total number of activity entries matching filters.
func (d *DB) GetActivityLogCount(user, action string) (int, error) {
	query := "SELECT COUNT(*) FROM activity_log"
	var args []interface{}
	var conditions []string

	if user != "" {
		conditions = append(conditions, "user = ?")
		args = append(args, user)
	}
	if action != "" {
		conditions = append(conditions, "event_type = ?")
		args = append(args, action)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int
	err := d.db.QueryRow(query, args...).Scan(&count)
	return count, err
}

// --- Uploads ---

// SaveUpload records a file upload.
func (d *DB) SaveUpload(user, filename, originalName, fileType string, fileSize int64, organizedTo, status, errMsg string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`INSERT INTO uploads (user, filename, original_name, file_type, file_size, organized_to, status, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		user, filename, originalName, fileType, fileSize, organizedTo, status, errMsg,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetUploads returns recent uploads.
func (d *DB) GetUploads(limit, offset int) ([]models.UploadRecord, error) {
	rows, err := d.db.Query(
		`SELECT id, user, filename, original_name, file_type, file_size, organized_to, status, error, created_at
		 FROM uploads ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uploads []models.UploadRecord
	for rows.Next() {
		var u models.UploadRecord
		var ts float64
		if err := rows.Scan(&u.ID, &u.User, &u.Filename, &u.OriginalName, &u.FileType, &u.FileSize, &u.OrganizedTo, &u.Status, &u.Error, &ts); err != nil {
			continue
		}
		u.CreatedAt = time.Unix(int64(ts), 0)
		uploads = append(uploads, u)
	}
	return uploads, nil
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

// --- Requests ---

// CreateRequest inserts a new book request.
func (d *DB) CreateRequest(req *models.Request) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`INSERT INTO requests (id, user_id, username, title, author, book_type, status, cover_url, description, year, series_name, series_position, search_query, selected_result_id, download_id, attention_note, auto_approved, retry_count, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ID, req.UserID, req.Username, req.Title, req.Author, req.BookType,
		req.Status, req.CoverURL, req.Description, req.Year,
		req.SeriesName, req.SeriesPosition, req.SearchQuery,
		req.SelectedResultID, req.DownloadID, req.AttentionNote,
		boolToInt(req.AutoApproved), req.RetryCount,
		float64(req.CreatedAt.Unix()), float64(req.UpdatedAt.Unix()),
	)
	return err
}

// GetRequest retrieves a request by ID.
func (d *DB) GetRequest(id string) (*models.Request, error) {
	row := d.db.QueryRow(
		`SELECT id, user_id, username, title, author, book_type, status, cover_url, description, year, series_name, series_position, search_query, selected_result_id, download_id, attention_note, auto_approved, retry_count, created_at, updated_at
		 FROM requests WHERE id = ?`, id,
	)
	return scanRequest(row)
}

// ListRequests returns requests filtered by optional user ID and status.
// If userID is 0, all requests are returned (admin view).
func (d *DB) ListRequests(userID int64, status string, limit, offset int) ([]models.Request, error) {
	query := "SELECT id, user_id, username, title, author, book_type, status, cover_url, description, year, series_name, series_position, search_query, selected_result_id, download_id, attention_note, auto_approved, retry_count, created_at, updated_at FROM requests"
	var args []interface{}
	var conditions []string

	if userID > 0 {
		conditions = append(conditions, "user_id = ?")
		args = append(args, userID)
	}
	if status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, status)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var requests []models.Request
	for rows.Next() {
		req, err := scanRequestFromRows(rows)
		if err != nil {
			continue
		}
		requests = append(requests, *req)
	}
	return requests, nil
}

// CountRequests returns the number of requests matching the filters.
func (d *DB) CountRequests(userID int64, status string) (int, error) {
	query := "SELECT COUNT(*) FROM requests"
	var args []interface{}
	var conditions []string

	if userID > 0 {
		conditions = append(conditions, "user_id = ?")
		args = append(args, userID)
	}
	if status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, status)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int
	err := d.db.QueryRow(query, args...).Scan(&count)
	return count, err
}

// UpdateRequestStatus updates the status and optional fields of a request.
func (d *DB) UpdateRequestStatus(id, status string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE requests SET status = ?, updated_at = ? WHERE id = ?`,
		status, float64(time.Now().Unix()), id,
	)
	return err
}

// UpdateRequest updates mutable fields on a request.
func (d *DB) UpdateRequest(req *models.Request) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE requests SET status = ?, search_query = ?, selected_result_id = ?, download_id = ?, attention_note = ?, retry_count = ?, updated_at = ?
		 WHERE id = ?`,
		req.Status, req.SearchQuery, req.SelectedResultID, req.DownloadID,
		req.AttentionNote, req.RetryCount, float64(time.Now().Unix()), req.ID,
	)
	return err
}

// DeleteRequest removes a request by ID.
func (d *DB) DeleteRequest(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM requests WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("request not found")
	}
	return nil
}

func scanRequest(row *sql.Row) (*models.Request, error) {
	var req models.Request
	var createdAt, updatedAt float64
	var author, coverURL, description, year, seriesName, seriesPosition sql.NullString
	var searchQuery, selectedResultID, downloadID, attentionNote sql.NullString
	var autoApproved int

	err := row.Scan(
		&req.ID, &req.UserID, &req.Username, &req.Title, &author,
		&req.BookType, &req.Status, &coverURL, &description, &year,
		&seriesName, &seriesPosition, &searchQuery, &selectedResultID,
		&downloadID, &attentionNote, &autoApproved, &req.RetryCount,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}

	req.Author = nullStr(author)
	req.CoverURL = nullStr(coverURL)
	req.Description = nullStr(description)
	req.Year = nullStr(year)
	req.SeriesName = nullStr(seriesName)
	req.SeriesPosition = nullStr(seriesPosition)
	req.SearchQuery = nullStr(searchQuery)
	req.SelectedResultID = nullStr(selectedResultID)
	req.DownloadID = nullStr(downloadID)
	req.AttentionNote = nullStr(attentionNote)
	req.AutoApproved = autoApproved == 1
	req.CreatedAt = time.Unix(int64(createdAt), 0)
	req.UpdatedAt = time.Unix(int64(updatedAt), 0)
	return &req, nil
}

func scanRequestFromRows(rows *sql.Rows) (*models.Request, error) {
	var req models.Request
	var createdAt, updatedAt float64
	var author, coverURL, description, year, seriesName, seriesPosition sql.NullString
	var searchQuery, selectedResultID, downloadID, attentionNote sql.NullString
	var autoApproved int

	err := rows.Scan(
		&req.ID, &req.UserID, &req.Username, &req.Title, &author,
		&req.BookType, &req.Status, &coverURL, &description, &year,
		&seriesName, &seriesPosition, &searchQuery, &selectedResultID,
		&downloadID, &attentionNote, &autoApproved, &req.RetryCount,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}

	req.Author = nullStr(author)
	req.CoverURL = nullStr(coverURL)
	req.Description = nullStr(description)
	req.Year = nullStr(year)
	req.SeriesName = nullStr(seriesName)
	req.SeriesPosition = nullStr(seriesPosition)
	req.SearchQuery = nullStr(searchQuery)
	req.SelectedResultID = nullStr(selectedResultID)
	req.DownloadID = nullStr(downloadID)
	req.AttentionNote = nullStr(attentionNote)
	req.AutoApproved = autoApproved == 1
	req.CreatedAt = time.Unix(int64(createdAt), 0)
	req.UpdatedAt = time.Unix(int64(updatedAt), 0)
	return &req, nil
}

func nullStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// --- Notifications ---

// CreateNotification inserts a new notification.
func (d *DB) CreateNotification(n *models.Notification) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`INSERT INTO notifications (user_id, type, title, message, request_id, read, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, ?)`,
		n.UserID, n.Type, n.Title, n.Message, n.RequestID,
		float64(n.CreatedAt.Unix()),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetNotifications returns notifications for a user, newest first.
func (d *DB) GetNotifications(userID int64, limit, offset int) ([]models.Notification, error) {
	rows, err := d.db.Query(
		`SELECT id, user_id, type, title, message, request_id, read, created_at
		 FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		userID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifications []models.Notification
	for rows.Next() {
		n, err := scanNotificationFromRows(rows)
		if err != nil {
			continue
		}
		notifications = append(notifications, *n)
	}
	return notifications, nil
}

// CountUnreadNotifications returns the number of unread notifications for a user.
func (d *DB) CountUnreadNotifications(userID int64) (int, error) {
	var count int
	err := d.db.QueryRow(
		`SELECT COUNT(*) FROM notifications WHERE user_id = ? AND read = 0`, userID,
	).Scan(&count)
	return count, err
}

// MarkNotificationRead marks a single notification as read.
func (d *DB) MarkNotificationRead(id int64, userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`UPDATE notifications SET read = 1 WHERE id = ? AND user_id = ?`, id, userID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("notification not found")
	}
	return nil
}

// MarkAllNotificationsRead marks all notifications as read for a user.
func (d *DB) MarkAllNotificationsRead(userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(
		`UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0`, userID,
	)
	return err
}

// DeleteNotification removes a notification by ID (must belong to user).
func (d *DB) DeleteNotification(id int64, userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec(
		`DELETE FROM notifications WHERE id = ? AND user_id = ?`, id, userID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("notification not found")
	}
	return nil
}

func scanNotificationFromRows(rows *sql.Rows) (*models.Notification, error) {
	var n models.Notification
	var createdAt float64
	var readInt int
	var message, requestID sql.NullString

	err := rows.Scan(&n.ID, &n.UserID, &n.Type, &n.Title, &message, &requestID, &readInt, &createdAt)
	if err != nil {
		return nil, err
	}
	n.Message = nullStr(message)
	n.RequestID = nullStr(requestID)
	n.Read = readInt == 1
	n.CreatedAt = time.Unix(int64(createdAt), 0)
	return &n, nil
}

// --- Webhook Configs ---

// GetWebhookConfigs returns all webhook configurations.
func (d *DB) GetWebhookConfigs() ([]webhook.Config, error) {
	rows, err := d.db.Query("SELECT id, name, url, type, enabled, events FROM webhook_configs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []webhook.Config
	for rows.Next() {
		var c webhook.Config
		var enabled int
		if err := rows.Scan(&c.ID, &c.Name, &c.URL, &c.Type, &enabled, &c.Events); err != nil {
			continue
		}
		c.Enabled = enabled == 1
		configs = append(configs, c)
	}
	return configs, nil
}

// CreateWebhookConfig inserts a new webhook config.
func (d *DB) CreateWebhookConfig(c *webhook.Config) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	enabledInt := 0
	if c.Enabled {
		enabledInt = 1
	}

	result, err := d.db.Exec(
		`INSERT INTO webhook_configs (name, url, type, enabled, events) VALUES (?, ?, ?, ?, ?)`,
		c.Name, c.URL, c.Type, enabledInt, c.Events,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// DeleteWebhookConfig removes a webhook config by ID.
func (d *DB) DeleteWebhookConfig(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM webhook_configs WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("webhook not found")
	}
	return nil
}

// --- Reading History ---

// AddReadingHistory inserts a reading history entry.
func (d *DB) AddReadingHistory(userID int64, bookTitle, author, format string, startedAt, finishedAt *time.Time, rating *int, notes string, libraryItemID *int64) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var startedAtVal, finishedAtVal sql.NullFloat64
	if startedAt != nil {
		startedAtVal = sql.NullFloat64{Float64: float64(startedAt.Unix()), Valid: true}
	}
	if finishedAt != nil {
		finishedAtVal = sql.NullFloat64{Float64: float64(finishedAt.Unix()), Valid: true}
	}

	var ratingVal sql.NullInt64
	if rating != nil {
		ratingVal = sql.NullInt64{Int64: int64(*rating), Valid: true}
	}

	result, err := d.db.Exec(
		`INSERT INTO reading_history (user_id, book_title, author, format, started_at, finished_at, rating, notes, library_item_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		userID, bookTitle, author, format, startedAtVal, finishedAtVal, ratingVal, notes, libraryItemID,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetReadingHistory returns paginated reading history for a user.
func (d *DB) GetReadingHistory(userID int64, status string, limit, offset int) ([]ReadingHistoryEntry, error) {
	query := `SELECT id, user_id, book_title, author, format, started_at, finished_at, rating, notes, library_item_id
		FROM reading_history WHERE user_id = ?`
	args := []interface{}{userID}

	switch status {
	case "reading":
		query += " AND finished_at IS NULL"
	case "finished":
		query += " AND finished_at IS NOT NULL"
	}

	query += " ORDER BY COALESCE(finished_at, started_at) DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ReadingHistoryEntry
	for rows.Next() {
		var e ReadingHistoryEntry
		var startedAt, finishedAt sql.NullFloat64
		var rating sql.NullInt64
		var libraryItemID sql.NullInt64

		if err := rows.Scan(&e.ID, &e.UserID, &e.BookTitle, &e.Author, &e.Format, &startedAt, &finishedAt, &rating, &e.Notes, &libraryItemID); err != nil {
			continue
		}

		if startedAt.Valid {
			t := time.Unix(int64(startedAt.Float64), 0)
			e.StartedAt = &t
		}
		if finishedAt.Valid {
			t := time.Unix(int64(finishedAt.Float64), 0)
			e.FinishedAt = &t
			e.Status = "finished"
		} else {
			e.Status = "reading"
		}
		if rating.Valid {
			r := int(rating.Int64)
			e.Rating = &r
		}
		if libraryItemID.Valid {
			lid := libraryItemID.Int64
			e.LibraryItemID = &lid
		}

		entries = append(entries, e)
	}
	return entries, nil
}

// UpdateReadingHistory updates a history entry (finish date, rating, notes).
func (d *DB) UpdateReadingHistory(id, userID int64, finishedAt *time.Time, rating *int, notes *string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Build dynamic update.
	sets := []string{}
	args := []interface{}{}

	if finishedAt != nil {
		sets = append(sets, "finished_at = ?")
		args = append(args, float64(finishedAt.Unix()))
	}
	if rating != nil {
		sets = append(sets, "rating = ?")
		args = append(args, *rating)
	}
	if notes != nil {
		sets = append(sets, "notes = ?")
		args = append(args, *notes)
	}

	if len(sets) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE reading_history SET %s WHERE id = ? AND user_id = ?",
		strings.Join(sets, ", "))
	args = append(args, id, userID)

	result, err := d.db.Exec(query, args...)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("entry not found")
	}
	return nil
}

// DeleteReadingHistory removes a history entry by ID.
func (d *DB) DeleteReadingHistory(id, userID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM reading_history WHERE id = ? AND user_id = ?", id, userID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("entry not found")
	}
	return nil
}

// GetReadingStats returns reading statistics for a user.
func (d *DB) GetReadingStats(userID int64) (map[string]interface{}, error) {
	stats := map[string]interface{}{}

	// Total finished.
	var totalFinished int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NOT NULL", userID).Scan(&totalFinished)
	stats["total_finished"] = totalFinished

	// Currently reading.
	var currentlyReading int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NULL", userID).Scan(&currentlyReading)
	stats["currently_reading"] = currentlyReading

	// Average rating.
	var avgRating sql.NullFloat64
	d.db.QueryRow("SELECT AVG(rating) FROM reading_history WHERE user_id = ? AND rating IS NOT NULL", userID).Scan(&avgRating)
	if avgRating.Valid {
		stats["avg_rating"] = float64(int(avgRating.Float64*10)) / 10
	} else {
		stats["avg_rating"] = 0
	}

	// Books this month.
	monthStart := time.Now().AddDate(0, 0, -time.Now().Day()+1)
	var booksThisMonth int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NOT NULL AND finished_at >= ?",
		userID, float64(monthStart.Unix())).Scan(&booksThisMonth)
	stats["books_this_month"] = booksThisMonth

	// Books this year.
	yearStart := time.Date(time.Now().Year(), 1, 1, 0, 0, 0, 0, time.Local)
	var booksThisYear int
	d.db.QueryRow("SELECT COUNT(*) FROM reading_history WHERE user_id = ? AND finished_at IS NOT NULL AND finished_at >= ?",
		userID, float64(yearStart.Unix())).Scan(&booksThisYear)
	stats["books_this_year"] = booksThisYear

	return stats, nil
}

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

// --- Tags ---

// Tag represents a tag record.
type Tag struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
}

func (d *DB) CreateTag(name, color string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec("INSERT INTO tags (name, color) VALUES (?, ?)", name, color)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetTags() ([]Tag, error) {
	rows, err := d.db.Query("SELECT id, name, color FROM tags ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.ID, &t.Name, &t.Color); err != nil {
			continue
		}
		tags = append(tags, t)
	}
	return tags, nil
}

func (d *DB) DeleteTag(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Delete tag assignments first.
	d.db.Exec("DELETE FROM item_tags WHERE tag_id = ?", id)
	result, err := d.db.Exec("DELETE FROM tags WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("tag not found")
	}
	return nil
}

func (d *DB) AddItemTag(itemID, tagID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("INSERT OR IGNORE INTO item_tags (item_id, tag_id) VALUES (?, ?)", itemID, tagID)
	return err
}

func (d *DB) RemoveItemTag(itemID, tagID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("DELETE FROM item_tags WHERE item_id = ? AND tag_id = ?", itemID, tagID)
	return err
}

func (d *DB) GetItemTags(itemID int64) ([]Tag, error) {
	rows, err := d.db.Query(
		"SELECT t.id, t.name, t.color FROM tags t JOIN item_tags it ON t.id = it.tag_id WHERE it.item_id = ? ORDER BY t.name",
		itemID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.ID, &t.Name, &t.Color); err != nil {
			continue
		}
		tags = append(tags, t)
	}
	return tags, nil
}

func (d *DB) GetItemsByTag(tagID int64, limit, offset int) ([]models.LibraryItem, error) {
	rows, err := d.db.Query(
		`SELECT li.id, li.title, li.author, li.file_path, li.original_path, li.file_size, li.file_format,
		        li.media_type, li.source, li.source_id, li.metadata, li.added_at
		 FROM library_items li JOIN item_tags it ON li.id = it.item_id
		 WHERE it.tag_id = ? ORDER BY li.added_at DESC LIMIT ? OFFSET ?`,
		tagID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLibraryItems(rows)
}

// --- Monitored Authors ---

// MonitoredAuthor represents a monitored author record.
type MonitoredAuthor struct {
	ID                int64     `json:"id"`
	Name              string    `json:"name"`
	LastChecked       time.Time `json:"last_checked"`
	LastBookFound     string    `json:"last_book_found"`
	CheckIntervalDays int       `json:"check_interval_days"`
}

func (d *DB) AddMonitoredAuthor(name string, intervalDays int) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		"INSERT INTO monitored_authors (name, check_interval_days) VALUES (?, ?)",
		name, intervalDays,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) GetMonitoredAuthors() ([]MonitoredAuthor, error) {
	rows, err := d.db.Query("SELECT id, name, last_checked, last_book_found, check_interval_days FROM monitored_authors ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var authors []MonitoredAuthor
	for rows.Next() {
		var a MonitoredAuthor
		var lastChecked float64
		if err := rows.Scan(&a.ID, &a.Name, &lastChecked, &a.LastBookFound, &a.CheckIntervalDays); err != nil {
			continue
		}
		if lastChecked > 0 {
			a.LastChecked = time.Unix(int64(lastChecked), 0)
		}
		authors = append(authors, a)
	}
	return authors, nil
}

func (d *DB) DeleteMonitoredAuthor(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
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

// GetDBPath returns the file path to the database.
func (d *DB) GetDBPath() string {
	return d.path
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
