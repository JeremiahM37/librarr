package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

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
