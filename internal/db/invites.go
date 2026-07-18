package db

import (
	"fmt"
	"time"
)

// --- Invite Codes ---

// CreateInviteCode generates a new invite code.
func (d *DB) CreateInviteCode(code string, createdBy int64, role string, maxUses int, expiresAt *float64) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	result, err := d.db.Exec(
		"INSERT INTO invite_codes (code, created_by, role, max_uses, expires_at) VALUES (?, ?, ?, ?, ?)",
		code, createdBy, role, maxUses, expiresAt,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// ValidateInviteCode checks if an invite code is valid (exists, not expired, uses remaining).
// Returns the role the new user should get, or an error.
func (d *DB) ValidateInviteCode(code string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var role string
	var maxUses, uses int
	var expiresAt *float64
	err := d.db.QueryRow(
		"SELECT role, max_uses, uses, expires_at FROM invite_codes WHERE code = ?", code,
	).Scan(&role, &maxUses, &uses, &expiresAt)
	if err != nil {
		return "", fmt.Errorf("invalid invite code")
	}
	if uses >= maxUses {
		return "", fmt.Errorf("invite code has been used")
	}
	if expiresAt != nil {
		now := float64(time.Now().Unix())
		if now > *expiresAt {
			return "", fmt.Errorf("invite code has expired")
		}
	}
	return role, nil
}

// UseInviteCode increments the use count.
func (d *DB) UseInviteCode(code string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE invite_codes SET uses = uses + 1 WHERE code = ?", code)
	return err
}

// ListInviteCodes returns all invite codes for admin display.
func (d *DB) ListInviteCodes() ([]map[string]interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rows, err := d.db.Query("SELECT id, code, role, max_uses, uses, expires_at, created_at FROM invite_codes ORDER BY id DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var codes []map[string]interface{}
	for rows.Next() {
		var id, maxUses, uses int
		var code, role string
		var expiresAt *float64
		var createdAt float64
		rows.Scan(&id, &code, &role, &maxUses, &uses, &expiresAt, &createdAt)
		codes = append(codes, map[string]interface{}{
			"id": id, "code": code, "role": role,
			"max_uses": maxUses, "uses": uses,
			"expires_at": expiresAt, "created_at": createdAt,
		})
	}
	return codes, nil
}

// DeleteInviteCode removes an invite code.
func (d *DB) DeleteInviteCode(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("DELETE FROM invite_codes WHERE id = ?", id)
	return err
}
