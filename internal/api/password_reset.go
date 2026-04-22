package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/db"
)

// PasswordResetManager handles password reset token generation, validation, and expiry.
type PasswordResetManager struct {
	db     *db.DB
	mu     sync.Mutex
	tokens map[string]*resetToken // token hash -> reset token
}

// resetToken tracks a password reset request.
type resetToken struct {
	userID    string
	email     string
	expiresAt time.Time
	used      bool
}

// NewPasswordResetManager creates a new password reset manager.
func NewPasswordResetManager(database *db.DB) *PasswordResetManager {
	prm := &PasswordResetManager{
		db:     database,
		tokens: make(map[string]*resetToken),
	}

	// Periodic cleanup of expired tokens
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		for range ticker.C {
			prm.cleanupExpiredTokens()
		}
	}()

	return prm
}

// GenerateResetToken creates a new password reset token for the given user.
// The token is 32 bytes of random data, hex-encoded.
// Returns the token (to send to user) and any error.
func (prm *PasswordResetManager) GenerateResetToken(userID, email string) (string, error) {
	prm.mu.Lock()
	defer prm.mu.Unlock()

	// Generate 32 bytes of random data
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("failed to generate reset token: %w", err)
	}

	tokenStr := hex.EncodeToString(tokenBytes)
	tokenHash := sha256.Sum256([]byte(tokenStr))
	tokenHashStr := hex.EncodeToString(tokenHash[:])

	// Store token (24 hour expiry)
	prm.tokens[tokenHashStr] = &resetToken{
		userID:    userID,
		email:     email,
		expiresAt: time.Now().Add(24 * time.Hour),
		used:      false,
	}

	slog.Info("password reset token generated", "userID", userID, "email", email)

	return tokenStr, nil
}

// ValidateResetToken checks if a reset token is valid (exists, not expired, not used).
// Returns the userID and any error.
func (prm *PasswordResetManager) ValidateResetToken(token string) (string, error) {
	prm.mu.Lock()
	defer prm.mu.Unlock()

	if token == "" {
		return "", fmt.Errorf("reset token is required")
	}

	// Hash the token
	tokenHash := sha256.Sum256([]byte(token))
	tokenHashStr := hex.EncodeToString(tokenHash[:])

	rt, exists := prm.tokens[tokenHashStr]
	if !exists {
		return "", fmt.Errorf("invalid reset token")
	}

	if time.Now().After(rt.expiresAt) {
		delete(prm.tokens, tokenHashStr)
		return "", fmt.Errorf("reset token has expired")
	}

	if rt.used {
		// Attempt to reuse already-used token
		slog.Warn("password reset token reuse attempted", "userID", rt.userID)
		return "", fmt.Errorf("this reset token has already been used")
	}

	return rt.userID, nil
}

// CompletePasswordReset uses a reset token to actually reset the password.
// After calling this, the token is marked as used.
func (prm *PasswordResetManager) CompletePasswordReset(token, newPassword string) error {
	if err := validatePasswordStrength(newPassword); err != nil {
		return err
	}

	prm.mu.Lock()

	// Hash the token
	tokenHash := sha256.Sum256([]byte(token))
	tokenHashStr := hex.EncodeToString(tokenHash[:])

	rt, exists := prm.tokens[tokenHashStr]
	if !exists {
		prm.mu.Unlock()
		return fmt.Errorf("invalid reset token")
	}

	if time.Now().After(rt.expiresAt) {
		delete(prm.tokens, tokenHashStr)
		prm.mu.Unlock()
		return fmt.Errorf("reset token has expired")
	}

	if rt.used {
		prm.mu.Unlock()
		slog.Warn("password reset token reuse attempted", "userID", rt.userID)
		return fmt.Errorf("this reset token has already been used")
	}

	// Mark token as used
	rt.used = true

	prm.mu.Unlock()

	// Hash the new password with argon2id (shared auth policy).
	hashedPassword, err := hashPassword(newPassword)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	// Update password in database
	if err := prm.db.SetUserPassword(rt.userID, hashedPassword); err != nil {
		return fmt.Errorf("failed to update password: %w", err)
	}

	// Invalidate all other sessions for this user (force re-login)
	if err := prm.db.InvalidateAllUserSessions(rt.userID); err != nil {
		slog.Error("failed to invalidate sessions after password reset", "userID", rt.userID, "error", err)
		// Don't fail the password reset if session invalidation fails
	}

	slog.Info("password reset completed", "userID", rt.userID)

	return nil
}

// cleanupExpiredTokens removes expired reset tokens
func (prm *PasswordResetManager) cleanupExpiredTokens() {
	prm.mu.Lock()
	defer prm.mu.Unlock()

	now := time.Now()
	for tokenHash, rt := range prm.tokens {
		if now.After(rt.expiresAt) {
			delete(prm.tokens, tokenHash)
		}
	}
}

// validatePasswordStrength checks password meets minimum security requirements
func validatePasswordStrength(password string) error {
	if len(password) < 12 {
		return fmt.Errorf("password must be at least 12 characters long")
	}

	hasUpper := false
	hasLower := false
	hasDigit := false

	for _, ch := range password {
		switch {
		case ch >= 'A' && ch <= 'Z':
			hasUpper = true
		case ch >= 'a' && ch <= 'z':
			hasLower = true
		case ch >= '0' && ch <= '9':
			hasDigit = true
		}
	}

	if !hasUpper || !hasLower || !hasDigit {
		return fmt.Errorf("password must contain uppercase, lowercase, and digits")
	}

	return nil
}

// HandlePasswordResetRequest handles the initial password reset request (user provides email).
func (s *Server) HandlePasswordResetRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Email string `json:"email"`
	}

	if err := parseJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "Invalid request body",
		})
		return
	}

	if req.Email == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "Email is required",
		})
		return
	}

	// Find user by email (don't reveal if user exists to prevent user enumeration)
	userID, err := s.db.GetUserIDByEmail(req.Email)
	if err != nil {
		// Return generic message to prevent user enumeration
		slog.Warn("password reset request for non-existent email", "email", req.Email)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"message": "If an account with that email exists, a password reset link will be sent shortly.",
		})
		return
	}

	// Generate reset token
	token, err := s.passwordResetMgr.GenerateResetToken(userID, req.Email)
	if err != nil {
		slog.Error("failed to generate reset token", "userID", userID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "Failed to initiate password reset",
		})
		return
	}

	// Send email (stub - implement actual email sending)
	if err := s.sendPasswordResetEmail(req.Email, token); err != nil {
		slog.Error("failed to send reset email", "email", req.Email, "error", err)
		// Don't expose error to user
	}

	// Generic response
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "If an account with that email exists, a password reset link will be sent shortly.",
	})
}

// HandlePasswordResetComplete handles the password reset confirmation (user provides token and new password).
func (s *Server) HandlePasswordResetComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Token       string `json:"token"`
		NewPassword string `json:"new_password"`
	}

	if err := parseJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "Invalid request body",
		})
		return
	}

	if req.Token == "" || req.NewPassword == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "Token and new password are required",
		})
		return
	}

	// Validate and use the reset token
	if err := s.passwordResetMgr.CompletePasswordReset(req.Token, req.NewPassword); err != nil {
		slog.Warn("password reset failed", "error", err)
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Password has been reset successfully. Please log in with your new password.",
	})
}

// sendPasswordResetEmail sends a password reset email to the user.
// This is a stub implementation - replace with actual email sending logic.
func (s *Server) sendPasswordResetEmail(email, token string) error {
	// TODO: Implement actual email sending using configured SMTP
	// For now, log the token (DO NOT DO THIS IN PRODUCTION)
	slog.Info("Password reset email would be sent",
		"email", email,
		"token", maskSecret(token),
		"reset_url", fmt.Sprintf("https://your-domain/reset?token=%s", token))

	return nil
}

// maskSecret returns a masked version of a secret (first 4 and last 4 chars visible)
func maskSecret(secret string) string {
	if len(secret) <= 8 {
		return "[REDACTED]"
	}
	return secret[:4] + "***" + secret[len(secret)-4:]
}

func parseJSON(r *http.Request, dst interface{}) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body must contain only one JSON object")
		}
		return fmt.Errorf("invalid trailing JSON data")
	}

	return nil
}
