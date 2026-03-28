package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"golang.org/x/crypto/bcrypt"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

const (
	ctxUserID   contextKey = "userID"
	ctxUserRole contextKey = "userRole"
	ctxUsername  contextKey = "username"
)

// SessionData holds session metadata.
type SessionData struct {
	UserID   int64
	Username string
	Role     string
	Expiry   time.Time
}

// PendingTOTP holds a pending TOTP verification.
type PendingTOTP struct {
	UserID int64
	Expiry time.Time
}

// SessionStore manages session-based authentication with user tracking.
type SessionStore struct {
	mu             sync.RWMutex
	sessions       map[string]*SessionData
	pendingTOTP    map[string]*PendingTOTP
}

// NewSessionStore creates a new session store.
func NewSessionStore() *SessionStore {
	return &SessionStore{
		sessions:    make(map[string]*SessionData),
		pendingTOTP: make(map[string]*PendingTOTP),
	}
}

// Create generates a new session token for a user, valid for 24 hours.
func (s *SessionStore) Create(userID int64, username, role string) string {
	b := make([]byte, 32)
	rand.Read(b)
	token := hex.EncodeToString(b)

	s.mu.Lock()
	s.sessions[token] = &SessionData{
		UserID:   userID,
		Username: username,
		Role:     role,
		Expiry:   time.Now().Add(24 * time.Hour),
	}
	s.mu.Unlock()

	return token
}

// CreatePendingTOTP creates a temporary token for TOTP verification (5 min expiry).
func (s *SessionStore) CreatePendingTOTP(userID int64) string {
	b := make([]byte, 32)
	rand.Read(b)
	token := hex.EncodeToString(b)

	s.mu.Lock()
	s.pendingTOTP[token] = &PendingTOTP{
		UserID: userID,
		Expiry: time.Now().Add(5 * time.Minute),
	}
	s.mu.Unlock()

	return token
}

// ValidatePendingTOTP checks and consumes a pending TOTP token.
func (s *SessionStore) ValidatePendingTOTP(token string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending, ok := s.pendingTOTP[token]
	if !ok {
		return 0, false
	}
	delete(s.pendingTOTP, token)

	if time.Now().After(pending.Expiry) {
		return 0, false
	}
	return pending.UserID, true
}

// Get retrieves session data if the token is valid.
func (s *SessionStore) Get(token string) (*SessionData, bool) {
	s.mu.RLock()
	data, ok := s.sessions[token]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}
	if time.Now().After(data.Expiry) {
		s.mu.Lock()
		delete(s.sessions, token)
		s.mu.Unlock()
		return nil, false
	}
	return data, true
}

// Valid checks if a session token is valid and not expired (backward compat).
func (s *SessionStore) Valid(token string) bool {
	_, ok := s.Get(token)
	return ok
}

// Delete removes a session.
func (s *SessionStore) Delete(token string) {
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
}

// exemptPaths are paths that do not require authentication.
var exemptPaths = map[string]bool{
	"/":                true, // Web UI (handles its own login)
	"/health":          true,
	"/api/health":      true,
	"/api/login":       true,
	"/api/login/totp":  true,
	"/api/register":    true,
	"/api/auth/status": true,
	"/readyz":          true,
}

// isExempt returns true if the path does not require auth.
func isExempt(path string) bool {
	if exemptPaths[path] {
		return true
	}
	// Torznab has its own apikey auth.
	if strings.HasPrefix(path, "/torznab/") {
		return true
	}
	// Static assets.
	if strings.HasPrefix(path, "/static/") {
		return true
	}
	// OPDS feeds (e-readers handle auth separately).
	if strings.HasPrefix(path, "/opds") {
		return true
	}
	// Prometheus metrics.
	if path == "/metrics" {
		return true
	}
	// OIDC auth endpoints.
	if strings.HasPrefix(path, "/auth/oidc/") {
		return true
	}
	return false
}

// authMiddleware returns an HTTP middleware that enforces authentication.
func authMiddleware(cfg *config.Config, database *db.DB, sessions *SessionStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if multi-user is active (any users in DB).
		userCount, _ := database.CountUsers()
		multiUser := userCount > 0

		// If no multi-user and no legacy auth, pass through.
		if !multiUser && !cfg.HasAuth() && !cfg.HasAPIKey() {
			next.ServeHTTP(w, r)
			return
		}

		// Exempt paths always pass through.
		if isExempt(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		// Check API key (header or query param) -- machine-to-machine auth.
		if cfg.HasAPIKey() {
			apiKey := r.Header.Get("X-Api-Key")
			if apiKey == "" {
				apiKey = r.URL.Query().Get("apikey")
			}
			if apiKey == cfg.APIKey {
				// API key users get admin-level access.
				ctx := context.WithValue(r.Context(), ctxUserRole, "admin")
				ctx = context.WithValue(ctx, ctxUsername, "api")
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}

		// Check session cookie for multi-user mode.
		if multiUser {
			cookie, err := r.Cookie("librarr_session")
			if err == nil {
				if data, ok := sessions.Get(cookie.Value); ok {
					ctx := context.WithValue(r.Context(), ctxUserID, data.UserID)
					ctx = context.WithValue(ctx, ctxUserRole, data.Role)
					ctx = context.WithValue(ctx, ctxUsername, data.Username)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
		}

		// Legacy single-user session auth (when no multi-user DB users exist).
		if !multiUser && cfg.HasAuth() {
			cookie, err := r.Cookie("librarr_session")
			if err == nil && sessions.Valid(cookie.Value) {
				ctx := context.WithValue(r.Context(), ctxUserRole, "admin")
				ctx = context.WithValue(ctx, ctxUsername, cfg.AuthUsername)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}

		// No valid auth found.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
	})
}

// requireAdmin is middleware that checks if the current user has admin role.
func requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		role, _ := r.Context().Value(ctxUserRole).(string)
		if role != "admin" {
			writeJSON(w, http.StatusForbidden, map[string]interface{}{
				"success": false,
				"error":   "Admin access required",
			})
			return
		}
		next(w, r)
	}
}

// getUserIDFromContext extracts the user ID from the request context.
func getUserIDFromContext(r *http.Request) int64 {
	id, _ := r.Context().Value(ctxUserID).(int64)
	return id
}

// hashPassword hashes a password using bcrypt.
func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

// checkPassword verifies a password against a bcrypt hash.
func checkPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// hashBackupCode creates a SHA-256 hash of a backup code (not bcrypt for performance with 8 codes).
func hashBackupCode(code string) string {
	h := sha256.Sum256([]byte(code))
	return hex.EncodeToString(h[:])
}

// handleLogin handles POST /api/login for session-based auth.
func handleLogin(cfg *config.Config, database *db.DB, sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		// Check if multi-user mode is active.
		userCount, _ := database.CountUsers()
		multiUser := userCount > 0

		if multiUser {
			// Multi-user login against DB.
			user, err := database.GetUserByUsername(req.Username)
			if err != nil || !checkPassword(req.Password, user.PasswordHash) {
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
					"success": false,
					"error":   "Invalid credentials",
				})
				return
			}

			// If TOTP is enabled, return pending token.
			if user.TOTPEnabled {
				pendingToken := sessions.CreatePendingTOTP(user.ID)
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success":         true,
					"needs_totp":      true,
					"session_pending": pendingToken,
				})
				return
			}

			// No TOTP — create full session.
			database.UpdateLastLogin(user.ID)
			token := sessions.Create(user.ID, user.Username, user.Role)
			http.SetCookie(w, &http.Cookie{
				Name:     "librarr_session",
				Value:    token,
				Path:     "/",
				MaxAge:   86400,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})

			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":  true,
				"token":    token,
				"username": user.Username,
				"role":     user.Role,
			})
			return
		}

		// Legacy single-user mode.
		if !cfg.HasAuth() {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success": true,
				"message": "Auth not configured",
			})
			return
		}

		if req.Username != cfg.AuthUsername || req.Password != cfg.AuthPassword {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Invalid credentials",
			})
			return
		}

		token := sessions.Create(0, cfg.AuthUsername, "admin")
		http.SetCookie(w, &http.Cookie{
			Name:     "librarr_session",
			Value:    token,
			Path:     "/",
			MaxAge:   86400,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":  true,
			"token":    token,
			"username": cfg.AuthUsername,
			"role":     "admin",
		})
	}
}

// handleLoginTOTP handles POST /api/login/totp — second step of 2FA login.
func handleLoginTOTP(database *db.DB, sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SessionPending string `json:"session_pending"`
			Code           string `json:"code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		userID, valid := sessions.ValidatePendingTOTP(req.SessionPending)
		if !valid {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Invalid or expired TOTP session",
			})
			return
		}

		user, err := database.GetUser(userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "User not found",
			})
			return
		}

		// Try TOTP code first.
		if validateTOTPCode(user.TOTPSecret, req.Code) {
			database.UpdateLastLogin(user.ID)
			token := sessions.Create(user.ID, user.Username, user.Role)
			http.SetCookie(w, &http.Cookie{
				Name:     "librarr_session",
				Value:    token,
				Path:     "/",
				MaxAge:   86400,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":  true,
				"token":    token,
				"username": user.Username,
				"role":     user.Role,
			})
			return
		}

		// Try backup code.
		codeHash := hashBackupCode(req.Code)
		used, _ := database.UseBackupCode(user.ID, codeHash)
		if used {
			database.UpdateLastLogin(user.ID)
			token := sessions.Create(user.ID, user.Username, user.Role)
			http.SetCookie(w, &http.Cookie{
				Name:     "librarr_session",
				Value:    token,
				Path:     "/",
				MaxAge:   86400,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":       true,
				"token":         token,
				"username":      user.Username,
				"role":          user.Role,
				"backup_code_used": true,
			})
			return
		}

		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Invalid TOTP code",
		})
	}
}

// handleRegister handles POST /api/register — create a new user.
// First user becomes admin. After that, only admins can register new users.
func handleRegister(database *db.DB, sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		if len(req.Username) < 3 || len(req.Password) < 6 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Username must be at least 3 characters, password at least 6",
			})
			return
		}

		userCount, _ := database.CountUsers()
		isFirstUser := userCount == 0

		// After first user, only admins can register.
		if !isFirstUser {
			role, _ := r.Context().Value(ctxUserRole).(string)
			if role != "admin" {
				writeJSON(w, http.StatusForbidden, map[string]interface{}{
					"success": false,
					"error":   "Only admins can create new users",
				})
				return
			}
		}

		role := "user"
		if isFirstUser {
			role = "admin"
		}

		hash, err := hashPassword(req.Password)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to hash password",
			})
			return
		}

		id, err := database.CreateUser(req.Username, hash, role)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE") {
				writeJSON(w, http.StatusConflict, map[string]interface{}{
					"success": false,
					"error":   "Username already exists",
				})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to create user",
			})
			return
		}

		slog.Info("user registered", "id", id, "username", req.Username, "role", role)

		// If first user, auto-login.
		if isFirstUser {
			database.UpdateLastLogin(id)
			token := sessions.Create(id, req.Username, role)
			http.SetCookie(w, &http.Cookie{
				Name:     "librarr_session",
				Value:    token,
				Path:     "/",
				MaxAge:   86400,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})
			writeJSON(w, http.StatusCreated, map[string]interface{}{
				"success":  true,
				"id":       id,
				"username": req.Username,
				"role":     role,
				"token":    token,
				"message":  "First user created as admin",
			})
			return
		}

		writeJSON(w, http.StatusCreated, map[string]interface{}{
			"success":  true,
			"id":       id,
			"username": req.Username,
			"role":     role,
		})
	}
}

// handleAuthStatus returns the current auth state (are there users? is user logged in?)
func handleAuthStatus(database *db.DB, sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userCount, _ := database.CountUsers()

		resp := map[string]interface{}{
			"multi_user":    userCount > 0,
			"has_users":     userCount > 0,
			"authenticated": false,
		}

		// Check session.
		cookie, err := r.Cookie("librarr_session")
		if err == nil {
			if data, ok := sessions.Get(cookie.Value); ok {
				resp["authenticated"] = true
				resp["username"] = data.Username
				resp["role"] = data.Role
				resp["user_id"] = data.UserID
			}
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// handleListUsers handles GET /api/users — admin only.
func handleListUsers(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		users, err := database.ListUsers()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to list users",
			})
			return
		}

		// Sanitize output — don't expose hashes.
		type safeUser struct {
			ID          int64  `json:"id"`
			Username    string `json:"username"`
			Role        string `json:"role"`
			TOTPEnabled bool   `json:"totp_enabled"`
			CreatedAt   string `json:"created_at"`
			LastLogin   string `json:"last_login,omitempty"`
		}

		var result []safeUser
		for _, u := range users {
			su := safeUser{
				ID:          u.ID,
				Username:    u.Username,
				Role:        u.Role,
				TOTPEnabled: u.TOTPEnabled,
				CreatedAt:   u.CreatedAt.Format(time.RFC3339),
			}
			if !u.LastLogin.IsZero() {
				su.LastLogin = u.LastLogin.Format(time.RFC3339)
			}
			result = append(result, su)
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"users":   result,
		})
	}
}

// handleUpdateUser handles PATCH /api/users/{id} — admin only.
func handleUpdateUser(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid user ID",
			})
			return
		}

		var req struct {
			Role     string `json:"role"`
			Password string `json:"password,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		user, err := database.GetUser(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   "User not found",
			})
			return
		}

		if req.Role != "" && (req.Role == "admin" || req.Role == "user") {
			if err := database.UpdateUser(id, user.Username, req.Role); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to update user",
				})
				return
			}
		}

		if req.Password != "" {
			hash, err := hashPassword(req.Password)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to hash password",
				})
				return
			}
			if err := database.UpdateUserPassword(id, hash); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to update password",
				})
				return
			}
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	}
}

// handleDeleteUser handles DELETE /api/users/{id} — admin only.
func handleDeleteUser(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid user ID",
			})
			return
		}

		// Prevent deleting yourself.
		currentID := getUserIDFromContext(r)
		if currentID == id {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Cannot delete your own account",
			})
			return
		}

		if err := database.DeleteUser(id); err != nil {
			writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("Failed to delete user: %s", err.Error()),
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	}
}

// handleLogout handles POST /api/logout.
func handleLogout(sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("librarr_session")
		if err == nil {
			sessions.Delete(cookie.Value)
		}
		http.SetCookie(w, &http.Cookie{
			Name:     "librarr_session",
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	}
}
