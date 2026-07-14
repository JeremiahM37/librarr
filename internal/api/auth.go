package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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
	ctxUsername contextKey = "username"
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
	mu          sync.RWMutex
	sessions    map[string]*SessionData
	pendingTOTP map[string]*PendingTOTP
}

// NewSessionStore creates a new session store.
func NewSessionStore() *SessionStore {
	s := &SessionStore{
		sessions:    make(map[string]*SessionData),
		pendingTOTP: make(map[string]*PendingTOTP),
	}

	// Periodically clean up expired sessions and pending TOTP tokens.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		for range ticker.C {
			now := time.Now()
			s.mu.Lock()
			for token, data := range s.sessions {
				if now.After(data.Expiry) {
					delete(s.sessions, token)
				}
			}
			for token, pending := range s.pendingTOTP {
				if now.After(pending.Expiry) {
					delete(s.pendingTOTP, token)
				}
			}
			s.mu.Unlock()
		}
	}()

	return s
}

// Create generates a new session token for a user, valid for 24 hours. It
// returns an error if the system CSPRNG fails, so callers fail closed rather
// than mint a predictable all-zero token.
func (s *SessionStore) Create(userID int64, username, role string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	token := hex.EncodeToString(b)

	s.mu.Lock()
	s.sessions[token] = &SessionData{
		UserID:   userID,
		Username: username,
		Role:     role,
		Expiry:   time.Now().Add(24 * time.Hour),
	}
	s.mu.Unlock()

	return token, nil
}

// CreatePendingTOTP creates a temporary token for TOTP verification (5 min
// expiry). Like Create, it fails closed if the CSPRNG errors.
func (s *SessionStore) CreatePendingTOTP(userID int64) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate pending TOTP token: %w", err)
	}
	token := hex.EncodeToString(b)

	s.mu.Lock()
	s.pendingTOTP[token] = &PendingTOTP{
		UserID: userID,
		Expiry: time.Now().Add(5 * time.Minute),
	}
	s.mu.Unlock()

	return token, nil
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
	"/":                 true, // Web UI (handles its own login)
	"/health":           true,
	"/api/health":       true,
	"/api/login":        true,
	"/api/login/totp":   true,
	"/api/register":     true,
	"/api/auth/status":  true,
	"/readyz":           true,
	"/api/openapi.json": true, // public API schema for AI/tooling discovery
}

// isExempt returns true if the path does not require auth.
func isExempt(path string) bool {
	if exemptPaths[path] {
		return true
	}
	// Torznab has its own apikey auth (checked inside the handler itself).
	// Both the canonical path and the Prowlarr-compat /api alias are exempt;
	// the alias is mounted as exact path /api only, so this does NOT match
	// /api/search, /api/library, etc.
	if path == "/api" || strings.HasPrefix(path, "/torznab/") {
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

		// Trusted reverse-proxy SSO headers should short-circuit the normal
		// login flow when OIDC is configured. This lets Authentik-backed
		// deployments log users in transparently instead of requiring a second
		// click on the Librarr login button.
		if cfg != nil && cfg.HasOIDCProxyHeaders() {
			username := proxyIdentityFromRequest(r)
			if username != "" {
				if user, err := resolveOIDCUser(cfg, database, username); err == nil && user != nil {
					if sessions != nil {
						if ensureSessionForUser(w, r, sessions, user) {
							_ = database.UpdateLastLogin(user.ID)
						}
					}
					ctx := context.WithValue(r.Context(), ctxUserID, user.ID)
					ctx = context.WithValue(ctx, ctxUserRole, user.Role)
					ctx = context.WithValue(ctx, ctxUsername, user.Username)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				} else if err != nil && cfg != nil && cfg.HasOIDC() {
					slog.Warn("proxy SSO login rejected", "username", username, "error", err)
				}
			}
		}

		// No multi-user, no legacy auth, no API key: the instance is open.
		// Treat the local caller as an admin rather than passing through
		// role-less, so admin-gated routes (e.g. POST /api/settings) work on
		// userless instances instead of failing requireAdmin with a 403.
		if !multiUser && !cfg.HasAuth() && !cfg.HasAPIKey() {
			ctx := context.WithValue(r.Context(), ctxUserRole, "admin")
			ctx = context.WithValue(ctx, ctxUsername, "local")
			next.ServeHTTP(w, r.WithContext(ctx))
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
			if subtle.ConstantTimeCompare([]byte(apiKey), []byte(cfg.APIKey)) == 1 {
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
				pendingToken, err := sessions.CreatePendingTOTP(user.ID)
				if err != nil {
					writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
						"success": false,
						"error":   "Failed to create session",
					})
					return
				}
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success":         true,
					"needs_totp":      true,
					"session_pending": pendingToken,
				})
				return
			}

			// No TOTP — create full session.
			database.UpdateLastLogin(user.ID)
			token, err := sessions.Create(user.ID, user.Username, user.Role)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to create session",
				})
				return
			}
			setSessionCookie(w, r, token, 86400)

			database.LogActivity(user.Username, "login", user.Username, "User logged in")

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

		token, err := sessions.Create(0, cfg.AuthUsername, "admin")
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to create session",
			})
			return
		}
		setSessionCookie(w, r, token, 86400)

		database.LogActivity(cfg.AuthUsername, "login", cfg.AuthUsername, "User logged in (legacy)")

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
			token, err := sessions.Create(user.ID, user.Username, user.Role)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to create session",
				})
				return
			}
			setSessionCookie(w, r, token, 86400)
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
			token, err := sessions.Create(user.ID, user.Username, user.Role)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to create session",
				})
				return
			}
			setSessionCookie(w, r, token, 86400)
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success":          true,
				"token":            token,
				"username":         user.Username,
				"role":             user.Role,
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
// First user becomes admin. After that, registration requires either:
//   - An admin session (admin creating users directly), OR
//   - A valid invite code (self-registration with a code the admin shared).
func handleRegister(database *db.DB, sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Username   string `json:"username"`
			Password   string `json:"password"`
			InviteCode string `json:"invite_code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		if len(req.Username) < 3 || len(req.Username) > 64 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Username must be 3-64 characters",
			})
			return
		}
		if len(req.Password) < 6 || len(req.Password) > 72 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Password must be 6-72 characters",
			})
			return
		}

		userCount, _ := database.CountUsers()
		isFirstUser := userCount == 0

		role := "user"
		if isFirstUser {
			role = "admin"
		}

		// After first user, require either admin session or valid invite code.
		if !isFirstUser {
			ctxRole, _ := r.Context().Value(ctxUserRole).(string)
			isAdmin := ctxRole == "admin"

			if req.InviteCode != "" {
				// Validate invite code.
				inviteRole, err := database.ValidateInviteCode(req.InviteCode)
				if err != nil {
					writeJSON(w, http.StatusForbidden, map[string]interface{}{
						"success": false,
						"error":   err.Error(),
					})
					return
				}
				role = inviteRole
			} else if !isAdmin {
				writeJSON(w, http.StatusForbidden, map[string]interface{}{
					"success": false,
					"error":   "Registration requires an invite code",
				})
				return
			}
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

		// Mark invite code as used.
		if req.InviteCode != "" {
			_ = database.UseInviteCode(req.InviteCode)
		}

		slog.Info("user registered", "id", id, "username", req.Username, "role", role)

		// If first user, auto-login.
		if isFirstUser {
			database.UpdateLastLogin(id)
			token, err := sessions.Create(id, req.Username, role)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   "Failed to create session",
				})
				return
			}
			setSessionCookie(w, r, token, 86400)
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

// handleAuthStatus returns the current auth state (are there users? is user logged in? is oidc available?)
func handleAuthStatus(cfg *config.Config, database *db.DB, sessions *SessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userCount, _ := database.CountUsers()

		resp := map[string]any{
			"multi_user":    userCount > 0,
			"has_users":     userCount > 0,
			"authenticated": false,
			"oidc_enabled":  false,
		}

		// OIDC hints
		if cfg != nil && cfg.HasOIDC() {
			resp["oidc_enabled"] = true
			resp["oidc_provider_name"] = cfg.OIDCProviderName
		}

		if username, _ := r.Context().Value(ctxUsername).(string); username != "" {
			resp["authenticated"] = true
			resp["username"] = username
		}
		if role, _ := r.Context().Value(ctxUserRole).(string); role != "" {
			resp["role"] = role
		}
		if userID, _ := r.Context().Value(ctxUserID).(int64); userID != 0 {
			resp["user_id"] = userID
		}

		// Check session.
		cookie, err := r.Cookie(sessionCookieName)
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

// handleChangeOwnPassword handles POST /api/me/password — any authenticated user.
// Verifies the current password before updating so a stolen session can't reset
// the password without knowing the existing one.
func handleChangeOwnPassword(database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := getUserIDFromContext(r)
		if userID == 0 {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Authentication required",
			})
			return
		}

		var req struct {
			CurrentPassword string `json:"current_password"`
			NewPassword     string `json:"new_password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid request body",
			})
			return
		}

		if req.CurrentPassword == "" || req.NewPassword == "" {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Both current_password and new_password are required",
			})
			return
		}

		if len(req.NewPassword) < 6 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "New password must be at least 6 characters",
			})
			return
		}

		user, err := database.GetUser(userID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   "User not found",
			})
			return
		}

		if !checkPassword(req.CurrentPassword, user.PasswordHash) {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Current password is incorrect",
			})
			return
		}

		hash, err := hashPassword(req.NewPassword)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to hash password",
			})
			return
		}
		if err := database.UpdateUserPassword(userID, hash); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   "Failed to update password",
			})
			return
		}

		database.LogActivity(user.Username, "password_changed", "self", "User changed their own password")
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

		// Prevent leaving the system with no admins. API-key callers have no
		// userID (id=0), so the self-delete check above does not protect them;
		// without this guard, a leaked API key (or a misclicked SPA button)
		// could nuke the only admin.
		if users, err := database.ListUsers(); err == nil {
			admins := 0
			targetIsAdmin := false
			for _, u := range users {
				if u.Role == "admin" {
					admins++
					if u.ID == id {
						targetIsAdmin = true
					}
				}
			}
			if targetIsAdmin && admins <= 1 {
				writeJSON(w, http.StatusConflict, map[string]interface{}{
					"success": false,
					"error":   "Cannot delete the last admin account",
				})
				return
			}
		}

		if err := database.DeleteUser(id); err != nil {
			slog.Error("failed to delete user", "id", id, "error", err)
			writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   "Failed to delete user",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	}
}

// handleLogout handles POST /api/logout.
func handleLogout(sessions *SessionStore, database *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username, _ := r.Context().Value(ctxUsername).(string)
		cookie, err := r.Cookie("librarr_session")
		if err == nil {
			sessions.Delete(cookie.Value)
		}
		database.LogActivity(username, "logout", username, "User logged out")
		setSessionCookie(w, r, "", -1)
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	}
}
