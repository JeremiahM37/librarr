package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

const (
	ctxUserID                          contextKey = "userID"
	ctxUserRole                        contextKey = "userRole"
	ctxUsername                        contextKey = "username"
	ctxAPIKeyScope                     contextKey = "apiKeyScope"
	apiKeyQueryParamDeprecationWarning            = "299 librarr \"?apikey=\" is deprecated; use the X-Api-Key header"

	// loginMaxFailures is the number of consecutive failures before an IP is locked out.
	loginMaxFailures = 5
	// loginLockoutDuration is how long an IP is locked out after hitting loginMaxFailures.
	loginLockoutDuration = 15 * time.Minute

	// Argon2 defaults are intentionally conservative so low-power devices
	// like Raspberry Pis can still handle auth without lag spikes.
	argon2Time    uint32 = 1
	argon2Memory  uint32 = 16 * 1024 // KiB (16 MiB)
	argon2Threads uint8  = 1
	argon2KeyLen  uint32 = 32
	argon2SaltLen        = 16
)

// loginFailureEntry records consecutive auth failures for a single IP.
type loginFailureEntry struct {
	count       int
	lockedUntil time.Time
}

// LoginThrottle tracks per-IP login failures and enforces lockout.
type LoginThrottle struct {
	mu      sync.Mutex
	entries map[string]*loginFailureEntry
}

// NewLoginThrottle creates a LoginThrottle with periodic cleanup.
func NewLoginThrottle() *LoginThrottle {
	lt := &LoginThrottle{entries: make(map[string]*loginFailureEntry)}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			now := time.Now()
			lt.mu.Lock()
			for ip, e := range lt.entries {
				if now.After(e.lockedUntil) && e.count == 0 {
					delete(lt.entries, ip)
				}
			}
			lt.mu.Unlock()
		}
	}()
	return lt
}

// Check returns (allowed, retryAfterSeconds). Call before checking credentials.
func (lt *LoginThrottle) Check(ip string) (bool, int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	e, ok := lt.entries[ip]
	if !ok {
		return true, 0
	}
	if e.count >= loginMaxFailures {
		remaining := int(time.Until(e.lockedUntil).Seconds())
		if remaining > 0 {
			return false, remaining
		}
		// Lockout expired — reset.
		delete(lt.entries, ip)
	}
	return true, 0
}

// Failure records a failed login attempt for an IP.
func (lt *LoginThrottle) Failure(ip string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	e, ok := lt.entries[ip]
	if !ok {
		e = &loginFailureEntry{}
		lt.entries[ip] = e
	}
	e.count++
	if e.count >= loginMaxFailures {
		e.lockedUntil = time.Now().Add(loginLockoutDuration)
		slog.Warn("login throttle: IP locked out after repeated failures",
			"ip", ip, "failures", e.count,
			"locked_until", e.lockedUntil.UTC().Format(time.RFC3339))
	}
}

// Success resets the failure counter for an IP on successful login.
func (lt *LoginThrottle) Success(ip string) {
	lt.mu.Lock()
	delete(lt.entries, ip)
	lt.mu.Unlock()
}

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
	db          *db.DB // optional — persists sessions across restarts
}

var tokenFallbackCounter uint64

func generateTokenHex(numBytes int) string {
	b := make([]byte, numBytes)
	if _, err := rand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}

	// Fallback is only used if crypto/rand fails unexpectedly.
	seed := time.Now().UTC().Format(time.RFC3339Nano) + ":" + strconv.FormatUint(atomic.AddUint64(&tokenFallbackCounter, 1), 10)
	h := sha256.Sum256([]byte(seed))
	slog.Error("crypto random generation failed; using deterministic fallback token", "error", "rand.Read failed")
	return hex.EncodeToString(h[:])
}

// NewSessionStore creates a new session store. The database argument is optional;
// when non-nil, sessions are persisted to the database and survive restarts.
func NewSessionStore(databases ...*db.DB) *SessionStore {
	var database *db.DB
	if len(databases) > 0 {
		database = databases[0]
	}

	s := &SessionStore{
		sessions:    make(map[string]*SessionData),
		pendingTOTP: make(map[string]*PendingTOTP),
		db:          database,
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
			if database != nil {
				if err := database.DeleteExpiredSessions(now.Unix()); err != nil {
					slog.Warn("failed to delete expired sessions from db", "err", err)
				}
			}
		}
	}()

	return s
}

// Create generates a new session token for a user, valid for 24 hours.
func (s *SessionStore) Create(userID int64, username, role string) string {
	token := generateTokenHex(32)

	expiry := time.Now().Add(24 * time.Hour)
	s.mu.Lock()
	s.sessions[token] = &SessionData{
		UserID:   userID,
		Username: username,
		Role:     role,
		Expiry:   expiry,
	}
	s.mu.Unlock()

	if s.db != nil {
		if err := s.db.CreateSession(token, userID, username, role, expiry.Unix()); err != nil {
			slog.Warn("failed to persist session to db", "err", err)
		}
	}

	return token
}

// CreatePendingTOTP creates a temporary token for TOTP verification (5 min expiry).
func (s *SessionStore) CreatePendingTOTP(userID int64) string {
	token := generateTokenHex(32)

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

	if ok {
		if time.Now().After(data.Expiry) {
			s.mu.Lock()
			delete(s.sessions, token)
			s.mu.Unlock()
			if s.db != nil {
				s.db.DeleteSession(token) //nolint:errcheck
			}
			return nil, false
		}
		return data, true
	}

	// Cache miss — look up in DB (handles restart recovery).
	if s.db != nil {
		userID, username, role, expiresAt, found, err := s.db.GetSession(token)
		if err != nil {
			slog.Warn("failed to look up session from db", "err", err)
			return nil, false
		}
		if !found {
			return nil, false
		}
		expiry := time.Unix(expiresAt, 0)
		if time.Now().After(expiry) {
			s.db.DeleteSession(token) //nolint:errcheck
			return nil, false
		}
		// Re-populate in-memory cache.
		sd := &SessionData{UserID: userID, Username: username, Role: role, Expiry: expiry}
		s.mu.Lock()
		s.sessions[token] = sd
		s.mu.Unlock()
		return sd, true
	}

	return nil, false
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
	if s.db != nil {
		if err := s.db.DeleteSession(token); err != nil {
			slog.Warn("failed to delete session from db", "err", err)
		}
	}
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

type apiKeyScope string

const (
	apiKeyScopeAdmin    apiKeyScope = "admin"
	apiKeyScopeReadOnly apiKeyScope = "read"
)

// isReadOnlyMethod determines whether a scoped read-only API key can use the method.
func isReadOnlyMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

// resolveAPIKeyScope resolves API key scope for the incoming request.
func resolveAPIKeyScope(cfg *config.Config, r *http.Request) (apiKeyScope, bool, bool) {
	apiKey, usedQueryParam := apiKeyFromRequest(r)
	if apiKey == "" {
		return "", false, usedQueryParam
	}

	if cfg.APIKey != "" && subtle.ConstantTimeCompare([]byte(apiKey), []byte(cfg.APIKey)) == 1 {
		return apiKeyScopeAdmin, true, usedQueryParam
	}

	if cfg.APIKeyReadOnly != "" && subtle.ConstantTimeCompare([]byte(apiKey), []byte(cfg.APIKeyReadOnly)) == 1 {
		return apiKeyScopeReadOnly, true, usedQueryParam
	}

	return "", false, usedQueryParam
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

		// Check scoped API keys (header or query param) -- machine-to-machine auth.
		if cfg.HasAPIKey() {
			scope, ok, usedQueryParam := resolveAPIKeyScope(cfg, r)
			if ok {
				if usedQueryParam {
					w.Header().Add("Warning", apiKeyQueryParamDeprecationWarning)
					w.Header().Set("Deprecation", "true")
				}

				// Read-only keys are allowed to call safe read endpoints only.
				if scope == apiKeyScopeReadOnly && !isReadOnlyMethod(r.Method) {
					writeJSON(w, http.StatusForbidden, map[string]interface{}{
						"success": false,
						"error":   "Read-only API key cannot access write endpoints",
					})
					return
				}

				role := "admin"
				username := "api-admin"
				if scope == apiKeyScopeReadOnly {
					role = "reader"
					username = "api-readonly"
				}

				ctx := context.WithValue(r.Context(), ctxUserRole, role)
				ctx = context.WithValue(ctx, ctxUsername, username)
				ctx = context.WithValue(ctx, ctxAPIKeyScope, string(scope))
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

func apiKeyFromRequest(r *http.Request) (string, bool) {
	apiKey := r.Header.Get("X-Api-Key")
	if apiKey != "" {
		return apiKey, false
	}
	apiKey = r.URL.Query().Get("apikey")
	return apiKey, apiKey != ""
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

// isAdmin checks if the request context indicates an admin user.
func isAdmin(r *http.Request) bool {
	role, _ := r.Context().Value(ctxUserRole).(string)
	return role == "admin"
}

// getUserIDFromContext extracts the user ID from the request context.
func getUserIDFromContext(r *http.Request) int64 {
	id, _ := r.Context().Value(ctxUserID).(int64)
	return id
}

// hashPassword hashes a password using argon2id.
func hashPassword(password string) (string, error) {
	salt := make([]byte, argon2SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate argon2 salt: %w", err)
	}

	threads := argon2Threads
	if runtime.GOMAXPROCS(0) <= 0 {
		threads = 1
	}

	hash := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, threads, argon2KeyLen)
	encodedSalt := base64.RawStdEncoding.EncodeToString(salt)
	encodedHash := base64.RawStdEncoding.EncodeToString(hash)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		argon2Memory,
		argon2Time,
		threads,
		encodedSalt,
		encodedHash,
	), nil
}

// checkPassword verifies a password against either argon2id (preferred)
// or legacy bcrypt hashes.
func checkPassword(password, hash string) bool {
	if strings.HasPrefix(hash, "$argon2id$") {
		ok, err := verifyArgon2Password(password, hash)
		return err == nil && ok
	}

	// Legacy bcrypt support so existing users can still authenticate.
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func verifyArgon2Password(password, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return false, fmt.Errorf("invalid argon2 hash format")
	}

	versionPart := parts[2]
	if versionPart != fmt.Sprintf("v=%d", argon2.Version) {
		return false, fmt.Errorf("unsupported argon2 version")
	}

	var memory uint32
	var timeCost uint32
	var parallelism uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &timeCost, &parallelism); err != nil {
		return false, fmt.Errorf("invalid argon2 parameters")
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("invalid argon2 salt")
	}

	decodedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("invalid argon2 hash")
	}

	computed := argon2.IDKey([]byte(password), salt, timeCost, memory, parallelism, uint32(len(decodedHash)))
	return subtle.ConstantTimeCompare(decodedHash, computed) == 1, nil
}

func isLegacyBcryptHash(hash string) bool {
	return strings.HasPrefix(hash, "$2a$") || strings.HasPrefix(hash, "$2b$") || strings.HasPrefix(hash, "$2y$")
}

// migratePasswordHashIfNeeded upgrades legacy bcrypt hashes to argon2id
// right after a successful password verification.
func migratePasswordHashIfNeeded(database *db.DB, userID int64, plaintext, currentHash string) {
	if !isLegacyBcryptHash(currentHash) {
		return
	}

	newHash, err := hashPassword(plaintext)
	if err != nil {
		slog.Warn("password hash migration skipped: failed to generate argon2 hash", "user_id", userID, "error", err)
		return
	}

	if err := database.UpdateUserPassword(userID, newHash); err != nil {
		slog.Warn("password hash migration skipped: failed to persist argon2 hash", "user_id", userID, "error", err)
		return
	}

	slog.Info("password hash upgraded to argon2id", "user_id", userID)
}

// hashBackupCode creates a SHA-256 hash of a backup code (not bcrypt for performance with 8 codes).
func hashBackupCode(code string) string {
	h := sha256.Sum256([]byte(code))
	return hex.EncodeToString(h[:])
}

// remoteIP extracts the IP address from r.RemoteAddr (strips port).
func remoteIP(r *http.Request) string {
	addr := r.RemoteAddr
	if i := strings.LastIndex(addr, ":"); i != -1 {
		addr = addr[:i]
	}
	return strings.Trim(addr, "[]")
}

// handleLogin handles POST /api/login for session-based auth.
func handleLogin(cfg *config.Config, database *db.DB, sessions *SessionStore, throttle *LoginThrottle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := remoteIP(r)

		// Check lockout before touching credentials.
		if allowed, retryAfter := throttle.Check(ip); !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
				"success":     false,
				"error":       "Too many failed login attempts; try again later",
				"retry_after": retryAfter,
			})
			return
		}

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
				throttle.Failure(ip)
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
					"success": false,
					"error":   "Invalid credentials",
				})
				return
			}

			// Seamless migration path: once a bcrypt user logs in successfully,
			// upgrade their stored hash to argon2id.
			migratePasswordHashIfNeeded(database, user.ID, req.Password, user.PasswordHash)

			// If TOTP is enabled, return pending token.
			if user.TOTPEnabled {
				throttle.Success(ip) // credentials were valid; clear failure count
				pendingToken := sessions.CreatePendingTOTP(user.ID)
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success":         true,
					"needs_totp":      true,
					"session_pending": pendingToken,
				})
				return
			}

			// No TOTP — create full session.
			throttle.Success(ip)
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
			throttle.Failure(ip)
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Invalid credentials",
			})
			return
		}

		throttle.Success(ip)
		token := sessions.Create(0, cfg.AuthUsername, "admin")
		http.SetCookie(w, &http.Cookie{
			Name:     "librarr_session",
			Value:    token,
			Path:     "/",
			MaxAge:   86400,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})

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
