package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"golang.org/x/crypto/bcrypt"
)

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
