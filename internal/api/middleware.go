package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
)

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
