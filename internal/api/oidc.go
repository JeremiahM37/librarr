package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDCHandler manages OIDC authentication flow.
type OIDCHandler struct {
	cfg      *config.Config
	db       *db.DB
	sessions *SessionStore

	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	oauth2   oauth2.Config

	// State nonce store (state -> expiry).
	mu     sync.Mutex
	states map[string]time.Time
}

// NewOIDCHandler initializes the OIDC provider and returns a handler.
// Returns nil if OIDC is not configured.
func NewOIDCHandler(cfg *config.Config, database *db.DB, sessions *SessionStore) *OIDCHandler {
	if !cfg.HasOIDC() {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	provider, err := oidc.NewProvider(ctx, cfg.OIDCIssuer)
	if err != nil {
		slog.Error("failed to initialize OIDC provider", "issuer", cfg.OIDCIssuer, "error", err)
		return nil
	}

	verifier := provider.Verifier(&oidc.Config{ClientID: cfg.OIDCClientID})

	oauth2Cfg := oauth2.Config{
		ClientID:     cfg.OIDCClientID,
		ClientSecret: cfg.OIDCClientSecret,
		Endpoint:     provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}

	if cfg.OIDCRedirectURI != "" {
		oauth2Cfg.RedirectURL = cfg.OIDCRedirectURI
	}

	slog.Info("OIDC provider initialized", "issuer", cfg.OIDCIssuer, "provider_name", cfg.OIDCProviderName)

	return &OIDCHandler{
		cfg:      cfg,
		db:       database,
		sessions: sessions,
		provider: provider,
		verifier: verifier,
		oauth2:   oauth2Cfg,
		states:   make(map[string]time.Time),
	}
}

// generateState creates a random state string for CSRF protection.
func (h *OIDCHandler) generateState() string {
	b := make([]byte, 16)
	rand.Read(b)
	state := hex.EncodeToString(b)

	h.mu.Lock()
	h.states[state] = time.Now().Add(10 * time.Minute)
	h.mu.Unlock()

	return state
}

// validateState checks and consumes a state nonce.
func (h *OIDCHandler) validateState(state string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	expiry, ok := h.states[state]
	if !ok {
		return false
	}
	delete(h.states, state)
	return time.Now().Before(expiry)
}

// HandleLogin redirects to the OIDC provider.
func (h *OIDCHandler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "OIDC not configured",
		})
		return
	}

	// Auto-detect redirect URI from request if not set.
	oauth2Cfg := h.oauth2
	if oauth2Cfg.RedirectURL == "" {
		scheme := "http"
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			scheme = "https"
		}
		host := r.Host
		oauth2Cfg.RedirectURL = fmt.Sprintf("%s://%s/auth/oidc/callback", scheme, host)
	}

	state := h.generateState()
	http.Redirect(w, r, oauth2Cfg.AuthCodeURL(state), http.StatusFound)
}

// HandleCallback handles the OIDC provider callback.
func (h *OIDCHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "OIDC not configured",
		})
		return
	}

	// Validate state.
	state := r.URL.Query().Get("state")
	if !h.validateState(state) {
		http.Error(w, "Invalid state parameter", http.StatusBadRequest)
		return
	}

	// Check for errors from provider.
	if errParam := r.URL.Query().Get("error"); errParam != "" {
		errDesc := r.URL.Query().Get("error_description")
		slog.Warn("OIDC callback error", "error", errParam, "description", errDesc)
		http.Error(w, fmt.Sprintf("OIDC error: %s - %s", errParam, errDesc), http.StatusBadRequest)
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing authorization code", http.StatusBadRequest)
		return
	}

	// Auto-detect redirect URI for token exchange.
	oauth2Cfg := h.oauth2
	if oauth2Cfg.RedirectURL == "" {
		scheme := "http"
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			scheme = "https"
		}
		oauth2Cfg.RedirectURL = fmt.Sprintf("%s://%s/auth/oidc/callback", scheme, r.Host)
	}

	// Exchange code for tokens.
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	token, err := oauth2Cfg.Exchange(ctx, code)
	if err != nil {
		slog.Error("OIDC token exchange failed", "error", err)
		http.Error(w, "Failed to exchange authorization code", http.StatusInternalServerError)
		return
	}

	// Extract and verify ID token.
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		http.Error(w, "No ID token in response", http.StatusInternalServerError)
		return
	}

	idToken, err := h.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		slog.Error("OIDC ID token verification failed", "error", err)
		http.Error(w, "Invalid ID token", http.StatusInternalServerError)
		return
	}

	// Extract claims.
	var claims struct {
		Email             string `json:"email"`
		EmailVerified     bool   `json:"email_verified"`
		Name              string `json:"name"`
		PreferredUsername string `json:"preferred_username"`
		Sub               string `json:"sub"`
	}
	if err := idToken.Claims(&claims); err != nil {
		slog.Error("failed to parse OIDC claims", "error", err)
		http.Error(w, "Failed to parse user info", http.StatusInternalServerError)
		return
	}

	// Determine username from claims (prefer preferred_username, then email, then sub).
	username := claims.PreferredUsername
	if username == "" {
		username = claims.Email
	}
	if username == "" {
		username = claims.Name
	}
	if username == "" {
		username = claims.Sub
	}

	// Sanitize username.
	username = strings.TrimSpace(username)
	if username == "" {
		http.Error(w, "Could not determine username from OIDC claims", http.StatusInternalServerError)
		return
	}

	slog.Info("OIDC login", "username", username, "email", claims.Email, "sub", claims.Sub)

	// Find or create user.
	user, err := h.db.GetUserByUsername(username)
	if err != nil {
		// User doesn't exist.
		if !h.cfg.OIDCAutoCreateUsers {
			http.Error(w, "User not found and auto-creation is disabled", http.StatusForbidden)
			return
		}

		// Determine role: first user is admin, otherwise use default.
		userCount, _ := h.db.CountUsers()
		role := h.cfg.OIDCDefaultRole
		if userCount == 0 {
			role = "admin"
		}

		// Create user with a random password (OIDC users don't use password login).
		randomPass := make([]byte, 32)
		rand.Read(randomPass)
		passHash, _ := hashPassword(hex.EncodeToString(randomPass))

		id, err := h.db.CreateUser(username, passHash, role)
		if err != nil {
			slog.Error("failed to create OIDC user", "username", username, "error", err)
			http.Error(w, "Failed to create user account", http.StatusInternalServerError)
			return
		}

		user, err = h.db.GetUser(id)
		if err != nil {
			http.Error(w, "Failed to retrieve created user", http.StatusInternalServerError)
			return
		}

		slog.Info("OIDC user created", "id", id, "username", username, "role", role)
	}

	// Create session.
	h.db.UpdateLastLogin(user.ID)
	sessionToken := h.sessions.Create(user.ID, user.Username, user.Role)

	http.SetCookie(w, &http.Cookie{
		Name:     "librarr_session",
		Value:    sessionToken,
		Path:     "/",
		MaxAge:   86400,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	// Redirect to app root.
	http.Redirect(w, r, "/", http.StatusFound)
}
