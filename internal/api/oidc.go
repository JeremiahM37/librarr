package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
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
	} else {
		slog.Warn("OIDC redirect URI not configured; login will be rejected until OIDC_REDIRECT_URI is set")
	}

	slog.Info("OIDC provider initialized", "issuer", cfg.OIDCIssuer, "provider_name", cfg.OIDCProviderName)

	h := &OIDCHandler{
		cfg:      cfg,
		db:       database,
		sessions: sessions,
		provider: provider,
		verifier: verifier,
		oauth2:   oauth2Cfg,
		states:   make(map[string]time.Time),
	}

	// Periodically clean up expired OIDC state nonces.
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			h.mu.Lock()
			now := time.Now()
			for state, expiry := range h.states {
				if now.After(expiry) {
					delete(h.states, state)
				}
			}
			h.mu.Unlock()
		}
	}()

	return h
}

func (h *OIDCHandler) configuredRedirectURL() (string, error) {
	if h.oauth2.RedirectURL == "" {
		return "", errors.New("OIDC redirect URI is not configured")
	}
	parsed, err := url.Parse(h.oauth2.RedirectURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("OIDC redirect URI is invalid")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("OIDC redirect URI must use http or https")
	}
	return parsed.String(), nil
}

// generateState creates a random state string for CSRF protection.
func (h *OIDCHandler) generateState() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		slog.Error("failed to generate OIDC state token", "error", err)
		seed := time.Now().UTC().Format(time.RFC3339Nano)
		hash := sha256.Sum256([]byte(seed))
		copy(b, hash[:16])
	}
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

// validateAudienceClaim checks that the aud claim matches the configured client ID.
// The aud claim can be a string or an array of strings, both must be supported.
func validateAudienceClaim(aud interface{}, expectedClientID string) bool {
	switch v := aud.(type) {
	case string:
		return v == expectedClientID
	case []interface{}:
		for _, audItem := range v {
			if audStr, ok := audItem.(string); ok && audStr == expectedClientID {
				return true
			}
		}
		return false
	default:
		return false
	}
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

	redirectURL, err := h.configuredRedirectURL()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	oauth2Cfg := h.oauth2
	oauth2Cfg.RedirectURL = redirectURL

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

	redirectURL, err := h.configuredRedirectURL()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	oauth2Cfg := h.oauth2
	oauth2Cfg.RedirectURL = redirectURL

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
		Email             string      `json:"email"`
		EmailVerified     bool        `json:"email_verified"`
		Name              string      `json:"name"`
		PreferredUsername string      `json:"preferred_username"`
		Sub               string      `json:"sub"`
		Aud               interface{} `json:"aud"`
		Iat               int64       `json:"iat"`
	}
	if err := idToken.Claims(&claims); err != nil {
		slog.Error("failed to parse OIDC claims", "error", err)
		http.Error(w, "Failed to parse user info", http.StatusInternalServerError)
		return
	}

	// Enhanced security validations
	// 1. Validate audience (aud) claim matches our client ID
	if !validateAudienceClaim(claims.Aud, h.cfg.OIDCClientID) {
		slog.Warn("OIDC audience claim mismatch", "expected", h.cfg.OIDCClientID, "got", claims.Aud)
		http.Error(w, "Invalid audience claim in ID token", http.StatusForbidden)
		return
	}

	// 2. Validate issued at (iat) timestamp - reject tokens from the future (clock skew tolerance ±1 min)
	if claims.Iat > 0 {
		iatTime := time.Unix(claims.Iat, 0)
		now := time.Now()
		clockSkewTolerance := 1 * time.Minute
		if iatTime.After(now.Add(clockSkewTolerance)) {
			slog.Warn("OIDC token issued in the future", "iat", iatTime, "now", now)
			http.Error(w, "ID token appears to be from the future", http.StatusForbidden)
			return
		}
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
		if _, err := rand.Read(randomPass); err != nil {
			slog.Error("failed to generate random password for OIDC user", "error", err)
			http.Error(w, "Failed to create user account", http.StatusInternalServerError)
			return
		}
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
