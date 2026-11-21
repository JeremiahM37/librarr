package api

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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

	// initMu serializes on-demand and background discovery attempts;
	// readyMu guards provider, verifier, oauth2, endSessionEndpoint, and
	// retryInBackground.
	initMu            sync.Mutex
	readyMu           sync.RWMutex
	provider          *oidc.Provider
	verifier          *oidc.IDTokenVerifier
	oauth2            oauth2.Config
	retryInBackground bool

	// endSessionEndpoint is the provider's RP-initiated logout endpoint from
	// discovery, or "" when it doesn't advertise one.
	endSessionEndpoint string

	// State nonce store.
	mu     sync.Mutex
	states map[string]oidcState
}

// oidcState is a pending authorization request.
type oidcState struct {
	expiry time.Time
	// linkUserID is set when a signed-in local user is linking their account
	// to SSO; the callback then links instead of signing in.
	linkUserID int64
}

// Provider discovery at startup is retried, since the provider is often
// deployed alongside Librarr and may not be answering yet. It runs before the
// HTTP server listens, so the worst case (about 25s) stays short enough for
// container health checks. If startup discovery still fails, the handler stays
// mounted and retries in the background and on demand so SSO recovers once the
// provider comes up without requiring a restart.
const (
	oidcDiscoveryAttempts           = 3
	oidcDiscoveryTimeout            = 5 * time.Second
	oidcDiscoveryRetryWait          = 5 * time.Second
	oidcDiscoveryBackgroundInterval = 30 * time.Second
)

// NewOIDCHandler initializes the OIDC provider and returns a handler.
// Returns nil only if OIDC is not configured. If the provider cannot be reached
// at startup, the returned handler is not yet ready (Ready() == false) and will
// retry discovery in the background and on demand.
func NewOIDCHandler(cfg *config.Config, database *db.DB, sessions *SessionStore) *OIDCHandler {
	return newOIDCHandler(cfg, database, sessions, time.Sleep)
}

func newOIDCHandler(cfg *config.Config, database *db.DB, sessions *SessionStore, sleep func(time.Duration)) *OIDCHandler {
	if !cfg.HasOIDC() {
		return nil
	}

	h := &OIDCHandler{
		cfg:      cfg,
		db:       database,
		sessions: sessions,
		states:   make(map[string]oidcState),
	}

	provider, err := discoverOIDCProvider(cfg.OIDCIssuer, sleep)
	if err != nil {
		h.retryInBackground = transientDiscoveryError(err)
		slog.Error("failed to initialize OIDC provider at startup; will retry when the provider becomes reachable",
			"issuer", cfg.OIDCIssuer, "attempts", oidcDiscoveryAttempts, "error", err)
	} else {
		h.applyProvider(provider)
	}

	// Periodically clean up expired OIDC state nonces.
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			h.mu.Lock()
			now := time.Now()
			for state, pending := range h.states {
				if now.After(pending.expiry) {
					delete(h.states, state)
				}
			}
			h.mu.Unlock()
		}
	}()

	return h
}

func (h *OIDCHandler) applyProvider(provider *oidc.Provider) {
	verifier := provider.Verifier(&oidc.Config{ClientID: h.cfg.OIDCClientID})

	oauth2Cfg := oauth2.Config{
		ClientID:     h.cfg.OIDCClientID,
		ClientSecret: h.cfg.OIDCClientSecret,
		Endpoint:     provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}

	if h.cfg.OIDCRedirectURI != "" {
		oauth2Cfg.RedirectURL = h.cfg.OIDCRedirectURI
	}

	var discovery struct {
		EndSessionEndpoint string `json:"end_session_endpoint"`
	}
	if err := provider.Claims(&discovery); err != nil {
		slog.Warn("could not read OIDC discovery metadata; provider logout disabled", "error", err)
	}

	h.readyMu.Lock()
	h.provider = provider
	h.verifier = verifier
	h.oauth2 = oauth2Cfg
	h.endSessionEndpoint = discovery.EndSessionEndpoint
	h.retryInBackground = false
	h.readyMu.Unlock()

	slog.Info("OIDC provider initialized", "issuer", h.cfg.OIDCIssuer, "provider_name", h.cfg.OIDCProviderName,
		"provider_logout", discovery.EndSessionEndpoint != "")
}

func (h *OIDCHandler) isReadyLocked() bool {
	return h.provider != nil || h.oauth2.Endpoint.AuthURL != ""
}

// Ready reports whether OIDC provider discovery has succeeded.
func (h *OIDCHandler) Ready() bool {
	if h == nil {
		return false
	}
	h.readyMu.RLock()
	defer h.readyMu.RUnlock()
	return h.isReadyLocked()
}

// ensureReady initializes the OIDC provider on demand if startup discovery
// did not succeed. Concurrent callers share a single discovery attempt.
func (h *OIDCHandler) ensureReady(ctx context.Context) error {
	if h.Ready() {
		return nil
	}

	h.initMu.Lock()
	defer h.initMu.Unlock()

	if h.Ready() {
		return nil
	}

	discoverCtx, cancel := context.WithTimeout(ctx, oidcDiscoveryTimeout)
	defer cancel()

	provider, err := oidc.NewProvider(discoverCtx, h.cfg.OIDCIssuer)
	if err != nil {
		return err
	}
	h.applyProvider(provider)
	return nil
}

// StartDiscoveryLoop retries OIDC discovery in the background when startup
// discovery failed with a transient error, stopping as soon as discovery
// succeeds, hits a configuration error, or ctx is cancelled.
func (h *OIDCHandler) StartDiscoveryLoop(ctx context.Context) {
	h.startDiscoveryLoop(ctx, oidcDiscoveryBackgroundInterval)
}

func (h *OIDCHandler) startDiscoveryLoop(ctx context.Context, interval time.Duration) {
	if h == nil {
		return
	}
	h.readyMu.RLock()
	shouldRun := !h.isReadyLocked() && h.retryInBackground
	h.readyMu.RUnlock()
	if !shouldRun {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if h.Ready() {
				return
			}
			err := h.ensureReady(ctx)
			if err == nil {
				return
			}
			if !transientDiscoveryError(err) {
				slog.Error("OIDC discovery failed with non-transient error; stopping background retries",
					"issuer", h.cfg.OIDCIssuer, "error", err)
				return
			}
			slog.Warn("OIDC provider still not reachable; will retry",
				"issuer", h.cfg.OIDCIssuer, "retry_in", interval, "error", err)
		}
	}
}

// discoverOIDCProvider fetches the issuer's discovery document, retrying a
// few times so a provider that is still starting doesn't leave SSO off.
func discoverOIDCProvider(issuer string, sleep func(time.Duration)) (*oidc.Provider, error) {
	var err error
	for attempt := 1; attempt <= oidcDiscoveryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), oidcDiscoveryTimeout)
		var provider *oidc.Provider
		provider, err = oidc.NewProvider(ctx, issuer)
		cancel()
		if err == nil {
			return provider, nil
		}
		if !transientDiscoveryError(err) {
			return nil, err
		}
		if attempt < oidcDiscoveryAttempts {
			slog.Warn("OIDC provider not reachable; retrying", "issuer", issuer,
				"attempt", attempt, "retry_in", oidcDiscoveryRetryWait, "error", err)
			sleep(oidcDiscoveryRetryWait)
		}
	}
	return nil, err
}

// transientDiscoveryError reports whether a discovery failure may clear up on
// its own: the provider is unreachable, or answers 5xx/429 while starting.
// A 4xx, a malformed document or an issuer mismatch is a configuration error,
// and retrying it only delays startup.
func transientDiscoveryError(err error) bool {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	// go-oidc reports a non-200 answer only as "<status>: <body>".
	msg := err.Error()
	return strings.HasPrefix(msg, "5") || strings.HasPrefix(msg, "429 ") ||
		strings.HasPrefix(msg, "unable to read response body")
}

// generateState creates a random state string for CSRF protection. It returns
// an error if the CSPRNG fails, so login fails closed rather than proceed with
// a predictable state nonce.
func (h *OIDCHandler) generateState(linkUserID int64) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate OIDC state: %w", err)
	}
	state := hex.EncodeToString(b)

	h.mu.Lock()
	h.states[state] = oidcState{expiry: time.Now().Add(10 * time.Minute), linkUserID: linkUserID}
	h.mu.Unlock()

	return state, nil
}

// validateState checks and consumes a state nonce.
func (h *OIDCHandler) validateState(state string) (oidcState, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	pending, ok := h.states[state]
	if !ok {
		return oidcState{}, false
	}
	delete(h.states, state)
	return pending, time.Now().Before(pending.expiry)
}

// oauth2ConfigFor returns the OAuth2 config with the redirect URI filled in
// from the request when OIDC_REDIRECT_URI is not set.
func (h *OIDCHandler) oauth2ConfigFor(r *http.Request) oauth2.Config {
	h.readyMu.RLock()
	oauth2Cfg := h.oauth2
	h.readyMu.RUnlock()
	if oauth2Cfg.RedirectURL == "" {
		scheme := "http"
		if isSecureRequest(r) {
			scheme = "https"
		}
		oauth2Cfg.RedirectURL = fmt.Sprintf("%s://%s/auth/oidc/callback", scheme, r.Host)
	}
	return oauth2Cfg
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
	if err := h.ensureReady(r.Context()); err != nil {
		slog.Warn("OIDC login unavailable; provider discovery failed", "issuer", h.cfg.OIDCIssuer, "error", err)
		http.Error(w, "OIDC provider is temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	state, err := h.generateState(0)
	if err != nil {
		http.Error(w, "Failed to start login", http.StatusInternalServerError)
		return
	}
	oauth2Cfg := h.oauth2ConfigFor(r)
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
	if err := h.ensureReady(r.Context()); err != nil {
		slog.Warn("OIDC callback unavailable; provider discovery failed", "issuer", h.cfg.OIDCIssuer, "error", err)
		http.Error(w, "OIDC provider is temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	// Validate state.
	state := r.URL.Query().Get("state")
	pending, ok := h.validateState(state)
	if !ok {
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

	oauth2Cfg := h.oauth2ConfigFor(r)

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

	h.readyMu.RLock()
	verifier := h.verifier
	h.readyMu.RUnlock()

	idToken, err := verifier.Verify(ctx, rawIDToken)
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

	if h.cfg.OIDCRequireEmailVerified && claims.Email != "" && !claims.EmailVerified {
		slog.Warn("OIDC login rejected: email not verified", "email", claims.Email)
		http.Error(w, "Email address is not verified", http.StatusForbidden)
		return
	}

	if pending.linkUserID != 0 {
		h.completeLink(w, r, pending.linkUserID, claims.Sub, rawIDToken)
		return
	}

	slog.Info("OIDC login", "username", username, "email", claims.Email, "sub", claims.Sub)

	// Find or create user.
	user, err := resolveOIDCUser(h.cfg, h.db, h.sessions, username, claims.Sub)
	if err != nil {
		slog.Error("failed to resolve OIDC user", "username", username, "error", err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if user == nil {
		http.Error(w, "Failed to resolve OIDC user", http.StatusInternalServerError)
		return
	}

	// Create session.
	h.db.UpdateLastLogin(user.ID)
	sessionToken, err := h.sessions.CreateOIDC(user.ID, user.Username, user.Role, rawIDToken)
	if err != nil {
		http.Error(w, "Failed to create session", http.StatusInternalServerError)
		return
	}
	setSessionCookie(w, r, sessionToken, 86400)

	// Redirect to app root.
	http.Redirect(w, r, "/", http.StatusFound)
}

// providerLogoutURL returns where the browser should go to end the provider's
// session after a Librarr sign-out, or "" to stay in Librarr.
//
//   - OIDC_LOGOUT_URL, when set, is used for every SSO session (including
//     proxy-header ones, where Librarr never sees the provider).
//   - Otherwise a session from the OIDC callback goes to the provider's
//     end_session_endpoint (RP-initiated logout), with its ID token as
//     id_token_hint and OIDC_POST_LOGOUT_REDIRECT_URI if configured.
//   - Local sessions, and providers without an end_session_endpoint, get "".
//
// h may be nil (OIDC not configured or the provider failed to initialise).
func (h *OIDCHandler) providerLogoutURL(cfg *config.Config, session *SessionData, viaProxy bool) string {
	if cfg == nil || !cfg.HasOIDC() {
		return ""
	}
	isSSO := viaProxy || (session != nil && session.OIDC)
	if !isSSO {
		return ""
	}
	if cfg.OIDCLogoutURL != "" {
		return cfg.OIDCLogoutURL
	}
	if h == nil || session == nil || !session.OIDC {
		return ""
	}
	h.readyMu.RLock()
	endSessionEndpoint := h.endSessionEndpoint
	h.readyMu.RUnlock()
	if endSessionEndpoint == "" {
		return ""
	}

	u, err := url.Parse(endSessionEndpoint)
	if err != nil {
		slog.Warn("invalid OIDC end_session_endpoint", "error", err)
		return ""
	}
	q := u.Query()
	q.Set("client_id", cfg.OIDCClientID)
	if session.IDToken != "" {
		q.Set("id_token_hint", session.IDToken)
	}
	// Sent only when configured: providers reject an unregistered value, and
	// without one they show their own signed-out page.
	if cfg.OIDCPostLogoutRedirectURI != "" {
		q.Set("post_logout_redirect_uri", cfg.OIDCPostLogoutRedirectURI)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// HandleLinkStart handles POST /api/me/sso-link: a signed-in local user links
// their own account to their SSO identity. The current password is required,
// so a stolen session alone can't bind an attacker's identity to the account.
// The response carries the provider URL; the callback finishes the link.
func (h *OIDCHandler) HandleLinkStart(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "SSO is not available",
		})
		return
	}
	if err := h.ensureReady(r.Context()); err != nil {
		slog.Warn("OIDC account link unavailable; provider discovery failed", "issuer", h.cfg.OIDCIssuer, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"success": false,
			"error":   "SSO provider is temporarily unavailable",
		})
		return
	}
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}
	user, err := h.db.GetUser(userID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "User not found",
		})
		return
	}
	if user.IsOIDC() {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "This account already signs in with SSO",
		})
		return
	}
	if !checkPassword(req.Password, user.PasswordHash) {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Password is incorrect",
		})
		return
	}

	state, err := h.generateState(user.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to start linking",
		})
		return
	}
	oauth2Cfg := h.oauth2ConfigFor(r)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"redirect": oauth2Cfg.AuthCodeURL(state),
	})
}

// completeLink finishes HandleLinkStart on the OIDC callback: it converts the
// local account to an OIDC account bound to subject and signs the browser in
// with a fresh SSO session (keeping the ID token so sign-out can end the
// provider session). The provider's username claims play no part.
func (h *OIDCHandler) completeLink(w http.ResponseWriter, r *http.Request, userID int64, subject, rawIDToken string) {
	if subject == "" {
		http.Error(w, "The identity provider returned no subject", http.StatusBadRequest)
		return
	}
	// The browser finishing the link must be the one that started it.
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		http.Error(w, "Your session ended. Sign in again and retry linking.", http.StatusForbidden)
		return
	}
	if data, ok := h.sessions.Get(cookie.Value); !ok || data.UserID != userID {
		http.Error(w, "Your session ended. Sign in again and retry linking.", http.StatusForbidden)
		return
	}
	const alreadyLinked = "This SSO identity is already linked to another Librarr account"
	if _, err := h.db.GetUserByOIDCSubject(subject); err == nil {
		http.Error(w, alreadyLinked, http.StatusConflict)
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		slog.Error("failed to look up OIDC subject", "error", err)
		http.Error(w, "Failed to link account", http.StatusInternalServerError)
		return
	}
	if err := h.db.LinkUserToOIDC(userID, subject); err != nil {
		if errors.Is(err, db.ErrUserNotClaimable) {
			http.Error(w, "This account already signs in with SSO", http.StatusConflict)
			return
		}
		// Another link for the same subject won the race.
		if strings.Contains(err.Error(), "UNIQUE") {
			http.Error(w, alreadyLinked, http.StatusConflict)
			return
		}
		slog.Error("failed to link account to OIDC", "id", userID, "error", err)
		http.Error(w, "Failed to link account", http.StatusInternalServerError)
		return
	}
	user, err := h.db.GetUser(userID)
	if err != nil {
		http.Error(w, "Failed to load linked account", http.StatusInternalServerError)
		return
	}

	// Sessions from the password login end; this browser gets a new one.
	h.sessions.DeleteForUser(user.ID)
	h.db.LogActivity(user.Username, "sso_link", user.Username, "Account linked to SSO")
	slog.Info("account linked to OIDC", "id", user.ID, "username", user.Username)

	h.db.UpdateLastLogin(user.ID)
	sessionToken, err := h.sessions.CreateOIDC(user.ID, user.Username, user.Role, rawIDToken)
	if err != nil {
		http.Error(w, "Failed to create session", http.StatusInternalServerError)
		return
	}
	setSessionCookie(w, r, sessionToken, 86400)
	http.Redirect(w, r, "/", http.StatusFound)
}
