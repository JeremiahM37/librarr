package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
)

func TestSessionStore_CreateAndGet(t *testing.T) {
	store := NewSessionStore()

	token, err := store.Create(1, "testuser", "admin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	data, ok := store.Get(token)
	if !ok {
		t.Fatal("expected session to be valid")
	}
	if data.UserID != 1 {
		t.Errorf("expected user ID 1, got %d", data.UserID)
	}
	if data.Username != "testuser" {
		t.Errorf("expected username testuser, got %s", data.Username)
	}
	if data.Role != "admin" {
		t.Errorf("expected role admin, got %s", data.Role)
	}
}

func TestSessionStore_Valid(t *testing.T) {
	store := NewSessionStore()
	token, err := store.Create(1, "user", "admin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if !store.Valid(token) {
		t.Error("expected token to be valid")
	}

	if store.Valid("nonexistent-token") {
		t.Error("expected nonexistent token to be invalid")
	}
}

func TestSessionStore_Delete(t *testing.T) {
	store := NewSessionStore()
	token, err := store.Create(1, "user", "admin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	store.Delete(token)
	if store.Valid(token) {
		t.Error("expected deleted token to be invalid")
	}
}

func TestSessionStore_Expiry(t *testing.T) {
	store := NewSessionStore()
	token, err := store.Create(1, "user", "admin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Manually expire the session
	store.mu.Lock()
	store.sessions[token].Expiry = time.Now().Add(-1 * time.Hour)
	store.mu.Unlock()

	if store.Valid(token) {
		t.Error("expected expired token to be invalid")
	}

	// Should also be cleaned up from the store
	store.mu.RLock()
	_, exists := store.sessions[token]
	store.mu.RUnlock()
	if exists {
		t.Error("expected expired session to be deleted from store")
	}
}

func TestSessionStore_PendingTOTP(t *testing.T) {
	store := NewSessionStore()

	t.Run("create and validate", func(t *testing.T) {
		token, err := store.CreatePendingTOTP(42)
		if err != nil {
			t.Fatalf("CreatePendingTOTP: %v", err)
		}
		if token == "" {
			t.Fatal("expected non-empty pending token")
		}

		userID, valid := store.ValidatePendingTOTP(token)
		if !valid {
			t.Error("expected pending TOTP to be valid")
		}
		if userID != 42 {
			t.Errorf("expected user ID 42, got %d", userID)
		}
	})

	t.Run("consumed after first use", func(t *testing.T) {
		token, err := store.CreatePendingTOTP(1)
		if err != nil {
			t.Fatalf("CreatePendingTOTP: %v", err)
		}
		store.ValidatePendingTOTP(token)

		_, valid := store.ValidatePendingTOTP(token)
		if valid {
			t.Error("expected pending TOTP to be consumed after use")
		}
	})

	t.Run("expired pending TOTP", func(t *testing.T) {
		token, err := store.CreatePendingTOTP(1)
		if err != nil {
			t.Fatalf("CreatePendingTOTP: %v", err)
		}

		store.mu.Lock()
		store.pendingTOTP[token].Expiry = time.Now().Add(-1 * time.Minute)
		store.mu.Unlock()

		_, valid := store.ValidatePendingTOTP(token)
		if valid {
			t.Error("expected expired pending TOTP to be invalid")
		}
	})

	t.Run("nonexistent token", func(t *testing.T) {
		_, valid := store.ValidatePendingTOTP("nonexistent")
		if valid {
			t.Error("expected nonexistent token to be invalid")
		}
	})
}

func TestSessionStore_UniqueTokens(t *testing.T) {
	store := NewSessionStore()
	tokens := make(map[string]bool)

	for i := 0; i < 100; i++ {
		token, err := store.Create(int64(i), "user", "admin")
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if tokens[token] {
			t.Fatalf("duplicate token generated: %s", token)
		}
		tokens[token] = true
	}
}

func TestHashPassword_And_CheckPassword(t *testing.T) {
	password := "mysecretpassword"
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hashPassword failed: %v", err)
	}

	if !checkPassword(password, hash) {
		t.Error("expected correct password to match")
	}

	if checkPassword("wrongpassword", hash) {
		t.Error("expected wrong password to not match")
	}

	if checkPassword("", hash) {
		t.Error("expected empty password to not match")
	}
}

func TestHashBackupCode(t *testing.T) {
	code := "12345678"
	hash1 := hashBackupCode(code)
	hash2 := hashBackupCode(code)

	if hash1 != hash2 {
		t.Error("expected same hash for same code")
	}

	hash3 := hashBackupCode("87654321")
	if hash1 == hash3 {
		t.Error("expected different hash for different code")
	}

	if len(hash1) != 64 {
		t.Errorf("expected SHA-256 hex length 64, got %d", len(hash1))
	}
}

func TestSetSessionCookieSecureFlag(t *testing.T) {
	// httptest.NewRequest sets RemoteAddr to 192.0.2.1:1234, so a trusted-proxy
	// entry of 192.0.2.1/32 marks the request's peer as a trusted proxy.
	t.Cleanup(func() { setTrustedProxies(nil) })

	tests := []struct {
		name       string
		trusted    []string
		configure  func(*http.Request)
		wantSecure bool
	}{
		{
			name: "plain HTTP dev request",
		},
		{
			name: "direct TLS request",
			configure: func(r *http.Request) {
				r.TLS = &tls.ConnectionState{}
			},
			wantSecure: true,
		},
		{
			name:    "X-Forwarded-Proto from trusted proxy",
			trusted: []string{"192.0.2.1/32"},
			configure: func(r *http.Request) {
				r.Header.Set("X-Forwarded-Proto", "https")
			},
			wantSecure: true,
		},
		{
			name:    "standard Forwarded header from trusted proxy",
			trusted: []string{"192.0.2.1/32"},
			configure: func(r *http.Request) {
				r.Header.Set("Forwarded", "for=192.0.2.60;proto=https;host=books.example.com")
			},
			wantSecure: true,
		},
		{
			name: "forwarded HTTPS from untrusted peer is ignored",
			configure: func(r *http.Request) {
				r.Header.Set("X-Forwarded-Proto", "https")
			},
			wantSecure: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTrustedProxies(tt.trusted)
			req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
			if tt.configure != nil {
				tt.configure(req)
			}
			rr := httptest.NewRecorder()
			setSessionCookie(rr, req, "token", 86400)

			cookies := rr.Result().Cookies()
			if len(cookies) != 1 {
				t.Fatalf("cookie count = %d, want 1", len(cookies))
			}
			if cookies[0].Secure != tt.wantSecure {
				t.Fatalf("Secure = %v, want %v", cookies[0].Secure, tt.wantSecure)
			}
		})
	}
}

func TestIsExempt(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/", true},
		{"/health", true},
		{"/api/health", true},
		{"/api/login", true},
		{"/api/login/totp", true},
		{"/api/register", true},
		{"/api/auth/status", true},
		{"/readyz", true},
		{"/torznab/api", true},
		{"/torznab/api?t=caps", true},
		{"/static/style.css", true},
		// OPDS lists and serves the library, so it must never be exempt
		// (GHSA-368r-6vrp-m3pw). E-readers use HTTP Basic instead.
		{"/opds", false},
		{"/opds/", false},
		{"/opds/books", false},
		{"/opds/search", false},
		{"/opds/download/1", false},
		{"/opds/opensearch.xml", false},
		{"/metrics", true},
		{"/auth/oidc/callback", true},
		// OpenAPI spec is public so AI agents / tooling can introspect the
		// API without prior auth (same precedent as /metrics and /health).
		{"/api/openapi.json", true},
		{"/api/search", false},
		{"/api/download", false},
		{"/api/library", false},
		{"/api/users", false},
		// Suffix-attack guard — only the exact path should be exempt.
		{"/api/openapi.jsonx", false},
		// Prowlarr's indexer-discovery probe hits bare /api — must be exempt
		// because the Torznab handler does its own apikey check. But the
		// exemption MUST be exact-path only; any /api/... JSON endpoint
		// must still require auth.
		{"/api", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := isExempt(tt.path)
			if result != tt.expected {
				t.Errorf("isExempt(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestHandleAuthStatus_OIDCHints(t *testing.T) {
	// /api/auth/status is the canonical pre-auth endpoint the login modal
	// hits to decide whether to render the SSO button. /api/config is gated
	// behind the multi-user middleware once any user exists, so the modal
	// MUST be able to read the OIDC hint here or the button silently
	// disappears after the first user registers.
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	sessions := NewSessionStore()

	tests := []struct {
		name             string
		cfg              *config.Config
		wantEnabled      bool
		wantProviderName string
	}{
		{
			name:        "nil config",
			cfg:         nil,
			wantEnabled: false,
		},
		{
			name:        "OIDC disabled",
			cfg:         &config.Config{OIDCEnabled: false},
			wantEnabled: false,
		},
		{
			name: "OIDC partially configured (no client secret)",
			cfg: &config.Config{
				OIDCEnabled:      true,
				OIDCIssuer:       "https://idp.example.com",
				OIDCClientID:     "client",
				OIDCProviderName: "Ignored",
			},
			wantEnabled: false,
		},
		{
			name: "OIDC fully configured",
			cfg: &config.Config{
				OIDCEnabled:      true,
				OIDCIssuer:       "https://idp.example.com",
				OIDCClientID:     "client",
				OIDCClientSecret: "secret",
				OIDCProviderName: "PocketID",
			},
			wantEnabled:      true,
			wantProviderName: "PocketID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/auth/status", nil)
			rr := httptest.NewRecorder()
			handleAuthStatus(tt.cfg, database, sessions)(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
			}
			var resp map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			enabled, _ := resp["oidc_enabled"].(bool)
			if enabled != tt.wantEnabled {
				t.Errorf("oidc_enabled = %v, want %v", enabled, tt.wantEnabled)
			}
			if tt.wantEnabled {
				if got, _ := resp["oidc_provider_name"].(string); got != tt.wantProviderName {
					t.Errorf("oidc_provider_name = %q, want %q", got, tt.wantProviderName)
				}
				// MUST NOT leak the client secret on the public endpoint.
				for k, v := range resp {
					if s, ok := v.(string); ok && s == "secret" {
						t.Errorf("response leaks secret-like value at key %q", k)
					}
				}
			} else if _, present := resp["oidc_provider_name"]; present {
				t.Errorf("oidc_provider_name MUST be absent when OIDC is disabled, got %v", resp["oidc_provider_name"])
			}
		})
	}
}

func newOIDCTestConfig() *config.Config {
	return &config.Config{
		OIDCEnabled:         true,
		OIDCIssuer:          "https://idp.example.com",
		OIDCClientID:        "client",
		OIDCClientSecret:    "secret",
		OIDCAutoCreateUsers: true,
		OIDCDefaultRole:     "user",
		OIDCProxyHeaders:    true,
	}
}

func TestAuthMiddleware_AcceptsAuthentikProxyHeaders(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	sessions := NewSessionStore()

	// httptest.NewRequest's peer is 192.0.2.1; identity headers are honored
	// only from a configured reverse proxy.
	setTrustedProxies([]string{"192.0.2.1"})
	t.Cleanup(func() { setTrustedProxies(nil) })

	// SSO needs a local admin to exist first.
	seedLocalUser(t, database, "root", "admin")

	var gotUsername, gotRole string
	var gotUserID int64
	handler := authMiddleware(newOIDCTestConfig(), database, sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserID = getUserIDFromContext(r)
		gotUsername, _ = r.Context().Value(ctxUsername).(string)
		gotRole, _ = r.Context().Value(ctxUserRole).(string)
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.Header.Set("X-Authentik-Username", "alice")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rr.Code, rr.Body.String())
	}
	if gotUsername != "alice" {
		t.Fatalf("username = %q, want alice", gotUsername)
	}
	if gotRole != "user" {
		t.Fatalf("role = %q, want the OIDC default role", gotRole)
	}
	if gotUserID == 0 {
		t.Fatal("expected a resolved user ID")
	}
	if u, err := database.GetUserByUsername("alice"); err != nil {
		t.Fatalf("expected alice user to be created: %v", err)
	} else if !u.IsOIDC() {
		t.Fatalf("auth_source = %q, want oidc", u.AuthSource)
	}

	var sessionCookie bool
	for _, cookie := range rr.Result().Cookies() {
		if cookie.Name == sessionCookieName {
			sessionCookie = true
			break
		}
	}
	if !sessionCookie {
		t.Fatal("expected proxy-auth login to set a session cookie")
	}
}

func TestAuthMiddleware_IgnoresAuthentikHeadersWhenOIDCDisabled(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	sessions := NewSessionStore()

	hash, err := hashPassword("password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if _, err := database.CreateUser("existing", hash, "admin"); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	called := false
	handler := authMiddleware(&config.Config{}, database, sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.Header.Set("X-Authentik-Username", "alice")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if called {
		t.Fatal("expected proxy header to be ignored when OIDC is disabled")
	}
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

func TestAuthMiddleware_IgnoresAuthentikHeadersWhenProxyHeadersDisabled(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	sessions := NewSessionStore()

	hash, err := hashPassword("password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if _, err := database.CreateUser("existing", hash, "admin"); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	cfg := newOIDCTestConfig()
	cfg.OIDCProxyHeaders = false

	called := false
	handler := authMiddleware(cfg, database, sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.Header.Set("X-Authentik-Username", "alice")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if called {
		t.Fatal("expected proxy header to be ignored when proxy header auth is disabled")
	}
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

func TestHandleAuthStatus_AuthentikProxyHeaders(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	sessions := NewSessionStore()

	seedLocalUser(t, database, "root", "admin")
	user, err := resolveOIDCUser(newOIDCTestConfig(), database, sessions, "alice", "")
	if err != nil {
		t.Fatalf("seed SSO user: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/auth/status", nil)
	req.Header.Set("X-Authentik-Username", "alice")
	req = req.WithContext(context.WithValue(req.Context(), ctxUserID, user.ID))
	req = req.WithContext(context.WithValue(req.Context(), ctxUsername, user.Username))
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, user.Role))
	rr := httptest.NewRecorder()
	handleAuthStatus(newOIDCTestConfig(), database, sessions)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got, _ := resp["authenticated"].(bool); !got {
		t.Fatalf("authenticated = %v, want true", got)
	}
	if got, _ := resp["username"].(string); got != "alice" {
		t.Fatalf("username = %q, want alice", got)
	}
	if got, _ := resp["role"].(string); got != "user" {
		t.Fatalf("role = %q, want user", got)
	}
	if got, _ := resp["user_id"].(float64); got == 0 {
		t.Fatal("expected a resolved user_id")
	}
}

func TestHandleAuthStatus_ProxyHeaderDoesNotCreateUser(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	sessions := NewSessionStore()

	req := httptest.NewRequest(http.MethodGet, "/api/auth/status", nil)
	req.Header.Set("X-Authentik-Username", "alice")
	rr := httptest.NewRecorder()
	handleAuthStatus(newOIDCTestConfig(), database, sessions)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	count, err := database.CountUsers()
	if err != nil {
		t.Fatalf("count users: %v", err)
	}
	if count != 0 {
		t.Fatalf("user count = %d, want 0", count)
	}
}

// TestAuthMiddleware_OpenInstanceGrantsAdmin is a regression test for issue
// #76: on a userless instance with no auth and no API key configured, the
// open-access pass-through must mark the caller as admin so admin-gated routes
// (POST /api/settings, etc.) work instead of failing requireAdmin with a 403.
func TestAuthMiddleware_OpenInstanceGrantsAdmin(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	// Empty config => no legacy auth, no API key. No users in the DB => not
	// multi-user. This is a fresh, open instance.
	called := false
	var gotRole string
	final := func(w http.ResponseWriter, r *http.Request) {
		called = true
		gotRole, _ = r.Context().Value(ctxUserRole).(string)
		w.WriteHeader(http.StatusNoContent)
	}
	handler := authMiddleware(&config.Config{}, database, NewSessionStore(), requireAdmin(final))

	req := httptest.NewRequest(http.MethodPost, "/api/settings", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rr.Code, rr.Body.String())
	}
	if !called {
		t.Fatal("admin-gated handler was not reached on an open instance")
	}
	if gotRole != "admin" {
		t.Fatalf("role = %q, want admin on an open instance", gotRole)
	}
}

// --- OIDC account identity ---

func newTestDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

// seedLocalUser creates a local account with password "password".
func seedLocalUser(t *testing.T, database *db.DB, username, role string) int64 {
	t.Helper()
	hash, err := hashPassword("password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	id, err := database.CreateUser(username, hash, role)
	if err != nil {
		t.Fatalf("seed user %s: %v", username, err)
	}
	return id
}

func seedOIDCUser(t *testing.T, database *db.DB, username, subject, role string) int64 {
	t.Helper()
	id, err := database.CreateOIDCUser(username, subject, role)
	if err != nil {
		t.Fatalf("seed OIDC user %s: %v", username, err)
	}
	return id
}

func TestResolveOIDCUser_RefusedWithoutLocalAdmin(t *testing.T) {
	database := newTestDB(t)
	// An OIDC admin does not count: it cannot sign in when the IdP is down.
	seedOIDCUser(t, database, "sso-admin", "sub-admin", "admin")

	_, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "alice", "sub-alice")
	if !errors.Is(err, errOIDCNoLocalAdmin) {
		t.Fatalf("err = %v, want errOIDCNoLocalAdmin", err)
	}
	if _, err := database.GetUserByUsername("alice"); err == nil {
		t.Fatal("alice must not be created without a local admin")
	}
}

func TestResolveOIDCUser_CreatesOIDCUserWithSubject(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")

	user, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "alice", "sub-alice")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !user.IsOIDC() || user.OIDCSubject != "sub-alice" || user.Role != "user" {
		t.Fatalf("got source=%q subject=%q role=%q", user.AuthSource, user.OIDCSubject, user.Role)
	}
	if checkPassword("", user.PasswordHash) || checkPassword("!", user.PasswordHash) {
		t.Fatal("OIDC account must have an unusable password hash")
	}
}

func TestResolveOIDCUser_MatchesBySubject(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "sub-alice", "user")

	// The IdP renamed the user; the subject still identifies them.
	user, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "alice-renamed", "sub-alice")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if user.ID != id {
		t.Fatalf("user ID = %d, want %d", user.ID, id)
	}
	if _, err := database.GetUserByUsername("alice-renamed"); err == nil {
		t.Fatal("subject match must not create a second account")
	}
}

func TestResolveOIDCUser_LinksSubjectToProxyCreatedUser(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "", "user")

	user, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "alice", "sub-alice")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if user.ID != id {
		t.Fatalf("user ID = %d, want %d", user.ID, id)
	}
	stored, _ := database.GetUser(id)
	if stored.OIDCSubject != "sub-alice" {
		t.Fatalf("stored subject = %q, want sub-alice", stored.OIDCSubject)
	}
}

func TestResolveOIDCUser_ProxyMatchesOIDCUserByUsername(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "sub-alice", "user")

	user, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "alice", "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if user.ID != id {
		t.Fatalf("user ID = %d, want %d", user.ID, id)
	}
}

func TestResolveOIDCUser_SubjectMismatchRejected(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	seedOIDCUser(t, database, "alice", "sub-alice", "user")

	// A different IdP identity that claims the same username.
	_, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "alice", "sub-mallory")
	if !errors.Is(err, errOIDCSubjectMismatch) {
		t.Fatalf("err = %v, want errOIDCSubjectMismatch", err)
	}
}

func TestResolveOIDCUser_LocalAdminNotClaimable(t *testing.T) {
	for _, subject := range []string{"sub-root", ""} {
		database := newTestDB(t)
		id := seedLocalUser(t, database, "root", "admin")

		_, err := resolveOIDCUser(newOIDCTestConfig(), database, NewSessionStore(), "root", subject)
		if !errors.Is(err, errOIDCLocalAdmin) {
			t.Fatalf("subject %q: err = %v, want errOIDCLocalAdmin", subject, err)
		}
		stored, _ := database.GetUser(id)
		if stored.IsOIDC() || !checkPassword("password", stored.PasswordHash) {
			t.Fatalf("subject %q: local admin was modified", subject)
		}
	}
}

func TestResolveOIDCUser_ClaimsLocalUser(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	id := seedLocalUser(t, database, "bob", "user")
	if err := database.SetTOTPSecret(id, "JBSWY3DPEHPK3PXP"); err != nil {
		t.Fatalf("set totp: %v", err)
	}
	if err := database.EnableTOTP(id); err != nil {
		t.Fatalf("enable totp: %v", err)
	}
	if err := database.SaveBackupCodes(id, []string{hashBackupCode("12345678")}); err != nil {
		t.Fatalf("save backup codes: %v", err)
	}

	sessions := NewSessionStore()
	oldToken, _ := sessions.Create(id, "bob", "user")
	otherToken, _ := sessions.Create(999, "other", "user")
	pending, _ := sessions.CreatePendingTOTP(id)

	user, err := resolveOIDCUser(newOIDCTestConfig(), database, sessions, "bob", "sub-bob")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if user.ID != id || !user.IsOIDC() || user.OIDCSubject != "sub-bob" {
		t.Fatalf("got id=%d source=%q subject=%q", user.ID, user.AuthSource, user.OIDCSubject)
	}
	if checkPassword("password", user.PasswordHash) {
		t.Fatal("claimed account must not accept its old password")
	}
	if user.TOTPEnabled || user.TOTPSecret != "" {
		t.Fatal("claimed account must have TOTP cleared")
	}
	if used, _ := database.UseBackupCode(id, hashBackupCode("12345678")); used {
		t.Fatal("claimed account must have its backup codes removed")
	}
	if sessions.Valid(oldToken) {
		t.Fatal("old local session must be revoked")
	}
	if _, ok := sessions.ValidatePendingTOTP(pending); ok {
		t.Fatal("pending TOTP login must be revoked")
	}
	if !sessions.Valid(otherToken) {
		t.Fatal("other users' sessions must be kept")
	}

	// The claim is permanent: the next login matches by subject.
	again, err := resolveOIDCUser(newOIDCTestConfig(), database, sessions, "bob", "sub-bob")
	if err != nil || again.ID != id {
		t.Fatalf("second login: user=%v err=%v", again, err)
	}
}

func TestResolveOIDCUser_NoAutoCreate(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	cfg := newOIDCTestConfig()
	cfg.OIDCAutoCreateUsers = false

	if _, err := resolveOIDCUser(cfg, database, NewSessionStore(), "alice", "sub-alice"); err == nil {
		t.Fatal("expected an error when auto-creation is disabled")
	}
}

func TestAuthMiddleware_ProxyHeaderForLocalAdminFallsThrough(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	setTrustedProxies([]string{"192.0.2.1"})
	t.Cleanup(func() { setTrustedProxies(nil) })

	called := false
	handler := authMiddleware(newOIDCTestConfig(), database, NewSessionStore(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.Header.Set("X-Authentik-Username", "root")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if called || rr.Code != http.StatusUnauthorized {
		t.Fatalf("called=%v status=%d, want the normal 401 without a session", called, rr.Code)
	}
}

// adminRequest builds a request carrying an admin session context.
func adminRequest(method, target, body string, adminID int64) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	ctx := context.WithValue(req.Context(), ctxUserID, adminID)
	ctx = context.WithValue(ctx, ctxUserRole, "admin")
	ctx = context.WithValue(ctx, ctxUsername, "admin")
	return req.WithContext(ctx)
}

func TestHandleUpdateUser_RejectsPasswordForOIDCUser(t *testing.T) {
	database := newTestDB(t)
	adminID := seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "sub-alice", "user")

	req := adminRequest(http.MethodPatch, "/api/users/x", `{"role":"admin","password":"newpassword"}`, adminID)
	req.SetPathValue("id", strconv.FormatInt(id, 10))
	rr := httptest.NewRecorder()
	handleUpdateUser(database)(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
	stored, _ := database.GetUser(id)
	if stored.Role != "user" {
		t.Fatal("role must not change when the request is rejected")
	}
	if checkPassword("newpassword", stored.PasswordHash) {
		t.Fatal("OIDC account must not get a password")
	}
}

func TestHandleUpdateUser_RejectsDemotingLastLocalAdmin(t *testing.T) {
	database := newTestDB(t)
	id := seedLocalUser(t, database, "root", "admin")
	seedOIDCUser(t, database, "sso-admin", "sub-admin", "admin")

	req := adminRequest(http.MethodPatch, "/api/users/x", `{"role":"user","password":"newpassword"}`, id)
	req.SetPathValue("id", strconv.FormatInt(id, 10))
	rr := httptest.NewRecorder()
	handleUpdateUser(database)(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", rr.Code, rr.Body.String())
	}
	stored, _ := database.GetUser(id)
	if stored.Role != "admin" || !checkPassword("password", stored.PasswordHash) {
		t.Fatal("rejected request must not change the role or password")
	}
}

func TestHandleUpdateUser_AllowsDemotingWithAnotherLocalAdmin(t *testing.T) {
	database := newTestDB(t)
	id := seedLocalUser(t, database, "root", "admin")
	seedLocalUser(t, database, "root2", "admin")

	req := adminRequest(http.MethodPatch, "/api/users/x", `{"role":"user"}`, id)
	req.SetPathValue("id", strconv.FormatInt(id, 10))
	rr := httptest.NewRecorder()
	handleUpdateUser(database)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleDeleteUser_RejectsDeletingLastLocalAdmin(t *testing.T) {
	database := newTestDB(t)
	id := seedLocalUser(t, database, "root", "admin")
	ssoAdmin := seedOIDCUser(t, database, "sso-admin", "sub-admin", "admin")

	// Requested by the OIDC admin, so the self-delete and last-admin guards
	// don't apply; only the local-admin guard does.
	req := adminRequest(http.MethodDelete, "/api/users/x", "", ssoAdmin)
	req.SetPathValue("id", strconv.FormatInt(id, 10))
	rr := httptest.NewRecorder()
	handleDeleteUser(database)(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", rr.Code, rr.Body.String())
	}
	if _, err := database.GetUser(id); err != nil {
		t.Fatal("last local admin must not be deleted")
	}
}

func TestHandleChangeOwnPassword_RejectsOIDCUser(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "sub-alice", "user")

	req := adminRequest(http.MethodPost, "/api/me/password", `{"current_password":"x","new_password":"newpassword"}`, id)
	rr := httptest.NewRecorder()
	handleChangeOwnPassword(database)(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleTOTPSetup_RejectsOIDCUser(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "sub-alice", "user")

	req := adminRequest(http.MethodPost, "/api/totp/setup", "", id)
	rr := httptest.NewRecorder()
	handleTOTPSetup(database)(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
	if stored, _ := database.GetUser(id); stored.TOTPSecret != "" {
		t.Fatal("OIDC account must not get a TOTP secret")
	}
}

func TestHandleListUsers_IncludesAuthSource(t *testing.T) {
	database := newTestDB(t)
	adminID := seedLocalUser(t, database, "root", "admin")
	seedOIDCUser(t, database, "alice", "sub-alice", "user")

	rr := httptest.NewRecorder()
	handleListUsers(database)(rr, adminRequest(http.MethodGet, "/api/users", "", adminID))

	var resp struct {
		Users []map[string]any `json:"users"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := map[string]string{}
	for _, u := range resp.Users {
		got[u["username"].(string)], _ = u["auth_source"].(string)
		if _, leaked := u["oidc_subject"]; leaked {
			t.Fatal("oidc_subject must not be exposed")
		}
	}
	if got["root"] != "local" || got["alice"] != "oidc" {
		t.Fatalf("auth_source = %v", got)
	}
}
