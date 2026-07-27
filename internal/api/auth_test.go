package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/search"
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
		{"/opds", true},
		{"/opds/books", true},
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
	if gotRole != "admin" {
		t.Fatalf("role = %q, want admin for first SSO user", gotRole)
	}
	if gotUserID == 0 {
		t.Fatal("expected a resolved user ID")
	}
	if _, err := database.GetUserByUsername("alice"); err != nil {
		t.Fatalf("expected alice user to be created: %v", err)
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

	user, err := resolveOIDCUser(newOIDCTestConfig(), database, "alice")
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
	if got, _ := resp["role"].(string); got != "admin" {
		t.Fatalf("role = %q, want admin", got)
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

func TestHandleConfig_NormalizedFreshInstallRequiresSetup(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	s := &Server{
		cfg:       &config.Config{LibraryRepositoryMode: "normalized", ImportEngine: "legacy"},
		db:        database,
		searchMgr: search.NewManager(&config.Config{}, nil, search.NewHealthTracker(3, 300)),
	}

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rr := httptest.NewRecorder()
	s.handleConfig(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got, _ := resp["has_users"].(bool); got {
		t.Fatalf("has_users = %v, want false", got)
	}
	if got, _ := resp["setup_required"].(bool); !got {
		t.Fatalf("setup_required = %v, want true", got)
	}
	if got, ok := resp["current_user"]; ok && got != nil {
		t.Fatalf("current_user = %v, want omitted or null", got)
	}
	if got, ok := resp["current_role"]; ok && got != nil {
		t.Fatalf("current_role = %v, want omitted or null", got)
	}
}

func TestHandleAuthStatus_NormalizedFreshInstallRequiresSetup(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	req := httptest.NewRequest(http.MethodGet, "/api/auth/status", nil)
	rr := httptest.NewRecorder()
	handleAuthStatus(&config.Config{LibraryRepositoryMode: "normalized"}, database, NewSessionStore())(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got, _ := resp["authenticated"].(bool); got {
		t.Fatalf("authenticated = %v, want false", got)
	}
	if got, _ := resp["setup_required"].(bool); !got {
		t.Fatalf("setup_required = %v, want true", got)
	}
	if _, ok := resp["username"]; ok {
		t.Fatalf("username present on fresh install: %+v", resp["username"])
	}
	if _, ok := resp["role"]; ok {
		t.Fatalf("role present on fresh install: %+v", resp["role"])
	}
}

func TestAuthMiddleware_NormalizedFreshInstallDoesNotFabricateAdmin(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	var gotUser, gotRole string
	handler := authMiddleware(&config.Config{LibraryRepositoryMode: "normalized"}, database, NewSessionStore(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, _ = r.Context().Value(ctxUsername).(string)
		gotRole, _ = r.Context().Value(ctxUserRole).(string)
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rr.Code, rr.Body.String())
	}
	if gotUser != "" {
		t.Fatalf("username = %q, want empty", gotUser)
	}
	if gotRole != "" {
		t.Fatalf("role = %q, want empty", gotRole)
	}
}

func TestFirstUserRegistrationCreatesAuthenticatedAdminSession(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	sessions := NewSessionStore()
	body := `{"username":"admin","password":"secret123"}`
	req := httptest.NewRequest(http.MethodPost, "/api/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handleRegister(database, sessions)(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}

	var registerResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &registerResp); err != nil {
		t.Fatalf("decode register body: %v", err)
	}
	if got, _ := registerResp["role"].(string); got != "admin" {
		t.Fatalf("role = %q, want admin", got)
	}

	var sessionCookie *http.Cookie
	for _, cookie := range rr.Result().Cookies() {
		if cookie.Name == sessionCookieName {
			sessionCookie = cookie
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("expected first-user registration to set a session cookie")
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/api/auth/status", nil)
	statusReq.AddCookie(sessionCookie)
	statusRR := httptest.NewRecorder()
	handleAuthStatus(&config.Config{LibraryRepositoryMode: "normalized"}, database, sessions)(statusRR, statusReq)

	if statusRR.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", statusRR.Code, statusRR.Body.String())
	}
	var statusResp map[string]any
	if err := json.Unmarshal(statusRR.Body.Bytes(), &statusResp); err != nil {
		t.Fatalf("decode status body: %v", err)
	}
	if got, _ := statusResp["authenticated"].(bool); !got {
		t.Fatalf("authenticated = %v, want true", got)
	}
	if got, _ := statusResp["setup_required"].(bool); got {
		t.Fatalf("setup_required = %v, want false after first user", got)
	}
	if got, _ := statusResp["username"].(string); got != "admin" {
		t.Fatalf("username = %q, want admin", got)
	}
	if got, _ := statusResp["role"].(string); got != "admin" {
		t.Fatalf("role = %q, want admin", got)
	}
}

func TestAdminCanCreateUserWithRole(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	hash, _ := hashPassword("secret123")
	if _, err := database.CreateUser("admin", hash, "admin"); err != nil {
		t.Fatal(err)
	}

	body := `{"username":"second","password":"secret123","role":"admin"}`
	req := httptest.NewRequest(http.MethodPost, "/api/register", strings.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxUserRole, "admin"))
	rr := httptest.NewRecorder()
	handleRegister(database, NewSessionStore())(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}
	user, err := database.GetUserByUsername("second")
	if err != nil {
		t.Fatal(err)
	}
	if user.Role != "admin" {
		t.Fatalf("role = %q, want admin", user.Role)
	}
	if !user.Enabled {
		t.Fatal("created users should be enabled")
	}
}

func TestDisabledUserCannotLogin(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	hash, _ := hashPassword("secret123")
	id, err := database.CreateUser("reader", hash, "user")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetUserEnabled(id, false); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"username":"reader","password":"secret123"}`))
	rr := httptest.NewRecorder()
	handleLogin(&config.Config{}, database, NewSessionStore())(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rr.Code, rr.Body.String())
	}
}

func TestAuthMiddlewareRejectsDisabledSession(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	hash, _ := hashPassword("secret123")
	id, err := database.CreateUser("reader", hash, "user")
	if err != nil {
		t.Fatal(err)
	}
	sessions := NewSessionStore()
	token, err := sessions.Create(id, "reader", "user")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetUserEnabled(id, false); err != nil {
		t.Fatal(err)
	}

	called := false
	handler := authMiddleware(&config.Config{}, database, sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/library", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if called {
		t.Fatal("disabled session reached protected handler")
	}
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rr.Code, rr.Body.String())
	}
	if sessions.Valid(token) {
		t.Fatal("disabled session should be removed")
	}
}

func TestUpdateUserCanChangeNameRolePasswordAndEnabled(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	hash, _ := hashPassword("secret123")
	id, err := database.CreateUser("reader", hash, "user")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPatch, "/api/users/1", strings.NewReader(`{"username":"reader2","role":"admin","password":"newsecret","enabled":false}`))
	req.SetPathValue("id", strconv.FormatInt(id, 10))
	rr := httptest.NewRecorder()
	handleUpdateUser(database)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	user, err := database.GetUser(id)
	if err != nil {
		t.Fatal(err)
	}
	if user.Username != "reader2" || user.Role != "admin" || user.Enabled {
		t.Fatalf("updated user = %+v", user)
	}
	if !checkPassword("newsecret", user.PasswordHash) {
		t.Fatal("password was not updated")
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
