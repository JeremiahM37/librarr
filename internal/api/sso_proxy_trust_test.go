package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/db"
)

// GHSA-mqf7-4vg4-fxj7: the SSO identity headers were honored from any client,
// so one spoofed header logged the sender in as any user — or created one.
func TestAuthMiddleware_IgnoresIdentityHeadersFromUntrustedPeer(t *testing.T) {
	t.Cleanup(func() { setTrustedProxies(nil) })

	tests := []struct {
		name    string
		trusted []string
	}{
		// The allowlist an operator sets to defend this feature was never
		// consulted, so "configured but the peer is someone else" is the
		// case that matters most.
		{"peer is not the configured proxy", []string{"10.0.0.5"}},
		{"no trusted proxies configured", nil},
		{"only invalid entries configured", []string{"not-an-ip"}},
	}

	for _, tt := range tests {
		for _, header := range authentikIdentityHeaders {
			t.Run(tt.name+"/"+header, func(t *testing.T) {
				dir := t.TempDir()
				database, err := db.New(filepath.Join(dir, "test.db"))
				if err != nil {
					t.Fatalf("create test db: %v", err)
				}
				t.Cleanup(func() { database.Close() })

				// An existing account makes this a closed instance; with zero
				// users everything is admitted as "local" and the probe would
				// pass without ever reaching the header branch.
				hash, _ := hashPassword("pw")
				if _, err := database.CreateUser("admin", hash, "admin"); err != nil {
					t.Fatalf("seed admin: %v", err)
				}
				setTrustedProxies(tt.trusted)

				reached := false
				handler := authMiddleware(newOIDCTestConfig(), database, NewSessionStore(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					reached = true
				}))

				for _, identity := range []string{"admin", "attacker"} {
					req := httptest.NewRequest(http.MethodGet, "/api/settings", nil) // peer 192.0.2.1
					req.Header.Set(header, identity)
					rr := httptest.NewRecorder()
					handler.ServeHTTP(rr, req)

					if rr.Code != http.StatusUnauthorized || reached {
						t.Fatalf("spoofed %s: %s = %d (handler reached: %v), want 401", header, identity, rr.Code, reached)
					}
					if len(rr.Result().Cookies()) != 0 {
						t.Fatalf("spoofed %s: %s was issued a cookie", header, identity)
					}
				}
				if _, err := database.GetUserByUsername("attacker"); err == nil {
					t.Fatal("spoofed header auto-provisioned a user")
				}
				if n, _ := database.CountUsers(); n != 1 {
					t.Fatalf("user count = %d, want 1", n)
				}
			})
		}
	}
}

// Ignored, not rejected: someone connecting directly with a real session must
// not be locked out because a header happens to be present.
func TestAuthMiddleware_UntrustedIdentityHeaderDoesNotBreakRealSession(t *testing.T) {
	setTrustedProxies(nil)
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	hash, _ := hashPassword("pw")
	id, _ := database.CreateUser("bob", hash, "user")
	if _, err := database.CreateUser("admin", hash, "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}

	sessions := NewSessionStore()
	token, _ := sessions.Create(id, "bob", "user")

	var gotUser, gotRole string
	handler := authMiddleware(newOIDCTestConfig(), database, sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, _ = r.Context().Value(ctxUsername).(string)
		gotRole, _ = r.Context().Value(ctxUserRole).(string)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/library", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	req.Header.Set("Remote-User", "admin")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if gotUser != "bob" || gotRole != "user" {
		t.Fatalf("identity = %s/%s, want bob/user — the header must not override the session", gotUser, gotRole)
	}
}
