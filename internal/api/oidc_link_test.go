package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func newLinkTestHandler(t *testing.T) (*OIDCHandler, *SessionStore) {
	t.Helper()
	sessions := NewSessionStore()
	h := &OIDCHandler{
		cfg:      newOIDCTestConfig(),
		db:       newTestDB(t),
		sessions: sessions,
		oauth2: oauth2.Config{
			ClientID:    "client",
			Endpoint:    oauth2.Endpoint{AuthURL: "https://idp.example.com/authorize"},
			RedirectURL: "https://librarr.example.com/auth/oidc/callback",
		},
		states: make(map[string]oidcState),
	}
	return h, sessions
}

func linkStartRequest(userID int64, password string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/me/sso-link", strings.NewReader(`{"password":"`+password+`"}`))
	return req.WithContext(context.WithValue(req.Context(), ctxUserID, userID))
}

func TestHandleLinkStart(t *testing.T) {
	h, _ := newLinkTestHandler(t)
	id := seedLocalUser(t, h.db, "root", "admin")
	ssoID := seedOIDCUser(t, h.db, "alice", "sub-alice", "user")

	t.Run("wrong password", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.HandleLinkStart(rr, linkStartRequest(id, "wrong"))
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rr.Code)
		}
	})
	t.Run("already SSO", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.HandleLinkStart(rr, linkStartRequest(ssoID, "!"))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rr.Code)
		}
	})
	t.Run("not signed in", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.HandleLinkStart(rr, linkStartRequest(0, "password"))
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rr.Code)
		}
	})
	t.Run("SSO unavailable", func(t *testing.T) {
		rr := httptest.NewRecorder()
		(*OIDCHandler)(nil).HandleLinkStart(rr, linkStartRequest(id, "password"))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rr.Code)
		}
	})
	t.Run("starts the provider flow bound to the user", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.HandleLinkStart(rr, linkStartRequest(id, "password"))
		resp := decodeBody(t, rr)
		redirect, _ := resp["redirect"].(string)
		u, err := url.Parse(redirect)
		if rr.Code != http.StatusOK || err != nil || u.Host != "idp.example.com" {
			t.Fatalf("status=%d body=%v", rr.Code, resp)
		}
		pending, ok := h.validateState(u.Query().Get("state"))
		if !ok || pending.linkUserID != id {
			t.Fatalf("state = %+v ok=%v, want a link state for user %d", pending, ok, id)
		}
	})
}

func callbackRequest(token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/auth/oidc/callback", nil)
	if token != "" {
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	}
	return req
}

func TestCompleteLink_LinksAdminAndRotatesSessions(t *testing.T) {
	h, sessions := newLinkTestHandler(t)
	id := seedLocalUser(t, h.db, "root", "admin")
	if err := h.db.SetTOTPSecret(id, "JBSWY3DPEHPK3PXP"); err != nil {
		t.Fatal(err)
	}
	_ = h.db.EnableTOTP(id)
	token, _ := sessions.Create(id, "root", "admin")
	other, _ := sessions.Create(id, "root", "admin")

	rr := httptest.NewRecorder()
	h.completeLink(rr, callbackRequest(token), id, "sub-root")

	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302; body=%s", rr.Code, rr.Body.String())
	}
	user, _ := h.db.GetUser(id)
	if !user.IsOIDC() || user.OIDCSubject != "sub-root" || user.Role != "admin" {
		t.Fatalf("got source=%q subject=%q role=%q", user.AuthSource, user.OIDCSubject, user.Role)
	}
	if checkPassword("password", user.PasswordHash) || user.TOTPEnabled {
		t.Fatal("linking must remove the password and TOTP")
	}
	if sessions.Valid(token) || sessions.Valid(other) {
		t.Fatal("sessions from the password login must be revoked")
	}
	var fresh string
	for _, c := range rr.Result().Cookies() {
		if c.Name == sessionCookieName {
			fresh = c.Value
		}
	}
	if !sessions.Valid(fresh) {
		t.Fatal("the browser must get a new session")
	}
	// The next SSO login finds the account by subject, whatever the username.
	resolved, err := resolveOIDCUser(h.cfg, h.db, sessions, "someone-else", "sub-root")
	if err != nil || resolved.ID != id {
		t.Fatalf("resolve after link: user=%v err=%v", resolved, err)
	}
}

func TestCompleteLink_Refusals(t *testing.T) {
	h, sessions := newLinkTestHandler(t)
	id := seedLocalUser(t, h.db, "root", "admin")
	bob := seedLocalUser(t, h.db, "bob", "user")
	seedOIDCUser(t, h.db, "alice", "sub-alice", "user")
	rootToken, _ := sessions.Create(id, "root", "admin")
	bobToken, _ := sessions.Create(bob, "bob", "user")

	tests := []struct {
		name    string
		token   string
		subject string
		want    int
	}{
		{"no session", "", "sub-root", http.StatusForbidden},
		{"another user's session", bobToken, "sub-root", http.StatusForbidden},
		{"subject already linked", rootToken, "sub-alice", http.StatusConflict},
		{"no subject", rootToken, "", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.completeLink(rr, callbackRequest(tt.token), id, tt.subject)
			if rr.Code != tt.want {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tt.want, rr.Body.String())
			}
			if user, _ := h.db.GetUser(id); user.IsOIDC() {
				t.Fatal("a refused link must leave the account local")
			}
		})
	}
}

func TestHandleAuthStatus_ReportsAuthSource(t *testing.T) {
	database := newTestDB(t)
	sessions := NewSessionStore()
	seedLocalUser(t, database, "root", "admin")
	id := seedOIDCUser(t, database, "alice", "sub-alice", "user")
	token, _ := sessions.Create(id, "alice", "user")

	req := httptest.NewRequest(http.MethodGet, "/api/auth/status", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rr := httptest.NewRecorder()
	handleAuthStatus(newOIDCTestConfig(), database, sessions)(rr, req)

	if got := decodeBody(t, rr)["auth_source"]; got != "oidc" {
		t.Fatalf("auth_source = %v, want oidc", got)
	}
}

func TestAuthMiddleware_ProxyHeaderForLocalUserFallsThrough(t *testing.T) {
	database := newTestDB(t)
	seedLocalUser(t, database, "root", "admin")
	seedLocalUser(t, database, "bob", "user")
	setTrustedProxies([]string{"192.0.2.1"})
	t.Cleanup(func() { setTrustedProxies(nil) })

	called := false
	handler := authMiddleware(newOIDCTestConfig(), database, NewSessionStore(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.Header.Set("X-Authentik-Username", "bob")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if called || rr.Code != http.StatusUnauthorized {
		t.Fatalf("called=%v status=%d, want the normal 401 without a session", called, rr.Code)
	}
	if u, _ := database.GetUserByUsername("bob"); u.IsOIDC() {
		t.Fatal("bob must stay a local account")
	}
}

func decodeBody(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body %q: %v", rr.Body.String(), err)
	}
	return resp
}
