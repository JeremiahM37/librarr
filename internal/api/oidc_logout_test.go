package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

// newDiscoveryServer serves a minimal OIDC discovery document. endSession is
// advertised as end_session_endpoint when non-empty.
func newDiscoveryServer(t *testing.T, endSession string) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		doc := map[string]any{
			"issuer":                                srv.URL,
			"authorization_endpoint":                srv.URL + "/authorize",
			"token_endpoint":                        srv.URL + "/token",
			"jwks_uri":                              srv.URL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		}
		if endSession != "" {
			doc["end_session_endpoint"] = endSession
		}
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestNewOIDCHandler_ReadsEndSessionEndpoint(t *testing.T) {
	for _, endSession := range []string{"https://idp.example.com/logout", ""} {
		srv := newDiscoveryServer(t, endSession)
		cfg := newOIDCTestConfig()
		cfg.OIDCIssuer = srv.URL

		h := NewOIDCHandler(cfg, newTestDB(t), NewSessionStore())
		if h == nil {
			t.Fatal("expected the handler to initialise")
		}
		if h.endSessionEndpoint != endSession {
			t.Fatalf("endSessionEndpoint = %q, want %q", h.endSessionEndpoint, endSession)
		}
	}
}

func TestProviderLogoutURL(t *testing.T) {
	const endSession = "https://idp.example.com/logout?existing=1"
	withEndpoint := &OIDCHandler{endSessionEndpoint: endSession}
	oidcSession := &SessionData{UserID: 2, OIDC: true, IDToken: "raw.id.token"}
	localSession := &SessionData{UserID: 1}

	withOverride := newOIDCTestConfig()
	withOverride.OIDCLogoutURL = "/outpost.goauthentik.io/sign_out"
	withPostLogout := newOIDCTestConfig()
	withPostLogout.OIDCPostLogoutRedirectURI = "https://librarr.example.com/"

	tests := []struct {
		name     string
		h        *OIDCHandler
		cfg      *config.Config
		session  *SessionData
		viaProxy bool
		want     string // exact URL, or "provider" to check the end_session URL
	}{
		{"local session stays in Librarr", withEndpoint, newOIDCTestConfig(), localSession, false, ""},
		{"local session ignores override", withEndpoint, withOverride, localSession, false, ""},
		{"OIDC session goes to provider", withEndpoint, newOIDCTestConfig(), oidcSession, false, "provider"},
		{"OIDC session uses override", withEndpoint, withOverride, oidcSession, false, "/outpost.goauthentik.io/sign_out"},
		{"provider without end_session", &OIDCHandler{}, newOIDCTestConfig(), oidcSession, false, ""},
		{"provider down", nil, newOIDCTestConfig(), oidcSession, false, ""},
		{"provider down uses override", nil, withOverride, oidcSession, false, "/outpost.goauthentik.io/sign_out"},
		{"proxy session uses override", withEndpoint, withOverride, nil, true, "/outpost.goauthentik.io/sign_out"},
		{"proxy session without override", withEndpoint, newOIDCTestConfig(), nil, true, ""},
		{"OIDC not configured", withEndpoint, &config.Config{OIDCLogoutURL: "/x"}, oidcSession, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.h.providerLogoutURL(tt.cfg, tt.session, tt.viaProxy)
			if tt.want != "provider" {
				if got != tt.want {
					t.Fatalf("got %q, want %q", got, tt.want)
				}
				return
			}
			u, err := url.Parse(got)
			if err != nil || u.Host != "idp.example.com" || u.Path != "/logout" {
				t.Fatalf("got %q, want the provider's end_session_endpoint", got)
			}
			q := u.Query()
			if q.Get("existing") != "1" || q.Get("client_id") != "client" || q.Get("id_token_hint") != "raw.id.token" {
				t.Fatalf("query = %v", q)
			}
			if q.Has("post_logout_redirect_uri") {
				t.Fatal("post_logout_redirect_uri must only be sent when configured")
			}
		})
	}

	t.Run("post-logout redirect when configured", func(t *testing.T) {
		u, _ := url.Parse(withEndpoint.providerLogoutURL(withPostLogout, oidcSession, false))
		if got := u.Query().Get("post_logout_redirect_uri"); got != "https://librarr.example.com/" {
			t.Fatalf("post_logout_redirect_uri = %q", got)
		}
	})
}

func TestHandleLogout_ReturnsProviderLogoutURL(t *testing.T) {
	database := newTestDB(t)
	sessions := NewSessionStore()
	h := &OIDCHandler{endSessionEndpoint: "https://idp.example.com/logout"}
	cfg := newOIDCTestConfig()

	logout := func(token string) (*httptest.ResponseRecorder, map[string]any) {
		req := httptest.NewRequest(http.MethodPost, "/api/logout", nil)
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
		rr := httptest.NewRecorder()
		handleLogout(cfg, h, sessions, database)(rr, req)
		return rr, decodeBody(t, rr)
	}

	ssoToken, _ := sessions.CreateOIDC(2, "alice", "user", "raw.id.token")
	rr, resp := logout(ssoToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if got, _ := resp["logout_url"].(string); got == "" {
		t.Fatalf("expected logout_url for an SSO session, got %v", resp)
	}
	if sessions.Valid(ssoToken) {
		t.Fatal("the Librarr session must be deleted")
	}

	localToken, _ := sessions.Create(1, "root", "admin")
	_, resp = logout(localToken)
	if _, present := resp["logout_url"]; present {
		t.Fatalf("local session must not get logout_url, got %v", resp)
	}
}

func TestHandleLogout_ProxySessionUsesOverride(t *testing.T) {
	setTrustedProxies([]string{"192.0.2.1"})
	t.Cleanup(func() { setTrustedProxies(nil) })
	cfg := newOIDCTestConfig()
	cfg.OIDCLogoutURL = "/outpost.goauthentik.io/sign_out"

	req := httptest.NewRequest(http.MethodPost, "/api/logout", nil)
	req.Header.Set("X-Authentik-Username", "alice")
	rr := httptest.NewRecorder()
	handleLogout(cfg, nil, NewSessionStore(), newTestDB(t))(rr, req)

	if got := decodeBody(t, rr)["logout_url"]; got != "/outpost.goauthentik.io/sign_out" {
		t.Fatalf("logout_url = %v", got)
	}
}
