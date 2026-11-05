package api

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

// trustedProxies holds the parsed CIDR ranges of reverse proxies whose
// forwarded headers (X-Forwarded-Proto / Forwarded) may be trusted. Set once
// at server start via setTrustedProxies and read concurrently per request.
var (
	trustedProxiesMu sync.RWMutex
	trustedProxies   []*net.IPNet
)

// setTrustedProxies parses a list of CIDRs or bare IPs (bare IPs become /32 or
// /128) into the trusted-proxy set. Invalid entries are skipped.
func setTrustedProxies(entries []string) {
	var nets []*net.IPNet
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if !strings.Contains(e, "/") {
			if ip := net.ParseIP(e); ip != nil {
				if ip.To4() != nil {
					e += "/32"
				} else {
					e += "/128"
				}
			}
		}
		if _, n, err := net.ParseCIDR(e); err == nil {
			nets = append(nets, n)
		}
	}
	trustedProxiesMu.Lock()
	trustedProxies = nets
	trustedProxiesMu.Unlock()
}

// hasTrustedProxies reports whether any valid trusted-proxy entry is configured.
func hasTrustedProxies() bool {
	trustedProxiesMu.RLock()
	defer trustedProxiesMu.RUnlock()
	return len(trustedProxies) > 0
}

// remoteFromTrustedProxy reports whether the request's immediate peer
// (RemoteAddr) is in the configured trusted-proxy set. With no proxies
// configured this is always false, so spoofed forwarded headers are ignored.
func remoteFromTrustedProxy(r *http.Request) bool {
	trustedProxiesMu.RLock()
	nets := trustedProxies
	trustedProxiesMu.RUnlock()
	if len(nets) == 0 {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

const (
	sessionCookieName = "librarr_session"
)

var authentikIdentityHeaders = []string{
	"X-Authentik-Username",
	"X-Authentik-Email",
	"X-Authentik-Name",
	"X-Authentik-Uid",
	"Remote-User",
	"X-Forwarded-User",
}

func proxyIdentityFromRequest(r *http.Request) string {
	for _, header := range authentikIdentityHeaders {
		if value := strings.TrimSpace(r.Header.Get(header)); value != "" {
			return value
		}
	}
	return ""
}

// trustedProxyIdentity returns the SSO username a trusted reverse proxy sent
// with r, or "" when proxy headers are off, absent, or came from a peer
// outside LIBRARR_TRUSTED_PROXIES (which is logged and ignored).
func trustedProxyIdentity(cfg *config.Config, r *http.Request) string {
	if cfg == nil || !cfg.HasOIDCProxyHeaders() {
		return ""
	}
	username := proxyIdentityFromRequest(r)
	if username != "" && !remoteFromTrustedProxy(r) {
		slog.Warn("ignoring SSO identity header from a peer outside LIBRARR_TRUSTED_PROXIES",
			"remote", sanitizeLogValue(r.RemoteAddr), "username", sanitizeLogValue(username))
		return ""
	}
	return username
}

func isSecureRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	// Forwarded headers are honored only when the immediate peer is a
	// configured reverse proxy; otherwise any client could spoof them to flip
	// the Secure cookie flag on a plaintext connection.
	if !remoteFromTrustedProxy(r) {
		return false
	}
	proto := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0])
	if strings.EqualFold(proto, "https") {
		return true
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Forwarded")), "proto=https")
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, token string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   isSecureRequest(r),
		SameSite: http.SameSiteLaxMode,
	})
}

// Errors returned by resolveOIDCUser when an SSO identity must not sign in.
// Their text is shown to the user on the SSO callback.
var (
	errOIDCNoAdmin      = errors.New("SSO sign-in is unavailable until an admin account exists: create the admin account first")
	errOIDCLinkRequired = errors.New("an account with this username already exists. " +
		"Sign in with its password and link SSO under Settings > Account. " +
		"If the password is unknown, an admin can set one under Users, or run `librarr reset-password <username>` on the server")
	errOIDCLocalAdmin = errors.New("this username belongs to an admin account, which is never linked by username. " +
		"Sign in with its password and link SSO under Settings > Account. " +
		"If the password is unknown, run `librarr reset-password <username>` on the server")
	errOIDCSubjectMismatch      = errors.New("this username is linked to a different SSO identity")
	errOIDCProxyAccountUnlinked = errors.New("this account was created through the reverse proxy and is not linked to an SSO identity. " +
		"Keep signing in through the proxy, or have the operator set OIDC_LINK_BY_USERNAME while you sign in here once")
)

// requireAdminExists refuses to create or convert accounts through SSO until
// an admin exists. The admin is created locally on first run, so whoever
// reaches SSO first can't make themselves admin. This replaces the old
// "first SSO user becomes admin" bootstrap. It runs only on those paths, not
// for every proxied request by an existing account.
func requireAdminExists(database *db.DB) error {
	admins, err := database.CountAdmins()
	if err != nil {
		return fmt.Errorf("count admins: %w", err)
	}
	if admins == 0 {
		return errOIDCNoAdmin
	}
	return nil
}

// resolveOIDCUser returns the Librarr user for an SSO identity, or nil when
// OIDC is not configured.
//
// subject is the provider's "sub" claim from a verified ID token, or "" when
// the identity came from trusted reverse-proxy headers (which carry only a
// username). Every account signs in one way. An existing local account is
// never taken over by username alone: its owner links it explicitly (see
// OIDCHandler.HandleLinkStart), or, with OIDC_LINK_BY_USERNAME, a matching
// non-admin account is converted.
func resolveOIDCUser(cfg *config.Config, database *db.DB, sessions *SessionStore, username, subject string) (*models.User, error) {
	if cfg == nil || !cfg.HasOIDC() {
		return nil, nil
	}

	username = strings.TrimSpace(username)
	if username == "" {
		return nil, fmt.Errorf("empty SSO username")
	}

	// The subject is the stable identity: it survives username changes at
	// the provider.
	if subject != "" {
		if user, err := database.GetUserByOIDCSubject(subject); err == nil {
			return user, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("look up OIDC subject: %w", err)
		}
	}

	user, err := database.GetUserByUsername(username)
	switch {
	case err == nil && user.IsOIDC():
		switch {
		case user.OIDCSubject == "" && subject != "":
			// Created from proxy headers, so only the username is known.
			// Binding a subject on the strength of the username alone is the
			// takeover OIDC_LINK_BY_USERNAME opts into; without it, refuse.
			if !cfg.OIDCLinkByUsername {
				return nil, errOIDCProxyAccountUnlinked
			}
			if err := database.SetOIDCSubject(user.ID, subject); err != nil {
				return nil, fmt.Errorf("link OIDC subject: %w", err)
			}
			user.OIDCSubject = subject
			return user, nil
		case subject == "":
			// Proxy headers carry no subject; trust the proxy's username.
			return user, nil
		case user.OIDCSubject == subject:
			return user, nil
		default:
			return nil, errOIDCSubjectMismatch
		}

	case err == nil && user.Role == "admin":
		return nil, errOIDCLocalAdmin

	case err == nil && !cfg.OIDCLinkByUsername:
		return nil, errOIDCLinkRequired

	case err == nil:
		// OIDC_LINK_BY_USERNAME: convert the local non-admin account. The
		// change is permanent and removes its password and TOTP.
		if err := requireAdminExists(database); err != nil {
			return nil, err
		}
		if err := database.ClaimUserForOIDC(user.ID, subject); err != nil {
			return nil, fmt.Errorf("claim user for OIDC: %w", err)
		}
		if sessions != nil {
			sessions.DeleteForUser(user.ID)
		}
		database.LogActivity(user.Username, "sso_claim", user.Username, "Local account converted to SSO sign-in by username")
		slog.Info("local user claimed by OIDC username", "id", user.ID, "username", user.Username)
		claimed, err := database.GetUser(user.ID)
		if err != nil {
			return nil, fmt.Errorf("load claimed user: %w", err)
		}
		return claimed, nil

	case !errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("look up user: %w", err)
	}

	if err := requireAdminExists(database); err != nil {
		return nil, err
	}
	if !cfg.OIDCAutoCreateUsers {
		return nil, fmt.Errorf("user %q not found and auto-creation is disabled", username)
	}

	role := cfg.OIDCDefaultRole
	id, err := database.CreateOIDCUser(username, subject, role)
	if err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	user, err = database.GetUser(id)
	if err != nil {
		return nil, fmt.Errorf("load created user: %w", err)
	}

	slog.Info("OIDC user created", "id", id, "username", username, "role", role)

	return user, nil
}

// ensureSessionForUser creates a Librarr session cookie for the given user if
// the request does not already carry a matching valid session.
func ensureSessionForUser(w http.ResponseWriter, r *http.Request, sessions *SessionStore, user *models.User) bool {
	if sessions == nil || user == nil {
		return false
	}

	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		if data, ok := sessions.Get(cookie.Value); ok && data.UserID == user.ID && data.Username == user.Username && data.Role == user.Role {
			return false
		}
	}

	token, err := sessions.Create(user.ID, user.Username, user.Role)
	if err != nil {
		slog.Error("failed to create SSO proxy session", "error", err, "username", user.Username)
		return false
	}
	setSessionCookie(w, r, token, 86400)
	return true
}
