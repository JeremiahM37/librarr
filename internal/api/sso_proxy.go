package api

import (
	"crypto/rand"
	"encoding/hex"
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

// resolveOIDCUser returns an existing Librarr user for the supplied SSO
// identity, or auto-creates one when OIDC user provisioning is enabled.
func resolveOIDCUser(cfg *config.Config, database *db.DB, username string) (*models.User, error) {
	if cfg == nil || !cfg.HasOIDC() {
		return nil, nil
	}

	username = strings.TrimSpace(username)
	if username == "" {
		return nil, fmt.Errorf("empty SSO username")
	}

	user, err := database.GetUserByUsername(username)
	if err == nil {
		return user, nil
	}

	if !cfg.OIDCAutoCreateUsers {
		return nil, fmt.Errorf("user %q not found and auto-creation is disabled", username)
	}

	userCount, _ := database.CountUsers()
	role := cfg.OIDCDefaultRole
	if userCount == 0 {
		role = "admin"
	}

	randomPass := make([]byte, 32)
	if _, err := rand.Read(randomPass); err != nil {
		return nil, fmt.Errorf("generate random password seed: %w", err)
	}
	passHash, err := hashPassword(hex.EncodeToString(randomPass))
	if err != nil {
		return nil, fmt.Errorf("hash random password: %w", err)
	}

	id, err := database.CreateUser(username, passHash, role)
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
