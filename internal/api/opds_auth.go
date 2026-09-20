package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
)

// OPDS clients are e-reader apps: they cannot complete the JSON login flow,
// hold a session cookie, or add an X-Api-Key header, and an ?apikey= on the
// catalog URL is lost as soon as the reader follows a relative link in the
// feed. HTTP Basic is what they all support, so OPDS paths accept it in
// addition to the session and API-key auth every other route uses.

const (
	// opdsMaxFailures failed Basic logins per opdsFailureWindow are allowed
	// from one peer before further attempts get 429.
	opdsMaxFailures   = 10
	opdsFailureWindow = time.Minute
	// opdsVerifiedTTL bounds how long a verified credential skips bcrypt.
	// Readers resend Basic on every request (each page, cover and download),
	// and a bcrypt compare per request is slow enough to notice.
	opdsVerifiedTTL = 10 * time.Minute
	opdsMaxTracked  = 4096
)

type opdsAuthStatus int

const (
	opdsAuthDenied opdsAuthStatus = iota
	opdsAuthOK
	opdsAuthThrottled
)

type opdsIdentity struct {
	userID   int64
	username string
	role     string
}

// opdsVerified remembers that a credential checked out against a specific
// stored password hash. It is only honored while the user's hash is still
// that one, so a password change, TOTP enrollment, role change or deletion
// takes effect on the next request, not after the TTL.
type opdsVerified struct {
	userID       int64
	passwordHash string
	expires      time.Time
}

type opdsBasicAuth struct {
	mu       sync.Mutex
	verified map[[sha256.Size]byte]opdsVerified
	failures map[string][]time.Time
	now      func() time.Time
}

func newOPDSBasicAuth() *opdsBasicAuth {
	return &opdsBasicAuth{
		verified: make(map[[sha256.Size]byte]opdsVerified),
		failures: make(map[string][]time.Time),
		now:      time.Now,
	}
}

func isOPDSPath(path string) bool {
	return path == "/opds" || strings.HasPrefix(path, "/opds/")
}

// authenticate checks the request's HTTP Basic credentials. Accepted:
//   - a Librarr user's username and password (multi-user mode), unless the
//     account has TOTP enabled — Basic cannot carry a second factor, and
//     accepting the password alone would make OPDS a way around it;
//   - AUTH_USERNAME / AUTH_PASSWORD (single-user mode);
//   - the API key as the password, with any username. This is the route for
//     TOTP accounts and for SSO-only users, who have no usable password.
func (a *opdsBasicAuth) authenticate(r *http.Request, cfg *config.Config, database *db.DB, multiUser bool) (opdsIdentity, opdsAuthStatus) {
	username, password, ok := r.BasicAuth()
	if !ok || password == "" {
		return opdsIdentity{}, opdsAuthDenied
	}

	credKey := sha256.Sum256([]byte(username + "\x00" + password))
	if ident, ok := a.cached(credKey, username, database); ok {
		return ident, opdsAuthOK
	}

	// Checked after the cache so a reader with good credentials is not locked
	// out by someone else failing from the same address (e.g. a shared proxy).
	peer := opdsPeer(r)
	if a.throttled(peer) {
		return opdsIdentity{}, opdsAuthThrottled
	}

	if cfg.HasAPIKey() && subtle.ConstantTimeCompare([]byte(password), []byte(cfg.APIKey)) == 1 {
		return opdsIdentity{username: "api", role: "admin"}, opdsAuthOK
	}

	if multiUser {
		user, err := database.GetUserByUsername(username)
		if err != nil {
			// Spend the same bcrypt time as a real user so response time
			// does not reveal which usernames exist.
			checkPassword(password, opdsDummyHash())
		} else if checkPassword(password, user.PasswordHash) && !user.TOTPEnabled {
			a.remember(credKey, opdsVerified{userID: user.ID, passwordHash: user.PasswordHash})
			return opdsIdentity{userID: user.ID, username: user.Username, role: user.Role}, opdsAuthOK
		}
	} else if cfg.HasAuth() {
		userOK := subtle.ConstantTimeCompare([]byte(username), []byte(cfg.AuthUsername))
		passOK := subtle.ConstantTimeCompare([]byte(password), []byte(cfg.AuthPassword))
		if userOK&passOK == 1 {
			return opdsIdentity{username: cfg.AuthUsername, role: "admin"}, opdsAuthOK
		}
	}

	a.recordFailure(peer)
	return opdsIdentity{}, opdsAuthDenied
}

func (a *opdsBasicAuth) cached(key [sha256.Size]byte, username string, database *db.DB) (opdsIdentity, bool) {
	a.mu.Lock()
	entry, ok := a.verified[key]
	if ok && a.now().After(entry.expires) {
		delete(a.verified, key)
		ok = false
	}
	a.mu.Unlock()
	if !ok {
		return opdsIdentity{}, false
	}

	user, err := database.GetUser(entry.userID)
	if err != nil || user == nil || user.Username != username || user.TOTPEnabled ||
		subtle.ConstantTimeCompare([]byte(user.PasswordHash), []byte(entry.passwordHash)) != 1 {
		a.mu.Lock()
		delete(a.verified, key)
		a.mu.Unlock()
		return opdsIdentity{}, false
	}
	return opdsIdentity{userID: user.ID, username: user.Username, role: user.Role}, true
}

func (a *opdsBasicAuth) remember(key [sha256.Size]byte, entry opdsVerified) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if len(a.verified) >= opdsMaxTracked {
		for k, v := range a.verified {
			if now.After(v.expires) {
				delete(a.verified, k)
			}
		}
		if len(a.verified) >= opdsMaxTracked {
			return
		}
	}
	entry.expires = now.Add(opdsVerifiedTTL)
	a.verified[key] = entry
}

func (a *opdsBasicAuth) throttled(peer string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	recent := a.recentFailures(peer)
	return len(recent) >= opdsMaxFailures
}

func (a *opdsBasicAuth) recordFailure(peer string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.failures) >= opdsMaxTracked {
		for p := range a.failures {
			a.recentFailures(p)
		}
	}
	a.failures[peer] = append(a.recentFailures(peer), a.now())
}

// recentFailures drops failures older than the window and returns the rest.
// Callers hold a.mu.
func (a *opdsBasicAuth) recentFailures(peer string) []time.Time {
	cutoff := a.now().Add(-opdsFailureWindow)
	times := a.failures[peer]
	i := 0
	for i < len(times) && times[i].Before(cutoff) {
		i++
	}
	times = times[i:]
	if len(times) == 0 {
		delete(a.failures, peer)
		return nil
	}
	a.failures[peer] = times
	return times
}

// opdsPeer is the address failed logins are counted against. X-Forwarded-For
// is honored only from a configured reverse proxy; from anyone else it is
// attacker-controlled and would hand out a fresh failure budget per request.
func opdsPeer(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if remoteFromTrustedProxy(r) {
		parts := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
		if last := strings.TrimSpace(parts[len(parts)-1]); last != "" {
			return last
		}
	}
	return host
}

var (
	opdsDummyHashOnce  sync.Once
	opdsDummyHashValue string
)

func opdsDummyHash() string {
	opdsDummyHashOnce.Do(func() {
		opdsDummyHashValue, _ = hashPassword("librarr-opds-timing-equalizer")
	})
	return opdsDummyHashValue
}
