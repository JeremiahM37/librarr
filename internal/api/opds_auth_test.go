package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

const opdsSecretBody = "the exact real file content"

// newOPDSTestServer builds the real middleware chain and the real OPDS
// routes over a library holding one file, so the probes below reach the
// handler that streams library content rather than a stub.
func newOPDSTestServer(t *testing.T, cfg *config.Config) (http.Handler, *db.DB, *SessionStore) {
	t.Helper()
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	ebookDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	ebookDir = filepath.Join(ebookDir, "ebooks")
	if err := os.MkdirAll(ebookDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	bookPath := filepath.Join(ebookDir, "secret-book.epub")
	if err := os.WriteFile(bookPath, []byte(opdsSecretBody), 0o644); err != nil {
		t.Fatalf("write book: %v", err)
	}
	if _, err := database.AddItem(&models.LibraryItem{
		Title: "Secret Victim Book", FilePath: bookPath, FileFormat: "epub", MediaType: "ebook",
	}); err != nil {
		t.Fatalf("add item: %v", err)
	}

	cfg.EbookDir = ebookDir
	s := &Server{cfg: cfg, db: database, sessions: NewSessionStore(), mux: http.NewServeMux()}
	s.registerFeedRoutes()
	return s.Handler(), database, s.sessions
}

var opdsPaths = []string{
	"/opds", "/opds/", "/opds/books", "/opds/search?q=secret",
	"/opds/download/1", "/opds/opensearch.xml",
}

func opdsGet(h http.Handler, path string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if mutate != nil {
		mutate(req)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func basic(user, pass string) func(*http.Request) {
	return func(r *http.Request) { r.SetBasicAuth(user, pass) }
}

// GHSA-368r-6vrp-m3pw: every OPDS route was reachable with no credentials on
// an instance with auth configured.
func TestOPDS_AnonymousIsRejectedOnEveryRoute(t *testing.T) {
	modes := map[string]*config.Config{
		"single-user": {AuthUsername: "reader", AuthPassword: "hunter2"},
		"api-key":     {APIKey: "k-123"},
	}
	for name, cfg := range modes {
		t.Run(name, func(t *testing.T) {
			h, _, _ := newOPDSTestServer(t, cfg)
			for _, path := range opdsPaths {
				rr := opdsGet(h, path, nil)
				if rr.Code != http.StatusUnauthorized {
					t.Errorf("GET %s anonymous = %d, want 401", path, rr.Code)
				}
				if !strings.HasPrefix(rr.Header().Get("WWW-Authenticate"), "Basic ") {
					t.Errorf("GET %s: missing Basic challenge, e-readers will not prompt", path)
				}
				if body := rr.Body.String(); strings.Contains(body, opdsSecretBody) || strings.Contains(body, "Secret Victim Book") {
					t.Errorf("GET %s leaked library content to an anonymous client", path)
				}
			}
		})
	}

	t.Run("multi-user", func(t *testing.T) {
		h, database, _ := newOPDSTestServer(t, &config.Config{})
		hash, _ := hashPassword("hunter2")
		if _, err := database.CreateUser("reader", hash, "user"); err != nil {
			t.Fatalf("create user: %v", err)
		}
		for _, path := range opdsPaths {
			if rr := opdsGet(h, path, nil); rr.Code != http.StatusUnauthorized {
				t.Errorf("GET %s anonymous = %d, want 401", path, rr.Code)
			}
		}
	})
}

func TestOPDS_BasicAuthSingleUser(t *testing.T) {
	h, _, _ := newOPDSTestServer(t, &config.Config{AuthUsername: "reader", AuthPassword: "hunter2", APIKey: "k-123"})

	rr := opdsGet(h, "/opds/download/1", basic("reader", "hunter2"))
	if rr.Code != http.StatusOK || rr.Body.String() != opdsSecretBody {
		t.Fatalf("valid credentials: status=%d body=%q", rr.Code, rr.Body.String())
	}
	if rr := opdsGet(h, "/opds/books", basic("reader", "hunter2")); rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "Secret Victim Book") {
		t.Fatalf("valid credentials on feed: status=%d", rr.Code)
	}

	for _, c := range [][2]string{{"reader", "wrong"}, {"other", "hunter2"}, {"reader", ""}, {"", ""}} {
		if rr := opdsGet(h, "/opds/download/1", basic(c[0], c[1])); rr.Code != http.StatusUnauthorized {
			t.Errorf("credentials %q/%q = %d, want 401", c[0], c[1], rr.Code)
		}
	}

	// The API key works as the Basic password, and the pre-existing
	// machine-auth forms still work on OPDS paths.
	if rr := opdsGet(h, "/opds/download/1", basic("anything", "k-123")); rr.Code != http.StatusOK {
		t.Errorf("API key as Basic password = %d, want 200", rr.Code)
	}
	if rr := opdsGet(h, "/opds/download/1?apikey=k-123", nil); rr.Code != http.StatusOK {
		t.Errorf("?apikey= = %d, want 200", rr.Code)
	}
	if rr := opdsGet(h, "/opds/download/1", func(r *http.Request) { r.Header.Set("X-Api-Key", "k-123") }); rr.Code != http.StatusOK {
		t.Errorf("X-Api-Key = %d, want 200", rr.Code)
	}
}

func TestOPDS_BasicAuthMultiUser(t *testing.T) {
	h, database, sessions := newOPDSTestServer(t, &config.Config{})
	hash, _ := hashPassword("hunter2")
	id, err := database.CreateUser("reader", hash, "user")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if rr := opdsGet(h, "/opds/download/1", basic("reader", "hunter2")); rr.Code != http.StatusOK {
		t.Fatalf("valid credentials = %d, want 200", rr.Code)
	}
	if rr := opdsGet(h, "/opds/download/1", basic("reader", "wrong")); rr.Code != http.StatusUnauthorized {
		t.Errorf("wrong password = %d, want 401", rr.Code)
	}
	if rr := opdsGet(h, "/opds/download/1", basic("nobody", "hunter2")); rr.Code != http.StatusUnauthorized {
		t.Errorf("unknown user = %d, want 401", rr.Code)
	}

	// A browser session still works on OPDS paths.
	token, _ := sessions.Create(id, "reader", "user")
	if rr := opdsGet(h, "/opds/books", func(r *http.Request) {
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	}); rr.Code != http.StatusOK {
		t.Errorf("session cookie = %d, want 200", rr.Code)
	}

	// Read once first so the next requests exercise the verified-credential
	// cache: a password change must lock the old password out immediately,
	// not after the cache TTL.
	newHash, _ := hashPassword("correct horse")
	if err := database.UpdateUserPassword(id, newHash); err != nil {
		t.Fatalf("update password: %v", err)
	}
	if rr := opdsGet(h, "/opds/download/1", basic("reader", "hunter2")); rr.Code != http.StatusUnauthorized {
		t.Errorf("old password after change = %d, want 401 (stale cache)", rr.Code)
	}
	if rr := opdsGet(h, "/opds/download/1", basic("reader", "correct horse")); rr.Code != http.StatusOK {
		t.Errorf("new password = %d, want 200", rr.Code)
	}

	// Basic cannot carry a second factor; enrolling in TOTP must end
	// password-only access, including for an already-cached credential.
	if err := database.SetTOTPSecret(id, "JBSWY3DPEHPK3PXP"); err != nil {
		t.Fatalf("set totp: %v", err)
	}
	if err := database.EnableTOTP(id); err != nil {
		t.Fatalf("enable totp: %v", err)
	}
	if rr := opdsGet(h, "/opds/download/1", basic("reader", "correct horse")); rr.Code != http.StatusUnauthorized {
		t.Errorf("TOTP account with password only = %d, want 401", rr.Code)
	}
}

func TestOPDS_FailedLoginsAreThrottled(t *testing.T) {
	setTrustedProxies(nil)
	h, _, _ := newOPDSTestServer(t, &config.Config{AuthUsername: "reader", AuthPassword: "hunter2"})

	for i := 0; i < opdsMaxFailures; i++ {
		// A spoofed X-Forwarded-For from an untrusted peer must not buy a
		// fresh failure budget.
		rr := opdsGet(h, "/opds/books", func(r *http.Request) {
			r.SetBasicAuth("reader", fmt.Sprintf("guess-%d", i))
			r.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", i))
		})
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("guess %d = %d, want 401", i, rr.Code)
		}
	}
	if rr := opdsGet(h, "/opds/books", basic("reader", "hunter2")); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("after %d failures even the right password = %d, want 429", opdsMaxFailures, rr.Code)
	}
	// Another address is unaffected.
	if rr := opdsGet(h, "/opds/books", func(r *http.Request) {
		r.RemoteAddr = "198.51.100.7:4444"
		r.SetBasicAuth("reader", "hunter2")
	}); rr.Code != http.StatusOK {
		t.Fatalf("different peer = %d, want 200", rr.Code)
	}
}

func TestOPDSBasicAuth_VerifiedReaderSurvivesThrottleAndWindowExpires(t *testing.T) {
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	hash, _ := hashPassword("hunter2")
	if _, err := database.CreateUser("reader", hash, "user"); err != nil {
		t.Fatalf("create user: %v", err)
	}

	now := time.Unix(1_700_000_000, 0)
	a := newOPDSBasicAuth()
	a.now = func() time.Time { return now }
	cfg := &config.Config{}
	attempt := func(pass string) opdsAuthStatus {
		req := httptest.NewRequest(http.MethodGet, "/opds/books", nil)
		req.SetBasicAuth("reader", pass)
		_, status := a.authenticate(req, cfg, database, true)
		return status
	}

	if got := attempt("hunter2"); got != opdsAuthOK {
		t.Fatalf("valid login = %v, want OK", got)
	}
	for i := 0; i < opdsMaxFailures; i++ {
		attempt("wrong")
	}
	if got := attempt("wrong"); got != opdsAuthThrottled {
		t.Fatalf("after failures = %v, want throttled", got)
	}
	// Same address (think: shared reverse proxy), already-verified reader.
	if got := attempt("hunter2"); got != opdsAuthOK {
		t.Fatalf("verified reader during throttle = %v, want OK", got)
	}

	now = now.Add(opdsFailureWindow + time.Second)
	if got := attempt("wrong"); got != opdsAuthDenied {
		t.Fatalf("after window = %v, want a normal denial", got)
	}
}

// With no auth configured at all the instance is open by design, and OPDS
// must keep working without credentials.
func TestOPDS_OpenInstanceStaysOpen(t *testing.T) {
	h, _, _ := newOPDSTestServer(t, &config.Config{})
	if rr := opdsGet(h, "/opds/download/1", nil); rr.Code != http.StatusOK {
		t.Fatalf("open instance = %d, want 200", rr.Code)
	}
}
