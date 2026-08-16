package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func cspHeader(t *testing.T, cfg *config.Config) string {
	t.Helper()
	s := &Server{cfg: cfg}
	rr := httptest.NewRecorder()
	s.securityHeadersMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	return rr.Header().Get("Content-Security-Policy")
}

func imgSrc(t *testing.T, csp string) string {
	t.Helper()
	for _, d := range strings.Split(csp, ";") {
		if d = strings.TrimSpace(d); strings.HasPrefix(d, "img-src ") {
			return d
		}
	}
	t.Fatalf("no img-src directive in %q", csp)
	return ""
}

// TestCSPAllowsConfiguredHTTPMediaOrigins covers issue #103: a self-hosted
// Audiobookshelf on the LAN is plain http, so its cover images were blocked by
// "img-src 'self' data: https:".
func TestCSPAllowsConfiguredHTTPMediaOrigins(t *testing.T) {
	got := imgSrc(t, cspHeader(t, &config.Config{
		ABSURL:     "http://192.168.0.225:13378",
		KavitaURL:  "http://192.168.0.225:5005/",
		CalibreURL: "http://calibre.lan:8083/#/library",
	}))

	for _, want := range []string{
		"'self'", "data:", "https:",
		"http://192.168.0.225:13378",
		"http://192.168.0.225:5005",
		"http://calibre.lan:8083",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("img-src missing %q\ngot: %s", want, got)
		}
	}
	// Paths and fragments are not valid CSP sources.
	if strings.Contains(got, "#") || strings.Contains(got, "/library") {
		t.Errorf("img-src leaked a path or fragment into a source: %s", got)
	}
}

// TestCSPStaysStrictWithoutIntegrations pins the default: no configuration must
// not widen the policy.
func TestCSPStaysStrictWithoutIntegrations(t *testing.T) {
	if got, want := imgSrc(t, cspHeader(t, &config.Config{})), "img-src 'self' data: https:"; got != want {
		t.Errorf("default img-src = %q, want %q", got, want)
	}
}

// TestCSPDoesNotAllowHTTPWholesale is the point of deriving the list from
// configuration: an unconfigured http origin must still be blocked.
func TestCSPDoesNotAllowHTTPWholesale(t *testing.T) {
	got := imgSrc(t, cspHeader(t, &config.Config{ABSURL: "http://192.168.0.225:13378"}))
	for _, src := range strings.Fields(strings.TrimPrefix(got, "img-src ")) {
		if src == "http:" {
			t.Errorf("img-src allows all http origins, not just configured ones: %s", got)
		}
	}
	if strings.Contains(got, "http://evil.example") {
		t.Errorf("img-src contains an origin that was never configured: %s", got)
	}
}

// TestCSPRejectsMalformedIntegrationURLs — configured values are operator input,
// but a stray space or quote must not be able to inject a directive.
func TestCSPRejectsMalformedIntegrationURLs(t *testing.T) {
	csp := cspHeader(t, &config.Config{
		ABSURL:    "http://good.lan:13378",
		KavitaURL: "http://bad.lan:5005; script-src 'unsafe-inline'",
		KomgaURL:  "not a url",
	})
	if strings.Contains(csp, "unsafe-inline'; script-src") || strings.Count(csp, "script-src") != 1 {
		t.Errorf("malformed integration URL altered the policy: %s", csp)
	}
	if !strings.Contains(imgSrc(t, csp), "http://good.lan:13378") {
		t.Errorf("valid origin was dropped alongside the malformed ones: %s", csp)
	}
}

// TestCSPSkipsRedundantHTTPSOrigins keeps the header short: https origins are
// already covered by the blanket https: source.
func TestCSPSkipsRedundantHTTPSOrigins(t *testing.T) {
	got := imgSrc(t, cspHeader(t, &config.Config{ABSURL: "https://abs.example.com"}))
	if strings.Contains(got, "abs.example.com") {
		t.Errorf("https origin needlessly duplicated into img-src: %s", got)
	}
}
