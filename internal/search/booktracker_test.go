package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
)

// booktrackerSearchPage is a minimal phpBB-style search results page with one
// topic row. It mirrors the selectors the parser looks for: an <a.topictitle>
// whose href carries the topic ID, plus a seed cell tagged with .seedmed.
const booktrackerSearchPage = `<html><body>
<table>
 <tr>
   <td><a class="topictitle" href="viewtopic.php?t=98765">Толстой - Анна Каренина [epub, 2.3 МБ]</a></td>
   <td class="seedmed">12</td>
   <td>4</td>
 </tr>
 <tr>
   <td>header row (no topictitle, must be skipped)</td>
   <td class="seedmed">999</td>
 </tr>
 <tr>
   <td><a class="topictitle" href="viewtopic.php?t=98765">Duplicate topic</a></td>
   <td class="seedmed">7</td>
 </tr>
</table>
</body></html>`

// Helper: BookTracker server that correctly sets an authenticated cookie
// (phpbb_data-like) on login, and serves results on search.
func newBookTrackerTestServer(t *testing.T, authCookie bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/login.php", func(w http.ResponseWriter, r *http.Request) {
		if authCookie {
			// Mimic a phpBB persistent-login cookie (value is a serialized blob).
			http.SetCookie(w, &http.Cookie{Name: "phpbb2mysql_data", Value: "a%3A2%3A%7Bi%3A0%3Bs%3A1%3A%221%22%3B%7D", Path: "/"})
		} else {
			// Only an anonymous session cookie, not a login-bearing one.
			http.SetCookie(w, &http.Cookie{Name: "phpbb2mysql_sid", Value: "anonsessid", Path: "/"})
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/search.php", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(booktrackerSearchPage))
	})
	return httptest.NewServer(mux)
}

func TestBookTrackerSearch(t *testing.T) {
	srv := newBookTrackerTestServer(t, true)
	defer srv.Close()

	cfg := &config.Config{
		BookTrackerURL:     srv.URL,
		BookTrackerUser:    "u",
		BookTrackerPass:    "p",
		BookTrackerEnabled: true,
		UserAgent:          "test",
	}
	s := NewBookTracker(cfg, &http.Client{Timeout: 5 * time.Second}, "main")

	if !s.Enabled() {
		t.Fatal("BookTracker should be enabled with all creds set")
	}
	if s.Name() != "booktracker" {
		t.Errorf("Name() = %q", s.Name())
	}

	results, err := s.Search(context.Background(), "анна")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1 (duplicate topic ID must be dropped)", len(results))
	}
	r := results[0]
	if !strings.HasPrefix(r.Title, "Толстой") {
		t.Errorf("Title = %q", r.Title)
	}
	if r.Author != "Толстой" {
		t.Errorf("Author = %q, want Толстой (parsed from 'Author - Title' format)", r.Author)
	}
	if r.Format != "epub" {
		t.Errorf("Format = %q, want epub", r.Format)
	}
	if r.Seeders != 12 {
		t.Errorf("Seeders = %d, want 12 (from .seedmed cell)", r.Seeders)
	}
	if !strings.Contains(r.DownloadURL, "download.php?t=98765") {
		t.Errorf("DownloadURL = %q", r.DownloadURL)
	}
	if !strings.Contains(r.URL, "viewtopic.php?t=98765") {
		t.Errorf("URL = %q", r.URL)
	}
}

// TestBookTrackerLoginRejectsAnonymousSession verifies the login check does
// not treat a plain anonymous phpBB session cookie as success. This used to
// accept "any cookie" which silently swallowed bad-credentials logins.
func TestBookTrackerLoginRejectsAnonymousSession(t *testing.T) {
	srv := newBookTrackerTestServer(t, false)
	defer srv.Close()

	cfg := &config.Config{
		BookTrackerURL:     srv.URL,
		BookTrackerUser:    "u",
		BookTrackerPass:    "wrong",
		BookTrackerEnabled: true,
		UserAgent:          "test",
	}
	s := NewBookTracker(cfg, &http.Client{Timeout: 5 * time.Second}, "main")

	_, err := s.Search(context.Background(), "x")
	if err == nil {
		t.Fatal("expected login failure when only an anonymous session cookie is present")
	}
	if !strings.Contains(err.Error(), "login") {
		t.Errorf("err = %v, want something referencing login", err)
	}
}

func TestBookTrackerDisabled(t *testing.T) {
	cfg := &config.Config{BookTrackerEnabled: true} // missing url/user/pass
	s := NewBookTracker(cfg, &http.Client{}, "main")
	if s.Enabled() {
		t.Errorf("Enabled() true without credentials; want false")
	}
}

func TestBookTrackerAudiobookTab(t *testing.T) {
	srv := newBookTrackerTestServer(t, true)
	defer srv.Close()

	cfg := &config.Config{
		BookTrackerURL: srv.URL, BookTrackerUser: "u", BookTrackerPass: "p",
		BookTrackerEnabled: true, UserAgent: "test",
	}
	s := NewBookTracker(cfg, &http.Client{Timeout: 5 * time.Second}, "audiobook")
	if s.Name() != "booktracker_audiobook" {
		t.Errorf("Name() = %q", s.Name())
	}
	results, err := s.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	for _, r := range results {
		if r.MediaType != "audiobook" {
			t.Errorf("result MediaType = %q, want audiobook", r.MediaType)
		}
	}
}
