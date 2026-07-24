package download

import (
	"context"
	"crypto/sha1"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

func newTestQBClient(serverURL string) *QBittorrentClient {
	cfg := &config.Config{
		QBUrl:  serverURL,
		QBUser: "admin",
		QBPass: "adminadmin",
	}
	return NewQBittorrentClient(cfg)
}

// Simulates qBittorrent 4.x: HTTP 200 with body "Ok." and a session cookie.
func TestLogin_QB4x_OkBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "SID", Value: "session4x"})
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ok."))
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	if err := q.Login(); err != nil {
		t.Fatalf("expected qBittorrent 4.x login to succeed, got error: %v", err)
	}
	if !q.authenticated {
		t.Errorf("expected authenticated=true after successful 4.x login")
	}
}

// Simulates qBittorrent 5.x: HTTP 204 No Content with empty body and a QBT_SID_* cookie.
func TestLogin_QB5x_NoContentWithSessionCookie(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID_8080", Value: "session5x"})
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	if err := q.Login(); err != nil {
		t.Fatalf("expected qBittorrent 5.x login to succeed, got error: %v", err)
	}
	if !q.authenticated {
		t.Errorf("expected authenticated=true after successful 5.x login")
	}
	// Session cookie should be retained for subsequent requests.
	found := false
	for _, c := range q.cookies {
		if c.Name == "QBT_SID_8080" && c.Value == "session5x" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected QBT_SID_8080 session cookie to be stored")
	}
}

// Some 5.x deployments may return 200 OK with empty body but the session cookie set.
func TestLogin_QB5x_EmptyBodyWithSessionCookie(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID_8080", Value: "session5x"})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	if err := q.Login(); err != nil {
		t.Fatalf("expected login with empty body + cookie to succeed, got error: %v", err)
	}
	if !q.authenticated {
		t.Errorf("expected authenticated=true")
	}
}

// Wrong credentials in 4.x: HTTP 200 with body "Fails." and no session cookie.
func TestLogin_QB4x_FailsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Fails."))
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	err := q.Login()
	if err == nil {
		t.Fatalf("expected login to fail when body is 'Fails.'")
	}
	if !strings.Contains(err.Error(), "Fails.") {
		t.Errorf("expected error to include 'Fails.', got: %v", err)
	}
	if q.authenticated {
		t.Errorf("expected authenticated=false after failed login")
	}
}

// IP ban response should be detected from body text.
func TestLogin_Banned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("Your IP address has been banned"))
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	err := q.Login()
	if err == nil || !strings.Contains(err.Error(), "banned") {
		t.Fatalf("expected ban error, got: %v", err)
	}
	if q.authenticated {
		t.Errorf("expected authenticated=false when banned")
	}
}

// Empty body without a session cookie must fail (not be mistaken for 5.x success).
func TestLogin_EmptyBodyNoCookieFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	err := q.Login()
	if err == nil {
		t.Fatalf("expected login failure on 403 + empty body + no cookie")
	}
	if !strings.Contains(err.Error(), "HTTP 403") {
		t.Errorf("expected error to mention HTTP 403, got: %v", err)
	}
	if q.authenticated {
		t.Errorf("expected authenticated=false")
	}
}

func TestMapTorrentStatus(t *testing.T) {
	tests := []struct {
		state    string
		expected string
	}{
		{"downloading", "downloading"},
		{"stalledDL", "downloading"},
		{"metaDL", "downloading"},
		{"forcedDL", "downloading"},
		{"pausedDL", "paused"},
		{"queuedDL", "queued"},
		{"uploading", "completed"},
		{"stalledUP", "completed"},
		{"pausedUP", "completed"},
		{"queuedUP", "completed"},
		{"stoppedUP", "completed"},
		{"checkingDL", "checking"},
		{"checkingUP", "checking"},
		{"error", "error"},
		{"missingFiles", "missingFiles"},
		{"unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			result := MapTorrentStatus(tt.state)
			if result != tt.expected {
				t.Errorf("MapTorrentStatus(%q) = %q, want %q", tt.state, result, tt.expected)
			}
		})
	}
}

func TestMapSABStatus(t *testing.T) {
	tests := []struct {
		status   string
		expected string
	}{
		{"Downloading", "downloading"},
		{"Paused", "paused"},
		{"Queued", "queued"},
		{"Completed", "completed"},
		{"downloading", "downloading"},
		{"SomeOtherStatus", "SomeOtherStatus"},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			result := mapSABStatus(tt.status)
			if result != tt.expected {
				t.Errorf("mapSABStatus(%q) = %q, want %q", tt.status, result, tt.expected)
			}
		})
	}
}

func TestValidTransitions(t *testing.T) {
	tests := []struct {
		from    string
		to      string
		allowed bool
	}{
		{"queued", "searching", true},
		{"queued", "downloading", true},
		{"queued", "error", true},
		{"queued", "completed", false},
		{"searching", "downloading", true},
		{"searching", "queued", true},
		{"downloading", "importing", true},
		{"downloading", "completed", true},
		{"downloading", "error", true},
		{"downloading", "retry_wait", true},
		{"downloading", "queued", false},
		{"importing", "completed", true},
		{"importing", "error", true},
		{"importing", "queued", false},
		{"retry_wait", "downloading", true},
		{"retry_wait", "searching", true},
		{"error", "queued", true},
		{"error", "dead_letter", true},
		{"error", "downloading", false},
		{"dead_letter", "queued", true},
		{"dead_letter", "downloading", false},
		{"completed", "queued", false},
	}

	for _, tt := range tests {
		t.Run(tt.from+"->"+tt.to, func(t *testing.T) {
			allowed, ok := validTransitions[tt.from]
			if !ok {
				t.Fatalf("no transitions defined for state %q", tt.from)
			}
			result := allowed[tt.to]
			if result != tt.allowed {
				t.Errorf("transition %s -> %s: got %v, want %v", tt.from, tt.to, result, tt.allowed)
			}
		})
	}
}

func TestQBittorrentAddTorrentAcceptsJSONSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		if _, err := r.Cookie("QBT_SID"); err != nil {
			t.Fatalf("expected auth cookie on add request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"added_torrent_ids":["e2f71d638953c009f17594d6982c6de68b06d985"],"failure_count":0,"pending_count":0,"success_count":1}`))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler("e2f71d638953c009f17594d6982c6de68b06d985", "Test Book"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())

	if err := q.AddTorrent("magnet:?xt=urn:btih:e2f71d638953c009f17594d6982c6de68b06d985", "Test Book", "", "", ""); err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
}

func TestQBittorrentAddTorrentAcceptsOkBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler("abc", "Test Book"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())

	if err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Test Book", "", "", ""); err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
}

func TestQBittorrentAddTorrentRejectsHTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request"))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())

	err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Test Book", "", "", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "add torrent HTTP 400: bad request") {
		t.Fatalf("error = %q, want HTTP 400 response", got)
	}
}

func TestQBittorrentAddTorrentRejectsJSONFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"added_torrent_ids":[],"failure_count":1,"pending_count":0,"success_count":0,"error":"invalid torrent"}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())

	err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Test Book", "", "", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "invalid torrent") {
		t.Fatalf("error = %q, want invalid torrent", got)
	}
}

func TestQBittorrentAddTorrentAcceptsJSONPending(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"added_torrent_ids":[],"failure_count":0,"pending_count":1,"success_count":0}`))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler("abc", "Test Book"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())

	if err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Test Book", "", "", ""); err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
}

func TestQBittorrentAddTorrentRejectsJSONPartialFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"added_torrent_ids":["e2f71d638953c009f17594d6982c6de68b06d985"],"failure_count":1,"pending_count":0,"success_count":1}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())

	err := q.AddTorrent("magnet:?xt=urn:btih:e2f71d638953c009f17594d6982c6de68b06d985", "Test Book", "", "", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "success_count=1 failure_count=1 pending_count=0") {
		t.Fatalf("error = %q, want partial failure counts", got)
	}
}

func TestQBittorrentHTTPURLFetchedAndUploadedAsMultipart(t *testing.T) {
	hash, err := torrentInfoHash(validTorrentBytes())
	if err != nil {
		t.Fatalf("torrentInfoHash: %v", err)
	}
	var sawDownload bool
	var sawMultipart bool

	mux := http.NewServeMux()
	mux.HandleFunc("/download/book.torrent", func(w http.ResponseWriter, r *http.Request) {
		sawDownload = true
		if got := r.Header.Get("X-Api-Key"); got != "prowlarr-key" {
			t.Fatalf("X-Api-Key = %q, want prowlarr-key", got)
		}
		w.Header().Set("Content-Type", "application/x-bittorrent")
		w.Header().Set("Content-Disposition", `attachment; filename="Useful Name.torrent"`)
		_, _ = w.Write(validTorrentBytes())
	})
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		sawMultipart = true
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			t.Fatalf("Content-Type = %q, want multipart/form-data", r.Header.Get("Content-Type"))
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("ParseMultipartForm: %v", err)
		}
		files := r.MultipartForm.File["torrents"]
		if len(files) != 1 {
			t.Fatalf("expected one torrents file, got %d", len(files))
		}
		if files[0].Filename != "Useful Name.torrent" {
			t.Fatalf("filename = %q", files[0].Filename)
		}
		f, err := files[0].Open()
		if err != nil {
			t.Fatalf("open uploaded file: %v", err)
		}
		defer f.Close()
		body, _ := io.ReadAll(f)
		if string(body) != string(validTorrentBytes()) {
			t.Fatalf("uploaded body mismatch")
		}
		if got := r.MultipartForm.Value["savepath"]; len(got) != 1 || got[0] != "/downloads" {
			t.Fatalf("savepath = %#v", got)
		}
		if got := r.MultipartForm.Value["category"]; len(got) != 1 || got[0] != "librarr" {
			t.Fatalf("category = %#v", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"added_torrent_ids":["` + hash + `"],"success_count":1}`))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler(hash, "Actual qB Name"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	q.cfg.ProwlarrURL = srv.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	if err := q.AddTorrent(srv.URL+"/download/book.torrent", "Search Result Name", "", "", hash); err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
	if !sawDownload {
		t.Fatal("expected Librarr to fetch torrent URL")
	}
	if !sawMultipart {
		t.Fatal("expected multipart upload to qBittorrent")
	}
}

func TestFetchTorrentAllowsConfiguredProwlarrOrigin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Api-Key"); got != "prowlarr-key" {
			t.Fatalf("X-Api-Key = %q, want prowlarr-key", got)
		}
		w.Header().Set("Content-Type", "application/x-bittorrent")
		_, _ = w.Write(validTorrentBytes())
	}))
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	q.cfg.ProwlarrURL = srv.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	if _, err := q.fetchTorrent(srv.URL + "/download.torrent"); err != nil {
		t.Fatalf("fetchTorrent returned error: %v", err)
	}
}

func TestFetchTorrentRejectsDifferentOriginInputs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("rejected URL should not be fetched")
	}))
	defer srv.Close()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{"different hostname", "http://other.example:9696/download.torrent", "host or port"},
		{"different port", differentPortURL(t, srv.URL) + "/download.torrent", "host or port"},
		{"credentials", strings.Replace(srv.URL, "http://", "http://user:pass@", 1) + "/download.torrent", "credentials"},
		{"unsupported scheme", "ftp://example.com/download.torrent", "http or https"},
		{"missing host", "http:///download.torrent", "host"},
	}

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	q.cfg.ProwlarrURL = srv.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := q.fetchTorrent(tt.url)
			if err == nil {
				t.Fatal("expected rejection")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestFetchTorrentRejectsCrossOriginRedirects(t *testing.T) {
	var leakedKey bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "" {
			leakedKey = true
		}
		_, _ = w.Write(validTorrentBytes())
	}))
	defer target.Close()

	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/download.torrent", http.StatusFound)
	}))
	defer prowlarr.Close()

	q := newAddTorrentTestQBClient(prowlarr.URL, prowlarr.Client())
	q.cfg.ProwlarrURL = prowlarr.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	_, err := q.fetchTorrent(prowlarr.URL + "/start")
	if err == nil {
		t.Fatal("expected redirect rejection")
	}
	if !strings.Contains(err.Error(), "redirect rejected") {
		t.Fatalf("error = %q, want redirect rejected", err.Error())
	}
	if leakedKey {
		t.Fatal("Prowlarr API key leaked to rejected redirect target")
	}
}

func TestFetchTorrentRejectsRedirectToDifferentHostname(t *testing.T) {
	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://other.example:9696/download.torrent", http.StatusFound)
	}))
	defer prowlarr.Close()

	q := newAddTorrentTestQBClient(prowlarr.URL, prowlarr.Client())
	q.cfg.ProwlarrURL = prowlarr.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	_, err := q.fetchTorrent(prowlarr.URL + "/start")
	if err == nil {
		t.Fatal("expected redirect rejection")
	}
	if !strings.Contains(err.Error(), "host or port") {
		t.Fatalf("error = %q, want host or port", err.Error())
	}
}

func TestFetchTorrentRejectsRedirectToDifferentPort(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("rejected redirect should not be fetched")
	}))
	defer target.Close()

	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/download.torrent", http.StatusFound)
	}))
	defer prowlarr.Close()

	q := newAddTorrentTestQBClient(prowlarr.URL, prowlarr.Client())
	q.cfg.ProwlarrURL = prowlarr.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	_, err := q.fetchTorrent(prowlarr.URL + "/start")
	if err == nil {
		t.Fatal("expected redirect rejection")
	}
	if !strings.Contains(err.Error(), "host or port") {
		t.Fatalf("error = %q, want host or port", err.Error())
	}
}

func TestFetchTorrentAllowsSameOriginRedirect(t *testing.T) {
	var downloadSawKey bool
	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/start":
			if got := r.Header.Get("X-Api-Key"); got != "prowlarr-key" {
				t.Fatalf("start X-Api-Key = %q, want prowlarr-key", got)
			}
			http.Redirect(w, r, "/download.torrent", http.StatusFound)
		case "/download.torrent":
			if got := r.Header.Get("X-Api-Key"); got != "prowlarr-key" {
				t.Fatalf("download X-Api-Key = %q, want prowlarr-key", got)
			}
			downloadSawKey = true
			w.Header().Set("Content-Type", "application/x-bittorrent")
			_, _ = w.Write(validTorrentBytes())
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer prowlarr.Close()

	q := newAddTorrentTestQBClient(prowlarr.URL, prowlarr.Client())
	q.cfg.ProwlarrURL = prowlarr.URL
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	if _, err := q.fetchTorrent(prowlarr.URL + "/start"); err != nil {
		t.Fatalf("fetchTorrent returned error: %v", err)
	}
	if !downloadSawKey {
		t.Fatal("same-origin redirect target was not fetched")
	}
}

func TestQBittorrentMagnetStillUsesURLFormField(t *testing.T) {
	const hash = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Fatalf("Content-Type = %q, want form urlencoded", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("urls"); got != "magnet:?xt=urn:btih:"+hash {
			t.Fatalf("urls = %q", got)
		}
		if r.MultipartForm != nil {
			t.Fatal("magnet should not be uploaded as multipart file")
		}
		_, _ = w.Write([]byte(`{"added_torrent_ids":["` + hash + `"],"success_count":1}`))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler(hash, "Magnet Book"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	if err := q.AddTorrent("magnet:?xt=urn:btih:"+hash, "Magnet Book", "", "", ""); err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
}

func TestTorrentInfoHashUsesRawInfoDictionaryBytes(t *testing.T) {
	body := []byte("d4:infod4:zkey1:a4:akey1:bee")
	got, err := torrentInfoHash(body)
	if err != nil {
		t.Fatalf("torrentInfoHash returned error: %v", err)
	}
	rawInfo := []byte("d4:zkey1:a4:akey1:be")
	wantBytes := sha1.Sum(rawInfo)
	want := hex.EncodeToString(wantBytes[:])
	if got != want {
		t.Fatalf("torrentInfoHash = %q, want raw-info hash %q", got, want)
	}
}

func TestTorrentVerificationMatchesHashAcrossCategoryAndName(t *testing.T) {
	const hash = "0123456789abcdef0123456789abcdef01234567"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123"})
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"Actual qB Name","hash":"` + hash + `","category":"other","save_path":"/remote/downloads"}]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	if err := q.AddTorrent("magnet:?xt=urn:btih:"+hash, "Search Result Name", "", "librarr", ""); err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
}

func TestTorrentVerificationIgnoresUnrelatedTorrents(t *testing.T) {
	if torrentListContains([]TorrentInfo{{Name: "Other", Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", nil, "Target", "librarr") {
		t.Fatal("unrelated torrent was accepted")
	}
}

func TestMagnetInfoHashNormalization(t *testing.T) {
	hexHash := "0123456789abcdef0123456789abcdef01234567"
	base32Hash := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte{0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255, 16, 32, 48, 64})
	wantBase32 := "00112233445566778899aabbccddeeff10203040"
	if got := infoHashFromMagnet("magnet:?xt=urn:btih:" + strings.ToUpper(hexHash)); got != hexHash {
		t.Fatalf("hex magnet hash = %q, want %q", got, hexHash)
	}
	if got := infoHashFromMagnet("magnet:?xt=urn:btih:" + base32Hash); got != wantBase32 {
		t.Fatalf("base32 magnet hash = %q, want %q", got, wantBase32)
	}
}

func TestTorrentVerificationContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123"})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/torrents/info":
			_, _ = w.Write([]byte(`[]`))
		default:
			_, _ = w.Write([]byte("Ok."))
		}
	}))
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := q.verifyTorrentAddedContext(ctx, "abc", nil, "Target", "librarr")
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("verification cancellation error=%v duration=%s", err, time.Since(start))
	}
}

func TestFetchTorrentRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		contentType string
		body        string
		want        string
	}{
		{"html", http.StatusOK, "text/html", "<html>login</html>", "HTML"},
		{"json", http.StatusOK, "application/json", `{"error":"denied"}`, "JSON"},
		{"empty", http.StatusOK, "application/x-bittorrent", "", "empty"},
		{"non-2xx", http.StatusForbidden, "text/plain", "forbidden", "HTTP 403"},
		{"malformed", http.StatusOK, "application/x-bittorrent", "not bencode", "bencode"},
		{"missing info dictionary", http.StatusOK, "application/x-bittorrent", "d4:name4:teste", "info dictionary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tt.contentType)
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			q := newAddTorrentTestQBClient(srv.URL, srv.Client())
			q.cfg.ProwlarrURL = srv.URL
			_, err := q.fetchTorrent(srv.URL + "/download.torrent")
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestQBittorrentVerificationTimeout(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Missing Book", "", "", "")
	if err == nil {
		t.Fatal("expected verification timeout")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %q, want timeout", err.Error())
	}
	var warning *TorrentVerificationWarning
	if !errors.As(err, &warning) {
		t.Fatalf("error type = %T, want TorrentVerificationWarning", err)
	}
}

func TestQBittorrentVerificationWaitsForDelayedVisibility(t *testing.T) {
	infoCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		infoCalls++
		if infoCalls < 4 {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		_, _ = w.Write([]byte(`[{"name":"Delayed Book","hash":"abc"}]`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	q.verifyTimeout = 300 * time.Millisecond
	q.verifyInterval = 20 * time.Millisecond
	if err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Delayed Book", "", "", ""); err != nil {
		t.Fatalf("AddTorrent returned error for delayed visibility: %v", err)
	}
	if infoCalls < 4 {
		t.Fatalf("verification made %d info calls, want at least 4", infoCalls)
	}
}

func TestQBittorrentVerificationAPIError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	err := q.AddTorrent("magnet:?xt=urn:btih:abc", "Broken Info", "", "", "")
	if err == nil {
		t.Fatal("expected verification API error")
	}
	if !strings.Contains(err.Error(), "get torrents HTTP 500") {
		t.Fatalf("error = %q, want HTTP 500", err.Error())
	}
}

func TestQBittorrentPublicTorrentURLPassedThrough(t *testing.T) {
	const hash = "0123456789abcdef0123456789abcdef01234567"
	const publicURL = "https://nyaa.example/download/12345.torrent"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Fatalf("Content-Type = %q, want form urlencoded", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("urls"); got != publicURL {
			t.Fatalf("urls = %q, want the public torrent URL passed through", got)
		}
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler(hash, "Manga Volume"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	q.cfg.ProwlarrURL = "http://prowlarr.internal:9696"
	q.cfg.ProwlarrAPIKey = "prowlarr-key"

	if err := q.AddTorrent(publicURL, "Manga Volume", "", "", hash); err != nil {
		t.Fatalf("AddTorrent returned error for public torrent URL: %v", err)
	}
}

func TestQBittorrentHTTPURLWithoutProwlarrPassedThrough(t *testing.T) {
	const hash = "89abcdef0123456789abcdef0123456789abcdef"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/add", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("urls"); got == "" {
			t.Fatal("urls form field missing, want URL passthrough when Prowlarr is not configured")
		}
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/api/v2/torrents/info", torrentInfoHandler(hash, "Some Book"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	q := newAddTorrentTestQBClient(srv.URL, srv.Client())
	if err := q.AddTorrent("https://tracker.example/file.torrent", "Some Book", "", "", hash); err != nil {
		t.Fatalf("AddTorrent returned error without Prowlarr configured: %v", err)
	}
}

func newAddTorrentTestQBClient(serverURL string, client *http.Client) *QBittorrentClient {
	cfg := &config.Config{
		QBUrl:      serverURL,
		QBUser:     "admin",
		QBPass:     "secret",
		QBSavePath: "/downloads",
		QBCategory: "librarr",
	}
	q := NewQBittorrentClient(cfg)
	q.client = client
	q.verifyTimeout = 40 * time.Millisecond
	q.verifyInterval = 5 * time.Millisecond
	return q
}

func torrentInfoHandler(hash, name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if hash == "" {
			_, _ = w.Write([]byte(`[{"name":"` + name + `","hash":""}]`))
			return
		}
		_, _ = w.Write([]byte(`[{"name":"` + name + `","hash":"` + hash + `"}]`))
	}
}

func differentPortURL(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	u.Host = u.Hostname() + ":" + strconv.Itoa(port+1)
	return u.String()
}

func validTorrentBytes() []byte {
	return []byte("d4:infod4:name4:testee")
}

func TestGetTorrentFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "test"})
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Ok."))
		case "/api/v2/torrents/files":
			hash := r.URL.Query().Get("hash")
			if hash == "testhash" {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`[{"name": "RootFolder/file1.mp3"}, {"name": "RootFolder/file2.mp3"}]`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	q := newTestQBClient(srv.URL)
	files, err := q.GetTorrentFiles("testhash")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got: %d", len(files))
	}
	if files[0].Name != "RootFolder/file1.mp3" {
		t.Errorf("expected RootFolder/file1.mp3, got: %s", files[0].Name)
	}
}
