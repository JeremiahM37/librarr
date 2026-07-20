package download

import (
	"bytes"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/sources"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func exhaustedAnnasClient(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		status := http.StatusOK
		body := ""
		switch {
		case req.URL.Host == "mirror.test" && req.URL.Path == "/ads.php":
			body = `<a href="get.php?md5=original&key=test">GET</a>`
		case req.URL.Host == "mirror.test" && req.URL.Path == "/get.php":
			status = http.StatusGatewayTimeout
		case req.URL.Host == "annas.test" && req.URL.Path == "/search":
			body = `<a href="/md5/36eba0c0be766d6ba02cb234088c30ab">Chasing Molecules: Poisonous Products, Human Health</a>`
		default:
			t.Fatalf("unexpected request: %s", req.URL)
		}
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
}

func exhaustedAnnasConfig(dir string) *config.Config {
	return &config.Config{
		AnnasArchiveDomain:  "annas.test",
		IncomingDir:         filepath.Join(dir, "incoming"),
		EbookDir:            filepath.Join(dir, "ebooks"),
		UserAgent:           "test",
		MaxRetries:          2,
		RetryBackoffSeconds: 0,
		Sources: &sources.Registry{
			LibgenMirrors: []string{"https://mirror.test"},
		},
	}
}

func TestDownloadFromAnnasRejectsUnrelatedFallbackAndExhaustsCandidates(t *testing.T) {
	cfg := exhaustedAnnasConfig(t.TempDir())
	direct := NewDirectDownloader(cfg, exhaustedAnnasClient(t))
	direct.validate = nil

	_, _, _, err := direct.DownloadFromAnnas("original", "Human Transit", nil)
	if err == nil || !strings.Contains(err.Error(), "all matching LibGen candidates exhausted") {
		t.Fatalf("expected exhausted-candidates error, got %v", err)
	}
}

func TestDownloadFromAnnasReturnsSuccessfulFallbackMD5(t *testing.T) {
	const fallbackMD5 = "48d427b054f3199f44171ba55c21adb2"
	pdf := append([]byte("%PDF-1.7\n"), bytes.Repeat([]byte{'x'}, 1500)...)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		status := http.StatusOK
		var body []byte
		header := make(http.Header)
		switch {
		case req.URL.Host == "mirror.test" && req.URL.Path == "/ads.php" && req.URL.Query().Get("md5") == "original":
			body = []byte("File not found in DB")
		case req.URL.Host == "annas.test" && req.URL.Path == "/search":
			body = []byte(`<a href="/md5/48d427b054f3199f44171ba55c21adb2">The Adventures of Sherlock Holmes</a>`)
		case req.URL.Host == "mirror.test" && req.URL.Path == "/ads.php" && req.URL.Query().Get("md5") == fallbackMD5:
			body = []byte(`<a href="get.php?md5=48d427b054f3199f44171ba55c21adb2&amp;key=test">GET</a>`)
		case req.URL.Host == "mirror.test" && req.URL.Path == "/get.php":
			header.Set("Content-Type", "application/pdf")
			body = pdf
		default:
			t.Fatalf("unexpected request: %s", req.URL)
		}
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader(body)), Request: req}, nil
	})}
	cfg := exhaustedAnnasConfig(t.TempDir())
	direct := NewDirectDownloader(cfg, client)
	direct.validate = nil

	_, _, downloadedMD5, err := direct.DownloadFromAnnas("original", "The Adventures of Sherlock Holmes", nil)
	if err != nil {
		t.Fatalf("download fallback: %v", err)
	}
	if downloadedMD5 != fallbackMD5 {
		t.Errorf("downloaded MD5 = %q, want %q", downloadedMD5, fallbackMD5)
	}
}

func TestAnnasTransientExhaustionUsesConfiguredRetries(t *testing.T) {
	dir := t.TempDir()
	cfg := exhaustedAnnasConfig(dir)
	database, err := db.New(filepath.Join(dir, "librarr.db"))
	if err != nil {
		t.Fatalf("create DB: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	direct := NewDirectDownloader(cfg, exhaustedAnnasClient(t))
	direct.validate = nil
	manager := NewManager(
		cfg,
		database,
		nil,
		nil,
		direct,
		organize.NewOrganizer(cfg),
		nil,
		search.NewHealthTracker(3, 300),
	)

	job, err := manager.StartAnnasDownload("original", "Human Transit")
	if err != nil {
		t.Fatalf("start download: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		status := job.Status
		manager.mu.Unlock()
		if status == "dead_letter" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if job.Status != "dead_letter" {
		t.Fatalf("status = %q, want dead_letter", job.Status)
	}
	if job.RetryCount != job.MaxRetries {
		t.Errorf("retry count = %d, want %d", job.RetryCount, job.MaxRetries)
	}
	if job.Detail != "Max retries exceeded" {
		t.Errorf("detail = %q", job.Detail)
	}
}

func TestParseAnnasFastDownloadURLAbsoluteAndRelative(t *testing.T) {
	api := "https://annas.test/dyn/api/fast_download.json?md5=abc&key=k"
	got, err := parseAnnasFastDownloadURL(api, []byte(`{"download_url":"https://cdn.test/file.epub"}`))
	if err != nil || got != "https://cdn.test/file.epub" {
		t.Fatalf("absolute: got %q err %v", got, err)
	}
	got, err = parseAnnasFastDownloadURL(api, []byte(`{"download_url":"/get.php?md5=abc"}`))
	if err != nil || got != "https://annas.test/get.php?md5=abc" {
		t.Fatalf("relative: got %q err %v", got, err)
	}
	if _, err := parseAnnasFastDownloadURL(api, []byte(`{"error":"not a member"}`)); err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("expected membership error, got %v", err)
	}
}

func TestDownloadFromAnnasUsesFastDownloadWhenKeyConfigured(t *testing.T) {
	const md5 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	epub := append([]byte("PK"), bytes.Repeat([]byte{'z'}, 2000)...)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		switch {
		case req.URL.Host == "annas.test" && req.URL.Path == "/dyn/api/fast_download.json":
			if req.URL.Query().Get("key") != "vip-key" || req.URL.Query().Get("md5") != md5 {
				t.Fatalf("unexpected fast_download query: %s", req.URL.RawQuery)
			}
			body := []byte(`{"download_url":"https://cdn.test/book.epub"}`)
			return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(bytes.NewReader(body)), Request: req}, nil
		case req.URL.Host == "cdn.test" && req.URL.Path == "/book.epub":
			header.Set("Content-Type", "application/epub+zip")
			return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(bytes.NewReader(epub)), Request: req}, nil
		case req.URL.Host == "mirror.test":
			t.Fatalf("should not fall back to LibGen when fast download succeeds: %s", req.URL)
		default:
			t.Fatalf("unexpected request: %s", req.URL)
		}
		return nil, nil
	})}
	cfg := exhaustedAnnasConfig(t.TempDir())
	cfg.AnnasArchiveSecretKey = "vip-key"
	direct := NewDirectDownloader(cfg, client)
	direct.validate = nil

	path, size, gotMD5, err := direct.DownloadFromAnnas(md5, "VIP Book", nil)
	if err != nil {
		t.Fatalf("fast download: %v", err)
	}
	if gotMD5 != md5 {
		t.Errorf("md5 = %q, want %q", gotMD5, md5)
	}
	if size < 1000 {
		t.Errorf("size = %d, want larger file", size)
	}
	if path == "" {
		t.Fatal("expected local path")
	}
}

func TestDownloadFromAnnasFallsBackToLibgenWhenFastFails(t *testing.T) {
	const md5 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	epub := append([]byte("PK"), bytes.Repeat([]byte{'y'}, 2000)...)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		switch {
		case req.URL.Host == "annas.test" && req.URL.Path == "/dyn/api/fast_download.json":
			body := []byte(`{"error":"quota exceeded"}`)
			return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(bytes.NewReader(body)), Request: req}, nil
		case req.URL.Host == "mirror.test" && req.URL.Path == "/ads.php":
			body := `<a href="get.php?md5=` + md5 + `&amp;key=test">GET</a>`
			return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
		case req.URL.Host == "mirror.test" && req.URL.Path == "/get.php":
			header.Set("Content-Type", "application/epub+zip")
			return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(bytes.NewReader(epub)), Request: req}, nil
		default:
			t.Fatalf("unexpected request: %s", req.URL)
		}
		return nil, nil
	})}
	cfg := exhaustedAnnasConfig(t.TempDir())
	cfg.AnnasArchiveSecretKey = "vip-key"
	direct := NewDirectDownloader(cfg, client)
	direct.validate = nil

	_, _, gotMD5, err := direct.DownloadFromAnnas(md5, "Fallback Book", nil)
	if err != nil {
		t.Fatalf("libgen fallback: %v", err)
	}
	if gotMD5 != md5 {
		t.Errorf("md5 = %q, want %q", gotMD5, md5)
	}
}
