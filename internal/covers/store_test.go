package covers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

var (
	jpegBytes = append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, bytes.Repeat([]byte{0}, 32)...)
	pngBytes  = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)
	gifBytes  = append([]byte("GIF89a"), bytes.Repeat([]byte{0}, 32)...)
	webpBytes = append(append([]byte("RIFF\x24\x00\x00\x00WEBP"), []byte("VP8 ")...), bytes.Repeat([]byte{0}, 32)...)
)

func TestDetectImage(t *testing.T) {
	good := []struct {
		name string
		b    []byte
		ext  string
		ct   string
	}{
		{"jpeg", jpegBytes, ".jpg", "image/jpeg"},
		{"png", pngBytes, ".png", "image/png"},
		{"gif", gifBytes, ".gif", "image/gif"},
		{"webp", webpBytes, ".webp", "image/webp"},
	}
	for _, tc := range good {
		ext, ct, ok := DetectImage(tc.b)
		if !ok || ext != tc.ext || ct != tc.ct {
			t.Errorf("%s: got %q %q %v", tc.name, ext, ct, ok)
		}
	}
	bad := map[string][]byte{
		"svg":   []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`),
		"html":  []byte("<html><body>hi</body></html>"),
		"empty": nil,
		"text":  []byte("this is cover.jpg, honest"),
	}
	for name, b := range bad {
		if _, _, ok := DetectImage(b); ok {
			t.Errorf("%s: should be rejected", name)
		}
	}
}

func TestSaveAndOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "covers", "nested")
	s := NewStore(dir, nil, nil)
	file, err := s.Save(7, jpegBytes)
	if err != nil || file != "7.jpg" {
		t.Fatalf("Save: %q %v", file, err)
	}
	rc, ct, err := s.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, jpegBytes) || ct != "image/jpeg" {
		t.Fatalf("Open: ct=%q equal=%v", ct, bytes.Equal(got, jpegBytes))
	}
	if ok, err := s.Exists("7.jpg"); !ok || err != nil {
		t.Fatalf("Exists: %v %v", ok, err)
	}
	if _, err := s.Save(7, pngBytes); err != nil {
		t.Fatal(err)
	}
	list, err := s.List()
	if err != nil || !reflect.DeepEqual(list, []string{"7.png"}) {
		t.Fatalf("List: %v %v", list, err)
	}
	if err := s.Remove("7.png"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Exists("7.png"); ok || err != nil {
		t.Fatalf("still exists: %v %v", ok, err)
	}
}

func TestSaveRejectsNonImage(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, nil, nil)
	if _, err := s.Save(1, []byte("<html></html>")); err == nil {
		t.Fatal("expected error")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("leftover files: %v", entries)
	}
}

func TestOpenIgnoresPathsInFile(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "covers")
	s := NewStore(dir, nil, nil)
	if err := os.WriteFile(filepath.Join(root, "secret.jpg"), jpegBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"../secret.jpg", "../../etc/passwd", "/etc/passwd"} {
		if ok, err := s.Exists(f); ok || err != nil {
			t.Errorf("Exists(%q) = %v, %v", f, ok, err)
		}
		if rc, _, err := s.Open(f); err == nil {
			rc.Close()
			t.Errorf("Open(%q) succeeded", f)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "secret.jpg"), pngBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	rc, ct, err := s.Open("../secret.jpg")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if ct != "image/png" {
		t.Fatalf("expected the file inside dir, got %q", ct)
	}
}

func TestExistsReportsStatFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "covers")
	s := NewStore(dir, nil, nil)
	if ok, err := s.Exists("7.jpg"); ok || err != nil {
		t.Fatalf("missing directory: %v %v", ok, err)
	}
	if _, err := s.Save(7, jpegBytes); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Exists("8.jpg"); ok || err != nil {
		t.Fatalf("missing file: %v %v", ok, err)
	}
	lockDir(t, dir, 0)
	if _, err := s.Exists("7.jpg"); err == nil {
		t.Fatal("an unreadable directory must be an error, not a missing file")
	}
}

func TestAllowedCoverHost(t *testing.T) {
	base := "https://covers.openlibrary.org"
	cases := map[string]bool{
		"https://covers.openlibrary.org/b/id/1-M.jpg": true,
		"https://archive.org/x":                       true,
		"https://ia800703.us.archive.org/x":           true,
		"http://archive.org/x":                        false,
		"https://archive.org.evil.com/x":              false,
		"https://evilarchive.org/x":                   false,
		"https://example.com/x":                       false,
	}
	for u, want := range cases {
		if got := allowedCoverHost(u, base); got != want {
			t.Errorf("%s: got %v want %v", u, got, want)
		}
	}
}

func newDownloadStore(t *testing.T, srv *httptest.Server) *Store {
	t.Helper()
	t.Setenv("LIBRARR_INSECURE_ALLOW_PRIVATE_URLS", "1")
	return NewStore(t.TempDir(), srv.Client(), func() string { return srv.URL })
}

func TestDownload(t *testing.T) {
	var other int32
	otherSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&other, 1)
		w.Write(jpegBytes)
	}))
	defer otherSrv.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.Write(jpegBytes)
		case "/404":
			http.NotFound(w, r)
		case "/big":
			w.Write(append(append([]byte{}, jpegBytes...), make([]byte, MaxImageBytes)...))
		case "/html":
			w.Write([]byte("<html></html>"))
		case "/redir":
			http.Redirect(w, r, otherSrv.URL+"/x", http.StatusFound)
		}
	}))
	defer srv.Close()
	s := newDownloadStore(t, srv)
	ctx := context.Background()

	got, err := s.Download(ctx, srv.URL+"/ok")
	if err != nil || !bytes.Equal(got, jpegBytes) {
		t.Fatalf("ok: %v", err)
	}
	for _, p := range []string{"/404", "/big", "/html", "/redir"} {
		if _, err := s.Download(ctx, srv.URL+p); err == nil {
			t.Errorf("%s: expected error", p)
		}
	}
	if atomic.LoadInt32(&other) != 0 {
		t.Error("redirect target was contacted")
	}

	before := atomic.LoadInt32(&other)
	if _, err := s.Download(ctx, otherSrv.URL+"/x"); err == nil {
		t.Error("disallowed host should error")
	}
	if atomic.LoadInt32(&other) != before {
		t.Error("request made to disallowed host")
	}
}

func TestDownloadFollowsRedirectOnSameHost(t *testing.T) {
	var target int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/id/1-M.jpg":
			http.Redirect(w, r, "/moved/1.jpg", http.StatusFound)
		case "/moved/1.jpg":
			atomic.AddInt32(&target, 1)
			w.Write(jpegBytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	s := newDownloadStore(t, srv)

	got, err := s.Download(context.Background(), srv.URL+"/b/id/1-M.jpg")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if !bytes.Equal(got, jpegBytes) {
		t.Fatalf("got %d bytes, want the redirect target's image", len(got))
	}
	if atomic.LoadInt32(&target) != 1 {
		t.Fatalf("redirect target hit %d times", target)
	}
}

func TestDownloadLimitsRedirects(t *testing.T) {
	var final int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/hop/"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		if n > 0 {
			http.Redirect(w, r, fmt.Sprintf("/hop/%d", n-1), http.StatusFound)
			return
		}
		atomic.AddInt32(&final, 1)
		w.Write(jpegBytes)
	}))
	defer srv.Close()
	s := newDownloadStore(t, srv)

	got, err := s.Download(context.Background(), srv.URL+"/hop/5")
	if err != nil || !bytes.Equal(got, jpegBytes) {
		t.Fatalf("5 redirects: %d bytes, %v", len(got), err)
	}
	atomic.StoreInt32(&final, 0)
	if _, err := s.Download(context.Background(), srv.URL+"/hop/6"); err == nil {
		t.Fatal("6 redirects: expected an error")
	}
	if atomic.LoadInt32(&final) != 0 {
		t.Fatal("6 redirects: the end of the chain was fetched")
	}
}

func TestDownloadRejectsPrivateByDefault(t *testing.T) {
	t.Setenv("LIBRARR_INSECURE_ALLOW_PRIVATE_URLS", "")
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Write(jpegBytes)
	}))
	defer srv.Close()
	s := NewStore(t.TempDir(), srv.Client(), func() string { return srv.URL })
	if _, err := s.Download(context.Background(), srv.URL+"/ok"); err == nil {
		t.Fatal("expected error")
	}
	if hits != 0 {
		t.Fatal("request was made")
	}
}
