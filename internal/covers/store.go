// Package covers stores cover images for library items and downloads them
// from a restricted set of hosts.
package covers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/metadata"
	"github.com/JeremiahM37/librarr/internal/netutil"
)

// MaxImageBytes is the largest cover image accepted from any source.
const MaxImageBytes = 10 << 20

const (
	defaultCoverBase = "https://covers.openlibrary.org"
	maxRedirects     = 5
	tempPrefix       = ".tmp-"
)

// DetectImage identifies JPEG, PNG, GIF and WebP images by their magic bytes.
// Anything else, including SVG, is rejected.
func DetectImage(b []byte) (ext, contentType string, ok bool) {
	switch {
	case bytes.HasPrefix(b, []byte{0xFF, 0xD8, 0xFF}):
		return ".jpg", "image/jpeg", true
	case bytes.HasPrefix(b, []byte("\x89PNG\r\n\x1a\n")):
		return ".png", "image/png", true
	case bytes.HasPrefix(b, []byte("GIF87a")), bytes.HasPrefix(b, []byte("GIF89a")):
		return ".gif", "image/gif", true
	case len(b) >= 12 && bytes.HasPrefix(b, []byte("RIFF")) && bytes.Equal(b[8:12], []byte("WEBP")):
		return ".webp", "image/webp", true
	}
	return "", "", false
}

// CoolDownError means the cover host answered 429 or 503: downloads should
// wait Wait before trying again.
type CoolDownError struct {
	Status int
	Wait   time.Duration
}

func (e *CoolDownError) Error() string {
	return fmt.Sprintf("cover download: status %d, retry in %s", e.Status, e.Wait)
}

// Store keeps cover files in a single directory and downloads new ones.
type Store struct {
	dir          string
	client       *http.Client
	coverBaseURL func() string
	userAgent    string
}

// NewStore returns a Store rooted at dir. client and coverBaseURL may be nil.
func NewStore(dir string, client *http.Client, coverBaseURL func() string) *Store {
	return &Store{dir: dir, client: client, coverBaseURL: coverBaseURL}
}

// WithUserAgent sets the User-Agent sent with downloads and returns s.
func (s *Store) WithUserAgent(ua string) *Store {
	s.userAgent = ua
	return s
}

func (s *Store) base() string {
	if s.coverBaseURL != nil {
		if b := s.coverBaseURL(); b != "" {
			return b
		}
	}
	return defaultCoverBase
}

// path resolves a stored file name inside dir, discarding any directory parts.
func (s *Store) path(file string) (string, error) {
	name := filepath.Base(file)
	if name == "." || name == ".." || name == string(filepath.Separator) || strings.HasPrefix(name, tempPrefix) {
		return "", fmt.Errorf("invalid cover file name")
	}
	return filepath.Join(s.dir, name), nil
}

// Save validates image and writes it as <id><ext>, replacing any cover the
// item had under a different extension. It returns the file name.
func (s *Store) Save(id int64, image []byte) (string, error) {
	ext, _, ok := DetectImage(image)
	if !ok {
		return "", errors.New("not a supported image")
	}
	if len(image) > MaxImageBytes {
		return "", errors.New("image too large")
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(s.dir, tempPrefix+"*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(image); err != nil {
		tmp.Close()
		cleanup()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		cleanup()
		return "", err
	}
	file := strconv.FormatInt(id, 10) + ext
	if err := os.Rename(tmpName, filepath.Join(s.dir, file)); err != nil {
		cleanup()
		return "", err
	}
	for _, other := range []string{".jpg", ".png", ".gif", ".webp"} {
		if other != ext {
			_ = os.Remove(filepath.Join(s.dir, strconv.FormatInt(id, 10)+other))
		}
	}
	return file, nil
}

// Open returns the stored file and its content type, derived from the bytes.
func (s *Store) Open(file string) (io.ReadCloser, string, error) {
	p, err := s.path(file)
	if err != nil {
		return nil, "", err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, "", err
	}
	head := make([]byte, 512)
	n, err := io.ReadFull(f, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		f.Close()
		return nil, "", err
	}
	_, ct, ok := DetectImage(head[:n])
	if !ok {
		f.Close()
		return nil, "", errors.New("stored file is not a supported image")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, "", err
	}
	return f, ct, nil
}

// Exists reports whether file is present in the store. An error means the store
// could not tell, as when the directory is unreadable: the file may well be there.
func (s *Store) Exists(file string) (bool, error) {
	p, err := s.path(file)
	if err != nil {
		return false, nil
	}
	fi, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return fi.Mode().IsRegular(), nil
}

// Remove deletes file from the store. A missing file is not an error.
func (s *Store) Remove(file string) error {
	p, err := s.path(file)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List returns the names of the cover files in the store.
func (s *Store) List() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.Type().IsRegular() && !strings.HasPrefix(e.Name(), tempPrefix) {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// allowedCoverHost reports whether rawURL may be fetched: the same scheme and
// host as base, or https on archive.org or one of its subdomains.
func allowedCoverHost(rawURL, base string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)
	if b, err := url.Parse(base); err == nil && b.Host != "" &&
		scheme == strings.ToLower(b.Scheme) && host == strings.ToLower(b.Host) {
		return true
	}
	if scheme != "https" {
		return false
	}
	h := strings.ToLower(u.Hostname())
	return u.Port() == "" && (h == "archive.org" || strings.HasSuffix(h, ".archive.org"))
}

func (s *Store) checkURL(rawURL string) error {
	if !allowedCoverHost(rawURL, s.base()) {
		return errors.New("cover host not allowed")
	}
	return netutil.ValidateOutboundURL(rawURL)
}

// Download fetches an image from an allowed host, following at most five
// redirects that are each re-checked, and returns the validated bytes.
func (s *Store) Download(ctx context.Context, rawURL string) ([]byte, error) {
	if err := s.checkURL(rawURL); err != nil {
		return nil, err
	}
	var client http.Client
	if s.client != nil {
		client = *s.client
	} else {
		client.Timeout = 30 * time.Second
	}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > maxRedirects {
			return errors.New("too many redirects")
		}
		return s.checkURL(req.URL.String())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if s.userAgent != "" {
		req.Header.Set("User-Agent", s.userAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		return nil, &CoolDownError{Status: resp.StatusCode, Wait: metadata.CoverRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cover download: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxImageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxImageBytes {
		return nil, errors.New("cover image too large")
	}
	if _, _, ok := DetectImage(data); !ok {
		return nil, errors.New("cover is not a supported image")
	}
	return data, nil
}
