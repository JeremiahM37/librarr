package download

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/netutil"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/zlibraryparse"
	"github.com/PuerkitoBio/goquery"
)

// DirectDownloader handles direct HTTP file downloads (Anna's Archive, Gutenberg, etc.).
type DirectDownloader struct {
	cfg    *config.Config
	client *http.Client
	// validate guards outbound download URLs against SSRF. It defaults to
	// netutil.ValidateOutboundURL; tests that hit loopback httptest servers
	// override it. A nil validator allows everything.
	validate func(string) error
}

// NewDirectDownloader creates a new direct file downloader.
func NewDirectDownloader(cfg *config.Config, client *http.Client) *DirectDownloader {
	return &DirectDownloader{cfg: cfg, client: client, validate: netutil.ValidateOutboundURL}
}

// checkURL runs the configured SSRF guard against a candidate outbound URL.
func (d *DirectDownloader) checkURL(rawURL string) error {
	if d.validate == nil {
		return nil
	}
	return d.validate(rawURL)
}

var getLinkRe = regexp.MustCompile(`href="(get\.php\?md5=[^"]+)"`)
var errLibgenNoMatch = errors.New("libgen no matching MD5")

// noMatchError surfaces a user-friendly message via Error() while still
// matching errLibgenNoMatch under errors.Is. Lets callers route on the
// sentinel (errors.Is) instead of the user-facing string, so rewording or
// localizing the message later can't silently break detection.
type noMatchError struct{ msg string }

func (e *noMatchError) Error() string        { return e.msg }
func (e *noMatchError) Is(target error) bool { return target == errLibgenNoMatch }
func (e *noMatchError) Unwrap() error        { return errLibgenNoMatch }

const zlibraryUserAgent = "Mozilla/5.0 (X11; Linux x86_64; rv:151.0) Gecko/20100101 Firefox/151.0"

// mirrors returns the list of libgen-style ads.php/get.php mirrors to try, in
// order. Sourced from the runtime registry (cfg.Sources.LibgenMirrors).
func (d *DirectDownloader) mirrors() []string {
	return d.cfg.Sources.LibgenMirrors
}

// DownloadFromAnnas downloads a file from Anna's Archive via libgen.
// Returns the local file path and size, or an error.
func (d *DirectDownloader) DownloadFromAnnas(md5, title string, progressFn func(string)) (string, int64, string, error) {
	if progressFn != nil {
		progressFn("Fetching download link from Anna's Archive...")
	}

	attemptedURLs := make(map[string]bool)
	filePath, fileSize, originalErr := d.downloadFromLibgenMirrors(md5, title, attemptedURLs, progressFn)
	if originalErr == nil {
		return filePath, fileSize, md5, nil
	} else if progressFn != nil {
		progressFn("Original file unavailable, searching matching alternatives...")
	}

	results, err := (&annasSearchHelper{cfg: d.cfg, client: d.client}).searchForTitle(context.Background(), title)
	if err != nil {
		return "", 0, "", fmt.Errorf("search alternative Anna's Archive files: %w", err)
	}

	lastErr := originalErr
	matchingAlternatives := 0
	for _, result := range results {
		if result.MD5 == "" || result.MD5 == md5 || !titlesMatch(title, result.Title) {
			continue
		}
		if matchingAlternatives >= 5 {
			break
		}
		matchingAlternatives++
		if progressFn != nil {
			progressFn(fmt.Sprintf("Trying matching alternative %d...", matchingAlternatives))
		}
		filePath, fileSize, err := d.downloadFromLibgenMirrors(result.MD5, title, attemptedURLs, progressFn)
		if err == nil {
			return filePath, fileSize, result.MD5, nil
		}
		lastErr = err
	}

	if matchingAlternatives == 0 && errors.Is(originalErr, errLibgenNoMatch) {
		return "", 0, "", &noMatchError{msg: "Anna's Archive could not find a matching LibGen MD5 for this book. Download it manually from Anna's Archive or choose another source."}
	}
	if lastErr == nil {
		lastErr = errLibgenNoMatch
	}
	return "", 0, "", fmt.Errorf("all matching LibGen candidates exhausted: %w", lastErr)
}

// downloadFromLibgenMirrors resolves and downloads one MD5 from each mirror.
// A bad response or failed file verification advances to the next mirror.
func (d *DirectDownloader) downloadFromLibgenMirrors(md5, title string, attemptedURLs map[string]bool, progressFn func(string)) (string, int64, error) {
	var lastErr error
	for i, mirror := range d.mirrors() {
		if progressFn != nil {
			progressFn(fmt.Sprintf("Trying mirror %d/%d...", i+1, len(d.mirrors())))
		}
		downloadURL, err := d.fetchLibgenDownloadURLFromMirror(mirror, md5)
		if err != nil {
			lastErr = err
			continue
		}
		if attemptedURLs[downloadURL] {
			lastErr = fmt.Errorf("%s resolved to an already attempted download URL", mirror)
			continue
		}
		attemptedURLs[downloadURL] = true

		slog.Info("found libgen download link", "title", title, "md5", md5, "mirror", mirror)
		if progressFn != nil {
			progressFn(fmt.Sprintf("Downloading from mirror %d/%d...", i+1, len(d.mirrors())))
		}
		filePath, fileSize, err := d.downloadFile(downloadURL, title, progressFn)
		if err == nil {
			return filePath, fileSize, nil
		}
		lastErr = fmt.Errorf("%s download: %w", mirror, err)
	}
	if lastErr == nil {
		lastErr = errLibgenNoMatch
	}
	return "", 0, lastErr
}

// fetchLibgenDownloadURL tries each libgen mirror to find a valid get.php
// download link. Returns the full URL on success, or the last error if all
// mirrors fail. Transient errors (5xx, network) move on to the next mirror;
// "link not found" on one mirror also tries the next since mirrors sometimes
// have the book while others don't.
func (d *DirectDownloader) fetchLibgenDownloadURL(md5 string, progressFn func(string)) (string, error) {
	var lastErr error
	allNoMatch := true
	for i, mirror := range d.mirrors() {
		if i > 0 && progressFn != nil {
			progressFn(fmt.Sprintf("Trying mirror: %s", mirror))
		}

		downloadURL, err := d.fetchLibgenDownloadURLFromMirror(mirror, md5)
		if err != nil {
			lastErr = err
			if !errors.Is(err, errLibgenNoMatch) {
				allNoMatch = false
			}
			continue
		}
		return downloadURL, nil
	}
	if lastErr != nil && allNoMatch {
		return "", fmt.Errorf("%w", errLibgenNoMatch)
	}
	return "", lastErr
}

func (d *DirectDownloader) fetchLibgenDownloadURLFromMirror(mirror, md5 string) (string, error) {
	adsURL := fmt.Sprintf("%s/ads.php?md5=%s", mirror, md5)
	req, err := http.NewRequest("GET", adsURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", d.cfg.UserAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%s: %w", mirror, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("%s HTTP %d", mirror, resp.StatusCode)
	}
	if strings.Contains(string(body), "File not found in DB") || strings.Contains(string(body), "File not found") {
		return "", fmt.Errorf("%s: %w", mirror, errLibgenNoMatch)
	}
	match := getLinkRe.FindSubmatch(body)
	if len(match) < 2 {
		return "", fmt.Errorf("%s: no get.php link for md5=%s", mirror, md5)
	}
	return fmt.Sprintf("%s/%s", mirror, string(match[1])), nil
}

// DownloadFromURL downloads a file from any direct URL.
func (d *DirectDownloader) DownloadFromURL(fileURL, title string, progressFn func(string)) (string, int64, error) {
	return d.downloadFileWithClient(d.client, fileURL, title, progressFn)
}

func (d *DirectDownloader) DownloadFromZLibrary(fileURL, title, author, sourceID string, progressFn func(string)) (string, int64, error) {
	if progressFn != nil {
		progressFn("Signing in to Z-Library...")
	}

	client, err := d.zlibraryClient()
	if err != nil {
		return "", 0, err
	}
	if err := d.zlibraryLogin(client); err != nil {
		return "", 0, err
	}

	if progressFn != nil {
		progressFn("Resolving Z-Library download link...")
	}
	resolvedURL, err := d.resolveZLibraryDownloadURL(client, fileURL, title, author, sourceID)
	if err != nil {
		return "", 0, err
	}
	return d.downloadFileWithClientAndUserAgent(client, resolvedURL, title, progressFn, zlibraryUserAgent)
}

func (d *DirectDownloader) downloadFile(fileURL, title string, progressFn func(string)) (string, int64, error) {
	return d.downloadFileWithClient(d.client, fileURL, title, progressFn)
}

func (d *DirectDownloader) downloadFileWithClient(client *http.Client, fileURL, title string, progressFn func(string)) (string, int64, error) {
	return d.downloadFileWithClientAndUserAgent(client, fileURL, title, progressFn, d.cfg.UserAgent)
}

func (d *DirectDownloader) downloadFileWithClientAndUserAgent(client *http.Client, fileURL, title string, progressFn func(string), userAgent string) (string, int64, error) {
	return d.downloadFileAttempt(client, fileURL, title, progressFn, true, userAgent, 0)
}

// maxDownloadHops bounds how many times downloadFileAttempt will follow an HTML
// "GET" page (or retry after a browser challenge) before giving up. Without it,
// a malicious or misconfigured mirror can serve an HTML page whose download
// link points at another HTML page, recursing until the goroutine stack is
// exhausted and the download job wedges.
const maxDownloadHops = 5

func (d *DirectDownloader) downloadFileAttempt(client *http.Client, fileURL, title string, progressFn func(string), allowChallenge bool, userAgent string, depth int) (string, int64, error) {
	if depth > maxDownloadHops {
		return "", 0, fmt.Errorf("too many download hops (exceeded %d): mirror kept returning HTML instead of a file", maxDownloadHops)
	}

	// Re-validate on every hop. This is the single chokepoint every download
	// request funnels through, including URLs scraped from HTML "GET" pages,
	// which are attacker-influenced and never pass the entry-point guard.
	if err := d.checkURL(fileURL); err != nil {
		return "", 0, err
	}

	req, err := http.NewRequest("GET", fileURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("download file: %w", err)
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(contentType, "text/html") && allowChallenge {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		if token, ok := solveZLibraryChallenge(body); ok {
			if progressFn != nil {
				progressFn("Passing Z-Library browser check...")
			}
			if err := addCookie(client, fileURL, "c_token", token); err != nil {
				return "", 0, err
			}
			_ = addCookie(client, fileURL, "c_time", "0.1")
			return d.downloadFileAttempt(client, fileURL, title, progressFn, false, userAgent, depth+1)
		}
		return "", 0, fmt.Errorf("zlibrary browser challenge changed or could not be solved")
	}

	if resp.StatusCode != 200 {
		return "", 0, fmt.Errorf("download HTTP %d", resp.StatusCode)
	}

	// If we got an HTML response, try to find the actual download link.
	if strings.Contains(contentType, "text/html") {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024)) // 2MB max for HTML
		bodyStr := string(body)

		if strings.Contains(bodyStr, "File not found") || strings.Contains(bodyStr, "Error</h1>") {
			return "", 0, fmt.Errorf("file not found on server")
		}

		// Look for a GET link on the page.
		getLink := regexp.MustCompile(`href="(https?://[^"]+)"[^>]*>GET</a>`).FindStringSubmatch(bodyStr)
		if len(getLink) < 2 {
			fileLink := regexp.MustCompile(`href="(https?://[^"]*\.(epub|pdf|mobi)[^"]*)"`).FindStringSubmatch(bodyStr)
			if len(fileLink) < 2 {
				if dlLink := zlibraryparse.FindDownloadLinkInHTML(fileURL, body); dlLink != "" {
					return d.downloadFileAttempt(client, dlLink, title, progressFn, true, userAgent, depth+1)
				}
				return "", 0, fmt.Errorf("no download link found in HTML response")
			}
			return d.downloadFileAttempt(client, fileLink[1], title, progressFn, true, userAgent, depth+1)
		}
		return d.downloadFileAttempt(client, getLink[1], title, progressFn, true, userAgent, depth+1)
	}

	// Save to incoming directory.
	safeTitle := sanitizeFilename(title, 80)
	if err := os.MkdirAll(d.cfg.IncomingDir, 0755); err != nil {
		return "", 0, fmt.Errorf("create incoming dir: %w", err)
	}

	// Initial extension from Content-Type (may be corrected after inspecting bytes).
	ext := ".epub"
	if strings.Contains(contentType, "pdf") {
		ext = ".pdf"
	}

	filePath := filepath.Join(d.cfg.IncomingDir, safeTitle+ext)
	f, err := os.Create(filePath)
	if err != nil {
		return "", 0, fmt.Errorf("create file: %w", err)
	}

	written, err := io.Copy(f, resp.Body)
	f.Close()
	if err != nil {
		os.Remove(filePath)
		return "", 0, fmt.Errorf("write file: %w", err)
	}

	if written < 1000 {
		os.Remove(filePath)
		return "", 0, fmt.Errorf("downloaded file too small (%d bytes)", written)
	}

	// Detect actual file type from magic bytes and correct the extension if needed.
	// Content-Type headers often lie (e.g., application/octet-stream) so we always
	// verify by reading the file signature. This fixes #8.
	actualExt, err := detectFileExtension(filePath)
	if err != nil {
		slog.Warn("failed to detect file type from content", "error", err, "path", filePath)
	} else if actualExt != "" && actualExt != ext {
		correctedPath := filepath.Join(d.cfg.IncomingDir, safeTitle+actualExt)
		if err := os.Rename(filePath, correctedPath); err == nil {
			slog.Info("corrected file extension based on content", "title", title,
				"from", ext, "to", actualExt)
			filePath = correctedPath
			ext = actualExt
		}
	}

	slog.Info("file downloaded", "title", title, "size", written, "path", filePath)

	// EPUB verification: validate ZIP and title match (only for actual EPUB files).
	if strings.HasSuffix(strings.ToLower(filePath), ".epub") {
		if err := d.verifyEPUB(filePath, title); err != nil {
			os.Remove(filePath)
			return "", 0, fmt.Errorf("EPUB verification failed: %w", err)
		}
	}

	return filePath, written, nil
}

func (d *DirectDownloader) zlibraryClient() (*http.Client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("zlibrary cookie jar: %w", err)
	}
	client := &http.Client{
		Timeout:       d.client.Timeout,
		CheckRedirect: d.client.CheckRedirect,
		Jar:           jar,
	}
	if d.client.Transport != nil {
		client.Transport = d.client.Transport
	}
	return client, nil
}

func (d *DirectDownloader) zlibraryBase() string {
	if d.cfg.ZLibraryURL != "" {
		return strings.TrimRight(d.cfg.ZLibraryURL, "/")
	}
	return strings.TrimRight(d.cfg.Sources.ZLibraryDefault, "/")
}

func (d *DirectDownloader) zlibraryLogin(client *http.Client) error {
	baseURL := d.zlibraryBase()
	form := url.Values{}
	form.Set("isModal", "true")
	form.Set("email", d.cfg.ZLibraryEmail)
	form.Set("password", d.cfg.ZLibraryPassword)
	form.Set("site_mode", "books")
	form.Set("action", "login")
	form.Set("redirectUrl", baseURL+"/")
	form.Set("gg_json_mode", "1")

	req, err := http.NewRequest("POST", baseURL+"/rpc.php", strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("zlibrary login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("User-Agent", zlibraryUserAgent)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("zlibrary login: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("zlibrary login HTTP %d", resp.StatusCode)
	}

	var loginResp struct {
		Errors   []string `json:"errors"`
		Response struct {
			UserID  int    `json:"user_id"`
			UserKey string `json:"user_key"`
		} `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("zlibrary decode login: %w", err)
	}
	if len(loginResp.Errors) > 0 {
		return fmt.Errorf("zlibrary login failed: %s", strings.Join(loginResp.Errors, "; "))
	}
	if loginResp.Response.UserID == 0 || loginResp.Response.UserKey == "" {
		return fmt.Errorf("zlibrary login failed: missing session credentials")
	}
	return nil
}

func (d *DirectDownloader) resolveZLibraryDownloadURL(client *http.Client, currentURL, title, author, sourceID string) (string, error) {
	baseURL := d.zlibraryBase()
	if id, hash, ok := parseZLibrarySourceID(sourceID); ok {
		return d.resolveZLibraryBookURL(client, baseURL, id, hash)
	}

	bookURL, err := d.resolveZLibraryBookFromSearch(client, baseURL, title, author)
	if err == nil {
		return bookURL, nil
	}

	if currentURL == "" {
		return "", err
	}
	return currentURL, nil
}

func (d *DirectDownloader) resolveZLibraryBookURL(client *http.Client, baseURL string, id int, hash string) (string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/eapi/book/%d/%s", baseURL, id, hash), nil)
	if err != nil {
		return "", fmt.Errorf("zlibrary detail request: %w", err)
	}
	req.Header.Set("User-Agent", zlibraryUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("zlibrary detail: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("zlibrary detail HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("zlibrary read detail: %w", err)
	}
	if dl, err := zlibraryparse.DetailDownloadFromJSON(body); err == nil && dl != "" {
		return zlibraryparse.AbsoluteURL(baseURL, dl), nil
	}
	if dl := zlibraryparse.FindDownloadLinkInHTML(baseURL, body); dl != "" {
		return dl, nil
	}
	return "", fmt.Errorf("zlibrary detail missing download link")
}

func (d *DirectDownloader) resolveZLibraryBookFromSearch(client *http.Client, baseURL, title, author string) (string, error) {
	form := url.Values{}
	query := strings.TrimSpace(title + " " + author)
	form.Set("message", query)
	form.Set("limit", "10")
	form.Set("page", "1")

	req, err := http.NewRequest("POST", baseURL+"/eapi/book/search", strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("zlibrary search request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", zlibraryUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("zlibrary search: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("zlibrary search HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("zlibrary read search: %w", err)
	}
	if dl := zlibraryparse.FindDownloadLinkInHTML(baseURL, body); dl != "" {
		return dl, nil
	}
	books, err := zlibraryparse.BooksFromJSON(body)
	if err != nil {
		return "", fmt.Errorf("zlibrary decode search: %w", err)
	}

	titleNorm := strings.ToLower(strings.TrimSpace(title))
	authorNorm := strings.ToLower(strings.TrimSpace(author))
	for _, book := range books {
		if strings.ToLower(strings.TrimSpace(book.Title)) != titleNorm {
			continue
		}
		if authorNorm != "" && !strings.Contains(strings.ToLower(book.Author), authorNorm) {
			continue
		}
		if book.Hash != "" {
			return d.resolveZLibraryBookURL(client, baseURL, book.ID, book.Hash)
		}
		if book.DL != "" {
			return zlibraryparse.AbsoluteURL(baseURL, book.DL), nil
		}
	}
	return "", fmt.Errorf("zlibrary search did not find an exact downloadable match")
}

func parseZLibrarySourceID(sourceID string) (int, string, bool) {
	if !strings.HasPrefix(sourceID, "zlibrary-") {
		return 0, "", false
	}
	rest := strings.TrimPrefix(sourceID, "zlibrary-")
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", false
	}
	var id int
	if _, err := fmt.Sscanf(parts[0], "%d", &id); err != nil || id == 0 {
		return 0, "", false
	}
	return id, parts[1], true
}

func solveZLibraryChallenge(body []byte) (string, bool) {
	if !bytesContains(body, []byte("Checking your browser")) || !bytesContains(body, []byte("c_token=")) {
		return "", false
	}
	seedMatch := regexp.MustCompile(`\['([A-F0-9]{40})','c_token=','array'\]`).FindSubmatch(body)
	if len(seedMatch) < 2 {
		return "", false
	}
	seed := string(seedMatch[1])
	startIndex, ok := firstHexNibble(seed)
	if !ok || startIndex >= sha1.Size-1 {
		return "", false
	}
	for i := 0; i < 5_000_000; i++ {
		value := fmt.Sprintf("%s%d", seed, i)
		sum := sha1.Sum([]byte(value))
		if sum[startIndex] == 0xb0 && sum[startIndex+1] == 0x0b {
			return value, true
		}
	}
	return "", false
}

func firstHexNibble(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	switch {
	case s[0] >= '0' && s[0] <= '9':
		return int(s[0] - '0'), true
	case s[0] >= 'a' && s[0] <= 'f':
		return int(s[0]-'a') + 10, true
	case s[0] >= 'A' && s[0] <= 'F':
		return int(s[0]-'A') + 10, true
	default:
		return 0, false
	}
}

func addCookie(client *http.Client, rawURL, name, value string) error {
	if client.Jar == nil {
		jar, err := cookiejar.New(nil)
		if err != nil {
			return fmt.Errorf("create cookie jar: %w", err)
		}
		client.Jar = jar
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse cookie URL: %w", err)
	}
	client.Jar.SetCookies(parsed, []*http.Cookie{{Name: name, Value: value, Path: "/"}})
	return nil
}

func bytesContains(haystack, needle []byte) bool {
	return strings.Contains(string(haystack), string(needle))
}

// detectFileExtension inspects the first bytes of a file and returns the
// appropriate extension for its actual content. Returns "" if the format
// is not recognized (caller should keep the original extension).
// DetectFileExtension inspects file magic bytes and returns an appropriate extension.
func DetectFileExtension(path string) (string, error) {
	return detectFileExtension(path)
}

func detectFileExtension(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var header [8]byte
	n, err := f.Read(header[:])
	if err != nil && err != io.EOF {
		return "", err
	}
	if n < 4 {
		return "", nil
	}

	// Magic byte signatures
	switch {
	case string(header[:5]) == "%PDF-":
		return ".pdf", nil
	case header[0] == 0x50 && header[1] == 0x4B && (header[2] == 0x03 || header[2] == 0x05):
		// ZIP container — could be EPUB, CBZ, or plain ZIP. For ebook downloads,
		// EPUB is overwhelmingly the most common format, so we assume it.
		return ".epub", nil
	case header[0] == 0x52 && header[1] == 0x61 && header[2] == 0x72 && header[3] == 0x21:
		return ".cbr", nil // RAR, likely CBR in ebook context
	case string(header[:4]) == "BOOK" || (header[0] == 0xEB && header[2] == 0x48):
		return ".mobi", nil
	}
	return "", nil
}

// verifyEPUB validates that an EPUB file is a valid ZIP and its title matches.
func (d *DirectDownloader) verifyEPUB(filePath, expectedTitle string) error {
	// Validate ZIP structure.
	if _, err := os.Stat(filePath); err != nil {
		return err
	}

	// Check title overlap (60% threshold).
	ok, actualTitle, err := organize.VerifyEPUBTitle(filePath, expectedTitle, 0.6)
	if err != nil {
		slog.Warn("EPUB metadata extraction failed (allowing download)", "error", err)
		return nil // Can't verify, let it pass.
	}
	if !ok {
		return fmt.Errorf("wrong book: expected %q, got %q", expectedTitle, actualTitle)
	}
	return nil
}

// annasSearchHelper is a minimal helper to search Anna's Archive for alt MD5s.
type annasSearchHelper struct {
	cfg    *config.Config
	client *http.Client
}

type annasSearchCandidate struct {
	MD5   string
	Title string
}

func (a *annasSearchHelper) searchForTitle(ctx context.Context, title string) ([]annasSearchCandidate, error) {
	baseURL := fmt.Sprintf("https://%s/search", a.cfg.AnnasArchiveDomain)
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("q", title)
	q.Set("ext", "epub")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", a.cfg.UserAgent)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return parseAnnasSearchCandidates(io.LimitReader(resp.Body, 2*1024*1024))
}

func parseAnnasSearchCandidates(reader io.Reader) ([]annasSearchCandidate, error) {
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		return nil, fmt.Errorf("parse Anna's Archive search HTML: %w", err)
	}

	seen := make(map[string]bool)
	md5Re := regexp.MustCompile(`(?i)/md5/([a-f0-9]{32})`)
	var results []annasSearchCandidate
	doc.Find("a[href*='/md5/']").Each(func(_ int, selection *goquery.Selection) {
		href, ok := selection.Attr("href")
		if !ok {
			return
		}
		match := md5Re.FindStringSubmatch(href)
		candidateTitle := strings.TrimSpace(selection.Text())
		if len(match) < 2 || candidateTitle == "" {
			return
		}
		md5 := strings.ToLower(match[1])
		if seen[md5] {
			return
		}
		seen[md5] = true
		results = append(results, annasSearchCandidate{MD5: md5, Title: candidateTitle})
	})
	return results, nil
}

var titleWordRe = regexp.MustCompile(`[\p{L}\p{N}]+`)

var titleStopwords = map[string]bool{
	"a": true, "an": true, "and": true, "at": true, "by": true,
	"for": true, "from": true, "in": true, "of": true, "on": true,
	"or": true, "the": true, "to": true, "with": true,
}

func titlesMatch(expected, candidate string) bool {
	expectedWords := significantTitleWords(expected)
	candidateWords := significantTitleWords(candidate)
	if len(expectedWords) == 0 || len(candidateWords) == 0 {
		return false
	}

	matches := 0
	for word := range expectedWords {
		if candidateWords[word] {
			matches++
		}
	}
	// Anna results often prefix an author or append a file extension, but a
	// matching edition should still contain the book's primary title. Requiring
	// that core prevents a generic subtitle fragment such as "Public Transit"
	// from selecting an unrelated result.
	primary := primaryTitle(expected)
	primaryWords := significantTitleWords(primary)
	primaryMatches := 0
	for word := range primaryWords {
		if candidateWords[word] {
			primaryMatches++
		}
	}
	if primary != expected && len(primaryWords) >= 2 && primaryMatches == len(primaryWords) {
		return true
	}

	return matches >= 2 &&
		float64(matches)/float64(len(expectedWords)) >= 0.5 &&
		float64(matches)/float64(len(candidateWords)) >= 0.5
}

func primaryTitle(title string) string {
	if before, _, ok := strings.Cut(title, ":"); ok {
		return before
	}
	return title
}

func significantTitleWords(title string) map[string]bool {
	words := make(map[string]bool)
	for _, word := range titleWordRe.FindAllString(strings.ToLower(title), -1) {
		if len(word) > 1 && !titleStopwords[word] && word != "epub" && word != "pdf" {
			words[word] = true
		}
	}
	return words
}

var unsafeCharsRe = regexp.MustCompile(`[^\w\s-]`)

func sanitizeFilename(name string, maxLen int) string {
	name = unsafeCharsRe.ReplaceAllString(name, "")
	name = strings.TrimSpace(name)
	if len(name) > maxLen {
		name = name[:maxLen]
	}
	if name == "" {
		name = "book"
	}
	return name
}
