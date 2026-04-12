package download

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/organize"
)

// DirectDownloader handles direct HTTP file downloads (Anna's Archive, Gutenberg, etc.).
type DirectDownloader struct {
	cfg    *config.Config
	client *http.Client
}

// NewDirectDownloader creates a new direct file downloader.
func NewDirectDownloader(cfg *config.Config, client *http.Client) *DirectDownloader {
	return &DirectDownloader{cfg: cfg, client: client}
}

var getLinkRe = regexp.MustCompile(`href="(get\.php\?md5=[^"]+)"`)

// libgenMirrors are alternative libgen front-ends that host the same data.
// Tried in order when one fails with transient errors (5xx).
var libgenMirrors = []string{
	"https://libgen.li",
	"https://libgen.is",
	"https://libgen.rs",
	"https://libgen.st",
}

// DownloadFromAnnas downloads a file from Anna's Archive via libgen.
// Returns the local file path and size, or an error.
func (d *DirectDownloader) DownloadFromAnnas(md5, title string, progressFn func(string)) (string, int64, error) {
	if progressFn != nil {
		progressFn("Fetching download link from Anna's Archive...")
	}

	// Step 1: Try each libgen mirror to get the download key.
	downloadURL, mirrorErr := d.fetchLibgenDownloadURL(md5, progressFn)
	if mirrorErr != nil {
		// All libgen mirrors failed. Try Anna's Archive's own slow_download endpoint as a last resort.
		if progressFn != nil {
			progressFn("libgen unavailable, trying Anna's Archive slow download...")
		}
		slowURL := fmt.Sprintf("https://annas-archive.gl/slow_download/%s/0/0", md5)
		if _, _, err := d.downloadFile(slowURL, title, progressFn); err == nil {
			return d.downloadFile(slowURL, title, progressFn)
		}

		// Final fallback: alternative MD5 hashes by re-searching Anna's Archive.
		if progressFn != nil {
			progressFn("No direct link found, trying alternative mirrors...")
		}
		altURL, altErr := d.tryAltMD5(title, md5, progressFn)
		if altErr != nil {
			return "", 0, fmt.Errorf("all libgen mirrors failed (%v); alt search also failed: %v", mirrorErr, altErr)
		}
		return d.downloadFile(altURL, title, progressFn)
	}

	slog.Info("found libgen download link", "title", title, "url", downloadURL[:min(60, len(downloadURL))])

	if progressFn != nil {
		progressFn("Downloading...")
	}

	// Step 2: Download the file.
	return d.downloadFile(downloadURL, title, progressFn)
}

// fetchLibgenDownloadURL tries each libgen mirror to find a valid get.php download link.
// Returns the full URL on success, or the last error if all mirrors fail.
func (d *DirectDownloader) fetchLibgenDownloadURL(md5 string, progressFn func(string)) (string, error) {
	var lastErr error
	for i, mirror := range libgenMirrors {
		if i > 0 && progressFn != nil {
			progressFn(fmt.Sprintf("Mirror %d failed, trying %s...", i, mirror))
		}

		adsURL := fmt.Sprintf("%s/ads.php?md5=%s", mirror, md5)
		req, err := http.NewRequest("GET", adsURL, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("User-Agent", d.cfg.UserAgent)

		resp, err := d.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("fetch %s: %w", mirror, err)
			continue
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		resp.Body.Close()

		if resp.StatusCode >= 500 {
			// Transient server error — move to next mirror.
			lastErr = fmt.Errorf("%s ads page HTTP %d", mirror, resp.StatusCode)
			continue
		}
		if resp.StatusCode != 200 {
			// Non-transient error — this mirror has a permanent issue, try next.
			lastErr = fmt.Errorf("%s ads page HTTP %d", mirror, resp.StatusCode)
			continue
		}

		match := getLinkRe.FindSubmatch(body)
		if len(match) < 2 {
			lastErr = fmt.Errorf("%s returned no get.php link for md5=%s", mirror, md5)
			continue
		}

		return fmt.Sprintf("%s/%s", mirror, string(match[1])), nil
	}
	return "", lastErr
}

// DownloadFromURL downloads a file from any direct URL.
func (d *DirectDownloader) DownloadFromURL(fileURL, title string, progressFn func(string)) (string, int64, error) {
	return d.downloadFile(fileURL, title, progressFn)
}

func (d *DirectDownloader) downloadFile(fileURL, title string, progressFn func(string)) (string, int64, error) {
	req, err := http.NewRequest("GET", fileURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", d.cfg.UserAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", 0, fmt.Errorf("download HTTP %d", resp.StatusCode)
	}

	// If we got an HTML response, try to find the actual download link.
	contentType := resp.Header.Get("Content-Type")
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
				return "", 0, fmt.Errorf("no download link found in HTML response")
			}
			return d.downloadFile(fileLink[1], title, progressFn)
		}
		return d.downloadFile(getLink[1], title, progressFn)
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
	// verify by reading the file signature.
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

// detectFileExtension inspects the first bytes of a file and returns the
// appropriate extension for its actual content. Returns "" if the format
// is not recognized (caller should keep the original extension).
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
		// ZIP container — could be EPUB, CBZ, or plain ZIP.
		// For EPUBs the mimetype file is conventionally present, but checking it
		// requires parsing the ZIP. Since our downloads target ebooks, assume EPUB.
		return ".epub", nil
	case header[0] == 0x52 && header[1] == 0x61 && header[2] == 0x72 && header[3] == 0x21:
		return ".cbr", nil // RAR, likely CBR for ebook context
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

// tryAltMD5 searches Anna's Archive for alternative MD5 hashes and tries them.
func (d *DirectDownloader) tryAltMD5(title, originalMD5 string, progressFn func(string)) (string, error) {
	ctx := context.Background()
	annas := &annasSearchHelper{cfg: d.cfg, client: d.client}
	results, err := annas.searchForTitle(ctx, title)
	if err != nil {
		return "", err
	}

	tried := 0
	for _, r := range results {
		if r.MD5 == "" || r.MD5 == originalMD5 {
			continue
		}
		if tried >= 3 {
			break
		}
		tried++

		if progressFn != nil {
			progressFn(fmt.Sprintf("Trying alt mirror %d/3...", tried))
		}

		adsURL := fmt.Sprintf("https://libgen.li/ads.php?md5=%s", r.MD5)
		req, err := http.NewRequest("GET", adsURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", d.cfg.UserAgent)

		resp, err := d.client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		resp.Body.Close()

		if resp.StatusCode != 200 {
			continue
		}

		match := getLinkRe.FindSubmatch(body)
		if len(match) >= 2 {
			downloadURL := fmt.Sprintf("https://libgen.li/%s", string(match[1]))
			slog.Info("found alt libgen download link", "title", title, "alt_md5", r.MD5)
			return downloadURL, nil
		}
	}

	return "", fmt.Errorf("no alternative MD5 hashes had working download links")
}

// annasSearchHelper is a minimal helper to search Anna's Archive for alt MD5s.
type annasSearchHelper struct {
	cfg    *config.Config
	client *http.Client
}

func (a *annasSearchHelper) searchForTitle(ctx context.Context, title string) ([]struct{ MD5 string }, error) {
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

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	md5Re := regexp.MustCompile(`/md5/([a-f0-9]+)`)
	matches := md5Re.FindAllStringSubmatch(string(body), -1)

	seen := make(map[string]bool)
	var results []struct{ MD5 string }
	for _, m := range matches {
		if len(m) >= 2 && !seen[m[1]] {
			seen[m[1]] = true
			results = append(results, struct{ MD5 string }{m[1]})
		}
	}
	return results, nil
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
