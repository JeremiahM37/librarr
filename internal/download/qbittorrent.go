package download

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/netutil"
)

// QBittorrentClient wraps the qBittorrent Web API.
type QBittorrentClient struct {
	cfg            *config.Config
	client         *http.Client
	mu             sync.Mutex
	authenticated  bool
	cookies        []*http.Cookie
	banUntil       time.Time
	nextLogin      time.Time
	backoffSec     int
	lastError      string
	verifyTimeout  time.Duration
	verifyInterval time.Duration
	fetchTimeout   time.Duration
}

// TorrentVerificationWarning indicates that qBittorrent accepted the add
// request but the torrent was not visible before the verification deadline.
// The submission remains accepted; callers may surface this as a warning.
type TorrentVerificationWarning struct {
	Err error
}

func (e *TorrentVerificationWarning) Error() string {
	return fmt.Sprintf("torrent accepted but verification incomplete: %v", e.Err)
}

func (e *TorrentVerificationWarning) Unwrap() error { return e.Err }

type torrentVerificationTimeoutError struct{}

func (torrentVerificationTimeoutError) Error() string {
	return "timed out waiting for torrent to appear in qBittorrent"
}

// Name identifies this client in logs and the TorrentClient interface.
func (q *QBittorrentClient) Name() string { return "qbittorrent" }

// NewQBittorrentClient creates a new qBittorrent API client.
func NewQBittorrentClient(cfg *config.Config) *QBittorrentClient {
	return &QBittorrentClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
			// Do not follow redirects automatically for cookie handling.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		backoffSec:     3,
		verifyTimeout:  25 * time.Second,
		verifyInterval: 250 * time.Millisecond,
		fetchTimeout:   30 * time.Second,
	}
}

// Login authenticates with qBittorrent.
func (q *QBittorrentClient) Login() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.login()
}

func (q *QBittorrentClient) login() error {
	if !q.cfg.HasQBittorrent() {
		return fmt.Errorf("qBittorrent not configured")
	}

	now := time.Now()
	if !q.nextLogin.IsZero() && now.Before(q.nextLogin) {
		return fmt.Errorf("login cooldown active, retry in %ds", int(q.nextLogin.Sub(now).Seconds()))
	}

	data := url.Values{
		"username": {q.cfg.QBUser},
		"password": {q.cfg.QBPass},
	}

	resp, err := q.client.PostForm(q.cfg.QBUrl+"/api/v2/auth/login", data)
	if err != nil {
		q.scheduleBackoff()
		return fmt.Errorf("qbittorrent login: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		q.scheduleBackoff()
		return fmt.Errorf("read qBittorrent login response: %w", err)
	}
	bodyStr := string(body)

	if strings.Contains(strings.ToLower(bodyStr), "banned") {
		q.banUntil = now.Add(60 * time.Second)
		q.authenticated = false
		q.nextLogin = now.Add(60 * time.Second)
		return fmt.Errorf("IP banned by qBittorrent")
	}

	// qBittorrent 4.x returns 200 OK with body "Ok." on success.
	// qBittorrent 5.x returns 204 No Content with an empty body; the session
	// cookie (QBT_SID_*) in response headers signals success.
	cookies := resp.Cookies()
	hasSession := false
	for _, c := range cookies {
		if strings.HasPrefix(c.Name, "QBT_SID") {
			hasSession = true
			break
		}
	}
	success := resp.StatusCode >= 200 && resp.StatusCode < 300 && (bodyStr == "Ok." || hasSession)

	if !success {
		q.nextLogin = now.Add(30 * time.Second)
		q.authenticated = false
		if bodyStr == "" {
			return fmt.Errorf("login failed: HTTP %d", resp.StatusCode)
		}
		return fmt.Errorf("login failed: %s", bodyStr)
	}

	q.cookies = cookies
	q.authenticated = true
	q.backoffSec = 3
	q.nextLogin = time.Time{}
	q.lastError = ""
	slog.Info("qBittorrent authenticated")
	return nil
}

func (q *QBittorrentClient) scheduleBackoff() {
	q.nextLogin = time.Now().Add(time.Duration(q.backoffSec) * time.Second)
	if q.backoffSec < 60 {
		q.backoffSec *= 2
	}
}

func (q *QBittorrentClient) ensureAuth() error {
	if q.authenticated {
		return nil
	}
	if time.Now().Before(q.banUntil) {
		return fmt.Errorf("qBittorrent login cooldown active")
	}
	return q.login()
}

func (q *QBittorrentClient) doRequest(method, path string, data url.Values) (*http.Response, error) {
	return q.doRequestContext(context.Background(), method, path, data)
}

func (q *QBittorrentClient) doRequestContext(ctx context.Context, method, path string, data url.Values) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.ensureAuth(); err != nil {
		return nil, err
	}

	var resp *http.Response
	var err error

	for attempt := 0; attempt < 2; attempt++ {
		var req *http.Request
		if method == "POST" && data != nil {
			req, err = http.NewRequest(method, q.cfg.QBUrl+path, strings.NewReader(data.Encode()))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			req, err = http.NewRequest(method, q.cfg.QBUrl+path, nil)
			if err != nil {
				return nil, err
			}
			if data != nil {
				req.URL.RawQuery = data.Encode()
			}
		}
		req = req.WithContext(ctx)

		for _, c := range q.cookies {
			req.AddCookie(c)
		}

		resp, err = q.client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 403 && attempt == 0 {
			resp.Body.Close()
			q.authenticated = false
			if err := q.login(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}

	return resp, nil
}

// AddTorrent adds a torrent to qBittorrent.
func (q *QBittorrentClient) AddTorrent(torrentURL, title, savePath, category, expectedInfoHash string) error {
	if savePath == "" {
		savePath = q.cfg.QBSavePath
	}
	if category == "" {
		category = q.cfg.QBCategory
	}

	expectedInfoHash = firstNonEmptyHash(expectedInfoHash, infoHashFromMagnet(torrentURL))
	logTitle := netutil.SanitizeLogValue(title)
	logCategory := netutil.SanitizeLogValue(category)
	var ids []string
	var err error
	if isMagnetURL(torrentURL) {
		slog.Info("submitting magnet to qBittorrent", "title", logTitle, "category", logCategory)
		ids, err = q.addTorrentURL(torrentURL, savePath, category)
	} else if isHTTPURL(torrentURL) && q.isProwlarrTorrentURL(torrentURL) {
		slog.Info("fetching torrent before qBittorrent upload", "title", logTitle, "category", logCategory)
		var fetched fetchedTorrent
		if fetched, err = q.fetchTorrent(torrentURL); err == nil {
			if fetched.magnetURL != "" {
				magnetHash := infoHashFromMagnet(fetched.magnetURL)
				if supplied := firstNonEmptyHash(expectedInfoHash); supplied != "" && supplied != magnetHash {
					slog.Warn(
						"redirected magnet info hash differs from search result",
						"title", logTitle,
						"expected_hash", netutil.SanitizeLogValue(supplied),
						"magnet_hash", netutil.SanitizeLogValue(magnetHash),
					)
				}

				// The hash carried by the magnet Prowlarr redirected to is what
				// qBittorrent will register, so it is authoritative for
				// verification.
				expectedInfoHash = magnetHash

				slog.Info(
					"submitting redirected magnet to qBittorrent",
					"title", logTitle,
					"category", logCategory,
				)

				ids, err = q.addTorrentURL(
					fetched.magnetURL,
					savePath,
					category,
				)
			} else {
				if supplied := firstNonEmptyHash(expectedInfoHash); supplied != "" && supplied != fetched.infoHash {
					slog.Warn(
						"torrent info hash differs from search result",
						"title", logTitle,
						"expected_hash", netutil.SanitizeLogValue(supplied),
						"fetched_hash", netutil.SanitizeLogValue(fetched.infoHash),
					)
				}

				// The hash of the fetched bytes is authoritative for verification.
				expectedInfoHash = fetched.infoHash

				slog.Info(
					"uploading torrent bytes to qBittorrent",
					"title", logTitle,
					"category", logCategory,
					"filename", netutil.SanitizeLogValue(fetched.filename),
					"bytes", len(fetched.body),
				)

				ids, err = q.addTorrentFile(
					fetched,
					savePath,
					category,
				)
			}
		}
	} else {
		slog.Info("submitting torrent URL to qBittorrent", "title", logTitle, "category", logCategory)
		ids, err = q.addTorrentURL(torrentURL, savePath, category)
	}
	if err != nil {
		slog.Warn("qBittorrent torrent submission failed", "title", logTitle, "category", logCategory, "error", netutil.SanitizeSensitiveText(err.Error()))
		return err
	}

	if err := q.verifyTorrentAdded(expectedInfoHash, ids, title, category); err != nil {
		slog.Warn("qBittorrent torrent verification failed", "title", logTitle, "category", logCategory, "error", netutil.SanitizeSensitiveText(err.Error()))
		var timeoutErr torrentVerificationTimeoutError
		if errors.As(err, &timeoutErr) {
			return &TorrentVerificationWarning{Err: err}
		}
		return err
	}

	slog.Info("torrent added to qBittorrent", "title", logTitle, "category", logCategory)
	return nil
}

func (q *QBittorrentClient) addTorrentURL(torrentURL, savePath, category string) ([]string, error) {
	data := url.Values{
		"urls":     {torrentURL},
		"savepath": {savePath},
		"category": {category},
	}

	resp, err := q.doRequest("POST", "/api/v2/torrents/add", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read add torrent response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("add torrent HTTP %d: %s", resp.StatusCode, string(body))
	}

	parsed, err := parseQBittorrentAddTorrentResponse(body)
	if err != nil {
		return nil, fmt.Errorf("add torrent failed: %w", err)
	}

	return parsed.AddedTorrentIDs, nil
}

func (q *QBittorrentClient) addTorrentFile(torrent fetchedTorrent, savePath, category string) ([]string, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("torrents", torrent.filename)
	if err != nil {
		return nil, err
	}
	if _, err := part.Write(torrent.body); err != nil {
		return nil, err
	}
	if savePath != "" {
		if err := writer.WriteField("savepath", savePath); err != nil {
			return nil, fmt.Errorf("write torrent save path: %w", err)
		}
	}
	if category != "" {
		if err := writer.WriteField("category", category); err != nil {
			return nil, fmt.Errorf("write torrent category: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	resp, err := q.doMultipartRequest("/api/v2/torrents/add", writer.FormDataContentType(), buf.Bytes())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read add torrent response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("add torrent HTTP %d: %s", resp.StatusCode, string(body))
	}

	parsed, err := parseQBittorrentAddTorrentResponse(body)
	if err != nil {
		return nil, fmt.Errorf("add torrent failed: %w", err)
	}
	return parsed.AddedTorrentIDs, nil
}

func (q *QBittorrentClient) doMultipartRequest(path, contentType string, body []byte) (*http.Response, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.ensureAuth(); err != nil {
		return nil, err
	}

	var resp *http.Response
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		req, reqErr := http.NewRequest("POST", q.cfg.QBUrl+path, bytes.NewReader(body))
		if reqErr != nil {
			return nil, reqErr
		}
		req.Header.Set("Content-Type", contentType)
		for _, c := range q.cookies {
			req.AddCookie(c)
		}

		resp, err = q.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == 403 && attempt == 0 {
			resp.Body.Close()
			q.authenticated = false
			if err := q.login(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	return resp, nil
}

type qbittorrentAddTorrentResponse struct {
	AddedTorrentIDs []string `json:"added_torrent_ids"`
	SuccessCount    int      `json:"success_count"`
	FailureCount    int      `json:"failure_count"`
	PendingCount    int      `json:"pending_count"`
	Error           string   `json:"error"`
}

func parseQBittorrentAddTorrentResponse(body []byte) (qbittorrentAddTorrentResponse, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "Ok." {
		return qbittorrentAddTorrentResponse{}, nil
	}

	var parsed qbittorrentAddTorrentResponse
	if err := json.Unmarshal(body, &parsed); err == nil {
		if parsed.Error != "" {
			return parsed, errors.New(parsed.Error)
		}
		if parsed.FailureCount > 0 {
			return parsed, fmt.Errorf("success_count=%d failure_count=%d pending_count=%d", parsed.SuccessCount, parsed.FailureCount, parsed.PendingCount)
		}
		if parsed.SuccessCount > 0 || parsed.PendingCount > 0 || len(parsed.AddedTorrentIDs) > 0 {
			return parsed, nil
		}
	}

	return parsed, errors.New(trimmed)
}

const maxTorrentFetchBytes = 50 << 20

type fetchedTorrent struct {
	filename  string
	body      []byte
	infoHash  string
	magnetURL string
}

// isProwlarrTorrentURL reports whether the URL belongs to the configured
// Prowlarr origin. Only those URLs are fetched by Librarr and re-uploaded as
// torrent bytes: a remote qBittorrent cannot reach a locally hosted Prowlarr,
// but it can fetch public tracker URLs (e.g. Nyaa) itself, so those are passed
// through unchanged and never receive the Prowlarr API key.
func (q *QBittorrentClient) isProwlarrTorrentURL(rawURL string) bool {
	if q.cfg == nil || q.cfg.ProwlarrURL == "" {
		return false
	}
	_, err := netutil.ValidateSameOriginHTTPURL(rawURL, q.cfg.ProwlarrURL)
	return err == nil
}

func (q *QBittorrentClient) fetchTorrent(rawURL string) (fetchedTorrent, error) {
	if q.cfg == nil || q.cfg.ProwlarrURL == "" {
		return fetchedTorrent{}, fmt.Errorf("Prowlarr URL is not configured")
	}
	u, err := netutil.ValidateSameOriginHTTPURL(rawURL, q.cfg.ProwlarrURL)
	if err != nil {
		return fetchedTorrent{}, fmt.Errorf("torrent URL rejected: %w", err)
	}
	base, err := netutil.ValidateSameOriginHTTPURL(q.cfg.ProwlarrURL, q.cfg.ProwlarrURL)
	if err != nil {
		return fetchedTorrent{}, fmt.Errorf("configured Prowlarr URL invalid: %w", err)
	}
	// Build the request from the configured Prowlarr origin plus the validated
	// path and query, so the fetched host and port always come from config.
	requestURL := base.Scheme + "://" + base.Host + u.RequestURI()

	var redirectedMagnet string

	timeout := q.fetchTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	client := &http.Client{
		Timeout:   timeout,
		Transport: q.client.Transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("stopped after 5 redirects")
			}

			// Prowlarr may respond to its HTTP download URL with a
			// magnet URI instead of returning a .torrent file.
			//
			// Do not attempt to make an HTTP request to the magnet.
			// Capture it and let AddTorrent submit it directly to qBittorrent.
			if strings.EqualFold(req.URL.Scheme, "magnet") {
				magnet := req.URL.String()

				if infoHashFromMagnet(magnet) == "" {
					return fmt.Errorf("torrent redirect rejected: invalid magnet URL")
				}

				redirectedMagnet = magnet
				return http.ErrUseLastResponse
			}

			if _, err := netutil.ValidateSameOriginHTTPURL(
				req.URL.String(),
				q.cfg.ProwlarrURL,
			); err != nil {
				req.Header.Del("X-Api-Key")
				return fmt.Errorf("torrent redirect rejected: %w", err)
			}

			q.applyProwlarrAuth(req)

			return nil
		},
	}

	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return fetchedTorrent{}, fmt.Errorf("torrent URL request: %w", err)
	}
	q.applyProwlarrAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		return fetchedTorrent{}, fmt.Errorf("fetch torrent: %s", netutil.SanitizeSensitiveText(err.Error()))
	}
	defer resp.Body.Close()

	if redirectedMagnet != "" {
		return fetchedTorrent{
			magnetURL: redirectedMagnet,
		}, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTorrentFetchBytes+1))
	if err != nil {
		return fetchedTorrent{}, fmt.Errorf("read torrent response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fetchedTorrent{}, fmt.Errorf("fetch torrent HTTP %d: %s", resp.StatusCode, summarizeHTTPBody(body))
	}
	if len(body) > maxTorrentFetchBytes {
		return fetchedTorrent{}, fmt.Errorf("torrent response too large")
	}
	infoHash, err := validateTorrentBytes(resp.Header.Get("Content-Type"), body)
	if err != nil {
		return fetchedTorrent{}, err
	}

	return fetchedTorrent{
		filename: torrentFilename(resp, rawURL),
		body:     body,
		infoHash: infoHash,
	}, nil
}

func (q *QBittorrentClient) applyProwlarrAuth(req *http.Request) {
	if q.cfg == nil || q.cfg.ProwlarrAPIKey == "" {
		req.Header.Del("X-Api-Key")
		return
	}
	if _, err := netutil.ValidateSameOriginHTTPURL(req.URL.String(), q.cfg.ProwlarrURL); err != nil {
		req.Header.Del("X-Api-Key")
		return
	}
	req.Header.Set("X-Api-Key", q.cfg.ProwlarrAPIKey)
}

func validateTorrentBytes(contentType string, body []byte) (string, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return "", fmt.Errorf("torrent response was empty")
	}

	ct := strings.ToLower(contentType)
	switch {
	case strings.Contains(ct, "text/html"):
		return "", fmt.Errorf("torrent response was HTML, not a .torrent file")
	case strings.Contains(ct, "application/json"):
		return "", fmt.Errorf("torrent response was JSON, not a .torrent file: %s", summarizeHTTPBody(body))
	}

	if trimmed[0] == '<' {
		return "", fmt.Errorf("torrent response was HTML, not a .torrent file")
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		return "", fmt.Errorf("torrent response was JSON, not a .torrent file: %s", summarizeHTTPBody(body))
	}
	infoHash, err := torrentInfoHash(trimmed)
	if err != nil {
		return "", fmt.Errorf("torrent response did not contain valid bencode: %w", err)
	}
	return infoHash, nil
}

// torrentInfoHash computes the BitTorrent v1 info hash from the exact raw
// bencoded info dictionary bytes. Re-encoding the dictionary could change its
// byte ordering and therefore produce a different hash.
func torrentInfoHash(body []byte) (string, error) {
	pos := 0
	if len(body) == 0 || body[pos] != 'd' {
		return "", fmt.Errorf("torrent metadata is not a dictionary")
	}
	pos++
	var info []byte
	for pos < len(body) && body[pos] != 'e' {
		key, err := parseBencodeStringAt(body, &pos)
		if err != nil {
			return "", err
		}
		start := pos
		end, err := skipBencodeValue(body, pos, 0)
		if err != nil {
			return "", err
		}
		if string(key) == "info" {
			if body[start] != 'd' {
				return "", fmt.Errorf("info value is not a dictionary")
			}
			info = body[start:end]
		}
		pos = end
	}
	if pos >= len(body) || body[pos] != 'e' {
		return "", fmt.Errorf("torrent metadata has an unterminated top-level dictionary")
	}
	pos++
	if pos != len(body) {
		return "", fmt.Errorf("torrent metadata has trailing data")
	}
	if len(info) == 0 {
		return "", fmt.Errorf("torrent metadata is missing an info dictionary")
	}

	sum := sha1.Sum(info)
	return hex.EncodeToString(sum[:]), nil
}

func parseBencodeStringAt(data []byte, pos *int) ([]byte, error) {
	if *pos >= len(data) || data[*pos] < '0' || data[*pos] > '9' {
		return nil, fmt.Errorf("invalid bencode string")
	}
	length := 0
	for *pos < len(data) && data[*pos] >= '0' && data[*pos] <= '9' {
		digit := int(data[*pos] - '0')
		if length > (len(data)-digit)/10 {
			return nil, fmt.Errorf("bencode string length is too large")
		}
		length = length*10 + digit
		(*pos)++
	}
	if *pos >= len(data) || data[*pos] != ':' {
		return nil, fmt.Errorf("invalid bencode string length")
	}
	(*pos)++
	if length > len(data)-*pos {
		return nil, fmt.Errorf("bencode string exceeds response")
	}
	start := *pos
	*pos += length
	return data[start:*pos], nil
}

func skipBencodeValue(data []byte, pos, depth int) (int, error) {
	if depth > 64 || pos >= len(data) {
		return 0, fmt.Errorf("invalid bencode nesting")
	}
	switch data[pos] {
	case 'i':
		pos++
		start := pos
		if pos < len(data) && data[pos] == '-' {
			pos++
		}
		if pos >= len(data) || data[pos] < '0' || data[pos] > '9' {
			return 0, fmt.Errorf("invalid bencode integer")
		}
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
		if (pos-start > 1 && data[start] == '0') ||
			(pos-start > 2 && data[start] == '-' && data[start+1] == '0') {
			return 0, fmt.Errorf("invalid bencode integer")
		}
		if pos >= len(data) || data[pos] != 'e' {
			return 0, fmt.Errorf("unterminated bencode integer")
		}
		return pos + 1, nil
	case 'l':
		pos++
		for pos < len(data) && data[pos] != 'e' {
			var err error
			pos, err = skipBencodeValue(data, pos, depth+1)
			if err != nil {
				return 0, err
			}
		}
		if pos >= len(data) {
			return 0, fmt.Errorf("unterminated bencode list")
		}
		return pos + 1, nil
	case 'd':
		pos++
		for pos < len(data) && data[pos] != 'e' {
			if _, err := parseBencodeStringAt(data, &pos); err != nil {
				return 0, err
			}
			var err error
			pos, err = skipBencodeValue(data, pos, depth+1)
			if err != nil {
				return 0, err
			}
		}
		if pos >= len(data) {
			return 0, fmt.Errorf("unterminated bencode dictionary")
		}
		return pos + 1, nil
	default:
		_, err := parseBencodeStringAt(data, &pos)
		return pos, err
	}
}

func summarizeHTTPBody(body []byte) string {
	s := strings.TrimSpace(string(body))
	s = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, s)
	if len(s) > 160 {
		return s[:160]
	}
	if s == "" {
		return "empty response"
	}
	return s
}

func torrentFilename(resp *http.Response, rawURL string) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if filename := cleanTorrentFilename(params["filename"]); filename != "" {
				return filename
			}
			if filename := cleanTorrentFilename(params["filename*"]); filename != "" {
				return filename
			}
		}
	}
	if u, err := url.Parse(rawURL); err == nil {
		if filename := cleanTorrentFilename(path.Base(u.Path)); filename != "" {
			return filename
		}
	}
	return "download.torrent"
}

func cleanTorrentFilename(filename string) string {
	filename = strings.TrimSpace(path.Base(filename))
	if filename == "." || filename == "/" || filename == "" {
		return ""
	}
	filename = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 || r == '/' || r == '\\' {
			return -1
		}
		return r
	}, filename)
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return ""
	}
	if !strings.HasSuffix(strings.ToLower(filename), ".torrent") {
		filename += ".torrent"
	}
	return filename
}

func isHTTPURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && (u.Scheme == "http" || u.Scheme == "https")
}

func isMagnetURL(raw string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), "magnet:")
}

func infoHashFromMagnet(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "magnet") {
		return ""
	}
	for _, xt := range u.Query()["xt"] {
		const prefix = "urn:btih:"
		if strings.HasPrefix(strings.ToLower(xt), prefix) {
			return normalizeInfoHash(xt[len(prefix):])
		}
	}
	return ""
}

func firstNonEmptyHash(values ...string) string {
	for _, v := range values {
		if h := normalizeInfoHash(v); h != "" {
			return h
		}
	}
	return ""
}

func normalizeInfoHash(hash string) string {
	hash = strings.TrimSpace(hash)
	hash = strings.TrimPrefix(strings.ToLower(hash), "urn:btih:")
	if len(hash) == 32 {
		if decoded, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(hash)); err == nil && len(decoded) == sha1.Size {
			return hex.EncodeToString([]byte(decoded))
		}
	}
	return hash
}

func (q *QBittorrentClient) verifyTorrentAdded(expectedInfoHash string, addedIDs []string, title, category string) error {
	ctx, cancel := context.WithTimeout(context.Background(), q.verifyTimeout)
	defer cancel()
	return q.verifyTorrentAddedContext(ctx, expectedInfoHash, addedIDs, title, category)
}

func (q *QBittorrentClient) verifyTorrentAddedContext(ctx context.Context, expectedInfoHash string, addedIDs []string, title, category string) error {
	expected := firstNonEmptyHash(expectedInfoHash)
	for _, id := range addedIDs {
		if expected == "" {
			expected = normalizeInfoHash(id)
			break
		}
	}

	var lastErr error
	ticker := time.NewTicker(q.verifyInterval)
	defer ticker.Stop()
	for {
		torrents, err := q.getTorrentsContext(ctx, "")
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				lastErr = err
			}
		} else {
			slog.Debug("qBittorrent verification poll", "expected_hash", netutil.SanitizeLogValue(expected), "added_ids", netutil.SanitizeLogValue(strings.Join(addedIDs, ",")), "expected_category", netutil.SanitizeLogValue(category), "expected_title", netutil.SanitizeLogValue(title), "candidate_count", len(torrents))
			if torrentListContains(torrents, expected, addedIDs, title, category) {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				if lastErr != nil {
					return fmt.Errorf("verify torrent in qBittorrent: %w", lastErr)
				}
				return torrentVerificationTimeoutError{}
			}
			return fmt.Errorf("verify torrent in qBittorrent: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func torrentListContains(torrents []TorrentInfo, expectedInfoHash string, addedIDs []string, title, expectedCategory string) bool {
	expectedInfoHash = normalizeInfoHash(expectedInfoHash)
	added := make(map[string]bool, len(addedIDs))
	for _, id := range addedIDs {
		if h := normalizeInfoHash(id); h != "" {
			added[h] = true
		}
	}
	title = strings.ToLower(strings.TrimSpace(title))

	for _, t := range torrents {
		hash := normalizeInfoHash(t.Hash)
		if expectedInfoHash != "" && hash == expectedInfoHash {
			slog.Debug("qBittorrent verification candidate accepted", "hash", netutil.SanitizeLogValue(hash), "name", netutil.SanitizeLogValue(t.Name), "category", netutil.SanitizeLogValue(t.Category), "save_path", netutil.SanitizeLogValue(t.SavePath), "reason", "matching info hash", "expected_category", netutil.SanitizeLogValue(expectedCategory))
			return true
		}
		if added[hash] {
			slog.Debug("qBittorrent verification candidate accepted", "hash", netutil.SanitizeLogValue(hash), "name", netutil.SanitizeLogValue(t.Name), "category", netutil.SanitizeLogValue(t.Category), "save_path", netutil.SanitizeLogValue(t.SavePath), "reason", "matching add response id", "expected_category", netutil.SanitizeLogValue(expectedCategory))
			return true
		}
		if expectedInfoHash == "" && len(added) == 0 && title != "" && strings.EqualFold(strings.TrimSpace(t.Name), title) {
			slog.Debug("qBittorrent verification candidate accepted", "hash", netutil.SanitizeLogValue(hash), "name", netutil.SanitizeLogValue(t.Name), "category", netutil.SanitizeLogValue(t.Category), "save_path", netutil.SanitizeLogValue(t.SavePath), "reason", "exact title fallback", "expected_category", netutil.SanitizeLogValue(expectedCategory))
			return true
		}
		slog.Debug("qBittorrent verification candidate rejected", "hash", netutil.SanitizeLogValue(hash), "name", netutil.SanitizeLogValue(t.Name), "category", netutil.SanitizeLogValue(t.Category), "save_path", netutil.SanitizeLogValue(t.SavePath), "reason", verificationRejectReason(expectedInfoHash, added, title), "expected_category", netutil.SanitizeLogValue(expectedCategory))
	}
	return false
}

func verificationRejectReason(expectedHash string, added map[string]bool, title string) string {
	if expectedHash != "" {
		return "info hash did not match"
	}
	if len(added) > 0 {
		return "add response id did not match"
	}
	if title != "" {
		return "title did not match"
	}
	return "no verification identifier available"
}

// TorrentInfo represents a torrent from the qBittorrent API.
type TorrentInfo struct {
	Name        string  `json:"name"`
	ContentPath string  `json:"content_path"`
	SavePath    string  `json:"save_path"`
	Hash        string  `json:"hash"`
	State       string  `json:"state"`
	Progress    float64 `json:"progress"`
	TotalSize   int64   `json:"total_size"`
	DlSpeed     int64   `json:"dlspeed"`
	Category    string  `json:"category"`
}

// TorrentFile represents a file inside a torrent.
type TorrentFile struct {
	Name string `json:"name"`
}

// GetTorrentFiles returns the list of files for a given torrent hash.
func (q *QBittorrentClient) GetTorrentFiles(hash string) ([]TorrentFile, error) {
	data := url.Values{
		"hash": {hash},
	}
	resp, err := q.doRequest("GET", "/api/v2/torrents/files", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("get torrent files HTTP %d", resp.StatusCode)
	}

	var files []TorrentFile
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read torrent files response: %w", err)
	}
	if err := jsonUnmarshal(body, &files); err != nil {
		return nil, err
	}
	return files, nil
}

// GetTorrents returns torrents, optionally filtered by category.
func (q *QBittorrentClient) GetTorrents(category string) ([]TorrentInfo, error) {
	return q.getTorrentsContext(context.Background(), category)
}

func (q *QBittorrentClient) getTorrentsContext(ctx context.Context, category string) ([]TorrentInfo, error) {
	data := url.Values{}
	if category != "" {
		data.Set("category", category)
	}

	resp, err := q.doRequestContext(ctx, "GET", "/api/v2/torrents/info", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("get torrents HTTP %d", resp.StatusCode)
	}

	var torrents []TorrentInfo
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read torrents response: %w", err)
	}
	if err := jsonUnmarshal(body, &torrents); err != nil {
		return nil, err
	}
	return torrents, nil
}

// DeleteTorrent removes a torrent by hash.
func (q *QBittorrentClient) DeleteTorrent(hash string, deleteFiles bool) error {
	data := url.Values{
		"hashes":      {hash},
		"deleteFiles": {fmt.Sprintf("%v", deleteFiles)},
	}

	resp, err := q.doRequest("POST", "/api/v2/torrents/delete", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("delete torrent HTTP %d", resp.StatusCode)
	}
	return nil
}

// Diagnose checks qBittorrent connectivity and authentication.
func (q *QBittorrentClient) Diagnose() map[string]interface{} {
	if !q.cfg.HasQBittorrent() {
		return map[string]interface{}{"success": false, "error": "qBittorrent not configured"}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.ensureAuth(); err != nil {
		return map[string]interface{}{"success": false, "error": err.Error()}
	}

	req, err := http.NewRequest("GET", q.cfg.QBUrl+"/api/v2/app/version", nil)
	if err != nil {
		return map[string]interface{}{"success": false, "error": "invalid qBittorrent URL"}
	}
	for _, c := range q.cookies {
		req.AddCookie(c)
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return map[string]interface{}{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]interface{}{"success": false, "error": "read qBittorrent version response failed"}
	}
	if resp.StatusCode != 200 {
		return map[string]interface{}{"success": false, "error": fmt.Sprintf("HTTP %d", resp.StatusCode)}
	}

	return map[string]interface{}{
		"success": true,
		"version": strings.TrimSpace(string(body)),
	}
}

// MapTorrentStatus maps qBittorrent state strings to Librarr status strings.
func MapTorrentStatus(state string) string {
	switch state {
	case "downloading", "stalledDL", "metaDL", "forcedDL":
		return "downloading"
	case "pausedDL":
		return "paused"
	case "queuedDL":
		return "queued"
	case "uploading", "stalledUP", "pausedUP", "queuedUP", "stoppedUP":
		return "completed"
	case "checkingDL", "checkingUP":
		return "checking"
	default:
		return state
	}
}
