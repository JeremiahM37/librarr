package download

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

// QBittorrentClient wraps the qBittorrent Web API.
type QBittorrentClient struct {
	cfg           *config.Config
	client        *http.Client
	mu            sync.Mutex
	authenticated bool
	cookies       []*http.Cookie
	banUntil      time.Time
	nextLogin     time.Time
	backoffSec    int
	lastError     string
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
		backoffSec: 3,
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

	body, _ := io.ReadAll(resp.Body)
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
func (q *QBittorrentClient) AddTorrent(torrentURL, title, savePath, category string) error {
	if savePath == "" {
		savePath = q.cfg.QBSavePath
	}
	if category == "" {
		category = q.cfg.QBCategory
	}

	data := url.Values{
		"urls":     {torrentURL},
		"savepath": {savePath},
		"category": {category},
	}

	resp, err := q.doRequest("POST", "/api/v2/torrents/add", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("add torrent HTTP %d: %s", resp.StatusCode, string(body))
	}

	if err := parseQBittorrentAddTorrentResponse(body); err != nil {
		return fmt.Errorf("add torrent failed: %w", err)
	}

	slog.Info("torrent added to qBittorrent", "title", title)
	return nil
}

type qbittorrentAddTorrentResponse struct {
	AddedTorrentIDs []string `json:"added_torrent_ids"`
	SuccessCount    int      `json:"success_count"`
	FailureCount    int      `json:"failure_count"`
	PendingCount    int      `json:"pending_count"`
	Error           string   `json:"error"`
}

func parseQBittorrentAddTorrentResponse(body []byte) error {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "Ok." {
		return nil
	}

	var parsed qbittorrentAddTorrentResponse
	if err := json.Unmarshal(body, &parsed); err == nil {
		if parsed.Error != "" {
			return errors.New(parsed.Error)
		}
		if parsed.FailureCount > 0 {
			return fmt.Errorf("success_count=%d failure_count=%d pending_count=%d", parsed.SuccessCount, parsed.FailureCount, parsed.PendingCount)
		}
		if parsed.SuccessCount > 0 || parsed.PendingCount > 0 || len(parsed.AddedTorrentIDs) > 0 {
			return nil
		}
	}

	return errors.New(trimmed)
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
	body, _ := io.ReadAll(resp.Body)
	if err := jsonUnmarshal(body, &files); err != nil {
		return nil, err
	}
	return files, nil
}

// GetTorrents returns torrents, optionally filtered by category.
func (q *QBittorrentClient) GetTorrents(category string) ([]TorrentInfo, error) {
	data := url.Values{}
	if category != "" {
		data.Set("category", category)
	}

	resp, err := q.doRequest("GET", "/api/v2/torrents/info", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("get torrents HTTP %d", resp.StatusCode)
	}

	var torrents []TorrentInfo
	body, _ := io.ReadAll(resp.Body)
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

	req, _ := http.NewRequest("GET", q.cfg.QBUrl+"/api/v2/app/version", nil)
	for _, c := range q.cookies {
		req.AddCookie(c)
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return map[string]interface{}{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
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
