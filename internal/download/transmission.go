package download

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

// TransmissionClient wraps the Transmission RPC API and satisfies TorrentClient.
//
// Transmission has no persistent login; instead the RPC endpoint answers the
// first request with 409 Conflict plus an X-Transmission-Session-Id header that
// must be echoed on subsequent requests (CSRF protection). We cache that id and
// transparently re-handshake when it expires. Librarr scopes its torrents with
// the qBittorrent-style category mapped onto a Transmission label, so listing
// and clearing never touch the user's other torrents.
type TransmissionClient struct {
	cfg       *config.Config
	client    *http.Client
	mu        sync.Mutex
	sessionID string
}

// NewTransmissionClient creates a new Transmission RPC client.
func NewTransmissionClient(cfg *config.Config) *TransmissionClient {
	return &TransmissionClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// Name identifies this client in logs and the TorrentClient interface.
func (t *TransmissionClient) Name() string { return "transmission" }

// transmissionRequest is the RPC request format.
type transmissionRequest struct {
	Method    string                 `json:"method"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// transmissionResponse is the RPC response format.
type transmissionResponse struct {
	Result    string                 `json:"result"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// transmissionTorrent is the typed view of a torrent-get result row.
type transmissionTorrent struct {
	ID           int      `json:"id"`
	Name         string   `json:"name"`
	Status       int      `json:"status"`
	PercentDone  float64  `json:"percentDone"`
	TotalSize    int64    `json:"totalSize"`
	RateDownload int64    `json:"rateDownload"`
	HashString   string   `json:"hashString"`
	DownloadDir  string   `json:"downloadDir"`
	Error        int      `json:"error"`
	Labels       []string `json:"labels"`
}

// AddTorrent submits a torrent URL or magnet link to Transmission. The Librarr
// category is attached as a Transmission label so the torrent can be found again
// by GetTorrents; savePath becomes the download-dir. Both fall back to the
// configured qBittorrent-equivalent defaults when empty, matching the
// qBittorrent client's behaviour for drop-in parity.
func (t *TransmissionClient) AddTorrent(torrentURL, title, savePath, category, expectedInfoHash string) error {
	if savePath == "" {
		savePath = t.cfg.QBSavePath
	}
	if category == "" {
		category = t.cfg.QBCategory
	}

	args := map[string]interface{}{
		"filename": torrentURL,
		"paused":   false,
	}
	if savePath != "" {
		args["download-dir"] = savePath
	}
	if category != "" {
		args["labels"] = []string{category}
	}

	t.mu.Lock()
	resp, err := t.call("torrent-add", args)
	t.mu.Unlock()
	if err != nil {
		return err
	}
	if resp.Result != "success" {
		return fmt.Errorf("transmission add torrent: %s", resp.Result)
	}

	// A "torrent-duplicate" result is not an error — the torrent is already
	// present, which is the desired end state.
	slog.Info("torrent added to Transmission", "title", title, "category", category)
	return nil
}

// GetTorrents returns Librarr-scoped torrents, optionally filtered to a single
// category (label). State is reported using the qBittorrent vocabulary so
// callers can keep using MapTorrentStatus.
func (t *TransmissionClient) GetTorrents(category string) ([]TorrentInfo, error) {
	fields := []string{
		"id", "name", "status", "percentDone", "totalSize",
		"rateDownload", "hashString", "downloadDir", "error", "labels",
	}

	t.mu.Lock()
	resp, err := t.call("torrent-get", map[string]interface{}{"fields": fields})
	t.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if resp.Result != "success" {
		return nil, fmt.Errorf("transmission get torrents: %s", resp.Result)
	}

	raw, ok := resp.Arguments["torrents"]
	if !ok {
		return nil, nil
	}
	rawJSON, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var rows []transmissionTorrent
	if err := json.Unmarshal(rawJSON, &rows); err != nil {
		return nil, err
	}

	var out []TorrentInfo
	for _, r := range rows {
		// Only surface torrents Librarr added (those carrying our label).
		if !hasLabel(r.Labels, category) {
			continue
		}
		out = append(out, TorrentInfo{
			Name:        r.Name,
			Hash:        r.HashString,
			State:       mapTransmissionState(r.Status, r.PercentDone, r.Error),
			Progress:    r.PercentDone,
			TotalSize:   r.TotalSize,
			DlSpeed:     r.RateDownload,
			Category:    firstLabel(r.Labels),
			SavePath:    r.DownloadDir,
			ContentPath: path.Join(r.DownloadDir, r.Name),
		})
	}
	return out, nil
}

// GetTorrentFiles lists the files inside a torrent identified by hash.
func (t *TransmissionClient) GetTorrentFiles(hash string) ([]TorrentFile, error) {
	t.mu.Lock()
	resp, err := t.call("torrent-get", map[string]interface{}{
		"ids":    []string{hash},
		"fields": []string{"files"},
	})
	t.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if resp.Result != "success" {
		return nil, fmt.Errorf("transmission get files: %s", resp.Result)
	}

	raw, ok := resp.Arguments["torrents"]
	if !ok {
		return nil, nil
	}
	rawJSON, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Files []struct {
			Name string `json:"name"`
		} `json:"files"`
	}
	if err := json.Unmarshal(rawJSON, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var files []TorrentFile
	for _, f := range rows[0].Files {
		files = append(files, TorrentFile{Name: f.Name})
	}
	return files, nil
}

// DeleteTorrent removes a torrent by hash. Transmission accepts SHA-1 hash
// strings directly in the "ids" array.
func (t *TransmissionClient) DeleteTorrent(hash string, deleteFiles bool) error {
	t.mu.Lock()
	resp, err := t.call("torrent-remove", map[string]interface{}{
		"ids":               []string{hash},
		"delete-local-data": deleteFiles,
	})
	t.mu.Unlock()
	if err != nil {
		return err
	}
	if resp.Result != "success" {
		return fmt.Errorf("transmission remove torrent: %s", resp.Result)
	}
	return nil
}

// Diagnose tests the Transmission connection for the settings "Test" button.
func (t *TransmissionClient) Diagnose() map[string]interface{} {
	if !t.cfg.HasTransmission() {
		return map[string]interface{}{"success": false, "error": "Transmission not configured"}
	}

	t.mu.Lock()
	resp, err := t.call("session-get", nil)
	t.mu.Unlock()
	if err != nil {
		return map[string]interface{}{"success": false, "error": err.Error()}
	}
	if resp.Result != "success" {
		return map[string]interface{}{"success": false, "error": resp.Result}
	}

	result := map[string]interface{}{"success": true}
	if v, ok := resp.Arguments["version"].(string); ok {
		result["version"] = v
	}
	return result
}

// call performs an RPC call, transparently handling the 409 session-id
// handshake. Callers must hold t.mu.
func (t *TransmissionClient) call(method string, args map[string]interface{}) (*transmissionResponse, error) {
	reqBody := transmissionRequest{Method: method, Arguments: args}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/transmission/rpc", t.cfg.TransmissionURL)
	resp, err := t.doRequest(url, body)
	if err != nil {
		return nil, err
	}

	// Handle 409 Conflict — Transmission requires a session-id header that it
	// hands back on the first request. Cache it and replay once.
	if resp.StatusCode == http.StatusConflict {
		t.sessionID = resp.Header.Get("X-Transmission-Session-Id")
		resp.Body.Close()
		if t.sessionID == "" {
			return nil, fmt.Errorf("transmission: 409 but no session ID header")
		}
		resp, err = t.doRequest(url, body)
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("transmission: authentication failed")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("transmission: HTTP %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var trResp transmissionResponse
	if err := json.Unmarshal(respBody, &trResp); err != nil {
		return nil, fmt.Errorf("transmission: invalid JSON response")
	}
	return &trResp, nil
}

func (t *TransmissionClient) doRequest(url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.sessionID != "" {
		req.Header.Set("X-Transmission-Session-Id", t.sessionID)
	}
	if t.cfg.TransmissionUser != "" {
		req.SetBasicAuth(t.cfg.TransmissionUser, t.cfg.TransmissionPass)
	}
	return t.client.Do(req)
}

// mapTransmissionState converts a Transmission status code into the qBittorrent
// state vocabulary so MapTorrentStatus works uniformly across backends.
//
// Transmission status codes: 0 stopped, 1 check-wait, 2 checking,
// 3 download-wait, 4 downloading, 5 seed-wait, 6 seeding.
//
// Transmission error codes: 0 ok, 1 tracker warning, 2 tracker error,
// 3 local error. Only a local error (3 — disk full, permissions, missing
// data) is fatal. Tracker warning/error are NOT: the torrent keeps working
// via DHT/PEX/webseeds and routinely shows code 1/2 on public torrents with a
// flaky tracker. Treating those as "error" would both mislabel healthy
// torrents and let ClearFinished remove a still-downloading torrent, so we
// fall through to the status-based mapping for codes 1 and 2.
func mapTransmissionState(status int, percentDone float64, errCode int) string {
	if errCode == 3 {
		return "error"
	}
	switch status {
	case 0: // stopped — distinguish finished vs paused-incomplete
		if percentDone >= 1 {
			return "pausedUP"
		}
		return "pausedDL"
	case 1, 2: // verifying
		return "checkingDL"
	case 3: // queued to download
		return "queuedDL"
	case 4: // downloading
		return "downloading"
	case 5: // queued to seed
		return "queuedUP"
	case 6: // seeding (treated as completed, like qBittorrent "uploading")
		return "uploading"
	default:
		return "downloading"
	}
}

// hasLabel reports whether labels contains want. An empty want matches any
// torrent (used for "list everything Librarr could see").
func hasLabel(labels []string, want string) bool {
	if want == "" {
		return true
	}
	for _, l := range labels {
		if l == want {
			return true
		}
	}
	return false
}

func firstLabel(labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return ""
}
