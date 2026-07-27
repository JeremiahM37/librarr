package download

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamie75/librarr/internal/config"
)

// DelugeClient wraps the Deluge JSON-RPC API (Web UI).
type DelugeClient struct {
	cfg    *config.Config
	client *http.Client
	mu     sync.Mutex
	cookie string
	reqID  atomic.Int64
}

// NewDelugeClient creates a new Deluge API client.
func NewDelugeClient(cfg *config.Config) *DelugeClient {
	return &DelugeClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// delugeRequest is the JSON-RPC request format for Deluge.
type delugeRequest struct {
	ID     int64         `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

// delugeResponse is the JSON-RPC response format.
type delugeResponse struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *delugeError    `json:"error"`
}

type delugeError struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// Login authenticates with the Deluge Web UI.
func (d *DelugeClient) Login() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.login()
}

func (d *DelugeClient) login() error {
	resp, err := d.call("auth.login", []interface{}{d.cfg.DelugePassword})
	if err != nil {
		return fmt.Errorf("deluge login: %w", err)
	}

	var result bool
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return fmt.Errorf("deluge login: invalid response")
	}
	if !result {
		return fmt.Errorf("deluge login: authentication failed")
	}
	return nil
}

// AddTorrent adds a torrent to Deluge by URL or magnet link.
func (d *DelugeClient) AddTorrent(url string, options map[string]interface{}) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if options == nil {
		options = map[string]interface{}{}
	}

	resp, err := d.call("core.add_torrent_url", []interface{}{url, options})
	if err != nil {
		// Try re-auth once.
		if loginErr := d.login(); loginErr != nil {
			return "", fmt.Errorf("deluge re-auth: %w", loginErr)
		}
		resp, err = d.call("core.add_torrent_url", []interface{}{url, options})
		if err != nil {
			return "", err
		}
	}

	if resp.Error != nil {
		return "", fmt.Errorf("deluge: %s", resp.Error.Message)
	}

	var torrentID string
	if err := json.Unmarshal(resp.Result, &torrentID); err != nil {
		return "", fmt.Errorf("deluge: invalid torrent ID response")
	}
	return torrentID, nil
}

// GetTorrentStatus returns status info for a specific torrent.
func (d *DelugeClient) GetTorrentStatus(torrentID string, keys []string) (map[string]interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	resp, err := d.call("core.get_torrent_status", []interface{}{torrentID, keys})
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("deluge: %s", resp.Error.Message)
	}

	var status map[string]interface{}
	if err := json.Unmarshal(resp.Result, &status); err != nil {
		return nil, err
	}
	return status, nil
}

// RemoveTorrent removes a torrent from Deluge.
func (d *DelugeClient) RemoveTorrent(torrentID string, removeData bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	resp, err := d.call("core.remove_torrent", []interface{}{torrentID, removeData})
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("deluge: %s", resp.Error.Message)
	}
	return nil
}

// Diagnose tests the Deluge connection.
func (d *DelugeClient) Diagnose() map[string]interface{} {
	err := d.Login()
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		}
	}
	return map[string]interface{}{
		"success": true,
	}
}

func (d *DelugeClient) call(method string, params []interface{}) (*delugeResponse, error) {
	id := d.reqID.Add(1)
	reqBody := delugeRequest{
		ID:     id,
		Method: method,
		Params: params,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/json", d.cfg.DelugeURL)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.cookie != "" {
		req.Header.Set("Cookie", d.cookie)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("deluge request: %w", err)
	}
	defer resp.Body.Close()

	// Store session cookie.
	for _, c := range resp.Cookies() {
		if c.Name == "_session_id" {
			d.cookie = fmt.Sprintf("_session_id=%s", c.Value)
		}
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var delugeResp delugeResponse
	if err := json.Unmarshal(respBody, &delugeResp); err != nil {
		return nil, fmt.Errorf("deluge: invalid JSON response")
	}

	return &delugeResp, nil
}
