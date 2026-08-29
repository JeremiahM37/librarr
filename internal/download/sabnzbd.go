package download

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

// SABnzbdClient wraps the SABnzbd API.
type SABnzbdClient struct {
	cfg    *config.Config
	client *http.Client
}

// NewSABnzbdClient creates a new SABnzbd API client.
func NewSABnzbdClient(cfg *config.Config) *SABnzbdClient {
	return &SABnzbdClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// SABnzbdSlot represents a slot in the SABnzbd queue.
type SABnzbdSlot struct {
	NzoID      string  `json:"nzo_id"`
	Filename   string  `json:"filename"`
	Status     string  `json:"status"`
	Percentage string  `json:"percentage"`
	Size       string  `json:"size"`
	Timeleft   string  `json:"timeleft"`
	MBLeft     float64 `json:"mbleft"`
	MB         float64 `json:"mb"`
}

// SABnzbdQueueResponse is the response from mode=queue.
type SABnzbdQueueResponse struct {
	Queue struct {
		Slots []SABnzbdSlot `json:"slots"`
	} `json:"queue"`
}

// SABnzbdHistorySlot represents a completed download in history.
type SABnzbdHistorySlot struct {
	NzoID       string `json:"nzo_id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Size        string `json:"size"`
	Category    string `json:"category"`
	Storage     string `json:"storage"`      // final on-disk path once complete
	FailMessage string `json:"fail_message"` // populated when Status == "Failed"
}

// SABnzbdHistoryResponse is the response from mode=history.
type SABnzbdHistoryResponse struct {
	History struct {
		Slots []SABnzbdHistorySlot `json:"slots"`
	} `json:"history"`
}

// doRequest issues a GET to the SABnzbd API for the given mode, merging in
// apikey/output automatically, and returns the raw response body once a 200
// status is confirmed. Every public method below built its own url.Values,
// GET, and status check inline; this is the one place that sequence lives
// now, mirroring the doRequest/doRequestContext helper qbittorrent.go uses
// for the same purpose (SABnzbd's apikey-per-request auth needs none of
// that helper's cookie/session/retry handling, so this is its own, simpler
// equivalent rather than a literal reuse of qBittorrent's).
func (s *SABnzbdClient) doRequest(mode string, extra url.Values) ([]byte, error) {
	if !s.cfg.HasSABnzbd() {
		return nil, fmt.Errorf("SABnzbd not configured")
	}

	params := url.Values{
		"mode":   {mode},
		"apikey": {s.cfg.SABnzbdAPIKey},
		"output": {"json"},
	}
	for k, vs := range extra {
		for _, v := range vs {
			params.Add(k, v)
		}
	}

	reqURL := fmt.Sprintf("%s/api?%s", s.cfg.SABnzbdURL, params.Encode())
	resp, err := s.client.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("sabnzbd %s: %w", mode, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("sabnzbd %s HTTP %d: %s", mode, resp.StatusCode, string(body))
	}
	return body, nil
}

// AddNZB sends an NZB URL to SABnzbd for download.
func (s *SABnzbdClient) AddNZB(nzbURL, title string) (string, error) {
	extra := url.Values{
		"name":    {nzbURL},
		"nzbname": {title},
	}
	if s.cfg.SABnzbdCategory != "" {
		extra.Set("cat", s.cfg.SABnzbdCategory)
	}

	body, err := s.doRequest("addurl", extra)
	if err != nil {
		return "", err
	}

	var result struct {
		Status bool     `json:"status"`
		NzoIDs []string `json:"nzo_ids"`
		Error  string   `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("sabnzbd response parse: %w", err)
	}

	if !result.Status {
		return "", fmt.Errorf("sabnzbd add failed: %s", result.Error)
	}

	nzoID := ""
	if len(result.NzoIDs) > 0 {
		nzoID = result.NzoIDs[0]
	}

	slog.Info("NZB added to SABnzbd", "title", title, "nzo_id", nzoID)
	return nzoID, nil
}

// GetQueue returns the current download queue.
func (s *SABnzbdClient) GetQueue() ([]SABnzbdSlot, error) {
	body, err := s.doRequest("queue", nil)
	if err != nil {
		return nil, err
	}

	var queueResp SABnzbdQueueResponse
	if err := json.Unmarshal(body, &queueResp); err != nil {
		return nil, err
	}

	return queueResp.Queue.Slots, nil
}

// GetHistory returns recent completed downloads.
func (s *SABnzbdClient) GetHistory(limit int) ([]SABnzbdHistorySlot, error) {
	body, err := s.doRequest("history", url.Values{"limit": {fmt.Sprintf("%d", limit)}})
	if err != nil {
		return nil, err
	}

	var histResp SABnzbdHistoryResponse
	if err := json.Unmarshal(body, &histResp); err != nil {
		return nil, err
	}

	return histResp.History.Slots, nil
}

// DeleteNZB removes a download from the SABnzbd queue.
func (s *SABnzbdClient) DeleteNZB(nzoID string) error {
	body, err := s.doRequest("queue", url.Values{"name": {"delete"}, "value": {nzoID}})
	if err != nil {
		return err
	}

	var result struct {
		Status bool   `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		// SABnzbd's queue-delete reply shape has drifted before; don't fail
		// the delete over a body we can't parse, just stop pretending we
		// verified it.
		return fmt.Errorf("sabnzbd delete response parse: %w", err)
	}
	if !result.Status {
		return fmt.Errorf("sabnzbd delete failed: %s", result.Error)
	}
	return nil
}

// Diagnose checks SABnzbd connectivity.
func (s *SABnzbdClient) Diagnose() map[string]interface{} {
	body, err := s.doRequest("version", nil)
	if err != nil {
		return map[string]interface{}{"success": false, "error": err.Error()}
	}

	var result struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return map[string]interface{}{"success": true, "version": string(body)}
	}

	return map[string]interface{}{
		"success": true,
		"version": result.Version,
	}
}
