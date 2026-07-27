package download

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/jamie75/librarr/internal/config"
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
	NzoID  string `json:"nzo_id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Size   string `json:"size"`
}

// SABnzbdHistoryResponse is the response from mode=history.
type SABnzbdHistoryResponse struct {
	History struct {
		Slots []SABnzbdHistorySlot `json:"slots"`
	} `json:"history"`
}

// AddNZB sends an NZB URL to SABnzbd for download.
func (s *SABnzbdClient) AddNZB(nzbURL, title string) (string, error) {
	if !s.cfg.HasSABnzbd() {
		return "", fmt.Errorf("SABnzbd not configured")
	}

	params := url.Values{
		"mode":    {"addurl"},
		"name":    {nzbURL},
		"nzbname": {title},
		"apikey":  {s.cfg.SABnzbdAPIKey},
		"output":  {"json"},
	}
	if s.cfg.SABnzbdCategory != "" {
		params.Set("cat", s.cfg.SABnzbdCategory)
	}

	reqURL := fmt.Sprintf("%s/api?%s", s.cfg.SABnzbdURL, params.Encode())
	resp, err := s.client.Get(reqURL)
	if err != nil {
		return "", fmt.Errorf("sabnzbd addurl: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("sabnzbd HTTP %d: %s", resp.StatusCode, string(body))
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
	if !s.cfg.HasSABnzbd() {
		return nil, fmt.Errorf("SABnzbd not configured")
	}

	params := url.Values{
		"mode":   {"queue"},
		"apikey": {s.cfg.SABnzbdAPIKey},
		"output": {"json"},
	}

	reqURL := fmt.Sprintf("%s/api?%s", s.cfg.SABnzbdURL, params.Encode())
	resp, err := s.client.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("sabnzbd queue HTTP %d", resp.StatusCode)
	}

	var queueResp SABnzbdQueueResponse
	if err := json.NewDecoder(resp.Body).Decode(&queueResp); err != nil {
		return nil, err
	}

	return queueResp.Queue.Slots, nil
}

// GetHistory returns recent completed downloads.
func (s *SABnzbdClient) GetHistory(limit int) ([]SABnzbdHistorySlot, error) {
	if !s.cfg.HasSABnzbd() {
		return nil, fmt.Errorf("SABnzbd not configured")
	}

	params := url.Values{
		"mode":   {"history"},
		"apikey": {s.cfg.SABnzbdAPIKey},
		"output": {"json"},
		"limit":  {fmt.Sprintf("%d", limit)},
	}

	reqURL := fmt.Sprintf("%s/api?%s", s.cfg.SABnzbdURL, params.Encode())
	resp, err := s.client.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("sabnzbd history HTTP %d", resp.StatusCode)
	}

	var histResp SABnzbdHistoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&histResp); err != nil {
		return nil, err
	}

	return histResp.History.Slots, nil
}

// DeleteNZB removes a download from SABnzbd queue.
func (s *SABnzbdClient) DeleteNZB(nzoID string) error {
	if !s.cfg.HasSABnzbd() {
		return fmt.Errorf("SABnzbd not configured")
	}

	params := url.Values{
		"mode":   {"queue"},
		"name":   {"delete"},
		"value":  {nzoID},
		"apikey": {s.cfg.SABnzbdAPIKey},
		"output": {"json"},
	}

	reqURL := fmt.Sprintf("%s/api?%s", s.cfg.SABnzbdURL, params.Encode())
	resp, err := s.client.Get(reqURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

// Diagnose checks SABnzbd connectivity.
func (s *SABnzbdClient) Diagnose() map[string]interface{} {
	if !s.cfg.HasSABnzbd() {
		return map[string]interface{}{"success": false, "error": "SABnzbd not configured"}
	}

	params := url.Values{
		"mode":   {"version"},
		"apikey": {s.cfg.SABnzbdAPIKey},
		"output": {"json"},
	}

	reqURL := fmt.Sprintf("%s/api?%s", s.cfg.SABnzbdURL, params.Encode())
	resp, err := s.client.Get(reqURL)
	if err != nil {
		return map[string]interface{}{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return map[string]interface{}{"success": false, "error": fmt.Sprintf("HTTP %d", resp.StatusCode)}
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
