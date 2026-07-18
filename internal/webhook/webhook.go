// Package webhook delivers librarr event notifications to configured
// webhook endpoints such as Discord.
package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// EventType represents the type of webhook event.
type EventType string

const (
	EventDownloadComplete EventType = "download_complete"
	EventDownloadFailed   EventType = "download_failed"
	EventRequestApproved  EventType = "request_approved"
	EventRequestCompleted EventType = "request_completed"
	EventRequestFailed    EventType = "request_failed"
	EventSchedulerMatch   EventType = "scheduler_match"
	EventSeriesMissing    EventType = "series_missing"
	EventInfo             EventType = "info"
)

// Config represents a webhook endpoint configuration.
type Config struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Type    string `json:"type"` // "discord" or "generic"
	Enabled bool   `json:"enabled"`
	Events  string `json:"events"` // comma-separated event types, or "*" for all
}

// Payload is the data sent to webhooks.
type Payload struct {
	Event     EventType              `json:"event"`
	Title     string                 `json:"title"`
	Message   string                 `json:"message"`
	Status    string                 `json:"status"` // completed, failed, info
	Timestamp string                 `json:"timestamp"`
	Extra     map[string]interface{} `json:"extra,omitempty"`
}

// Sender manages sending webhook notifications.
type Sender struct {
	client  *http.Client
	configs []Config
	mu      sync.RWMutex
	sem     chan struct{}
}

const maxConcurrentWebhooks = 10

// NewSender creates a new webhook sender.
func NewSender() *Sender {
	return &Sender{
		client: &http.Client{Timeout: 10 * time.Second},
		sem:    make(chan struct{}, maxConcurrentWebhooks),
	}
}

// SetConfigs replaces the current webhook configurations.
func (s *Sender) SetConfigs(configs []Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = configs
}

// GetConfigs returns a copy of current configurations.
func (s *Sender) GetConfigs() []Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Config, len(s.configs))
	copy(result, s.configs)
	return result
}

// Send dispatches a webhook payload asynchronously to all matching configs.
func (s *Sender) Send(payload Payload) {
	s.mu.RLock()
	configs := make([]Config, len(s.configs))
	copy(configs, s.configs)
	s.mu.RUnlock()

	payload.Timestamp = time.Now().UTC().Format(time.RFC3339)

	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		if cfg.Events != "*" && !eventMatches(cfg.Events, string(payload.Event)) {
			continue
		}
		go func(c Config) {
			// Block until a delivery slot is free rather than dropping the
			// notification. The semaphore still caps concurrency, but a burst
			// now backpressures instead of silently losing webhooks.
			s.sem <- struct{}{}
			defer func() { <-s.sem }()
			s.send(c, payload)
		}(cfg)
	}
}

// Test sends a test payload to a specific webhook URL.
func (s *Sender) Test(url, webhookType string) error {
	cfg := Config{URL: url, Type: webhookType, Enabled: true, Events: "*"}
	payload := Payload{
		Event:     EventInfo,
		Title:     "Librarr Webhook Test",
		Message:   "This is a test notification from Librarr.",
		Status:    "info",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	return s.send(cfg, payload)
}

func (s *Sender) send(cfg Config, payload Payload) error {
	var body []byte
	var err error

	if cfg.Type == "discord" {
		body, err = json.Marshal(buildDiscordEmbed(payload))
	} else {
		body, err = json.Marshal(payload)
	}
	if err != nil {
		slog.Error("webhook marshal error", "url", cfg.URL, "error", err)
		return err
	}

	req, err := http.NewRequest("POST", cfg.URL, bytes.NewReader(body))
	if err != nil {
		slog.Error("webhook request error", "url", cfg.URL, "error", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Librarr/2.0")

	resp, err := s.client.Do(req)
	if err != nil {
		slog.Error("webhook send error", "url", cfg.URL, "error", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		slog.Warn("webhook returned error status", "url", cfg.URL, "status", resp.StatusCode)
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	slog.Debug("webhook sent", "url", cfg.URL, "event", payload.Event)
	return nil
}

// Discord embed format.
type discordMessage struct {
	Embeds []discordEmbed `json:"embeds"`
}

type discordEmbed struct {
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Color       int            `json:"color"`
	Timestamp   string         `json:"timestamp"`
	Footer      *discordFooter `json:"footer,omitempty"`
}

type discordFooter struct {
	Text string `json:"text"`
}

func buildDiscordEmbed(p Payload) discordMessage {
	color := 3447003 // blue (info)
	switch p.Status {
	case "completed":
		color = 3066993 // green
	case "failed":
		color = 15158332 // red
	}

	return discordMessage{
		Embeds: []discordEmbed{
			{
				Title:       p.Title,
				Description: p.Message,
				Color:       color,
				Timestamp:   p.Timestamp,
				Footer:      &discordFooter{Text: "Librarr"},
			},
		},
	}
}

func eventMatches(events, event string) bool {
	for _, e := range splitEvents(events) {
		if e == event {
			return true
		}
	}
	return false
}

func splitEvents(events string) []string {
	var result []string
	start := 0
	for i := 0; i < len(events); i++ {
		if events[i] == ',' {
			e := trimSpace(events[start:i])
			if e != "" {
				result = append(result, e)
			}
			start = i + 1
		}
	}
	e := trimSpace(events[start:])
	if e != "" {
		result = append(result, e)
	}
	return result
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && s[start] == ' ' {
		start++
	}
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}
