package download

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestSABnzbdClient_AddNZB(t *testing.T) {
	t.Run("not configured", func(t *testing.T) {
		cfg := &config.Config{}
		sab := NewSABnzbdClient(cfg)
		_, err := sab.AddNZB("http://example.com/test.nzb", "Test")
		if err == nil {
			t.Error("expected error when not configured")
		}
	})

	t.Run("successful add", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			if q.Get("mode") != "addurl" {
				t.Errorf("expected mode=addurl, got %s", q.Get("mode"))
			}
			if q.Get("apikey") != "test-key" {
				t.Errorf("expected apikey=test-key, got %s", q.Get("apikey"))
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  true,
				"nzo_ids": []string{"SABnzbd_nzo_12345"},
			})
		}))
		defer server.Close()

		cfg := &config.Config{SABnzbdURL: server.URL, SABnzbdAPIKey: "test-key"}
		sab := NewSABnzbdClient(cfg)

		nzoID, err := sab.AddNZB("http://example.com/test.nzb", "Test Book")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nzoID != "SABnzbd_nzo_12345" {
			t.Errorf("expected nzo_id, got %s", nzoID)
		}
	})

	t.Run("add failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": false,
				"error":  "Invalid NZB",
			})
		}))
		defer server.Close()

		cfg := &config.Config{SABnzbdURL: server.URL, SABnzbdAPIKey: "key"}
		sab := NewSABnzbdClient(cfg)

		_, err := sab.AddNZB("http://example.com/bad.nzb", "Bad")
		if err == nil {
			t.Error("expected error on failed add")
		}
	})
}

func TestSABnzbdClient_GetQueue(t *testing.T) {
	t.Run("not configured", func(t *testing.T) {
		cfg := &config.Config{}
		sab := NewSABnzbdClient(cfg)
		_, err := sab.GetQueue()
		if err == nil {
			t.Error("expected error when not configured")
		}
	})

	t.Run("successful queue", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(SABnzbdQueueResponse{
				Queue: struct {
					Slots []SABnzbdSlot `json:"slots"`
				}{
					Slots: []SABnzbdSlot{
						{NzoID: "nzo_1", Filename: "Book.nzb", Status: "Downloading", Percentage: "50"},
					},
				},
			})
		}))
		defer server.Close()

		cfg := &config.Config{SABnzbdURL: server.URL, SABnzbdAPIKey: "key"}
		sab := NewSABnzbdClient(cfg)

		slots, err := sab.GetQueue()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(slots) != 1 {
			t.Fatalf("expected 1 slot, got %d", len(slots))
		}
		if slots[0].Filename != "Book.nzb" {
			t.Errorf("expected filename Book.nzb, got %s", slots[0].Filename)
		}
	})
}

func TestSABnzbdClient_Diagnose(t *testing.T) {
	t.Run("not configured", func(t *testing.T) {
		cfg := &config.Config{}
		sab := NewSABnzbdClient(cfg)
		result := sab.Diagnose()
		if result["success"].(bool) {
			t.Error("expected success=false when not configured")
		}
	})

	t.Run("configured and reachable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]string{"version": "3.7.2"})
		}))
		defer server.Close()

		cfg := &config.Config{SABnzbdURL: server.URL, SABnzbdAPIKey: "key"}
		sab := NewSABnzbdClient(cfg)

		result := sab.Diagnose()
		if !result["success"].(bool) {
			t.Errorf("expected success=true, got error: %v", result["error"])
		}
		if result["version"] != "3.7.2" {
			t.Errorf("expected version 3.7.2, got %v", result["version"])
		}
	})
}
