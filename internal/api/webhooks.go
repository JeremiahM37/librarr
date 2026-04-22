package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/webhook"
)

// handleGetWebhooks returns all configured webhooks.
func (s *Server) handleGetWebhooks(w http.ResponseWriter, r *http.Request) {
	configs, err := s.db.GetWebhookConfigs()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to load webhooks",
		})
		return
	}
	if configs == nil {
		configs = []webhook.Config{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"webhooks": configs,
	})
}

// handleCreateWebhook adds a new webhook configuration.
func (s *Server) handleCreateWebhook(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var cfg webhook.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid JSON",
		})
		return
	}

	if cfg.URL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "URL is required",
		})
		return
	}
	if err := validateTestURL(cfg.URL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}
	if cfg.Type == "" {
		cfg.Type = "generic"
	} else {
		// Validate webhook type
		if err := ValidateEnumValue(cfg.Type, []string{"generic", "discord", "slack", "matrix"}); err != nil {
			cfg.Type = "generic" // Default to generic if invalid
		}
	}
	if cfg.Events == "" {
		cfg.Events = "*"
	} else {
		// Validate events string - should be comma-separated or *
		if cfg.Events != "*" && len(cfg.Events) > 200 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": "Events string too long",
			})
			return
		}
	}
	if cfg.Name == "" {
		cfg.Name = cfg.Type + " webhook"
	} else {
		// Validate webhook name
		if err := ValidateStringLength(cfg.Name, 1, 100); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": "Webhook name: " + err.Error(),
			})
			return
		}
	}

	id, err := s.db.CreateWebhookConfig(&cfg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": "Failed to create webhook",
		})
		return
	}

	cfg.ID = id

	// Refresh sender configs.
	s.refreshWebhookSender()

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"webhook": cfg,
	})
}

// handleDeleteWebhook removes a webhook config by ID.
func (s *Server) handleDeleteWebhook(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "Invalid webhook ID",
		})
		return
	}

	if err := s.db.DeleteWebhookConfig(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false, "error": "Webhook not found",
		})
		return
	}

	s.refreshWebhookSender()

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// handleTestWebhook sends a test notification to a webhook URL.
func (s *Server) handleTestWebhook(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req struct {
		URL  string `json:"url"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "URL is required",
		})
		return
	}
	if req.Type == "" {
		req.Type = "generic"
	}
	if err := validateTestURL(req.URL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	if err := s.webhookSender.Test(req.URL, req.Type); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "message": "Test sent"})
}

// refreshWebhookSender reloads webhook configs from DB into the sender.
func (s *Server) refreshWebhookSender() {
	if s.webhookSender == nil {
		return
	}
	configs, err := s.db.GetWebhookConfigs()
	if err != nil {
		return
	}
	s.webhookSender.SetConfigs(configs)
}

// sendWebhook is a convenience method to send a webhook notification.
func (s *Server) sendWebhook(event webhook.EventType, title, message, status string, extra map[string]interface{}) {
	if s.webhookSender == nil {
		return
	}
	s.webhookSender.Send(webhook.Payload{
		Event:   event,
		Title:   title,
		Message: message,
		Status:  status,
		Extra:   extra,
	})
}
