package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/netutil"
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
	if err := netutil.ValidateOutboundURL(cfg.URL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}
	if cfg.Type == "" {
		cfg.Type = "generic"
	}
	if cfg.Events == "" {
		cfg.Events = "*"
	}
	if cfg.Name == "" {
		cfg.Name = cfg.Type + " webhook"
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

	if err := netutil.ValidateOutboundURL(req.URL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": err.Error(),
		})
		return
	}

	if err := s.webhookSender.Test(req.URL, req.Type); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]interface{}{
			"success": false, "error": "Webhook delivery failed",
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
