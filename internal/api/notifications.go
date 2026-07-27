package api

import (
	"net/http"
	"strconv"

	"github.com/jamie75/librarr/internal/models"
)

// handleGetNotifications handles GET /api/notifications — current user's notifications.
func (s *Server) handleGetNotifications(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
		return
	}

	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	notifications, err := s.db.GetNotifications(userID, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to get notifications",
		})
		return
	}

	if notifications == nil {
		notifications = []models.Notification{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"notifications": notifications,
	})
}

// handleUnreadCount handles GET /api/notifications/unread — count of unread notifications.
func (s *Server) handleUnreadCount(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
		return
	}

	count, err := s.db.CountUnreadNotifications(userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to count notifications",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"count":   count,
	})
}

// handleMarkRead handles PUT /api/notifications/{id}/read — mark one notification as read.
func (s *Server) handleMarkRead(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
		return
	}

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid notification ID",
		})
		return
	}

	if err := s.db.MarkNotificationRead(id, userID); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Notification not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// handleMarkAllRead handles PUT /api/notifications/read-all — mark all notifications as read.
func (s *Server) handleMarkAllRead(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
		return
	}

	if err := s.db.MarkAllNotificationsRead(userID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to mark notifications as read",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// handleDeleteNotification handles DELETE /api/notifications/{id}.
func (s *Server) handleDeleteNotification(w http.ResponseWriter, r *http.Request) {
	userID := getUserIDFromContext(r)
	if userID == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Authentication required",
		})
		return
	}

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid notification ID",
		})
		return
	}

	if err := s.db.DeleteNotification(id, userID); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Notification not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}
