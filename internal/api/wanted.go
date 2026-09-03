package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/quality"
)

// --- Wanted list (the wishlist, now a persistent monitored catalogue) ---

// decorateWanted fills the derived fields the UI shows: state, profile name,
// and whether the current file meets the profile cutoff.
func (s *Server) decorateWanted(items []models.WishlistItem) []models.WishlistItem {
	cache := map[string]struct {
		name string
		p    quality.Profile
	}{}
	for i := range items {
		it := &items[i]
		key := strconv.FormatInt(it.QualityProfileID, 10) + ":" + it.MediaType
		entry, ok := cache[key]
		if !ok {
			rec := s.db.ResolveQualityProfile(it.QualityProfileID, it.MediaType)
			entry.name = rec.Name
			entry.p = rec.Profile()
			cache[key] = entry
		}
		it.ProfileName = entry.name
		it.CutoffMet = it.LibraryItemID != 0 && entry.p.CutoffMet(it.CurrentFormat)
		it.State = quality.State(entry.p, it.Monitored, it.ActiveJobID != "", it.LibraryItemID != 0, it.CurrentFormat, s.cfg.AutoUpgradeEnabled)
	}
	return items
}

func (s *Server) handleGetWishlist(w http.ResponseWriter, _ *http.Request) {
	items, err := s.db.GetWishlist()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	if items == nil {
		items = []models.WishlistItem{}
	}
	items = s.decorateWanted(items)
	counts := map[string]int{}
	for _, it := range items {
		counts[it.State]++
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items":            items,
		"counts":           counts,
		"upgrades_enabled": s.cfg.AutoUpgradeEnabled,
	})
}

func (s *Server) handleAddWishlist(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title            string `json:"title"`
		Author           string `json:"author"`
		MediaType        string `json:"media_type"`
		QualityProfileID int64  `json:"quality_profile_id"`
		Monitored        *bool  `json:"monitored"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}

	if req.Title == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Title is required",
		})
		return
	}
	// Cap user-supplied strings so a misbehaving client can't bloat the DB.
	if len(req.Title) > 500 || len(req.Author) > 500 || len(req.MediaType) > 50 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Field exceeds maximum length",
		})
		return
	}
	if req.QualityProfileID != 0 {
		if _, err := s.db.GetQualityProfile(req.QualityProfileID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": "Unknown quality profile",
			})
			return
		}
	}
	monitored := true
	if req.Monitored != nil {
		monitored = *req.Monitored
	}

	id, err := s.db.AddWishlistItemWithOptions(models.WishlistItem{
		Title: req.Title, Author: req.Author, MediaType: req.MediaType,
		QualityProfileID: req.QualityProfileID, Monitored: monitored, Source: "manual",
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"id":      id,
	})
}

// handleUpdateWishlist changes a wanted item's monitored flag or profile.
func (s *Server) handleUpdateWishlist(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid ID"})
		return
	}
	var req struct {
		Monitored        *bool  `json:"monitored"`
		QualityProfileID *int64 `json:"quality_profile_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid request body"})
		return
	}
	if req.QualityProfileID != nil && *req.QualityProfileID != 0 {
		if _, err := s.db.GetQualityProfile(*req.QualityProfileID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Unknown quality profile"})
			return
		}
	}
	if err := s.db.UpdateWishlistItem(id, req.Monitored, req.QualityProfileID); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	item, err := s.db.GetWishlistItem(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "wishlist item not found"})
		return
	}
	items := s.decorateWanted([]models.WishlistItem{*item})
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "item": items[0]})
}

// handleSearchWishlistItem runs the scheduler's decision for one item now.
// Body: {"dry_run": true} reports the choice without grabbing.
func (s *Server) handleSearchWishlistItem(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "Invalid ID"})
		return
	}
	var req struct {
		DryRun bool `json:"dry_run"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // an empty body means a real search
	}
	if s.scheduler == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"success": false, "error": "Scheduler not initialized"})
		return
	}
	outcome, err := s.scheduler.SearchItem(r.Context(), id, req.DryRun)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, sql.ErrNoRows) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	item, _ := s.db.GetWishlistItem(id)
	resp := map[string]interface{}{"success": true, "outcome": outcome, "dry_run": req.DryRun}
	if item != nil {
		resp["item"] = s.decorateWanted([]models.WishlistItem{*item})[0]
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleDeleteWishlist(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	if err := s.db.DeleteWishlistItem(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}
