package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

const (
	maxRequestTitleLength       = 200
	maxRequestAuthorLength      = 120
	maxRequestDescriptionLength = 4000
	maxRequestSeriesLength      = 200
	maxRequestYearLength        = 16
)

// generateRequestID returns a random hex ID for a request.
func generateRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		slog.Error("failed to generate request ID with crypto random", "error", err)
		h := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
		copy(b, h[:16])
	}
	return hex.EncodeToString(b)
}

// handleCreateRequest handles POST /api/requests — any authenticated user.
func (s *Server) handleCreateRequest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title          string `json:"title"`
		Author         string `json:"author"`
		BookType       string `json:"book_type"`
		CoverURL       string `json:"cover_url"`
		Description    string `json:"description"`
		Year           string `json:"year"`
		SeriesName     string `json:"series_name"`
		SeriesPosition string `json:"series_position"`
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
	req.Title = strings.TrimSpace(req.Title)
	req.Author = strings.TrimSpace(req.Author)
	req.Description = strings.TrimSpace(req.Description)
	req.SeriesName = strings.TrimSpace(req.SeriesName)
	req.SeriesPosition = strings.TrimSpace(req.SeriesPosition)
	req.Year = strings.TrimSpace(req.Year)
	
	// Validate all metadata fields with comprehensive checks
	if err := ValidateStringLength(req.Title, 1, maxRequestTitleLength); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Title: " + err.Error(),
		})
		return
	}
	if err := ValidateMetadataString(req.Title, maxRequestTitleLength); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Title: " + err.Error(),
		})
		return
	}
	
	if req.Author != "" {
		if err := ValidateStringLength(req.Author, 1, maxRequestAuthorLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Author: " + err.Error(),
			})
			return
		}
		if err := ValidateMetadataString(req.Author, maxRequestAuthorLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Author: " + err.Error(),
			})
			return
		}
	}
	
	if len(req.Description) > maxRequestDescriptionLength {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Description too long (max %d characters)", maxRequestDescriptionLength),
		})
		return
	}
	if req.Description != "" {
		if err := ValidateMetadataString(req.Description, maxRequestDescriptionLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Description: " + err.Error(),
			})
			return
		}
	}
	
	// Validate series and year fields
	if req.SeriesName != "" {
		if err := ValidateStringLength(req.SeriesName, 1, maxRequestSeriesLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Series name: " + err.Error(),
			})
			return
		}
		if err := ValidateMetadataString(req.SeriesName, maxRequestSeriesLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Series name: " + err.Error(),
			})
			return
		}
	}
	
	if req.SeriesPosition != "" {
		if err := ValidateStringLength(req.SeriesPosition, 1, maxRequestSeriesLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Series position: " + err.Error(),
			})
			return
		}
	}
	
	if req.Year != "" {
		if err := ValidateStringLength(req.Year, 1, maxRequestYearLength); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Year: " + err.Error(),
			})
			return
		}
	}

	bookType := req.BookType
	if bookType == "" {
		bookType = "ebook"
	}
	
	// Validate book type enum
	if err := ValidateEnumValue(bookType, []string{"ebook", "audiobook", "manga"}); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "book_type must be ebook, audiobook, or manga",
		})
		return
	}
	
	// Validate cover URL if provided
	if req.CoverURL != "" {
		if err := ValidateURLSafety(req.CoverURL, DefaultURLValidationConfig()); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid cover URL: " + err.Error(),
			})
			return
		}
	}

	userID := getUserIDFromContext(r)
	username, _ := r.Context().Value(ctxUsername).(string)

	now := time.Now()
	request := &models.Request{
		ID:             generateRequestID(),
		UserID:         userID,
		Username:       username,
		Title:          req.Title,
		Author:         req.Author,
		BookType:       bookType,
		Status:         "pending",
		CoverURL:       req.CoverURL,
		Description:    req.Description,
		Year:           req.Year,
		SeriesName:     req.SeriesName,
		SeriesPosition: req.SeriesPosition,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.db.CreateRequest(request); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to create request",
		})
		return
	}

	_ = s.db.LogEvent("request_created", request.Title, fmt.Sprintf("Request by %s for %s", username, bookType), nil, request.ID)
	s.db.LogActivity(username, "request_created", request.Title, fmt.Sprintf("Request by %s for %s", username, bookType))

	slog.Info("request created", "id", request.ID, "title", request.Title, "user", username, "type", bookType)

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"request": request,
	})
}

// handleListRequests handles GET /api/requests — users see own, admins see all.
func (s *Server) handleListRequests(w http.ResponseWriter, r *http.Request) {
	role, _ := r.Context().Value(ctxUserRole).(string)
	userID := getUserIDFromContext(r)

	status := r.URL.Query().Get("status")
	limit := QueryIntBounded(r, "limit", 50, 1, 200)
	offset := QueryIntBounded(r, "offset", 0, 0, 10000)

	// Admins see all requests; regular users see only their own.
	filterUserID := userID
	if role == "admin" {
		filterUserID = 0
		// But allow admin to filter by user_id if they want.
		if uid := r.URL.Query().Get("user_id"); uid != "" {
			if v, err := strconv.ParseInt(uid, 10, 64); err == nil {
				filterUserID = v
			}
		}
	}

	requests, err := s.db.ListRequests(filterUserID, status, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to list requests",
		})
		return
	}

	total, _ := s.db.CountRequests(filterUserID, status)

	if requests == nil {
		requests = []models.Request{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"requests": requests,
		"total":    total,
	})
}

// handleGetRequest handles GET /api/requests/{id}.
func (s *Server) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	role, _ := r.Context().Value(ctxUserRole).(string)
	userID := getUserIDFromContext(r)

	request, err := s.db.GetRequest(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Request not found",
		})
		return
	}

	// Users can only see their own requests.
	if role != "admin" && request.UserID != userID {
		writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Access denied",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"request": request,
	})
}

// handleApproveRequest handles PUT /api/requests/{id}/approve — admin only.
func (s *Server) handleApproveRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	request, err := s.db.GetRequest(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Request not found",
		})
		return
	}

	if request.Status != "pending" && request.Status != "failed" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Cannot approve request in status '%s'", request.Status),
		})
		return
	}

	request.Status = "approved"
	request.UpdatedAt = time.Now()
	if err := s.db.UpdateRequest(request); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to update request",
		})
		return
	}

	// Notify the requester.
	s.createNotification(request.UserID, "request_approved", request.Title,
		fmt.Sprintf("Your request for \"%s\" has been approved and will be searched.", request.Title),
		request.ID)

	// Start the search + download pipeline in the background.
	go s.processApprovedRequest(request)

	slog.Info("request approved", "id", request.ID, "title", request.Title)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"request": request,
	})
}

// handleCancelRequest handles PUT /api/requests/{id}/cancel.
func (s *Server) handleCancelRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	role, _ := r.Context().Value(ctxUserRole).(string)
	userID := getUserIDFromContext(r)

	request, err := s.db.GetRequest(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Request not found",
		})
		return
	}

	// Users can cancel their own; admins can cancel any.
	if role != "admin" && request.UserID != userID {
		writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Access denied",
		})
		return
	}

	if request.Status == "completed" || request.Status == "cancelled" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Cannot cancel request in status '%s'", request.Status),
		})
		return
	}

	request.Status = "cancelled"
	request.UpdatedAt = time.Now()
	if err := s.db.UpdateRequest(request); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to cancel request",
		})
		return
	}

	slog.Info("request cancelled", "id", request.ID, "title", request.Title)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"request": request,
	})
}

// handleRetryRequest handles PUT /api/requests/{id}/retry — admin only.
func (s *Server) handleRetryRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	request, err := s.db.GetRequest(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Request not found",
		})
		return
	}

	if request.Status != "failed" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Cannot retry request in status '%s'", request.Status),
		})
		return
	}

	request.Status = "approved"
	request.RetryCount++
	request.AttentionNote = ""
	request.UpdatedAt = time.Now()
	if err := s.db.UpdateRequest(request); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to update request",
		})
		return
	}

	go s.processApprovedRequest(request)

	slog.Info("request retried", "id", request.ID, "title", request.Title, "retry", request.RetryCount)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"request": request,
	})
}

// handleSelectResult handles PUT /api/requests/{id}/select — admin selects a search result.
func (s *Server) handleSelectResult(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var body struct {
		ResultIndex int `json:"result_index"` // index into search results
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}

	request, err := s.db.GetRequest(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Request not found",
		})
		return
	}

	if request.Status != "pending" && request.Status != "failed" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Cannot select result for request in status '%s'", request.Status),
		})
		return
	}

	request.SelectedResultID = strconv.Itoa(body.ResultIndex)
	request.Status = "approved"
	request.UpdatedAt = time.Now()
	if err := s.db.UpdateRequest(request); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to update request",
		})
		return
	}

	// Notify the requester.
	s.createNotification(request.UserID, "request_approved", request.Title,
		fmt.Sprintf("Your request for \"%s\" has been approved.", request.Title),
		request.ID)

	go s.processApprovedRequest(request)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"request": request,
	})
}

// handleDeleteRequest handles DELETE /api/requests/{id} — admin only.
func (s *Server) handleDeleteRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.db.DeleteRequest(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Request not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// processApprovedRequest runs the search + download pipeline for an approved request.
func (s *Server) processApprovedRequest(req *models.Request) {
	// Determine which search tab to use.
	tab := "main"
	switch req.BookType {
	case "audiobook":
		tab = "audiobook"
	case "manga":
		tab = "manga"
	}

	// Build search query.
	query := req.Title
	if req.Author != "" {
		query = req.Title + " " + req.Author
	}
	if req.SearchQuery != "" {
		query = req.SearchQuery
	}

	// Update status to searching.
	req.Status = "searching"
	req.SearchQuery = query
	req.UpdatedAt = time.Now()
	_ = s.db.UpdateRequest(req)

	slog.Info("searching for request", "id", req.ID, "query", query, "tab", tab)

	// Run search.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	results, _ := s.searchMgr.Search(ctx, tab, query)
	if len(results) == 0 {
		req.Status = "failed"
		req.AttentionNote = "No search results found"
		req.UpdatedAt = time.Now()
		_ = s.db.UpdateRequest(req)

		s.createNotification(req.UserID, "request_failed", req.Title,
			fmt.Sprintf("No results found for \"%s\". An admin may retry with different terms.", req.Title),
			req.ID)

		slog.Warn("no results for request", "id", req.ID, "query", query)
		return
	}

	// Pick the best result. If admin selected a specific index, use that.
	var chosen models.SearchResult
	if req.SelectedResultID != "" {
		idx, err := strconv.Atoi(req.SelectedResultID)
		if err == nil && idx >= 0 && idx < len(results) {
			chosen = results[idx]
		} else {
			chosen = pickBestResult(results, req.BookType)
		}
	} else {
		chosen = pickBestResult(results, req.BookType)
	}

	slog.Info("selected result for request", "id", req.ID, "source", chosen.Source, "title", chosen.Title)

	// Update status to downloading.
	req.Status = "downloading"
	req.UpdatedAt = time.Now()
	_ = s.db.UpdateRequest(req)

	// Build a download request and use the existing download infrastructure.
	dlReq := models.DownloadRequest{
		Source:           chosen.Source,
		Title:            chosen.Title,
		DownloadURL:      chosen.DownloadURL,
		MagnetURL:        chosen.MagnetURL,
		InfoHash:         chosen.InfoHash,
		GUID:             chosen.GUID,
		MD5:              chosen.MD5,
		URL:              chosen.URL,
		AbbURL:           chosen.AbbURL,
		MediaType:        req.BookType,
		DownloadProtocol: chosen.DownloadProtocol,
		Force:            true, // bypass duplicate check for requests
	}

	// Determine download type.
	source := s.searchMgr.GetSource(chosen.Source)
	downloadType := "direct"
	if source != nil {
		downloadType = source.DownloadType()
	} else if chosen.Source == "torrent" || chosen.Source == "audiobook" || chosen.Source == "prowlarr_manga" {
		downloadType = "torrent"
	}

	// Check if NZB.
	if chosen.DownloadProtocol == "nzb" && s.cfg.HasSABnzbd() {
		nzoID, err := s.downloadMgr.StartNZBDownload(chosen.DownloadURL, chosen.Title)
		if err != nil {
			s.failRequest(req, fmt.Sprintf("NZB download failed: %v", err))
			return
		}
		req.DownloadID = nzoID
		req.Status = "processing"
		req.UpdatedAt = time.Now()
		_ = s.db.UpdateRequest(req)
		// NZB downloads are managed by SABnzbd; we mark as processing.
		// A background watcher could update later; for now, mark completed.
		s.completeRequest(req)
		return
	}

	switch downloadType {
	case "torrent":
		url := resolveTorrentURLForRequest(dlReq, chosen)
		if url == "" {
			s.failRequest(req, "No torrent download URL available")
			return
		}

		savePath, category := s.resolveSavePathAndCategory(req.BookType)
		if err := s.downloadMgr.StartTorrentDownload(url, chosen.Title, savePath, category); err != nil {
			s.failRequest(req, fmt.Sprintf("Torrent download failed: %v", err))
			return
		}
		req.Status = "processing"
		req.UpdatedAt = time.Now()
		_ = s.db.UpdateRequest(req)
		// Torrent completion is handled by the torrent watcher.
		s.completeRequest(req)

	default:
		// Direct download (Anna's Archive, Gutenberg, etc.)
		if dlReq.MD5 != "" {
			job, err := s.downloadMgr.StartAnnasDownload(dlReq.MD5, dlReq.Title)
			if err != nil {
				s.failRequest(req, fmt.Sprintf("Download failed: %v", err))
				return
			}
			req.DownloadID = job.ID
			req.Status = "processing"
			req.UpdatedAt = time.Now()
			_ = s.db.UpdateRequest(req)

			// Wait for the job to finish.
			s.waitForJob(req, job.ID)
		} else if dlReq.DownloadURL != "" || dlReq.URL != "" {
			fileURL := dlReq.DownloadURL
			if fileURL == "" {
				fileURL = dlReq.URL
			}
			job, err := s.downloadMgr.StartDirectDownload(fileURL, dlReq.Title, dlReq.Source, "")
			if err != nil {
				s.failRequest(req, fmt.Sprintf("Download failed: %v", err))
				return
			}
			req.DownloadID = job.ID
			req.Status = "processing"
			req.UpdatedAt = time.Now()
			_ = s.db.UpdateRequest(req)

			s.waitForJob(req, job.ID)
		} else {
			s.failRequest(req, "No download method available for selected result")
		}
	}
}

// waitForJob polls a download job until it completes or fails, then updates the request.
func (s *Server) waitForJob(req *models.Request, jobID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	timeout := time.After(30 * time.Minute)

	for {
		select {
		case <-ticker.C:
			job, err := s.db.GetJob(jobID)
			if err != nil {
				continue
			}
			switch job.Status {
			case "completed":
				s.completeRequest(req)
				return
			case "error", "dead_letter":
				s.failRequest(req, fmt.Sprintf("Download failed: %s", job.Error))
				return
			}
		case <-timeout:
			s.failRequest(req, "Download timed out after 30 minutes")
			return
		}
	}
}

// completeRequest marks a request as completed and notifies the user.
func (s *Server) completeRequest(req *models.Request) {
	req.Status = "completed"
	req.UpdatedAt = time.Now()
	_ = s.db.UpdateRequest(req)

	s.createNotification(req.UserID, "request_completed", req.Title,
		fmt.Sprintf("Your request for \"%s\" has been downloaded and is now available.", req.Title),
		req.ID)

	_ = s.db.LogEvent("request_completed", req.Title, fmt.Sprintf("Request by %s completed", req.Username), nil, req.ID)

	slog.Info("request completed", "id", req.ID, "title", req.Title)
}

// failRequest marks a request as failed and notifies the user.
func (s *Server) failRequest(req *models.Request, reason string) {
	req.Status = "failed"
	req.AttentionNote = reason
	req.UpdatedAt = time.Now()
	_ = s.db.UpdateRequest(req)

	s.createNotification(req.UserID, "request_failed", req.Title,
		fmt.Sprintf("Your request for \"%s\" failed: %s", req.Title, reason),
		req.ID)

	slog.Warn("request failed", "id", req.ID, "title", req.Title, "reason", reason)
}

// createNotification is a helper to create a notification and log errors.
func (s *Server) createNotification(userID int64, notifType, title, message, requestID string) {
	n := &models.Notification{
		UserID:    userID,
		Type:      notifType,
		Title:     title,
		Message:   message,
		RequestID: requestID,
		CreatedAt: time.Now(),
	}
	if _, err := s.db.CreateNotification(n); err != nil {
		slog.Error("failed to create notification", "error", err, "type", notifType, "user_id", userID)
	}
}

// pickBestResult selects the best search result based on seeders and format.
func pickBestResult(results []models.SearchResult, bookType string) models.SearchResult {
	if len(results) == 1 {
		return results[0]
	}

	best := results[0]
	bestScore := scoreResult(best, bookType)

	for _, r := range results[1:] {
		score := scoreResult(r, bookType)
		if score > bestScore {
			best = r
			bestScore = score
		}
	}
	return best
}

// scoreResult assigns a simple score to a search result for ranking.
func scoreResult(r models.SearchResult, bookType string) int {
	score := 0

	// Prefer results with seeders (for torrents).
	score += r.Seeders * 10

	// Prefer epub format for ebooks.
	if bookType == "ebook" && r.Format == "epub" {
		score += 50
	}

	// Prefer results with larger size (more likely to be complete).
	if r.Size > 0 {
		score += int(r.Size / (1024 * 1024)) // MB
	}

	// Prefer direct download sources (faster).
	if r.MD5 != "" || r.DownloadURL != "" || r.URL != "" || r.EpubURL != "" {
		score += 20
	}

	return score
}

// resolveTorrentURLForRequest resolves the torrent URL from a download request and chosen result.
func resolveTorrentURLForRequest(dlReq models.DownloadRequest, chosen models.SearchResult) string {
	if dlReq.DownloadURL != "" {
		return dlReq.DownloadURL
	}
	if dlReq.MagnetURL != "" {
		return dlReq.MagnetURL
	}
	if chosen.MagnetURL != "" {
		return chosen.MagnetURL
	}
	if chosen.DownloadURL != "" {
		return chosen.DownloadURL
	}
	if dlReq.InfoHash != "" {
		return "magnet:?xt=urn:btih:" + dlReq.InfoHash
	}
	return ""
}

// resolveSavePathAndCategory returns the qBittorrent save path and category for a book type.
func (s *Server) resolveSavePathAndCategory(bookType string) (string, string) {
	switch bookType {
	case "audiobook":
		return s.cfg.QBAudiobookSavePath, s.cfg.QBAudiobookCategory
	case "manga":
		return s.cfg.QBMangaSavePath, s.cfg.QBMangaCategory
	default:
		return s.cfg.QBSavePath, s.cfg.QBCategory
	}
}
