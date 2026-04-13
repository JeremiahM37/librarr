package api

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/metadata"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/scheduler"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/torznab"
	"github.com/JeremiahM37/librarr/internal/webhook"
	"github.com/JeremiahM37/librarr/web"
)

// indexHTML holds the embedded web UI.
var indexHTML = web.IndexHTML

// Server holds the API dependencies.
type Server struct {
	cfg            *config.Config
	db             *db.DB
	searchMgr      *search.Manager
	downloadMgr    *download.Manager
	qb             *download.QBittorrentClient
	sab            *download.SABnzbdClient
	mux            *http.ServeMux
	sessions       *SessionStore
	metrics        *MetricsCollector
	rateLimiter    *RateLimiter
	oidc           *OIDCHandler
	metadataClient *metadata.Client
	organizer      *organize.Organizer
	targets        *organize.LibraryTargets
	webhookSender  *webhook.Sender
	scheduler      *scheduler.Scheduler
	seriesDetector *scheduler.SeriesDetector
	authorMonitor  *scheduler.AuthorMonitor
}

// NewServer creates the HTTP API server.
func NewServer(cfg *config.Config, database *db.DB, searchMgr *search.Manager, downloadMgr *download.Manager, qb *download.QBittorrentClient, sab *download.SABnzbdClient, organizer *organize.Organizer, targets *organize.LibraryTargets) *Server {
	sessions := NewSessionStore()

	// Initialize webhook sender.
	ws := webhook.NewSender()
	// Load webhook configs from DB.
	if configs, err := database.GetWebhookConfigs(); err == nil {
		ws.SetConfigs(configs)
	}
	// If env-based webhook is set, add it as a default.
	if cfg.WebhookURL != "" {
		envConfig := webhook.Config{
			Name:    "Default (" + cfg.WebhookType + ")",
			URL:     cfg.WebhookURL,
			Type:    cfg.WebhookType,
			Enabled: true,
			Events:  "*",
		}
		// Only add if not already in DB configs.
		existing, _ := database.GetWebhookConfigs()
		found := false
		for _, c := range existing {
			if c.URL == cfg.WebhookURL {
				found = true
				break
			}
		}
		if !found {
			id, err := database.CreateWebhookConfig(&envConfig)
			if err == nil {
				envConfig.ID = id
				configs := ws.GetConfigs()
				configs = append(configs, envConfig)
				ws.SetConfigs(configs)
			}
		}
	}

	// Wire webhook sender into download manager.
	downloadMgr.SetWebhookSender(ws)

	// Initialize scheduler, series detector, and author monitor.
	sched := scheduler.NewScheduler(cfg, database, searchMgr, downloadMgr, ws)
	seriesDet := scheduler.NewSeriesDetector(database, searchMgr, ws)
	authorMon := scheduler.NewAuthorMonitor(cfg, database, ws)

	s := &Server{
		cfg:            cfg,
		db:             database,
		searchMgr:      searchMgr,
		downloadMgr:    downloadMgr,
		qb:             qb,
		sab:            sab,
		mux:            http.NewServeMux(),
		sessions:       sessions,
		metrics:        NewMetricsCollector(),
		metadataClient: metadata.NewClient(&http.Client{Timeout: 15 * time.Second}),
		organizer:      organizer,
		targets:        targets,
		webhookSender:  ws,
		scheduler:      sched,
		seriesDetector: seriesDet,
		authorMonitor:  authorMon,
	}

	// Initialize OIDC handler if configured.
	s.oidc = NewOIDCHandler(cfg, database, sessions)

	// Initialize rate limiter if enabled.
	if cfg.RateLimitEnabled {
		s.rateLimiter = NewRateLimiter(60, map[string]int{
			"login":    20,
			"search":   120,
			"download": 60,
			"api":      300,
			"default":  600,
		})
	}

	s.registerRoutes()
	return s
}

// StartScheduler starts the background scheduler loop.
func (s *Server) StartScheduler(ctx context.Context) {
	// Start author monitor in a separate goroutine.
	if s.authorMonitor != nil && s.cfg.AuthorMonitorEnabled {
		go s.runAuthorMonitorLoop(ctx)
	}
	// Scheduler.Start blocks until ctx is cancelled.
	if s.scheduler != nil {
		s.scheduler.Start(ctx)
	}
}

// runAuthorMonitorLoop runs the author monitor on its configured interval.
func (s *Server) runAuthorMonitorLoop(ctx context.Context) {
	interval := time.Duration(s.cfg.AuthorCheckIntervalDays) * 24 * time.Hour
	if interval < time.Hour {
		interval = 7 * 24 * time.Hour
	}

	slog.Info("author monitor started", "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("author monitor stopped")
			return
		case <-ticker.C:
			s.authorMonitor.CheckAuthors()
		}
	}
}

// Handler returns the HTTP handler with middleware.
func (s *Server) Handler() http.Handler {
	var handler http.Handler = s.mux
	handler = authMiddleware(s.cfg, s.db, s.sessions, handler)
	handler = RateLimitMiddleware(s.rateLimiter, handler)
	handler = s.corsMiddleware(handler)
	handler = s.securityHeadersMiddleware(handler)
	handler = s.requestSizeLimitMiddleware(handler)
	handler = s.logMiddleware(handler)
	return handler
}

func (s *Server) registerRoutes() {
	// Root -- API info page.
	s.mux.HandleFunc("GET /{$}", s.handleRoot)

	// Health checks.
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /api/health", s.handleHealth)

	// Authentication.
	s.mux.HandleFunc("POST /api/login", handleLogin(s.cfg, s.db, s.sessions))
	s.mux.HandleFunc("POST /api/login/totp", handleLoginTOTP(s.db, s.sessions))
	s.mux.HandleFunc("POST /api/register", handleRegister(s.db, s.sessions))
	s.mux.HandleFunc("POST /api/logout", handleLogout(s.sessions, s.db))
	s.mux.HandleFunc("GET /api/auth/status", handleAuthStatus(s.db, s.sessions))

	// User management (admin only).
	s.mux.HandleFunc("GET /api/users", requireAdmin(handleListUsers(s.db)))
	s.mux.HandleFunc("PATCH /api/users/{id}", requireAdmin(handleUpdateUser(s.db)))
	s.mux.HandleFunc("DELETE /api/users/{id}", requireAdmin(handleDeleteUser(s.db)))

	// TOTP 2FA.
	s.mux.HandleFunc("POST /api/totp/setup", handleTOTPSetup(s.db))
	s.mux.HandleFunc("POST /api/totp/verify", handleTOTPVerify(s.db))
	s.mux.HandleFunc("POST /api/totp/disable", handleTOTPDisable(s.db))
	s.mux.HandleFunc("GET /api/totp/status", handleTOTPStatus(s.db))

	// OIDC/SSO.
	if s.oidc != nil {
		s.mux.HandleFunc("GET /auth/oidc/login", s.oidc.HandleLogin)
		s.mux.HandleFunc("GET /auth/oidc/callback", s.oidc.HandleCallback)
	}

	// Search.
	s.mux.HandleFunc("GET /api/search", s.handleSearch)
	s.mux.HandleFunc("GET /api/search/audiobooks", s.handleSearchAudiobooks)
	s.mux.HandleFunc("GET /api/search/manga", s.handleSearchManga)

	// Downloads.
	s.mux.HandleFunc("POST /api/download", s.handleDownload)
	s.mux.HandleFunc("POST /api/download/torrent", s.handleDownloadTorrent)
	s.mux.HandleFunc("POST /api/download/annas", s.handleDownloadAnnas)
	s.mux.HandleFunc("POST /api/download/audiobook", s.handleDownloadAudiobook)
	s.mux.HandleFunc("GET /api/downloads", s.handleGetDownloads)
	s.mux.HandleFunc("DELETE /api/downloads/torrent/{hash}", s.handleDeleteTorrent)
	s.mux.HandleFunc("DELETE /api/downloads/novel/{jobID}", s.handleDeleteJob)
	s.mux.HandleFunc("POST /api/downloads/clear", s.handleClearFinished)

	// Job retry (dead letter).
	s.mux.HandleFunc("POST /api/downloads/jobs/{id}/retry", s.handleRetryJob)

	// Library.
	s.mux.HandleFunc("GET /api/library", s.handleLibrary)
	s.mux.HandleFunc("GET /api/library/audiobooks", s.handleLibraryAudiobooks)
	s.mux.HandleFunc("GET /api/library/manga", s.handleLibraryManga)
	s.mux.HandleFunc("DELETE /api/library/book/{id}", s.handleDeleteBook)
	s.mux.HandleFunc("DELETE /api/library/audiobook/{id}", s.handleDeleteAudiobook)
	s.mux.HandleFunc("GET /api/stats", s.handleStats)
	s.mux.HandleFunc("GET /api/activity", s.handleActivity)

	// Wishlist.
	s.mux.HandleFunc("GET /api/wishlist", s.handleGetWishlist)
	s.mux.HandleFunc("POST /api/wishlist", s.handleAddWishlist)
	s.mux.HandleFunc("DELETE /api/wishlist/{id}", s.handleDeleteWishlist)

	// Requests (book request workflow).
	s.mux.HandleFunc("POST /api/requests", s.handleCreateRequest)
	s.mux.HandleFunc("GET /api/requests", s.handleListRequests)
	s.mux.HandleFunc("GET /api/requests/{id}", s.handleGetRequest)
	s.mux.HandleFunc("PUT /api/requests/{id}/approve", requireAdmin(s.handleApproveRequest))
	s.mux.HandleFunc("PUT /api/requests/{id}/cancel", s.handleCancelRequest)
	s.mux.HandleFunc("PUT /api/requests/{id}/retry", requireAdmin(s.handleRetryRequest))
	s.mux.HandleFunc("PUT /api/requests/{id}/select", requireAdmin(s.handleSelectResult))
	s.mux.HandleFunc("DELETE /api/requests/{id}", requireAdmin(s.handleDeleteRequest))

	// Notifications.
	s.mux.HandleFunc("GET /api/notifications", s.handleGetNotifications)
	s.mux.HandleFunc("GET /api/notifications/unread", s.handleUnreadCount)
	s.mux.HandleFunc("PUT /api/notifications/{id}/read", s.handleMarkRead)
	s.mux.HandleFunc("PUT /api/notifications/read-all", s.handleMarkAllRead)
	s.mux.HandleFunc("DELETE /api/notifications/{id}", s.handleDeleteNotification)

	// Sources.
	s.mux.HandleFunc("GET /api/sources", s.handleSources)
	s.mux.HandleFunc("GET /api/config", s.handleConfig)

	// Duplicate check.
	s.mux.HandleFunc("GET /api/check-duplicate", s.handleCheckDuplicate)

	// Settings (admin only).
	s.mux.HandleFunc("GET /api/settings", requireAdmin(s.handleGetSettings))
	s.mux.HandleFunc("POST /api/settings", requireAdmin(s.handleSaveSettings))

	// Connection tests (admin only — SSRF risk).
	s.mux.HandleFunc("POST /api/test/prowlarr", requireAdmin(s.handleTestProwlarr))
	s.mux.HandleFunc("POST /api/test/qbittorrent", requireAdmin(s.handleTestQBittorrent))
	s.mux.HandleFunc("POST /api/test/audiobookshelf", requireAdmin(s.handleTestAudiobookshelf))
	s.mux.HandleFunc("POST /api/test/kavita", requireAdmin(s.handleTestKavita))
	s.mux.HandleFunc("POST /api/test/sabnzbd", requireAdmin(s.handleTestSABnzbd))

	// External URLs stub.
	s.mux.HandleFunc("GET /api/external-urls", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{})
	})

	// OPDS feed (Feature 14).
	s.mux.HandleFunc("GET /opds", s.handleOPDSRoot)
	s.mux.HandleFunc("GET /opds/", s.handleOPDSRoot)
	s.mux.HandleFunc("GET /opds/books", s.handleOPDSBooks)
	s.mux.HandleFunc("GET /opds/search", s.handleOPDSSearch)
	s.mux.HandleFunc("GET /opds/download/{id}", s.handleOPDSDownload)
	s.mux.HandleFunc("GET /opds/opensearch.xml", s.handleOPDSOpenSearch)

	// Prometheus metrics (Feature 16).
	if s.cfg.MetricsEnabled {
		s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	}

	// CSV bulk import (admin only — triggers downloads).
	s.mux.HandleFunc("POST /api/import/csv", requireAdmin(s.handleCSVImport))

	// Admin dashboard (admin only).
	s.mux.HandleFunc("GET /api/admin/dashboard", requireAdmin(s.handleAdminDashboard))
	s.mux.HandleFunc("GET /api/admin/activity", requireAdmin(s.handleAdminActivity))
	s.mux.HandleFunc("POST /api/admin/bulk/retry", requireAdmin(s.handleAdminBulkRetry))
	s.mux.HandleFunc("POST /api/admin/bulk/cancel", requireAdmin(s.handleAdminBulkCancel))
	s.mux.HandleFunc("GET /api/admin/health", requireAdmin(s.handleAdminHealth))

	// File upload.
	s.mux.HandleFunc("POST /api/upload", s.handleUpload)
	s.mux.HandleFunc("GET /api/uploads", s.handleListUploads)

	// Webhooks (admin only).
	s.mux.HandleFunc("GET /api/webhooks", requireAdmin(s.handleGetWebhooks))
	s.mux.HandleFunc("POST /api/webhooks", requireAdmin(s.handleCreateWebhook))
	s.mux.HandleFunc("DELETE /api/webhooks/{id}", requireAdmin(s.handleDeleteWebhook))
	s.mux.HandleFunc("POST /api/webhooks/test", requireAdmin(s.handleTestWebhook))

	// Import/Export.
	s.mux.HandleFunc("GET /api/export/library", s.handleExportLibrary)
	s.mux.HandleFunc("GET /api/export/wishlist", s.handleExportWishlist)
	s.mux.HandleFunc("GET /api/export/requests", s.handleExportRequests)
	s.mux.HandleFunc("POST /api/import/library", requireAdmin(s.handleImportLibrary))
	s.mux.HandleFunc("POST /api/import/wishlist", requireAdmin(s.handleImportWishlist))
	s.mux.HandleFunc("POST /api/import/csvdata", requireAdmin(s.handleImportCSVData))

	// Scheduler.
	s.mux.HandleFunc("GET /api/scheduler/status", s.handleSchedulerStatus)
	s.mux.HandleFunc("POST /api/scheduler/run", requireAdmin(s.handleSchedulerRun))
	s.mux.HandleFunc("PUT /api/scheduler/config", requireAdmin(s.handleSchedulerConfig))

	// Reading history.
	s.mux.HandleFunc("POST /api/history", s.handleAddHistory)
	s.mux.HandleFunc("GET /api/history", s.handleGetHistory)
	s.mux.HandleFunc("PATCH /api/history/{id}", s.handleUpdateHistory)
	s.mux.HandleFunc("DELETE /api/history/{id}", s.handleDeleteHistory)
	s.mux.HandleFunc("GET /api/history/stats", s.handleHistoryStats)

	// Series auto-complete.
	s.mux.HandleFunc("GET /api/series", s.handleListSeries)
	s.mux.HandleFunc("GET /api/series/{name}/missing", s.handleSeriesMissing)
	s.mux.HandleFunc("POST /api/series/{name}/search-missing", s.handleSearchMissingSeries)

	// Quality Profiles.
	s.mux.HandleFunc("GET /api/quality-profiles", s.handleGetQualityProfiles)
	s.mux.HandleFunc("GET /api/quality-profiles/default", s.handleGetDefaultQualityProfile)
	s.mux.HandleFunc("POST /api/quality-profiles", requireAdmin(s.handleCreateQualityProfile))
	s.mux.HandleFunc("PUT /api/quality-profiles/{id}", requireAdmin(s.handleUpdateQualityProfile))
	s.mux.HandleFunc("DELETE /api/quality-profiles/{id}", requireAdmin(s.handleDeleteQualityProfile))

	// Blocklist.
	s.mux.HandleFunc("GET /api/blocklist", s.handleGetBlocklist)
	s.mux.HandleFunc("POST /api/blocklist", requireAdmin(s.handleAddBlocklistEntry))
	s.mux.HandleFunc("DELETE /api/blocklist/{id}", requireAdmin(s.handleDeleteBlocklistEntry))
	s.mux.HandleFunc("POST /api/blocklist/clear", requireAdmin(s.handleClearBlocklist))

	// Release Profiles.
	s.mux.HandleFunc("GET /api/release-profiles", s.handleGetReleaseProfiles)
	s.mux.HandleFunc("POST /api/release-profiles", requireAdmin(s.handleCreateReleaseProfile))
	s.mux.HandleFunc("PUT /api/release-profiles/{id}", requireAdmin(s.handleUpdateReleaseProfile))
	s.mux.HandleFunc("DELETE /api/release-profiles/{id}", requireAdmin(s.handleDeleteReleaseProfile))

	// Manual Import.
	s.mux.HandleFunc("POST /api/import/scan", requireAdmin(s.handleScanImport))
	s.mux.HandleFunc("POST /api/import/files", requireAdmin(s.handleImportFiles))

	// Tags.
	s.mux.HandleFunc("GET /api/tags", s.handleGetTags)
	s.mux.HandleFunc("POST /api/tags", s.handleCreateTag)
	s.mux.HandleFunc("DELETE /api/tags/{id}", s.handleDeleteTag)
	s.mux.HandleFunc("GET /api/library/{id}/tags", s.handleGetItemTags)
	s.mux.HandleFunc("POST /api/library/{id}/tags", s.handleAddItemTags)
	s.mux.HandleFunc("DELETE /api/library/{id}/tags/{tagId}", s.handleRemoveItemTag)

	// Backup/Restore.
	s.mux.HandleFunc("GET /api/backup", requireAdmin(s.handleDownloadBackup))
	s.mux.HandleFunc("POST /api/backup/create", requireAdmin(s.handleCreateBackup))
	s.mux.HandleFunc("GET /api/backup/list", requireAdmin(s.handleListBackups))
	s.mux.HandleFunc("POST /api/restore", requireAdmin(s.handleRestore))

	// Author Monitoring.
	s.mux.HandleFunc("GET /api/authors", s.handleListMonitoredAuthors)
	s.mux.HandleFunc("POST /api/authors/monitor", requireAdmin(s.handleAddMonitoredAuthor))
	s.mux.HandleFunc("DELETE /api/authors/{id}", requireAdmin(s.handleDeleteMonitoredAuthor))

	// Goodreads / StoryGraph CSV Import.
	s.mux.HandleFunc("POST /api/import/goodreads", requireAdmin(s.handleImportGoodreads))
	s.mux.HandleFunc("POST /api/import/storygraph", requireAdmin(s.handleImportStoryGraph))

	// Torznab API.
	torznabHandler := torznab.NewHandler(s.cfg, s.searchMgr)
	s.mux.Handle("GET /torznab/api", torznabHandler)
}

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	err := s.downloadMgr.RetryDeadLetterJob(jobID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// requestSizeLimitMiddleware caps non-multipart request bodies at 1MB to prevent OOM.
// Multipart uploads have their own size limits set in their handlers.
func (s *Server) requestSizeLimitMiddleware(next http.Handler) http.Handler {
	const maxBodySize = 1 << 20 // 1MB
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		if r.Body != nil && !strings.HasPrefix(contentType, "multipart/") {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(wrapped, r)
		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration", time.Since(start).String(),
			"remote", r.RemoteAddr,
		)
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			// Reflect the request origin only if it matches the Host header
			// (same-origin) or is empty. This prevents cross-origin credential theft
			// while still allowing same-origin requests from the web UI.
			host := r.Host
			if strings.Contains(origin, host) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
			// For API-key-only requests (no cookies), allow any origin.
			// Accept the key via X-Api-Key header OR ?apikey= query param —
			// clients like the Homelab PWA use the query-param form because
			// they fetch() without custom headers (which would force a CORS
			// preflight the browser never sends the key on).
			if r.Header.Get("X-Api-Key") != "" || r.URL.Query().Get("apikey") != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Api-Key")
		w.Header().Set("Vary", "Origin")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
