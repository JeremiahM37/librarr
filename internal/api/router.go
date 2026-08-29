// Package api implements librarr's HTTP server: REST API handlers,
// authentication and sessions, and routing for the embedded web UI.
package api

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
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
	transmission   *download.TransmissionClient
	sab            *download.SABnzbdClient
	mux            *http.ServeMux
	sessions       *SessionStore
	metrics        *MetricsCollector
	rateLimiter    *RateLimiter
	oidc           *OIDCHandler
	metadataClient *metadata.Client
	discover       *metadata.DiscoverService
	organizer      *organize.Organizer
	targets        *organize.LibraryTargets
	webhookSender  *webhook.Sender
	scheduler      *scheduler.Scheduler
	wishlistClean  *scheduler.WishlistCleaner
	seriesDetector *scheduler.SeriesDetector
	authorMonitor  *scheduler.AuthorMonitor
}

// NewServer creates the HTTP API server.
func NewServer(cfg *config.Config, database *db.DB, searchMgr *search.Manager, downloadMgr *download.Manager, qb *download.QBittorrentClient, transmission *download.TransmissionClient, sab *download.SABnzbdClient, organizer *organize.Organizer, targets *organize.LibraryTargets) *Server {
	sessions := NewSessionStore()

	// Configure which reverse proxies may set forwarded headers we honor.
	setTrustedProxies(cfg.TrustedProxies)

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
	wishlistClean := scheduler.NewWishlistCleaner(cfg, database)
	seriesDet := scheduler.NewSeriesDetector(database, searchMgr, ws)
	authorMon := scheduler.NewAuthorMonitor(cfg, database, ws)

	s := &Server{
		cfg:            cfg,
		db:             database,
		searchMgr:      searchMgr,
		downloadMgr:    downloadMgr,
		qb:             qb,
		transmission:   transmission,
		sab:            sab,
		mux:            http.NewServeMux(),
		sessions:       sessions,
		metrics:        NewMetricsCollector(),
		metadataClient: metadata.NewClient(&http.Client{Timeout: 15 * time.Second}),
		organizer:      organizer,
		targets:        targets,
		webhookSender:  ws,
		scheduler:      sched,
		wishlistClean:  wishlistClean,
		seriesDetector: seriesDet,
		authorMonitor:  authorMon,
	}

	// Discover (browse) reuses the same Open Library client instance as
	// metadataClient rather than a second one, so both enrichment and
	// browse search share one HTTP client and cache.
	s.discover = metadata.NewDiscoverService(
		metadata.NewGoogleBooksClient(&http.Client{Timeout: 15 * time.Second}, cfg.GoogleBooksAPIKey),
		s.metadataClient,
	)

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
	if s.wishlistClean != nil && s.cfg.WishlistCleanupEnabled {
		go s.wishlistClean.Start(ctx)
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
	s.registerCoreRoutes()
	s.registerAuthRoutes()
	s.registerSearchRoutes()
	s.registerDownloadRoutes()
	s.registerLibraryRoutes()
	s.registerRequestRoutes()
	s.registerCurationRoutes()
	s.registerAdminRoutes()
	s.registerFeedRoutes()
}

// registerCoreRoutes wires the root page, static assets, health checks, and
// small informational endpoints.
func (s *Server) registerCoreRoutes() {
	// Root -- API info page.
	s.mux.HandleFunc("GET /{$}", s.handleRoot)

	// Frontend assets (CSS/JS/fonts), embedded in the binary. Served without
	// auth (isExempt allows /static/) — the login screen needs them too.
	s.mux.Handle("GET /static/", s.handleStatic())

	// Health checks.
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /api/health", s.handleHealth)

	// OpenAPI 3.1 spec — AI agents / tooling can introspect this to
	// discover endpoints, request shapes, and response shapes without
	// prior knowledge of the codebase.
	s.mux.HandleFunc("GET /api/openapi.json", s.handleOpenAPI)

	// Sources.
	s.mux.HandleFunc("GET /api/sources", s.handleSources)
	s.mux.HandleFunc("GET /api/config", s.handleConfig)

	// Duplicate check.
	s.mux.HandleFunc("GET /api/check-duplicate", s.handleCheckDuplicate)

	// External URLs stub.
	s.mux.HandleFunc("GET /api/external-urls", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{})
	})
}

// registerAuthRoutes wires authentication, account, and user management.
func (s *Server) registerAuthRoutes() {
	// Authentication.
	s.mux.HandleFunc("POST /api/login", handleLogin(s.cfg, s.db, s.sessions))
	s.mux.HandleFunc("POST /api/login/totp", handleLoginTOTP(s.db, s.sessions))
	s.mux.HandleFunc("POST /api/register", handleRegister(s.db, s.sessions))
	s.mux.HandleFunc("POST /api/logout", handleLogout(s.sessions, s.db))
	s.mux.HandleFunc("GET /api/auth/status", handleAuthStatus(s.cfg, s.db, s.sessions))

	// User management (admin only).
	s.mux.HandleFunc("GET /api/users", requireAdmin(handleListUsers(s.db)))
	s.mux.HandleFunc("PATCH /api/users/{id}", requireAdmin(handleUpdateUser(s.db)))
	s.mux.HandleFunc("DELETE /api/users/{id}", requireAdmin(handleDeleteUser(s.db)))

	// Invite codes (admin only).
	s.mux.HandleFunc("GET /api/invites", requireAdmin(s.handleListInvites))
	s.mux.HandleFunc("POST /api/invites", requireAdmin(s.handleCreateInvite))
	s.mux.HandleFunc("DELETE /api/invites/{id}", requireAdmin(s.handleDeleteInvite))

	// Self-service account.
	s.mux.HandleFunc("POST /api/me/password", handleChangeOwnPassword(s.db))

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
}

// registerSearchRoutes wires the search endpoints.
func (s *Server) registerSearchRoutes() {
	// Search.
	s.mux.HandleFunc("GET /api/search", s.handleSearch)
	s.mux.HandleFunc("GET /api/search/audiobooks", s.handleSearchAudiobooks)
	s.mux.HandleFunc("GET /api/search/manga", s.handleSearchManga)
	s.mux.HandleFunc("GET /api/search/stream", s.handleSearchStream)
	s.mux.HandleFunc("GET /api/search/audiobooks/stream", s.handleSearchAudiobooksStream)
	s.mux.HandleFunc("GET /api/search/manga/stream", s.handleSearchMangaStream)

	// Discover (browse) — catalog metadata search, distinct from indexer
	// search above: these results aren't downloadable releases, they're
	// what someone browses before deciding what to Request.
	s.mux.HandleFunc("GET /api/discover/search", s.handleDiscoverSearch)
	s.mux.HandleFunc("GET /api/discover/book/{source}/{id}", s.handleDiscoverDetail)
}

// registerDownloadRoutes wires download management and file uploads.
func (s *Server) registerDownloadRoutes() {
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

	// File upload.
	s.mux.HandleFunc("POST /api/upload", s.handleUpload)
	s.mux.HandleFunc("GET /api/uploads", s.handleListUploads)
}

// registerLibraryRoutes wires the library, wishlist, tags, reading history,
// series tracking, and monitored authors.
func (s *Server) registerLibraryRoutes() {
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

	// Tags.
	s.mux.HandleFunc("GET /api/tags", s.handleGetTags)
	s.mux.HandleFunc("POST /api/tags", s.handleCreateTag)
	s.mux.HandleFunc("DELETE /api/tags/{id}", s.handleDeleteTag)
	s.mux.HandleFunc("GET /api/library/{id}/tags", s.handleGetItemTags)
	s.mux.HandleFunc("POST /api/library/{id}/tags", s.handleAddItemTags)
	s.mux.HandleFunc("DELETE /api/library/{id}/tags/{tagId}", s.handleRemoveItemTag)

	// Author Monitoring.
	s.mux.HandleFunc("GET /api/authors", s.handleListMonitoredAuthors)
	s.mux.HandleFunc("POST /api/authors/monitor", requireAdmin(s.handleAddMonitoredAuthor))
	s.mux.HandleFunc("DELETE /api/authors/{id}", requireAdmin(s.handleDeleteMonitoredAuthor))
}

// registerRequestRoutes wires the book request workflow and notifications.
func (s *Server) registerRequestRoutes() {
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
}

// registerCurationRoutes wires quality profiles, the blocklist, and
// release profiles.
func (s *Server) registerCurationRoutes() {
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
}

// registerAdminRoutes wires settings, integration tests, imports/exports,
// webhooks, the scheduler, backups, and the admin dashboard.
func (s *Server) registerAdminRoutes() {
	// Settings (admin only).
	s.mux.HandleFunc("GET /api/settings", requireAdmin(s.handleGetSettings))
	s.mux.HandleFunc("POST /api/settings", requireAdmin(s.handleSaveSettings))

	// Connection tests (admin only — SSRF risk).
	s.mux.HandleFunc("POST /api/test/prowlarr", requireAdmin(s.handleTestProwlarr))
	s.mux.HandleFunc("POST /api/test/qbittorrent", requireAdmin(s.handleTestQBittorrent))
	s.mux.HandleFunc("POST /api/test/transmission", requireAdmin(s.handleTestTransmission))
	s.mux.HandleFunc("POST /api/test/audiobookshelf", requireAdmin(s.handleTestAudiobookshelf))
	s.mux.HandleFunc("POST /api/test/kavita", requireAdmin(s.handleTestKavita))
	s.mux.HandleFunc("POST /api/test/sabnzbd", requireAdmin(s.handleTestSABnzbd))

	// CSV bulk import (admin only — triggers downloads).
	s.mux.HandleFunc("POST /api/import/csv", requireAdmin(s.handleCSVImport))

	// Admin dashboard (admin only).
	s.mux.HandleFunc("GET /api/admin/dashboard", requireAdmin(s.handleAdminDashboard))
	s.mux.HandleFunc("GET /api/admin/activity", requireAdmin(s.handleAdminActivity))
	s.mux.HandleFunc("POST /api/admin/bulk/retry", requireAdmin(s.handleAdminBulkRetry))
	s.mux.HandleFunc("POST /api/admin/bulk/cancel", requireAdmin(s.handleAdminBulkCancel))
	s.mux.HandleFunc("GET /api/admin/health", requireAdmin(s.handleAdminHealth))

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

	// Manual Import.
	s.mux.HandleFunc("POST /api/import/scan", requireAdmin(s.handleScanImport))
	s.mux.HandleFunc("POST /api/import/files", requireAdmin(s.handleImportFiles))

	// Goodreads / StoryGraph CSV Import.
	s.mux.HandleFunc("POST /api/import/goodreads", requireAdmin(s.handleImportGoodreads))
	s.mux.HandleFunc("POST /api/import/storygraph", requireAdmin(s.handleImportStoryGraph))

	// Scheduler.
	s.mux.HandleFunc("GET /api/scheduler/status", s.handleSchedulerStatus)
	s.mux.HandleFunc("POST /api/scheduler/run", requireAdmin(s.handleSchedulerRun))
	s.mux.HandleFunc("PUT /api/scheduler/config", requireAdmin(s.handleSchedulerConfig))

	// Backup/Restore.
	s.mux.HandleFunc("GET /api/backup", requireAdmin(s.handleDownloadBackup))
	s.mux.HandleFunc("POST /api/backup/create", requireAdmin(s.handleCreateBackup))
	s.mux.HandleFunc("GET /api/backup/list", requireAdmin(s.handleListBackups))
	s.mux.HandleFunc("POST /api/restore", requireAdmin(s.handleRestore))
}

// registerFeedRoutes wires the OPDS feed, Prometheus metrics, and the
// Torznab indexer API.
func (s *Server) registerFeedRoutes() {
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

	// Torznab API.
	torznabHandler := torznab.NewHandler(s.cfg, s.searchMgr)
	s.mux.Handle("GET /torznab/api", torznabHandler)
	// Prowlarr's indexer-discovery probe hits bare /api?t=caps rather than
	// /torznab/api. Route the exact path /api (not /api/*) to the same
	// handler so Prowlarr can detect and save Librarr as an indexer.
	// /api/... (JSON endpoints) is unaffected — Go 1.22 ServeMux's "GET /api"
	// pattern matches only the exact path, not prefixes.
	s.mux.HandleFunc("GET /api", torznabHandler.ServeHTTPAlias)
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

// imgSrcDirective builds the img-src source list. Covers served over https are
// already allowed, but a self-hosted Audiobookshelf, Kavita, Calibre-Web, or
// Komga is normally reached over plain http on the LAN, and the browser blocks
// those covers. Rather than allowing http: wholesale, allow exactly the origins
// the operator configured — attacker-supplied URLs still cannot widen it.
func (s *Server) imgSrcDirective() string {
	sources := []string{"'self'", "data:", "https:"}
	if s.cfg == nil {
		return strings.Join(sources, " ")
	}
	seen := map[string]bool{}
	for _, raw := range []string{
		s.cfg.ABSURL, s.cfg.ABSPublicURL,
		s.cfg.KavitaURL, s.cfg.KavitaPublicURL,
		s.cfg.CalibreURL, s.cfg.KomgaURL,
	} {
		origin := httpOrigin(raw)
		// https origins are already covered by the blanket https: source.
		if origin == "" || seen[origin] || strings.HasPrefix(origin, "https://") {
			continue
		}
		seen[origin] = true
		sources = append(sources, origin)
	}
	return strings.Join(sources, " ")
}

// httpOrigin reduces a configured service URL to a bare CSP origin
// (scheme://host[:port]), or "" if it is not a usable http(s) URL. A CSP source
// cannot contain a path, userinfo, or wildcard host, so anything unparseable is
// dropped rather than emitted into the header.
func httpOrigin(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ""
	}
	// Reject anything that would inject extra directives or sources.
	if strings.ContainsAny(u.Host, " ;,'\"") {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

func (s *Server) securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		// Strict CSP: no inline scripts anywhere (the UI uses external files +
		// event delegation, and Tailwind/Inter are vendored under /static/).
		//   style-src 'unsafe-inline' — the Tailwind Play runtime injects a
		//     <style> element at load; style injection, unlike script, is not
		//     an XSS vector on its own.
		//   img-src https: data: — book/audiobook cover art is hotlinked from
		//     external sources (Open Library, Anna's Archive, indexers), plus
		//     the operator's own media services, which on a LAN are usually
		//     plain http and would otherwise be blocked.
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "+
				"img-src "+s.imgSrcDirective()+"; font-src 'self'; connect-src 'self'; "+
				"object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// handleStatic serves the embedded frontend assets under /static/.
func (s *Server) handleStatic() http.Handler {
	sub, err := fs.Sub(web.StaticFS, "static")
	if err != nil {
		// Impossible with a well-formed embed; fail loudly at startup if not.
		panic("web static assets missing from binary: " + err.Error())
	}
	fileServer := http.StripPrefix("/static/", http.FileServer(http.FS(sub)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Assets aren't content-hashed, so keep the client cache short: an
		// upgraded binary must be able to push new css/js within the hour.
		w.Header().Set("Cache-Control", "public, max-age=3600")
		fileServer.ServeHTTP(w, r)
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

func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
