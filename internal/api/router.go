package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/torznab"
	"github.com/JeremiahM37/librarr/web"
)

// indexHTML holds the embedded web UI.
var indexHTML = web.IndexHTML

// Server holds the API dependencies.
type Server struct {
	cfg         *config.Config
	db          *db.DB
	searchMgr   *search.Manager
	downloadMgr *download.Manager
	qb          *download.QBittorrentClient
	sab         *download.SABnzbdClient
	mux         *http.ServeMux
	sessions    *SessionStore
	metrics     *MetricsCollector
	rateLimiter *RateLimiter
	oidc        *OIDCHandler
}

// NewServer creates the HTTP API server.
func NewServer(cfg *config.Config, database *db.DB, searchMgr *search.Manager, downloadMgr *download.Manager, qb *download.QBittorrentClient, sab *download.SABnzbdClient) *Server {
	sessions := NewSessionStore()

	s := &Server{
		cfg:         cfg,
		db:          database,
		searchMgr:   searchMgr,
		downloadMgr: downloadMgr,
		qb:          qb,
		sab:         sab,
		mux:         http.NewServeMux(),
		sessions:    sessions,
		metrics:     NewMetricsCollector(),
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

// Handler returns the HTTP handler with middleware.
func (s *Server) Handler() http.Handler {
	var handler http.Handler = s.mux
	handler = authMiddleware(s.cfg, s.db, s.sessions, handler)
	handler = RateLimitMiddleware(s.rateLimiter, handler)
	handler = s.corsMiddleware(handler)
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
	s.mux.HandleFunc("POST /api/logout", handleLogout(s.sessions))
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

	// Sources.
	s.mux.HandleFunc("GET /api/sources", s.handleSources)
	s.mux.HandleFunc("GET /api/config", s.handleConfig)

	// Duplicate check.
	s.mux.HandleFunc("GET /api/check-duplicate", s.handleCheckDuplicate)

	// Settings (admin only in multi-user mode — middleware checks in handler).
	s.mux.HandleFunc("GET /api/settings", s.handleGetSettings)
	s.mux.HandleFunc("POST /api/settings", s.handleSaveSettings)

	// Connection tests (Feature 13).
	s.mux.HandleFunc("POST /api/test/prowlarr", s.handleTestProwlarr)
	s.mux.HandleFunc("POST /api/test/qbittorrent", s.handleTestQBittorrent)
	s.mux.HandleFunc("POST /api/test/audiobookshelf", s.handleTestAudiobookshelf)
	s.mux.HandleFunc("POST /api/test/kavita", s.handleTestKavita)
	s.mux.HandleFunc("POST /api/test/sabnzbd", s.handleTestSABnzbd)

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

	// CSV bulk import (Feature 17).
	s.mux.HandleFunc("POST /api/import/csv", s.handleCSVImport)

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
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Api-Key")
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
