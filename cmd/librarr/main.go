// Command librarr runs the librarr server: book search, downloads, and
// library management with a web UI, Torznab endpoint, and OPDS feed.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jamie75/librarr/internal/api"
	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/download"
	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	"github.com/jamie75/librarr/internal/netutil"
	"github.com/jamie75/librarr/internal/organize"
	"github.com/jamie75/librarr/internal/search"
	"github.com/jamie75/librarr/internal/version"
)

func main() {
	// Structured logging.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("starting Librarr", "version", version.Version)

	// Load configuration.
	cfg := config.Load()

	// Initialize database.
	database, err := db.New(cfg.DBPath)
	if err != nil {
		slog.Error("failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	// Initialize health tracker.
	health := search.NewHealthTracker(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerTimeout)

	// HTTP client shared across sources.
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Initialize search sources.
	var sources []search.Searcher

	// Anna's Archive (primary source).
	sources = append(sources, search.NewAnnasArchive(cfg, httpClient))

	// Prowlarr (ebooks, audiobooks, manga).
	if cfg.HasProwlarr() {
		sources = append(sources, search.NewProwlarr(cfg, httpClient, "main"))
		sources = append(sources, search.NewProwlarr(cfg, httpClient, "audiobook"))
		sources = append(sources, search.NewProwlarr(cfg, httpClient, "manga"))
	}

	// AudioBookBay (always enabled for audiobook search).
	sources = append(sources, search.NewAudioBookBay(cfg, httpClient))

	// Free ebook sources (always enabled).
	sources = append(sources, search.NewGutenberg(cfg, httpClient))
	sources = append(sources, search.NewOpenLibrary(cfg, httpClient))

	// Auto-registered sources (Standard Ebooks, Librivox, MangaDex, Nyaa, Anna's Manga, Web Novels).
	registeredSources := search.CreateRegisteredSources(cfg, httpClient)
	sources = append(sources, registeredSources...)

	searchMgr := search.NewManager(cfg, sources, health)

	// Graceful shutdown context (used by background workers).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Log enabled sources.
	for _, s := range sources {
		status := "disabled"
		if s.Enabled() {
			status = "enabled"
		}
		slog.Info("source loaded", "name", s.Name(), "label", s.Label(), "status", status, "tab", s.SearchTab())
	}

	// Initialize download components.
	qb := download.NewQBittorrentClient(cfg)
	transmission := download.NewTransmissionClient(cfg)
	sab := download.NewSABnzbdClient(cfg)
	torrentClient := download.SelectTorrentClient(cfg, qb, transmission)
	if torrentClient != nil {
		slog.Info("active torrent client", "client", torrentClient.Name())
	} else {
		slog.Info("no torrent client configured")
	}
	directDL := download.NewDirectDownloader(cfg, &http.Client{
		Timeout: 5 * time.Minute,
		// Re-run the SSRF guard on every redirect hop: a public URL can 30x to
		// an internal address, which the initial check would never see.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			return netutil.ValidateOutboundURL(req.URL.String())
		},
	})
	organizer := organize.NewOrganizer(cfg)
	if updated, err := database.BackfillLibraryMetadata(func(path string) (string, string) {
		meta := organize.ExtractEbookMetadata(path)
		return meta.Title, meta.Author
	}); err != nil {
		slog.Warn("ebook metadata backfill failed", "error", err)
	} else if updated > 0 {
		slog.Info("ebook metadata backfill complete", "updated", updated)
	}
	targets := organize.NewLibraryTargets(cfg)

	librarySelection, err := library.NewConfiguredLibraryService(ctx, cfg, database)
	if err != nil {
		slog.Error("failed to configure library repository", "error", err)
		os.Exit(1)
	}
	importSelection, err := libraryimport.NewConfiguredImportEngine(cfg, database, librarySelection.LibraryService, librarySelection.Mode)
	if err != nil {
		slog.Error("failed to configure import engine", "error", err)
		os.Exit(1)
	}
	slog.Info("import engine selected", "mode", importSelection.Mode)
	downloadMgr := download.NewManagerWithImportEngine(cfg, database, torrentClient, sab, directDL, organizer, targets, health, librarySelection.LibraryService, importSelection.Engine, importSelection.Mode)

	// Try to connect to qBittorrent on startup (Transmission has no persistent
	// login — it handshakes a session id lazily on first request).
	if cfg.ActiveTorrentClient() == "qbittorrent" {
		if err := qb.Login(); err != nil {
			slog.Warn("qBittorrent initial login failed (will retry on demand)", "error", err)
		}
	}

	// Start torrent completion watcher.
	watcher := download.NewWatcherWithImportEngine(cfg, database, torrentClient, organizer, targets, health, importSelection.Engine)
	go watcher.Start(ctx)

	// Start audiobook folder scanner (Feature 21).
	scanner := organize.NewAudiobookScanner(cfg, database, targets)
	if enabled, reason := scanner.Enabled(); enabled {
		slog.Info("legacy audiobook scanner startup decision",
			"repository_mode", librarySelection.Mode,
			"import_engine", importSelection.Mode,
			"legacy_scanner_enabled", true,
			"audiobook_scanner_enabled", true,
			"reason", reason,
		)
		go scanner.Start(ctx)
	} else {
		slog.Info("legacy audiobook scanner startup decision",
			"repository_mode", librarySelection.Mode,
			"import_engine", importSelection.Mode,
			"legacy_scanner_enabled", false,
			"audiobook_scanner_enabled", false,
			"reason", reason,
		)
	}

	// Create HTTP server (also initializes webhook sender, scheduler, series detector).
	server := api.NewServerWithServices(cfg, database, searchMgr, downloadMgr, qb, transmission, sab, organizer, targets, librarySelection.LibraryService, importSelection.Engine)

	// Start scheduled search goroutine.
	go server.StartScheduler(ctx)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      server.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		slog.Info("listening", "port", cfg.Port, "url", fmt.Sprintf("http://0.0.0.0:%d", cfg.Port))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	slog.Info("shutdown complete")
}
