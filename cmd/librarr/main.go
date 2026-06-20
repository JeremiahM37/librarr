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

	"github.com/JeremiahM37/librarr/internal/api"
	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/version"
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
	directDL := download.NewDirectDownloader(cfg, &http.Client{Timeout: 5 * time.Minute})
	organizer := organize.NewOrganizer(cfg)
	targets := organize.NewLibraryTargets(cfg)
	downloadMgr := download.NewManager(cfg, database, torrentClient, sab, directDL, organizer, targets, health)

	// Try to connect to qBittorrent on startup (Transmission has no persistent
	// login — it handshakes a session id lazily on first request).
	if cfg.ActiveTorrentClient() == "qbittorrent" {
		if err := qb.Login(); err != nil {
			slog.Warn("qBittorrent initial login failed (will retry on demand)", "error", err)
		}
	}

	// Graceful shutdown context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start torrent completion watcher.
	watcher := download.NewWatcher(cfg, database, torrentClient, organizer, targets, health)
	go watcher.Start(ctx)

	// Start audiobook folder scanner (Feature 21).
	scanner := organize.NewAudiobookScanner(cfg, database, targets)
	go scanner.Start(ctx)

	// Create HTTP server (also initializes webhook sender, scheduler, series detector).
	server := api.NewServer(cfg, database, searchMgr, downloadMgr, qb, transmission, sab, organizer, targets)

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
