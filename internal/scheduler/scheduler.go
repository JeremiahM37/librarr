// Package scheduler runs periodic background tasks such as wishlist
// searches, monitored-author checks, and series tracking updates.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/webhook"
)

// Scheduler runs periodic searches for wishlist items.
type Scheduler struct {
	cfg           *config.Config
	db            *db.DB
	searchMgr     *search.Manager
	downloadMgr   *download.Manager
	webhookSender *webhook.Sender

	mu         sync.Mutex
	running    bool
	lastRun    time.Time
	lastResult string
	itemsFound int
	cancel     context.CancelFunc
}

// NewScheduler creates a new scheduler.
func NewScheduler(cfg *config.Config, database *db.DB, searchMgr *search.Manager, downloadMgr *download.Manager, ws *webhook.Sender) *Scheduler {
	return &Scheduler{
		cfg:           cfg,
		db:            database,
		searchMgr:     searchMgr,
		downloadMgr:   downloadMgr,
		webhookSender: ws,
	}
}

// Start begins the scheduling loop. It blocks until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	if !s.cfg.SchedulerEnabled {
		slog.Info("scheduler disabled")
		return
	}

	interval := time.Duration(s.cfg.SchedulerIntervalHours) * time.Hour
	if interval < time.Hour {
		interval = 24 * time.Hour
	}

	slog.Info("scheduler started", "interval", interval, "auto_download", s.cfg.SchedulerAutoDownload, "min_score", s.cfg.SchedulerMinScore)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("scheduler stopped")
			return
		case <-ticker.C:
			s.Run()
		}
	}
}

// Run executes a single scan cycle.
func (s *Scheduler) Run() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	slog.Info("scheduler: starting wishlist scan")

	wishlist, err := s.db.GetWishlist()
	if err != nil {
		slog.Error("scheduler: failed to get wishlist", "error", err)
		s.mu.Lock()
		s.lastRun = time.Now()
		s.lastResult = "error: " + err.Error()
		s.mu.Unlock()
		return
	}

	found := 0
	for _, item := range wishlist {
		result := s.searchWishlistItem(item)
		if result != nil {
			found++
		}
		// Rate limit between searches.
		time.Sleep(5 * time.Second)
	}

	s.mu.Lock()
	s.lastRun = time.Now()
	s.itemsFound = found
	s.lastResult = fmt.Sprintf("scanned %d items, found %d matches", len(wishlist), found)
	s.mu.Unlock()

	slog.Info("scheduler: scan complete", "items_scanned", len(wishlist), "matches", found)
}

func (s *Scheduler) searchWishlistItem(item models.WishlistItem) *models.SearchResult {
	tab := "main"
	if item.MediaType == "audiobook" {
		tab = "audiobook"
	} else if item.MediaType == "manga" {
		tab = "manga"
	}

	query := item.Title
	if item.Author != "" {
		query = item.Title + " " + item.Author
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	results, _ := s.searchMgr.SearchWithAuthor(ctx, tab, query, item.Author)
	if len(results) == 0 {
		return nil
	}

	best := results[0]
	if best.Score < float64(s.cfg.SchedulerMinScore) {
		slog.Debug("scheduler: best result below threshold",
			"title", item.Title,
			"score", best.Score,
			"threshold", s.cfg.SchedulerMinScore,
		)
		return nil
	}

	slog.Info("scheduler: high-confidence match",
		"wishlist_title", item.Title,
		"result_title", best.Title,
		"score", best.Score,
		"source", best.Source,
	)

	// Send webhook notification.
	if s.webhookSender != nil {
		s.webhookSender.Send(webhook.Payload{
			Event:   webhook.EventSchedulerMatch,
			Title:   "Scheduler Match: " + item.Title,
			Message: fmt.Sprintf("Found '%s' from %s (score: %.0f)", best.Title, best.Source, best.Score),
			Status:  "info",
			Extra: map[string]interface{}{
				"wishlist_title": item.Title,
				"result_title":   best.Title,
				"score":          best.Score,
				"source":         best.Source,
			},
		})
	}

	if !s.cfg.SchedulerAutoDownload {
		return &best
	}

	// Auto-download the best result.
	s.startDownload(best, item.Title)

	// Remove from wishlist after auto-download.
	_ = s.db.DeleteWishlistItem(item.ID)

	return &best
}

func (s *Scheduler) startDownload(result models.SearchResult, title string) {
	if result.MD5 != "" {
		_, err := s.downloadMgr.StartAnnasDownload(result.MD5, title)
		if err != nil {
			slog.Error("scheduler: auto-download failed", "title", title, "error", err)
		}
	} else if result.DownloadURL != "" || result.EpubURL != "" {
		dlURL := result.DownloadURL
		if dlURL == "" {
			dlURL = result.EpubURL
		}
		_, err := s.downloadMgr.StartDirectDownload(dlURL, title, result.Source, result.SourceID, result.Author)
		if err != nil {
			slog.Error("scheduler: auto-download failed", "title", title, "error", err)
		}
	} else if result.MagnetURL != "" || result.InfoHash != "" {
		url := result.MagnetURL
		if url == "" {
			url = "magnet:?xt=urn:btih:" + result.InfoHash
		}
		err := s.downloadMgr.StartTorrentDownload(url, title, "", "")
		if err != nil {
			slog.Error("scheduler: auto-download failed", "title", title, "error", err)
		}
	}
}

// Status returns the current scheduler status.
func (s *Scheduler) Status() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := map[string]interface{}{
		"enabled":        s.cfg.SchedulerEnabled,
		"interval_hours": s.cfg.SchedulerIntervalHours,
		"auto_download":  s.cfg.SchedulerAutoDownload,
		"min_score":      s.cfg.SchedulerMinScore,
		"running":        s.running,
		"items_found":    s.itemsFound,
	}

	if !s.lastRun.IsZero() {
		status["last_run"] = s.lastRun.Format(time.RFC3339)
		status["last_result"] = s.lastResult
	}

	return status
}
