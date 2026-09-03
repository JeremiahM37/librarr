// Package scheduler runs periodic background tasks such as wanted-list
// searches, monitored-author checks, and series tracking updates.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/quality"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/webhook"
)

// wantedSearcher is the slice of search.Manager the scheduler uses.
type wantedSearcher interface {
	SearchWithAuthor(ctx context.Context, tab, query, author string) ([]models.SearchResult, int64)
}

// wantedDownloader is the slice of download.Manager the scheduler uses. Every
// grab carries the wanted row it serves so the import can link back to it.
type wantedDownloader interface {
	StartAnnasDownloadFor(md5, title string, wantedID int64) (*models.DownloadJob, error)
	StartDirectDownloadFor(fileURL, title, source, sourceID, author string, wantedID int64) (*models.DownloadJob, error)
	StartTorrentDownload(torrentURL, title, savePath, category, expectedInfoHash string) error
}

// Scheduler is the wanted-list loop: for every monitored item it searches,
// runs the item's quality profile over the results, and grabs the best
// acceptable release — a first copy when the item has no file, an upgrade
// when it has one below the cutoff.
type Scheduler struct {
	cfg           *config.Config
	db            *db.DB
	searchMgr     wantedSearcher
	downloadMgr   wantedDownloader
	webhookSender *webhook.Sender

	// now and sleep are swappable for tests.
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) bool

	mu         sync.Mutex
	running    bool
	lastRun    time.Time
	lastResult string
	itemsFound int
	lastStats  RunStats
	recent     []ItemOutcome
}

// RunStats summarises one pass over the wanted list.
type RunStats struct {
	Scanned   int    `json:"scanned"`
	Skipped   int    `json:"skipped"`  // not due: unmonitored, satisfied, or a grab already in flight
	Searched  int    `json:"searched"` // hit the sources
	Matched   int    `json:"matched"`  // an acceptable release was found
	Grabbed   int    `json:"grabbed"`  // a download was started
	Upgrades  int    `json:"upgrades"` // grabs that replace an existing file
	Linked    int    `json:"linked"`   // rows reconciled to files already in the library
	Errors    int    `json:"errors"`   // grabs that failed to start
	StartedAt string `json:"started_at,omitempty"`
	Duration  string `json:"duration,omitempty"`
}

// ItemOutcome is what happened to one wanted item in a pass (or a manual
// search). It is what the UI shows as "last result".
type ItemOutcome struct {
	WantedID  int64              `json:"wanted_id"`
	Title     string             `json:"title"`
	State     string             `json:"state"`
	Action    string             `json:"action"` // skipped | searched | matched | grabbed | upgrade | error
	Reason    string             `json:"reason"`
	Candidate string             `json:"candidate,omitempty"` // "Title (EPUB, source, score 84)"
	Results   int                `json:"results"`
	Decisions []CandidateSummary `json:"decisions,omitempty"`
	JobID     string             `json:"job_id,omitempty"`
	At        string             `json:"at"`
}

// CandidateSummary explains one search result's fate, for dry runs.
type CandidateSummary struct {
	Title    string  `json:"title"`
	Source   string  `json:"source"`
	Format   string  `json:"format"`
	Score    float64 `json:"score"`
	Accepted bool    `json:"accepted"`
	Upgrade  bool    `json:"upgrade,omitempty"`
	Reason   string  `json:"reason"`
}

// NewScheduler creates a new scheduler.
func NewScheduler(cfg *config.Config, database *db.DB, searchMgr *search.Manager, downloadMgr *download.Manager, ws *webhook.Sender) *Scheduler {
	s := &Scheduler{
		cfg:           cfg,
		db:            database,
		webhookSender: ws,
		now:           time.Now,
		sleep:         sleepCtx,
	}
	// Typed nils must not become non-nil interfaces.
	if searchMgr != nil {
		s.searchMgr = searchMgr
	}
	if downloadMgr != nil {
		s.downloadMgr = downloadMgr
	}
	return s
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
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

	slog.Info("scheduler started", "interval", interval, "auto_download", s.cfg.SchedulerAutoDownload, "min_score", s.cfg.SchedulerMinScore, "auto_upgrade", s.cfg.AutoUpgradeEnabled)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("scheduler stopped")
			return
		case <-ticker.C:
			s.RunCtx(ctx)
		}
	}
}

// Run executes a single scan cycle.
func (s *Scheduler) Run() { s.RunCtx(context.Background()) }

// RunCtx executes a single scan cycle, stopping early when ctx is cancelled.
func (s *Scheduler) RunCtx(ctx context.Context) RunStats {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return RunStats{}
	}
	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	start := s.now()
	stats := RunStats{StartedAt: start.Format(time.RFC3339)}
	slog.Info("scheduler: starting wanted-list scan")

	stats.Linked = s.reconcile()

	wishlist, err := s.db.GetWishlist()
	if err != nil {
		slog.Error("scheduler: failed to get wishlist", "error", err)
		s.mu.Lock()
		s.lastRun = s.now()
		s.lastResult = "error: " + err.Error()
		s.mu.Unlock()
		return stats
	}
	stats.Scanned = len(wishlist)

	delay := time.Duration(s.cfg.SchedulerItemDelaySeconds) * time.Second
	searchedAny := false
	for _, item := range wishlist {
		if ctx.Err() != nil {
			break
		}
		// Only pause between two items that actually hit the sources.
		if searchedAny && delay > 0 && s.itemIsDue(item) {
			if !s.sleep(ctx, delay) {
				break
			}
		}
		outcome := s.processItem(ctx, item, !s.cfg.SchedulerAutoDownload)
		switch outcome.Action {
		case "skipped":
			stats.Skipped++
		case "error":
			stats.Searched++
			stats.Errors++
			searchedAny = true
		default:
			stats.Searched++
			searchedAny = true
			if outcome.Action == "matched" || outcome.Action == "grabbed" || outcome.Action == "upgrade" {
				stats.Matched++
			}
			if outcome.Action == "grabbed" || outcome.Action == "upgrade" {
				stats.Grabbed++
			}
			if outcome.Action == "upgrade" {
				stats.Upgrades++
			}
		}
	}

	stats.Duration = s.now().Sub(start).Round(time.Millisecond).String()
	s.mu.Lock()
	s.lastRun = s.now()
	s.itemsFound = stats.Matched
	s.lastStats = stats
	s.lastResult = fmt.Sprintf("scanned %d items, searched %d, found %d matches, grabbed %d (%d upgrades)",
		stats.Scanned, stats.Searched, stats.Matched, stats.Grabbed, stats.Upgrades)
	s.mu.Unlock()

	slog.Info("scheduler: scan complete", "scanned", stats.Scanned, "searched", stats.Searched, "matched", stats.Matched, "grabbed", stats.Grabbed, "upgrades", stats.Upgrades, "linked", stats.Linked)
	return stats
}

// reconcile links unlinked wanted rows to files that already sit in the
// library — an existing collection, a manual download, or a torrent librarr
// could not tie to the row at grab time. Matching reuses the same ownership
// index the search page uses for its "in library" badge.
func (s *Scheduler) reconcile() int {
	items, err := s.db.GetWishlist()
	if err != nil {
		return 0
	}
	needs := false
	for _, it := range items {
		if it.LibraryItemID == 0 {
			needs = true
			break
		}
	}
	if !needs {
		return 0
	}
	idx, err := s.db.LibraryMatchIndex()
	if err != nil || idx == nil {
		return 0
	}
	linked := 0
	for _, it := range items {
		if it.LibraryItemID != 0 {
			continue
		}
		match, ok := idx.Lookup(it.Title, it.Author, it.MediaType)
		if !ok {
			continue
		}
		if err := s.db.LinkWishlistItem(it.ID, match.ID); err == nil {
			linked++
			slog.Info("scheduler: wanted item linked to existing library file", "wanted_id", it.ID, "title", it.Title, "library_item_id", match.ID)
		}
	}
	return linked
}

// itemIsDue reports whether processItem would search for this item.
func (s *Scheduler) itemIsDue(item models.WishlistItem) bool {
	o := s.skipReason(item)
	return o == ""
}

// skipReason returns why an item is not searched this pass, or "" if it is.
func (s *Scheduler) skipReason(item models.WishlistItem) string {
	if !item.Monitored {
		return "not monitored"
	}
	if item.ActiveJobID != "" {
		if s.activeJobStillRunning(item.ActiveJobID) {
			return "grab in progress (" + item.ActiveJobID + ")"
		}
		// The job is gone or failed: release the row so it can be retried.
		_ = s.db.SetWishlistActiveJob(item.ID, "")
	}
	if item.LibraryItemID == 0 {
		return ""
	}
	profile := s.db.ResolveQualityProfile(item.QualityProfileID, item.MediaType).Profile()
	if !s.cfg.AutoUpgradeEnabled {
		return "has " + strings.ToUpper(item.CurrentFormat) + "; upgrades are disabled"
	}
	if !profile.UpgradesAllowed {
		return "has " + strings.ToUpper(item.CurrentFormat) + "; profile \"" + profile.Name + "\" does not upgrade"
	}
	if profile.CutoffMet(item.CurrentFormat) {
		return "cutoff met (" + strings.ToUpper(item.CurrentFormat) + ")"
	}
	return ""
}

// activeJobStillRunning reports whether an in-flight marker still refers to
// live work. Torrent markers stay until the completion watcher imports the
// torrent; job markers are checked against the job table.
func (s *Scheduler) activeJobStillRunning(ref string) bool {
	if strings.HasPrefix(ref, "torrent:") {
		return true
	}
	job, err := s.db.GetJob(ref)
	if err != nil || job == nil {
		return false
	}
	switch job.Status {
	case "error", "dead_letter", "completed":
		return false
	}
	return true
}

// SearchItem runs the decision for one wanted item on demand. With dryRun the
// chosen release is reported but not grabbed; otherwise it behaves like one
// pass of the scheduler for that item, honouring the auto-download setting
// only insofar as a manual search always grabs.
func (s *Scheduler) SearchItem(ctx context.Context, wantedID int64, dryRun bool) (ItemOutcome, error) {
	item, err := s.db.GetWishlistItem(wantedID)
	if err != nil {
		return ItemOutcome{}, err
	}
	// A manual search ignores "not monitored" and the in-flight marker only
	// if that marker is stale; everything else (cutoff met, upgrades off) is
	// still reported honestly rather than forced.
	forced := *item
	forced.Monitored = true
	return s.processItem(ctx, forced, dryRun), nil
}

func (s *Scheduler) processItem(ctx context.Context, item models.WishlistItem, dryRun bool) ItemOutcome {
	profileRec := s.db.ResolveQualityProfile(item.QualityProfileID, item.MediaType)
	profile := profileRec.Profile()
	out := ItemOutcome{
		WantedID: item.ID,
		Title:    item.Title,
		At:       s.now().Format(time.RFC3339),
		State:    quality.State(profile, item.Monitored, item.ActiveJobID != "", item.LibraryItemID != 0, item.CurrentFormat, s.cfg.AutoUpgradeEnabled),
	}
	if reason := s.skipReason(item); reason != "" {
		out.Action = "skipped"
		out.Reason = reason
		s.remember(out)
		return out
	}

	if s.searchMgr == nil {
		out.Action = "error"
		out.Reason = "search is not available"
		s.record(item.ID, out)
		return out
	}

	results := s.search(ctx, item)
	out.Results = len(results)
	if len(results) == 0 {
		out.Action = "searched"
		out.Reason = "no results"
		s.record(item.ID, out)
		return out
	}

	// Match confidence gates; quality ranks. Releases on the blocklist
	// (rejected after an earlier import, or blocked by hand) never compete.
	var cands []quality.Candidate
	var below, blocked int
	for i, r := range results {
		if r.Score < float64(s.cfg.SchedulerMinScore) {
			below++
			continue
		}
		if s.isBlocklisted(r) {
			blocked++
			continue
		}
		cands = append(cands, quality.Candidate{Index: i, Format: search.DetectFormat(r), Score: r.Score, Size: r.Size})
	}
	best, decisions := profile.Choose(cands, item.CurrentFormat)
	for _, d := range decisions {
		r := results[d.Candidate.Index]
		out.Decisions = append(out.Decisions, CandidateSummary{
			Title: r.Title, Source: r.Source, Format: d.Candidate.Format, Score: r.Score,
			Accepted: d.Decision.Accept, Upgrade: d.Decision.Upgrade, Reason: d.Decision.Reason,
		})
	}
	if best == nil {
		out.Action = "searched"
		out.Reason = summariseRejection(len(results), below, blocked, decisions, s.cfg.SchedulerMinScore)
		s.record(item.ID, out)
		return out
	}

	chosen := results[best.Candidate.Index]
	out.Candidate = fmt.Sprintf("%s (%s, %s, score %.0f)", chosen.Title, strings.ToUpper(best.Candidate.Format), chosen.Source, chosen.Score)
	out.Action = "matched"
	out.Reason = best.Decision.Reason

	slog.Info("scheduler: acceptable release",
		"wanted_id", item.ID, "wishlist_title", item.Title, "result_title", chosen.Title,
		"format", best.Candidate.Format, "score", chosen.Score, "source", chosen.Source,
		"upgrade", best.Decision.Upgrade, "dry_run", dryRun)

	s.notifyMatch(item, chosen, best)

	if dryRun {
		s.record(item.ID, out)
		return out
	}

	ref, err := s.startDownload(chosen, item)
	if err != nil {
		out.Action = "error"
		out.Reason = "grab failed: " + err.Error()
		slog.Error("scheduler: auto-download failed", "title", item.Title, "error", err)
		s.record(item.ID, out)
		return out
	}
	if ref != "" {
		_ = s.db.SetWishlistActiveJob(item.ID, ref)
		out.JobID = ref
	}
	if best.Decision.Upgrade {
		out.Action = "upgrade"
		out.Reason = "grabbed " + best.Decision.Reason
	} else {
		out.Action = "grabbed"
		out.Reason = "grabbed " + strings.ToUpper(best.Candidate.Format) + " from " + chosen.Source
	}
	s.record(item.ID, out)
	return out
}

func summariseRejection(total, belowScore, blocked int, decisions []quality.CandidateDecision, minScore int) string {
	var msg string
	if len(decisions) == 0 {
		msg = fmt.Sprintf("%d results, none acceptable", total)
		if blocked == 0 {
			msg = fmt.Sprintf("%d results, none scored %d or higher", total, minScore)
		}
	} else {
		// Report the reason of the highest-scoring rejected candidate: it is
		// the one a person would have looked at first.
		bestIdx := 0
		for i, d := range decisions {
			if d.Candidate.Score > decisions[bestIdx].Candidate.Score {
				bestIdx = i
			}
		}
		msg = fmt.Sprintf("%d results, none acceptable: %s", total, decisions[bestIdx].Decision.Reason)
	}
	if belowScore > 0 {
		msg += fmt.Sprintf(" (%d below score %d)", belowScore, minScore)
	}
	if blocked > 0 {
		msg += fmt.Sprintf(" (%d blocklisted)", blocked)
	}
	return msg
}

// isBlocklisted reports whether any of a release's identifiers is on the
// blocklist: its download URL, magnet/info hash, or Anna's Archive MD5.
func (s *Scheduler) isBlocklisted(r models.SearchResult) bool {
	for _, u := range []string{r.DownloadURL, r.EpubURL, r.MagnetURL, download.AnnasBlocklistURL(r.MD5)} {
		if u != "" && s.db.IsBlocklisted(u, "") {
			return true
		}
	}
	if h := strings.ToLower(strings.TrimSpace(r.InfoHash)); h != "" && s.db.IsBlocklisted("", h) {
		return true
	}
	return false
}

func (s *Scheduler) search(ctx context.Context, item models.WishlistItem) []models.SearchResult {
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

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	results, _ := s.searchMgr.SearchWithAuthor(ctx, tab, query, item.Author)
	return results
}

func (s *Scheduler) notifyMatch(item models.WishlistItem, chosen models.SearchResult, best *quality.CandidateDecision) {
	if s.webhookSender == nil {
		return
	}
	title := "Scheduler Match: " + item.Title
	if best.Decision.Upgrade {
		title = "Upgrade Available: " + item.Title
	}
	s.webhookSender.Send(webhook.Payload{
		Event:   webhook.EventSchedulerMatch,
		Title:   title,
		Message: fmt.Sprintf("Found '%s' (%s) from %s (score: %.0f) — %s", chosen.Title, strings.ToUpper(best.Candidate.Format), chosen.Source, chosen.Score, best.Decision.Reason),
		Status:  "info",
		Extra: map[string]interface{}{
			"wishlist_title": item.Title,
			"wanted_id":      item.ID,
			"result_title":   chosen.Title,
			"format":         best.Candidate.Format,
			"upgrade":        best.Decision.Upgrade,
			"score":          chosen.Score,
			"source":         chosen.Source,
		},
	})
}

// startDownload dispatches the chosen release to the right client and
// returns the in-flight reference to store on the wanted row ("" when the
// grab cannot be tracked, e.g. a torrent with no known info hash).
func (s *Scheduler) startDownload(result models.SearchResult, item models.WishlistItem) (string, error) {
	if s.downloadMgr == nil {
		return "", errors.New("downloads are not available")
	}
	title := item.Title
	switch {
	case result.MD5 != "":
		job, err := s.downloadMgr.StartAnnasDownloadFor(result.MD5, title, item.ID)
		if err != nil {
			return "", err
		}
		return job.ID, nil
	case result.DownloadURL != "" || result.EpubURL != "":
		dlURL := result.DownloadURL
		if dlURL == "" {
			dlURL = result.EpubURL
		}
		job, err := s.downloadMgr.StartDirectDownloadFor(dlURL, title, result.Source, result.SourceID, result.Author, item.ID)
		if err != nil {
			return "", err
		}
		return job.ID, nil
	case result.MagnetURL != "" || result.InfoHash != "":
		url := result.MagnetURL
		if url == "" {
			url = "magnet:?xt=urn:btih:" + result.InfoHash
		}
		err := s.downloadMgr.StartTorrentDownload(url, title, "", "", result.InfoHash)
		if err != nil {
			var verificationWarning *download.TorrentVerificationWarning
			if errors.As(err, &verificationWarning) {
				slog.Warn("scheduler: torrent accepted; verification pending", "title", title, "warning", err.Error())
			} else {
				return "", err
			}
		}
		return download.TorrentWantedRef(result.InfoHash), nil
	}
	return "", errors.New("release has no downloadable link")
}

// record persists the outcome on the row and remembers it for the status API.
func (s *Scheduler) record(wantedID int64, out ItemOutcome) {
	line := out.Reason
	if out.Candidate != "" && (out.Action == "matched" || out.Action == "grabbed" || out.Action == "upgrade") {
		line = out.Reason + ": " + out.Candidate
	}
	_ = s.db.RecordWishlistSearch(wantedID, line)
	s.remember(out)
}

func (s *Scheduler) remember(out ItemOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recent = append(s.recent, out)
	if len(s.recent) > 50 {
		s.recent = s.recent[len(s.recent)-50:]
	}
}

// Status returns the current scheduler status.
func (s *Scheduler) Status() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := map[string]interface{}{
		"enabled":            s.cfg.SchedulerEnabled,
		"interval_hours":     s.cfg.SchedulerIntervalHours,
		"auto_download":      s.cfg.SchedulerAutoDownload,
		"min_score":          s.cfg.SchedulerMinScore,
		"item_delay_seconds": s.cfg.SchedulerItemDelaySeconds,
		"auto_upgrade":       s.cfg.AutoUpgradeEnabled,
		"keep_old_files":     s.cfg.UpgradeKeepOldFiles,
		"running":            s.running,
		"items_found":        s.itemsFound,
	}

	if !s.lastRun.IsZero() {
		status["last_run"] = s.lastRun.Format(time.RFC3339)
		status["last_result"] = s.lastResult
		status["last_stats"] = s.lastStats
	}
	recent := make([]ItemOutcome, len(s.recent))
	copy(recent, s.recent)
	// Newest first, without the per-candidate detail (that is for dry runs).
	for i, j := 0, len(recent)-1; i < j; i, j = i+1, j-1 {
		recent[i], recent[j] = recent[j], recent[i]
	}
	for i := range recent {
		recent[i].Decisions = nil
	}
	if len(recent) > 20 {
		recent = recent[:20]
	}
	status["recent"] = recent
	return status
}
