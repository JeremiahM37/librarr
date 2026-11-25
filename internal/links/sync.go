// Package links keeps track of where Librarr's library items are shelved in
// the user-facing libraries, Kavita and Audiobookshelf, so the Library can
// link out to them.
//
// Items are matched by path. Librarr files each media type under its own
// directory (EBOOK_DIR, AUDIOBOOK_DIR, MANGA_DIR), and Kavita and
// Audiobookshelf each report their library folders. A Kavita series or
// Audiobookshelf item is linked to the Librarr item at the same path relative
// to those roots, so the two containers may mount the library at different
// paths. An item also matches a series at a folder above its own, nearest
// first: Kavita shelves a series whose books sit in several folders by the
// folder they share. A Kavita library folder pointed at a parent or child of
// Librarr's directory is not matched.
package links

import (
	"context"
	"log/slog"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

// Services links can point to.
const (
	ServiceKavita = "kavita"
	ServiceABS    = "abs"
)

const (
	syncStartDelay = 45 * time.Second
	syncInterval   = 15 * time.Minute
	// After an import, the service needs time to finish the scan Librarr
	// asked for before the new item can be found.
	syncKickDelay = time.Minute
)

// entry is one series or item found in an external library, keyed by its
// path relative to the library folder.
type entry struct {
	mediaType string
	relPath   string
	link      models.LibraryLink
}

// Syncer matches library items to Kavita series and Audiobookshelf items and
// stores the links it finds.
type Syncer struct {
	cfg    *config.Config
	db     *db.DB
	client *http.Client
	kick   chan struct{}
	now    func() time.Time
}

// NewSyncer returns a Syncer. Configuration is read on every pass.
func NewSyncer(cfg *config.Config, database *db.DB) *Syncer {
	return &Syncer{
		cfg:    cfg,
		db:     database,
		client: &http.Client{Timeout: 30 * time.Second},
		kick:   make(chan struct{}, 1),
		now:    time.Now,
	}
}

// Kick asks for a pass shortly, as after an import. It never blocks.
func (s *Syncer) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Start runs passes until ctx is cancelled: once soon after start, then on an
// interval, and a minute after a kick. A run of kicks never postpones a pass
// already due.
func (s *Syncer) Start(ctx context.Context) {
	slog.Info("library link sync started", "interval", syncInterval)
	next := s.now().Add(syncStartDelay)
	for {
		timer := time.NewTimer(next.Sub(s.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.kick:
			timer.Stop()
			if soon := s.now().Add(syncKickDelay); soon.Before(next) {
				next = soon
			}
		case <-timer.C:
			s.RunOnce(ctx)
			next = s.now().Add(syncInterval)
		}
	}
}

// RunOnce syncs every service. A service that can't be reached keeps the
// links found last time; one that is no longer configured loses them.
func (s *Syncer) RunOnce(ctx context.Context) {
	items, err := s.db.LinkCandidates()
	if err != nil {
		slog.Warn("link sync: list library items failed", "error", err)
		return
	}
	for _, svc := range []struct {
		name       string
		configured bool
		fetch      func(context.Context) ([]entry, error)
	}{
		{ServiceKavita, s.cfg.HasKavita(), s.kavitaEntries},
		{ServiceABS, s.cfg.HasAudiobookshelf(), s.absEntries},
	} {
		if ctx.Err() != nil {
			return
		}
		var links []models.LibraryLink
		if svc.configured {
			entries, err := svc.fetch(ctx)
			if err != nil {
				slog.Warn("link sync: list external library failed", "service", svc.name, "error", err)
				continue
			}
			links = match(items, entries, s.roots())
		}
		if err := s.db.ReplaceLibraryLinks(svc.name, links, s.now()); err != nil {
			slog.Warn("link sync: store links failed", "service", svc.name, "error", err)
			continue
		}
		if svc.configured {
			slog.Debug("link sync: done", "service", svc.name, "linked", len(links))
		}
	}
}

// roots maps each media type to the directory Librarr files it under.
func (s *Syncer) roots() map[string]string {
	return map[string]string{
		"ebook":     s.cfg.EbookDir,
		"audiobook": s.cfg.AudiobookDir,
		"manga":     s.cfg.MangaDir,
	}
}

// match links each item to the entry of its media type at the same relative
// path, or else at the nearest folder above it.
func match(items []db.LinkCandidate, entries []entry, roots map[string]string) []models.LibraryLink {
	byPath := make(map[string]models.LibraryLink, len(entries))
	for _, e := range entries {
		key := e.mediaType + "\x00" + e.relPath
		if _, dup := byPath[key]; !dup {
			byPath[key] = e.link
		}
	}
	var links []models.LibraryLink
	for _, item := range items {
		for _, rel := range itemRelPaths(item.FilePath, roots[item.MediaType]) {
			if link, ok := byPath[item.MediaType+"\x00"+rel]; ok {
				link.ItemID = item.ID
				links = append(links, link)
				break
			}
		}
	}
	return links
}

// itemRelPaths returns the paths, relative to root and slash-separated, an
// item could be shelved under: its own, then each folder above it, nearest
// first. Both services shelve a book by the folder holding it.
func itemRelPaths(filePath, root string) []string {
	if filePath == "" || root == "" {
		return nil
	}
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(filePath))
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil
	}
	rel = filepath.ToSlash(rel)
	paths := []string{rel}
	for dir := path.Dir(rel); dir != "."; dir = path.Dir(dir) {
		paths = append(paths, dir)
	}
	return paths
}

// relativeTo returns p relative to the first of folders that contains it.
// Both are paths as the external service sees them.
func relativeTo(p string, folders []string) (string, bool) {
	p = path.Clean(p)
	for _, folder := range folders {
		folder = path.Clean(folder)
		if rel, ok := strings.CutPrefix(p, folder+"/"); ok && rel != "" {
			return rel, true
		}
	}
	return "", false
}
