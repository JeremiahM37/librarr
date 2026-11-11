package covers

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/metadata"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/organize"
)

const (
	jobBatchSize    = 25
	jobRetryAfter   = 24 * time.Hour
	jobLookupMaxAge = 30 * 24 * time.Hour
	// Lookups are paced by the resolver; this spaces out image downloads, which
	// Open Library asks people not to fetch in bulk.
	jobDownloadDelay = time.Second
	jobStartDelay    = 30 * time.Second
	jobCheckInterval = 5 * time.Minute
)

// coverFileName matches the names Store.Save produces.
var coverFileName = regexp.MustCompile(`^[0-9]+\.(jpg|png|gif|webp)$`)

// Resolver finds a cover image URL for a book.
type Resolver interface {
	Resolve(ctx context.Context, kind metadata.CoverKind, title, author string) (string, error)
}

// Job gives local library items a saved cover: the one embedded in the book
// when there is one, otherwise a cover found through a lookup. Failures leave
// the item on the placeholder and are retried after a day.
type Job struct {
	db         *db.DB
	store      *Store
	resolver   Resolver
	mediaTypes func() []string
	delay      time.Duration
	now        func() time.Time
}

// NewJob returns a Job. mediaTypes is read on every pass.
func NewJob(database *db.DB, store *Store, resolver Resolver, mediaTypes func() []string) *Job {
	return &Job{
		db:         database,
		store:      store,
		resolver:   resolver,
		mediaTypes: mediaTypes,
		delay:      jobDownloadDelay,
		now:        time.Now,
	}
}

// Start runs the job periodically until ctx is cancelled.
func (j *Job) Start(ctx context.Context) {
	slog.Info("cover job started", "interval", jobCheckInterval)
	ticker := time.NewTicker(jobCheckInterval)
	defer ticker.Stop()

	// Run once after a short delay.
	if !sleepCtx(ctx, jobStartDelay) {
		return
	}
	j.RunOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Info("cover job stopping")
			return
		case <-ticker.C:
			j.RunOnce(ctx)
		}
	}
}

// RunOnce restores missing files, processes every item that is due and then
// removes files no item references. It runs in a single goroutine, so the
// sweep never overlaps a write.
func (j *Job) RunOnce(ctx context.Context) {
	j.regenerateMissing()

	for ctx.Err() == nil {
		items, err := j.db.ItemsNeedingCover(j.mediaTypes(), j.now().Add(-jobRetryAfter), jobBatchSize)
		if err != nil {
			slog.Warn("cover job: list items failed", "error", err)
			break
		}
		if len(items) == 0 {
			break
		}
		for i := range items {
			if ctx.Err() != nil {
				break
			}
			downloaded, ok := j.process(ctx, &items[i])
			if !ok {
				return
			}
			if downloaded && !sleepCtx(ctx, j.delay) {
				break
			}
		}
	}
	if ctx.Err() != nil {
		return
	}
	j.sweep()
}

// regenerateMissing forgets covers whose files are gone, as after restoring
// a database backup without the covers folder. A file the store can't check keeps
// its row: clearing it would make the sweep delete the file once it is readable.
func (j *Job) regenerateMissing() {
	files, err := j.db.CoverFiles()
	if err != nil {
		slog.Warn("cover job: list covers failed", "error", err)
		return
	}
	unchecked := 0
	var checkErr error
	for id, file := range files {
		ok, err := j.store.Exists(file)
		if err != nil {
			unchecked++
			checkErr = err
			continue
		}
		if ok {
			continue
		}
		if err := j.db.ClearItemCover(id); err != nil {
			slog.Warn("cover job: clear cover failed", "id", id, "error", err)
		}
	}
	if unchecked > 0 {
		slog.Warn("cover job: check cover files failed", "files", unchecked, "error", checkErr)
	}
}

// process finds and saves one item's cover, reporting whether it downloaded an
// image. ok is false when lookups are cooling down, which leaves the item for the
// next pass, or when the database can't record the outcome, so the caller stops
// instead of looping.
func (j *Job) process(ctx context.Context, item *models.LibraryItem) (downloaded, ok bool) {
	file, downloaded, err := j.fetch(ctx, item)
	if errors.Is(err, metadata.ErrCoverCoolingDown) {
		slog.Debug("cover job: lookups cooling down, stopping this pass", "id", item.ID, "error", err)
		return false, false
	}
	if file == "" && ctx.Err() != nil {
		return downloaded, true // cancelled: leave the item unmarked
	}
	if err := j.db.SetItemCover(item.ID, file, j.now()); err != nil {
		slog.Warn("cover job: record cover failed", "id", item.ID, "error", err)
		return downloaded, false
	}
	return downloaded, true
}

// fetch returns the saved cover file name, or "" when none could be found, and
// whether it tried to download an image. The only error is
// metadata.ErrCoverCoolingDown, when the item has not been judged.
func (j *Job) fetch(ctx context.Context, item *models.LibraryItem) (file string, downloaded bool, err error) {
	var image []byte
	if strings.EqualFold(filepath.Ext(item.FilePath), ".epub") {
		b, err := organize.ExtractEPUBCover(item.FilePath, MaxImageBytes)
		if err != nil {
			slog.Debug("cover job: no embedded cover", "id", item.ID, "error", err)
		} else if _, _, ok := DetectImage(b); ok {
			image = b
		} else {
			slog.Debug("cover job: embedded cover is not an image", "id", item.ID)
		}
	}
	if image == nil {
		coverURL, err := j.resolver.Resolve(ctx, metadata.CoverKindBook, item.Title, item.Author)
		if errors.Is(err, metadata.ErrCoverCoolingDown) {
			return "", false, err
		}
		if err != nil {
			slog.Debug("cover job: lookup failed", "id", item.ID, "error", err)
			return "", false, nil
		}
		if coverURL == "" {
			slog.Debug("cover job: no cover found", "id", item.ID)
			return "", false, nil
		}
		downloaded = true
		image, err = j.store.Download(ctx, coverURL)
		if err != nil {
			slog.Debug("cover job: download failed", "id", item.ID, "error", err)
			return "", true, nil
		}
	}
	file, err = j.store.Save(item.ID, image)
	if err != nil {
		slog.Warn("cover job: save failed", "id", item.ID, "error", err)
		return "", downloaded, nil
	}
	return file, downloaded, nil
}

// sweep removes cover files no item references and old cached lookups. If the
// directory can't be listed it removes nothing.
func (j *Job) sweep() {
	files, err := j.db.CoverFiles()
	if err != nil {
		slog.Warn("cover job: list covers failed", "error", err)
		return
	}
	used := make(map[string]bool, len(files))
	for _, f := range files {
		used[f] = true
	}
	names, err := j.store.List()
	if err != nil {
		slog.Warn("cover job: list cover files failed", "error", err)
		return
	}
	for _, name := range names {
		if used[name] || !coverFileName.MatchString(name) {
			continue
		}
		if err := j.store.Remove(name); err != nil {
			slog.Warn("cover job: remove orphan failed", "file", name, "error", err)
		}
	}
	if err := j.db.PruneCoverLookups(j.now().Add(-jobLookupMaxAge)); err != nil {
		slog.Warn("cover job: prune lookups failed", "error", err)
	}
}

// sleepCtx waits for d and reports false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
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
