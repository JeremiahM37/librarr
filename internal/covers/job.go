package covers

import (
	"context"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/organize"
)

const (
	jobBatchSize     = 25
	jobRetryAfter    = 24 * time.Hour
	jobItemDelay     = time.Second
	jobStartDelay    = 30 * time.Second
	jobCheckInterval = 5 * time.Minute
)

// coverFileName matches the names Store.Save produces.
var coverFileName = regexp.MustCompile(`^[0-9]+\.(jpg|png|gif|webp)$`)

// Job gives local library items a saved cover: the one embedded in the book,
// for EPUBs. Items without one stay on the placeholder and are checked again
// after a day.
type Job struct {
	db         *db.DB
	store      *Store
	mediaTypes func() []string
	delay      time.Duration
	now        func() time.Time
}

// NewJob returns a Job. mediaTypes is read on every pass.
func NewJob(database *db.DB, store *Store, mediaTypes func() []string) *Job {
	return &Job{
		db:         database,
		store:      store,
		mediaTypes: mediaTypes,
		delay:      jobItemDelay,
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
			if !j.process(ctx, &items[i]) {
				return
			}
			if !sleepCtx(ctx, j.delay) {
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

// process finds and saves one item's cover. It returns false when the database
// can't record the outcome, so the caller stops instead of looping.
func (j *Job) process(ctx context.Context, item *models.LibraryItem) bool {
	file := j.fetch(item)
	if file == "" && ctx.Err() != nil {
		return true // cancelled: leave the item unmarked
	}
	if err := j.db.SetItemCover(item.ID, file, j.now()); err != nil {
		slog.Warn("cover job: record cover failed", "id", item.ID, "error", err)
		return false
	}
	return true
}

// fetch returns the saved cover file name, or "" when none could be found.
func (j *Job) fetch(item *models.LibraryItem) string {
	if !strings.EqualFold(filepath.Ext(item.FilePath), ".epub") {
		return ""
	}
	image, err := organize.ExtractEPUBCover(item.FilePath, MaxImageBytes)
	if err != nil {
		slog.Debug("cover job: no embedded cover", "id", item.ID, "error", err)
		return ""
	}
	if _, _, ok := DetectImage(image); !ok {
		slog.Debug("cover job: embedded cover is not an image", "id", item.ID)
		return ""
	}
	file, err := j.store.Save(item.ID, image)
	if err != nil {
		slog.Warn("cover job: save failed", "id", item.ID, "error", err)
		return ""
	}
	return file
}

// sweep removes cover files no item references. If the directory can't be
// listed it removes nothing.
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
