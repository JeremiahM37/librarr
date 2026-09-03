package download

import (
	"log/slog"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/quality"
)

// importedFormat is the format recorded for a freshly imported file: what is
// actually on disk, not what the source claimed. Direct downloads used to be
// recorded as "epub" unconditionally, which made a PDF look like it already
// met an EPUB cutoff.
func importedFormat(destPath string) string {
	if f := quality.FormatFromPath(destPath); f != "" {
		return f
	}
	return "epub"
}

// releaseRef identifies the release a grab came from, for the blocklist.
type releaseRef struct {
	ref      string // in-flight marker: job ID or "torrent:<hash>"
	source   string
	url      string // download URL, or "annas:md5:<md5>" for Anna's grabs
	infoHash string
}

// AnnasBlocklistURL is the blocklist key used for an Anna's Archive MD5.
func AnnasBlocklistURL(md5 string) string {
	if md5 == "" {
		return ""
	}
	return "annas:md5:" + strings.ToLower(md5)
}

// wantedImported settles a finished job against the wanted row it served.
func (m *Manager) wantedImported(job *models.DownloadJob, outcome db.AddItemOutcome) {
	if job == nil || job.WantedID == 0 || outcome.ID == 0 {
		return
	}
	ref := releaseRef{ref: job.ID, source: job.Source, url: job.URL}
	if job.MD5 != "" {
		ref.url = AnnasBlocklistURL(job.MD5)
	}
	settleWantedImport(m.db, m.organizer, m.cfg, job.WantedID, outcome, ref)
}

// TorrentWantedRef is the active-job marker a wanted row carries while a
// torrent grab is in flight; the completion watcher matches on it.
func TorrentWantedRef(infoHash string) string {
	h := strings.ToLower(strings.TrimSpace(infoHash))
	if h == "" {
		return ""
	}
	return "torrent:" + h
}

// linkTorrentToWanted is the watcher-side hook: a torrent import just landed,
// settle it against the wanted row (if any) waiting on this info hash.
func linkTorrentToWanted(database *db.DB, organizer *organize.Organizer, cfg *config.Config, t TorrentInfo, outcome db.AddItemOutcome) {
	if database == nil || outcome.ID == 0 {
		return
	}
	ref := TorrentWantedRef(t.Hash)
	if ref == "" {
		return
	}
	item, err := database.FindWishlistByActiveJob(ref)
	if err != nil || item == nil {
		return
	}
	settleWantedImport(database, organizer, cfg, item.ID, outcome, releaseRef{ref: ref, source: "torrent", infoHash: strings.ToLower(t.Hash)})
}

// settleWantedImport decides what a landed file means for its wanted row:
//
//   - no file yet            → the row is satisfied by it;
//   - a strictly better one  → upgrade: link it, retire the old file;
//   - nothing better         → the release lied about (or shares) its format:
//     reject it, blocklist the release so the next pass skips it, and remove
//     the file if this import created it — never the file already held.
//
// Every failure path leaves both files in place: a stray extra copy is
// recoverable, a wrongly deleted book is not.
func settleWantedImport(database *db.DB, organizer *organize.Organizer, cfg *config.Config, wantedID int64, outcome db.AddItemOutcome, rel releaseRef) {
	item, err := database.GetWishlistItem(wantedID)
	if err != nil || item == nil {
		return
	}
	newItem, err := database.GetItem(outcome.ID)
	if err != nil || newItem == nil {
		return
	}
	newFormat := strings.ToUpper(newItem.FileFormat)

	if item.LibraryItemID != 0 {
		profile := database.ResolveQualityProfile(item.QualityProfileID, item.MediaType).Profile()
		notBetter := item.LibraryItemID == outcome.ID
		reason := "delivered " + newFormat + " again"
		if !notBetter {
			d := profile.Evaluate(newItem.FileFormat, item.CurrentFormat)
			if !d.Accept {
				notBetter = true
				reason = "delivered " + newFormat + ": " + d.Reason
			}
		}
		if notBetter {
			rejectWantedImport(database, organizer, item, newItem, outcome, rel, reason)
			return
		}
	}

	previous, err := database.SatisfyWishlistItem(wantedID, outcome.ID)
	if err != nil {
		slog.Warn("wanted: failed to link library item", "wanted_id", wantedID, "library_item_id", outcome.ID, "error", err)
		return
	}
	if previous == 0 {
		slog.Info("wanted: item satisfied", "wanted_id", wantedID, "library_item_id", outcome.ID, "format", newItem.FileFormat, "ref", rel.ref)
		_ = database.LogEvent("wanted_satisfied", newItem.Title, "Wanted item satisfied ("+newFormat+")", &outcome.ID, "")
		_ = database.RecordWishlistSearch(wantedID, "imported "+newFormat)
		return
	}
	retireSuperseded(database, organizer, cfg, wantedID, previous, newItem)
}

// rejectWantedImport handles a grab that did not improve on the file the row
// already has. The release is blocklisted so the scheduler will not pick it
// again, and the freshly imported duplicate (only if this import inserted it)
// is removed from the library.
func rejectWantedImport(database *db.DB, organizer *organize.Organizer, item *models.WishlistItem, newItem *models.LibraryItem, outcome db.AddItemOutcome, rel releaseRef, reason string) {
	_ = database.SetWishlistActiveJob(item.ID, "")
	msg := reason + "; release blocklisted"
	if rel.url != "" || rel.infoHash != "" {
		if _, err := database.AddBlocklistEntry(item.Title, rel.source, rel.url, rel.infoHash, "wanted: "+reason); err != nil {
			slog.Warn("wanted: failed to blocklist release", "wanted_id", item.ID, "error", err)
			msg = reason
		}
	} else {
		msg = reason
	}
	slog.Warn("wanted: import rejected", "wanted_id", item.ID, "title", item.Title, "reason", reason, "url", rel.url, "info_hash", rel.infoHash)
	_ = database.RecordWishlistSearch(item.ID, msg)
	_ = database.LogEvent("wanted_rejected", item.Title, "Grab rejected after import: "+msg, &outcome.ID, "")

	if !outcome.Inserted || outcome.ID == item.LibraryItemID || organizer == nil {
		return // the row/file pre-existed, or is the very file the item holds
	}
	if err := organizer.RemoveSuperseded(newItem.FilePath, newItem.ContentHash); err != nil {
		slog.Warn("wanted: rejected file left in place", "path", newItem.FilePath, "reason", err)
		return
	}
	_ = database.DeleteItem(newItem.ID)
}

// retireSuperseded removes the library row (and, by default, the file) that
// an upgrade replaced. Every failure leaves both in place: a stray extra copy
// is recoverable, a wrongly deleted book is not.
func retireSuperseded(database *db.DB, organizer *organize.Organizer, cfg *config.Config, wantedID, previousID int64, newItem *models.LibraryItem) {
	old, err := database.GetItem(previousID)
	if err != nil || old == nil {
		return // the old row is already gone; nothing to retire
	}
	newFormat, newPath := "", ""
	var newID *int64
	if newItem != nil {
		newFormat, newPath = newItem.FileFormat, newItem.FilePath
		newID = &newItem.ID
	}
	detail := "Upgraded " + strings.ToUpper(old.FileFormat) + " → " + strings.ToUpper(newFormat)
	_ = database.RecordWishlistSearch(wantedID, "upgraded "+strings.ToUpper(old.FileFormat)+" → "+strings.ToUpper(newFormat))

	if cfg != nil && cfg.UpgradeKeepOldFiles {
		slog.Info("wanted: upgrade complete, keeping superseded file", "wanted_id", wantedID, "old_path", old.FilePath, "new_path", newPath)
		_ = database.LogEvent("wanted_upgraded", old.Title, detail+" (old file kept)", newID, "")
		return
	}
	if db.NormalizeLibraryPath(old.FilePath) == db.NormalizeLibraryPath(newPath) {
		// Same destination path: the organizer overwrote it, so the old row
		// is a duplicate pointing at the new bytes. Drop the row only.
		_ = database.DeleteItem(old.ID)
		return
	}
	if organizer == nil {
		slog.Warn("wanted: no organizer, leaving superseded file in place", "old_path", old.FilePath)
		return
	}
	if err := organizer.RemoveSuperseded(old.FilePath, old.ContentHash); err != nil {
		slog.Warn("wanted: superseded file left in place", "wanted_id", wantedID, "old_path", old.FilePath, "reason", err)
		_ = database.LogEvent("wanted_upgraded", old.Title, detail+" (old file left in place: "+err.Error()+")", newID, "")
		return
	}
	if err := database.DeleteItem(old.ID); err != nil {
		slog.Warn("wanted: superseded library row not removed", "library_item_id", old.ID, "error", err)
	}
	slog.Info("wanted: upgrade complete, superseded file removed", "wanted_id", wantedID, "old_path", old.FilePath, "new_path", newPath)
	_ = database.LogEvent("wanted_upgraded", old.Title, detail, newID, "")
}
