package download

import (
	"context"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/organize"
	"github.com/JeremiahM37/librarr/internal/search"
)

// Watcher monitors the active torrent client for completed torrents and runs
// the import pipeline.
type Watcher struct {
	cfg       *config.Config
	db        *db.DB
	torrent   TorrentClient
	sab       *SABnzbdClient
	organizer *organize.Organizer
	targets   *organize.LibraryTargets
	health    *search.HealthTracker

	processing    sync.Map // hash -> struct{}, tracks in-progress imports
	imported      sync.Map // hash -> struct{}, tracks already-imported hashes
	processingNZB sync.Map // nzo_id -> struct{}, tracks in-progress NZB imports
}

var errTorrentContentPending = errors.New("torrent content pending synchronization")

// NewWatcher creates a new download completion watcher covering both the
// torrent client and SABnzbd.
func NewWatcher(cfg *config.Config, database *db.DB, torrent TorrentClient, sab *SABnzbdClient, organizer *organize.Organizer, targets *organize.LibraryTargets, health *search.HealthTracker) *Watcher {
	return &Watcher{
		cfg:       cfg,
		db:        database,
		torrent:   torrent,
		sab:       sab,
		organizer: organizer,
		targets:   targets,
		health:    health,
	}
}

// Start begins the background watcher loop. It blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) {
	watchNZB := w.sab != nil && w.cfg.HasSABnzbd()
	if w.torrent == nil && !watchNZB {
		slog.Info("download watcher disabled (no torrent client or SABnzbd configured)")
		return
	}

	slog.Info("download completion watcher started", "torrent", w.torrent != nil, "sabnzbd", watchNZB, "interval", "30s")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run once immediately.
	w.poll()

	for {
		select {
		case <-ctx.Done():
			slog.Info("download watcher stopping")
			return
		case <-ticker.C:
			w.poll()
		}
	}
}

func (w *Watcher) poll() {
	if w.torrent != nil {
		w.checkCompleted()
	}
	if w.sab != nil && w.cfg.HasSABnzbd() {
		w.checkCompletedNZB()
	}
}

func (w *Watcher) checkCompleted() {
	categories := []struct {
		name      string
		mediaType string
	}{
		{w.cfg.QBCategory, "ebook"},
		{w.cfg.QBAudiobookCategory, "audiobook"},
		{w.cfg.QBMangaCategory, "manga"},
	}

	for _, cat := range categories {
		torrents, err := w.torrent.GetTorrents(cat.name)
		if err != nil {
			continue
		}
		for _, t := range torrents {
			if t.Progress < 1.0 {
				continue
			}

			// Skip already imported.
			if _, ok := w.imported.Load(t.Hash); ok {
				continue
			}
			// Skip currently processing.
			if _, loaded := w.processing.LoadOrStore(t.Hash, struct{}{}); loaded {
				continue
			}

			go w.importTorrent(t, cat.mediaType)
		}
	}
}

func (w *Watcher) importTorrent(t TorrentInfo, mediaType string) {
	defer w.processing.Delete(t.Hash)

	slog.Debug("processing completed torrent", "name", t.Name, "hash", t.Hash, "type", mediaType)

	savePath := w.resolveLocalPath(t, mediaType)

	var importErr error
	if _, err := os.Stat(savePath); err != nil {
		if os.IsNotExist(err) {
			slog.Info("torrent import pending", "name", t.Name, "hash", t.Hash, "type", mediaType, "path", savePath, "reason", "local synchronized path not available")
		} else {
			slog.Warn("torrent import pending", "name", t.Name, "hash", t.Hash, "type", mediaType, "path", savePath, "error", err)
		}
		return
	}
	var inLibrary bool
	switch mediaType {
	case "ebook":
		inLibrary, importErr = w.importEbook(t, savePath, "torrent")
	case "audiobook":
		inLibrary, importErr = w.importAudiobook(t, savePath, "torrent")
	case "manga":
		inLibrary, importErr = w.importManga(t, savePath, "torrent")
	}

	if importErr != nil {
		if errors.Is(importErr, errTorrentContentPending) {
			slog.Info("torrent import pending", "name", t.Name, "hash", t.Hash, "type", mediaType, "path", savePath, "error", importErr)
		} else {
			slog.Error("torrent import failed", "name", t.Name, "type", mediaType, "error", importErr)
		}
		return
	}

	// Optionally remove the torrent from the client after import. Default is to
	// remove; set REMOVE_TORRENT_AFTER_IMPORT=false to keep the torrent — which
	// only keeps it *seeding* when IMPORT_MODE also leaves the payload in place.
	keepsPayload := w.organizer != nil && w.organizer.KeepsPayload()
	if w.cfg.RemoveTorrentAfterImport {
		// In move mode the payload is already gone, so there is nothing to
		// delete. In hardlink/copy mode it is still sitting in the download
		// folder: delete it with the torrent, but only once every file is
		// known to exist in the library under its own path — a hardlink shares
		// the data, so the library entry survives. If any file failed to
		// organize, the download folder still holds the only copy.
		deleteFiles := keepsPayload && inLibrary
		if err := w.torrent.DeleteTorrent(t.Hash, deleteFiles); err != nil {
			slog.Warn("failed to remove torrent after import", "hash", t.Hash, "error", err)
		} else {
			slog.Info("removed completed torrent", "name", t.Name, "deleted_files", deleteFiles)
		}
	} else if keepsPayload {
		slog.Info("torrent left seeding after import", "name", t.Name, "hash", t.Hash,
			"mode", w.cfg.EffectiveImportMode())
	} else {
		// The record stays, but its files moved into the library, so the
		// torrent cannot actually seed. Say so instead of implying otherwise.
		slog.Info("torrent kept after import but cannot seed: its payload moved into the library",
			"name", t.Name, "hash", t.Hash, "hint", "set IMPORT_MODE=hardlink to keep seeding")
	}

	// Mark as imported.
	w.imported.Store(t.Hash, struct{}{})

	// Log the import.
	if err := w.db.LogEvent("torrent_import", t.Name, fmt.Sprintf("Imported %s from torrent", mediaType), nil, t.Hash); err != nil {
		slog.Warn("failed to log torrent import", "name", t.Name, "hash", t.Hash, "error", err)
	}
}

// sabHistoryLimit bounds how many SABnzbd history entries we scan per poll.
const sabHistoryLimit = 200

// checkCompletedNZB imports SABnzbd downloads that librarr submitted and that
// have finished post-processing. Because SABnzbd exposes only a single
// category, the media type is recovered from the nzb_jobs table (recorded at
// submit time) rather than from the history entry.
func (w *Watcher) checkCompletedNZB() {
	pending, err := w.db.PendingNZBJobs()
	if err != nil {
		slog.Warn("failed to list pending NZB jobs", "error", err)
		return
	}
	if len(pending) == 0 {
		return
	}

	mediaByNzo := make(map[string]string, len(pending))
	for _, j := range pending {
		mediaByNzo[j.NzoID] = j.MediaType
	}

	history, err := w.sab.GetHistory(sabHistoryLimit)
	if err != nil {
		slog.Warn("failed to fetch SABnzbd history", "error", err)
		return
	}

	for _, slot := range history {
		mediaType, tracked := mediaByNzo[slot.NzoID]
		if !tracked {
			continue
		}
		switch strings.ToLower(slot.Status) {
		case "completed":
			// Guard against a second goroutine for the same job while the
			// first is still importing.
			if _, loaded := w.processingNZB.LoadOrStore(slot.NzoID, struct{}{}); loaded {
				continue
			}
			go w.importNZB(slot, mediaType)
		case "failed":
			// A failed job never produces files; stop tracking it so it does
			// not linger in the pending list forever.
			slog.Warn("SABnzbd download failed", "name", slot.Name, "nzo_id", slot.NzoID, "reason", slot.FailMessage)
			if err := w.db.MarkNZBJobImported(slot.NzoID); err != nil {
				slog.Warn("failed to mark failed NZB job resolved", "nzo_id", slot.NzoID, "error", err)
			}
		}
	}
}

func (w *Watcher) importNZB(slot SABnzbdHistorySlot, mediaType string) {
	defer w.processingNZB.Delete(slot.NzoID)

	savePath := w.resolveNZBPath(slot, mediaType)
	if _, err := os.Stat(savePath); err != nil {
		// Not visible on the local mount yet — retry on the next poll.
		slog.Info("nzb import pending", "name", slot.Name, "nzo_id", slot.NzoID, "type", mediaType, "path", savePath)
		return
	}

	t := TorrentInfo{Name: slot.Name, Hash: slot.NzoID}
	var importErr error
	switch mediaType {
	case "audiobook":
		_, importErr = w.importAudiobook(t, savePath, "nzb")
	case "manga":
		_, importErr = w.importManga(t, savePath, "nzb")
	default:
		_, importErr = w.importEbook(t, savePath, "nzb")
	}

	if importErr != nil {
		if errors.Is(importErr, errTorrentContentPending) {
			slog.Info("nzb import pending", "name", slot.Name, "nzo_id", slot.NzoID, "type", mediaType, "path", savePath, "error", importErr)
		} else {
			slog.Error("nzb import failed", "name", slot.Name, "type", mediaType, "error", importErr)
		}
		return
	}

	if err := w.db.MarkNZBJobImported(slot.NzoID); err != nil {
		slog.Warn("failed to mark NZB job imported", "nzo_id", slot.NzoID, "error", err)
	}
	if err := w.db.LogEvent("nzb_import", slot.Name, fmt.Sprintf("Imported %s from SABnzbd", mediaType), nil, slot.NzoID); err != nil {
		slog.Warn("failed to log nzb import", "name", slot.Name, "nzo_id", slot.NzoID, "error", err)
	}
}

// resolveNZBPath returns the local path of a completed SABnzbd download. It
// prefers the storage path SABnzbd reports (valid when librarr and SABnzbd
// share the completed-downloads mount) and falls back to the media type's
// incoming directory joined with the job name.
func (w *Watcher) resolveNZBPath(slot SABnzbdHistorySlot, mediaType string) string {
	if storage := normalizeTorrentPath(slot.Storage); storage != "" {
		if _, err := os.Stat(storage); err == nil {
			return storage
		}
	}
	return filepath.Join(w.incomingDirForMedia(mediaType), normalizeTorrentPath(slot.Name))
}

// resolveLocalPath maps qBittorrent container paths to local paths.
// Each media type uses its dedicated INCOMING directory (where qBit
// downloads to), NOT the final organized library directory. Previously
// audiobooks used AudiobookDir (the library target) which caused files
// to be looked up in the wrong place and could lead to ebooks being
// misrouted if paths overlapped.
func (w *Watcher) resolveLocalPath(t TorrentInfo, mediaType string) string {
	var rootName string

	if t.ContentPath != "" {
		return w.resolveContentPath(t, mediaType)
	}

	// Fetch files from qBittorrent to find the actual root folder/file.
	var files []TorrentFile
	var err error
	if w.torrent != nil {
		files, err = w.torrent.GetTorrentFiles(t.Hash)
	}
	if err == nil && len(files) > 0 {
		var firstPart string
		allSameRoot := true
		for i, f := range files {
			parts := strings.Split(f.Name, "/")
			if len(parts) > 0 {
				if i == 0 {
					firstPart = parts[0]
				} else if firstPart != parts[0] {
					allSameRoot = false
					break
				}
			}
		}
		if allSameRoot && firstPart != "" {
			rootName = normalizeTorrentPath(firstPart)
		}
	}

	if rootName == "" {
		rootName = normalizeTorrentPath(t.Name)
	}

	return filepath.Join(w.incomingDirForMedia(mediaType), rootName)
}

func (w *Watcher) incomingDirForMedia(mediaType string) string {
	switch mediaType {
	case "ebook":
		return w.cfg.IncomingDir
	case "audiobook":
		if w.cfg.QBAudiobookSavePath != "" {
			return w.cfg.QBAudiobookSavePath
		}
		return w.cfg.IncomingDir
	case "manga":
		if w.cfg.MangaIncomingDir != "" {
			return w.cfg.MangaIncomingDir
		}
		return w.cfg.IncomingDir
	default:
		return w.cfg.IncomingDir
	}
}

func (w *Watcher) remoteSavePathForMedia(mediaType string) string {
	switch mediaType {
	case "audiobook":
		return w.cfg.QBAudiobookSavePath
	case "manga":
		return w.cfg.QBMangaSavePath
	default:
		return w.cfg.QBSavePath
	}
}

func (w *Watcher) resolveContentPath(t TorrentInfo, mediaType string) string {
	contentPath := normalizeTorrentPath(t.ContentPath)
	contentPath = filepath.Clean(contentPath)
	savePath := filepath.Clean(normalizeTorrentPath(t.SavePath))
	localIncoming := w.incomingDirForMedia(mediaType)

	if contentPath == "" {
		return localIncoming
	}

	remoteRoots := []string{savePath, filepath.Clean(normalizeTorrentPath(w.remoteSavePathForMedia(mediaType)))}
	for _, remoteRoot := range remoteRoots {
		if mapped, ok := mapTorrentPath(contentPath, remoteRoot, localIncoming); ok {
			return mapped
		}
	}

	if mapped, ok := mapTorrentPath(contentPath, localIncoming, localIncoming); ok {
		return mapped
	}

	if !filepath.IsAbs(contentPath) && contentPath != ".." && !strings.HasPrefix(contentPath, ".."+string(os.PathSeparator)) {
		return filepath.Join(localIncoming, contentPath)
	}

	return filepath.Join(localIncoming, filepath.Base(contentPath))
}

// mapTorrentPath translates a path reported by the remote torrent client into
// the corresponding locally mounted path. It only succeeds for paths at or
// below remoteRoot, preventing traversal and unrelated absolute paths from
// escaping localRoot.
func mapTorrentPath(reportedPath, remoteRoot, localRoot string) (string, bool) {
	reportedPath = filepath.Clean(normalizeTorrentPath(reportedPath))
	remoteRoot = filepath.Clean(normalizeTorrentPath(remoteRoot))
	localRoot = filepath.Clean(normalizeTorrentPath(localRoot))
	if reportedPath == "." || remoteRoot == "." || localRoot == "." ||
		!filepath.IsAbs(reportedPath) || !filepath.IsAbs(remoteRoot) || !filepath.IsAbs(localRoot) {
		return "", false
	}

	rel, err := filepath.Rel(remoteRoot, reportedPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return filepath.Join(localRoot, rel), true
}

// importEbook organizes every ebook found under savePath. The bool reports
// whether all of them now live in the library under a path of their own, i.e.
// whether the download folder still holds the only copy of anything.
func (w *Watcher) importEbook(t TorrentInfo, savePath, source string) (bool, error) {
	bookFiles := findFilesByExt(savePath, []string{".epub", ".mobi", ".pdf", ".azw3"})
	if len(bookFiles) == 0 {
		return false, fmt.Errorf("%w: no ebook files found at %s", errTorrentContentPending, savePath)
	}

	inLibrary := true
	for _, bf := range bookFiles {
		metadata := organize.ExtractEbookMetadata(bf)
		title := firstNonEmpty(metadata.Title, t.Name)
		author := metadata.Author
		destPath, err := w.organizer.OrganizeEbook(bf, title, author)
		if err != nil {
			slog.Warn("organize ebook failed", "file", bf, "error", err)
			destPath = bf
		}
		if destPath == bf {
			inLibrary = false
		}

		inserted, err := w.recordTorrentItem(source, t, "ebook", bf, destPath, title, author, metadata.Title, metadata.Author, fileFormat(destPath), t.TotalSize)
		if err != nil {
			return false, err
		}

		// Import to external libraries.
		if inserted && w.targets != nil {
			w.targets.ImportEbook(destPath, t.Name, author)
		}
	}

	return inLibrary, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// importAudiobook organizes the audiobook at savePath. See importEbook for the
// meaning of the returned bool.
func (w *Watcher) importAudiobook(t TorrentInfo, savePath, source string) (bool, error) {
	// If the source path doesn't even exist, fail the import.
	if _, statErr := os.Stat(savePath); os.IsNotExist(statErr) {
		return false, fmt.Errorf("%w: source path does not exist: %s", errTorrentContentPending, savePath)
	}

	// Extract author from torrent name if possible.
	author := ""
	title := t.Name
	if strings.Contains(title, " - ") {
		parts := strings.SplitN(title, " - ", 2)
		author = strings.TrimSpace(parts[0])
		title = strings.TrimSpace(parts[1])
	}
	if author == "" {
		author = "Unknown"
	}

	destPath, err := w.organizer.OrganizeAudiobook(savePath, title, author)
	if err != nil {
		return false, fmt.Errorf("organize audiobook %q: %w", savePath, err)
	}

	inserted, err := w.recordTorrentItem(source, t, "audiobook", savePath, destPath, title, author, title, author, fileFormat(destPath), t.TotalSize)
	if err != nil {
		return false, err
	}

	if inserted && w.targets != nil {
		w.targets.ImportAudiobook()
	}

	return destPath != savePath, nil
}

// importManga organizes every manga file found under savePath. See importEbook
// for the meaning of the returned bool.
func (w *Watcher) importManga(t TorrentInfo, savePath, source string) (bool, error) {
	mangaFiles := findFilesByExt(savePath, []string{".cbz", ".cbr", ".zip", ".pdf", ".epub"})
	if len(mangaFiles) == 0 {
		return false, fmt.Errorf("%w: no manga files found at %s", errTorrentContentPending, savePath)
	}

	inLibrary := true
	for _, mf := range mangaFiles {
		destPath, err := w.organizer.OrganizeManga(mf, t.Name)
		if err != nil {
			slog.Warn("organize manga failed", "file", mf, "error", err)
			destPath = mf
		}
		if destPath == mf {
			inLibrary = false
		}

		inserted, err := w.recordTorrentItem(source, t, "manga", mf, destPath, t.Name, "", t.Name, "", fileFormat(destPath), t.TotalSize)
		if err != nil {
			return false, err
		}

		if inserted && w.targets != nil {
			w.targets.ImportManga(destPath, t.Name)
		}
	}

	return inLibrary, nil
}

func (w *Watcher) recordTorrentItem(source string, t TorrentInfo, mediaType, sourcePath, destinationPath, title, author, metadataTitle, metadataAuthor, format string, fileSize int64) (bool, error) {
	if info, err := os.Stat(destinationPath); err == nil && info.Mode().IsRegular() {
		fileSize = info.Size()
	}
	item := &models.LibraryItem{
		Title:        title,
		Author:       author,
		FilePath:     destinationPath,
		OriginalPath: sourcePath,
		FileSize:     fileSize,
		FileFormat:   format,
		MediaType:    mediaType,
		Source:       source,
		SourceID:     t.Hash,
	}
	outcome, err := w.db.AddItemWithOutcome(item)
	fields := []any{
		"torrent_hash", t.Hash,
		"torrent_name", t.Name,
		"source_path", sourcePath,
		"normalized_source_path", db.NormalizeLibraryPath(sourcePath),
		"destination_path", destinationPath,
		"normalized_destination_path", outcome.NormalizedPath,
		"file_size", fileSize,
		"detected_format", format,
		"metadata_title", metadataTitle,
		"metadata_author", metadataAuthor,
		"content_hash", outcome.ContentHash,
		"existing_record_id", outcome.ExistingID,
		"duplicate_decision", outcome.Reason,
		"database_decision", map[bool]string{true: "insert", false: "reuse_existing"}[outcome.Inserted],
	}
	if err != nil {
		slog.Error("torrent library import database failure", append(fields, "error", err)...)
		return false, err
	}
	slog.Info("torrent library import decision", fields...)
	return outcome.Inserted, nil
}

func fileFormat(filePath string) string {
	return strings.TrimPrefix(strings.ToLower(filepath.Ext(filePath)), ".")
}

// normalizeTorrentPath unescapes HTML entities (e.g. &amp; -> &) that
// qBittorrent may embed in torrent names.
func normalizeTorrentPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return html.UnescapeString(value)
}

// findFilesByExt recursively finds files with given extensions.
func findFilesByExt(root string, exts []string) []string {
	var files []string

	info, err := os.Stat(root)
	if err != nil {
		return files
	}

	if !info.IsDir() {
		lower := strings.ToLower(root)
		for _, ext := range exts {
			if strings.HasSuffix(lower, ext) {
				return []string{root}
			}
		}
		return files
	}

	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		lower := strings.ToLower(path)
		for _, ext := range exts {
			if strings.HasSuffix(lower, ext) {
				files = append(files, path)
				break
			}
		}
		return nil
	}); err != nil {
		slog.Warn("failed to scan torrent files", "root", root, "error", err)
	}

	return files
}
