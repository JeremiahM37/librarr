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
	organizer *organize.Organizer
	targets   *organize.LibraryTargets
	health    *search.HealthTracker

	processing sync.Map // hash -> struct{}, tracks in-progress imports
	imported   sync.Map // hash -> struct{}, tracks already-imported hashes
}

var errTorrentContentPending = errors.New("torrent content pending synchronization")

// NewWatcher creates a new torrent completion watcher.
func NewWatcher(cfg *config.Config, database *db.DB, torrent TorrentClient, organizer *organize.Organizer, targets *organize.LibraryTargets, health *search.HealthTracker) *Watcher {
	return &Watcher{
		cfg:       cfg,
		db:        database,
		torrent:   torrent,
		organizer: organizer,
		targets:   targets,
		health:    health,
	}
}

// Start begins the background watcher loop. It blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) {
	if w.torrent == nil {
		slog.Info("torrent watcher disabled (no torrent client configured)")
		return
	}

	slog.Info("torrent completion watcher started", "client", w.torrent.Name(), "interval", "30s")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run once immediately.
	w.checkCompleted()

	for {
		select {
		case <-ctx.Done():
			slog.Info("torrent watcher stopping")
			return
		case <-ticker.C:
			w.checkCompleted()
		}
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
	switch mediaType {
	case "ebook":
		importErr = w.importEbook(t, savePath)
	case "audiobook":
		importErr = w.importAudiobook(t, savePath)
	case "manga":
		importErr = w.importManga(t, savePath)
	}

	if importErr != nil {
		if errors.Is(importErr, errTorrentContentPending) {
			slog.Info("torrent import pending", "name", t.Name, "hash", t.Hash, "type", mediaType, "path", savePath, "error", importErr)
		} else {
			slog.Error("torrent import failed", "name", t.Name, "type", mediaType, "error", importErr)
		}
		return
	}

	// Optionally remove torrent from qBit after import. Default is to remove;
	// set REMOVE_TORRENT_AFTER_IMPORT=false to keep seeding (e.g. private trackers).
	if w.cfg.RemoveTorrentAfterImport {
		if err := w.torrent.DeleteTorrent(t.Hash, false); err != nil {
			slog.Warn("failed to remove torrent after import", "hash", t.Hash, "error", err)
		} else {
			slog.Info("removed completed torrent", "name", t.Name)
		}
	} else {
		slog.Info("torrent left seeding after import", "name", t.Name, "hash", t.Hash)
	}

	// Mark as imported.
	w.imported.Store(t.Hash, struct{}{})

	// Log the import.
	if err := w.db.LogEvent("torrent_import", t.Name, fmt.Sprintf("Imported %s from torrent", mediaType), nil, t.Hash); err != nil {
		slog.Warn("failed to log torrent import", "name", t.Name, "hash", t.Hash, "error", err)
	}
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

func (w *Watcher) importEbook(t TorrentInfo, savePath string) error {
	bookFiles := findFilesByExt(savePath, []string{".epub", ".mobi", ".pdf", ".azw3"})
	if len(bookFiles) == 0 {
		return fmt.Errorf("%w: no ebook files found at %s", errTorrentContentPending, savePath)
	}

	for _, bf := range bookFiles {
		destPath, err := w.organizer.OrganizeEbook(bf, t.Name, "")
		if err != nil {
			slog.Warn("organize ebook failed", "file", bf, "error", err)
			destPath = bf
		}

		// Try to extract author from EPUB metadata.
		author := ""
		metadataTitle := t.Name
		if strings.HasSuffix(strings.ToLower(destPath), ".epub") {
			if meta, err := organize.ExtractEPUBMeta(destPath); err == nil {
				if meta.Author != "" {
					author = meta.Author
				}
				if meta.Title != "" {
					metadataTitle = meta.Title
				}
			}
		}

		inserted, err := w.recordTorrentItem(t, "ebook", bf, destPath, t.Name, author, metadataTitle, author, fileFormat(destPath), t.TotalSize)
		if err != nil {
			return err
		}

		// Import to external libraries.
		if inserted && w.targets != nil {
			w.targets.ImportEbook(destPath, t.Name, author)
		}
	}

	return nil
}

func (w *Watcher) importAudiobook(t TorrentInfo, savePath string) error {
	// If the source path doesn't even exist, fail the import.
	if _, statErr := os.Stat(savePath); os.IsNotExist(statErr) {
		return fmt.Errorf("%w: source path does not exist: %s", errTorrentContentPending, savePath)
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
		return fmt.Errorf("organize audiobook %q: %w", savePath, err)
	}

	inserted, err := w.recordTorrentItem(t, "audiobook", savePath, destPath, title, author, title, author, fileFormat(destPath), t.TotalSize)
	if err != nil {
		return err
	}

	if inserted && w.targets != nil {
		w.targets.ImportAudiobook()
	}

	return nil
}

func (w *Watcher) importManga(t TorrentInfo, savePath string) error {
	mangaFiles := findFilesByExt(savePath, []string{".cbz", ".cbr", ".zip", ".pdf", ".epub"})
	if len(mangaFiles) == 0 {
		return fmt.Errorf("%w: no manga files found at %s", errTorrentContentPending, savePath)
	}

	for _, mf := range mangaFiles {
		destPath, err := w.organizer.OrganizeManga(mf, t.Name)
		if err != nil {
			slog.Warn("organize manga failed", "file", mf, "error", err)
			destPath = mf
		}

		inserted, err := w.recordTorrentItem(t, "manga", mf, destPath, t.Name, "", t.Name, "", fileFormat(destPath), t.TotalSize)
		if err != nil {
			return err
		}

		if inserted && w.targets != nil {
			w.targets.ImportManga(destPath, t.Name)
		}
	}

	return nil
}

func (w *Watcher) recordTorrentItem(t TorrentInfo, mediaType, sourcePath, destinationPath, title, author, metadataTitle, metadataAuthor, format string, fileSize int64) (bool, error) {
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
		Source:       "torrent",
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
