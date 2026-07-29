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

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
	libraryimport "github.com/jamie75/librarr/internal/library/import"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/organize"
	"github.com/jamie75/librarr/internal/search"
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
	importer  libraryimport.ImportEngine

	processing sync.Map // hash -> struct{}, tracks in-progress imports
	imported   sync.Map // hash -> struct{}, tracks already-imported hashes
	pending    sync.Map // hash/type -> reason signature, suppresses repeated pending INFO logs
}

var errTorrentContentPending = errors.New("torrent content pending synchronization")

// NewWatcher creates a new torrent completion watcher.
func NewWatcher(cfg *config.Config, database *db.DB, torrent TorrentClient, organizer *organize.Organizer, targets *organize.LibraryTargets, health *search.HealthTracker) *Watcher {
	return NewWatcherWithImportEngine(cfg, database, torrent, organizer, targets, health, nil)
}

func NewWatcherWithImportEngine(cfg *config.Config, database *db.DB, torrent TorrentClient, organizer *organize.Organizer, targets *organize.LibraryTargets, health *search.HealthTracker, importer libraryimport.ImportEngine) *Watcher {
	return &Watcher{
		cfg:       cfg,
		db:        database,
		torrent:   torrent,
		organizer: organizer,
		targets:   targets,
		health:    health,
		importer:  importer,
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

	resolved := w.resolveLocalPathResult(t, mediaType)
	savePath := resolved.Path
	w.logPathResolution(t, mediaType, resolved)

	var importErr error
	if _, err := os.Stat(savePath); err != nil {
		if os.IsNotExist(err) {
			w.logImportPending(t, mediaType, savePath, "local synchronized path not available", nil)
		} else {
			w.logImportPending(t, mediaType, savePath, "local path stat failed", err)
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
			w.logImportPending(t, mediaType, savePath, "content not ready for import", importErr)
		} else {
			w.clearImportPending(t.Hash, mediaType)
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
	w.clearImportPending(t.Hash, mediaType)

	// Log the import.
	if err := w.db.LogEvent("torrent_import", t.Name, fmt.Sprintf("Imported %s from torrent", mediaType), nil, t.Hash); err != nil {
		slog.Warn("failed to log torrent import", "name", t.Name, "hash", t.Hash, "error", err)
	}
}

// resolveLocalPath maps qBittorrent container paths to local paths.
// Each media type resolves to Librarr's local synchronized incoming mount,
// not the remote qBittorrent save path or the final organized library
// directory. qBittorrent may report paths from a seedbox filesystem that
// Librarr can only see by basename under INCOMING_DIR after rclone sync.
func (w *Watcher) resolveLocalPath(t TorrentInfo, mediaType string) string {
	return w.resolveLocalPathResult(t, mediaType).Path
}

type localPathResolution struct {
	Path         string
	Strategy     string
	ReportedPath string
	IncomingDir  string
	Failure      string
}

func (w *Watcher) resolveLocalPathResult(t TorrentInfo, mediaType string) localPathResolution {
	var rootName string
	localIncoming := w.incomingDirForMedia(mediaType)

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

	path, ok := joinWithinRoot(localIncoming, rootName)
	if !ok {
		return localPathResolution{
			Path:         localIncoming,
			Strategy:     "rejected_torrent_name_or_files",
			ReportedPath: t.Name,
			IncomingDir:  localIncoming,
			Failure:      "torrent name or file root could not be resolved safely",
		}
	}
	return localPathResolution{
		Path:         path,
		Strategy:     "torrent_name_or_files",
		ReportedPath: t.Name,
		IncomingDir:  localIncoming,
	}
}

func (w *Watcher) incomingDirForMedia(mediaType string) string {
	switch mediaType {
	case "audiobook":
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

func (w *Watcher) resolveContentPath(t TorrentInfo, mediaType string) localPathResolution {
	contentPath := normalizeTorrentPath(t.ContentPath)
	contentPath = filepath.Clean(contentPath)
	savePath := filepath.Clean(normalizeTorrentPath(t.SavePath))
	localIncoming := w.incomingDirForMedia(mediaType)

	if contentPath == "" {
		return localPathResolution{Path: localIncoming, Strategy: "empty_content_path", ReportedPath: t.ContentPath, IncomingDir: localIncoming, Failure: "empty content path"}
	}

	if filepath.IsAbs(contentPath) {
		if _, err := os.Stat(contentPath); err == nil {
			return localPathResolution{Path: contentPath, Strategy: "reported_path_exists", ReportedPath: t.ContentPath, IncomingDir: localIncoming}
		}
	}

	remoteRoots := []string{savePath, filepath.Clean(normalizeTorrentPath(w.remoteSavePathForMedia(mediaType)))}
	for _, remoteRoot := range remoteRoots {
		if mapped, ok := mapTorrentPath(contentPath, remoteRoot, localIncoming); ok {
			return localPathResolution{Path: mapped, Strategy: "mapped_remote_root", ReportedPath: t.ContentPath, IncomingDir: localIncoming}
		}
	}

	if mapped, ok := mapTorrentPath(contentPath, localIncoming, localIncoming); ok {
		return localPathResolution{Path: mapped, Strategy: "already_under_incoming", ReportedPath: t.ContentPath, IncomingDir: localIncoming}
	}

	if !filepath.IsAbs(contentPath) && (contentPath == ".." || strings.HasPrefix(contentPath, ".."+string(os.PathSeparator))) {
		return localPathResolution{Path: localIncoming, Strategy: "rejected_relative_content_path", ReportedPath: t.ContentPath, IncomingDir: localIncoming, Failure: "relative content path escaped incoming root"}
	}

	if !filepath.IsAbs(contentPath) {
		if joined, ok := joinWithinRoot(localIncoming, contentPath); ok {
			return localPathResolution{Path: joined, Strategy: "relative_content_path", ReportedPath: t.ContentPath, IncomingDir: localIncoming}
		}
		return localPathResolution{Path: localIncoming, Strategy: "rejected_relative_content_path", ReportedPath: t.ContentPath, IncomingDir: localIncoming, Failure: "relative content path escaped incoming root"}
	}

	base := filepath.Base(contentPath)
	if joined, ok := joinWithinRoot(localIncoming, base); ok {
		return localPathResolution{Path: joined, Strategy: "incoming_basename_fallback", ReportedPath: t.ContentPath, IncomingDir: localIncoming}
	}
	return localPathResolution{Path: localIncoming, Strategy: "rejected_content_path", ReportedPath: t.ContentPath, IncomingDir: localIncoming, Failure: "content path could not be resolved safely"}
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

func joinWithinRoot(root, relPath string) (string, bool) {
	root = filepath.Clean(normalizeTorrentPath(root))
	relPath = filepath.Clean(normalizeTorrentPath(relPath))
	if root == "." || relPath == "." || relPath == "" || filepath.IsAbs(relPath) || !filepath.IsAbs(root) {
		return "", false
	}
	if relPath == ".." || strings.HasPrefix(relPath, ".."+string(os.PathSeparator)) {
		return "", false
	}
	candidate := filepath.Join(root, relPath)
	rel, err := filepath.Rel(root, candidate)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return candidate, true
}

func (w *Watcher) logPathResolution(t TorrentInfo, mediaType string, resolved localPathResolution) {
	fields := []any{
		"name", t.Name,
		"hash", t.Hash,
		"type", mediaType,
		"reported_path", resolved.ReportedPath,
		"incoming_dir", resolved.IncomingDir,
		"strategy", resolved.Strategy,
		"resolved_path", resolved.Path,
	}
	if resolved.Failure != "" {
		slog.Warn("torrent local path resolution warning", append(fields, "failure", resolved.Failure)...)
		return
	}
	slog.Debug("torrent local path resolved", fields...)
}

func (w *Watcher) logImportPending(t TorrentInfo, mediaType, path, reason string, err error) {
	fields := []any{
		"name", t.Name,
		"hash", t.Hash,
		"type", mediaType,
		"path", path,
		"reason", reason,
	}
	if err != nil {
		fields = append(fields, "error", err)
	}

	key := t.Hash + ":" + mediaType
	signature := path + ":" + reason
	if err != nil {
		signature += ":" + err.Error()
	}
	if previous, ok := w.pending.Load(key); ok && previous == signature {
		slog.Debug("torrent import still pending", fields...)
		return
	}
	w.pending.Store(key, signature)
	if err != nil && !errors.Is(err, errTorrentContentPending) {
		slog.Warn("torrent import pending", fields...)
		return
	}
	slog.Info("torrent import pending", fields...)
}

func (w *Watcher) clearImportPending(hash, mediaType string) {
	w.pending.Delete(hash + ":" + mediaType)
}

func (w *Watcher) importEbook(t TorrentInfo, savePath string) error {
	bookFiles := findFilesByExt(savePath, []string{".epub", ".mobi", ".pdf", ".azw3"})
	if len(bookFiles) == 0 {
		return fmt.Errorf("%w: no ebook files found at %s", errTorrentContentPending, savePath)
	}

	for _, bf := range bookFiles {
		metadata := organize.ExtractEbookMetadata(bf)
		title := firstNonEmpty(metadata.Title, t.Name)
		author := metadata.Author
		destPath, err := w.organizer.OrganizeEbook(bf, title, author)
		if err != nil {
			return fmt.Errorf("%w: organize ebook %q: %v", errTorrentContentPending, bf, err)
		}

		inserted, err := w.importTorrentItem(context.Background(), t, library.MediaTypeEbook, bf, destPath, title, author, metadata.Title, metadata.Author, fileFormat(destPath), t.TotalSize)
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
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

	inserted, err := w.importTorrentItem(context.Background(), t, library.MediaTypeAudiobook, savePath, destPath, title, author, title, author, fileFormat(destPath), t.TotalSize)
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
			return fmt.Errorf("%w: organize manga %q: %v", errTorrentContentPending, mf, err)
		}

		inserted, err := w.importTorrentItem(context.Background(), t, library.MediaTypeManga, mf, destPath, t.Name, "", t.Name, "", fileFormat(destPath), t.TotalSize)
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

func (w *Watcher) importTorrentItem(ctx context.Context, t TorrentInfo, mediaType library.MediaType, sourcePath, destinationPath, title, author, metadataTitle, metadataAuthor, format string, fileSize int64) (bool, error) {
	if w.importer == nil {
		return w.recordTorrentItem(t, string(mediaType), sourcePath, destinationPath, title, author, metadataTitle, metadataAuthor, format, fileSize)
	}

	result, err := w.importer.Import(ctx, libraryimport.ImportRequest{
		Source: library.ImportSource{
			Name:      "torrent",
			SourceID:  t.Hash,
			MediaType: mediaType,
		},
		RootPath:     destinationPath,
		OriginalPath: sourcePath,
		TitleHint:    title,
		AuthorHint:   author,
	})
	fields := []any{
		"torrent_hash", t.Hash,
		"torrent_name", t.Name,
		"source_path", sourcePath,
		"destination_path", destinationPath,
		"file_size", fileSize,
		"detected_format", format,
		"metadata_title", metadataTitle,
		"metadata_author", metadataAuthor,
	}
	if err != nil {
		slog.Error("torrent library import engine failure", append(fields, "error", err)...)
		return false, err
	}
	inserted := result != nil && result.InsertedCount > 0
	slog.Info("torrent library import decision", append(fields,
		"engine", "configured",
		"inserted_count", result.InsertedCount,
		"duplicate_count", result.DuplicateCount,
		"skipped_count", result.SkippedCount,
		"conflict_count", result.ConflictCount,
	)...)
	return inserted, nil
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
