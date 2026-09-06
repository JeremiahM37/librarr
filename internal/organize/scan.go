package organize

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

// AudiobookScanner periodically scans the audiobook directory for untracked files.
type AudiobookScanner struct {
	cfg     *config.Config
	db      *db.DB
	targets *LibraryTargets
}

// NewAudiobookScanner creates a new audiobook folder scanner.
func NewAudiobookScanner(cfg *config.Config, database *db.DB, targets *LibraryTargets) *AudiobookScanner {
	return &AudiobookScanner{
		cfg:     cfg,
		db:      database,
		targets: targets,
	}
}

// Start begins the background scan loop. It blocks until ctx is cancelled.
func (s *AudiobookScanner) Start(ctx context.Context) {
	if s.cfg.AudiobookDir == "" {
		slog.Info("audiobook scanner disabled (AUDIOBOOK_DIR not configured)")
		return
	}

	slog.Info("audiobook folder scanner started", "dir", s.cfg.AudiobookDir, "interval", "5m")
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Run once after a short delay.
	time.Sleep(30 * time.Second)
	s.scan()

	for {
		select {
		case <-ctx.Done():
			slog.Info("audiobook scanner stopping")
			return
		case <-ticker.C:
			s.scan()
		}
	}
}

func (s *AudiobookScanner) scan() {
	audioExts := map[string]bool{
		".mp3": true, ".m4a": true, ".m4b": true,
		".ogg": true, ".flac": true, ".opus": true,
	}

	if _, err := os.Stat(s.cfg.AudiobookDir); os.IsNotExist(err) {
		return
	}
	trackedPaths, err := s.db.LibraryPaths("audiobook")
	if err != nil {
		slog.Error("audiobook scanner could not load tracked paths", "error", err)
		return
	}
	trackedRoot := db.NormalizeLibraryPath(s.cfg.AudiobookDir)

	// Walk the audiobook directory looking for audio files.
	var newFiles []string
	filepath.Walk(s.cfg.AudiobookDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if !audioExts[ext] {
			return nil
		}

		// A torrent import may track either the file or the destination
		// directory, and its source ID is the torrent hash rather than the
		// scanner's synthetic ID. Walk ancestors so both import shapes are
		// recognized as already tracked.
		if isTrackedAudiobookPath(path, trackedRoot, trackedPaths) {
			return nil
		}

		newFiles = append(newFiles, path)
		return nil
	})

	if len(newFiles) == 0 {
		return
	}

	slog.Info("audiobook scanner found untracked files", "count", len(newFiles))

	// Group files by parent directory (each directory = one audiobook).
	dirFiles := make(map[string][]string)
	for _, f := range newFiles {
		dir := filepath.Dir(f)
		dirFiles[dir] = append(dirFiles[dir], f)
	}

	imported := 0
	for dir, files := range dirFiles {
		title := filepath.Base(dir)
		author := ""

		// Try to extract metadata from first audio file.
		if meta := ExtractAudioMetaFromDir(dir); meta != nil {
			if meta.Album != "" {
				title = meta.Album
			}
			if meta.Artist != "" {
				author = meta.Artist
			}
		}

		// Fallback: parse "Author - Title" from directory name.
		if author == "" && strings.Contains(title, " - ") {
			parts := strings.SplitN(title, " - ", 2)
			author = strings.TrimSpace(parts[0])
			title = strings.TrimSpace(parts[1])
		}

		var totalSize int64
		for _, f := range files {
			if info, err := os.Stat(f); err == nil {
				totalSize += info.Size()
			}
		}

		outcome, err := s.db.AddItemWithOutcome(&models.LibraryItem{
			Title:     title,
			Author:    author,
			FilePath:  dir,
			FileSize:  totalSize,
			MediaType: "audiobook",
			Source:    "scan",
			SourceID:  "scan-" + dir,
		})
		if err != nil {
			slog.Error("audiobook scanner library import failed", "path", dir, "error", err)
			continue
		}
		if !outcome.Inserted {
			continue
		}

		if err := s.db.LogEvent("scan_import", title, "Auto-imported from audiobook scan", &outcome.ID, ""); err != nil {
			slog.Warn("audiobook scanner activity log failed", "path", dir, "error", err)
		}
		imported++
	}

	// Trigger ABS scan ONCE after all imports (not per-directory).
	if imported > 0 && s.targets != nil {
		slog.Info("audiobook scanner triggering library scan", "imported", imported)
		s.targets.ImportAudiobook()
	}
}

// isTrackedAudiobookPath reports whether path or one of its parent
// directories belongs to a previously recorded audiobook import. Directory
// torrent imports record their destination directory while file imports
// record the individual destination file.
func isTrackedAudiobookPath(path, root string, trackedPaths map[string]struct{}) bool {
	current := db.NormalizeLibraryPath(path)
	if current == "" {
		return false
	}

	for {
		if _, ok := trackedPaths[current]; ok {
			return true
		}
		if current == root || current == filepath.Dir(current) {
			return false
		}
		current = filepath.Dir(current)
	}
}
