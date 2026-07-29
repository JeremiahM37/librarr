package organize

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
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
	enabled, reason := s.Enabled()
	if !enabled {
		slog.Info("legacy audiobook scanner disabled",
			"repository_mode", normalizedRepositoryMode(s.cfg),
			"import_engine", normalizedImportEngineMode(s.cfg),
			"legacy_scanner_enabled", false,
			"audiobook_scanner_enabled", false,
			"reason", reason,
		)
		return
	}
	if s.cfg.AudiobookDir == "" {
		slog.Info("legacy audiobook scanner disabled",
			"repository_mode", normalizedRepositoryMode(s.cfg),
			"import_engine", normalizedImportEngineMode(s.cfg),
			"legacy_scanner_enabled", false,
			"audiobook_scanner_enabled", false,
			"reason", "AUDIOBOOK_DIR not configured",
		)
		return
	}

	slog.Info("legacy audiobook folder scanner started",
		"dir", s.cfg.AudiobookDir,
		"interval", "5m",
		"repository_mode", normalizedRepositoryMode(s.cfg),
		"import_engine", normalizedImportEngineMode(s.cfg),
		"legacy_scanner_enabled", true,
		"audiobook_scanner_enabled", true,
	)
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

func (s *AudiobookScanner) Enabled() (bool, string) {
	if normalizedRepositoryMode(s.cfg) == "normalized" {
		return false, "normalized repository uses explicit library scan"
	}
	return true, "legacy repository mode"
}

func (s *AudiobookScanner) scan() {
	audioExts := map[string]bool{
		".mp3": true, ".m4a": true, ".m4b": true,
		".ogg": true, ".flac": true, ".opus": true,
	}

	if _, err := os.Stat(s.cfg.AudiobookDir); os.IsNotExist(err) {
		return
	}

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

		groupPath := audiobookScanGroupPath(s.cfg.AudiobookDir, path)
		if s.db.HasSourceID("scan-" + groupPath) {
			return nil
		}

		newFiles = append(newFiles, path)
		return nil
	})

	if len(newFiles) == 0 {
		return
	}

	slog.Info("audiobook scanner found untracked files", "count", len(newFiles))

	// Group deep files by parent directory; direct files under an author root are
	// independent single-file audiobooks.
	dirFiles := make(map[string][]string)
	for _, f := range newFiles {
		groupPath := audiobookScanGroupPath(s.cfg.AudiobookDir, f)
		dirFiles[groupPath] = append(dirFiles[groupPath], f)
	}

	imported := 0
	for groupPath, files := range dirFiles {
		title, author := s.parseAudiobook(groupPath, files)

		var totalSize int64
		for _, f := range files {
			if info, err := os.Stat(f); err == nil {
				totalSize += info.Size()
			}
		}

		_, _ = s.db.AddItem(&models.LibraryItem{
			Title:     title,
			Author:    author,
			FilePath:  groupPath,
			FileSize:  totalSize,
			MediaType: "audiobook",
			Source:    "scan",
			SourceID:  "scan-" + groupPath,
		})

		_ = s.db.LogEvent("scan_import", title, "Auto-imported from audiobook scan", nil, "")
		imported++
	}

	// Trigger ABS scan ONCE after all imports (not per-directory).
	if imported > 0 && s.targets != nil {
		slog.Info("audiobook scanner triggering library scan", "imported", imported)
		s.targets.ImportAudiobook()
	}
}

func (s *AudiobookScanner) parseAudiobook(dir string, files []string) (string, string) {
	return parseAudiobookScanPath(s.cfg.AudiobookDir, dir, files)
}

func parseAudiobookScanPath(root, dir string, files []string) (string, string) {
	groupPath := filepath.Clean(dir)
	title := filepath.Base(groupPath)
	author := ""

	metaDir := groupPath
	if len(files) == 1 && sameCleanPath(groupPath, filepath.Clean(files[0])) {
		metaDir = filepath.Dir(groupPath)
	}
	if meta := ExtractEmbeddedAudioMetaFromDir(metaDir); meta != nil {
		if meta.Album != "" {
			title = strings.TrimSpace(meta.Album)
		}
		if meta.Artist != "" {
			author = strings.TrimSpace(meta.Artist)
		}
	}

	if author == "" {
		if candidate := authorFromAudiobookDir(root, dir); candidate != "" {
			author = candidate
		}
	}

	if len(files) == 1 {
		if candidate := audiobookTitleFromFile(files[0], author); candidate != "" {
			title = candidate
		}
	}

	// Legacy fallback for older "Author - Title" directory layouts.
	if author == "" && strings.Contains(title, " - ") {
		parts := strings.SplitN(title, " - ", 2)
		author = strings.TrimSpace(parts[0])
		title = strings.TrimSpace(parts[1])
	}

	return strings.TrimSpace(title), strings.TrimSpace(author)
}

func audiobookScanGroupPath(root, path string) string {
	root = filepath.Clean(root)
	cleanPath := filepath.Clean(path)
	parent := filepath.Dir(cleanPath)
	rel, err := filepath.Rel(root, parent)
	if err != nil || rel == "." || rel == "" {
		return cleanPath
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return parent
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) == 1 {
		return cleanPath
	}
	return parent
}

func sameCleanPath(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}

func authorFromAudiobookDir(root, dir string) string {
	root = filepath.Clean(root)
	dir = filepath.Clean(dir)
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == "." || rel == "" {
		return ""
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	parts := strings.Split(rel, string(filepath.Separator))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			return part
		}
	}
	return ""
}

func ExtractEmbeddedAudioMetaFromDir(dirPath string) *AudioMeta {
	audioExts := map[string]bool{
		".mp3": true, ".m4a": true, ".m4b": true,
		".ogg": true, ".flac": true, ".opus": true,
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if !audioExts[ext] {
			continue
		}
		if meta := extractEmbeddedAudioMeta(filepath.Join(dirPath, entry.Name())); meta != nil {
			return meta
		}
	}
	return nil
}

func normalizedRepositoryMode(cfg *config.Config) string {
	if cfg == nil {
		return "legacy"
	}
	mode := strings.ToLower(strings.TrimSpace(cfg.LibraryRepositoryMode))
	if mode == "" {
		return "legacy"
	}
	return mode
}

func normalizedImportEngineMode(cfg *config.Config) string {
	if cfg == nil {
		return "legacy"
	}
	mode := strings.ToLower(strings.TrimSpace(cfg.ImportEngine))
	if mode == "" {
		return "legacy"
	}
	return mode
}
