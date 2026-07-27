// Package organize moves downloaded ebooks, audiobooks, and manga into
// the configured library layout, extracting metadata where possible.
package organize

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jamie75/librarr/internal/config"
)

// Organizer handles post-download file organization.
type Organizer struct {
	cfg *config.Config
}

// NewOrganizer creates a new file organizer.
func NewOrganizer(cfg *config.Config) *Organizer {
	return &Organizer{cfg: cfg}
}

// OrganizeEbook moves an ebook file into the organized directory structure: {EbookDir}/{Author}/{Title}/{file}
// Also copies to KAVITA_LIBRARY_PATH if configured.
func (o *Organizer) OrganizeEbook(filePath, title, author string) (string, error) {
	if !o.cfg.FileOrgEnabled {
		return filePath, nil
	}

	if author == "" {
		// Try to extract author from EPUB metadata.
		if strings.HasSuffix(strings.ToLower(filePath), ".epub") {
			if meta, err := ExtractEPUBMeta(filePath); err == nil && meta.Author != "" {
				author = meta.Author
			}
		}
	}
	if author == "" {
		author = "Unknown"
	}

	safeAuthor := sanitizePath(author, 80)
	safeTitle := sanitizePath(title, 80)

	destDir := filepath.Join(o.cfg.EbookDir, safeAuthor, safeTitle)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return filePath, err
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))
	if err := moveFile(filePath, destPath); err != nil {
		return filePath, err
	}

	slog.Info("ebook organized", "title", title, "dest", destPath)

	// Also copy to Kavita ebook library if configured.
	if o.cfg.KavitaLibraryPath != "" {
		kavitaDir := filepath.Join(o.cfg.KavitaLibraryPath, safeAuthor, safeTitle)
		if err := os.MkdirAll(kavitaDir, 0755); err == nil {
			kavitaDest := filepath.Join(kavitaDir, filepath.Base(destPath))
			if err := copyFileForOrg(destPath, kavitaDest); err != nil {
				slog.Warn("copy to kavita ebook library failed", "error", err)
			} else {
				slog.Info("copied to kavita ebook library", "path", kavitaDest)
			}
		}
	}

	return destPath, nil
}

// OrganizeAudiobook moves audiobook files into the organized directory structure: {AudiobookDir}/{Author}/{Title}/
func (o *Organizer) OrganizeAudiobook(filePath, title, author string) (string, error) {
	if !o.cfg.FileOrgEnabled {
		return filePath, nil
	}

	if author == "" {
		author = "Unknown"
	}

	// If source is a directory, move its contents.
	info, err := os.Lstat(filePath)
	if err != nil {
		return filePath, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return filePath, fmt.Errorf("refusing to organize symlink source %q", filePath)
	}

	safeAuthor := sanitizePath(author, 80)
	safeTitle := sanitizePath(title, 80)

	destDir := filepath.Join(o.cfg.AudiobookDir, safeAuthor, safeTitle)
	if info.IsDir() {
		if err := moveDirTree(filePath, destDir); err != nil {
			return filePath, err
		}
		return destDir, nil
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return filePath, err
	}

	destPath := filepath.Join(destDir, filepath.Base(filePath))
	if err := moveFile(filePath, destPath); err != nil {
		return filePath, err
	}

	return destPath, nil
}

// OrganizeManga moves manga files into the organized directory structure: {MangaDir}/{Series}/{file}
// Also copies to KAVITA_MANGA_LIBRARY_PATH if configured.
func (o *Organizer) OrganizeManga(filePath, seriesTitle string) (string, error) {
	if !o.cfg.FileOrgEnabled {
		return filePath, nil
	}

	safeTitle := cleanSeriesTitle(seriesTitle)
	destDir := filepath.Join(o.cfg.MangaDir, safeTitle)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return filePath, err
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return filePath, err
	}

	var resultPath string
	if info.IsDir() {
		entries, err := os.ReadDir(filePath)
		if err != nil {
			return filePath, err
		}
		for _, entry := range entries {
			src := filepath.Join(filePath, entry.Name())
			dst := filepath.Join(destDir, entry.Name())
			_ = moveFile(src, dst)
		}
		_ = os.RemoveAll(filePath)
		resultPath = destDir
	} else {
		destPath := filepath.Join(destDir, filepath.Base(filePath))
		if err := moveFile(filePath, destPath); err != nil {
			return filePath, err
		}
		resultPath = destPath
	}

	// Also copy to Kavita manga library if configured.
	if o.cfg.KavitaMangaLibraryPath != "" {
		kavitaDir := filepath.Join(o.cfg.KavitaMangaLibraryPath, safeTitle)
		if err := os.MkdirAll(kavitaDir, 0755); err == nil {
			resultInfo, err := os.Stat(resultPath)
			if err == nil {
				if resultInfo.IsDir() {
					entries, _ := os.ReadDir(resultPath)
					for _, entry := range entries {
						src := filepath.Join(resultPath, entry.Name())
						dst := filepath.Join(kavitaDir, entry.Name())
						_ = copyFileForOrg(src, dst)
					}
				} else {
					dst := filepath.Join(kavitaDir, filepath.Base(resultPath))
					_ = copyFileForOrg(resultPath, dst)
				}
				slog.Info("copied to kavita manga library", "path", kavitaDir)
			}
		}
	}

	return resultPath, nil
}

var (
	unsafePathRe = regexp.MustCompile(`[<>:"/\\|?*]`)
	whitespaceRe = regexp.MustCompile(`\s+`)
	bracketRe    = regexp.MustCompile(`\[[^\]]*\]`)
	parenTagsRe  = regexp.MustCompile(`\((?i:Digital|f|c2c|Viz|Complete)\)`)
	volumeRe     = regexp.MustCompile(`(?i)\s*(?:Vol\.?|Volume|v)\s*\d+.*$`)
	rangeRe      = regexp.MustCompile(`\s*\d+-\d+.*$`)
)

func sanitizePath(name string, maxLen int) string {
	name = unsafePathRe.ReplaceAllString(name, "")
	name = whitespaceRe.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	name = strings.Trim(name, ".")
	if len(name) > maxLen {
		name = strings.TrimSpace(name[:maxLen])
	}
	if name == "" {
		name = "Unknown"
	}
	return name
}

func cleanSeriesTitle(name string) string {
	// Strip file extensions.
	name = regexp.MustCompile(`(?i)\.(epub|cbz|cbr|pdf|zip|mobi|azw3)$`).ReplaceAllString(name, "")
	name = bracketRe.ReplaceAllString(name, "")
	name = parenTagsRe.ReplaceAllString(name, "")
	name = volumeRe.ReplaceAllString(name, "")
	name = rangeRe.ReplaceAllString(name, "")
	name = whitespaceRe.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	name = strings.TrimRight(name, "-")
	name = strings.TrimSpace(name)
	if name == "" {
		name = "Unknown"
	}
	return name
}

func moveFile(src, dst string) error {
	// Try rename first (same filesystem).
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	// Fall back to copy + delete.
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		return err
	}
	return os.Remove(src)
}

func moveDirTree(srcDir, dstDir string) error {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == srcDir {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}

		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dstDir, rel)
		if d.IsDir() {
			return os.MkdirAll(dstPath, 0755)
		}

		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			return err
		}
		return moveFile(path, dstPath)
	})
	if err != nil {
		return err
	}

	return os.RemoveAll(srcDir)
}

// copyFileForOrg copies a file without removing the source.
func copyFileForOrg(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
