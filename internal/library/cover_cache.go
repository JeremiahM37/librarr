package library

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamie75/librarr/internal/organize"
)

// CoverCache stores user-owned extracted artwork as files under /data (or the
// configured data directory) and records lightweight cover metadata in SQLite.
type CoverCache struct {
	dir string
}

func NewCoverCache(dir string) *CoverCache {
	return &CoverCache{dir: filepath.Clean(dir)}
}

func (c *CoverCache) Dir() string {
	if c == nil {
		return ""
	}
	return c.dir
}

func (c *CoverCache) ExtractForScan(jobID, candidateID, sourcePath string) (string, error) {
	if c == nil || strings.TrimSpace(c.dir) == "" {
		return "", nil
	}
	cover, err := organize.ExtractEmbeddedCover(sourcePath)
	if err != nil || cover == nil {
		return "", err
	}
	targetDir := filepath.Join(c.dir, "scan", safeCoverSegment(jobID))
	targetPath := filepath.Join(targetDir, safeCoverSegment(candidateID)+cover.Ext)
	if err := writeCoverFile(targetPath, cover.Data); err != nil {
		return "", err
	}
	return targetPath, nil
}

func (c *CoverCache) AttachBookCover(ctx context.Context, svc *LibraryService, bookID int64, sourcePath string) (*Cover, error) {
	if c == nil || svc == nil || bookID == 0 || strings.TrimSpace(c.dir) == "" {
		return nil, nil
	}
	if existing, err := svc.GetPrimaryCover(ctx, bookID); err == nil && usableCover(existing) {
		return existing, nil
	}
	cover, err := organize.ExtractEmbeddedCover(sourcePath)
	if err != nil {
		return nil, err
	}
	if cover == nil {
		return nil, nil
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", bookID, sourcePath)))
	filename := hex.EncodeToString(sum[:12]) + cover.Ext
	targetPath := filepath.Join(c.dir, "books", filename)
	if err := writeCoverFile(targetPath, cover.Data); err != nil {
		return nil, err
	}
	attached, err := svc.AttachCover(ctx, Cover{
		BookID:    bookID,
		Source:    cover.Source,
		LocalPath: targetPath,
		MimeType:  cover.MimeType,
		Width:     cover.Width,
		Height:    cover.Height,
		IsPrimary: true,
	})
	if err != nil {
		return nil, err
	}
	slog.Debug("attached local book cover", "book_id", bookID, "source", cover.Source, "mime_type", cover.MimeType)
	return attached, nil
}

func usableCover(cover *Cover) bool {
	if cover == nil {
		return false
	}
	if strings.TrimSpace(cover.SourceURL) != "" {
		return true
	}
	localPath := strings.TrimSpace(cover.LocalPath)
	if localPath == "" {
		return false
	}
	info, err := os.Stat(localPath)
	return err == nil && !info.IsDir() && info.Size() > 0
}

func writeCoverFile(targetPath string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("cover image is empty")
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return fmt.Errorf("create cover cache directory: %w", err)
	}
	return os.WriteFile(targetPath, data, 0644)
}

func safeCoverSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", "-", "\x00", "")
	value = replacer.Replace(value)
	value = strings.Join(strings.Fields(value), "-")
	return strings.Trim(value, ".-")
}
