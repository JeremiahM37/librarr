package organize

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrOutsideLibrary is returned when asked to retire a file that does not live
// under one of the configured library roots.
var ErrOutsideLibrary = errors.New("file is outside the library directories")

// ErrContentChanged is returned when the file on disk no longer matches the
// hash the library recorded for it.
var ErrContentChanged = errors.New("file content differs from the library record")

// RemoveSuperseded deletes a library file that a quality upgrade has replaced.
//
// It is deliberately narrow. The file must sit under EBOOK_DIR, AUDIOBOOK_DIR
// or MANGA_DIR — never the incoming folder or anywhere a torrent client still
// seeds from — and, when the library recorded a content hash, the bytes on disk
// must still match it, so a file the user swapped by hand is left alone. A
// missing file is not an error: the outcome the caller wants already holds.
// An emptied parent directory is removed too, but never a library root.
func (o *Organizer) RemoveSuperseded(path, expectedSHA256 string) error {
	if o == nil || o.cfg == nil {
		return fmt.Errorf("organizer not configured")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty path")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	root, ok := o.libraryRootFor(abs)
	if !ok {
		return ErrOutsideLibrary
	}

	info, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	if expected := strings.ToLower(strings.TrimSpace(expectedSHA256)); expected != "" {
		actual, err := sha256File(abs)
		if err != nil {
			return err
		}
		if actual != expected {
			return ErrContentChanged
		}
	}
	if err := os.Remove(abs); err != nil {
		return err
	}
	// Tidy an emptied author/title folder, stopping at the library root.
	dir := filepath.Dir(abs)
	for dir != root && strings.HasPrefix(dir, root+string(filepath.Separator)) {
		if err := os.Remove(dir); err != nil {
			break // not empty, or already gone
		}
		dir = filepath.Dir(dir)
	}
	return nil
}

// libraryRootFor returns the configured library root that contains abs.
func (o *Organizer) libraryRootFor(abs string) (string, bool) {
	for _, root := range []string{o.cfg.EbookDir, o.cfg.AudiobookDir, o.cfg.MangaDir} {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		cleanRoot, err := filepath.Abs(filepath.Clean(root))
		if err != nil {
			continue
		}
		if resolved, err := filepath.EvalSymlinks(cleanRoot); err == nil {
			cleanRoot = resolved
		}
		candidate := abs
		if resolved, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
			candidate = filepath.Join(resolved, filepath.Base(abs))
		}
		if strings.HasPrefix(candidate, cleanRoot+string(filepath.Separator)) {
			return cleanRoot, true
		}
	}
	return "", false
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
