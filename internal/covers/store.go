// Package covers stores cover images for library items.
package covers

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MaxImageBytes is the largest cover image accepted from any source.
const MaxImageBytes = 10 << 20

const tempPrefix = ".tmp-"

// DetectImage identifies JPEG, PNG, GIF and WebP images by their magic bytes.
// Anything else, including SVG, is rejected.
func DetectImage(b []byte) (ext, contentType string, ok bool) {
	switch {
	case bytes.HasPrefix(b, []byte{0xFF, 0xD8, 0xFF}):
		return ".jpg", "image/jpeg", true
	case bytes.HasPrefix(b, []byte("\x89PNG\r\n\x1a\n")):
		return ".png", "image/png", true
	case bytes.HasPrefix(b, []byte("GIF87a")), bytes.HasPrefix(b, []byte("GIF89a")):
		return ".gif", "image/gif", true
	case len(b) >= 12 && bytes.HasPrefix(b, []byte("RIFF")) && bytes.Equal(b[8:12], []byte("WEBP")):
		return ".webp", "image/webp", true
	}
	return "", "", false
}

// Store keeps cover files in a single directory.
type Store struct {
	dir string
}

// NewStore returns a Store rooted at dir.
func NewStore(dir string) *Store {
	return &Store{dir: dir}
}

// path resolves a stored file name inside dir, discarding any directory parts.
func (s *Store) path(file string) (string, error) {
	name := filepath.Base(file)
	if name == "." || name == ".." || name == string(filepath.Separator) || strings.HasPrefix(name, tempPrefix) {
		return "", fmt.Errorf("invalid cover file name")
	}
	return filepath.Join(s.dir, name), nil
}

// Save validates image and writes it as <id><ext>, replacing any cover the
// item had under a different extension. It returns the file name.
func (s *Store) Save(id int64, image []byte) (string, error) {
	ext, _, ok := DetectImage(image)
	if !ok {
		return "", errors.New("not a supported image")
	}
	if len(image) > MaxImageBytes {
		return "", errors.New("image too large")
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(s.dir, tempPrefix+"*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(image); err != nil {
		tmp.Close()
		cleanup()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		cleanup()
		return "", err
	}
	file := strconv.FormatInt(id, 10) + ext
	if err := os.Rename(tmpName, filepath.Join(s.dir, file)); err != nil {
		cleanup()
		return "", err
	}
	for _, other := range []string{".jpg", ".png", ".gif", ".webp"} {
		if other != ext {
			_ = os.Remove(filepath.Join(s.dir, strconv.FormatInt(id, 10)+other))
		}
	}
	return file, nil
}

// Open returns the stored file and its content type, derived from the bytes.
func (s *Store) Open(file string) (io.ReadCloser, string, error) {
	p, err := s.path(file)
	if err != nil {
		return nil, "", err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, "", err
	}
	head := make([]byte, 512)
	n, err := io.ReadFull(f, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		f.Close()
		return nil, "", err
	}
	_, ct, ok := DetectImage(head[:n])
	if !ok {
		f.Close()
		return nil, "", errors.New("stored file is not a supported image")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, "", err
	}
	return f, ct, nil
}

// Exists reports whether file is present in the store. An error means the store
// could not tell, as when the directory is unreadable: the file may well be there.
func (s *Store) Exists(file string) (bool, error) {
	p, err := s.path(file)
	if err != nil {
		return false, nil
	}
	fi, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return fi.Mode().IsRegular(), nil
}

// Remove deletes file from the store. A missing file is not an error.
func (s *Store) Remove(file string) error {
	p, err := s.path(file)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List returns the names of the cover files in the store.
func (s *Store) List() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.Type().IsRegular() && !strings.HasPrefix(e.Name(), tempPrefix) {
			out = append(out, e.Name())
		}
	}
	return out, nil
}
