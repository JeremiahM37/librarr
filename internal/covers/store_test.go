package covers

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var (
	jpegBytes = append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, bytes.Repeat([]byte{0}, 32)...)
	pngBytes  = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)
	gifBytes  = append([]byte("GIF89a"), bytes.Repeat([]byte{0}, 32)...)
	webpBytes = append(append([]byte("RIFF\x24\x00\x00\x00WEBP"), []byte("VP8 ")...), bytes.Repeat([]byte{0}, 32)...)
)

func TestDetectImage(t *testing.T) {
	good := []struct {
		name string
		b    []byte
		ext  string
		ct   string
	}{
		{"jpeg", jpegBytes, ".jpg", "image/jpeg"},
		{"png", pngBytes, ".png", "image/png"},
		{"gif", gifBytes, ".gif", "image/gif"},
		{"webp", webpBytes, ".webp", "image/webp"},
	}
	for _, tc := range good {
		ext, ct, ok := DetectImage(tc.b)
		if !ok || ext != tc.ext || ct != tc.ct {
			t.Errorf("%s: got %q %q %v", tc.name, ext, ct, ok)
		}
	}
	bad := map[string][]byte{
		"svg":   []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`),
		"html":  []byte("<html><body>hi</body></html>"),
		"empty": nil,
		"text":  []byte("this is cover.jpg, honest"),
	}
	for name, b := range bad {
		if _, _, ok := DetectImage(b); ok {
			t.Errorf("%s: should be rejected", name)
		}
	}
}

func TestSaveAndOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "covers", "nested")
	s := NewStore(dir)
	file, err := s.Save(7, jpegBytes)
	if err != nil || file != "7.jpg" {
		t.Fatalf("Save: %q %v", file, err)
	}
	rc, ct, err := s.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, jpegBytes) || ct != "image/jpeg" {
		t.Fatalf("Open: ct=%q equal=%v", ct, bytes.Equal(got, jpegBytes))
	}
	if ok, err := s.Exists("7.jpg"); !ok || err != nil {
		t.Fatalf("Exists: %v %v", ok, err)
	}
	if _, err := s.Save(7, pngBytes); err != nil {
		t.Fatal(err)
	}
	list, err := s.List()
	if err != nil || !reflect.DeepEqual(list, []string{"7.png"}) {
		t.Fatalf("List: %v %v", list, err)
	}
	if err := s.Remove("7.png"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Exists("7.png"); ok || err != nil {
		t.Fatalf("still exists: %v %v", ok, err)
	}
}

func TestSaveRejectsNonImage(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	if _, err := s.Save(1, []byte("<html></html>")); err == nil {
		t.Fatal("expected error")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("leftover files: %v", entries)
	}
}

func TestOpenIgnoresPathsInFile(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "covers")
	s := NewStore(dir)
	if err := os.WriteFile(filepath.Join(root, "secret.jpg"), jpegBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"../secret.jpg", "../../etc/passwd", "/etc/passwd"} {
		if ok, err := s.Exists(f); ok || err != nil {
			t.Errorf("Exists(%q) = %v, %v", f, ok, err)
		}
		if rc, _, err := s.Open(f); err == nil {
			rc.Close()
			t.Errorf("Open(%q) succeeded", f)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "secret.jpg"), pngBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	rc, ct, err := s.Open("../secret.jpg")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if ct != "image/png" {
		t.Fatalf("expected the file inside dir, got %q", ct)
	}
}

func TestExistsReportsStatFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "covers")
	s := NewStore(dir)
	if ok, err := s.Exists("7.jpg"); ok || err != nil {
		t.Fatalf("missing directory: %v %v", ok, err)
	}
	if _, err := s.Save(7, jpegBytes); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Exists("8.jpg"); ok || err != nil {
		t.Fatalf("missing file: %v %v", ok, err)
	}
	lockDir(t, dir, 0)
	if _, err := s.Exists("7.jpg"); err == nil {
		t.Fatal("an unreadable directory must be an error, not a missing file")
	}
}
