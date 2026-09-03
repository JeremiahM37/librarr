package organize

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func writeTestFile(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
}

func TestRemoveSuperseded(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{
		EbookDir:     filepath.Join(root, "ebooks"),
		AudiobookDir: filepath.Join(root, "audiobooks"),
		MangaDir:     "", // unset roots are ignored, not treated as "/"
		IncomingDir:  filepath.Join(root, "incoming"),
	}
	o := NewOrganizer(cfg)

	t.Run("removes a matching file and its emptied folders", func(t *testing.T) {
		path := filepath.Join(cfg.EbookDir, "Author", "Title", "Title.pdf")
		hash := writeTestFile(t, path, "pdf bytes")
		if err := o.RemoveSuperseded(path, hash); err != nil {
			t.Fatalf("RemoveSuperseded: %v", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("file still exists")
		}
		if _, err := os.Stat(filepath.Join(cfg.EbookDir, "Author")); !os.IsNotExist(err) {
			t.Fatal("emptied author folder should be removed")
		}
		if _, err := os.Stat(cfg.EbookDir); err != nil {
			t.Fatal("library root must never be removed")
		}
	})

	t.Run("keeps sibling files and non-empty folders", func(t *testing.T) {
		old := filepath.Join(cfg.EbookDir, "A", "T", "T.pdf")
		keep := filepath.Join(cfg.EbookDir, "A", "T", "T.epub")
		hash := writeTestFile(t, old, "old")
		writeTestFile(t, keep, "new")
		if err := o.RemoveSuperseded(old, hash); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(keep); err != nil {
			t.Fatal("sibling removed")
		}
	})

	t.Run("refuses a file whose content changed", func(t *testing.T) {
		path := filepath.Join(cfg.EbookDir, "B", "B.pdf")
		writeTestFile(t, path, "as imported")
		writeTestFile(t, path, "edited by the user")
		hashOfImported := fmt.Sprintf("%x", sha256.Sum256([]byte("as imported")))
		err := o.RemoveSuperseded(path, hashOfImported)
		if !errors.Is(err, ErrContentChanged) {
			t.Fatalf("expected ErrContentChanged, got %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatal("changed file must be left in place")
		}
	})

	t.Run("no recorded hash still removes (legacy rows)", func(t *testing.T) {
		path := filepath.Join(cfg.AudiobookDir, "C", "C.mp3")
		writeTestFile(t, path, "mp3")
		if err := o.RemoveSuperseded(path, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("file should be removed")
		}
	})

	t.Run("refuses anything outside the library roots", func(t *testing.T) {
		for _, p := range []string{
			filepath.Join(cfg.IncomingDir, "seeding.epub"),
			filepath.Join(root, "elsewhere.epub"),
			filepath.Join(root, "ebooks-not-root", "x.epub"), // prefix trick
			cfg.EbookDir, // the root itself
			"/etc/hostname",
		} {
			if p != cfg.EbookDir && p != "/etc/hostname" {
				writeTestFile(t, p, "x")
			}
			err := o.RemoveSuperseded(p, "")
			if !errors.Is(err, ErrOutsideLibrary) && p != cfg.EbookDir {
				t.Errorf("%s: expected ErrOutsideLibrary, got %v", p, err)
			}
			if p == cfg.EbookDir && err == nil {
				t.Errorf("root itself must be refused")
			}
			if p != cfg.EbookDir && p != "/etc/hostname" {
				if _, err := os.Stat(p); err != nil {
					t.Errorf("%s was removed", p)
				}
			}
		}
	})

	t.Run("missing file is not an error", func(t *testing.T) {
		if err := o.RemoveSuperseded(filepath.Join(cfg.EbookDir, "gone", "gone.epub"), "abc"); err != nil {
			t.Fatalf("missing file: %v", err)
		}
	})

	t.Run("refuses directories and symlinks", func(t *testing.T) {
		dir := filepath.Join(cfg.EbookDir, "D")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := o.RemoveSuperseded(dir, ""); err == nil {
			t.Fatal("directory should be refused")
		}
		target := filepath.Join(root, "outside.epub")
		writeTestFile(t, target, "outside")
		link := filepath.Join(cfg.EbookDir, "D", "link.epub")
		if err := os.Symlink(target, link); err != nil {
			t.Skip("symlinks unsupported")
		}
		if err := o.RemoveSuperseded(link, ""); err == nil {
			t.Fatal("symlink should be refused")
		}
		if _, err := os.Stat(target); err != nil {
			t.Fatal("symlink target must survive")
		}
	})

	t.Run("nil organizer", func(t *testing.T) {
		var nilOrg *Organizer
		if err := nilOrg.RemoveSuperseded("/x", ""); err == nil {
			t.Fatal("expected error")
		}
		if err := (&Organizer{}).RemoveSuperseded("/x", ""); err == nil {
			t.Fatal("expected error for organizer without config")
		}
	})
}
