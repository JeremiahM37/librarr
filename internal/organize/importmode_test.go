package organize

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

// sameData reports whether two paths are the same inode, i.e. a hardlink pair
// rather than two independent copies.
func sameData(t *testing.T, a, b string) bool {
	t.Helper()
	ai, err := os.Stat(a)
	if err != nil {
		t.Fatalf("stat %s: %v", a, err)
	}
	bi, err := os.Stat(b)
	if err != nil {
		t.Fatalf("stat %s: %v", b, err)
	}
	return os.SameFile(ai, bi)
}

func writeFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0644); err != nil {
		t.Fatal(err)
	}
}

func ebookOrganizer(t *testing.T, mode string) (*Organizer, string, string) {
	t.Helper()
	root := t.TempDir()
	incoming := filepath.Join(root, "incoming")
	library := filepath.Join(root, "books")
	if err := os.MkdirAll(incoming, 0755); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{FileOrgEnabled: true, EbookDir: library, ImportMode: mode}
	return NewOrganizer(cfg), incoming, library
}

// The bug behind issue #59: with the default move mode the payload leaves the
// download folder, so keeping the torrent record does not keep it seedable.
func TestOrganizeEbook_MoveConsumesThePayload(t *testing.T) {
	o, incoming, _ := ebookOrganizer(t, config.ImportModeMove)
	src := filepath.Join(incoming, "book.epub")
	writeFile(t, src, []byte("payload"))

	dst, err := o.OrganizeEbook(src, "Title", "Author")
	if err != nil {
		t.Fatalf("OrganizeEbook: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("move mode should remove the source, stat err = %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("library file missing: %v", err)
	}
	if o.KeepsPayload() {
		t.Error("KeepsPayload() = true for move mode")
	}
}

func TestOrganizeEbook_HardlinkKeepsPayloadSeedable(t *testing.T) {
	o, incoming, _ := ebookOrganizer(t, config.ImportModeHardlink)
	src := filepath.Join(incoming, "book.epub")
	payload := []byte("seedable-payload")
	writeFile(t, src, payload)

	dst, err := o.OrganizeEbook(src, "Title", "Author")
	if err != nil {
		t.Fatalf("OrganizeEbook: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("hardlink mode must leave the download payload in place: %v", err)
	}
	if !sameData(t, src, dst) {
		t.Error("library file is not a hardlink of the download payload")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("dst content = %q, want %q", got, payload)
	}
	if !o.KeepsPayload() {
		t.Error("KeepsPayload() = false for hardlink mode")
	}
}

func TestOrganizeEbook_CopyKeepsIndependentPayload(t *testing.T) {
	o, incoming, _ := ebookOrganizer(t, config.ImportModeCopy)
	src := filepath.Join(incoming, "book.epub")
	payload := []byte("copied-payload")
	writeFile(t, src, payload)

	dst, err := o.OrganizeEbook(src, "Title", "Author")
	if err != nil {
		t.Fatalf("OrganizeEbook: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("copy mode must leave the download payload in place: %v", err)
	}
	if sameData(t, src, dst) {
		t.Error("copy mode produced a hardlink, want an independent copy")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("dst content = %q, want %q", got, payload)
	}
	if !o.KeepsPayload() {
		t.Error("KeepsPayload() = false for copy mode")
	}
}

// A cross-filesystem or hardlink-less mount (CIFS, exFAT) must not fail the
// import, and must not eat the source on the way to the fallback copy.
func TestOrganizeEbook_HardlinkFallsBackToCopyWithoutLosingSource(t *testing.T) {
	o, incoming, _ := ebookOrganizer(t, config.ImportModeHardlink)
	src := filepath.Join(incoming, "book.epub")
	payload := []byte("cross-device-payload")
	writeFile(t, src, payload)

	orig := linkFile
	linkFile = func(string, string) error { return errors.New("invalid cross-device link") }
	defer func() { linkFile = orig }()

	dst, err := o.OrganizeEbook(src, "Title", "Author")
	if err != nil {
		t.Fatalf("OrganizeEbook: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("fallback copy must leave the source in place: %v", err)
	}
	if sameData(t, src, dst) {
		t.Error("fallback should be a copy, not a link")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("dst content = %q, want %q", got, payload)
	}
}

// An import that repeats (re-download of the same book) must overwrite the
// library entry the same way move and copy do, not fail on EEXIST.
func TestHardlinkFileReplacesExistingDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.epub")
	dst := filepath.Join(dir, "dst.epub")
	writeFile(t, src, []byte("new"))
	writeFile(t, dst, []byte("stale"))

	if err := hardlinkFile(src, dst); err != nil {
		t.Fatalf("hardlinkFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("dst content = %q, want %q", got, "new")
	}
	if !sameData(t, src, dst) {
		t.Error("dst should be a hardlink of src after replacing the stale file")
	}
}

func TestOrganizeAudiobook_DirectoryHardlinkKeepsSourceTree(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "incoming", "Book - Author")
	writeFile(t, filepath.Join(srcDir, "01.mp3"), []byte("part one"))
	writeFile(t, filepath.Join(srcDir, "disc2", "02.mp3"), []byte("part two"))

	cfg := &config.Config{
		FileOrgEnabled: true,
		AudiobookDir:   filepath.Join(root, "audiobooks"),
		ImportMode:     config.ImportModeHardlink,
	}
	o := NewOrganizer(cfg)

	destDir, err := o.OrganizeAudiobook(srcDir, "Book", "Author")
	if err != nil {
		t.Fatalf("OrganizeAudiobook: %v", err)
	}
	if _, err := os.Stat(srcDir); err != nil {
		t.Fatalf("hardlink mode must keep the source tree: %v", err)
	}
	for _, rel := range []string{"01.mp3", filepath.Join("disc2", "02.mp3")} {
		src := filepath.Join(srcDir, rel)
		dst := filepath.Join(destDir, rel)
		if !sameData(t, src, dst) {
			t.Errorf("%s is not hardlinked into the library", rel)
		}
	}
}

func TestOrganizeAudiobook_DirectoryMoveStillRemovesSourceTree(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "incoming", "Book - Author")
	writeFile(t, filepath.Join(srcDir, "01.mp3"), []byte("part one"))

	cfg := &config.Config{
		FileOrgEnabled: true,
		AudiobookDir:   filepath.Join(root, "audiobooks"),
		ImportMode:     config.ImportModeMove,
	}
	destDir, err := NewOrganizer(cfg).OrganizeAudiobook(srcDir, "Book", "Author")
	if err != nil {
		t.Fatalf("OrganizeAudiobook: %v", err)
	}
	if _, err := os.Stat(srcDir); !os.IsNotExist(err) {
		t.Fatalf("move mode should remove the source tree, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(destDir, "01.mp3")); err != nil {
		t.Fatalf("library file missing: %v", err)
	}
}

func TestOrganizeManga_DirectoryCopyKeepsSourceDirectory(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "incoming", "Series")
	writeFile(t, filepath.Join(srcDir, "ch1.cbz"), []byte("chapter one"))

	cfg := &config.Config{
		FileOrgEnabled: true,
		MangaDir:       filepath.Join(root, "manga"),
		ImportMode:     config.ImportModeCopy,
	}
	destDir, err := NewOrganizer(cfg).OrganizeManga(srcDir, "Series")
	if err != nil {
		t.Fatalf("OrganizeManga: %v", err)
	}
	if _, err := os.Stat(filepath.Join(srcDir, "ch1.cbz")); err != nil {
		t.Fatalf("copy mode must keep the source directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destDir, "ch1.cbz")); err != nil {
		t.Fatalf("library file missing: %v", err)
	}
}

func TestOrganizeManga_DirectoryMoveStillRemovesSourceDirectory(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "incoming", "Series")
	writeFile(t, filepath.Join(srcDir, "ch1.cbz"), []byte("chapter one"))

	cfg := &config.Config{
		FileOrgEnabled: true,
		MangaDir:       filepath.Join(root, "manga"),
		ImportMode:     config.ImportModeMove,
	}
	if _, err := NewOrganizer(cfg).OrganizeManga(srcDir, "Series"); err != nil {
		t.Fatalf("OrganizeManga: %v", err)
	}
	if _, err := os.Stat(srcDir); !os.IsNotExist(err) {
		t.Fatalf("move mode should remove the source directory, stat err = %v", err)
	}
}

// Uploads are librarr's own temp files: nothing seeds them, so they must move
// even when the configured mode would keep the payload.
func TestMovingOverridesConfiguredImportMode(t *testing.T) {
	o, incoming, _ := ebookOrganizer(t, config.ImportModeHardlink)
	src := filepath.Join(incoming, "upload.epub")
	writeFile(t, src, []byte("uploaded"))

	mover := o.Moving()
	if mover.KeepsPayload() {
		t.Fatal("Moving() organizer should not keep the payload")
	}
	if _, err := mover.OrganizeEbook(src, "Title", "Author"); err != nil {
		t.Fatalf("OrganizeEbook: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("Moving() should remove the source, stat err = %v", err)
	}
	// The override must not leak back into the shared organizer.
	if !o.KeepsPayload() {
		t.Error("Moving() mutated the original organizer")
	}
}

func TestKeepsPayloadTreatsUnsetModeAsMove(t *testing.T) {
	o := NewOrganizer(&config.Config{FileOrgEnabled: true})
	if o.KeepsPayload() {
		t.Error("an unset ImportMode must behave as move")
	}
}
