package organize

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestParseAudioFilename(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		artist string
		album  string
		title  string
	}{
		{
			"artist dash title",
			"/music/Bob Dylan - Blowin in the Wind.mp3",
			"Bob Dylan",
			"",
			"Blowin in the Wind",
		},
		{
			"artist dash album dash title",
			"/music/Pink Floyd - The Wall - Another Brick.mp3",
			"Pink Floyd",
			"The Wall",
			"Another Brick",
		},
		{
			"no dash pattern",
			"/music/simple_track.mp3",
			"",
			"",
			"simple_track",
		},
		{
			"with extension stripped",
			"/music/Artist - Title.flac",
			"Artist",
			"",
			"Title",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := parseAudioFilename(tt.path)
			if meta == nil {
				t.Fatal("expected non-nil meta")
			}
			if meta.Artist != tt.artist {
				t.Errorf("artist = %q, want %q", meta.Artist, tt.artist)
			}
			if meta.Album != tt.album {
				t.Errorf("album = %q, want %q", meta.Album, tt.album)
			}
			if meta.Title != tt.title {
				t.Errorf("title = %q, want %q", meta.Title, tt.title)
			}
		})
	}
}

func TestExtractAudioMeta_NonMP3(t *testing.T) {
	// For non-MP3 files, it should fall back to filename parsing
	meta := ExtractAudioMeta("/some/path/Artist - Album - Track.ogg")
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.Artist != "Artist" {
		t.Errorf("expected artist 'Artist', got %q", meta.Artist)
	}
	if meta.Album != "Album" {
		t.Errorf("expected album 'Album', got %q", meta.Album)
	}
	if meta.Title != "Track" {
		t.Errorf("expected title 'Track', got %q", meta.Title)
	}
}

func TestExtractAudioMeta_NonexistentMP3(t *testing.T) {
	// MP3 file that doesn't exist should fall back to filename parsing
	meta := ExtractAudioMeta("/nonexistent/Artist - Title.mp3")
	if meta == nil {
		t.Fatal("expected non-nil meta from filename fallback")
	}
	if meta.Artist != "Artist" {
		t.Errorf("expected artist 'Artist', got %q", meta.Artist)
	}
}

func TestExtractAudioMetaFromDir_NonexistentDir(t *testing.T) {
	meta := ExtractAudioMetaFromDir("/nonexistent/path")
	if meta != nil {
		t.Error("expected nil for nonexistent directory")
	}
}

func TestOrganizeAudiobookMissingSourceDoesNotCreateDestDir(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{
		FileOrgEnabled: true,
		AudiobookDir:   filepath.Join(root, "audiobooks"),
		// Matches the shipped default: torrents are removed after import, so
		// the automatic import mode resolves to a move.
		RemoveTorrentAfterImport: true,
	}
	o := NewOrganizer(cfg)

	missing := filepath.Join(root, "incoming", "missing.m4b")
	_, err := o.OrganizeAudiobook(missing, "Missing Book", "Missing Author")
	if err == nil {
		t.Fatal("expected error")
	}

	destDir := filepath.Join(cfg.AudiobookDir, "Missing Author", "Missing Book")
	if _, statErr := os.Stat(destDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected no dest dir on failure, stat err=%v", statErr)
	}
}

func TestOrganizeAudiobookMovesNestedTreeRecursively(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "incoming", "book")
	cd1 := filepath.Join(src, "CD1")
	cd2 := filepath.Join(src, "CD2", "Extras")

	if err := os.MkdirAll(cd1, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cd2, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cd1, "track01.m4b"), []byte("cd1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cd2, "track02.m4b"), []byte("cd2"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		FileOrgEnabled: true,
		AudiobookDir:   filepath.Join(root, "audiobooks"),
		// Matches the shipped default: torrents are removed after import, so
		// the automatic import mode resolves to a move.
		RemoveTorrentAfterImport: true,
	}
	o := NewOrganizer(cfg)

	dest, err := o.OrganizeAudiobook(src, "Nested Book", "Recursive Author")
	if err != nil {
		t.Fatalf("organize failed: %v", err)
	}

	wantRoot := filepath.Join(cfg.AudiobookDir, "Recursive Author", "Nested Book")
	if dest != wantRoot {
		t.Fatalf("dest = %q, want %q", dest, wantRoot)
	}

	if _, err := os.Stat(filepath.Join(wantRoot, "CD1", "track01.m4b")); err != nil {
		t.Fatalf("expected CD1 track at destination: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wantRoot, "CD2", "Extras", "track02.m4b")); err != nil {
		t.Fatalf("expected nested track at destination: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("expected source tree removed, stat err=%v", err)
	}
}

func TestOrganizeAudiobookSkipsSymlinks(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "incoming", "book")
	outside := filepath.Join(root, "outside.txt")

	if err := os.MkdirAll(src, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "track01.m4b"), []byte("audio"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, []byte("outside"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(src, "linked.m4b")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	cfg := &config.Config{
		FileOrgEnabled: true,
		AudiobookDir:   filepath.Join(root, "audiobooks"),
		// Matches the shipped default: torrents are removed after import, so
		// the automatic import mode resolves to a move.
		RemoveTorrentAfterImport: true,
	}
	o := NewOrganizer(cfg)

	dest, err := o.OrganizeAudiobook(src, "Symlink Book", "Careful Author")
	if err != nil {
		t.Fatalf("organize failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dest, "track01.m4b")); err != nil {
		t.Fatalf("expected regular track at destination: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dest, "linked.m4b")); !os.IsNotExist(err) {
		t.Fatalf("expected symlink skipped, lstat err=%v", err)
	}
	data, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("expected symlink target left untouched: %v", err)
	}
	if string(data) != "outside" {
		t.Fatalf("outside file = %q, want %q", data, "outside")
	}
}

func TestOrganizeAudiobookRejectsSymlinkSource(t *testing.T) {
	root := t.TempDir()
	realSrc := filepath.Join(root, "incoming", "book")
	linkSrc := filepath.Join(root, "incoming", "linked-book")

	if err := os.MkdirAll(realSrc, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(realSrc, "track01.m4b"), []byte("audio"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realSrc, linkSrc); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	cfg := &config.Config{
		FileOrgEnabled: true,
		AudiobookDir:   filepath.Join(root, "audiobooks"),
		// Matches the shipped default: torrents are removed after import, so
		// the automatic import mode resolves to a move.
		RemoveTorrentAfterImport: true,
	}
	o := NewOrganizer(cfg)

	_, err := o.OrganizeAudiobook(linkSrc, "Linked Book", "Careful Author")
	if err == nil {
		t.Fatal("expected symlink source error")
	}

	destDir := filepath.Join(cfg.AudiobookDir, "Careful Author", "Linked Book")
	if _, err := os.Stat(destDir); !os.IsNotExist(err) {
		t.Fatalf("expected no destination for symlink source, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(realSrc, "track01.m4b")); err != nil {
		t.Fatalf("expected symlink target left untouched: %v", err)
	}
	if _, err := os.Lstat(linkSrc); err != nil {
		t.Fatalf("expected symlink source left untouched: %v", err)
	}
}
