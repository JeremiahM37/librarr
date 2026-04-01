package organize

import (
	"testing"
)

func TestParseAudioFilename(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		artist   string
		album    string
		title    string
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
