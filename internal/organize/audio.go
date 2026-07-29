package organize

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// AudioMeta holds extracted audio metadata.
type AudioMeta struct {
	Artist string
	Album  string
	Title  string
}

// ExtractAudioMeta extracts metadata from an audio file.
// It tries to read ID3v2 tags from MP3 files by parsing the header directly.
// Falls back to filename parsing if no tags found.
func ExtractAudioMeta(path string) *AudioMeta {
	// Try to read ID3v2 tags from the file header.
	if embedded := extractEmbeddedAudioMeta(path); embedded != nil {
		return embedded
	}

	// Fallback: parse from filename.
	return parseAudioFilename(path)
}

func extractEmbeddedAudioMeta(path string) *AudioMeta {
	if strings.HasSuffix(strings.ToLower(path), ".mp3") {
		return readID3v2Tags(path)
	}
	return nil
}

// readID3v2Tags reads basic ID3v2.3/2.4 tags from an MP3 file.
func readID3v2Tags(path string) *AudioMeta {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	// Check for ID3v2 header.
	header := make([]byte, 10)
	if _, err := f.Read(header); err != nil {
		return nil
	}

	if string(header[:3]) != "ID3" {
		return nil
	}

	// Parse header size (syncsafe integer).
	size := int(header[6])<<21 | int(header[7])<<14 | int(header[8])<<7 | int(header[9])
	if size <= 0 || size > 1024*1024 { // limit to 1MB header
		return nil
	}

	tagData := make([]byte, size)
	if _, err := f.Read(tagData); err != nil {
		return nil
	}

	meta := &AudioMeta{}
	pos := 0
	for pos+10 < len(tagData) {
		frameID := string(tagData[pos : pos+4])
		if frameID[0] == 0 {
			break
		}

		frameSize := int(tagData[pos+4])<<24 | int(tagData[pos+5])<<16 | int(tagData[pos+6])<<8 | int(tagData[pos+7])
		if frameSize <= 0 || pos+10+frameSize > len(tagData) {
			break
		}

		frameData := tagData[pos+10 : pos+10+frameSize]

		// Skip encoding byte.
		text := ""
		if len(frameData) > 1 {
			encoding := frameData[0]
			switch encoding {
			case 0, 3: // ISO-8859-1 or UTF-8
				text = strings.TrimRight(string(frameData[1:]), "\x00")
			case 1, 2: // UTF-16
				// Simple extraction: skip BOM and null bytes.
				var b []byte
				for i := 1; i < len(frameData); i++ {
					if frameData[i] != 0 {
						b = append(b, frameData[i])
					}
				}
				text = string(b)
			}
		}

		text = strings.TrimSpace(text)

		switch frameID {
		case "TPE1": // Artist
			if meta.Artist == "" {
				meta.Artist = text
			}
		case "TALB": // Album
			if meta.Album == "" {
				meta.Album = text
			}
		case "TIT2": // Title
			if meta.Title == "" {
				meta.Title = text
			}
		}

		pos += 10 + frameSize
	}

	if meta.Artist != "" || meta.Album != "" || meta.Title != "" {
		return meta
	}
	return nil
}

// parseAudioFilename extracts artist and title from filename patterns.
func parseAudioFilename(path string) *AudioMeta {
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	name = strings.TrimSuffix(name, ext)

	meta := &AudioMeta{}

	// Common pattern: "Artist - Title" or "Artist - Album - Title"
	dashRe := regexp.MustCompile(`^(.+?)\s*-\s*(.+)$`)
	if m := dashRe.FindStringSubmatch(name); len(m) >= 3 {
		meta.Artist = strings.TrimSpace(m[1])
		remainder := strings.TrimSpace(m[2])

		// Check for second dash (Artist - Album - Title).
		if m2 := dashRe.FindStringSubmatch(remainder); len(m2) >= 3 {
			meta.Album = strings.TrimSpace(m2[1])
			meta.Title = strings.TrimSpace(m2[2])
		} else {
			meta.Title = remainder
		}
		return meta
	}

	meta.Title = name
	return meta
}

// ExtractAudiobookPathMetadata derives conservative audiobook metadata from a
// library path when embedded tags are unavailable. It assumes the immediate
// parent folder is the author and the filename stem is the title, which matches
// the mounted-library layout used by Librarr's explicit library scanner.
func ExtractAudiobookPathMetadata(path string) *AudioMeta {
	author := strings.TrimSpace(filepath.Base(filepath.Dir(path)))
	title := audiobookTitleFromFile(path, author)
	return &AudioMeta{Artist: author, Title: title}
}

func audiobookTitleFromFile(path, author string) string {
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if author != "" {
		prefix := strings.TrimSpace(author) + " - "
		if strings.HasPrefix(strings.ToLower(name), strings.ToLower(prefix)) {
			name = strings.TrimSpace(name[len(prefix):])
		}
	}
	name = stripAudiobookPartSuffix(name)
	return strings.TrimSpace(name)
}

func stripAudiobookPartSuffix(title string) string {
	parts := strings.Split(title, " - ")
	if len(parts) <= 1 {
		return title
	}
	last := strings.TrimSpace(parts[len(parts)-1])
	if strings.EqualFold(last, "part") || strings.HasPrefix(strings.ToLower(last), "part ") {
		return strings.TrimSpace(strings.Join(parts[:len(parts)-1], " - "))
	}
	return title
}

// ExtractAudioMetaFromDir scans a directory for audio files and extracts
// artist/album from the first valid file found.
func ExtractAudioMetaFromDir(dirPath string) *AudioMeta {
	audioExts := map[string]bool{
		".mp3": true, ".m4a": true, ".m4b": true,
		".ogg": true, ".flac": true, ".opus": true,
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if !audioExts[ext] {
			continue
		}
		meta := ExtractAudioMeta(filepath.Join(dirPath, entry.Name()))
		if meta != nil && (meta.Artist != "" || meta.Album != "") {
			return meta
		}
	}
	return nil
}
