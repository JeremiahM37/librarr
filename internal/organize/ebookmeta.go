package organize

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

// EbookMetadata contains the best metadata available for an ebook file.
// Metadata embedded in the file is preferred; filename values are fallbacks.
type EbookMetadata struct {
	Title           string
	Subtitle        string
	Author          string
	Language        string
	Publisher       string
	PublicationDate string
	Description     string
	ISBN            string
	Identifiers     map[string]string
	Subjects        []string
	Series          string
	SeriesIndex     string
}

var pdfInfoValueRe = regexp.MustCompile(`/((?:Title)|(?:Author))\s*\(([^()]*)\)`)
var parenRe = regexp.MustCompile(`\([^)]*\)`)

// ExtractEbookMetadata returns metadata for supported ebook formats. Readers
// without a supported embedded metadata format still receive a useful
// filename-derived title, while callers retain their own final fallback.
func ExtractEbookMetadata(filePath string) EbookMetadata {
	filename := parseEbookFilename(filePath)
	if strings.EqualFold(filepath.Ext(filePath), ".mobi") {
		filename = parseMOBIFilename(filePath)
	}
	embedded := extractEmbeddedEbookMetadata(filePath)
	metadata := filename
	if embedded.Title != "" {
		metadata.Title = embedded.Title
	}
	if embedded.Author != "" {
		metadata.Author = embedded.Author
	}
	return metadata
}

// ExtractFilenameEbookMetadata returns the filename-derived metadata fallback
// without consulting embedded metadata. This supports planning and diagnostics
// that need to explain why a title or author was selected.
func ExtractFilenameEbookMetadata(filePath string) EbookMetadata {
	if strings.EqualFold(filepath.Ext(filePath), ".mobi") {
		return parseMOBIFilename(filePath)
	}
	return parseEbookFilename(filePath)
}

// ExtractEmbeddedEbookMetadata returns embedded metadata for supported ebook
// formats without applying filename fallback.
func ExtractEmbeddedEbookMetadata(filePath string) EbookMetadata {
	return extractEmbeddedEbookMetadata(filePath)
}

func extractEmbeddedEbookMetadata(filePath string) EbookMetadata {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".epub":
		if embedded, err := ExtractEPUBMeta(filePath); err == nil {
			return *embedded
		}
	case ".pdf":
		return extractPDFInfo(filePath)
	}
	return EbookMetadata{}
}

func parseEbookFilename(filePath string) EbookMetadata {
	name := strings.TrimSpace(strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath)))
	name = bracketRe.ReplaceAllString(name, "")
	name = whitespaceRe.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	if name == "" {
		return EbookMetadata{}
	}

	metadata := EbookMetadata{Title: name}
	if parts := strings.SplitN(name, " - ", 2); len(parts) == 2 {
		if author := strings.TrimSpace(parts[0]); author != "" {
			metadata.Author = author
		}
		if title := strings.TrimSpace(parts[1]); title != "" {
			metadata.Title = title
		}
	}
	return metadata
}

func parseMOBIFilename(filePath string) EbookMetadata {
	name := cleanMOBIFilename(filePath)
	if name == "" {
		return EbookMetadata{}
	}

	parts := splitFilenameParts(name)
	switch len(parts) {
	case 0:
		return EbookMetadata{}
	case 1:
		return EbookMetadata{Title: parts[0]}
	case 2:
		leftLooksAuthor := looksLikeAuthorName(parts[0])
		rightLooksAuthor := looksLikeAuthorName(parts[1])
		if rightLooksAuthor && !leftLooksAuthor {
			return EbookMetadata{Title: parts[0], Author: parts[1]}
		}
		if leftLooksAuthor && !rightLooksAuthor {
			return EbookMetadata{Title: parts[1], Author: parts[0]}
		}
		if rightLooksAuthor {
			return EbookMetadata{Title: parts[0], Author: parts[1]}
		}
		return EbookMetadata{Title: parts[1], Author: parts[0]}
	default:
		return EbookMetadata{Title: parts[len(parts)-2], Author: parts[len(parts)-1]}
	}
}

func cleanMOBIFilename(filePath string) string {
	name := strings.TrimSpace(strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath)))
	name = bracketRe.ReplaceAllString(name, "")
	name = parenRe.ReplaceAllString(name, "")
	name = whitespaceRe.ReplaceAllString(name, " ")
	return strings.TrimSpace(name)
}

func splitFilenameParts(name string) []string {
	rawParts := strings.Split(name, " - ")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.Trim(strings.TrimSpace(part), "- ")
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func looksLikeAuthorName(value string) bool {
	words := strings.Fields(value)
	if len(words) < 2 || len(words) > 4 {
		return false
	}
	first := strings.ToLower(strings.Trim(words[0], ".,;:!?\"'"))
	if first == "a" || first == "an" || first == "the" {
		return false
	}
	for _, word := range words {
		word = strings.Trim(word, ".,;:!?\"'")
		if word == "" {
			return false
		}
		if len([]rune(word)) == 1 {
			if !unicode.IsUpper([]rune(word)[0]) {
				return false
			}
			continue
		}
		if strings.HasSuffix(word, ".") && len([]rune(strings.TrimSuffix(word, "."))) == 1 {
			continue
		}
		firstRune := []rune(word)[0]
		if !unicode.IsUpper(firstRune) {
			return false
		}
	}
	return true
}

func extractPDFInfo(filePath string) EbookMetadata {
	f, err := os.Open(filePath)
	if err != nil {
		return EbookMetadata{}
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, 4<<20))
	if err != nil {
		return EbookMetadata{}
	}
	var metadata EbookMetadata
	for _, match := range pdfInfoValueRe.FindAllSubmatch(data, -1) {
		value := strings.TrimSpace(string(bytes.ReplaceAll(match[2], []byte(`\\`), []byte(`\`))))
		switch string(match[1]) {
		case "Title":
			metadata.Title = value
		case "Author":
			metadata.Author = value
		}
	}
	return metadata
}
