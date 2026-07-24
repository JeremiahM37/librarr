package organize

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// EbookMetadata contains the best metadata available for an ebook file.
// Metadata embedded in the file is preferred; filename values are fallbacks.
type EbookMetadata struct {
	Title  string
	Author string
}

var pdfInfoValueRe = regexp.MustCompile(`/((?:Title)|(?:Author))\s*\(([^()]*)\)`)

// ExtractEbookMetadata returns metadata for supported ebook formats. Readers
// without a supported embedded metadata format still receive a useful
// filename-derived title, while callers retain their own final fallback.
func ExtractEbookMetadata(filePath string) EbookMetadata {
	metadata := parseEbookFilename(filePath)
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".epub":
		if embedded, err := ExtractEPUBMeta(filePath); err == nil {
			if embedded.Title != "" {
				metadata.Title = embedded.Title
			}
			if embedded.Author != "" {
				metadata.Author = embedded.Author
			}
		}
	case ".pdf":
		if embedded := extractPDFInfo(filePath); embedded.Title != "" || embedded.Author != "" {
			if embedded.Title != "" {
				metadata.Title = embedded.Title
			}
			if embedded.Author != "" {
				metadata.Author = embedded.Author
			}
		}
	}
	return metadata
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
