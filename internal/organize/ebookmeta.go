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
	return overlayMetadata(parseEbookFilename(filePath), extractEmbeddedMetadata(filePath))
}

// ExtractEbookMetadataSet returns metadata for every file in one download.
// Files that are the same book in different formats (same folder and file
// name, different extension) share embedded metadata: a MOBI or AZW3, which
// has no supported embedded format, takes its title and author from the EPUB
// or PDF beside it instead of guessing from a filename whose word order
// varies by uploader ("Title - Author" vs "Author - Title").
func ExtractEbookMetadataSet(filePaths []string) map[string]EbookMetadata {
	embedded := make(map[string]EbookMetadata, len(filePaths))
	shared := make(map[string]EbookMetadata)
	for _, p := range filePaths {
		e := extractEmbeddedMetadata(p)
		embedded[p] = e
		key := bookKey(p)
		s := shared[key]
		if s.Title == "" {
			s.Title = e.Title
		}
		if s.Author == "" {
			s.Author = e.Author
		}
		shared[key] = s
	}

	result := make(map[string]EbookMetadata, len(filePaths))
	for _, p := range filePaths {
		e := overlayMetadata(shared[bookKey(p)], embedded[p])
		result[p] = overlayMetadata(parseEbookFilename(p), e)
	}
	return result
}

// bookKey identifies the formats of one book: same folder, same file name.
func bookKey(filePath string) string {
	return strings.ToLower(strings.TrimSuffix(filePath, filepath.Ext(filePath)))
}

// overlayMetadata returns base with any non-empty fields of top applied.
func overlayMetadata(base, top EbookMetadata) EbookMetadata {
	if top.Title != "" {
		base.Title = top.Title
	}
	if top.Author != "" {
		base.Author = top.Author
	}
	return base
}

// extractEmbeddedMetadata returns only the metadata stored inside the file.
func extractEmbeddedMetadata(filePath string) EbookMetadata {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".epub":
		if embedded, err := ExtractEPUBMeta(filePath); err == nil {
			return EbookMetadata{Title: embedded.Title, Author: embedded.Author}
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
