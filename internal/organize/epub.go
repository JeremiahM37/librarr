package organize

import (
	"archive/zip"
	"encoding/xml"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// EPUBMeta holds extracted EPUB metadata.
type EPUBMeta = EbookMetadata

// ExtractEPUBMeta reads an EPUB file (ZIP archive) and extracts dc:title and dc:creator
// from the OPF metadata file.
func ExtractEPUBMeta(path string) (*EPUBMeta, error) {
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("open epub zip: %w", err)
	}
	defer r.Close()

	// Find the .opf file.
	var opfFile *zip.File
	for _, f := range r.File {
		ext := strings.ToLower(filepath.Ext(f.Name))
		if ext == ".opf" {
			opfFile = f
			break
		}
	}

	if opfFile == nil {
		// Try to find via container.xml.
		for _, f := range r.File {
			if strings.ToLower(f.Name) == "meta-inf/container.xml" {
				rc, err := f.Open()
				if err != nil {
					continue
				}
				var container containerXML
				if err := xml.NewDecoder(rc).Decode(&container); err == nil {
					for _, rf := range container.Rootfiles {
						for _, zf := range r.File {
							if zf.Name == rf.FullPath {
								opfFile = zf
								break
							}
						}
						if opfFile != nil {
							break
						}
					}
				}
				rc.Close()
				break
			}
		}
	}

	if opfFile == nil {
		return nil, fmt.Errorf("no .opf file found in epub")
	}

	rc, err := opfFile.Open()
	if err != nil {
		return nil, fmt.Errorf("open opf: %w", err)
	}
	defer rc.Close()

	var pkg opfPackage
	if err := xml.NewDecoder(rc).Decode(&pkg); err != nil {
		return nil, fmt.Errorf("parse opf: %w", err)
	}

	author := normalizeAuthor(strings.TrimSpace(pkg.Metadata.Creator))

	return &EPUBMeta{
		Title:  strings.TrimSpace(pkg.Metadata.Title),
		Author: author,
	}, nil
}

// normalizeAuthor cleans up common author name formats from EPUB metadata.
// "Last, First" → "First Last" (dc:creator often uses inverted form).
// "First Last" → unchanged.
// Strips stray punctuation artifacts from bad metadata.
func normalizeAuthor(author string) string {
	if author == "" {
		return ""
	}
	// Handle "Last, First" or "Last, First Middle" — single comma only.
	// Don't touch "Author1 & Author2" or "A, B, C" (multiple authors).
	if strings.Count(author, ",") == 1 && !strings.Contains(author, "&") && !strings.Contains(author, " and ") {
		parts := strings.SplitN(author, ",", 2)
		last := strings.TrimSpace(parts[0])
		first := strings.TrimSpace(parts[1])
		if first != "" && last != "" {
			author = first + " " + last
		}
	}
	// Strip leading/trailing punctuation artifacts (periods, semicolons)
	// that bad metadata sometimes includes.
	author = strings.Trim(author, ".,;:!?\"'")
	return strings.TrimSpace(author)
}

// VerifyEPUBTitle checks that the EPUB's dc:title has >= threshold word overlap
// with the expected title. Returns true if verification passes.
func VerifyEPUBTitle(epubPath, expectedTitle string, threshold float64) (bool, string, error) {
	meta, err := ExtractEPUBMeta(epubPath)
	if err != nil {
		return false, "", err
	}
	if meta.Title == "" {
		// No title in metadata, can't verify -- let it pass.
		return true, "", nil
	}

	overlap := wordOverlap(expectedTitle, meta.Title)
	if overlap >= threshold {
		return true, meta.Title, nil
	}
	return false, meta.Title, nil
}

var wordExtractRe = regexp.MustCompile(`\w+`)

var epubStopwords = map[string]bool{
	"the": true, "a": true, "an": true, "of": true, "in": true,
	"on": true, "at": true, "to": true, "for": true, "and": true,
	"or": true, "is": true, "it": true, "by": true, "with": true,
}

func wordOverlap(expected, actual string) float64 {
	expectedWords := extractSignificantWords(expected)
	actualWords := extractSignificantWords(actual)

	if len(expectedWords) == 0 {
		return 1.0
	}

	matches := 0
	for w := range expectedWords {
		if actualWords[w] {
			matches++
		}
	}
	return float64(matches) / float64(len(expectedWords))
}

func extractSignificantWords(s string) map[string]bool {
	words := make(map[string]bool)
	for _, w := range wordExtractRe.FindAllString(strings.ToLower(s), -1) {
		if !epubStopwords[w] && len(w) > 1 {
			words[w] = true
		}
	}
	return words
}

// XML structures for EPUB parsing.

type containerXML struct {
	XMLName   xml.Name   `xml:"container"`
	Rootfiles []rootfile `xml:"rootfiles>rootfile"`
}

type rootfile struct {
	FullPath string `xml:"full-path,attr"`
}

type opfPackage struct {
	XMLName  xml.Name    `xml:"package"`
	Metadata opfMetadata `xml:"metadata"`
}

type opfMetadata struct {
	Title   string `xml:"title"`
	Creator string `xml:"creator"`
}
