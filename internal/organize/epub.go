package organize

import (
	"archive/zip"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"path"
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

	pkg, err := readOPFPackage(opfFile)
	if err != nil {
		return nil, err
	}

	return metadataFromOPF(pkg), nil
}

func metadataFromOPF(pkg *opfPackage) *EPUBMeta {
	if pkg == nil {
		return &EPUBMeta{}
	}
	author := normalizeAuthor(strings.TrimSpace(firstNonEmpty(pkg.Metadata.Creators...)))
	identifiers := map[string]string{}
	isbn := ""
	for _, identifier := range pkg.Metadata.Identifiers {
		value := strings.TrimSpace(identifier.Value)
		if value == "" {
			continue
		}
		scheme := strings.ToLower(strings.TrimSpace(firstNonEmpty(identifier.Scheme, identifier.ID)))
		if strings.Contains(scheme, "isbn") || looksLikeISBN(value) {
			clean := normalizeIdentifierValue(value)
			if clean != "" {
				isbn = clean
				identifiers["isbn"] = clean
			}
			continue
		}
		if scheme != "" {
			identifiers[scheme] = value
		}
	}
	for _, meta := range pkg.Metadata.Meta {
		property := strings.ToLower(strings.TrimSpace(firstNonEmpty(meta.Property, meta.Name)))
		content := strings.TrimSpace(firstNonEmpty(meta.Content, meta.Value))
		switch property {
		case "calibre:series", "belongs-to-collection":
			if pkgMetaValueRefinesCollection(meta) || property == "calibre:series" {
				pkg.Metadata.Series = firstNonEmpty(pkg.Metadata.Series, content)
			}
		case "calibre:series_index", "group-position":
			pkg.Metadata.SeriesIndex = firstNonEmpty(pkg.Metadata.SeriesIndex, content)
		case "dcterms:modified":
			// Internal EPUB timestamp, not publication metadata.
		}
	}
	return &EPUBMeta{
		Title:           strings.TrimSpace(pkg.Metadata.Title),
		Subtitle:        strings.TrimSpace(pkg.Metadata.Subtitle),
		Author:          author,
		Language:        strings.TrimSpace(firstNonEmpty(pkg.Metadata.Languages...)),
		Publisher:       strings.TrimSpace(firstNonEmpty(pkg.Metadata.Publishers...)),
		PublicationDate: strings.TrimSpace(firstNonEmpty(pkg.Metadata.Dates...)),
		Description:     strings.TrimSpace(pkg.Metadata.Description),
		ISBN:            isbn,
		Identifiers:     identifiers,
		Subjects:        uniqueCleanStrings(pkg.Metadata.Subjects),
		Series:          strings.TrimSpace(pkg.Metadata.Series),
		SeriesIndex:     strings.TrimSpace(pkg.Metadata.SeriesIndex),
	}
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
	Manifest opfManifest `xml:"manifest"`
	Guide    opfGuide    `xml:"guide"`
}

type opfMetadata struct {
	Title       string          `xml:"title"`
	Subtitle    string          `xml:"subtitle"`
	Creators    []string        `xml:"creator"`
	Languages   []string        `xml:"language"`
	Publishers  []string        `xml:"publisher"`
	Dates       []string        `xml:"date"`
	Description string          `xml:"description"`
	Subjects    []string        `xml:"subject"`
	Identifiers []opfIdentifier `xml:"identifier"`
	Meta        []opfMeta       `xml:"meta"`
	Series      string
	SeriesIndex string
}

type opfIdentifier struct {
	ID     string `xml:"id,attr"`
	Scheme string `xml:"scheme,attr"`
	Value  string `xml:",chardata"`
}

type opfMeta struct {
	Name     string `xml:"name,attr"`
	Property string `xml:"property,attr"`
	Refines  string `xml:"refines,attr"`
	Content  string `xml:"content,attr"`
	Value    string `xml:",chardata"`
}

type opfManifest struct {
	Items []opfManifestItem `xml:"item"`
}

type opfManifestItem struct {
	ID         string `xml:"id,attr"`
	Href       string `xml:"href,attr"`
	MediaType  string `xml:"media-type,attr"`
	Properties string `xml:"properties,attr"`
}

type opfGuide struct {
	References []opfGuideReference `xml:"reference"`
}

type opfGuideReference struct {
	Type  string `xml:"type,attr"`
	Href  string `xml:"href,attr"`
	Title string `xml:"title,attr"`
}

func ReadEPUBPackageMetadata(filePath string) (*EPUBMeta, error) {
	return ExtractEPUBMeta(filePath)
}

func readEPUBOPF(filePath string) (*opfPackage, string, error) {
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("open epub zip: %w", err)
	}
	defer r.Close()
	opfFile, err := findOPFFile(r.File)
	if err != nil {
		return nil, "", err
	}
	pkg, err := readOPFPackage(opfFile)
	if err != nil {
		return nil, "", err
	}
	return pkg, path.Dir(opfFile.Name), nil
}

func openZipText(file *zip.File, limit int64) (string, error) {
	rc, err := file.Open()
	if err != nil {
		return "", err
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > limit {
		return "", fmt.Errorf("zip entry is too large")
	}
	return string(data), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func uniqueCleanStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}

func looksLikeISBN(value string) bool {
	clean := normalizeIdentifierValue(value)
	return len(clean) == 10 || len(clean) == 13
}

func normalizeIdentifierValue(value string) string {
	value = strings.TrimSpace(value)
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme == "urn" {
		parts := strings.Split(parsed.Opaque, ":")
		value = parts[len(parts)-1]
	}
	var b strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else if r == 'X' || r == 'x' {
			b.WriteRune('X')
		}
	}
	return b.String()
}

func pkgMetaValueRefinesCollection(meta opfMeta) bool {
	refines := strings.TrimPrefix(strings.TrimSpace(meta.Refines), "#")
	return refines != ""
}
