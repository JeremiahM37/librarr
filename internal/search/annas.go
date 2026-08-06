package search

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/PuerkitoBio/goquery"
)

var annasMetadataFormatRe = regexp.MustCompile(`(?i)·\s*([a-z0-9]+)\s*·`)

// Anna's Archive prints one interpunct-separated metadata line per result card:
//
//	English [en] · EPUB · 1.2MB · 2012 · 📕 Book (fiction) · 🚀/lgli/zlib
//
// The segments are not positional — a card may omit the year, the language, or
// both — so each one is classified independently and unrecognised segments
// (content type, mirror list, the trailing "Save" control) are ignored.
var (
	annasLangRe = regexp.MustCompile(`^[\p{L} ()'.-]+\[([a-zA-Z]{2,3}(?:-[a-zA-Z]{2,4})?)\]$`)
	annasSizeRe = regexp.MustCompile(`(?i)^\d+(?:\.\d+)?\s*(?:[KMGT]i?)?B$`)
	annasYearRe = regexp.MustCompile(`^[12]\d{3}$`)
	// Trailing "[Riordan, Rick]" style inverted-name duplicates on author links.
	annasAuthorAltRe = regexp.MustCompile(`\s*\[[^\]]*\]\s*$`)
)

// annasFormats lists the file extensions Anna's Archive reports in the metadata
// line. Anything else in that position is treated as unknown rather than
// guessed at, so a stray segment never masquerades as a format.
var annasFormats = map[string]bool{
	"epub": true, "pdf": true, "mobi": true, "azw": true, "azw3": true,
	"djvu": true, "fb2": true, "cbz": true, "cbr": true, "txt": true,
	"rtf": true, "doc": true, "docx": true, "lit": true, "zip": true,
	"m4b": true, "m4a": true, "mp3": true, "opf": true,
}

// annasCardMeta is the subset of an Anna's Archive card that librarr surfaces.
type annasCardMeta struct {
	Language string
	Format   string
	Size     string
	Year     string
}

// parseAnnasMetaLine classifies each interpunct-separated segment of a card's
// metadata line by shape, so segment order and optional fields don't matter.
func parseAnnasMetaLine(line string) annasCardMeta {
	var meta annasCardMeta
	for _, seg := range strings.Split(line, "·") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		switch {
		case meta.Language == "" && annasLangRe.MatchString(seg):
			meta.Language = strings.ToLower(annasLangRe.FindStringSubmatch(seg)[1])
		case meta.Size == "" && annasSizeRe.MatchString(seg):
			meta.Size = seg
		case meta.Year == "" && annasYearRe.MatchString(seg):
			meta.Year = seg
		case meta.Format == "" && annasFormats[strings.ToLower(seg)]:
			meta.Format = strings.ToLower(seg)
		}
	}
	return meta
}

// annasMetaLineText finds a card's metadata line. The line lives in a
// `font-semibold … leading-[1.2]` div that sits next to the title link, but
// Anna's markup churns, so the search is by shape: the shortest descendant div
// carrying interpuncts. Scripts are stripped first — goquery's Text() would
// otherwise splice the card's inline JS into the line.
func annasMetaLineText(link *goquery.Selection) string {
	best := ""
	consider := func(sel *goquery.Selection) {
		clone := sel.Clone()
		clone.Find("script, style").Remove()
		text := strings.TrimSpace(clone.Text())
		if !strings.Contains(text, "·") {
			return
		}
		if best == "" || len(text) < len(best) {
			best = text
		}
	}

	// Some layouts nest the metadata inside the title link itself.
	link.Find("div").Each(func(_ int, sel *goquery.Selection) { consider(sel) })
	if best != "" {
		return best
	}
	link.Closest("div.flex").Find("div").Each(func(_ int, sel *goquery.Selection) { consider(sel) })
	return best
}

// annasPublisherImprint reduces the publisher link's full citation to just the
// imprint. Anna's crams series, city, distributor and date into that one link:
//
//	"Hyperion Book CH, The Heroes of Olympus, Book Three, New York, USA, 2012"
//	"Disney Book Group : Made available through hoopla"
//
// so the imprint is whatever precedes the first separator. An over-long result
// means the shape wasn't what we expected — drop it rather than show noise.
func annasPublisherImprint(citation string) string {
	cut := len(citation)
	for _, sep := range []string{",", " : ", ";"} {
		if idx := strings.Index(citation, sep); idx > 0 && idx < cut {
			cut = idx
		}
	}
	imprint := strings.TrimSpace(citation[:cut])
	if len(imprint) > 80 {
		return ""
	}
	return imprint
}

// annasCardLink returns the text of the sibling link marked with the given
// Material Design icon class — Anna's tags the author link with
// `mdi--user-edit` and the publisher link with `mdi--company`.
func annasCardLink(link *goquery.Selection, iconClass string) string {
	matches := link.Closest("div.flex").Find("a").FilterFunction(func(_ int, a *goquery.Selection) bool {
		return a.Find("span[class*='"+iconClass+"']").Length() > 0
	})
	// Exactly one, or the enclosing div.flex wasn't this card's — showing a
	// neighbouring book's author would be worse than showing none.
	if matches.Length() != 1 {
		return ""
	}
	return strings.TrimSpace(matches.First().Text())
}

// AnnasArchive searches Anna's Archive by scraping HTML results.
type AnnasArchive struct {
	cfg    *config.Config
	client *http.Client
}

// NewAnnasArchive creates a new Anna's Archive searcher.
func NewAnnasArchive(cfg *config.Config, client *http.Client) *AnnasArchive {
	return &AnnasArchive{cfg: cfg, client: client}
}

func (a *AnnasArchive) Name() string         { return "annas" }
func (a *AnnasArchive) Label() string        { return "Anna's Archive" }
func (a *AnnasArchive) Enabled() bool        { return a.cfg.AnnasArchiveDomain != "" }
func (a *AnnasArchive) SearchTab() string    { return "main" }
func (a *AnnasArchive) DownloadType() string { return "direct" }

func (a *AnnasArchive) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	var results []models.SearchResult
	seenMD5 := make(map[string]bool)

	// Search multiple variations for better coverage.
	searches := []struct {
		q   string
		ext string
	}{
		{query, "epub"},
		{query, ""},
	}

	for _, s := range searches {
		res, err := a.doSearch(ctx, s.q, s.ext, seenMD5)
		if err != nil {
			slog.Warn("anna's archive search variant failed", "query", s.q, "ext", s.ext, "error", err)
			continue
		}
		results = append(results, res...)
	}

	// Sort by size descending.
	sort.Slice(results, func(i, j int) bool {
		return parseSizeBytes(results[i].SizeHuman) > parseSizeBytes(results[j].SizeHuman)
	})

	if len(results) > 50 {
		results = results[:50]
	}
	return results, nil
}

func (a *AnnasArchive) doSearch(ctx context.Context, query, ext string, seenMD5 map[string]bool) ([]models.SearchResult, error) {
	baseURL := fmt.Sprintf("https://%s/search", a.cfg.AnnasArchiveDomain)

	req, err := http.NewRequestWithContext(ctx, "GET", baseURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("q", query)
	if ext != "" {
		q.Set("ext", ext)
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", a.cfg.UserAgent)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d from annas-archive", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse HTML: %w", err)
	}

	var results []models.SearchResult
	md5Re := regexp.MustCompile(`/md5/([a-f0-9]+)`)

	// Collect sizes from metadata lines. Use HTML string search because goquery .Text()
	// on divs includes all child text making them too long to filter reliably.
	// The metadata format is: "English [en] · EPUB · 0.4MB · ..."  in divs with class "leading-[1.2]"
	htmlStr, _ := doc.Html()
	metaSizeRe := regexp.MustCompile(`leading-\[1\.2\][^>]*>[^<]*?(\d+[\.\d]*[KMG]i?B)`)
	sizeMatches := metaSizeRe.FindAllStringSubmatch(htmlStr, -1)
	var metadataSizes []string
	for _, m := range sizeMatches {
		if len(m) >= 2 {
			metadataSizes = append(metadataSizes, m[1])
		}
	}
	// Collect title links (the ones with text, not image wrappers).
	resultIdx := 0
	doc.Find("a[href*='/md5/']").Each(func(_ int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists {
			return
		}
		md5Match := md5Re.FindStringSubmatch(href)
		if len(md5Match) < 2 {
			return
		}
		md5 := md5Match[1]
		title := strings.TrimSpace(s.Text())
		if title == "" {
			return
		}
		if seenMD5[md5] {
			return
		}
		seenMD5[md5] = true

		meta := parseAnnasMetaLine(annasMetaLineText(s))

		// Prefer the size printed on this card; fall back to positional matching
		// against the sizes scraped from the page as a whole.
		sizeHuman := meta.Size
		if sizeHuman == "" && resultIdx < len(metadataSizes) {
			sizeHuman = metadataSizes[resultIdx]
		}
		format := meta.Format
		if format == "" {
			format = ext
			metadataText := s.Closest("div.flex").Find("div.font-semibold").Last().Text()
			if match := annasMetadataFormatRe.FindStringSubmatch(metadataText); len(match) > 1 {
				format = strings.ToLower(match[1])
			} else if format == "" {
				format = extractFormatFromTitle(title)
			}
		}
		resultIdx++

		// Extract author from "LastName, FirstName - Title" format.
		author := ""
		if idx := strings.Index(title, " - "); idx > 0 {
			candidate := title[:idx]
			if len(candidate) < 60 && strings.Contains(candidate, ",") {
				author = candidate
				title = strings.TrimSpace(title[idx+3:])
			}
		}
		// That pattern only fires on a minority of titles; the card's own author
		// link is the reliable source when it doesn't.
		if author == "" {
			author = annasAuthorAltRe.ReplaceAllString(annasCardLink(s, "mdi--user-edit"), "")
			if len(author) > 120 {
				author = ""
			}
		}

		publisher := annasPublisherImprint(annasCardLink(s, "mdi--company"))

		results = append(results, models.SearchResult{
			Source:    "annas",
			Title:     title,
			Author:    author,
			SizeHuman: sizeHuman,
			Format:    format,
			Language:  meta.Language,
			Publisher: publisher,
			Year:      meta.Year,
			MD5:       md5,
			URL:       fmt.Sprintf("https://%s/md5/%s", a.cfg.AnnasArchiveDomain, md5),
		})
	})

	return results, nil
}

func parseSizeBytes(s string) float64 {
	if s == "" {
		return 0
	}
	re := regexp.MustCompile(`([\d.]+)\s*(GB|MB|KB|B)`)
	m := re.FindStringSubmatch(strings.ToUpper(s))
	if len(m) < 3 {
		return 0
	}
	val, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	switch m[2] {
	case "GB":
		return val * 1e9
	case "MB":
		return val * 1e6
	case "KB":
		return val * 1e3
	default:
		return val
	}
}
