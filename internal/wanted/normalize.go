package wanted

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/jamie75/librarr/internal/models"
)

const (
	ConfidenceLow    = "low"
	ConfidenceMedium = "medium"
	ConfidenceHigh   = "high"
)

type NormalizationResult struct {
	Original      models.WantedBook `json:"original"`
	Normalized    models.WantedBook `json:"normalized"`
	ChangedFields []string          `json:"changed_fields"`
	Confidence    string            `json:"confidence"`
	Warnings      []string          `json:"warnings,omitempty"`
	Applied       bool              `json:"applied"`
}

type parsedRelease struct {
	title      string
	author     string
	format     string
	language   string
	confidence string
	warnings   []string
}

var (
	bracketTokenRe     = regexp.MustCompile(`(?i)\s*[\[\(]\s*(?:eng|english|en|epub|mobi|azw3|pdf|cbz|cbr|m4b|mp3)(?:\s*(?:/|\s)\s*(?:eng|english|en|epub|mobi|azw3|pdf|cbz|cbr|m4b|mp3))*\s*[\]\)]\s*`)
	formatTokenRe      = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])(epub|mobi|azw3|pdf|cbz|cbr|m4b|mp3)(?:$|[^a-z0-9])`)
	languageTokenRe    = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])(eng|english|en)(?:$|[^a-z0-9])`)
	fileExtensionRe    = regexp.MustCompile(`(?i)\.(epub|mobi|azw3|pdf|cbz|cbr|m4b|mp3)$`)
	releaseGroupRe     = regexp.MustCompile(`\s+-\s+[A-Z0-9]{2,12}$`)
	repeatedSpaceRe    = regexp.MustCompile(`\s+`)
	titleByAuthorRe    = regexp.MustCompile(`(?i)\s+by\s+`)
	authorTitleDashRe  = regexp.MustCompile(`\s+-\s+`)
	dashLikeRe         = regexp.MustCompile(`[‐–—―]`)
	punctuationQueryRe = regexp.MustCompile(`[^\p{L}\p{N}]+`)
)

// NormalizeBook keeps Wanted's canonical identity separate from release context.
// Structured fields win; the raw release title is parsed only when title/author
// are missing or the title still looks like an unprocessed release name.
func NormalizeBook(book models.WantedBook) NormalizationResult {
	original := book
	book = trimBook(book)

	releaseTitle := strings.TrimSpace(book.OriginReleaseTitle)
	if releaseTitle == "" && looksLikeReleaseTitle(book.Title) {
		releaseTitle = book.Title
		book.OriginReleaseTitle = releaseTitle
	}
	parsed := parseReleaseTitle(releaseTitle)

	confidence := ConfidenceHigh
	var warnings []string
	if releaseTitle != "" {
		confidence = parsed.confidence
		warnings = append(warnings, parsed.warnings...)
	}

	if book.PreferredFormat == "" {
		book.PreferredFormat = parsed.format
	}
	if book.Language == "" {
		book.Language = parsed.language
	}

	if shouldUseParsedTitle(book, releaseTitle) && parsed.title != "" {
		book.Title = parsed.title
	}
	if strings.TrimSpace(book.Author) == "" && parsed.author != "" {
		book.Author = parsed.author
	}
	if strings.TrimSpace(book.Author) == "" && releaseTitle != "" {
		warnings = append(warnings, "author could not be parsed confidently")
	}
	book = trimBook(book)
	if book.MediaType == "" {
		book.MediaType = "ebook"
	}
	if book.Source == "" && book.OriginSource != "" {
		book.Source = book.OriginSource
	}

	changed := changedFields(original, book)
	return NormalizationResult{
		Original:      original,
		Normalized:    book,
		ChangedFields: changed,
		Confidence:    confidence,
		Warnings:      uniqueStrings(warnings),
	}
}

func CleanSearchPhrase(value string) string {
	value = removeReleaseTokens(value)
	value = punctuationQueryRe.ReplaceAllString(value, " ")
	return strings.Join(strings.Fields(value), " ")
}

func ParseReleaseForTest(title string) NormalizationResult {
	book := models.WantedBook{Title: title, OriginReleaseTitle: title, Source: "torrent", OriginSource: "prowlarr", MediaType: "ebook"}
	return NormalizeBook(book)
}

func shouldUseParsedTitle(book models.WantedBook, releaseTitle string) bool {
	title := strings.TrimSpace(book.Title)
	if title == "" {
		return true
	}
	if releaseTitle != "" && strings.EqualFold(title, releaseTitle) {
		return true
	}
	return looksLikeReleaseTitle(title) && strings.TrimSpace(book.Author) == ""
}

func parseReleaseTitle(title string) parsedRelease {
	title = strings.TrimSpace(title)
	if title == "" {
		return parsedRelease{confidence: ConfidenceLow}
	}

	format := detectFormat(title)
	language := detectLanguage(title)
	cleaned := removeReleaseTokens(title)
	cleaned = releaseGroupRe.ReplaceAllString(cleaned, "")
	cleaned = strings.TrimSpace(cleaned)

	if parts := titleByAuthorRe.Split(cleaned, -1); len(parts) >= 2 {
		author := strings.TrimSpace(parts[len(parts)-1])
		left := strings.TrimSpace(strings.Join(parts[:len(parts)-1], " by "))
		if looksLikeAuthor(author) && left != "" {
			return parsedRelease{
				title:      canonicalizeSubtitle(left),
				author:     normalizePersonName(author),
				format:     format,
				language:   language,
				confidence: ConfidenceHigh,
			}
		}
	}

	if parts := authorTitleDashRe.Split(cleaned, 2); len(parts) == 2 {
		left := strings.TrimSpace(parts[0])
		right := strings.TrimSpace(parts[1])
		if looksLikeAuthor(left) && right != "" {
			return parsedRelease{
				title:      canonicalizeSubtitle(right),
				author:     normalizePersonName(left),
				format:     format,
				language:   language,
				confidence: ConfidenceMedium,
			}
		}
	}

	return parsedRelease{
		title:      canonicalizeSubtitle(cleaned),
		format:     format,
		language:   language,
		confidence: ConfidenceLow,
		warnings:   []string{"release title did not contain a confident author pattern"},
	}
}

func removeReleaseTokens(title string) string {
	title = dashLikeRe.ReplaceAllString(title, "-")
	title = fileExtensionRe.ReplaceAllString(title, "")
	title = bracketTokenRe.ReplaceAllString(title, " ")
	title = repeatedSpaceRe.ReplaceAllString(title, " ")
	return strings.TrimSpace(title)
}

func detectFormat(title string) string {
	match := formatTokenRe.FindStringSubmatch(title)
	if len(match) > 1 {
		return strings.ToLower(match[1])
	}
	return ""
}

func detectLanguage(title string) string {
	match := languageTokenRe.FindStringSubmatch(title)
	if len(match) > 1 {
		return "en"
	}
	return ""
}

func looksLikeReleaseTitle(title string) bool {
	return detectFormat(title) != "" || detectLanguage(title) != "" || strings.Contains(strings.ToLower(title), " by ")
}

func looksLikeAuthor(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	words := strings.Fields(value)
	if len(words) < 2 || len(words) > 6 {
		return false
	}
	if detectFormat(value) != "" || detectLanguage(value) != "" {
		return false
	}
	for _, word := range words {
		word = strings.Trim(word, ".,'\"()[]")
		if word == "" {
			return false
		}
		runes := []rune(word)
		if len(runes) == 0 || !unicode.IsLetter(runes[0]) {
			return false
		}
	}
	return true
}

func canonicalizeSubtitle(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}
	if idx := strings.Index(title, ", "); idx > 0 && idx+2 < len(title) {
		next := []rune(title[idx+2:])
		if len(next) > 0 && unicode.IsLower(next[0]) {
			title = title[:idx] + ": " + upperFirst(title[idx+2:])
		}
	}
	return strings.Join(strings.Fields(title), " ")
}

func upperFirst(value string) string {
	runes := []rune(value)
	for i, r := range runes {
		if unicode.IsLetter(r) {
			runes[i] = unicode.ToUpper(r)
			break
		}
	}
	return string(runes)
}

func normalizePersonName(value string) string {
	parts := strings.Fields(strings.Trim(value, " .,"))
	for i, part := range parts {
		trimmed := strings.Trim(part, " .,")
		if len([]rune(trimmed)) == 1 && i > 0 && i < len(parts)-1 {
			parts[i] = trimmed + "."
		} else {
			parts[i] = trimmed
		}
	}
	return strings.Join(parts, " ")
}

func trimBook(in models.WantedBook) models.WantedBook {
	in.Title = strings.TrimSpace(in.Title)
	in.Author = strings.TrimSpace(in.Author)
	in.ISBN = strings.TrimSpace(in.ISBN)
	in.ASIN = strings.TrimSpace(in.ASIN)
	in.Series = strings.TrimSpace(in.Series)
	in.Publisher = strings.TrimSpace(in.Publisher)
	in.Language = strings.ToLower(strings.TrimSpace(in.Language))
	in.CoverURL = strings.TrimSpace(in.CoverURL)
	in.Description = strings.TrimSpace(in.Description)
	in.Source = strings.TrimSpace(in.Source)
	in.MediaType = strings.TrimSpace(strings.ToLower(in.MediaType))
	in.PreferredFormat = strings.TrimSpace(strings.ToLower(in.PreferredFormat))
	in.OriginSource = strings.TrimSpace(in.OriginSource)
	in.OriginReleaseTitle = strings.TrimSpace(in.OriginReleaseTitle)
	in.OriginIndexer = strings.TrimSpace(in.OriginIndexer)
	in.SourceID = strings.TrimSpace(in.SourceID)
	return in
}

func changedFields(before, after models.WantedBook) []string {
	fields := []struct {
		name string
		a    string
		b    string
	}{
		{"title", before.Title, after.Title},
		{"author", before.Author, after.Author},
		{"language", before.Language, after.Language},
		{"preferred_format", before.PreferredFormat, after.PreferredFormat},
		{"origin_source", before.OriginSource, after.OriginSource},
		{"origin_release_title", before.OriginReleaseTitle, after.OriginReleaseTitle},
		{"origin_indexer", before.OriginIndexer, after.OriginIndexer},
		{"source_id", before.SourceID, after.SourceID},
	}
	var changed []string
	for _, field := range fields {
		if strings.TrimSpace(field.a) != strings.TrimSpace(field.b) {
			changed = append(changed, field.name)
		}
	}
	return changed
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
