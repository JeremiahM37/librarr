package metadata

import (
	"regexp"
	"sort"
	"strings"
)

// A wrong cover is worse than no cover, so everything here errs towards a miss.
// Release names such as "Ursula K. Le Guin - The Left Hand of Darkness [EPUB]"
// are cleaned and split into candidate (title, author) readings, and an Open
// Library record is accepted only when one reading names it unambiguously.

// CoverKind separates the cover namespaces so a book and a manga series with the
// same words never share a cached cover.
type CoverKind string

const (
	CoverKindBook  CoverKind = "book"
	CoverKindManga CoverKind = "manga"
)

// coverReading is one way of reading a release name as a title and an author.
// Volume records that the raw name mentions a volume, so its subtitle may say
// which one and can't be dropped.
type coverReading struct {
	Title, Author string
	Volume        bool
}

var (
	coverBracketRe = regexp.MustCompile(`\[[^\]]*\]|\([^)]*\)|\{[^}]*\}`)
	coverGroupRe   = regexp.MustCompile(`-[A-Za-z0-9]+$`)
	coverNoiseRe   = regexp.MustCompile(`(?i)\b(?:epub|mobi|azw3|pdf|m4b|m4a|mp3|flac|aac|unabridged|abridged|retail|audiobook|ebook)\b|\b\d+\s*kbps\b`)
	coverYearRe    = regexp.MustCompile(`^(?:19|20)\d\d$`)
	coverDashRe    = regexp.MustCompile(`\s+[-–—]\s+`)
	coverByRe      = regexp.MustCompile(`(?i)\s+by\s+`)
	coverSpaceRe   = regexp.MustCompile(`\s+`)
	// Edition phrases aren't part of the title, and they keep Open Library's
	// search from finding the book at all ("10th-Anniversary Edition").
	coverEditionRe = regexp.MustCompile(`(?i)\b(?:(?:\d+(?:st|nd|rd|th)|first|second|third|tenth|twentieth|twenty-fifth|fiftieth)[\s-]+)?(?:anniversary|special|deluxe|revised|expanded|collector'?s|illustrated|annotated|updated|kindle)[\s-]+edition\b`)
	// Punctuation an edition phrase left hanging before " by ", " - " or the end.
	coverDanglingRe = regexp.MustCompile(`\s*[:;,]+(\s+(?i:by)\s|\s+[-–—]\s|\s*$)`)
	coverPunctGapRe = regexp.MustCompile(`\s+([:;,])`)
	coverSubtitleRe = regexp.MustCompile(`[:;]`)
	coverQueryPunct = regexp.MustCompile(`[:;,]`)
	coverVolumeRe   = regexp.MustCompile(`(?i)\b(?:vol(?:ume)?|book|part|tome|episode)\.?\s*(?:\d+|[ivxlc]+|one|two|three|four|five|six|seven|eight|nine|ten)\b|#\s*\d+`)
)

// cleanReleaseName strips the tags torrent and audiobook names carry around the
// real title: bracketed text, dotted word separators, release groups, format
// and bitrate words, and a trailing publication year.
func cleanReleaseName(raw string) string {
	s := coverBracketRe.ReplaceAllString(raw, " ")
	s = strings.TrimSpace(s)
	// A trailing "-GROUP" is only a release group on dotted names; "Catch-22"
	// and "Spider-Man" are titles.
	if !strings.Contains(s, " ") && strings.ContainsAny(s, "._") {
		s = coverGroupRe.ReplaceAllString(s, "")
		s = strings.NewReplacer(".", " ", "_", " ").Replace(s)
	}
	s = coverNoiseRe.ReplaceAllString(s, " ")
	s = coverEditionRe.ReplaceAllString(s, " ")
	s = coverDanglingRe.ReplaceAllString(s, "$1")
	s = coverPunctGapRe.ReplaceAllString(s, "$1")

	var kept []string
	for _, part := range coverDashRe.Split(s, -1) {
		words := strings.Fields(part)
		// A lone year is the title ("1984"), not a tag.
		if n := len(words); n > 1 && coverYearRe.MatchString(words[n-1]) {
			words = words[:n-1]
		}
		if len(words) > 0 {
			kept = append(kept, strings.Join(words, " "))
		}
	}
	// A year on its own among three or more parts is a tag ("Author - Title -
	// 2017"). Two parts always stay, so "George Orwell - 1984" keeps its title.
	for i := len(kept) - 1; i >= 0 && len(kept) > 2; i-- {
		if coverYearRe.MatchString(kept[i]) {
			kept = append(kept[:i], kept[i+1:]...)
		}
	}
	return strings.Join(kept, " - ")
}

// coverReadings lists the plausible (title, author) readings of a cleaned name,
// most specific first. A two-part dash name is tried in both orders, a longer
// one as "Author - Series - Title", and "Title by Author" in one; the whole string is always kept as a last resort so a title that
// itself contains a dash still has a chance.
func coverReadings(title, author string) []coverReading {
	cleaned := cleanReleaseName(title)
	if cleaned == "" {
		return nil
	}
	author = strings.TrimSpace(author)
	vol := coverVolumeRe.MatchString(title)
	r := func(t, a string) coverReading { return coverReading{Title: t, Author: a, Volume: vol} }

	parts := strings.Split(cleaned, " - ")
	byAuthor := false
	if len(parts) == 1 {
		if loc := coverByRe.FindAllStringIndex(cleaned, -1); len(loc) > 0 {
			last := loc[len(loc)-1]
			parts = []string{cleaned[:last[0]], cleaned[last[1]:]}
			byAuthor = true
		}
	}

	if author != "" {
		readings := []coverReading{r(cleaned, author)}
		if len(parts) > 1 {
			for _, p := range parts {
				readings = append(readings, r(p, author))
			}
		}
		return readings
	}

	var readings []coverReading
	switch {
	case byAuthor:
		// "Title by Author" has only one direction.
		readings = append(readings, r(parts[0], parts[1]))
	case len(parts) == 2:
		readings = append(readings,
			r(parts[0], parts[1]),
			r(parts[1], parts[0]))
	case len(parts) > 2:
		readings = append(readings, r(parts[len(parts)-1], parts[0]))
	}
	return append(readings, r(cleaned, ""))
}

// CoverKey identifies a cover lookup by the words of its cleaned title and
// author, ignoring order and noise, so equivalent release names share one cache
// entry. It is empty when there is nothing to search on.
func CoverKey(kind CoverKind, title, author string) string {
	text := coverDashRe.ReplaceAllString(cleanReleaseName(title), " ") + " " + author
	seen := map[string]bool{}
	var words []string
	for _, w := range olTitleWords(text) {
		if !seen[w] {
			seen[w] = true
			words = append(words, w)
		}
	}
	if len(words) == 0 {
		return ""
	}
	sort.Strings(words)
	return string(kind) + ":" + strings.Join(words, " ")
}

// coverQueryText is the free-text query sent to Open Library for a release name:
// its words without the separators. A "by" before the author is dropped, as
// coverReadings reads it, because it ranks study guides ("Summary of Sapiens by
// Yuval Noah Harari") above the book itself, often pushing the book out of the
// results.
func coverQueryText(title, author string) string {
	text := cleanReleaseName(title)
	if !strings.Contains(text, " - ") {
		if loc := coverByRe.FindAllStringIndex(text, -1); len(loc) > 0 {
			last := loc[len(loc)-1]
			text = text[:last[0]] + " " + text[last[1]:]
		}
	}
	text = coverDashRe.ReplaceAllString(text, " ")
	text = coverQueryPunct.ReplaceAllString(text, " ") + " " + author
	return strings.TrimSpace(coverSpaceRe.ReplaceAllString(text, " "))
}

// coverTitleMatches reports whether a record's title is the queried one: its
// full title or its main title (before a subtitle) has exactly the query's words.
func coverTitleMatches(query map[string]bool, title string) bool {
	if len(query) == 0 {
		return false
	}
	if olSameWords(query, olWordSet(olTitleWords(title))) {
		return true
	}
	if i := strings.IndexAny(title, ":("); i >= 0 {
		title = title[:i]
	}
	if i := strings.Index(title, " - "); i >= 0 {
		title = title[:i]
	}
	return olSameWords(query, olWordSet(olTitleWords(title)))
}

func coverNameTokens(name string) []string {
	return olWordRe.FindAllString(strings.ToLower(name), -1)
}

// coverAuthorMatches compares names token by token, so "Herbert, Frank" equals
// "Frank Herbert" and "J.R.R. Tolkien" equals "J. R. R. Tolkien". The shorter
// name must carry a real word, or the initial "R" would match every Tolkien.
func coverAuthorMatches(a, b string) bool {
	ta, tb := coverNameTokens(a), coverNameTokens(b)
	if len(ta) > len(tb) {
		ta, tb = tb, ta
	}
	long := false
	for _, t := range ta {
		if len([]rune(t)) >= 3 {
			long = true
		}
	}
	if !long {
		return false
	}
	set := olWordSet(tb)
	for _, t := range ta {
		if !set[t] {
			return false
		}
	}
	return true
}

// coverAuthorID reduces a name to its sorted tokens for exact comparison.
func coverAuthorID(name string) string {
	t := coverNameTokens(name)
	sort.Strings(t)
	return strings.Join(t, " ")
}

// coverDocMatches reports whether a record is the queried title, counting Open
// Library's separate subtitle as part of the title.
func coverDocMatches(query map[string]bool, d *olSearchDoc) bool {
	if coverTitleMatches(query, d.Title) {
		return true
	}
	return d.Subtitle != "" && olSameWords(query, olWordSet(olTitleWords(d.Title+" "+d.Subtitle)))
}

// coverMainTitle returns the words of a reading's title before its subtitle, or
// nil when there is no subtitle or the name mentions a volume, which the
// subtitle may be what names.
func coverMainTitle(r coverReading) map[string]bool {
	if r.Volume {
		return nil
	}
	loc := coverSubtitleRe.FindStringIndex(r.Title)
	if loc == nil {
		return nil
	}
	words := olTitleWords(r.Title[:loc[0]])
	if len(words) == 0 {
		return nil
	}
	return olWordSet(words)
}

// matchCoverDoc returns the record whose cover to show, or nil. Every reading is
// first tried against full titles, subtitles included. Only if none matches is
// a release's main title (before its subtitle) tried, and then only against
// records that have no subtitle of their own, so "Star Wars: Thrawn" never takes
// the cover of another "Star Wars" book.
func matchCoverDoc(readings []coverReading, docs []olSearchDoc) *olSearchDoc {
	for _, r := range readings {
		query := olWordSet(olTitleWords(r.Title))
		if d := pickCoverDoc(r, docs, func(d *olSearchDoc) bool { return coverDocMatches(query, d) }); d != nil {
			return d
		}
	}
	for _, r := range readings {
		main := coverMainTitle(r)
		if main == nil {
			continue
		}
		if d := pickCoverDoc(r, docs, func(d *olSearchDoc) bool {
			return d.Subtitle == "" && olSameWords(main, olWordSet(olTitleWords(d.Title)))
		}); d != nil {
			return d
		}
	}
	return nil
}

// pickCoverDoc returns the first record, in Open Library's order, whose title
// matches and that passes the author and box set rules and carries a cover.
func pickCoverDoc(r coverReading, docs []olSearchDoc, titleMatches func(*olSearchDoc) bool) *olSearchDoc {
	wantCollection := olCollectionRe.MatchString(r.Title)
	var candidates []*olSearchDoc
	for i := range docs {
		d := &docs[i]
		if !titleMatches(d) {
			continue
		}
		if olCollectionRe.MatchString(d.Title) && !wantCollection {
			continue
		}
		candidates = append(candidates, d)
	}

	if r.Author != "" {
		for _, d := range candidates {
			if d.CoverI <= 0 {
				continue
			}
			for _, a := range d.AuthorName {
				if coverAuthorMatches(r.Author, a) {
					return d
				}
			}
		}
		return nil
	}

	// Without an author, every same-titled record must agree on one, or
	// "Monster" could be Naoki Urasawa's or anybody else's.
	for _, d := range candidates {
		if len(d.AuthorName) == 0 || coverAuthorID(d.AuthorName[0]) == "" ||
			coverAuthorID(d.AuthorName[0]) != coverAuthorID(candidates[0].AuthorName[0]) {
			return nil
		}
	}
	for _, d := range candidates {
		if d.CoverI > 0 {
			return d
		}
	}
	return nil
}
