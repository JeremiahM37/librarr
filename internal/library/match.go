// Package library answers one question: does the user already own this book?
//
// It is used in two places that must agree — the wishlist cleaner (which
// deletes wishlist rows once the book lands) and search/download ownership
// detection (which flags and blocks duplicate grabs). Both compare a loose,
// human-entered title against the titles the importer wrote into
// library_items, so the normalization lives here rather than being reinvented
// per caller.
//
// Matching is deliberately conservative. A false positive tells a user they
// own a book they do not, and hides the download behind an extra click; that
// is a worse failure than missing a match, so every rule here only ever
// removes noise that cannot change which book is meant.
package library

import (
	"strings"
	"unicode"
)

// Candidate is the slim projection of a library row needed to answer
// ownership. Loading whole models.LibraryItem values (file paths, metadata
// blobs, hashes) for every one of a 3,000+ book library on each search would
// be pure waste.
type Candidate struct {
	ID        int64
	Title     string
	Author    string
	MediaType string
}

// Match is a resolved ownership hit.
type Match struct {
	ID    int64
	Title string
}

// NormalizeTitle folds a title to its comparable form: lowercase, punctuation
// collapsed to single spaces, edition noise removed.
//
// Collapsing punctuation is what makes "4:50 from Paddington" and
// "4.50 From Paddington" the same book — both become "4 50 from paddington".
func NormalizeTitle(s string) string {
	normalized := NormalizeWords(s)
	for _, suffix := range []string{" unabridged", " abridged"} {
		normalized = strings.TrimSuffix(normalized, suffix)
	}
	return strings.TrimSpace(normalized)
}

// NormalizePerson folds an author name to its comparable form.
func NormalizePerson(s string) string {
	return NormalizeWords(s)
}

// NormalizeWords lowercases, drops punctuation and symbols, and collapses
// whitespace runs to a single space.
func NormalizeWords(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastSpace := true
	for _, r := range s {
		switch {
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			b.WriteRune(r)
			lastSpace = false
		case unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r):
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

// AuthorMatches reports whether a normalized author agrees with a raw author
// field that may hold several names ("Neil Gaiman & Terry Pratchett").
// An empty wanted author agrees with anything; a known wanted author never
// agrees with a blank field.
func AuthorMatches(wantNormalized, raw string) bool {
	if wantNormalized == "" {
		return true
	}
	if strings.TrimSpace(raw) == "" {
		return false
	}
	if NormalizePerson(raw) == wantNormalized {
		return true
	}
	for _, part := range SplitAuthorList(raw) {
		if NormalizePerson(part) == wantNormalized {
			return true
		}
	}
	return false
}

// SplitAuthorList splits a multi-author field on the separators sources
// actually use.
func SplitAuthorList(author string) []string {
	replacer := strings.NewReplacer("&", ",", ";", ",", " and ", ",")
	return strings.Split(replacer.Replace(" "+strings.ToLower(author)+" "), ",")
}

// placeholderAuthors are the strings importers write when a file names no
// author. They are absence of information, not a person, and must not be
// compared as one.
var placeholderAuthors = map[string]struct{}{
	"unknown":         {},
	"unknown author":  {},
	"various":         {},
	"various authors": {},
	"anonymous":       {},
	"n a":             {}, // "n/a" after normalization
}

// KnownAuthor returns the normalized author name, or "" when the field carries
// no usable name.
func KnownAuthor(author string) string {
	normalized := NormalizePerson(author)
	if _, placeholder := placeholderAuthors[normalized]; placeholder {
		return ""
	}
	return normalized
}

// authorsAgree decides whether an author field blocks a title match.
//
// When either side names no author the titles decide alone. Imported
// collections routinely hold rows with an empty or "Unknown" author — an EPUB
// with no dc:creator, a Calibre export, a bare filename — and refusing to
// recognise those books would leave exactly the users this feature is for
// (people bringing in a big existing library) with no ownership detection at
// all. The cost of being wrong is a badge and one extra click, never a lost
// download.
func authorsAgree(wantNormalized, libraryAuthor string) bool {
	if wantNormalized == "" || KnownAuthor(libraryAuthor) == "" {
		return true
	}
	return AuthorMatches(wantNormalized, libraryAuthor)
}

// DefaultMediaType normalizes a possibly-empty media type. Search results from
// the ebook tab often carry no media type at all.
func DefaultMediaType(mediaType string) string {
	if trimmed := strings.TrimSpace(mediaType); trimmed != "" {
		return trimmed
	}
	return "ebook"
}

// Index is an ownership lookup built once from the library table and then
// queried per search result. Lookups are map hits, so annotating a full page
// of results costs nothing measurable.
//
// The zero value is a usable empty index that matches nothing, so callers with
// no database do not need a nil check.
type Index struct {
	// byTitle is keyed on normalized title + media type. Several editions of
	// one book share a key, which is fine: any of them proves ownership.
	byTitle map[string][]Candidate
	// authors holds every normalized author name in the library, used to
	// recognise an "Author - Title" prefix on a result title.
	authors map[string]struct{}
}

// NewIndex builds an ownership index from library rows.
func NewIndex(candidates []Candidate) *Index {
	idx := &Index{
		byTitle: make(map[string][]Candidate, len(candidates)),
		authors: make(map[string]struct{}),
	}
	for _, c := range candidates {
		title := NormalizeTitle(c.Title)
		if title == "" {
			continue
		}
		key := titleKey(title, DefaultMediaType(c.MediaType))
		idx.byTitle[key] = append(idx.byTitle[key], c)
		for _, part := range SplitAuthorList(c.Author) {
			if normalized := KnownAuthor(part); normalized != "" {
				idx.authors[normalized] = struct{}{}
			}
		}
	}
	return idx
}

// Len reports how many distinct title/media-type keys the index holds.
func (i *Index) Len() int {
	if i == nil {
		return 0
	}
	return len(i.byTitle)
}

// Lookup reports whether a search result names a book already in the library.
//
// A hit requires the normalized titles to be equal — after stripping the noise
// sources add around the title — and, when the result names an author, that
// author to agree with the library row. A result with no author at all matches
// on title alone, which is the best that can be done for sources that publish
// nothing else.
func (i *Index) Lookup(title, author, mediaType string) (Match, bool) {
	if i == nil || len(i.byTitle) == 0 {
		return Match{}, false
	}

	wantAuthor := KnownAuthor(author)
	mt := DefaultMediaType(mediaType)

	for _, variant := range i.titleVariants(title, wantAuthor) {
		for _, candidate := range i.byTitle[titleKey(variant, mt)] {
			if !authorsAgree(wantAuthor, candidate.Author) {
				continue
			}
			return Match{ID: candidate.ID, Title: candidate.Title}, true
		}
	}
	return Match{}, false
}

// titleVariants returns the normalized forms a result title could reasonably
// take, most faithful first. Sources wrap the actual title in author prefixes
// and parenthetical alternate titles, none of which appear in the library row
// the importer wrote.
func (i *Index) titleVariants(title string, wantAuthor string) []string {
	seen := make(map[string]struct{}, 4)
	var variants []string
	add := func(s string) {
		normalized := NormalizeTitle(s)
		if normalized == "" {
			return
		}
		if _, dup := seen[normalized]; dup {
			return
		}
		seen[normalized] = struct{}{}
		variants = append(variants, normalized)
	}

	add(title)

	// "4.50 from Paddington (aka What Mrs. McGillicuddy Saw)" -> the title
	// before the alternate-title aside.
	trimmed := stripTrailingAside(title)
	add(trimmed)

	// "Agatha Christie - 4.50 From Paddington" -> drop the author prefix. Only
	// done when the prefix is genuinely an author name: either the one the
	// result itself reports, or one the library already knows. Otherwise
	// "Dune - Book One" would lose half its title.
	for _, base := range []string{title, trimmed} {
		if stripped, ok := i.stripAuthorAffix(base, wantAuthor); ok {
			add(stripped)
			add(stripTrailingAside(stripped))
		}
	}

	return variants
}

// stripAuthorAffix removes a leading "Author - " or trailing " - Author"
// segment when that segment is recognisably an author name.
func (i *Index) stripAuthorAffix(title, wantAuthor string) (string, bool) {
	for _, sep := range []string{" - ", " – ", " — ", " _ "} {
		head, tail, found := strings.Cut(title, sep)
		if !found {
			continue
		}
		if strings.TrimSpace(tail) != "" && i.isKnownAuthor(head, wantAuthor) {
			return tail, true
		}
		// Trailing form: take the last segment as the candidate author.
		if idx := strings.LastIndex(title, sep); idx > 0 {
			lead, trail := title[:idx], title[idx+len(sep):]
			if strings.TrimSpace(lead) != "" && i.isKnownAuthor(trail, wantAuthor) {
				return lead, true
			}
		}
	}
	return "", false
}

func (i *Index) isKnownAuthor(segment, wantAuthor string) bool {
	normalized := NormalizePerson(segment)
	if normalized == "" {
		return false
	}
	if wantAuthor != "" && normalized == wantAuthor {
		return true
	}
	_, known := i.authors[normalized]
	return known
}

// stripTrailingAside removes a trailing parenthesised or bracketed segment,
// which sources use for alternate titles, formats and series notes.
func stripTrailingAside(title string) string {
	trimmed := strings.TrimSpace(title)
	for _, pair := range [][2]string{{"(", ")"}, {"[", "]"}, {"{", "}"}} {
		if !strings.HasSuffix(trimmed, pair[1]) {
			continue
		}
		if open := strings.LastIndex(trimmed, pair[0]); open > 0 {
			candidate := strings.TrimSpace(trimmed[:open])
			if candidate != "" {
				return candidate
			}
		}
	}
	return trimmed
}

func titleKey(normalizedTitle, mediaType string) string {
	return normalizedTitle + "\x00" + mediaType
}
