package metadata

import (
	"math"
	"regexp"
	"strings"
)

// Open Library's relevance ranking regularly places an omnibus above the volume
// that was actually searched for — `title=Catching Fire` returns "The Hunger
// Games Trilogy (Hunger Games / Catching Fire / Mockingjay)" ahead of "Catching
// Fire". Taking docs[0] therefore captions a single-volume search with a box
// set's cover, year and page count. pickBestDoc re-ranks the candidates so the
// volume wins, while leaving Open Library's own ordering as the tiebreak.

var (
	olWordRe = regexp.MustCompile(`\p{L}+|\d+`)

	// Titles that describe a bundle rather than a single volume. The book-range
	// alternative catches "Books I-III" and "Books 1 - 3" style spines.
	olCollectionRe = regexp.MustCompile(`(?i)\b(box(ed)?[ -]?sets?|boxsets?|omnibus|collections?|complete series|complete works|anthology|trilogy|quartet|quintet|tetralogy|bind[ -]?up|\d+\s+books?\b|books?\s+[ivxlcdm\d]+\s*[-–—/]\s*[ivxlcdm\d]+)`)
)

var olStopwords = map[string]bool{
	"the": true, "a": true, "an": true, "of": true, "and": true, "in": true,
	"on": true, "to": true, "for": true, "or": true, "by": true,
}

// olNumerals folds spelled-out and Roman volume numbers onto digits so a
// "Book Three" record matches a "book 3" query. Roman numerals shorter than two
// characters are deliberately absent: mapping a bare "I" would rewrite titles
// like "I Am Legend".
var olNumerals = map[string]string{
	"one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
	"six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
	"eleven": "11", "twelve": "12",
	"ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7",
	"viii": "8", "ix": "9", "x": "10", "xi": "11", "xii": "12",
}

// olTitleWords reduces a title to comparable tokens: lowercased, punctuation
// dropped, stopwords removed, volume numbers normalised to digits.
func olTitleWords(title string) []string {
	var words []string
	for _, w := range olWordRe.FindAllString(strings.ToLower(title), -1) {
		if olStopwords[w] {
			continue
		}
		if n, ok := olNumerals[w]; ok {
			w = n
		}
		words = append(words, w)
	}
	return words
}

func olWordSet(words []string) map[string]bool {
	set := make(map[string]bool, len(words))
	for _, w := range words {
		set[w] = true
	}
	return set
}

func olSameWords(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for w := range a {
		if !b[w] {
			return false
		}
	}
	return true
}

// olAuthorMatches reports whether any of a record's authors looks like the one
// the caller asked for. Open Library lists adaptors and illustrators alongside
// the author, so a hit on any entry counts.
func olAuthorMatches(names []string, author string) bool {
	want := strings.ToLower(strings.TrimSpace(author))
	if want == "" {
		return false
	}
	for _, n := range names {
		got := strings.ToLower(strings.TrimSpace(n))
		if got == "" {
			continue
		}
		if strings.Contains(got, want) || strings.Contains(want, got) {
			return true
		}
	}
	return false
}

// scoreOLDoc rates one candidate against the query. The weights encode a simple
// preference order: cover every word of the query, add nothing the query didn't
// ask for, and don't be a box set.
func scoreOLDoc(query string, queryWords []string, querySet map[string]bool, author string, doc olSearchDoc, index int) float64 {
	docWords := olTitleWords(doc.Title)
	if len(docWords) == 0 {
		return math.Inf(-1)
	}
	docSet := olWordSet(docWords)

	hits := 0
	for w := range querySet {
		if docSet[w] {
			hits++
		}
	}
	score := 60 * float64(hits) / float64(len(querySet))

	// Same word set means the titles differ only in punctuation or articles.
	if olSameWords(querySet, docSet) {
		score += 40
	}

	// Every word the query didn't ask for dilutes the match — an omnibus title
	// names all of its volumes, so this alone separates most of them out.
	extra := 0
	for w := range docSet {
		if !querySet[w] {
			extra++
		}
	}
	score -= math.Min(float64(extra), 12) * 3

	// ...and an explicit bundle marker settles the rest, unless the caller was
	// actually looking for the bundle.
	if olCollectionRe.MatchString(doc.Title) && !olCollectionRe.MatchString(query) {
		score -= 50
	}

	if olAuthorMatches(doc.AuthorName, author) {
		score += 20
	}

	// Preserve Open Library's ranking between otherwise equal candidates.
	return score - 0.5*float64(index)
}

// pickBestDoc chooses the record that best matches the queried title, falling
// back to Open Library's first result when the query carries no usable words.
func pickBestDoc(query, author string, docs []olSearchDoc) *olSearchDoc {
	if len(docs) == 0 {
		return nil
	}
	queryWords := olTitleWords(query)
	if len(queryWords) == 0 {
		return &docs[0]
	}
	querySet := olWordSet(queryWords)

	bestIdx, bestScore := 0, math.Inf(-1)
	for i := range docs {
		if s := scoreOLDoc(query, queryWords, querySet, author, docs[i], i); s > bestScore {
			bestIdx, bestScore = i, s
		}
	}
	return &docs[bestIdx]
}
