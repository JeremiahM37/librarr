package scheduler

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/JeremiahM37/librarr/internal/db"
)

// releaseFilter applies the enabled release profiles to a release title:
// every profile's must_not_contain terms reject, a profile with must_contain
// terms rejects a title containing none of them, and preferred words add
// their score. Matching is case-insensitive substring, as in Sonarr/Radarr.
type releaseFilter struct {
	profiles []db.ReleaseProfile
}

func newReleaseFilter(all []db.ReleaseProfile) releaseFilter {
	var f releaseFilter
	for _, p := range all {
		if p.Enabled {
			f.profiles = append(f.profiles, p)
		}
	}
	return f
}

// check reports whether title passes every enabled profile, the score bonus
// from preferred words, and, when rejected, why.
func (f releaseFilter) check(title string) (ok bool, bonus float64, reason string) {
	t := strings.ToLower(title)
	for _, p := range f.profiles {
		for _, term := range p.MustNotContain {
			if term = strings.TrimSpace(term); term != "" && strings.Contains(t, strings.ToLower(term)) {
				return false, 0, "release profile " + strconv.Quote(p.Name) + " rejects " + strconv.Quote(term)
			}
		}
		required, found := 0, false
		for _, term := range p.MustContain {
			if term = strings.TrimSpace(term); term == "" {
				continue
			}
			required++
			if strings.Contains(t, strings.ToLower(term)) {
				found = true
			}
		}
		if required > 0 && !found {
			return false, 0, "release profile " + strconv.Quote(p.Name) + " requires one of " + strings.Join(p.MustContain, ", ")
		}
		for _, w := range p.Preferred {
			if word := strings.TrimSpace(w.Word); word != "" && strings.Contains(t, strings.ToLower(word)) {
				bonus += float64(w.Score)
			}
		}
	}
	return true, bonus, ""
}

var (
	volNonWord = regexp.MustCompile(`[^\p{L}\p{N}]+`)
	volMarker  = map[string]bool{"vol": true, "volume": true, "book": true, "tome": true, "t": true, "part": true, "no": true}
)

// volumeTokens lower-cases a title and splits it into words.
func volumeTokens(s string) []string {
	return strings.Fields(volNonWord.ReplaceAllString(strings.ToLower(s), " "))
}

// wantedVolume splits a wanted title such as "Primal Hunter 5" or
// "Dune, Book 2" into its series words and volume number. ok is false when
// the title does not end in a number, or the number is the whole title.
func wantedVolume(title string) (base []string, vol int, ok bool) {
	toks := volumeTokens(title)
	if len(toks) < 2 {
		return nil, 0, false
	}
	n, err := strconv.Atoi(toks[len(toks)-1])
	if err != nil {
		return nil, 0, false
	}
	base = toks[:len(toks)-1]
	if len(base) > 1 && volMarker[base[len(base)-1]] {
		base = base[:len(base)-1]
	}
	return base, n, len(base) > 0
}

// wrongVolume reports whether a release is a different volume of the wanted
// series: the release names the series words followed by a number (optionally
// after "vol", "tome", "book", ...) and that number is not the wanted one.
// A release that does not number the series is left alone.
func wrongVolume(wanted, release string) bool {
	base, want, ok := wantedVolume(wanted)
	if !ok {
		return false
	}
	toks := volumeTokens(release)
	for i := 0; i+len(base) <= len(toks); i++ {
		match := true
		for j, b := range base {
			if toks[i+j] != b {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		k := i + len(base)
		if k < len(toks) && volMarker[toks[k]] {
			k++
		}
		if k < len(toks) {
			if n, err := strconv.Atoi(toks[k]); err == nil {
				return n != want
			}
		}
	}
	return false
}
