// Package quality implements quality profiles: an ordered ranking of file
// formats, a cutoff, and the accept/upgrade decision that turns "a release
// exists" into "this release is worth grabbing".
//
// The semantics mirror the rest of the *arr family. A profile lists the
// formats it is willing to grab, best first. A release whose format is not in
// the list is never grabbed automatically. When a wanted item already has a
// file, a release is grabbed only if it is a strictly better format AND the
// current file has not yet reached the cutoff — once the cutoff is met, the
// item is satisfied and upgrades stop, even if an even better format appears.
//
// Everything in this package is pure: no database, no HTTP, no clock. The
// scheduler and API layers feed it strings and act on the Decision.
package quality

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Profile is a quality profile as the decision engine sees it.
type Profile struct {
	ID        int64
	Name      string
	MediaType string
	// Ranking lists the grabbable formats, best first. Formats absent from the
	// ranking are not grabbable under this profile.
	Ranking []string
	// Cutoff is the format at which upgrading stops. Empty or unknown means
	// the best format in the ranking.
	Cutoff string
	// UpgradesAllowed enables replacing an existing file with a better format.
	UpgradesAllowed bool
	// PreferredSizeMin/Max (bytes) are a soft tie-break between releases of
	// the same format. Zero means no preference on that bound.
	PreferredSizeMin int64
	PreferredSizeMax int64
}

// Decision is the outcome of evaluating one candidate release.
type Decision struct {
	// Accept reports whether the candidate should be grabbed.
	Accept bool
	// Upgrade is set when Accept is true because the candidate would replace
	// an existing, lower-ranked file.
	Upgrade bool
	// Rank is the candidate's position in the profile ranking (0 = best), or
	// -1 when the format is not in the profile.
	Rank int
	// Reason is a short human-readable explanation, suitable for logs and for
	// the "last result" line on a wanted item.
	Reason string
}

// Candidate is one release under consideration by Choose.
type Candidate struct {
	// Index lets the caller map the choice back to its own slice.
	Index  int
	Format string
	// Score is the search match confidence (0-100). It is a gate applied by
	// the caller, and a late tie-break here; quality rank comes first.
	Score float64
	Size  int64
}

// CandidateDecision pairs a candidate with its decision, for dry runs and logs.
type CandidateDecision struct {
	Candidate Candidate
	Decision  Decision
}

// KnownFormats lists the formats the UI offers per media type. The decision
// engine itself accepts any format string; this list exists so a profile
// editor can render checkboxes and so validation can reject typos.
var KnownFormats = map[string][]string{
	"ebook":     {"epub", "azw3", "azw", "kfx", "mobi", "pdf", "fb2", "djvu", "lit", "lrf", "rtf", "txt", "doc", "docx", "html"},
	"audiobook": {"m4b", "m4a", "mp3", "flac", "opus", "ogg", "aac", "wma"},
	"manga":     {"cbz", "cbr", "epub", "pdf", "zip", "rar"},
}

// DefaultProfiles are the built-in profiles seeded on first start, one per
// media type. They are editable but not deletable, and are what a wanted
// item uses when no profile has been chosen for it.
func DefaultProfiles() []Profile {
	return []Profile{
		{
			Name:            "Default Ebook",
			MediaType:       "ebook",
			Ranking:         []string{"epub", "azw3", "mobi", "pdf"},
			Cutoff:          "epub",
			UpgradesAllowed: true,
		},
		{
			Name:            "Default Audiobook",
			MediaType:       "audiobook",
			Ranking:         []string{"m4b", "mp3"},
			Cutoff:          "m4b",
			UpgradesAllowed: true,
		},
		{
			Name:            "Default Manga",
			MediaType:       "manga",
			Ranking:         []string{"cbz", "cbr", "pdf"},
			Cutoff:          "cbz",
			UpgradesAllowed: true,
		},
	}
}

// Normalize canonicalises a format string: lower-case, no leading dot, no
// surrounding whitespace. "EPUB", ".epub" and " epub " all become "epub".
func Normalize(format string) string {
	f := strings.ToLower(strings.TrimSpace(format))
	f = strings.TrimPrefix(f, ".")
	return f
}

// FormatFromPath derives a format from a file name or path by extension.
func FormatFromPath(path string) string {
	return Normalize(filepath.Ext(path))
}

var formatTokenRe = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])(epub|mobi|azw3|azw|kfx|pdf|fb2|djvu|lit|lrf|cbz|cbr|m4b|m4a|mp3|flac|opus|ogg|aac)(?:$|[^a-z0-9])`)

// FormatFromTitle finds a standalone format token in a release title, such as
// "Dune - Frank Herbert [EPUB]". It returns "" when no token is present.
func FormatFromTitle(title string) string {
	m := formatTokenRe.FindStringSubmatch(title)
	if len(m) > 1 {
		return strings.ToLower(m[1])
	}
	return ""
}

// Validate reports why a profile is unusable. A usable profile has a name, a
// non-empty ranking with no duplicates, and a cutoff that is either empty or
// one of the ranked formats.
func Validate(p Profile) error {
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if len(p.Ranking) == 0 {
		return fmt.Errorf("format ranking must list at least one format")
	}
	seen := make(map[string]struct{}, len(p.Ranking))
	for _, raw := range p.Ranking {
		f := Normalize(raw)
		if f == "" {
			return fmt.Errorf("format ranking contains an empty format")
		}
		if _, dup := seen[f]; dup {
			return fmt.Errorf("format %q appears twice in the ranking", f)
		}
		seen[f] = struct{}{}
	}
	if c := Normalize(p.Cutoff); c != "" {
		if _, ok := seen[c]; !ok {
			return fmt.Errorf("cutoff %q is not in the format ranking", c)
		}
	}
	if p.PreferredSizeMin < 0 || p.PreferredSizeMax < 0 {
		return fmt.Errorf("preferred size bounds cannot be negative")
	}
	if p.PreferredSizeMin > 0 && p.PreferredSizeMax > 0 && p.PreferredSizeMin > p.PreferredSizeMax {
		return fmt.Errorf("preferred size minimum exceeds maximum")
	}
	return nil
}

// Rank returns the position of format in the profile ranking (0 = best) and
// whether the format is grabbable under this profile at all.
func (p Profile) Rank(format string) (int, bool) {
	f := Normalize(format)
	if f == "" {
		return -1, false
	}
	for i, r := range p.Ranking {
		if Normalize(r) == f {
			return i, true
		}
	}
	return -1, false
}

// CutoffRank is the rank an existing file must reach for the item to count as
// satisfied. An empty or unknown cutoff means the best ranked format.
func (p Profile) CutoffRank() int {
	if r, ok := p.Rank(p.Cutoff); ok {
		return r
	}
	return 0
}

// CutoffFormat is the effective cutoff after resolving an empty or unknown
// value to the best format. It is "" only for an empty ranking.
func (p Profile) CutoffFormat() string {
	if len(p.Ranking) == 0 {
		return ""
	}
	return Normalize(p.Ranking[p.CutoffRank()])
}

// CutoffMet reports whether a file of the given format satisfies the profile.
// A format outside the ranking never meets the cutoff.
func (p Profile) CutoffMet(currentFormat string) bool {
	r, ok := p.Rank(currentFormat)
	return ok && r <= p.CutoffRank()
}

// Evaluate decides whether a candidate of format candidateFormat should be
// grabbed for an item whose current file (if any) has currentFormat. Pass ""
// as currentFormat when the item has no file yet.
func (p Profile) Evaluate(candidateFormat, currentFormat string) Decision {
	cand := Normalize(candidateFormat)
	if cand == "" {
		return Decision{Rank: -1, Reason: "no format detected on the release"}
	}
	rank, ok := p.Rank(cand)
	if !ok {
		return Decision{Rank: -1, Reason: fmt.Sprintf("%s is not allowed by profile %q", strings.ToUpper(cand), p.Name)}
	}

	cur := Normalize(currentFormat)
	if cur == "" {
		return Decision{Accept: true, Rank: rank, Reason: fmt.Sprintf("no file yet; %s accepted", strings.ToUpper(cand))}
	}

	if !p.UpgradesAllowed {
		return Decision{Rank: rank, Reason: fmt.Sprintf("already have %s and profile %q does not upgrade", strings.ToUpper(cur), p.Name)}
	}

	curRank, curKnown := p.Rank(cur)
	if curKnown && curRank <= p.CutoffRank() {
		return Decision{Rank: rank, Reason: fmt.Sprintf("cutoff met: %s satisfies profile %q", strings.ToUpper(cur), p.Name)}
	}
	if !curKnown {
		// A file in a format the profile does not rank is treated as worse
		// than anything the profile does rank, so any allowed release upgrades it.
		curRank = len(p.Ranking)
	}
	if rank < curRank {
		return Decision{Accept: true, Upgrade: true, Rank: rank, Reason: fmt.Sprintf("upgrade %s → %s", strings.ToUpper(cur), strings.ToUpper(cand))}
	}
	return Decision{Rank: rank, Reason: fmt.Sprintf("%s is not an upgrade over %s", strings.ToUpper(cand), strings.ToUpper(cur))}
}

// Choose evaluates every candidate against the profile and picks the best
// acceptable one. Quality rank wins; among equal ranks a size inside the
// preferred window beats one outside it, then the higher match score, then
// the earlier candidate. The second return value carries every decision so
// callers can explain a dry run or log why nothing was chosen.
func (p Profile) Choose(candidates []Candidate, currentFormat string) (*CandidateDecision, []CandidateDecision) {
	decisions := make([]CandidateDecision, 0, len(candidates))
	var accepted []CandidateDecision
	for _, c := range candidates {
		d := p.Evaluate(c.Format, currentFormat)
		cd := CandidateDecision{Candidate: c, Decision: d}
		decisions = append(decisions, cd)
		if d.Accept {
			accepted = append(accepted, cd)
		}
	}
	if len(accepted) == 0 {
		return nil, decisions
	}
	sort.SliceStable(accepted, func(i, j int) bool {
		a, b := accepted[i], accepted[j]
		if a.Decision.Rank != b.Decision.Rank {
			return a.Decision.Rank < b.Decision.Rank
		}
		ai, bi := p.sizePreferred(a.Candidate.Size), p.sizePreferred(b.Candidate.Size)
		if ai != bi {
			return ai
		}
		if a.Candidate.Score != b.Candidate.Score {
			return a.Candidate.Score > b.Candidate.Score
		}
		return a.Candidate.Index < b.Candidate.Index
	})
	best := accepted[0]
	return &best, decisions
}

// sizePreferred reports whether size falls inside the profile's preferred
// window. With no bounds set every size is preferred; an unknown (zero) size
// is never preferred over a known in-range one.
func (p Profile) sizePreferred(size int64) bool {
	if p.PreferredSizeMin == 0 && p.PreferredSizeMax == 0 {
		return true
	}
	if size <= 0 {
		return false
	}
	if p.PreferredSizeMin > 0 && size < p.PreferredSizeMin {
		return false
	}
	if p.PreferredSizeMax > 0 && size > p.PreferredSizeMax {
		return false
	}
	return true
}

// Item states, as shown on the wanted list. They are derived, never stored:
// the stored facts are "monitored", "which file satisfies it" and "is a grab
// in flight", and the profile turns those into a state.
const (
	StateUnmonitored = "unmonitored"
	StateDownloading = "downloading"
	StateMissing     = "missing"
	StateUpgrade     = "upgrade"   // has a file, cutoff not met, upgrades on
	StateSatisfied   = "satisfied" // has a file that meets the cutoff (or upgrades are off)
)

// State derives the wanted-list state of an item.
func State(p Profile, monitored, downloading, hasFile bool, currentFormat string, upgradesEnabled bool) string {
	switch {
	case !monitored:
		return StateUnmonitored
	case downloading:
		return StateDownloading
	case !hasFile:
		return StateMissing
	case !upgradesEnabled || !p.UpgradesAllowed || p.CutoffMet(currentFormat):
		return StateSatisfied
	default:
		return StateUpgrade
	}
}
