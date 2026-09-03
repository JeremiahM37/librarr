package quality

import (
	"strings"
	"testing"
)

func ebookProfile() Profile {
	return Profile{
		Name:            "Test Ebook",
		MediaType:       "ebook",
		Ranking:         []string{"epub", "azw3", "mobi", "pdf"},
		Cutoff:          "epub",
		UpgradesAllowed: true,
	}
}

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"EPUB": "epub", ".epub": "epub", " Pdf ": "pdf", "": "", ".": "", "M4B": "m4b",
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatFromPathAndTitle(t *testing.T) {
	if got := FormatFromPath("/lib/A/B.EPUB"); got != "epub" {
		t.Errorf("FormatFromPath = %q", got)
	}
	if got := FormatFromPath("/lib/noext"); got != "" {
		t.Errorf("FormatFromPath(noext) = %q", got)
	}
	cases := map[string]string{
		"Dune - Frank Herbert [EPUB]":    "epub",
		"Dune (2021) mobi retail":        "mobi",
		"Dune audiobook M4B 64kbps":      "m4b",
		"Dune":                           "",
		"Superpdfbook":                   "", // token must stand alone
		"The Complete Works v1.2 azw3":   "azw3",
		"Some.Release.Name.PDF-GROUP":    "pdf",
		"Some.Release.Name.AZW3.vs.epub": "azw3", // first standalone token wins
	}
	for title, want := range cases {
		if got := FormatFromTitle(title); got != want {
			t.Errorf("FormatFromTitle(%q) = %q, want %q", title, got, want)
		}
	}
}

func TestValidate(t *testing.T) {
	ok := ebookProfile()
	if err := Validate(ok); err != nil {
		t.Fatalf("valid profile rejected: %v", err)
	}
	tests := []struct {
		name string
		mut  func(*Profile)
		want string
	}{
		{"empty name", func(p *Profile) { p.Name = "  " }, "name is required"},
		{"empty ranking", func(p *Profile) { p.Ranking = nil }, "at least one format"},
		{"blank format", func(p *Profile) { p.Ranking = []string{"epub", " "} }, "empty format"},
		{"duplicate (case-insensitive)", func(p *Profile) { p.Ranking = []string{"epub", "EPUB"} }, "appears twice"},
		{"cutoff outside ranking", func(p *Profile) { p.Cutoff = "cbz" }, "not in the format ranking"},
		{"negative size", func(p *Profile) { p.PreferredSizeMin = -1 }, "negative"},
		{"inverted sizes", func(p *Profile) { p.PreferredSizeMin = 10; p.PreferredSizeMax = 5 }, "exceeds maximum"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := ebookProfile()
			tc.mut(&p)
			err := Validate(p)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
	// An empty cutoff is valid and resolves to the best format.
	p := ebookProfile()
	p.Cutoff = ""
	if err := Validate(p); err != nil {
		t.Fatalf("empty cutoff should be valid: %v", err)
	}
	if p.CutoffFormat() != "epub" || p.CutoffRank() != 0 {
		t.Fatalf("empty cutoff should resolve to best: %q/%d", p.CutoffFormat(), p.CutoffRank())
	}
}

func TestRankAndCutoff(t *testing.T) {
	p := ebookProfile()
	p.Cutoff = "AZW3" // case-insensitive
	if r, ok := p.Rank(".MOBI"); !ok || r != 2 {
		t.Fatalf("Rank(.MOBI) = %d,%v", r, ok)
	}
	if _, ok := p.Rank("cbz"); ok {
		t.Fatal("cbz should not be ranked")
	}
	if _, ok := p.Rank(""); ok {
		t.Fatal("empty format should not be ranked")
	}
	if p.CutoffRank() != 1 || p.CutoffFormat() != "azw3" {
		t.Fatalf("cutoff = %d/%q", p.CutoffRank(), p.CutoffFormat())
	}
	for f, want := range map[string]bool{"epub": true, "azw3": true, "mobi": false, "pdf": false, "cbz": false, "": false} {
		if got := p.CutoffMet(f); got != want {
			t.Errorf("CutoffMet(%q) = %v, want %v", f, got, want)
		}
	}
}

// TestEvaluate is the contract the scheduler relies on. Each row is one
// (candidate, current file) pair; the expected accept/upgrade flags are the
// *arr semantics: grab when missing, upgrade strictly upward until the cutoff
// is reached, never grab a format the profile does not list.
func TestEvaluate(t *testing.T) {
	type row struct {
		name      string
		profile   func() Profile
		candidate string
		current   string
		accept    bool
		upgrade   bool
		reason    string
	}
	withCutoff := func(c string) func() Profile {
		return func() Profile { p := ebookProfile(); p.Cutoff = c; return p }
	}
	noUpgrades := func() Profile { p := ebookProfile(); p.UpgradesAllowed = false; return p }

	rows := []row{
		{"missing, best format", ebookProfile, "epub", "", true, false, "no file yet"},
		{"missing, worst allowed format", ebookProfile, "pdf", "", true, false, "no file yet"},
		{"missing, format not in profile", ebookProfile, "cbz", "", false, false, "not allowed"},
		{"missing, unknown format", ebookProfile, "", "", false, false, "no format detected"},
		{"case-insensitive candidate", ebookProfile, ".EPUB", "", true, false, "no file yet"},

		{"have pdf, epub offered (cutoff epub)", ebookProfile, "epub", "pdf", true, true, "upgrade PDF → EPUB"},
		{"have pdf, mobi offered (cutoff epub)", ebookProfile, "mobi", "pdf", true, true, "upgrade PDF → MOBI"},
		{"have mobi, pdf offered", ebookProfile, "pdf", "mobi", false, false, "not an upgrade"},
		{"have mobi, mobi offered", ebookProfile, "mobi", "mobi", false, false, "not an upgrade"},
		{"have epub, azw3 offered (cutoff met)", ebookProfile, "azw3", "epub", false, false, "cutoff met"},
		{"have epub, epub offered (cutoff met)", ebookProfile, "epub", "epub", false, false, "cutoff met"},

		// Cutoff below the top: once azw3 is on disk, stop even though epub is better.
		{"cutoff azw3: have azw3, epub offered", withCutoff("azw3"), "epub", "azw3", false, false, "cutoff met"},
		{"cutoff azw3: have mobi, epub offered", withCutoff("azw3"), "epub", "mobi", true, true, "upgrade MOBI → EPUB"},
		{"cutoff azw3: have mobi, azw3 offered", withCutoff("azw3"), "azw3", "mobi", true, true, "upgrade MOBI → AZW3"},
		{"cutoff pdf (lowest): have pdf, epub offered", withCutoff("pdf"), "epub", "pdf", false, false, "cutoff met"},

		// Existing file in a format the profile does not rank: any allowed release upgrades it.
		{"have unranked cbz, pdf offered", ebookProfile, "pdf", "cbz", true, true, "upgrade CBZ → PDF"},
		{"have unranked cbz, cbz offered", ebookProfile, "cbz", "cbz", false, false, "not allowed"},

		// Upgrades disabled: nothing replaces an existing file, no matter how bad it is.
		{"no upgrades: have pdf, epub offered", noUpgrades, "epub", "pdf", false, false, "does not upgrade"},
		{"no upgrades: missing still grabs", noUpgrades, "epub", "", true, false, "no file yet"},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			d := r.profile().Evaluate(r.candidate, r.current)
			if d.Accept != r.accept || d.Upgrade != r.upgrade {
				t.Fatalf("Evaluate(%q, %q) = accept=%v upgrade=%v (%s); want accept=%v upgrade=%v",
					r.candidate, r.current, d.Accept, d.Upgrade, d.Reason, r.accept, r.upgrade)
			}
			if !strings.Contains(d.Reason, r.reason) {
				t.Fatalf("reason %q does not contain %q", d.Reason, r.reason)
			}
			if d.Accept && d.Rank < 0 {
				t.Fatalf("accepted decision must carry a rank, got %d", d.Rank)
			}
		})
	}
}

func TestEvaluate_UpgradeIsNeverAcceptedWithoutBeingAnUpgrade(t *testing.T) {
	// Property check across the whole ranking: for every (have, offered) pair
	// an accepted upgrade must have a strictly better rank, and a met cutoff
	// must never be upgraded. This guards the ordering logic against off-by-one
	// edits better than a handful of hand-picked rows.
	p := ebookProfile()
	for cutoff := range p.Ranking {
		p.Cutoff = p.Ranking[cutoff]
		for have := range p.Ranking {
			for offered := range p.Ranking {
				d := p.Evaluate(p.Ranking[offered], p.Ranking[have])
				wantAccept := have > cutoff && offered < have
				if d.Accept != wantAccept {
					t.Errorf("cutoff=%s have=%s offered=%s: accept=%v want %v (%s)",
						p.Ranking[cutoff], p.Ranking[have], p.Ranking[offered], d.Accept, wantAccept, d.Reason)
				}
				if d.Accept && !d.Upgrade {
					t.Errorf("accepted replacement must be flagged as upgrade")
				}
			}
		}
	}
}

func TestChoose(t *testing.T) {
	p := ebookProfile()
	cands := []Candidate{
		{Index: 0, Format: "pdf", Score: 95},
		{Index: 1, Format: "mobi", Score: 80},
		{Index: 2, Format: "epub", Score: 75},
		{Index: 3, Format: "epub", Score: 90},
		{Index: 4, Format: "cbz", Score: 99}, // not allowed
		{Index: 5, Format: "", Score: 99},    // unknown
	}
	best, all := p.Choose(cands, "")
	if best == nil {
		t.Fatal("expected a choice")
	}
	if best.Candidate.Index != 3 {
		t.Fatalf("expected best epub by score (index 3), got index %d", best.Candidate.Index)
	}
	if len(all) != len(cands) {
		t.Fatalf("expected a decision per candidate, got %d", len(all))
	}
	accepted := 0
	for _, d := range all {
		if d.Decision.Accept {
			accepted++
		}
	}
	if accepted != 4 {
		t.Fatalf("expected 4 acceptable candidates (pdf, mobi, epub, epub), got %d", accepted)
	}

	// With an epub on disk, nothing qualifies.
	if b, _ := p.Choose(cands, "epub"); b != nil {
		t.Fatalf("cutoff met but Choose returned %+v", b.Candidate)
	}
	// With a pdf on disk, the epub upgrade wins over the higher-scoring mobi.
	b, _ := p.Choose(cands, "pdf")
	if b == nil || b.Candidate.Index != 3 || !b.Decision.Upgrade {
		t.Fatalf("expected epub upgrade (index 3), got %+v", b)
	}
	// Quality rank beats match score: a 75-score epub outranks a 95-score pdf.
	b, _ = p.Choose([]Candidate{{Index: 0, Format: "pdf", Score: 95}, {Index: 1, Format: "epub", Score: 75}}, "")
	if b == nil || b.Candidate.Index != 1 {
		t.Fatalf("rank should beat score, got %+v", b)
	}
	// Empty input.
	if b, all := p.Choose(nil, ""); b != nil || len(all) != 0 {
		t.Fatal("empty candidates should yield no choice")
	}
}

func TestChoose_SizePreferenceTieBreak(t *testing.T) {
	p := ebookProfile()
	p.PreferredSizeMin = 1 << 20  // 1 MiB
	p.PreferredSizeMax = 20 << 20 // 20 MiB
	cands := []Candidate{
		{Index: 0, Format: "epub", Score: 99, Size: 200 << 20}, // too big
		{Index: 1, Format: "epub", Score: 50, Size: 5 << 20},   // in range
		{Index: 2, Format: "epub", Score: 60, Size: 0},         // unknown size
	}
	b, _ := p.Choose(cands, "")
	if b == nil || b.Candidate.Index != 1 {
		t.Fatalf("in-range size should win the tie, got %+v", b)
	}
	// Without bounds, score decides and unknown size is not penalised.
	p.PreferredSizeMin, p.PreferredSizeMax = 0, 0
	b, _ = p.Choose(cands, "")
	if b == nil || b.Candidate.Index != 0 {
		t.Fatalf("without size bounds the top score should win, got %+v", b)
	}
	// Deterministic on full ties: earlier index wins.
	b, _ = p.Choose([]Candidate{{Index: 7, Format: "epub", Score: 1}, {Index: 8, Format: "epub", Score: 1}}, "")
	if b == nil || b.Candidate.Index != 7 {
		t.Fatalf("full tie should keep input order, got %+v", b)
	}
}

func TestState(t *testing.T) {
	p := ebookProfile()
	cases := []struct {
		name                            string
		monitored, downloading, hasFile bool
		format                          string
		upgradesEnabled                 bool
		want                            string
	}{
		{"unmonitored wins", false, true, true, "pdf", true, StateUnmonitored},
		{"downloading", true, true, false, "", true, StateDownloading},
		{"missing", true, false, false, "", true, StateMissing},
		{"pdf below cutoff", true, false, true, "pdf", true, StateUpgrade},
		{"pdf but upgrades globally off", true, false, true, "pdf", false, StateSatisfied},
		{"epub meets cutoff", true, false, true, "epub", true, StateSatisfied},
		{"unranked format, upgrades on", true, false, true, "cbz", true, StateUpgrade},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := State(p, c.monitored, c.downloading, c.hasFile, c.format, c.upgradesEnabled); got != c.want {
				t.Fatalf("State = %q, want %q", got, c.want)
			}
		})
	}
	np := p
	np.UpgradesAllowed = false
	if got := State(np, true, false, true, "pdf", true); got != StateSatisfied {
		t.Fatalf("profile with upgrades off should be satisfied by any file, got %q", got)
	}
}

func TestDefaultProfilesAreValid(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range DefaultProfiles() {
		if err := Validate(p); err != nil {
			t.Errorf("default profile %q invalid: %v", p.Name, err)
		}
		if seen[p.MediaType] {
			t.Errorf("two defaults for media type %q", p.MediaType)
		}
		seen[p.MediaType] = true
		known := KnownFormats[p.MediaType]
		for _, f := range p.Ranking {
			found := false
			for _, k := range known {
				if k == f {
					found = true
				}
			}
			if !found {
				t.Errorf("default %q ranks %q which is not in KnownFormats[%s]", p.Name, f, p.MediaType)
			}
		}
	}
	for _, mt := range []string{"ebook", "audiobook", "manga"} {
		if !seen[mt] {
			t.Errorf("no default profile for %s", mt)
		}
	}
}
