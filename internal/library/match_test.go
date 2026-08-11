package library

import "testing"

// The library standing in for a real user's imported collection. The Agatha
// Christie row is the one from issue #96, imported from Kavita with a colon in
// the title that the download sources write as a full stop.
func testIndex() *Index {
	return NewIndex([]Candidate{
		{ID: 1, Title: "4:50 from Paddington", Author: "Agatha Christie", MediaType: "ebook"},
		{ID: 2, Title: "The Hobbit", Author: "J. R. R. Tolkien", MediaType: "ebook"},
		{ID: 3, Title: "Good Omens", Author: "Neil Gaiman & Terry Pratchett", MediaType: "ebook"},
		{ID: 4, Title: "Project Hail Mary", Author: "Andy Weir", MediaType: "audiobook"},
		{ID: 5, Title: "Dune", Author: "", MediaType: "ebook"},
	})
}

func TestLookupMatchesIssue96Titles(t *testing.T) {
	idx := testIndex()

	tests := []struct {
		name      string
		title     string
		author    string
		mediaType string
		wantID    int64
	}{
		{
			name:   "exact title",
			title:  "4:50 from Paddington",
			author: "Agatha Christie",
			wantID: 1,
		},
		{
			name:   "punctuation differs between colon and full stop",
			title:  "4.50 From Paddington",
			author: "Agatha Christie",
			wantID: 1,
		},
		{
			name:   "source prefixes the title with the author",
			title:  "Agatha Christie - 4.50 From Paddington",
			author: "Agatha Christie",
			wantID: 1,
		},
		{
			name:   "alternate title in parentheses",
			title:  "4.50 from Paddington (aka What Mrs. Mcgillicuddy Saw)",
			author: "Agatha Christie",
			wantID: 1,
		},
		{
			name:   "author prefix and parenthetical aside together",
			title:  "Agatha Christie - 4.50 from Paddington (Miss Marple #8)",
			author: "Agatha Christie",
			wantID: 1,
		},
		{
			name:   "author suffix after the title",
			title:  "4.50 from Paddington - Agatha Christie",
			author: "Agatha Christie",
			wantID: 1,
		},
		{
			name:   "author prefix recognised from the library when the result reports none",
			title:  "Agatha Christie - 4.50 From Paddington",
			author: "",
			wantID: 1,
		},
		{
			name:   "case and spacing are irrelevant",
			title:  "  the   HOBBIT  ",
			author: "j. r. r. tolkien",
			wantID: 2,
		},
		{
			name:   "author matches one name in a multi-author library row",
			title:  "Good Omens",
			author: "Terry Pratchett",
			wantID: 3,
		},
		{
			name:   "unabridged suffix is edition noise, not a different book",
			title:  "Project Hail Mary Unabridged",
			author: "Andy Weir",
			// The audiobook shelf: an audiobook result must find the
			// audiobook row.
			mediaType: "audiobook",
			wantID:    4,
		},
		{
			// Imported collections are full of rows like this — an EPUB with
			// no dc:creator, a bare filename — so the title has to decide.
			name:   "library row without an author matches on title alone",
			title:  "Dune",
			author: "Frank Herbert",
			wantID: 5,
		},
		{
			name:   "result without an author matches the authorless row",
			title:  "Dune",
			author: "",
			wantID: 5,
		},
		{
			name:   "placeholder author on the result is not treated as a name",
			title:  "The Hobbit",
			author: "Unknown",
			wantID: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			match, ok := idx.Lookup(tc.title, tc.author, tc.mediaType)
			if tc.wantID == 0 {
				if ok {
					t.Fatalf("expected no match, got item %d (%q)", match.ID, match.Title)
				}
				return
			}
			if !ok {
				t.Fatalf("expected item %d, got no match", tc.wantID)
			}
			if match.ID != tc.wantID {
				t.Fatalf("matched item %d (%q), want %d", match.ID, match.Title, tc.wantID)
			}
			if match.Title == "" {
				t.Error("match carries no library title to show the user")
			}
		})
	}
}

// False positives are the expensive failure: they tell a user they own a book
// they do not and hide the download behind an extra click.
func TestLookupRejectsDifferentBooks(t *testing.T) {
	idx := testIndex()

	tests := []struct {
		name      string
		title     string
		author    string
		mediaType string
	}{
		{
			name:   "same title, different author",
			title:  "The Hobbit",
			author: "Someone Else",
		},
		{
			name:   "different book by an author in the library",
			title:  "Murder on the Orient Express",
			author: "Agatha Christie",
		},
		{
			name:   "title is a prefix of a library title",
			title:  "The",
			author: "J. R. R. Tolkien",
		},
		{
			name:   "title extends a library title with real words",
			title:  "The Hobbit Illustrated Companion",
			author: "J. R. R. Tolkien",
		},
		{
			name:      "right title on the wrong shelf",
			title:     "The Hobbit",
			author:    "J. R. R. Tolkien",
			mediaType: "audiobook",
		},
		{
			name:      "audiobook title searched as an ebook",
			title:     "Project Hail Mary",
			author:    "Andy Weir",
			mediaType: "ebook",
		},
		{
			name:   "hyphenated title whose lead segment is not an author",
			title:  "Dune - The Graphic Novel",
			author: "Frank Herbert",
		},
		{
			name:  "empty title",
			title: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if match, ok := idx.Lookup(tc.title, tc.author, tc.mediaType); ok {
				t.Fatalf("false positive: matched item %d (%q)", match.ID, match.Title)
			}
		})
	}
}

func TestLookupDefaultsMissingMediaTypeToEbook(t *testing.T) {
	// Most ebook sources leave media_type empty; those results must still be
	// checked against the ebook shelf rather than matching nothing.
	idx := NewIndex([]Candidate{{ID: 7, Title: "Neuromancer", Author: "William Gibson"}})
	if _, ok := idx.Lookup("Neuromancer", "William Gibson", ""); !ok {
		t.Fatal("empty media type on both sides should compare as ebook")
	}
	if _, ok := idx.Lookup("Neuromancer", "William Gibson", "ebook"); !ok {
		t.Fatal("explicit ebook should match a row with no media type")
	}
}

func TestEmptyAndNilIndexAreInert(t *testing.T) {
	// A user with no library, or a database read that failed, must leave
	// search behaving exactly as it did before ownership detection existed.
	var nilIdx *Index
	if _, ok := nilIdx.Lookup("The Hobbit", "Tolkien", "ebook"); ok {
		t.Error("nil index reported ownership")
	}
	if nilIdx.Len() != 0 {
		t.Error("nil index reported a non-zero length")
	}

	empty := NewIndex(nil)
	if _, ok := empty.Lookup("The Hobbit", "Tolkien", "ebook"); ok {
		t.Error("empty index reported ownership")
	}
	if empty.Len() != 0 {
		t.Errorf("empty index length = %d, want 0", empty.Len())
	}
}

func TestIndexSkipsUnusableRows(t *testing.T) {
	// Rows whose title normalizes away carry no signal and must not become a
	// key that swallows every untitled result.
	idx := NewIndex([]Candidate{
		{ID: 1, Title: "   ", Author: "Nobody"},
		{ID: 2, Title: "!!!", Author: "Nobody"},
		{ID: 3, Title: "Real Book", Author: "Nobody"},
	})
	if idx.Len() != 1 {
		t.Fatalf("index length = %d, want 1", idx.Len())
	}
	if _, ok := idx.Lookup("!!!", "Nobody", "ebook"); ok {
		t.Error("punctuation-only title matched")
	}
}

func TestLookupPrefersAnAuthorAgreeingEdition(t *testing.T) {
	// Two library rows share a normalized title. The one whose author agrees
	// is the honest answer to "do I own this?".
	idx := NewIndex([]Candidate{
		{ID: 1, Title: "The Gift", Author: "Alice Author", MediaType: "ebook"},
		{ID: 2, Title: "The Gift", Author: "Bob Writer", MediaType: "ebook"},
	})
	match, ok := idx.Lookup("The Gift", "Bob Writer", "ebook")
	if !ok {
		t.Fatal("expected a match")
	}
	if match.ID != 2 {
		t.Fatalf("matched item %d, want the row by Bob Writer (2)", match.ID)
	}
}

func TestNormalizeTitle(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"4:50 from Paddington", "4 50 from paddington"},
		{"4.50 From Paddington", "4 50 from paddington"},
		{"  Mixed   CASE  ", "mixed case"},
		{"Project Hail Mary (Unabridged)", "project hail mary"},
		{"Émile — Or On Education", "émile or on education"},
		{"!!!", ""},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := NormalizeTitle(tc.in); got != tc.want {
				t.Errorf("NormalizeTitle(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestAuthorMatches(t *testing.T) {
	tests := []struct {
		name string
		want string
		raw  string
		ok   bool
	}{
		{name: "no wanted author agrees with anything", want: "", raw: "Anyone", ok: true},
		{name: "known author versus blank library field", want: "agatha christie", raw: "", ok: false},
		{name: "exact", want: "agatha christie", raw: "Agatha Christie", ok: true},
		{name: "ampersand list", want: "terry pratchett", raw: "Neil Gaiman & Terry Pratchett", ok: true},
		{name: "and list", want: "terry pratchett", raw: "Neil Gaiman and Terry Pratchett", ok: true},
		{name: "semicolon list", want: "terry pratchett", raw: "Gaiman, Neil; Terry Pratchett", ok: true},
		{name: "different person", want: "agatha christie", raw: "Arthur Conan Doyle", ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := AuthorMatches(tc.want, tc.raw); got != tc.ok {
				t.Errorf("AuthorMatches(%q, %q) = %v, want %v", tc.want, tc.raw, got, tc.ok)
			}
		})
	}
}

// Importers write these when a file names nobody. Comparing them as people
// would hide every authorless book from ownership detection.
func TestKnownAuthorDropsPlaceholders(t *testing.T) {
	for _, placeholder := range []string{"", "  ", "Unknown", "unknown author", "N/A", "Various", "Anonymous"} {
		if got := KnownAuthor(placeholder); got != "" {
			t.Errorf("KnownAuthor(%q) = %q, want empty", placeholder, got)
		}
	}
	if got := KnownAuthor("Ursula K. Le Guin"); got != "ursula k le guin" {
		t.Errorf("KnownAuthor dropped a real name: %q", got)
	}
}

func TestAuthorlessLibraryRowStillRejectsADifferentTitle(t *testing.T) {
	// Relaxing the author rule must not relax the title rule.
	idx := NewIndex([]Candidate{{ID: 1, Title: "Dune", MediaType: "ebook"}})
	if _, ok := idx.Lookup("Dune Messiah", "Frank Herbert", "ebook"); ok {
		t.Error("a different title matched an authorless row")
	}
}

func TestAuthorMatchesIsUnchangedForKnownNames(t *testing.T) {
	tests := []struct {
		name string
		want string
		raw  string
		ok   bool
	}{
		{name: "blank library author with a known wanted author", want: "agatha christie", raw: "", ok: false},
		{name: "same person", want: "agatha christie", raw: "Agatha Christie", ok: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := AuthorMatches(tc.want, tc.raw); got != tc.ok {
				t.Errorf("AuthorMatches(%q, %q) = %v, want %v", tc.want, tc.raw, got, tc.ok)
			}
		})
	}
}
