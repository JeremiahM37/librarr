package metadata

import "testing"

func docs(titles ...string) []olSearchDoc {
	out := make([]olSearchDoc, 0, len(titles))
	for _, t := range titles {
		out = append(out, olSearchDoc{Title: t, Key: "/works/" + t})
	}
	return out
}

func TestPickBestDoc(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		author string
		docs   []olSearchDoc
		want   string
	}{
		{
			// The regression this function exists for: on 2026-08-06 the live
			// API ranked the trilogy first for title=Catching Fire.
			name:  "single volume beats the omnibus that outranks it",
			query: "Catching Fire",
			docs: docs(
				"The Hunger Games Trilogy (Hunger Games / Catching Fire / Mockingjay)",
				"Hunger Games, catching fire",
				"Catching fire",
			),
			want: "Catching fire",
		},
		{
			name:  "box set loses to the volume",
			query: "The Mark of Athena",
			docs: docs(
				"Heroes of Olympus Complete Collection 5 Books Box Set -The Lost Hero/The Son of Neptune/The Mark of Athena",
				"The Mark of Athena",
			),
			want: "The Mark of Athena",
		},
		{
			name:  "book-range spine loses to the volume",
			query: "A Storm of Swords",
			docs: docs(
				"A Song of Ice and Fire, Books I-III",
				"A Storm of Swords",
			),
			want: "A Storm of Swords",
		},
		{
			name:  "spelled-out volume number matches a digit query",
			query: "Heroes of Olympus Book 3",
			docs: docs(
				"Heroes of Olympus Series, 4 Books Collection Set",
				"Heroes of Olympus, Book Three: The Mark of Athena",
			),
			want: "Heroes of Olympus, Book Three: The Mark of Athena",
		},
		{
			// Penalising bundles must not make bundles unfindable.
			name:  "a collection query still gets the collection",
			query: "The Hunger Games Trilogy",
			docs: docs(
				"Catching Fire",
				"The Hunger Games Trilogy",
			),
			want: "The Hunger Games Trilogy",
		},
		{
			name:  "articles and punctuation do not matter",
			query: "Two Towers",
			docs: docs(
				"The Lord of the Rings: The Two Towers, The Return of the King",
				"The Two Towers",
			),
			want: "The Two Towers",
		},
		{
			// Nothing better to pick — Open Library's own ranking stands.
			name:  "falls back to the first result when all are equally bad",
			query: "Something Entirely Unrelated",
			docs:  docs("A Wholly Different Book", "Another Different Book"),
			want:  "A Wholly Different Book",
		},
		{
			name:  "query with no usable words keeps Open Library order",
			query: "The",
			docs:  docs("The Trial", "The Castle"),
			want:  "The Trial",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pickBestDoc(tt.query, tt.author, tt.docs)
			if got == nil {
				t.Fatalf("pickBestDoc(%q) = nil, want %q", tt.query, tt.want)
			}
			if got.Title != tt.want {
				t.Errorf("pickBestDoc(%q) = %q, want %q", tt.query, got.Title, tt.want)
			}
		})
	}
}

func TestPickBestDoc_AuthorTiebreakPicksTheRightRecord(t *testing.T) {
	// Same title twice: only the author field can separate them, so assert on
	// the record rather than the title.
	got := pickBestDoc("The Two Towers", "J.R.R. Tolkien", []olSearchDoc{
		{Title: "The Two Towers", AuthorName: []string{"Some Abridger"}},
		{Title: "The Two Towers", AuthorName: []string{"J.R.R. Tolkien"}},
	})
	if got == nil || len(got.AuthorName) == 0 || got.AuthorName[0] != "J.R.R. Tolkien" {
		t.Fatalf("pickBestDoc picked %+v, want the Tolkien record", got)
	}
}

func TestPickBestDoc_Empty(t *testing.T) {
	if got := pickBestDoc("anything", "", nil); got != nil {
		t.Errorf("pickBestDoc(nil docs) = %+v, want nil", got)
	}
}

func TestOLTitleWords(t *testing.T) {
	tests := []struct {
		title string
		want  []string
	}{
		{"The Mark of Athena", []string{"mark", "athena"}},
		{"Heroes of Olympus, Book Three", []string{"heroes", "olympus", "book", "3"}},
		{"Books I-III", []string{"books", "i", "3"}}, // bare "I" is left alone on purpose
		{"", nil},
		{"The", nil},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			got := olTitleWords(tt.title)
			if len(got) != len(tt.want) {
				t.Fatalf("olTitleWords(%q) = %v, want %v", tt.title, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("olTitleWords(%q) = %v, want %v", tt.title, got, tt.want)
				}
			}
		})
	}
}

func TestOLCollectionRe(t *testing.T) {
	bundles := []string{
		"The Hunger Games Trilogy",
		"Heroes of Olympus Complete Collection 5 Books Box Set",
		"A Song of Ice and Fire, Books I-III",
		"The Chronicles of Narnia Boxed Set",
		"Discworld Omnibus",
		"The Heroes of Olympus Paperback 3-Book Boxed Set",
		"Foundation: Books 1 - 3",
	}
	for _, title := range bundles {
		if !olCollectionRe.MatchString(title) {
			t.Errorf("olCollectionRe did not match bundle title %q", title)
		}
	}

	volumes := []string{
		"The Mark of Athena",
		"Catching Fire",
		"Harry Potter and the Chamber of Secrets",
		"The Two Towers",
		"A Storm of Swords",
		"The Collector", // "collection" must not match by prefix
	}
	for _, title := range volumes {
		if olCollectionRe.MatchString(title) {
			t.Errorf("olCollectionRe wrongly matched single volume %q", title)
		}
	}
}
