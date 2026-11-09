package metadata

import (
	"reflect"
	"testing"
)

func TestCleanReleaseName(t *testing.T) {
	tests := []struct{ in, want string }{
		{"The Left Hand of Darkness [EPUB]", "The Left Hand of Darkness"},
		{"Dune (Unabridged) 128kbps m4b", "Dune"},
		{"The.Left.Hand.of.Darkness.2017.EPUB-GROUP", "The Left Hand of Darkness"},
		{"Project Hail Mary 2021 MP3", "Project Hail Mary"},
		{"1984", "1984"},
		{"George Orwell - 1984", "George Orwell - 1984"},
		{"2001 A Space Odyssey", "2001 A Space Odyssey"},
		{"[EPUB]", ""},
		{"Catch-22", "Catch-22"},
		{"Spider-Man", "Spider-Man"},
		{"Ursula K. Le Guin - The Left Hand of Darkness - 2017", "Ursula K. Le Guin - The Left Hand of Darkness"},
		{"2017 - Ursula K. Le Guin - The Left Hand of Darkness", "Ursula K. Le Guin - The Left Hand of Darkness"},
		// Only the last of two year parts goes, so the title year stays.
		{"George Orwell - 1984 - 1949", "George Orwell - 1984"},
		// A year part among three or more is always read as a tag.
		{"Mistborn - 1984 - Someone", "Mistborn - Someone"},
		{"1984 - George Orwell", "1984 - George Orwell"},
		{"1984 - 1985", "1984 - 1985"},
		// Edition phrases go, along with the punctuation they leave behind.
		{"Eat Pray Love: 10th-Anniversary Edition by Elizabeth Gilbert", "Eat Pray Love by Elizabeth Gilbert"},
		{"Sapiens Tenth Anniversary Edition: A Brief History of Humankind", "Sapiens: A Brief History of Humankind"},
		{"Dune - Deluxe Edition", "Dune"},
	}
	for _, tt := range tests {
		if got := cleanReleaseName(tt.in); got != tt.want {
			t.Errorf("cleanReleaseName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestCoverReadings(t *testing.T) {
	r := func(title, author string) coverReading { return coverReading{Title: title, Author: author} }
	tests := []struct {
		title, author string
		want          []coverReading
	}{
		{"Dune", "", []coverReading{r("Dune", "")}},
		{"Dune", "Frank Herbert", []coverReading{r("Dune", "Frank Herbert")}},
		{"Ursula K. Le Guin - The Left Hand of Darkness", "", []coverReading{
			r("Ursula K. Le Guin", "The Left Hand of Darkness"),
			r("The Left Hand of Darkness", "Ursula K. Le Guin"),
			r("Ursula K. Le Guin - The Left Hand of Darkness", ""),
		}},
		{"Brandon Sanderson - Mistborn 01 - The Final Empire", "", []coverReading{
			r("The Final Empire", "Brandon Sanderson"),
			r("Brandon Sanderson - Mistborn 01 - The Final Empire", ""),
		}},
		{"Dune by Frank Herbert", "", []coverReading{
			r("Dune", "Frank Herbert"),
			r("Dune by Frank Herbert", ""),
		}},
		{"Red Rising - Pierce Brown", "Pierce Brown", []coverReading{
			r("Red Rising - Pierce Brown", "Pierce Brown"),
			r("Red Rising", "Pierce Brown"),
			r("Pierce Brown", "Pierce Brown"),
		}},
	}
	for _, tt := range tests {
		if got := coverReadings(tt.title, tt.author); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("coverReadings(%q, %q) = %v, want %v", tt.title, tt.author, got, tt.want)
		}
	}
}

func TestCoverKey(t *testing.T) {
	k := func(title, author string) string { return CoverKey(CoverKindBook, title, author) }
	a := k("Ursula K. Le Guin - The Left Hand of Darkness", "")
	if a == "" || a != k("The Left Hand of Darkness - Ursula K. Le Guin", "") ||
		a != k("The Left Hand of Darkness [EPUB]", "Ursula K. Le Guin") {
		t.Fatalf("name order or noise changed the key: %q", a)
	}
	if a == CoverKey(CoverKindManga, "Ursula K. Le Guin - The Left Hand of Darkness", "") {
		t.Fatal("kinds must not share keys")
	}
	if k("Foundation 2", "") == k("Foundation", "") {
		t.Fatal("volume dropped from key")
	}
	if k("Catch-22", "") == k("Catch", "") {
		t.Fatal("hyphenated suffix dropped from key")
	}
	if k("[EPUB]", "") != "" {
		t.Fatal("no words must produce an empty key")
	}
	if k("Война и мир", "") == "" {
		t.Fatal("non-Latin titles must produce a key")
	}
}

func TestCoverQueryText(t *testing.T) {
	got := coverQueryText("Ursula K. Le Guin - The Left Hand of Darkness [EPUB]", "")
	if want := "Ursula K. Le Guin The Left Hand of Darkness"; got != want {
		t.Errorf("coverQueryText = %q, want %q", got, want)
	}
	if got, want := coverQueryText("Dune", "Frank Herbert"), "Dune Frank Herbert"; got != want {
		t.Errorf("coverQueryText = %q, want %q", got, want)
	}
}

func TestMatchCoverDoc(t *testing.T) {
	doc := func(title string, coverID int, authors ...string) olSearchDoc {
		return olSearchDoc{Title: title, CoverI: coverID, AuthorName: authors}
	}
	tests := []struct {
		name          string
		title, author string
		docs          []olSearchDoc
		want          int
	}{
		{"author match, either order", "Ursula K. Le Guin - The Left Hand of Darkness", "",
			[]olSearchDoc{doc("The Left Hand of Darkness", 1, "Ursula K. Le Guin")}, 1},
		{"subtitle allowed", "Mistborn", "Brandon Sanderson",
			[]olSearchDoc{doc("Mistborn: The Final Empire", 2, "Brandon Sanderson")}, 2},
		{"whole name spans title and subtitle", "Mistborn - The Final Empire", "",
			[]olSearchDoc{doc("Mistborn: The Final Empire", 3, "Brandon Sanderson")}, 3},
		{"different book sharing a word", "Dune", "",
			[]olSearchDoc{doc("Dune Messiah", 4, "Frank Herbert")}, 0},
		{"adjacent volume", "Foundation 2", "Isaac Asimov",
			[]olSearchDoc{doc("Foundation", 5, "Isaac Asimov")}, 0},
		{"wrong author", "Dune", "Frank Herbert",
			[]olSearchDoc{doc("Dune", 6, "Someone Else")}, 0},
		{"no cover skipped", "Dune", "Frank Herbert",
			[]olSearchDoc{doc("Dune", 0, "Frank Herbert"), doc("Dune", 7, "Frank Herbert")}, 7},
		{"box set rejected", "Dune", "Frank Herbert",
			[]olSearchDoc{doc("Dune Box Set", 8, "Frank Herbert")}, 0},
		{"box set allowed when asked for", "Dune Box Set", "Frank Herbert",
			[]olSearchDoc{doc("Dune Box Set", 9, "Frank Herbert")}, 9},
		{"no author, one author throughout", "Dune", "",
			[]olSearchDoc{doc("Dune", 10, "Frank Herbert"), doc("Dune", 11, "Herbert, Frank")}, 10},
		{"no author, conflicting authors", "Monster", "",
			[]olSearchDoc{doc("Monster", 12, "A Author"), doc("Monster", 13, "B Writer")}, 0},
		{"no author, candidate has no author", "Dune", "",
			[]olSearchDoc{doc("Dune", 14, "Frank Herbert"), doc("Dune", 15)}, 0},
		{"author punctuation differs", "The Hobbit - J.R.R. Tolkien", "",
			[]olSearchDoc{doc("The Hobbit", 16, "J. R. R. Tolkien")}, 16},
		{"single initial is not an author", "The Hobbit", "R",
			[]olSearchDoc{doc("The Hobbit", 17, "J. R. R. Tolkien")}, 0},
		{"numeric title", "George Orwell - 1984", "",
			[]olSearchDoc{doc("1984", 18, "George Orwell")}, 18},
		{"year as its own dash part", "Ursula K. Le Guin - The Left Hand of Darkness - 2017", "",
			[]olSearchDoc{doc("The Left Hand of Darkness", 20, "Ursula K. Le Guin")}, 20},
		{"numeric title with a year part", "George Orwell - 1984 - 1949", "",
			[]olSearchDoc{doc("1984", 21, "George Orwell")}, 21},
		{"Cyrillic title", "Война и мир", "Лев Толстой",
			[]olSearchDoc{doc("Война и мир", 19, "Лев Толстой")}, 19},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchCoverDoc(coverReadings(tt.title, tt.author), tt.docs)
			switch {
			case got == nil && tt.want != 0:
				t.Fatalf("miss, want cover %d", tt.want)
			case got != nil && got.CoverI != tt.want:
				t.Fatalf("cover %d, want %d", got.CoverI, tt.want)
			}
		})
	}
}

// Release names and Open Library records seen in real searches.
func TestMatchCoverDocSubtitles(t *testing.T) {
	sub := func(title, subtitle string, coverID int, authors ...string) olSearchDoc {
		return olSearchDoc{Title: title, Subtitle: subtitle, CoverI: coverID, AuthorName: authors}
	}
	eatPrayLove := []olSearchDoc{
		sub("Eat, Pray, Love", "One Woman's Search for Everything Across Italy, India and Indonesia", 111433, "Elizabeth Gilbert"),
		sub("Eat, Pray, Love by Elizabeth Gilbert", "", 0, "Trivia-on-Books Staff"),
		sub("Eat pray love made me do it", "", 10210421, "Rebecca Asher"),
		sub("Eat Pray Love Made Me Do It", "", 13657572, "Elizabeth Gilbert"),
	}
	sapiens := []olSearchDoc{
		sub("Sapiens", "A Brief History of Humankind", 8634250, "Yuval Noah Harari"),
		sub("Summary of Sapiens by Yuval Noah Harari", "", 0, "FlashBooks Book Summaries"),
		sub("Summary of Sapiens A Brief History of Humankind", "", 0, "Book Addict"),
		sub("Sapiens and Homo Deus : the e-Book Collection", "", 0, "Yuval Noah Harari"),
		sub("Sapiens", "A Graphic History, Volume 2: The Pillars of Civilization", 15252176, "Yuval Noah Harari", "Daniel Casanave", "David VanderMeulen"),
	}
	tests := []struct {
		name          string
		title, author string
		docs          []olSearchDoc
		want          int
	}{
		{"subtitle in its own field", "Eat, Pray, Love: One Woman’s Search for Everything Across Italy, India and Indonesia", "Elizabeth Gilbert", eatPrayLove, 111433},
		{"subtitle in its own field, no author", "Eat, Pray, Love: One Woman’s Search for Everything Across Italy, India and Indonesia", "", eatPrayLove, 111433},
		{"by-author name with a subtitle", "Sapiens: A Brief History of Humankind by Yuval Noah Harari", "", sapiens, 8634250},
		{"edition in brackets", "Sapiens [Tenth Anniversary Edition]: A Brief History of Humankind by Yuval Noah Harari", "", sapiens, 8634250},
		{"author first, semicolon subtitle", "Yuval Noah Harari - Sapiens; A Brief History Of Humankind", "", sapiens, 8634250},
		{"no subtitle, no author", "Sapiens - Yuval Noah Harari", "", sapiens, 8634250},
		{"edition phrase", "Eat Pray Love: 10th-Anniversary Edition by Elizabeth Gilbert", "", eatPrayLove, 111433},
		{"main title when the record has no subtitle", "Eat Pray Love Made Me Do It: Life Journeys Inspired by the Bestselling Memoir by Elizabeth Gilbert", "", eatPrayLove, 13657572},
		{"volume picks its own record", "Sapiens: A Graphic History, Volume 2: The Pillars of Civilization by Yuval Noah Harari, David Vandermeulen, Daniel Casanave", "", sapiens, 15252176},

		// Each of these would show the wrong book's cover.
		{"volume in stripped brackets", "Sapiens: A Graphic History: The Birth of Humankind (Vol. 1) by Yuval Noah Harari, David Vandermeulen, Daniel Casanave", "", sapiens, 0},
		{"collection", "Sapiens and Homo Deus: The E-book Collection: A Brief History of Humankind and A Brief History of Tomorrow by Yuval Noah Harari", "", sapiens, 0},
		{"cookbook", "Eat Pray Love Cookbook: Attraversiamo! by Sharon Powell", "", eatPrayLove, 0},
		{"different book with a subtitle", "Eat, Pray, Love Pasta!: 40 Tempting Trattoria Recipes to Celebrate National Spaghetti Day by Martha Stephenson", "", eatPrayLove, 0},
		{"main title of a record with its own subtitle", "Star Wars: Thrawn", "Timothy Zahn",
			[]olSearchDoc{sub("Star Wars: Heir to the Empire", "", 5, "Timothy Zahn"), sub("Star Wars", "Dark Force Rising", 6, "Timothy Zahn")}, 0},
		{"volume named in the subtitle", "Red Rising: Book 2", "Pierce Brown",
			[]olSearchDoc{sub("Red Rising", "", 7, "Pierce Brown")}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchCoverDoc(coverReadings(tt.title, tt.author), tt.docs)
			switch {
			case got == nil && tt.want != 0:
				t.Fatalf("miss, want cover %d", tt.want)
			case got != nil && got.CoverI != tt.want:
				t.Fatalf("cover %d (%q), want %d", got.CoverI, got.Title, tt.want)
			}
		})
	}
}
