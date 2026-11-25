package db

import (
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

func addLinkItem(t *testing.T, d *DB, title, sourceID string) int64 {
	t.Helper()
	id, err := d.AddItem(&models.LibraryItem{Title: title, MediaType: "ebook", SourceID: sourceID, FilePath: "/books/" + title + ".epub"})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func linkServices(t *testing.T, d *DB, id int64) []string {
	t.Helper()
	stored, err := d.LibraryLinks([]int64{id})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range stored[id] {
		out = append(out, l.Service+":"+l.ExternalID)
	}
	return out
}

func TestReplaceLibraryLinksReplacesOneServiceOnly(t *testing.T) {
	d := newTestDB(t)
	a, b := addLinkItem(t, d, "A", "a"), addLinkItem(t, d, "B", "b")
	now := time.Now()
	if err := d.ReplaceLibraryLinks("kavita", []models.LibraryLink{
		{ItemID: a, ExternalID: "1", URLPath: "/library/1/series/1", CoverPath: "/cover/1"},
		{ItemID: b, ExternalID: "2", URLPath: "/library/1/series/2"},
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := d.ReplaceLibraryLinks("abs", []models.LibraryLink{{ItemID: a, ExternalID: "li_1", URLPath: "/item/li_1"}}, now); err != nil {
		t.Fatal(err)
	}
	// A later Kavita sync that no longer finds B drops only B's Kavita link.
	if err := d.ReplaceLibraryLinks("kavita", []models.LibraryLink{{ItemID: a, ExternalID: "1", URLPath: "/library/1/series/1"}}, now); err != nil {
		t.Fatal(err)
	}
	if got := linkServices(t, d, a); len(got) != 2 || got[0] != "abs:li_1" || got[1] != "kavita:1" {
		t.Errorf("A links = %v", got)
	}
	if got := linkServices(t, d, b); len(got) != 0 {
		t.Errorf("B links = %v, want none", got)
	}
}

func TestReplaceLibraryLinksSkipsMissingItems(t *testing.T) {
	d := newTestDB(t)
	a := addLinkItem(t, d, "A", "a")
	if err := d.ReplaceLibraryLinks("kavita", []models.LibraryLink{
		{ItemID: a, ExternalID: "1"},
		{ItemID: a + 100, ExternalID: "2"},
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	stored, err := d.LibraryLinks([]int64{a, a + 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 || len(stored[a]) != 1 {
		t.Errorf("stored = %v", stored)
	}
}

func TestDeleteItemDropsLinks(t *testing.T) {
	d := newTestDB(t)
	a := addLinkItem(t, d, "A", "a")
	if err := d.ReplaceLibraryLinks("kavita", []models.LibraryLink{{ItemID: a, ExternalID: "1"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteItem(a); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := d.db.QueryRow("SELECT COUNT(*) FROM library_links").Scan(&n); err != nil || n != 0 {
		t.Errorf("%d links left, %v", n, err)
	}
}

func TestLinkCandidates(t *testing.T) {
	d := newTestDB(t)
	a := addLinkItem(t, d, "A", "a")
	got, err := d.LinkCandidates()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (LinkCandidate{ID: a, FilePath: "/books/A.epub", MediaType: "ebook"}) {
		t.Errorf("candidates = %+v", got)
	}
}
