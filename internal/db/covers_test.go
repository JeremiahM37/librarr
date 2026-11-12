package db

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

func addCoverTestItem(t *testing.T, d *DB, title, mediaType string) int64 {
	t.Helper()
	id, err := d.AddItem(&models.LibraryItem{Title: title, MediaType: mediaType, FilePath: "/fixture/" + title})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCoverLookupRoundTrip(t *testing.T) {
	d := newTestDB(t)
	if _, found, _, err := d.GetCoverLookup("missing"); err != nil || found {
		t.Fatalf("missing key: found=%v err=%v", found, err)
	}

	at := time.Unix(1700000000, 0)
	if err := d.PutCoverLookup("hit", "https://example.com/a.jpg", at); err != nil {
		t.Fatal(err)
	}
	if err := d.PutCoverLookup("miss", "", at); err != nil {
		t.Fatal(err)
	}
	url, found, fetchedAt, err := d.GetCoverLookup("hit")
	if err != nil || !found || url != "https://example.com/a.jpg" || !fetchedAt.Equal(at) {
		t.Fatalf("hit: %q %v %v %v", url, found, fetchedAt, err)
	}
	url, found, _, err = d.GetCoverLookup("miss")
	if err != nil || !found || url != "" {
		t.Fatalf("miss: %q %v %v", url, found, err)
	}

	if err := d.PutCoverLookup("hit", "https://example.com/b.jpg", at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	url, _, fetchedAt, _ = d.GetCoverLookup("hit")
	if url != "https://example.com/b.jpg" || !fetchedAt.Equal(at.Add(time.Hour)) {
		t.Fatalf("second Put did not replace: %q %v", url, fetchedAt)
	}
}

func TestPruneCoverLookups(t *testing.T) {
	d := newTestDB(t)
	base := time.Unix(1700000000, 0)
	d.PutCoverLookup("old", "u", base)
	d.PutCoverLookup("new", "u", base.Add(2*time.Hour))
	if err := d.PruneCoverLookups(base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, found, _, _ := d.GetCoverLookup("old"); found {
		t.Error("old entry should be pruned")
	}
	if _, found, _, _ := d.GetCoverLookup("new"); !found {
		t.Error("new entry should remain")
	}
}

func TestItemsNeedingCover(t *testing.T) {
	d := newTestDB(t)
	cutoff := time.Unix(1700000000, 0)
	fresh := addCoverTestItem(t, d, "fresh", "ebook")
	covered := addCoverTestItem(t, d, "covered", "ebook")
	recent := addCoverTestItem(t, d, "recent", "ebook")
	stale := addCoverTestItem(t, d, "stale", "ebook")
	other := addCoverTestItem(t, d, "other", "manga")
	if err := d.SetItemCover(covered, "c.jpg", cutoff.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := d.SetItemCover(recent, "", cutoff.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := d.SetItemCover(stale, "", cutoff.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	items, err := d.ItemsNeedingCover([]string{"ebook"}, cutoff, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]bool{}
	for _, it := range items {
		got[it.ID] = true
	}
	if !got[fresh] || !got[stale] || len(got) != 2 {
		t.Fatalf("want fresh and stale only, got %v (covered=%d recent=%d other=%d)", got, covered, recent, other)
	}

	items, err = d.ItemsNeedingCover([]string{"ebook"}, cutoff, 1)
	if err != nil || len(items) != 1 || items[0].ID != stale {
		t.Fatalf("limit 1 should return newest (%d): %+v, %v", stale, items, err)
	}
}

func TestClearItemCover(t *testing.T) {
	d := newTestDB(t)
	id := addCoverTestItem(t, d, "book", "ebook")
	now := time.Now()
	if err := d.SetItemCover(id, "1.jpg", now); err != nil {
		t.Fatal(err)
	}
	if items, _ := d.ItemsNeedingCover([]string{"ebook"}, now.Add(time.Hour), 10); len(items) != 0 {
		t.Fatalf("covered item should not need a cover: %+v", items)
	}
	files, err := d.CoverFiles()
	if err != nil || files[id] != "1.jpg" {
		t.Fatalf("CoverFiles: %v, %v", files, err)
	}
	if err := d.ClearItemCover(id); err != nil {
		t.Fatal(err)
	}
	items, err := d.ItemsNeedingCover([]string{"ebook"}, now.Add(-time.Hour), 10)
	if err != nil || len(items) != 1 || items[0].ID != id {
		t.Fatalf("cleared item should need a cover again: %+v, %v", items, err)
	}
	if files, _ := d.CoverFiles(); len(files) != 0 {
		t.Fatalf("CoverFiles should be empty after clear: %v", files)
	}
}

func TestItemToJSONCoverURL(t *testing.T) {
	m := ItemToJSON(models.LibraryItem{ID: 7, CoverFile: "7.jpg"})
	if m["cover_url"] != "/api/library/7/cover" {
		t.Errorf("cover_url = %v", m["cover_url"])
	}
	if _, ok := ItemToJSON(models.LibraryItem{ID: 7})["cover_url"]; ok {
		t.Error("cover_url should be absent without a cover")
	}
}

func TestCoverColumnsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	d, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	id := addCoverTestItem(t, d, "book", "ebook")
	if err := d.SetItemCover(id, "x.jpg", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := d.PutCoverLookup("k", "u", time.Now()); err != nil {
		t.Fatal(err)
	}
	d.Close()

	d, err = New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d.Close()
	item, err := d.GetItem(id)
	if err != nil || item.CoverFile != "x.jpg" {
		t.Fatalf("cover file lost: %+v, %v", item, err)
	}
	if _, found, _, _ := d.GetCoverLookup("k"); !found {
		t.Error("lookup lost")
	}
}
