package db

import (
	"path/filepath"
	"testing"
	"time"
)

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

func TestCoverLookupsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	d, err := New(path)
	if err != nil {
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
	if _, found, _, _ := d.GetCoverLookup("k"); !found {
		t.Error("lookup lost")
	}
}
