package db

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/quality"
)

// TestMigrate_UpgradesPreWantedSchema opens a database laid out the way the
// last release created it (a bare wishlist, profiles without media_type, jobs
// without wanted_id) and checks the additive migration leaves existing rows
// readable with sane defaults. This is the incremental path real users hit;
// a fresh database exercises only the CREATE statements.
func TestMigrate_UpgradesPreWantedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		`CREATE TABLE wishlist (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL DEFAULT '', author TEXT NOT NULL DEFAULT '', media_type TEXT NOT NULL DEFAULT 'ebook', added_at REAL NOT NULL DEFAULT (strftime('%s','now')))`,
		`INSERT INTO wishlist (title, author, media_type) VALUES ('Old Wish', 'Someone', 'ebook')`,
		`CREATE TABLE quality_profiles (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL DEFAULT '', format_ranking TEXT NOT NULL DEFAULT '[]', preferred_size_min INTEGER NOT NULL DEFAULT 0, preferred_size_max INTEGER NOT NULL DEFAULT 0, upgrade_allowed INTEGER NOT NULL DEFAULT 0, cutoff_format TEXT NOT NULL DEFAULT '')`,
		`INSERT INTO quality_profiles (name, format_ranking, cutoff_format) VALUES ('Legacy', '["EPUB","PDF"]', 'EPUB')`,
		`CREATE TABLE download_jobs (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'queued', detail TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', url TEXT NOT NULL DEFAULT '', md5 TEXT NOT NULL DEFAULT '', media_type TEXT NOT NULL DEFAULT 'ebook', retry_count INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL DEFAULT 2, created_at REAL NOT NULL DEFAULT (strftime('%s','now')), updated_at REAL NOT NULL DEFAULT (strftime('%s','now')))`,
		`INSERT INTO download_jobs (id, title, status) VALUES ('job1', 'Old Job', 'completed')`,
		`CREATE TABLE monitored_authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL DEFAULT '', last_checked REAL NOT NULL DEFAULT 0, last_book_found TEXT NOT NULL DEFAULT '', check_interval_days INTEGER NOT NULL DEFAULT 7)`,
		`INSERT INTO monitored_authors (name) VALUES ('Old Author')`,
	}
	for _, s := range stmts {
		if _, err := raw.Exec(s); err != nil {
			t.Fatalf("seed old schema: %v\n%s", err, s)
		}
	}
	raw.Close()

	d, err := New(path)
	if err != nil {
		t.Fatalf("migrate old schema: %v", err)
	}
	defer d.Close()

	items, err := d.GetWishlist()
	if err != nil || len(items) != 1 {
		t.Fatalf("GetWishlist after migration: %v, %d items", err, len(items))
	}
	w := items[0]
	if !w.Monitored || w.QualityProfileID != 0 || w.LibraryItemID != 0 || w.Source != "manual" || w.CurrentFormat != "" {
		t.Fatalf("migrated wishlist row has wrong defaults: %+v", w)
	}

	profiles, err := d.GetQualityProfiles()
	if err != nil {
		t.Fatal(err)
	}
	var legacy *QualityProfile
	builtinByType := map[string]int{}
	for i := range profiles {
		if profiles[i].Name == "Legacy" {
			legacy = &profiles[i]
		}
		if profiles[i].Builtin {
			builtinByType[profiles[i].MediaType]++
		}
	}
	if legacy == nil {
		t.Fatal("legacy profile lost in migration")
	}
	if legacy.Builtin || legacy.MediaType != "ebook" {
		t.Fatalf("legacy profile should be a non-builtin ebook profile: %+v", legacy)
	}
	if legacy.FormatRanking[0] != "EPUB" {
		t.Fatalf("legacy ranking should be preserved verbatim until edited, got %v", legacy.FormatRanking)
	}
	for _, mt := range []string{"ebook", "audiobook", "manga"} {
		if builtinByType[mt] != 1 {
			t.Errorf("expected exactly one builtin %s profile, got %d", mt, builtinByType[mt])
		}
	}

	job, err := d.GetJob("job1")
	if err != nil || job.WantedID != 0 {
		t.Fatalf("old job unreadable after migration: %v %+v", err, job)
	}

	authors, err := d.GetMonitoredAuthors()
	if err != nil || len(authors) != 1 || !authors[0].AutoAdd || authors[0].SeenWorks != 0 {
		t.Fatalf("old author row wrong after migration: %v %+v", err, authors)
	}

	// Migrating twice is a no-op.
	d.Close()
	d2, err := New(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer d2.Close()
	profiles2, _ := d2.GetQualityProfiles()
	if len(profiles2) != len(profiles) {
		t.Fatalf("second migration changed profile count %d -> %d", len(profiles), len(profiles2))
	}
}

func TestDefaultProfilesSeededAndProtected(t *testing.T) {
	d := newTestDB(t)
	for _, mt := range []string{"ebook", "audiobook", "manga"} {
		qp, err := d.GetDefaultQualityProfile(mt)
		if err != nil {
			t.Fatalf("no default for %s: %v", mt, err)
		}
		if !qp.Builtin || qp.MediaType != mt || !qp.UpgradeAllowed {
			t.Fatalf("default %s profile malformed: %+v", mt, qp)
		}
		if err := quality.Validate(qp.Profile()); err != nil {
			t.Fatalf("seeded %s profile invalid: %v", mt, err)
		}
		if err := d.DeleteQualityProfile(qp.ID); err != ErrBuiltinProfile {
			t.Fatalf("deleting builtin should fail with ErrBuiltinProfile, got %v", err)
		}
		// Editing a builtin keeps it builtin and on its media type.
		qp.Name = "Renamed"
		qp.Builtin = false
		qp.MediaType = "manga"
		if err := d.UpdateQualityProfile(qp); err != nil {
			t.Fatal(err)
		}
		got, _ := d.GetQualityProfile(qp.ID)
		if !got.Builtin || got.MediaType != mt || got.Name != "Renamed" {
			t.Fatalf("builtin edit changed protected fields: %+v", got)
		}
	}

	// Formats are normalised on write so the engine and the UI agree.
	id, err := d.CreateQualityProfile(&QualityProfile{Name: "Custom", FormatRanking: []string{"EPUB", ".Pdf"}, CutoffFormat: "PDF"})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := d.GetQualityProfile(id)
	if got.FormatRanking[0] != "epub" || got.FormatRanking[1] != "pdf" || got.CutoffFormat != "pdf" || got.MediaType != "ebook" || got.Builtin {
		t.Fatalf("custom profile not normalised: %+v", got)
	}

	// ResolveQualityProfile falls back sensibly.
	if r := d.ResolveQualityProfile(id, "ebook"); r.ID != id {
		t.Fatalf("explicit profile not resolved: %+v", r)
	}
	if r := d.ResolveQualityProfile(9999, "audiobook"); !r.Builtin || r.MediaType != "audiobook" {
		t.Fatalf("missing profile should fall back to audiobook default: %+v", r)
	}
	if r := d.ResolveQualityProfile(0, ""); r.MediaType != "ebook" {
		t.Fatalf("empty media type should resolve to ebook default: %+v", r)
	}

	// Deleting a custom profile resets items that used it to the default.
	wid, _ := d.AddWishlistItemWithOptions(models.WishlistItem{Title: "X", Monitored: true, QualityProfileID: id})
	if err := d.DeleteQualityProfile(id); err != nil {
		t.Fatal(err)
	}
	w, _ := d.GetWishlistItem(wid)
	if w.QualityProfileID != 0 {
		t.Fatalf("item should fall back to default profile after delete, got %d", w.QualityProfileID)
	}
	if _, err := d.GetQualityProfile(id); err != sql.ErrNoRows {
		t.Fatalf("deleted profile still readable: %v", err)
	}
}

func TestWishlist_StateMachine(t *testing.T) {
	d := newTestDB(t)
	id, err := d.AddWishlistItem("Dune", "Frank Herbert", "ebook")
	if err != nil {
		t.Fatal(err)
	}
	w, err := d.GetWishlistItem(id)
	if err != nil {
		t.Fatal(err)
	}
	if !w.Monitored || w.LibraryItemID != 0 || w.ActiveJobID != "" || w.Source != "manual" {
		t.Fatalf("fresh item defaults wrong: %+v", w)
	}

	// Grab in flight.
	if err := d.SetWishlistActiveJob(id, "job-abc"); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.FindWishlistByActiveJob("job-abc"); got == nil || got.ID != id {
		t.Fatalf("FindWishlistByActiveJob failed: %+v", got)
	}
	if got, _ := d.FindWishlistByActiveJob(""); got != nil {
		t.Fatal("empty job ref must not match anything")
	}
	if err := d.RecordWishlistSearch(id, "3 results, chose PDF"); err != nil {
		t.Fatal(err)
	}

	// A PDF lands: the item is satisfied by it.
	pdfID, _ := d.AddItem(&models.LibraryItem{Title: "Dune", Author: "Frank Herbert", FilePath: filepath.Join(t.TempDir(), "dune.pdf"), FileFormat: "pdf", MediaType: "ebook"})
	prev, err := d.SatisfyWishlistItem(id, pdfID)
	if err != nil || prev != 0 {
		t.Fatalf("first satisfy: prev=%d err=%v", prev, err)
	}
	w, _ = d.GetWishlistItem(id)
	if w.LibraryItemID != pdfID || w.CurrentFormat != "pdf" || w.ActiveJobID != "" || w.LastResult != "3 results, chose PDF" || w.LastSearched.IsZero() {
		t.Fatalf("after satisfy: %+v", w)
	}

	// An EPUB upgrade lands: previous link is reported so the PDF can be retired.
	epubID, _ := d.AddItem(&models.LibraryItem{Title: "Dune", Author: "Frank Herbert", FilePath: filepath.Join(t.TempDir(), "dune.epub"), FileFormat: "epub", MediaType: "ebook"})
	prev, err = d.SatisfyWishlistItem(id, epubID)
	if err != nil || prev != pdfID {
		t.Fatalf("upgrade satisfy: prev=%d want %d err=%v", prev, pdfID, err)
	}
	// Re-satisfying with the same item reports no previous file.
	prev, _ = d.SatisfyWishlistItem(id, epubID)
	if prev != 0 {
		t.Fatalf("same-item satisfy should report prev=0, got %d", prev)
	}

	// Deleting the library row sends the item back to missing.
	if err := d.UnlinkLibraryItemFromWishlist(epubID); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteItem(epubID); err != nil {
		t.Fatal(err)
	}
	w, _ = d.GetWishlistItem(id)
	if w.LibraryItemID != 0 || w.CurrentFormat != "" {
		t.Fatalf("unlink failed: %+v", w)
	}

	// A dangling link (row deleted without unlinking) also reads as no file.
	if err := d.LinkWishlistItem(id, 424242); err != nil {
		t.Fatal(err)
	}
	w, _ = d.GetWishlistItem(id)
	if w.LibraryItemID != 0 {
		t.Fatalf("dangling link should read as unlinked, got %d", w.LibraryItemID)
	}
	if err := d.ClearWishlistLink(id); err != nil {
		t.Fatal(err)
	}

	// Editable fields.
	off := false
	pid := int64(7)
	if err := d.UpdateWishlistItem(id, &off, &pid); err != nil {
		t.Fatal(err)
	}
	w, _ = d.GetWishlistItem(id)
	if w.Monitored || w.QualityProfileID != 7 {
		t.Fatalf("update failed: %+v", w)
	}
	if err := d.UpdateWishlistItem(id, nil, nil); err != nil {
		t.Fatalf("no-op update errored: %v", err)
	}
	if err := d.UpdateWishlistItem(9999, &off, nil); err == nil {
		t.Fatal("updating a missing item should error")
	}
	if _, err := d.GetWishlistItem(9999); err != sql.ErrNoRows {
		t.Fatalf("missing item should be ErrNoRows, got %v", err)
	}

	// Ordering: newest first, list carries the joined format.
	id2, _ := d.AddWishlistItemWithOptions(models.WishlistItem{Title: "Second", MediaType: "audiobook", Monitored: true, Source: "author:3"})
	items, _ := d.GetWishlist()
	if len(items) != 2 || items[0].ID != id2 || items[0].Source != "author:3" || items[0].MediaType != "audiobook" {
		t.Fatalf("list order/fields wrong: %+v", items)
	}
}

func TestJobs_WantedIDRoundTrip(t *testing.T) {
	d := newTestDB(t)
	job := &models.DownloadJob{ID: "j1", Title: "T", Source: "gutenberg", Status: "queued", WantedID: 42}
	if err := d.SaveJob(job); err != nil {
		t.Fatal(err)
	}
	got, err := d.GetJob("j1")
	if err != nil || got.WantedID != 42 {
		t.Fatalf("GetJob: %v %+v", err, got)
	}
	jobs, _ := d.GetJobs()
	if len(jobs) != 1 || jobs[0].WantedID != 42 {
		t.Fatalf("GetJobs lost wanted_id: %+v", jobs)
	}
}

func TestMonitoredAuthors_SeenWorks(t *testing.T) {
	d := newTestDB(t)
	id, err := d.AddMonitoredAuthorWithOptions("Ann Leckie", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	a, err := d.GetMonitoredAuthor(id)
	if err != nil {
		t.Fatal(err)
	}
	if a.AutoAdd || a.CheckIntervalDays != 7 || a.SeenWorks != 0 {
		t.Fatalf("author defaults: %+v", a)
	}

	works := []SeenWork{{WorkKey: "/works/OL1W", Title: "Ancillary Justice", Year: 2013}, {WorkKey: "/works/OL2W", Title: "Ancillary Sword", Year: 2014}, {WorkKey: ""}}
	if err := d.AddSeenWorks(id, works); err != nil {
		t.Fatal(err)
	}
	// Idempotent.
	if err := d.AddSeenWorks(id, works[:1]); err != nil {
		t.Fatal(err)
	}
	keys, _ := d.SeenWorkKeys(id)
	if len(keys) != 2 || !keys["/works/OL1W"] || !keys["/works/OL2W"] {
		t.Fatalf("seen keys: %v", keys)
	}
	a, _ = d.GetMonitoredAuthor(id)
	if a.SeenWorks != 2 {
		t.Fatalf("SeenWorks count = %d", a.SeenWorks)
	}

	days, auto := 3, true
	if err := d.UpdateMonitoredAuthor(id, &days, &auto); err != nil {
		t.Fatal(err)
	}
	a, _ = d.GetMonitoredAuthor(id)
	if a.CheckIntervalDays != 3 || !a.AutoAdd {
		t.Fatalf("update: %+v", a)
	}

	if err := d.DeleteMonitoredAuthor(id); err != nil {
		t.Fatal(err)
	}
	keys, _ = d.SeenWorkKeys(id)
	if len(keys) != 0 {
		t.Fatalf("seen works should be removed with the author, got %v", keys)
	}
	if err := d.AddSeenWorks(id, nil); err != nil {
		t.Fatalf("empty AddSeenWorks should be a no-op: %v", err)
	}
}
