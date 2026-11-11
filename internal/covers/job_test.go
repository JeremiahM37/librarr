package covers

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/metadata"
	"github.com/JeremiahM37/librarr/internal/models"
)

type fakeResolver struct {
	calls []resolveCall
	url   string
	err   error
}

type resolveCall struct {
	kind          metadata.CoverKind
	title, author string
}

func (f *fakeResolver) Resolve(_ context.Context, kind metadata.CoverKind, title, author string) (string, error) {
	f.calls = append(f.calls, resolveCall{kind, title, author})
	return f.url, f.err
}

type jobEnv struct {
	db    *db.DB
	store *Store
	res   *fakeResolver
	job   *Job
	dir   string
	now   time.Time
	srv   *httptest.Server
	types []string
}

func newJobEnv(t *testing.T) *jobEnv {
	t.Helper()
	t.Setenv("LIBRARR_INSECURE_ALLOW_PRIVATE_URLS", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad.jpg" {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(pngBytes)
	}))
	t.Cleanup(srv.Close)
	database, err := db.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	dir := filepath.Join(t.TempDir(), "covers")
	e := &jobEnv{
		db:    database,
		store: NewStore(dir, nil, func() string { return srv.URL }),
		res:   &fakeResolver{url: srv.URL + "/c.jpg"},
		dir:   dir,
		now:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		srv:   srv,
		types: []string{"ebook", "audiobook"},
	}
	e.job = NewJob(database, e.store, e.res, func() []string { return e.types })
	e.job.delay = 0
	e.job.now = func() time.Time { return e.now }
	return e
}

func (e *jobEnv) add(t *testing.T, title, mediaType, path string) int64 {
	t.Helper()
	id, err := e.db.AddItem(&models.LibraryItem{
		Title: title, Author: "Author of " + title, FilePath: path, FileFormat: "epub",
		MediaType: mediaType, Source: "manual_import", SourceID: "src-" + title,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *jobEnv) cover(t *testing.T, id int64) string {
	t.Helper()
	files, err := e.db.CoverFiles()
	if err != nil {
		t.Fatal(err)
	}
	return files[id]
}

func writeEPUB(t *testing.T, path string, entries map[string][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for name, data := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(data)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func epubWithCover(t *testing.T, img []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "b.epub")
	writeEPUB(t, p, map[string][]byte{
		"content.opf": []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata><meta name="cover" content="c"/></metadata><manifest><item id="c" href="cover.img"/></manifest></package>`),
		"cover.img":   img,
	})
	return p
}

func TestJobUsesEmbeddedCover(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Embedded", "ebook", epubWithCover(t, jpegBytes))
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != fmt.Sprintf("%d.jpg", id) {
		t.Fatalf("cover = %q", got)
	}
	if len(e.res.calls) != 0 {
		t.Fatalf("resolver called: %v", e.res.calls)
	}
}

func TestJobFallsBackToLookup(t *testing.T) {
	e := newJobEnv(t)
	noCover := filepath.Join(t.TempDir(), "n.epub")
	writeEPUB(t, noCover, map[string][]byte{"content.opf": []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata/><manifest/></package>`)})
	corrupt := filepath.Join(t.TempDir(), "c.epub")
	_ = os.WriteFile(corrupt, []byte("not a zip"), 0o644)
	paths := map[string]string{
		"NoCover": noCover, "Corrupt": corrupt,
		"Missing": filepath.Join(t.TempDir(), "gone.epub"), "Pdf": "/x/book.pdf",
	}
	ids := map[string]int64{}
	for title, p := range paths {
		ids[title] = e.add(t, title, "ebook", p)
	}
	e.job.RunOnce(context.Background())
	if len(e.res.calls) != len(paths) {
		t.Fatalf("calls = %d", len(e.res.calls))
	}
	for _, c := range e.res.calls {
		if c.kind != metadata.CoverKindBook || c.author != "Author of "+c.title {
			t.Errorf("bad call %+v", c)
		}
	}
	for title, id := range ids {
		if got := e.cover(t, id); got != fmt.Sprintf("%d.png", id) {
			t.Errorf("%s cover = %q", title, got)
		}
	}
}

func TestJobRejectsNonImageEmbedded(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Html", "ebook", epubWithCover(t, []byte("<html>hi</html>")))
	e.job.RunOnce(context.Background())
	if len(e.res.calls) != 1 {
		t.Fatalf("calls = %d", len(e.res.calls))
	}
	if got := e.cover(t, id); got != fmt.Sprintf("%d.png", id) {
		t.Fatalf("cover = %q", got)
	}
}

func TestJobMissRetriesAfterDelay(t *testing.T) {
	e := newJobEnv(t)
	e.res.url = ""
	id := e.add(t, "Miss", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	if e.cover(t, id) != "" || len(e.res.calls) != 1 {
		t.Fatalf("cover %q calls %d", e.cover(t, id), len(e.res.calls))
	}
	e.job.RunOnce(context.Background())
	if len(e.res.calls) != 1 {
		t.Fatalf("immediate retry: %d calls", len(e.res.calls))
	}
	e.now = e.now.Add(24*time.Hour + time.Second)
	e.job.RunOnce(context.Background())
	if len(e.res.calls) != 2 {
		t.Fatalf("calls after delay = %d", len(e.res.calls))
	}
}

func TestJobFailureDoesNotLoop(t *testing.T) {
	e := newJobEnv(t)
	e.res.err = errors.New("boom")
	a := e.add(t, "ErrA", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	e.res.err = nil
	e.res.url = e.srv.URL + "/bad.jpg"
	b := e.add(t, "ErrB", "ebook", "/x/b.pdf")
	e.job.RunOnce(context.Background())
	if e.cover(t, a) != "" || e.cover(t, b) != "" {
		t.Fatal("unexpected cover")
	}
	if len(e.res.calls) != 2 {
		t.Fatalf("calls = %d", len(e.res.calls))
	}
	items, _ := e.db.ItemsNeedingCover(e.types, e.now.Add(-24*time.Hour), 10)
	if len(items) != 0 {
		t.Fatalf("items still due: %d", len(items))
	}
}

func TestJobCoverIsFinal(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Final", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	e.now = e.now.Add(365 * 24 * time.Hour)
	e.job.RunOnce(context.Background())
	if len(e.res.calls) != 1 || e.cover(t, id) == "" {
		t.Fatalf("calls %d cover %q", len(e.res.calls), e.cover(t, id))
	}
}

func TestJobSkipsMediaTypes(t *testing.T) {
	e := newJobEnv(t)
	e.types = []string{"ebook"}
	e.add(t, "Ebook", "ebook", "/x/a.pdf")
	au := e.add(t, "Audio", "audiobook", "/x/a.m4b")
	mg := e.add(t, "Manga", "manga", "/x/a.cbz")
	e.job.RunOnce(context.Background())
	if len(e.res.calls) != 1 || e.cover(t, au) != "" || e.cover(t, mg) != "" {
		t.Fatalf("calls %d", len(e.res.calls))
	}
}

func TestJobUnwritableDir(t *testing.T) {
	e := newJobEnv(t)
	if err := os.WriteFile(e.dir, []byte("file"), 0o644); err != nil {
		t.Fatal(err)
	}
	id := e.add(t, "NoDir", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	if e.cover(t, id) != "" {
		t.Fatal("cover set")
	}
	items, _ := e.db.ItemsNeedingCover(e.types, e.now.Add(-24*time.Hour), 10)
	if len(items) != 0 {
		t.Fatal("item not marked checked")
	}
}

func TestJobSweepsOrphans(t *testing.T) {
	e := newJobEnv(t)
	keep := e.add(t, "Keep", "ebook", "/x/a.pdf")
	del1 := e.add(t, "Del1", "ebook", "/x/b.pdf")
	del2 := e.add(t, "Del2", "ebook", "/x/c.pdf")
	e.job.RunOnce(context.Background())
	if err := e.db.DeleteItem(del1); err != nil {
		t.Fatal(err)
	}
	if err := e.db.DeleteItemBySourceID("src-Del2"); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(e.dir, ".DS_Store")
	_ = os.WriteFile(stray, []byte("x"), 0o644)
	e.job.RunOnce(context.Background())
	if ok, err := e.store.Exists(e.cover(t, keep)); !ok || err != nil {
		t.Fatalf("live file removed: %v", err)
	}
	for _, id := range []int64{del1, del2} {
		if ok, err := e.store.Exists(fmt.Sprintf("%d.png", id)); ok || err != nil {
			t.Fatalf("orphan %d stays: %v", id, err)
		}
	}
	if _, err := os.Stat(stray); err != nil {
		t.Fatal("unrecognised file removed")
	}
}

func TestJobRegeneratesMissingFiles(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Restore", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	file := e.cover(t, id)
	if err := e.store.Remove(file); err != nil {
		t.Fatal(err)
	}
	e.job.RunOnce(context.Background())
	if ok, err := e.store.Exists(file); !ok || err != nil {
		t.Fatalf("file not regenerated: %v", err)
	}
	if got := e.cover(t, id); got != file {
		t.Fatalf("not regenerated: %q", got)
	}
}

func TestJobStopsOnCancel(t *testing.T) {
	e := newJobEnv(t)
	e.job.delay = time.Hour
	a := e.add(t, "A", "ebook", "/x/a.pdf")
	b := e.add(t, "B", "ebook", "/x/b.pdf")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.job.RunOnce(ctx); close(done) }()
	for i := 0; i < 200 && e.cover(t, a)+e.cover(t, b) == ""; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce did not return")
	}
	n := 0
	for _, id := range []int64{a, b} {
		if e.cover(t, id) != "" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d items processed, want 1", n)
	}
	items, _ := e.db.ItemsNeedingCover(e.types, e.now.Add(-24*time.Hour), 10)
	if len(items) != 1 {
		t.Fatalf("%d items left unmarked, want 1", len(items))
	}
}

func TestJobStopsWhileCoolingDown(t *testing.T) {
	e := newJobEnv(t)
	e.res.err = fmt.Errorf("lookup: %w", metadata.ErrCoverCoolingDown)
	a := e.add(t, "A", "ebook", "/x/a.pdf")
	b := e.add(t, "B", "ebook", "/x/b.pdf")
	if err := os.MkdirAll(e.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(e.dir, "999.png")
	if err := os.WriteFile(orphan, pngBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	e.job.RunOnce(context.Background())

	if len(e.res.calls) != 1 {
		t.Fatalf("calls = %d, want the pass to stop after 1", len(e.res.calls))
	}
	items, _ := e.db.ItemsNeedingCover(e.types, e.now.Add(-time.Second), 10)
	if len(items) != 2 {
		t.Fatalf("%d items left unmarked, want 2", len(items))
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("the stopped pass swept: %v", err)
	}

	// The next pass carries on, with no 24-hour wait, and finishes normally.
	e.res.err = nil
	e.job.RunOnce(context.Background())
	if e.cover(t, a) == "" || e.cover(t, b) == "" {
		t.Fatalf("not resumed: %q %q", e.cover(t, a), e.cover(t, b))
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan not swept after a full pass: %v", err)
	}
}

// lockDir makes dir unusable with the given mode and restores it at cleanup, so
// the test's temp directory can still be removed.
func lockDir(t *testing.T, dir string, mode os.FileMode) (restore func()) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	restore = func() {
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(dir, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restore)
	return restore
}

// A directory that can't be read says nothing about whether covers are there.
func TestJobLeavesCoversWhenDirUnreadable(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Live", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	file := e.cover(t, id)
	if file == "" {
		t.Fatal("no cover to start with")
	}

	restore := lockDir(t, e.dir, 0)
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != file {
		t.Fatalf("row changed while the directory was unreadable: %q", got)
	}

	restore()
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != file {
		t.Fatalf("row changed after the directory came back: %q", got)
	}
	if _, err := os.Stat(filepath.Join(e.dir, file)); err != nil {
		t.Fatalf("cover file lost: %v", err)
	}
	if len(e.res.calls) != 1 {
		t.Fatalf("looked up again: %d calls", len(e.res.calls))
	}
}

// Mode 0300 fails the listing but still allows removal, so only the sweep's own
// check keeps files safe.
func TestJobSweepRemovesNothingWhenListFails(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Live", "ebook", "/x/a.pdf")
	e.job.RunOnce(context.Background())
	file := e.cover(t, id)
	orphan := filepath.Join(e.dir, "999.png")
	if err := os.WriteFile(orphan, pngBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	restore := lockDir(t, e.dir, 0o300)
	e.job.RunOnce(context.Background())
	restore()

	if got := e.cover(t, id); got != file {
		t.Fatalf("row changed: %q", got)
	}
	for _, f := range []string{file, "999.png"} {
		if _, err := os.Stat(filepath.Join(e.dir, f)); err != nil {
			t.Errorf("%s removed: %v", f, err)
		}
	}
}

// Only an image download pauses the pass. Embedded covers need no network, and
// lookups are paced by the resolver.
func TestJobPausesOnlyAfterDownloads(t *testing.T) {
	e := newJobEnv(t)
	e.job.delay = time.Hour
	e.res.url = ""
	var epubs []int64
	for i := 0; i < 3; i++ {
		p := filepath.Join(t.TempDir(), "b.epub")
		writeEPUB(t, p, map[string][]byte{
			"content.opf": []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata><meta name="cover" content="c"/></metadata><manifest><item id="c" href="cover.img"/></manifest></package>`),
			"cover.img":   pngBytes,
			"path.txt":    []byte(p), // AddItem merges identical files
		})
		epubs = append(epubs, e.add(t, fmt.Sprintf("Embedded %d", i), "ebook", p))
	}
	for i := 0; i < 3; i++ {
		e.add(t, fmt.Sprintf("Miss %d", i), "ebook", fmt.Sprintf("/x/%d.pdf", i))
	}

	done := make(chan struct{})
	go func() { e.job.RunOnce(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass paused without downloading anything")
	}
	for _, id := range epubs {
		if e.cover(t, id) == "" {
			t.Errorf("item %d has no cover", id)
		}
	}
	if len(e.res.calls) != 3 {
		t.Errorf("lookups = %d, want 3", len(e.res.calls))
	}
	items, _ := e.db.ItemsNeedingCover(e.types, e.now.Add(-24*time.Hour), 10)
	if len(items) != 0 {
		t.Errorf("%d items left unmarked", len(items))
	}
}
