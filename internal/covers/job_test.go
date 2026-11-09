package covers

import (
	"archive/zip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

type jobEnv struct {
	db    *db.DB
	store *Store
	job   *Job
	dir   string
	now   time.Time
	types []string
}

func newJobEnv(t *testing.T) *jobEnv {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	dir := filepath.Join(t.TempDir(), "covers")
	e := &jobEnv{
		db:    database,
		store: NewStore(dir),
		dir:   dir,
		now:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		types: []string{"ebook"},
	}
	e.job = NewJob(database, e.store, func() []string { return e.types })
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
	writeCoverEPUB(t, p, img)
	return p
}

func writeCoverEPUB(t *testing.T, p string, img []byte) {
	t.Helper()
	writeEPUB(t, p, map[string][]byte{
		"content.opf": []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata><meta name="cover" content="c"/></metadata><manifest><item id="c" href="cover.img"/></manifest></package>`),
		"cover.img":   img,
		// AddItem merges identical files, so make each fixture distinct.
		"path.txt": []byte(p),
	})
}

func TestJobUsesEmbeddedCover(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Embedded", "ebook", epubWithCover(t, jpegBytes))
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != fmt.Sprintf("%d.jpg", id) {
		t.Fatalf("cover = %q", got)
	}
}

func TestJobLeavesItemsWithoutEmbeddedCover(t *testing.T) {
	e := newJobEnv(t)
	noCover := filepath.Join(t.TempDir(), "n.epub")
	writeEPUB(t, noCover, map[string][]byte{"content.opf": []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata/><manifest/></package>`)})
	corrupt := filepath.Join(t.TempDir(), "c.epub")
	_ = os.WriteFile(corrupt, []byte("not a zip"), 0o644)
	paths := map[string]string{
		"NoCover": noCover, "Corrupt": corrupt, "Html": epubWithCover(t, []byte("<html>hi</html>")),
		"Missing": filepath.Join(t.TempDir(), "gone.epub"), "Pdf": "/x/book.pdf",
	}
	ids := map[string]int64{}
	for title, p := range paths {
		ids[title] = e.add(t, title, "ebook", p)
	}
	e.job.RunOnce(context.Background())
	for title, id := range ids {
		if got := e.cover(t, id); got != "" {
			t.Errorf("%s cover = %q", title, got)
		}
	}
	items, _ := e.db.ItemsNeedingCover(e.types, e.now.Add(-24*time.Hour), 10)
	if len(items) != 0 {
		t.Fatalf("items still due: %d", len(items))
	}
}

func TestJobMissRetriesAfterDelay(t *testing.T) {
	e := newJobEnv(t)
	p := filepath.Join(t.TempDir(), "later.epub")
	writeEPUB(t, p, map[string][]byte{"content.opf": []byte(`<package xmlns="http://www.idpf.org/2007/opf"><metadata/><manifest/></package>`)})
	id := e.add(t, "Miss", "ebook", p)
	e.job.RunOnce(context.Background())
	writeCoverEPUB(t, p, pngBytes)
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != "" {
		t.Fatalf("immediate retry: cover = %q", got)
	}
	e.now = e.now.Add(24*time.Hour + time.Second)
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != fmt.Sprintf("%d.png", id) {
		t.Fatalf("cover after delay = %q", got)
	}
}

func TestJobCoverIsFinal(t *testing.T) {
	e := newJobEnv(t)
	p := epubWithCover(t, pngBytes)
	id := e.add(t, "Final", "ebook", p)
	e.job.RunOnce(context.Background())
	writeCoverEPUB(t, p, jpegBytes)
	e.now = e.now.Add(365 * 24 * time.Hour)
	e.job.RunOnce(context.Background())
	if got := e.cover(t, id); got != fmt.Sprintf("%d.png", id) {
		t.Fatalf("cover = %q", got)
	}
}

func TestJobSkipsMediaTypes(t *testing.T) {
	e := newJobEnv(t)
	eb := e.add(t, "Ebook", "ebook", epubWithCover(t, pngBytes))
	au := e.add(t, "Audio", "audiobook", epubWithCover(t, pngBytes))
	mg := e.add(t, "Manga", "manga", epubWithCover(t, pngBytes))
	e.job.RunOnce(context.Background())
	if e.cover(t, eb) == "" || e.cover(t, au) != "" || e.cover(t, mg) != "" {
		t.Fatalf("covers: %q %q %q", e.cover(t, eb), e.cover(t, au), e.cover(t, mg))
	}
}

func TestJobUnwritableDir(t *testing.T) {
	e := newJobEnv(t)
	if err := os.WriteFile(e.dir, []byte("file"), 0o644); err != nil {
		t.Fatal(err)
	}
	id := e.add(t, "NoDir", "ebook", epubWithCover(t, pngBytes))
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
	keep := e.add(t, "Keep", "ebook", epubWithCover(t, pngBytes))
	del1 := e.add(t, "Del1", "ebook", epubWithCover(t, pngBytes))
	del2 := e.add(t, "Del2", "ebook", epubWithCover(t, pngBytes))
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
	id := e.add(t, "Restore", "ebook", epubWithCover(t, pngBytes))
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
	a := e.add(t, "A", "ebook", epubWithCover(t, pngBytes))
	b := e.add(t, "B", "ebook", epubWithCover(t, pngBytes))
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
	id := e.add(t, "Live", "ebook", epubWithCover(t, pngBytes))
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
}

// Mode 0300 fails the listing but still allows removal, so only the sweep's own
// check keeps files safe.
func TestJobSweepRemovesNothingWhenListFails(t *testing.T) {
	e := newJobEnv(t)
	id := e.add(t, "Live", "ebook", epubWithCover(t, pngBytes))
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
