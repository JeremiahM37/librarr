package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/quality"
	"github.com/JeremiahM37/librarr/internal/webhook"
)

// fakeSearcher returns canned results per query substring and counts calls.
type fakeSearcher struct {
	mu      sync.Mutex
	results map[string][]models.SearchResult // keyed by wishlist title
	calls   []string
}

func (f *fakeSearcher) SearchWithAuthor(_ context.Context, tab, query, author string) ([]models.SearchResult, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, tab+"|"+query+"|"+author)
	for title, rs := range f.results {
		if strings.Contains(query, title) {
			return rs, 0
		}
	}
	return nil, 0
}

func (f *fakeSearcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type grab struct {
	kind     string
	url      string
	title    string
	wantedID int64
	hash     string
}

// fakeDownloader records grabs and can be made to fail. Like the real
// manager it persists the job row before returning, which is what lets the
// scheduler tell an in-flight grab from a cleared one.
type fakeDownloader struct {
	mu    sync.Mutex
	db    *db.DB
	grabs []grab
	fail  error
	seq   int
}

func (f *fakeDownloader) StartAnnasDownloadFor(md5, title string, wantedID int64) (*models.DownloadJob, error) {
	return f.job("annas", md5, title, wantedID)
}

func (f *fakeDownloader) StartDirectDownloadFor(fileURL, title, _, _, _ string, wantedID int64) (*models.DownloadJob, error) {
	return f.job("direct", fileURL, title, wantedID)
}

func (f *fakeDownloader) StartTorrentDownload(torrentURL, title, _, _, expectedInfoHash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.grabs = append(f.grabs, grab{kind: "torrent", url: torrentURL, title: title, hash: expectedInfoHash})
	return nil
}

func (f *fakeDownloader) job(kind, url, title string, wantedID int64) (*models.DownloadJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return nil, f.fail
	}
	f.seq++
	f.grabs = append(f.grabs, grab{kind: kind, url: url, title: title, wantedID: wantedID})
	job := &models.DownloadJob{ID: "job-" + itoa(int64(f.seq)), Title: title, WantedID: wantedID, Status: "downloading"}
	if f.db != nil {
		_ = f.db.SaveJob(job)
	}
	return job, nil
}

func (f *fakeDownloader) last() grab {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.grabs) == 0 {
		return grab{}
	}
	return f.grabs[len(f.grabs)-1]
}

func (f *fakeDownloader) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.grabs)
}

type schedFixture struct {
	t     *testing.T
	cfg   *config.Config
	db    *db.DB
	s     *Scheduler
	srch  *fakeSearcher
	dl    *fakeDownloader
	slept []time.Duration
}

func newSchedFixture(t *testing.T) *schedFixture {
	t.Helper()
	f := &schedFixture{t: t}
	f.cfg = &config.Config{
		SchedulerEnabled:          true,
		SchedulerAutoDownload:     true,
		SchedulerMinScore:         70,
		SchedulerItemDelaySeconds: 5,
		AutoUpgradeEnabled:        true,
	}
	f.db = newTestDB(t)
	f.srch = &fakeSearcher{results: map[string][]models.SearchResult{}}
	f.dl = &fakeDownloader{db: f.db}
	f.s = NewScheduler(f.cfg, f.db, nil, nil, webhook.NewSender())
	f.s.searchMgr = f.srch
	f.s.downloadMgr = f.dl
	f.s.sleep = func(_ context.Context, d time.Duration) bool { f.slept = append(f.slept, d); return true }
	return f
}

func (f *schedFixture) want(title, author, mediaType string) int64 {
	id, err := f.db.AddWishlistItem(title, author, mediaType)
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *schedFixture) item(id int64) models.WishlistItem {
	it, err := f.db.GetWishlistItem(id)
	if err != nil {
		f.t.Fatal(err)
	}
	return *it
}

// addFile puts a library row with a real file behind a wanted item.
func (f *schedFixture) addFile(wantedID int64, title, format string) int64 {
	dir := f.t.TempDir()
	path := filepath.Join(dir, title+"."+format)
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		f.t.Fatal(err)
	}
	itemID, err := f.db.AddItem(&models.LibraryItem{Title: title, FilePath: path, FileFormat: format, MediaType: "ebook"})
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.db.SatisfyWishlistItem(wantedID, itemID); err != nil {
		f.t.Fatal(err)
	}
	return itemID
}

func res(title, source, format string, score float64, url string) models.SearchResult {
	return models.SearchResult{Title: title, Source: source, Format: format, Score: score, DownloadURL: url}
}

func TestNewScheduler_TypedNilsStayNil(t *testing.T) {
	s := NewScheduler(&config.Config{}, newTestDB(t), nil, nil, nil)
	if s.searchMgr != nil || s.downloadMgr != nil {
		t.Fatal("nil managers must not become non-nil interfaces")
	}
	out, err := s.SearchItem(context.Background(), 1, true)
	if err == nil {
		t.Fatalf("missing item should error, got %+v", out)
	}
}

func TestRun_GrabsMissingItemWithBestFormat(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Dune", "Frank Herbert", "ebook")
	f.srch.results["Dune"] = []models.SearchResult{
		res("Dune [PDF]", "src", "pdf", 95, "http://x/dune.pdf"),
		res("Dune", "src", "epub", 80, "http://x/dune.epub"),
		res("Dune deluxe", "src", "cbz", 99, "http://x/dune.cbz"), // not in profile
		res("Dune lowscore", "src", "epub", 10, "http://x/low.epub"),
	}

	stats := f.s.RunCtx(context.Background())
	if stats.Scanned != 1 || stats.Searched != 1 || stats.Matched != 1 || stats.Grabbed != 1 || stats.Upgrades != 0 {
		t.Fatalf("stats: %+v", stats)
	}
	g := f.dl.last()
	if g.kind != "direct" || g.url != "http://x/dune.epub" || g.wantedID != id {
		t.Fatalf("grabbed %+v, want the epub for wanted %d", g, id)
	}
	it := f.item(id)
	if it.ActiveJobID != "job-1" || !strings.Contains(it.LastResult, "grabbed EPUB") || it.LastSearched.IsZero() {
		t.Fatalf("row after grab: %+v", it)
	}
	if len(f.slept) != 0 {
		t.Fatalf("a single item must not sleep, slept %v", f.slept)
	}

	// Second pass: the grab is in flight, so it is skipped without searching.
	before := f.srch.callCount()
	stats = f.s.RunCtx(context.Background())
	if stats.Skipped != 1 || f.srch.callCount() != before {
		t.Fatalf("in-flight item should be skipped: %+v (calls %d->%d)", stats, before, f.srch.callCount())
	}
}

func TestRun_UpgradesOnlyBelowCutoff(t *testing.T) {
	f := newSchedFixture(t)
	pdfID := f.want("Have PDF", "", "ebook")
	f.addFile(pdfID, "Have PDF", "pdf")
	epubID := f.want("Have EPUB", "", "ebook")
	f.addFile(epubID, "Have EPUB", "epub")
	mobiID := f.want("Have MOBI", "", "ebook")
	f.addFile(mobiID, "Have MOBI", "mobi")

	f.srch.results["Have PDF"] = []models.SearchResult{res("Have PDF", "s", "epub", 90, "http://x/a.epub")}
	f.srch.results["Have EPUB"] = []models.SearchResult{res("Have EPUB", "s", "azw3", 90, "http://x/b.azw3")}
	f.srch.results["Have MOBI"] = []models.SearchResult{res("Have MOBI", "s", "pdf", 90, "http://x/c.pdf")}

	stats := f.s.RunCtx(context.Background())
	if stats.Scanned != 3 || stats.Searched != 2 || stats.Skipped != 1 || stats.Grabbed != 1 || stats.Upgrades != 1 {
		t.Fatalf("stats: %+v", stats)
	}
	if g := f.dl.last(); g.wantedID != pdfID || g.url != "http://x/a.epub" {
		t.Fatalf("expected PDF→EPUB upgrade grab, got %+v", g)
	}
	// The EPUB item is skipped (cutoff met) — it is not even searched.
	if it := f.item(epubID); !it.LastSearched.IsZero() || it.LastResult != "" {
		t.Fatalf("epub item should be skipped for cutoff, not searched: %+v", it)
	}
	skippedReason := ""
	for _, o := range f.s.Status()["recent"].([]ItemOutcome) {
		if o.WantedID == epubID {
			skippedReason = o.Reason
		}
	}
	if !strings.Contains(skippedReason, "cutoff met") {
		t.Fatalf("skip reason should mention the cutoff, got %q", skippedReason)
	}
	if it := f.item(mobiID); !strings.Contains(it.LastResult, "not an upgrade") {
		t.Fatalf("mobi item: PDF offered must be rejected as not an upgrade, got %q", it.LastResult)
	}
	// Two items were actually searched, so exactly one polite pause.
	if len(f.slept) != 1 || f.slept[0] != 5*time.Second {
		t.Fatalf("expected one 5s pause between searched items, got %v", f.slept)
	}
}

func TestRun_GlobalUpgradeSwitchAndProfileFlag(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Locked", "", "ebook")
	f.addFile(id, "Locked", "pdf")
	f.srch.results["Locked"] = []models.SearchResult{res("Locked", "s", "epub", 90, "http://x/l.epub")}

	f.cfg.AutoUpgradeEnabled = false
	stats := f.s.RunCtx(context.Background())
	if stats.Skipped != 1 || f.dl.count() != 0 {
		t.Fatalf("global switch off must skip: %+v grabs=%d", stats, f.dl.count())
	}

	f.cfg.AutoUpgradeEnabled = true
	// Custom profile with upgrades off.
	pid, _ := f.db.CreateQualityProfile(&db.QualityProfile{Name: "No upgrades", FormatRanking: []string{"epub", "pdf"}, UpgradeAllowed: false})
	_ = f.db.UpdateWishlistItem(id, nil, &pid)
	stats = f.s.RunCtx(context.Background())
	if stats.Skipped != 1 || f.dl.count() != 0 {
		t.Fatalf("profile with upgrades off must skip: %+v", stats)
	}

	// Default profile again: upgrade happens.
	zero := int64(0)
	_ = f.db.UpdateWishlistItem(id, nil, &zero)
	stats = f.s.RunCtx(context.Background())
	if stats.Upgrades != 1 || f.dl.count() != 1 {
		t.Fatalf("default profile should upgrade: %+v", stats)
	}
}

func TestRun_UnmonitoredAndAutoDownloadOff(t *testing.T) {
	f := newSchedFixture(t)
	off := false
	unmon := f.want("Ignored", "", "ebook")
	_ = f.db.UpdateWishlistItem(unmon, &off, nil)
	id := f.want("Manual", "", "ebook")
	f.srch.results["Manual"] = []models.SearchResult{res("Manual", "s", "epub", 90, "http://x/m.epub")}

	f.cfg.SchedulerAutoDownload = false
	stats := f.s.RunCtx(context.Background())
	if stats.Skipped != 1 || stats.Matched != 1 || stats.Grabbed != 0 || f.dl.count() != 0 {
		t.Fatalf("auto-download off: %+v", stats)
	}
	it := f.item(id)
	if it.ActiveJobID != "" || !strings.Contains(it.LastResult, "Manual (EPUB") {
		t.Fatalf("matched-but-not-grabbed row: %+v", it)
	}
	if !strings.Contains(f.item(unmon).LastResult+"|", "|") { // never searched: last_result stays empty
		t.Fatal("unreachable")
	}
	if f.item(unmon).LastResult != "" {
		t.Fatalf("unmonitored item must not be searched, got %q", f.item(unmon).LastResult)
	}
}

func TestRun_NoResultsAndBelowScore(t *testing.T) {
	f := newSchedFixture(t)
	none := f.want("Nothing", "", "ebook")
	low := f.want("Weak", "", "ebook")
	f.srch.results["Weak"] = []models.SearchResult{res("Weak", "s", "epub", 40, "http://x/w.epub")}
	stats := f.s.RunCtx(context.Background())
	if stats.Searched != 2 || stats.Matched != 0 {
		t.Fatalf("stats: %+v", stats)
	}
	if f.item(none).LastResult != "no results" {
		t.Fatalf("no-results row: %q", f.item(none).LastResult)
	}
	if got := f.item(low).LastResult; !strings.Contains(got, "none scored 70") {
		t.Fatalf("below-score row: %q", got)
	}
}

func TestRun_UnknownFormatIsNeverGrabbed(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Mystery", "", "ebook")
	f.srch.results["Mystery"] = []models.SearchResult{
		{Title: "Mystery retail", Source: "prowlarr", Score: 95, MagnetURL: "magnet:?xt=urn:btih:abc"},
	}
	f.s.RunCtx(context.Background())
	if f.dl.count() != 0 {
		t.Fatal("a release with no detectable format must not be grabbed automatically")
	}
	if got := f.item(id).LastResult; !strings.Contains(got, "no format detected") {
		t.Fatalf("reason: %q", got)
	}
	// Format in the title is enough.
	f.srch.results["Mystery"] = []models.SearchResult{
		{Title: "Mystery retail [EPUB]", Source: "prowlarr", Score: 95, MagnetURL: "magnet:?xt=urn:btih:ABCDEF", InfoHash: "ABCDEF"},
	}
	f.s.RunCtx(context.Background())
	if g := f.dl.last(); g.kind != "torrent" || g.hash != "ABCDEF" {
		t.Fatalf("expected torrent grab, got %+v", g)
	}
	if it := f.item(id); it.ActiveJobID != "torrent:abcdef" {
		t.Fatalf("torrent grab should be tracked by hash: %+v", it)
	}
}

func TestRun_GrabFailureIsReportedAndRetriedNextPass(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Flaky", "", "ebook")
	f.srch.results["Flaky"] = []models.SearchResult{res("Flaky", "s", "epub", 90, "http://x/f.epub")}
	f.dl.fail = errors.New("client down")
	stats := f.s.RunCtx(context.Background())
	if stats.Errors != 1 || stats.Grabbed != 0 {
		t.Fatalf("stats: %+v", stats)
	}
	if it := f.item(id); it.ActiveJobID != "" || !strings.Contains(it.LastResult, "grab failed: client down") {
		t.Fatalf("row: %+v", it)
	}
	f.dl.fail = nil
	stats = f.s.RunCtx(context.Background())
	if stats.Grabbed != 1 {
		t.Fatalf("should retry next pass: %+v", stats)
	}
}

func TestRun_StaleJobMarkerIsReleased(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Stale", "", "ebook")
	f.srch.results["Stale"] = []models.SearchResult{res("Stale", "s", "epub", 90, "http://x/s.epub")}
	// A marker pointing at a dead-lettered job must not block forever.
	_ = f.db.SaveJob(&models.DownloadJob{ID: "dead", Status: "dead_letter", WantedID: id})
	_ = f.db.SetWishlistActiveJob(id, "dead")
	stats := f.s.RunCtx(context.Background())
	if stats.Grabbed != 1 {
		t.Fatalf("dead job marker should be released and the item regrabbed: %+v", stats)
	}
	// A marker for a job that still runs blocks.
	_ = f.db.SaveJob(&models.DownloadJob{ID: "live", Status: "downloading", WantedID: id})
	_ = f.db.SetWishlistActiveJob(id, "live")
	stats = f.s.RunCtx(context.Background())
	if stats.Skipped != 1 {
		t.Fatalf("live job marker should block: %+v", stats)
	}
	// A torrent marker blocks until the watcher clears it.
	_ = f.db.SetWishlistActiveJob(id, "torrent:abc")
	if stats = f.s.RunCtx(context.Background()); stats.Skipped != 1 {
		t.Fatalf("torrent marker should block: %+v", stats)
	}
}

func TestRun_ReconcileLinksExistingLibraryFiles(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Already Here", "Some Author", "ebook")
	path := filepath.Join(t.TempDir(), "here.epub")
	_ = os.WriteFile(path, []byte("x"), 0o644)
	itemID, _ := f.db.AddItem(&models.LibraryItem{Title: "Already Here", Author: "Some Author", FilePath: path, FileFormat: "epub", MediaType: "ebook"})
	f.srch.results["Already Here"] = []models.SearchResult{res("Already Here", "s", "epub", 99, "http://x/h.epub")}

	stats := f.s.RunCtx(context.Background())
	if stats.Linked != 1 || stats.Grabbed != 0 || stats.Skipped != 1 {
		t.Fatalf("existing file should be linked, not regrabbed: %+v", stats)
	}
	if it := f.item(id); it.LibraryItemID != itemID || it.CurrentFormat != "epub" {
		t.Fatalf("row: %+v", it)
	}
	// Wrong author does not link.
	other := f.want("Already Here", "Different Person", "ebook")
	f.s.RunCtx(context.Background())
	if it := f.item(other); it.LibraryItemID != 0 {
		t.Fatalf("author mismatch must not link: %+v", it)
	}
}

func TestSearchItem_DryRunExplainsEveryCandidate(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Explain", "", "ebook")
	f.addFile(id, "Explain", "mobi")
	f.srch.results["Explain"] = []models.SearchResult{
		res("Explain", "a", "epub", 90, "http://x/1.epub"),
		res("Explain", "b", "pdf", 95, "http://x/2.pdf"),
		res("Explain", "c", "mobi", 85, "http://x/3.mobi"),
	}
	out, err := f.s.SearchItem(context.Background(), id, true)
	if err != nil {
		t.Fatal(err)
	}
	if out.Action != "matched" || !strings.Contains(out.Candidate, "EPUB") || len(out.Decisions) != 3 || f.dl.count() != 0 {
		t.Fatalf("dry run: %+v", out)
	}
	byFormat := map[string]CandidateSummary{}
	for _, d := range out.Decisions {
		byFormat[d.Format] = d
	}
	if !byFormat["epub"].Accepted || !byFormat["epub"].Upgrade {
		t.Fatalf("epub should be an accepted upgrade: %+v", byFormat["epub"])
	}
	if byFormat["pdf"].Accepted || !strings.Contains(byFormat["pdf"].Reason, "not an upgrade") {
		t.Fatalf("pdf: %+v", byFormat["pdf"])
	}
	if byFormat["mobi"].Accepted {
		t.Fatalf("same format is not an upgrade: %+v", byFormat["mobi"])
	}
	if out.State != quality.StateUpgrade {
		t.Fatalf("state = %q", out.State)
	}

	// Manual search of an unmonitored item still works (forced), and a real
	// (non-dry) manual search grabs even when auto-download is off.
	off := false
	_ = f.db.UpdateWishlistItem(id, &off, nil)
	f.cfg.SchedulerAutoDownload = false
	out, _ = f.s.SearchItem(context.Background(), id, false)
	if out.Action != "upgrade" || f.dl.count() != 1 || f.dl.last().wantedID != id {
		t.Fatalf("manual search should grab: %+v", out)
	}
	// Recent outcomes surface in Status without the per-candidate detail.
	st := f.s.Status()
	recent := st["recent"].([]ItemOutcome)
	if len(recent) == 0 || recent[0].WantedID != id || recent[0].Decisions != nil {
		t.Fatalf("status recent: %+v", recent)
	}
}

func TestRunCtx_StopsOnCancel(t *testing.T) {
	f := newSchedFixture(t)
	for _, n := range []string{"A1", "A2", "A3"} {
		f.want(n, "", "ebook")
		f.srch.results[n] = []models.SearchResult{res(n, "s", "epub", 90, "http://x/"+n)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.s.sleep = func(_ context.Context, _ time.Duration) bool { cancel(); return false }
	stats := f.s.RunCtx(ctx)
	if stats.Searched != 1 || f.dl.count() != 1 {
		t.Fatalf("cancel during the pause should stop the pass: %+v", stats)
	}
}

func TestRun_DoesNotOverlap(t *testing.T) {
	f := newSchedFixture(t)
	f.s.mu.Lock()
	f.s.running = true
	f.s.mu.Unlock()
	if stats := f.s.RunCtx(context.Background()); stats.Scanned != 0 {
		t.Fatalf("second concurrent run must be a no-op, got %+v", stats)
	}
}

func TestRun_BlocklistedReleasesNeverCompete(t *testing.T) {
	f := newSchedFixture(t)
	id := f.want("Blocked", "", "ebook")
	f.srch.results["Blocked"] = []models.SearchResult{
		res("Blocked", "s", "epub", 95, "http://x/bad.epub"),
		res("Blocked", "s", "pdf", 90, "http://x/ok.pdf"),
	}
	_, _ = f.db.AddBlocklistEntry("Blocked", "s", "http://x/bad.epub", "", "wanted: delivered PDF again")
	f.s.RunCtx(context.Background())
	if g := f.dl.last(); g.url != "http://x/ok.pdf" {
		t.Fatalf("blocklisted epub must be skipped in favour of the pdf, got %+v", g)
	}
	// Everything blocklisted: the reason says so.
	_ = f.db.SetWishlistActiveJob(id, "")
	_, _ = f.db.AddBlocklistEntry("Blocked", "s", "http://x/ok.pdf", "", "manual")
	f.s.RunCtx(context.Background())
	if got := f.item(id).LastResult; !strings.Contains(got, "2 blocklisted") {
		t.Fatalf("reason should count blocklisted releases, got %q", got)
	}
	// Info hash and Anna's MD5 keys are honoured too.
	hid := f.want("Hashed", "", "ebook")
	f.srch.results["Hashed"] = []models.SearchResult{
		{Title: "Hashed [EPUB]", Source: "p", Score: 95, MagnetURL: "magnet:?xt=urn:btih:FEED", InfoHash: "FEED"},
		{Title: "Hashed", Source: "annas", Format: "epub", Score: 80, MD5: "ABC"},
	}
	_, _ = f.db.AddBlocklistEntry("Hashed", "p", "", "feed", "manual")
	_, _ = f.db.AddBlocklistEntry("Hashed", "annas", "annas:md5:abc", "", "manual")
	before := f.dl.count()
	f.s.RunCtx(context.Background())
	if f.dl.count() != before {
		t.Fatalf("hash/md5 blocklist entries ignored: %+v", f.dl.last())
	}
	if got := f.item(hid).LastResult; !strings.Contains(got, "2 blocklisted") {
		t.Fatalf("reason: %q", got)
	}
}
