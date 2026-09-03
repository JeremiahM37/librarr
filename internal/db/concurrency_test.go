package db

import (
	"fmt"

	"github.com/JeremiahM37/librarr/internal/models"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestNew_AppliesWALAndBusyTimeout pins the connection pragmas. The DSN used
// to pass mattn-style keys that modernc.org/sqlite ignores, so the database
// ran in rollback-journal mode with no busy timeout and concurrent writes
// failed with "database is locked".
func TestNew_AppliesWALAndBusyTimeout(t *testing.T) {
	d := newTestDB(t)
	var mode string
	if err := d.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	var timeout int
	if err := d.db.QueryRow("PRAGMA busy_timeout").Scan(&timeout); err != nil {
		t.Fatal(err)
	}
	if timeout != 10000 {
		t.Fatalf("busy_timeout = %d, want 10000", timeout)
	}
	// The pragmas are per connection: exhaust the pool a little and re-check
	// on whatever connection answers.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var m string
			if err := d.db.QueryRow("PRAGMA journal_mode").Scan(&m); err != nil || m != "wal" {
				t.Errorf("pooled connection journal_mode = %q err=%v", m, err)
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentReadersAndWriters is the shape of the bug that surfaced in the
// wanted-list scheduler: one goroutine reading rows while another imports.
// Every write must succeed; before the DSN fix a fraction failed silently.
func TestConcurrentReadersAndWriters(t *testing.T) {
	d := newTestDB(t)
	dir := t.TempDir()
	const writers, perWriter = 6, 40
	var readersWG, writersWG sync.WaitGroup
	errs := make(chan error, writers*perWriter*4)
	stop := make(chan struct{})

	// Readers hammer the tables the scheduler reads mid-pass.
	for r := 0; r < 4; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := d.GetWishlist(); err != nil {
					errs <- fmt.Errorf("GetWishlist: %w", err)
				}
				if _, err := d.GetItems("", 50, 0); err != nil {
					errs <- fmt.Errorf("GetItems: %w", err)
				}
				_ = d.ResolveQualityProfile(0, "ebook")
			}
		}()
	}

	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				title := fmt.Sprintf("Book %d-%d", w, i)
				path := filepath.Join(dir, title+".epub")
				if err := os.WriteFile(path, []byte(title), 0o644); err != nil {
					errs <- err
					continue
				}
				id, err := d.AddWishlistItem(title, "", "ebook")
				if err != nil {
					errs <- fmt.Errorf("AddWishlistItem: %w", err)
					continue
				}
				outcome, err := d.AddItemWithOutcome(&models.LibraryItem{Title: title, FilePath: path, FileFormat: "epub", MediaType: "ebook"})
				if err != nil {
					errs <- fmt.Errorf("AddItemWithOutcome: %w", err)
					continue
				}
				if _, err := d.SatisfyWishlistItem(id, outcome.ID); err != nil {
					errs <- fmt.Errorf("SatisfyWishlistItem: %w", err)
				}
				if err := d.LogEvent("test", title, "", &outcome.ID, ""); err != nil {
					errs <- fmt.Errorf("LogEvent: %w", err)
				}
			}
		}(w)
	}
	writersWG.Wait()
	close(stop)
	readersWG.Wait()
	close(errs)

	failures := 0
	for err := range errs {
		failures++
		if failures <= 5 {
			t.Error(err)
		}
	}
	if failures > 5 {
		t.Errorf("... and %d more errors", failures-5)
	}
	n, _ := d.CountItems("ebook")
	if n != writers*perWriter {
		t.Fatalf("expected %d library rows, got %d", writers*perWriter, n)
	}
	items, _ := d.GetWishlist()
	for _, it := range items {
		if it.LibraryItemID == 0 {
			t.Fatalf("wanted row %q lost its link", it.Title)
		}
	}
}
