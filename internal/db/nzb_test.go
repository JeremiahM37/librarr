package db

import (
	"testing"
)

func TestNZBJobLifecycle(t *testing.T) {
	database := newTestDB(t)

	if err := database.RecordNZBJob("nzo_1", "An Audiobook", "audiobook"); err != nil {
		t.Fatalf("RecordNZBJob: %v", err)
	}

	pending, err := database.PendingNZBJobs()
	if err != nil {
		t.Fatalf("PendingNZBJobs: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if pending[0].NzoID != "nzo_1" || pending[0].MediaType != "audiobook" {
		t.Fatalf("pending[0] = %+v, want nzo_1/audiobook", pending[0])
	}

	if err := database.MarkNZBJobImported("nzo_1"); err != nil {
		t.Fatalf("MarkNZBJobImported: %v", err)
	}
	pending, err = database.PendingNZBJobs()
	if err != nil {
		t.Fatalf("PendingNZBJobs after import: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after import = %d, want 0", len(pending))
	}
}

func TestRecordNZBJobIsIdempotentAndDoesNotResurrect(t *testing.T) {
	database := newTestDB(t)

	if err := database.RecordNZBJob("nzo_1", "Book", "ebook"); err != nil {
		t.Fatal(err)
	}
	if err := database.MarkNZBJobImported("nzo_1"); err != nil {
		t.Fatal(err)
	}
	// A duplicate submit of the same nzo_id must not flip imported back to 0.
	if err := database.RecordNZBJob("nzo_1", "Book", "ebook"); err != nil {
		t.Fatal(err)
	}
	pending, err := database.PendingNZBJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %d, want 0 (already-imported job must not resurrect)", len(pending))
	}
}

func TestRecordNZBJobDefaultsMediaType(t *testing.T) {
	database := newTestDB(t)

	if err := database.RecordNZBJob("nzo_1", "Book", ""); err != nil {
		t.Fatal(err)
	}
	pending, err := database.PendingNZBJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].MediaType != "ebook" {
		t.Fatalf("pending = %+v, want one ebook job", pending)
	}
}
