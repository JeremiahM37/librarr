package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	d, err := New(path)
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestNew_CreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "deep", "test.db")
	d, err := New(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer d.Close()

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		t.Error("expected directory to be created")
	}
}

func TestLibraryItems_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("add and retrieve item", func(t *testing.T) {
		item := &models.LibraryItem{
			Title:     "The Great Gatsby",
			Author:    "F. Scott Fitzgerald",
			FilePath:  "/books/gatsby.epub",
			FileSize:  500000,
			FileFormat: "epub",
			MediaType: "ebook",
			Source:    "annas",
			SourceID:  "md5-abc123",
		}

		id, err := d.AddItem(item)
		if err != nil {
			t.Fatalf("AddItem failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}
	})

	t.Run("find by title", func(t *testing.T) {
		items, err := d.FindByTitle("The Great Gatsby")
		if err != nil {
			t.Fatalf("FindByTitle failed: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Author != "F. Scott Fitzgerald" {
			t.Errorf("expected author F. Scott Fitzgerald, got %s", items[0].Author)
		}
	})

	t.Run("find by title case insensitive", func(t *testing.T) {
		items, err := d.FindByTitle("the great gatsby")
		if err != nil {
			t.Fatalf("FindByTitle failed: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item (case insensitive), got %d", len(items))
		}
	})

	t.Run("has source id", func(t *testing.T) {
		if !d.HasSourceID("md5-abc123") {
			t.Error("expected HasSourceID to return true")
		}
		if d.HasSourceID("nonexistent") {
			t.Error("expected HasSourceID to return false for unknown ID")
		}
		if d.HasSourceID("") {
			t.Error("expected HasSourceID to return false for empty string")
		}
	})

	t.Run("count items", func(t *testing.T) {
		count, err := d.CountItems("")
		if err != nil {
			t.Fatalf("CountItems failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}

		count, err = d.CountItems("ebook")
		if err != nil {
			t.Fatalf("CountItems ebook failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 ebook, got %d", count)
		}

		count, err = d.CountItems("audiobook")
		if err != nil {
			t.Fatalf("CountItems audiobook failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 audiobooks, got %d", count)
		}
	})

	t.Run("get items paginated", func(t *testing.T) {
		items, err := d.GetItems("", 10, 0)
		if err != nil {
			t.Fatalf("GetItems failed: %v", err)
		}
		if len(items) != 1 {
			t.Errorf("expected 1 item, got %d", len(items))
		}
	})

	t.Run("delete item", func(t *testing.T) {
		items, _ := d.GetItems("", 10, 0)
		if len(items) == 0 {
			t.Fatal("no items to delete")
		}
		err := d.DeleteItem(items[0].ID)
		if err != nil {
			t.Fatalf("DeleteItem failed: %v", err)
		}

		count, _ := d.CountItems("")
		if count != 0 {
			t.Errorf("expected 0 items after delete, got %d", count)
		}
	})

	t.Run("delete nonexistent item", func(t *testing.T) {
		err := d.DeleteItem(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent item")
		}
	})
}

func TestDownloadJobs_CRUD(t *testing.T) {
	d := newTestDB(t)

	job := &models.DownloadJob{
		ID:        "test-job-1",
		Title:     "Test Book",
		Source:    "annas",
		Status:    "queued",
		URL:       "https://example.com",
		MD5:       "abc123",
		MediaType: "ebook",
		MaxRetries: 2,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	t.Run("save and get job", func(t *testing.T) {
		err := d.SaveJob(job)
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		got, err := d.GetJob("test-job-1")
		if err != nil {
			t.Fatalf("GetJob failed: %v", err)
		}
		if got.Title != "Test Book" {
			t.Errorf("expected title Test Book, got %s", got.Title)
		}
		if got.Source != "annas" {
			t.Errorf("expected source annas, got %s", got.Source)
		}
	})

	t.Run("update job status", func(t *testing.T) {
		err := d.UpdateJobStatus("test-job-1", "downloading", "50% complete", "")
		if err != nil {
			t.Fatalf("UpdateJobStatus failed: %v", err)
		}

		got, _ := d.GetJob("test-job-1")
		if got.Status != "downloading" {
			t.Errorf("expected status downloading, got %s", got.Status)
		}
		if got.Detail != "50% complete" {
			t.Errorf("expected detail '50%% complete', got %s", got.Detail)
		}
	})

	t.Run("get all jobs", func(t *testing.T) {
		jobs, err := d.GetJobs()
		if err != nil {
			t.Fatalf("GetJobs failed: %v", err)
		}
		if len(jobs) != 1 {
			t.Errorf("expected 1 job, got %d", len(jobs))
		}
	})

	t.Run("clear finished jobs", func(t *testing.T) {
		// Mark job as completed
		d.UpdateJobStatus("test-job-1", "completed", "Done", "")
		count, err := d.ClearFinishedJobs()
		if err != nil {
			t.Fatalf("ClearFinishedJobs failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 cleared, got %d", count)
		}

		jobs, _ := d.GetJobs()
		if len(jobs) != 0 {
			t.Errorf("expected 0 jobs after clear, got %d", len(jobs))
		}
	})

	t.Run("delete job", func(t *testing.T) {
		job2 := &models.DownloadJob{
			ID: "test-job-2", Title: "Another", Source: "gutenberg",
			Status: "queued", CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		d.SaveJob(job2)
		err := d.DeleteJob("test-job-2")
		if err != nil {
			t.Fatalf("DeleteJob failed: %v", err)
		}
	})

	t.Run("get nonexistent job", func(t *testing.T) {
		_, err := d.GetJob("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent job")
		}
	})
}

func TestDownloadJobs_StatusHistory(t *testing.T) {
	d := newTestDB(t)

	job := &models.DownloadJob{
		ID:        "history-job",
		Title:     "History Test",
		Source:    "annas",
		Status:    "queued",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		StatusHistory: []models.StatusTransition{
			{From: "queued", To: "downloading", Detail: "Starting", Timestamp: time.Now().Format(time.RFC3339)},
		},
	}

	if err := d.SaveJob(job); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	got, err := d.GetJob("history-job")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if len(got.StatusHistory) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(got.StatusHistory))
	}
	if got.StatusHistory[0].From != "queued" {
		t.Errorf("expected From=queued, got %s", got.StatusHistory[0].From)
	}
}

func TestWishlist_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("add and list", func(t *testing.T) {
		id, err := d.AddWishlistItem("Dune", "Frank Herbert", "ebook")
		if err != nil {
			t.Fatalf("AddWishlistItem failed: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive ID, got %d", id)
		}

		items, err := d.GetWishlist()
		if err != nil {
			t.Fatalf("GetWishlist failed: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Title != "Dune" {
			t.Errorf("expected title Dune, got %s", items[0].Title)
		}
		if items[0].Author != "Frank Herbert" {
			t.Errorf("expected author Frank Herbert, got %s", items[0].Author)
		}
	})

	t.Run("default media type", func(t *testing.T) {
		id, err := d.AddWishlistItem("Book", "Author", "")
		if err != nil {
			t.Fatalf("AddWishlistItem failed: %v", err)
		}
		_ = id

		items, _ := d.GetWishlist()
		// Find the one with empty media type input (should be "ebook")
		found := false
		for _, item := range items {
			if item.Title == "Book" && item.MediaType == "ebook" {
				found = true
			}
		}
		if !found {
			t.Error("expected default media type 'ebook'")
		}
	})

	t.Run("delete", func(t *testing.T) {
		items, _ := d.GetWishlist()
		if len(items) == 0 {
			t.Fatal("no items to delete")
		}
		err := d.DeleteWishlistItem(items[0].ID)
		if err != nil {
			t.Fatalf("DeleteWishlistItem failed: %v", err)
		}
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		err := d.DeleteWishlistItem(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent wishlist item")
		}
	})
}

func TestUsers_CRUD(t *testing.T) {
	d := newTestDB(t)

	t.Run("create and get user", func(t *testing.T) {
		id, err := d.CreateUser("testuser", "hashedpw", "admin")
		if err != nil {
			t.Fatalf("CreateUser failed: %v", err)
		}

		user, err := d.GetUser(id)
		if err != nil {
			t.Fatalf("GetUser failed: %v", err)
		}
		if user.Username != "testuser" {
			t.Errorf("expected username testuser, got %s", user.Username)
		}
		if user.Role != "admin" {
			t.Errorf("expected role admin, got %s", user.Role)
		}
	})

	t.Run("get by username case insensitive", func(t *testing.T) {
		user, err := d.GetUserByUsername("TESTUSER")
		if err != nil {
			t.Fatalf("GetUserByUsername failed: %v", err)
		}
		if user.Username != "testuser" {
			t.Errorf("expected testuser, got %s", user.Username)
		}
	})

	t.Run("list users", func(t *testing.T) {
		users, err := d.ListUsers()
		if err != nil {
			t.Fatalf("ListUsers failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user, got %d", len(users))
		}
	})

	t.Run("count users", func(t *testing.T) {
		count, err := d.CountUsers()
		if err != nil {
			t.Fatalf("CountUsers failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}
	})

	t.Run("update user", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.UpdateUser(users[0].ID, "testuser", "user")
		if err != nil {
			t.Fatalf("UpdateUser failed: %v", err)
		}
		user, _ := d.GetUser(users[0].ID)
		if user.Role != "user" {
			t.Errorf("expected role user, got %s", user.Role)
		}
	})

	t.Run("update password", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.UpdateUserPassword(users[0].ID, "newhash")
		if err != nil {
			t.Fatalf("UpdateUserPassword failed: %v", err)
		}
		user, _ := d.GetUser(users[0].ID)
		if user.PasswordHash != "newhash" {
			t.Errorf("expected password hash updated")
		}
	})

	t.Run("update last login", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.UpdateLastLogin(users[0].ID)
		if err != nil {
			t.Fatalf("UpdateLastLogin failed: %v", err)
		}
	})

	t.Run("TOTP flow", func(t *testing.T) {
		users, _ := d.ListUsers()
		uid := users[0].ID

		err := d.SetTOTPSecret(uid, "JBSWY3DPEHPK3PXP")
		if err != nil {
			t.Fatalf("SetTOTPSecret failed: %v", err)
		}

		err = d.EnableTOTP(uid)
		if err != nil {
			t.Fatalf("EnableTOTP failed: %v", err)
		}

		user, _ := d.GetUser(uid)
		if !user.TOTPEnabled {
			t.Error("expected TOTP to be enabled")
		}
		if user.TOTPSecret != "JBSWY3DPEHPK3PXP" {
			t.Errorf("expected TOTP secret, got %s", user.TOTPSecret)
		}

		err = d.DisableTOTP(uid)
		if err != nil {
			t.Fatalf("DisableTOTP failed: %v", err)
		}

		user, _ = d.GetUser(uid)
		if user.TOTPEnabled {
			t.Error("expected TOTP to be disabled")
		}
	})

	t.Run("backup codes", func(t *testing.T) {
		users, _ := d.ListUsers()
		uid := users[0].ID

		hashes := []string{"hash1", "hash2", "hash3"}
		err := d.SaveBackupCodes(uid, hashes)
		if err != nil {
			t.Fatalf("SaveBackupCodes failed: %v", err)
		}

		// Use a backup code
		used, err := d.UseBackupCode(uid, "hash1")
		if err != nil {
			t.Fatalf("UseBackupCode failed: %v", err)
		}
		if !used {
			t.Error("expected backup code to be used")
		}

		// Try to use same code again
		used, _ = d.UseBackupCode(uid, "hash1")
		if used {
			t.Error("expected already-used code to fail")
		}

		// Use nonexistent code
		used, _ = d.UseBackupCode(uid, "nonexistent")
		if used {
			t.Error("expected nonexistent code to fail")
		}
	})

	t.Run("delete user", func(t *testing.T) {
		users, _ := d.ListUsers()
		err := d.DeleteUser(users[0].ID)
		if err != nil {
			t.Fatalf("DeleteUser failed: %v", err)
		}

		count, _ := d.CountUsers()
		if count != 0 {
			t.Errorf("expected 0 users after delete, got %d", count)
		}
	})

	t.Run("delete nonexistent user", func(t *testing.T) {
		err := d.DeleteUser(99999)
		if err == nil {
			t.Error("expected error deleting nonexistent user")
		}
	})

	t.Run("duplicate username", func(t *testing.T) {
		d.CreateUser("dup", "hash", "user")
		_, err := d.CreateUser("dup", "hash2", "user")
		if err == nil {
			t.Error("expected error on duplicate username")
		}
	})
}

func TestActivityLog(t *testing.T) {
	d := newTestDB(t)

	t.Run("log and retrieve events", func(t *testing.T) {
		err := d.LogEvent("download_complete", "Test Book", "Downloaded successfully", nil, "job-1")
		if err != nil {
			t.Fatalf("LogEvent failed: %v", err)
		}

		events, err := d.GetActivity(10, 0)
		if err != nil {
			t.Fatalf("GetActivity failed: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
		if events[0].EventType != "download_complete" {
			t.Errorf("expected event type download_complete, got %s", events[0].EventType)
		}
		if events[0].Title != "Test Book" {
			t.Errorf("expected title Test Book, got %s", events[0].Title)
		}
	})

	t.Run("count activity", func(t *testing.T) {
		count, err := d.CountActivity()
		if err != nil {
			t.Fatalf("CountActivity failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}
	})
}

func TestGetStats(t *testing.T) {
	d := newTestDB(t)

	d.AddItem(&models.LibraryItem{Title: "Book 1", MediaType: "ebook", Source: "annas"})
	d.AddItem(&models.LibraryItem{Title: "Book 2", MediaType: "ebook", Source: "annas"})
	d.AddItem(&models.LibraryItem{Title: "Audiobook 1", MediaType: "audiobook", Source: "librivox"})

	stats, err := d.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats["total_items"].(int) != 3 {
		t.Errorf("expected total 3, got %v", stats["total_items"])
	}
	if stats["ebooks"].(int) != 2 {
		t.Errorf("expected 2 ebooks, got %v", stats["ebooks"])
	}
	if stats["audiobooks"].(int) != 1 {
		t.Errorf("expected 1 audiobook, got %v", stats["audiobooks"])
	}
}

func TestItemToJSON(t *testing.T) {
	item := models.LibraryItem{
		ID:       1,
		Title:    "Test",
		Author:   "Author",
		Metadata: `{"key": "value"}`,
		AddedAt:  time.Now(),
	}

	m := ItemToJSON(item)
	if m["title"] != "Test" {
		t.Errorf("expected title Test, got %v", m["title"])
	}
	if m["author"] != "Author" {
		t.Errorf("expected author Author, got %v", m["author"])
	}
	if m["metadata"] == nil {
		t.Error("expected metadata to be parsed")
	}

	// Test with empty metadata
	item2 := models.LibraryItem{Title: "No Meta", Metadata: "{}", AddedAt: time.Now()}
	m2 := ItemToJSON(item2)
	if _, ok := m2["metadata"]; ok {
		t.Error("expected no metadata key for empty JSON")
	}
}
