package db

import (
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
)

func TestLibraryDeleteRollsBackWantedFailure(t *testing.T) {
	for _, bySource := range []bool{false, true} {
		database := newTestDB(t)
		itemID, err := database.AddItem(&models.LibraryItem{Title: "Keep", MediaType: "manga", SourceID: "provider-id", FilePath: "/fixture/keep.cbz"})
		if err != nil {
			t.Fatal(err)
		}
		wantedID, err := database.AddWishlistItem("Keep", "", "manga")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.SatisfyWishlistItem(wantedID, itemID); err != nil {
			t.Fatal(err)
		}
		tagID, err := database.CreateTag("Fixture tag", "blue")
		if err != nil {
			t.Fatal(err)
		}
		if err := database.AddItemTag(itemID, tagID); err != nil {
			t.Fatal(err)
		}
		if _, err := database.db.Exec(`CREATE TRIGGER prevent_wanted_update BEFORE UPDATE ON wishlist BEGIN SELECT RAISE(ABORT, 'fixture failure'); END`); err != nil {
			t.Fatal(err)
		}
		if bySource {
			err = database.DeleteItemBySourceID("provider-id")
		} else {
			err = database.DeleteItem(itemID)
		}
		if err == nil {
			t.Fatal("delete must report the failed transaction")
		}
		if _, err := database.GetItem(itemID); err != nil {
			t.Fatalf("failed deletion lost item: %v", err)
		}
		tags, err := database.GetItemTags(itemID)
		if err != nil || len(tags) != 1 {
			t.Fatalf("failed deletion lost tags: %+v, %v", tags, err)
		}
		wanted, err := database.GetWishlistItem(wantedID)
		if err != nil || wanted.LibraryItemID != itemID {
			t.Fatalf("failed deletion changed wanted row: %+v, %v", wanted, err)
		}
		if _, err := database.db.Exec("DROP TRIGGER prevent_wanted_update"); err != nil {
			t.Fatal(err)
		}
		if bySource {
			err = database.DeleteItemBySourceID("provider-id")
		} else {
			err = database.DeleteItem(itemID)
		}
		if err != nil {
			t.Fatal(err)
		}
		wanted, err = database.GetWishlistItem(wantedID)
		if err != nil || wanted.LibraryItemID != 0 {
			t.Fatalf("successful deletion did not unlink: %+v, %v", wanted, err)
		}
		var linkedID int64
		if err := database.db.QueryRow("SELECT library_item_id FROM wishlist WHERE id = ?", wantedID).Scan(&linkedID); err != nil || linkedID != 0 {
			t.Fatalf("persisted wanted link: %d, %v", linkedID, err)
		}
		tags, err = database.GetItemTags(itemID)
		if err != nil || len(tags) != 0 {
			t.Fatalf("successful deletion left tags: %+v, %v", tags, err)
		}
	}
}
