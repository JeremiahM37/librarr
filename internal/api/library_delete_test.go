package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/models"
)

func TestLibraryDeleteRoutes(t *testing.T) {
	for _, media := range []string{"ebook", "audiobook", "manga"} {
		t.Run(media, func(t *testing.T) {
			server := libraryMediaTypeTestSetup(t)
			server.mux = http.NewServeMux()
			server.registerLibraryRoutes()
			file := filepath.Join(t.TempDir(), "keep.cbz")
			if err := os.WriteFile(file, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			itemID, err := server.db.AddItem(&models.LibraryItem{Title: "Remove", MediaType: media, FilePath: file})
			if err != nil {
				t.Fatal(err)
			}
			wantedID, err := server.db.AddWishlistItem("Remove", "", media)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := server.db.SatisfyWishlistItem(wantedID, itemID); err != nil {
				t.Fatal(err)
			}
			tagID, err := server.db.CreateTag("Keep tag", "blue")
			if err != nil {
				t.Fatal(err)
			}
			if err := server.db.AddItemTag(itemID, tagID); err != nil {
				t.Fatal(err)
			}
			route := media
			if media == "ebook" {
				route = "book"
			}
			endpoint := fmt.Sprintf("/api/library/%s/%d", route, itemID)
			server.cfg.APIKey = "fixture-key"
			handler := authMiddleware(server.cfg, server.db, NewSessionStore(), server.mux)
			for _, test := range []struct {
				path, key string
				status    int
			}{
				{endpoint, "", http.StatusUnauthorized},
				{endpoint, "wrong", http.StatusUnauthorized},
				{"/api/library/" + route + "/invalid", "fixture-key", http.StatusBadRequest},
				{"/api/library/" + route + "/999999", "fixture-key", http.StatusNotFound},
				{endpoint, "fixture-key", http.StatusOK},
				{endpoint, "fixture-key", http.StatusNotFound},
			} {
				request := httptest.NewRequest("DELETE", test.path, nil)
				request.Header.Set("X-Api-Key", test.key)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != test.status {
					t.Fatalf("%s: got %d want %d: %s", test.path, response.Code, test.status, response.Body)
				}
			}
			if _, err := server.db.GetItem(itemID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("deleted item: %v", err)
			}
			wanted, err := server.db.GetWishlistItem(wantedID)
			if err != nil || wanted.LibraryItemID != 0 {
				t.Fatalf("wanted not unlinked: %+v, %v", wanted, err)
			}
			tagged, err := server.db.GetItemsByTag(tagID, 50, 0)
			if err != nil || len(tagged) != 0 {
				t.Fatalf("tag associations: %+v, %v", tagged, err)
			}
			tags, err := server.db.GetItemTags(itemID)
			if err != nil || len(tags) != 0 {
				t.Fatalf("orphaned tag associations: %+v, %v", tags, err)
			}
			remaining, err := server.db.CountItems("")
			if err != nil || remaining != 3 {
				t.Fatalf("unrelated rows changed: %d, %v", remaining, err)
			}
			if contents, err := os.ReadFile(file); err != nil || string(contents) != "keep" {
				t.Fatalf("file changed: %q, %v", contents, err)
			}
		})
	}
}

func TestLibraryDeleteRejectsOtherCategories(t *testing.T) {
	for _, route := range []string{"book", "audiobook", "manga"} {
		server := libraryMediaTypeTestSetup(t)
		server.mux = http.NewServeMux()
		server.registerLibraryRoutes()
		items, _, err := server.db.FindItems("", "", 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if item.MediaType == route || (route == "book" && item.MediaType == "ebook") {
				continue
			}
			response := httptest.NewRecorder()
			server.mux.ServeHTTP(response, httptest.NewRequest("DELETE", fmt.Sprintf("/api/library/%s/%d", route, item.ID), nil))
			if response.Code != http.StatusNotFound {
				t.Fatalf("%s deleted %s: %d", route, item.MediaType, response.Code)
			}
			if _, err := server.db.GetItem(item.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMangaDeleteKavitaIDCollision(t *testing.T) {
	server := libraryMediaTypeTestSetup(t)
	server.cfg.KavitaURL, server.cfg.KavitaUser, server.cfg.KavitaPass = "http://unused.invalid", "user", "pass"
	server.mux = http.NewServeMux()
	server.registerLibraryRoutes()
	items, _, err := server.db.FindItems("manga", "", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("/api/library/manga/%d", items[0].ID)
	response := httptest.NewRecorder()
	server.mux.ServeHTTP(response, httptest.NewRequest("DELETE", endpoint, nil))
	if response.Code != http.StatusConflict {
		t.Fatalf("ambiguous ID: %d %s", response.Code, response.Body)
	}
	if _, err := server.db.GetItem(items[0].ID); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	server.mux.ServeHTTP(response, httptest.NewRequest("DELETE", endpoint+"?source=local", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("explicit local ID: %d %s", response.Code, response.Body)
	}
}

// Audiobook rows are Librarr records, so a delete never reaches
// Audiobookshelf, and an Audiobookshelf item ID is not accepted.
func TestAudiobookDeleteLeavesAudiobookshelfAlone(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request to Audiobookshelf: %s %s", r.Method, r.URL.Path)
	}))
	defer provider.Close()
	server := libraryMediaTypeTestSetup(t)
	server.cfg.ABSURL, server.cfg.ABSToken = provider.URL, "token"
	server.mux = http.NewServeMux()
	server.registerLibraryRoutes()

	response := httptest.NewRecorder()
	server.mux.ServeHTTP(response, httptest.NewRequest("DELETE", "/api/library/audiobook/abs-item", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("non-numeric ID: %d %s", response.Code, response.Body)
	}

	items, _, err := server.db.FindItems("audiobook", "", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	server.mux.ServeHTTP(response, httptest.NewRequest("DELETE", fmt.Sprintf("/api/library/audiobook/%d", items[0].ID), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("local audiobook with Audiobookshelf configured: %d %s", response.Code, response.Body)
	}
	if _, err := server.db.GetItem(items[0].ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("audiobook record remains: %v", err)
	}
}
