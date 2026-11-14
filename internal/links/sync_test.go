package links

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
)

// kavitaStub serves libraries and their series as Kavita does, one series per
// page so paging is exercised. Kavita mounts the library under /kavita.
type kavitaStub struct {
	*httptest.Server
	mu        sync.Mutex
	libraries []kavitaLibrary
	series    map[int][]kavitaSeries
	logins    int
	auth      []string
	fail      bool
}

func newKavitaStub(t *testing.T) *kavitaStub {
	t.Helper()
	k := &kavitaStub{
		libraries: []kavitaLibrary{
			{ID: 1, Type: kavitaBook, Folders: []string{"/kavita/books"}},
			{ID: 2, Type: kavitaManga, Folders: []string{"/kavita/manga"}},
		},
		series: map[int][]kavitaSeries{
			1: {
				{ID: 5, LibraryID: 1, LowestFolderPath: "/kavita/books/Pierce Brown/Red Rising"},
				{ID: 6, LibraryID: 1, LowestFolderPath: "/kavita/books/Only In Kavita/Book"},
			},
			2: {
				{ID: 9, LibraryID: 2, FolderPath: "/kavita/manga/Akira"},
				// Same relative path as an ebook: must not link across types.
				{ID: 10, LibraryID: 2, LowestFolderPath: "/kavita/manga/Pierce Brown/Red Rising"},
			},
		},
	}
	k.Server = httptest.NewServer(http.HandlerFunc(k.serve))
	t.Cleanup(k.Close)
	return k
}

func (k *kavitaStub) serve(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if r.URL.Path == "/api/Account/login" {
		k.logins++
		io.WriteString(w, `{"token":"jwt-123"}`)
		return
	}
	k.auth = append(k.auth, r.Header.Get("x-api-key")+r.Header.Get("Authorization"))
	if k.fail {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	switch r.URL.Path {
	case "/api/Library/libraries":
		json.NewEncoder(w).Encode(k.libraries)
	case "/api/Series/all-v2":
		var filter struct {
			Statements []struct {
				Field int    `json:"field"`
				Value string `json:"value"`
			} `json:"statements"`
		}
		json.NewDecoder(r.Body).Decode(&filter)
		if len(filter.Statements) != 1 || filter.Statements[0].Field != kavitaFilterLibraries {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var series []kavitaSeries
		for id, s := range k.series {
			if filter.Statements[0].Value == itoa(id) {
				series = s
			}
		}
		page := r.URL.Query().Get("pageNumber")
		var out []kavitaSeries
		for i, s := range series {
			if itoa(i+1) == page {
				out = append(out, s)
			}
		}
		w.Header().Set("Pagination", `{"totalPages":`+itoa(len(series))+`}`)
		json.NewEncoder(w).Encode(out)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func newABSStub(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" || r.URL.Path != "/api/libraries/audio/items" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		io.WriteString(w, `{"results":[{"id":"li_1","relPath":"John Green/Tuberculosis"}],"total":1}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

type fixture struct {
	db    *db.DB
	cfg   *config.Config
	ids   map[string]int64
	sync  *Syncer
	kav   *kavitaStub
	abs   *httptest.Server
	media string
}

// newFixture records library items as Librarr files them under its own
// /media root, which differs from Kavita's /kavita mount.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	database, err := db.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	f := &fixture{db: database, ids: map[string]int64{}, kav: newKavitaStub(t), abs: newABSStub(t), media: root}
	f.cfg = &config.Config{
		EbookDir:     filepath.Join(root, "ebooks"),
		AudiobookDir: filepath.Join(root, "audiobooks"),
		MangaDir:     filepath.Join(root, "manga"),
		KavitaURL:    f.kav.URL, KavitaAPIKey: "key",
		ABSURL: f.abs.URL, ABSToken: "tok", ABSLibraryID: "audio",
	}
	for name, item := range map[string]models.LibraryItem{
		"epub":      {MediaType: "ebook", FilePath: filepath.Join(root, "ebooks/Pierce Brown/Red Rising/Red Rising.epub")},
		"mobi":      {MediaType: "ebook", FilePath: filepath.Join(root, "ebooks/Pierce Brown/Red Rising/Red Rising.mobi")},
		"unshelved": {MediaType: "ebook", FilePath: filepath.Join(root, "ebooks/Someone/Other/Other.epub")},
		"outside":   {MediaType: "ebook", FilePath: "/elsewhere/Pierce Brown/Red Rising/x.epub"},
		"manga":     {MediaType: "manga", FilePath: filepath.Join(root, "manga/Akira")},
		"audio":     {MediaType: "audiobook", FilePath: filepath.Join(root, "audiobooks/John Green/Tuberculosis")},
	} {
		if err := os.MkdirAll(filepath.Dir(item.FilePath), 0o755); err != nil && name != "outside" {
			t.Fatal(err)
		}
		item.Title = name
		id, err := database.AddItem(&item)
		if err != nil {
			t.Fatal(err)
		}
		f.ids[name] = id
	}
	// Directories as imported: the manga series and the audiobook folder.
	for _, dir := range []string{"manga/Akira", "audiobooks/John Green/Tuberculosis"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	f.sync = NewSyncer(f.cfg, database)
	return f
}

// links returns each linked item's "service:external id".
func (f *fixture) links(t *testing.T) map[string][]string {
	t.Helper()
	ids := make([]int64, 0, len(f.ids))
	for _, id := range f.ids {
		ids = append(ids, id)
	}
	stored, err := f.db.LibraryLinks(ids)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for name, id := range f.ids {
		for _, l := range stored[id] {
			out[name] = append(out[name], l.Service+":"+l.ExternalID)
		}
	}
	return out
}

func assertLinks(t *testing.T, got map[string][]string, want map[string][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("links = %v, want %v", got, want)
	}
	for name, w := range want {
		if len(got[name]) != len(w) {
			t.Fatalf("%s links = %v, want %v (all: %v)", name, got[name], w, got)
		}
		for i := range w {
			if got[name][i] != w[i] {
				t.Fatalf("%s links = %v, want %v", name, got[name], w)
			}
		}
	}
}

func TestSyncMatchesByPathWithinEachLibrary(t *testing.T) {
	f := newFixture(t)
	f.sync.RunOnce(context.Background())
	assertLinks(t, f.links(t), map[string][]string{
		"epub":  {"kavita:5"},
		"mobi":  {"kavita:5"},
		"manga": {"kavita:9"},
		"audio": {"abs:li_1"},
	})

	stored, err := f.db.LibraryLinks([]int64{f.ids["epub"]})
	if err != nil {
		t.Fatal(err)
	}
	if l := stored[f.ids["epub"]][0]; l.URLPath != "/library/1/series/5" || l.CoverPath != "/api/image/series-cover?seriesId=5" {
		t.Errorf("kavita link = %+v", l)
	}
}

func TestSyncConfiguredKavitaLibraryIsTheOnlyOneOfItsType(t *testing.T) {
	f := newFixture(t)
	f.kav.libraries = append(f.kav.libraries, kavitaLibrary{ID: 3, Type: kavitaBook, Folders: []string{"/kavita/other"}})
	f.kav.series[3] = []kavitaSeries{{ID: 30, LibraryID: 3, LowestFolderPath: "/kavita/other/Pierce Brown/Red Rising"}}
	f.cfg.KavitaEbookLibraryID = "3"
	f.sync.RunOnce(context.Background())
	got := f.links(t)
	if len(got["epub"]) != 1 || got["epub"][0] != "kavita:30" {
		t.Fatalf("epub links = %v, want the configured library's series 30", got["epub"])
	}
}

func TestSyncKeepsLinksWhenServiceFailsAndDropsThemWhenUnconfigured(t *testing.T) {
	f := newFixture(t)
	f.sync.RunOnce(context.Background())

	f.kav.fail = true
	f.sync.RunOnce(context.Background())
	if got := f.links(t); len(got["epub"]) != 1 {
		t.Fatalf("an unreachable Kavita dropped links: %v", got)
	}

	f.cfg.KavitaURL = ""
	f.sync.RunOnce(context.Background())
	assertLinks(t, f.links(t), map[string][]string{"audio": {"abs:li_1"}})
}

func TestSyncDropsLinksOfDeletedItems(t *testing.T) {
	f := newFixture(t)
	f.sync.RunOnce(context.Background())
	if err := f.db.DeleteItem(f.ids["epub"]); err != nil {
		t.Fatal(err)
	}
	delete(f.ids, "epub")
	if got := f.links(t); len(got["epub"]) != 0 || len(got["mobi"]) != 1 {
		t.Fatalf("links after delete = %v", got)
	}
}

func TestSyncKavitaAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		apiKey     string
		wantLogins int
		wantAuth   string
	}{
		{"auth key without login", "kavita-key", 0, "kavita-key"},
		{"password after login", "", 1, "Bearer jwt-123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.cfg.KavitaAPIKey, f.cfg.KavitaUser, f.cfg.KavitaPass = tc.apiKey, "u", "p"
			f.sync.RunOnce(context.Background())
			if f.kav.logins != tc.wantLogins {
				t.Errorf("logins = %d, want %d", f.kav.logins, tc.wantLogins)
			}
			if len(f.kav.auth) == 0 {
				t.Fatal("no requests reached Kavita")
			}
			for _, got := range f.kav.auth {
				if got != tc.wantAuth {
					t.Errorf("credentials = %q, want %q", got, tc.wantAuth)
				}
			}
		})
	}
}

func TestKickNeverBlocks(t *testing.T) {
	s := NewSyncer(&config.Config{}, nil)
	for i := 0; i < 3; i++ {
		s.Kick()
	}
}

// Kavita shelves a series whose books sit in several folders by the folder
// they share; the nearest series above an item wins.
func TestSyncMatchesSeriesSpanningFolders(t *testing.T) {
	f := newFixture(t)
	f.kav.series[1] = append(f.kav.series[1], kavitaSeries{ID: 7, LibraryID: 1, LowestFolderPath: "/kavita/books/Pierce Brown"})
	golden := models.LibraryItem{Title: "golden", MediaType: "ebook", FilePath: filepath.Join(f.media, "ebooks/Pierce Brown/Golden Son/Golden Son.epub")}
	id, err := f.db.AddItem(&golden)
	if err != nil {
		t.Fatal(err)
	}
	f.ids["golden"] = id
	f.sync.RunOnce(context.Background())
	got := f.links(t)
	if len(got["golden"]) != 1 || got["golden"][0] != "kavita:7" {
		t.Errorf("golden links = %v, want the shared series 7", got["golden"])
	}
	if len(got["epub"]) != 1 || got["epub"][0] != "kavita:5" {
		t.Errorf("epub links = %v, want its own series 5", got["epub"])
	}
}

func TestSyncKavitaLibraryConfiguredForBothTypes(t *testing.T) {
	f := newFixture(t)
	f.kav.series[1] = append(f.kav.series[1], kavitaSeries{ID: 11, LibraryID: 1, LowestFolderPath: "/kavita/books/Akira"})
	f.cfg.KavitaEbookLibraryID, f.cfg.KavitaMangaLibraryID = "1", "1"
	f.sync.RunOnce(context.Background())
	got := f.links(t)
	if len(got["epub"]) != 1 || got["epub"][0] != "kavita:5" {
		t.Errorf("epub links = %v, want series 5", got["epub"])
	}
	if len(got["manga"]) != 1 || got["manga"][0] != "kavita:11" {
		t.Errorf("manga links = %v, want series 11 from the shared library", got["manga"])
	}
}
