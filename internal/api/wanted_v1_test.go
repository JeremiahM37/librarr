package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/download"
	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/scheduler"
	"github.com/jamie75/librarr/internal/search"
)

func newWantedAPIServer(t *testing.T) (*Server, func()) {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "wanted.db"))
	if err != nil {
		t.Fatal(err)
	}
	seedCompletedEmptyBackfill(t, database)

	cfg := &config.Config{LibraryRepositoryMode: "normalized"}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, database)
	if err != nil {
		t.Fatal(err)
	}

	return &Server{
		cfg:            cfg,
		db:             database,
		libraryService: selection.LibraryService,
	}, func() { _ = database.Close() }
}

func TestWantedCreateListPatchDelete(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	body := bytes.NewBufferString(`{"title":"The Martian","author":"Andy Weir","media_type":"ebook","monitored":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", body)
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		Success bool `json:"success"`
		Item    struct {
			ID        int64  `json:"id"`
			Title     string `json:"title"`
			Author    string `json:"author"`
			Monitored bool   `json:"monitored"`
			Status    string `json:"status"`
		} `json:"item"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !created.Success || created.Item.ID == 0 || created.Item.Status != "wanted" || !created.Item.Monitored {
		t.Fatalf("created = %+v", created)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/wanted", nil)
	rr = httptest.NewRecorder()
	s.handleV1WantedList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", rr.Code, rr.Body.String())
	}
	var listed wantedListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 1 || listed.Counts["wanted"] != 1 || listed.Counts["monitored"] != 1 {
		t.Fatalf("listed = %+v", listed)
	}

	patch := bytes.NewBufferString(`{"monitored":false,"status":"ignored"}`)
	req = httptest.NewRequest(http.MethodPatch, "/api/v1/wanted/1", patch)
	req.SetPathValue("id", "1")
	rr = httptest.NewRecorder()
	s.handleV1WantedPatch(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("patch status = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/wanted", nil)
	rr = httptest.NewRecorder()
	s.handleV1WantedList(rr, req)
	if err := json.Unmarshal(rr.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Monitored || listed.Items[0].Status != "ignored" || listed.Counts["ignored"] != 1 {
		t.Fatalf("updated list = %+v", listed)
	}

	req = httptest.NewRequest(http.MethodDelete, "/api/v1/wanted/1", nil)
	req.SetPathValue("id", "1")
	rr = httptest.NewRecorder()
	s.handleV1WantedDelete(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWantedListMarksExistingLibraryMatchImported(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	createAPIScanBook(t, s.libraryService, "American Marxism", "Mark R. Levin")
	wanted, err := s.db.CreateWantedBook(models.WantedBook{
		Title:     "American Marxism",
		Author:    "Mark R Levin",
		MediaType: "ebook",
		Monitored: true,
		Status:    "downloading",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wanted", nil)
	rr := httptest.NewRecorder()
	s.handleV1WantedList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", rr.Code, rr.Body.String())
	}

	var listed wantedListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Status != "imported" || listed.Counts["imported"] != 1 {
		t.Fatalf("wanted list = %+v", listed)
	}

	stored, err := s.db.GetWantedBook(wanted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != "imported" {
		t.Fatalf("stored status = %q, want imported", stored.Status)
	}
}

func TestWantedListDoesNotMarkSameAuthorDifferentTitleImported(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	createAPIScanBook(t, s.libraryService, "American Marxism", "Mark R. Levin")
	if _, err := s.db.CreateWantedBook(models.WantedBook{
		Title:     "Men in Black: How the Supreme Court is Destroying America",
		Author:    "Mark R Levin",
		MediaType: "ebook",
		Monitored: true,
		Status:    "found",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wanted", nil)
	rr := httptest.NewRecorder()
	s.handleV1WantedList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", rr.Code, rr.Body.String())
	}

	var listed wantedListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Status != "found" {
		t.Fatalf("wanted list = %+v", listed)
	}
}

func TestWantedDuplicateRejected(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	body := []byte(`{"title":"Project Hail Mary","author":"Andy Weir","media_type":"ebook"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("first create status = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewReader(body))
	rr = httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWantedCreateNormalizesProwlarrReleaseContext(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	raw := "Rebel Prince, the Power, Passion and Defiance of Prince Charles by Tom Bower [ENG / MOBI]"
	body := bytes.NewBufferString(`{
		"title":"` + raw + `",
		"source":"torrent",
		"origin_source":"prowlarr",
		"origin_release_title":"` + raw + `",
		"indexer":"Books",
		"media_type":"ebook"
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", body)
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		Item models.WantedBook `json:"item"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Item.Title != "Rebel Prince: The Power, Passion and Defiance of Prince Charles" {
		t.Fatalf("title = %q", created.Item.Title)
	}
	if created.Item.Author != "Tom Bower" || created.Item.Language != "en" || created.Item.PreferredFormat != "mobi" {
		t.Fatalf("normalized item = %+v", created.Item)
	}
	if created.Item.OriginReleaseTitle != raw || created.Item.OriginIndexer != "Books" {
		t.Fatalf("release context = %+v", created.Item)
	}
}

func TestWantedCreateFromProwlarrReleaseSeedsOriginRelease(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	raw := "American Marxism by Mark R Levin [ENG / EPUB PDF]"
	body := bytes.NewBufferString(`{
		"title":"` + raw + `",
		"source":"prowlarr",
		"origin_source":"prowlarr",
		"origin_release_title":"` + raw + `",
		"origin_indexer":"MyAnonamouse",
		"indexer":"MyAnonamouse",
		"media_type":"ebook",
		"download_protocol":"torrent",
		"download_url":"https://prowlarr.example/download/123?apikey=secret",
		"guid":"mam-guid",
		"source_id":"mam-guid",
		"format":"epub",
		"language":"en",
		"size":123456,
		"size_human":"120 KB",
		"seeders":32,
		"leechers":1,
		"grabs":4,
		"publish_date":"2026-01-02T00:00:00Z",
		"categories":["7000","7020"],
		"score":97
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", body)
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		Item models.WantedBook `json:"item"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Item.Title != "American Marxism" || created.Item.Author != "Mark R. Levin" {
		t.Fatalf("canonical item = %+v", created.Item)
	}
	if created.Item.Status != "found" || created.Item.LastResultCount != 1 || created.Item.LastMatchTitle != raw || created.Item.LastSearch != nil {
		t.Fatalf("origin found status = %+v", created.Item)
	}
	if created.Item.Language != "en" || created.Item.PreferredFormat != "epub" || created.Item.OriginReleaseTitle != raw || created.Item.OriginIndexer != "MyAnonamouse" || created.Item.SourceID != "mam-guid" {
		t.Fatalf("origin context = %+v", created.Item)
	}

	releases, err := s.db.ListWantedReleases(created.Item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 {
		t.Fatalf("release count = %d, want 1", len(releases))
	}
	if releases[0].Title != raw || releases[0].Indexer != "MyAnonamouse" || releases[0].DownloadURL == "" || releases[0].Seeders != 32 || releases[0].Format != "epub" {
		t.Fatalf("seeded release = %+v", releases[0])
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/wanted/1/releases", nil)
	req.SetPathValue("id", strconv.FormatInt(created.Item.ID, 10))
	rr = httptest.NewRecorder()
	s.handleV1WantedReleases(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("releases status = %d body=%s", rr.Code, rr.Body.String())
	}
	var releaseResponse wantedReleasesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &releaseResponse); err != nil {
		t.Fatal(err)
	}
	if releaseResponse.Total != 1 || !releaseResponse.Items[0].DownloadAvailable || releaseResponse.Items[0].DownloadURL != "" {
		t.Fatalf("release response = %+v", releaseResponse)
	}
}

func TestWantedCreateUncertainReleaseParsingDoesNotInventAuthor(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{
		"title":"Unparseable Release Name [ENG / EPUB]",
		"source":"prowlarr",
		"origin_source":"prowlarr",
		"origin_release_title":"Unparseable Release Name [ENG / EPUB]",
		"media_type":"ebook"
	}`))
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		Item models.WantedBook `json:"item"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Item.Author != "" {
		t.Fatalf("invented author for uncertain release: %+v", created.Item)
	}
	if created.Item.Title != "Unparseable Release Name" {
		t.Fatalf("title cleanup = %q", created.Item.Title)
	}
}

func TestWantedDuplicateRejectedAfterCanonicalNormalization(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	raw := "Rebel Prince, the Power, Passion and Defiance of Prince Charles by Tom Bower [ENG / MOBI]"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{"title":"`+raw+`","source":"torrent","origin_source":"prowlarr","origin_release_title":"`+raw+`","media_type":"ebook"}`))
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("first create status = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{"title":"Rebel Prince: The Power, Passion and Defiance of Prince Charles","author":"Tom Bower","media_type":"ebook"}`))
	rr = httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWantedDuplicateRejectedAcrossTitlePunctuation(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{"title":"Men in Black: How the Supreme Court is Destroying America","author":"Mark R Levin","media_type":"ebook"}`))
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("first create status = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{"title":"Men in Black- How the Supreme Court is Destroying America","author":"Mark R. Levin","media_type":"ebook"}`))
	rr = httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("punctuation duplicate status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWantedValidation(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{"author":"Unknown"}`))
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing title status = %d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPatch, "/api/v1/wanted/12", bytes.NewBufferString(`{"status":"bogus"}`))
	req.SetPathValue("id", "12")
	rr = httptest.NewRecorder()
	s.handleV1WantedPatch(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid patch status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWantedSearchEndpoints(t *testing.T) {
	database, err := db.New(filepath.Join(t.TempDir(), "wanted-search.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	seedCompletedEmptyBackfill(t, database)

	book, err := database.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}

	prowlarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"title":       "The Martian EPUB",
			"downloadUrl": "http://example.test/download",
			"protocol":    "torrent",
			"seeders":     9,
			"infoHash":    "hash-1",
		}})
	}))
	defer prowlarr.Close()

	cfg := &config.Config{
		LibraryRepositoryMode: "normalized",
		ProwlarrURL:           prowlarr.URL,
		ProwlarrAPIKey:        "test-key",
		WantedMaxResultsKeep:  10,
	}
	selection, err := library.NewConfiguredLibraryService(context.Background(), cfg, database)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		cfg:            cfg,
		db:             database,
		libraryService: selection.LibraryService,
		wantedMonitor:  scheduler.NewWantedMonitor(cfg, database, nil, prowlarr.Client()),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/search", nil)
	req.SetPathValue("id", "1")
	rr := httptest.NewRecorder()
	s.handleV1WantedSearchOne(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("search one status = %d body=%s", rr.Code, rr.Body.String())
	}

	refetched, err := database.GetWantedBook(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if refetched.Status != "found" {
		t.Fatalf("status = %q, want found", refetched.Status)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/wanted/history", nil)
	rr = httptest.NewRecorder()
	s.handleV1WantedHistory(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("history status = %d body=%s", rr.Code, rr.Body.String())
	}
	var history wantedHistoryResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	if len(history.Items) != 1 || history.Items[0].WantedBookID != book.ID {
		t.Fatalf("history = %+v", history)
	}
}

func TestWantedReleasesEndpoint(t *testing.T) {
	s, cleanup := newWantedAPIServer(t)
	defer cleanup()

	book, err := s.db.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    "wanted",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wanted/1/releases", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	rr := httptest.NewRecorder()
	s.handleV1WantedReleases(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("empty releases status = %d body=%s", rr.Code, rr.Body.String())
	}
	var empty wantedReleasesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &empty); err != nil {
		t.Fatal(err)
	}
	if empty.Total != 0 || len(empty.Items) != 0 {
		t.Fatalf("empty releases = %+v", empty)
	}

	if err := s.db.ReplaceWantedReleases(book.ID, []models.WantedRelease{
		{Title: "Lower", Score: 75, Format: "epub", Protocol: "torrent"},
		{Title: "Higher", Score: 98, Format: "mobi", Protocol: "torrent"},
	}); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/wanted/1/releases", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	rr = httptest.NewRecorder()
	s.handleV1WantedReleases(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("releases status = %d body=%s", rr.Code, rr.Body.String())
	}
	var response wantedReleasesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Total != 2 || response.Items[0].Title != "Higher" || response.Items[0].Format != "mobi" {
		t.Fatalf("response = %+v", response)
	}
	if response.Items[0].DownloadURL != "" {
		t.Fatalf("download_url leaked in release response: %+v", response.Items[0])
	}
}

func newWantedDownloadAPIServer(t *testing.T, client *recordingTorrentClient) (*Server, func()) {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "wanted-download.db"))
	if err != nil {
		t.Fatal(err)
	}
	seedCompletedEmptyBackfill(t, database)
	cfg := &config.Config{
		QBUrl:      "http://qbit.test",
		QBSavePath: "/downloads/ebooks",
		QBCategory: "librarr",
	}
	manager := download.NewManager(cfg, database, client, nil, nil, nil, nil, search.NewHealthTracker(3, 300))
	return &Server{cfg: cfg, db: database, downloadMgr: manager}, func() { _ = database.Close() }
}

func createWantedReleaseForDownload(t *testing.T, s *Server, status string, release models.WantedRelease) (*models.WantedBook, models.WantedRelease) {
	t.Helper()
	book, err := s.db.CreateWantedBook(models.WantedBook{
		Title:     "The Martian",
		Author:    "Andy Weir",
		MediaType: "ebook",
		Monitored: true,
		Status:    status,
	})
	if err != nil {
		t.Fatal(err)
	}
	if release.Title == "" {
		release.Title = "The Martian release"
	}
	if err := s.db.ReplaceWantedReleases(book.ID, []models.WantedRelease{release}); err != nil {
		t.Fatal(err)
	}
	releases, err := s.db.ListWantedReleases(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	return book, releases[0]
}

func TestWantedReleaseDownloadSubmitsStoredRelease(t *testing.T) {
	client := &recordingTorrentClient{}
	s, cleanup := newWantedDownloadAPIServer(t, client)
	defer cleanup()
	book, release := createWantedReleaseForDownload(t, s, "found", models.WantedRelease{
		Title:       "The Martian 2014 EPUB",
		Indexer:     "Books",
		Protocol:    "torrent",
		Format:      "epub",
		DownloadURL: "magnet:?xt=urn:btih:abc",
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/releases/1/download", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	req.SetPathValue("release_id", strconv.FormatInt(release.ID, 10))
	rr := httptest.NewRecorder()
	s.handleV1WantedReleaseDownload(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("download status = %d body=%s", rr.Code, rr.Body.String())
	}
	if client.url != "magnet:?xt=urn:btih:abc" || client.title != "The Martian 2014 EPUB" || client.savePath != "/downloads/ebooks" || client.category != "librarr" {
		t.Fatalf("torrent submission = url=%q title=%q savePath=%q category=%q", client.url, client.title, client.savePath, client.category)
	}
	updated, err := s.db.GetWantedBook(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != "downloading" || updated.SelectedReleaseID != release.ID || updated.SelectedReleaseTitle != release.Title || updated.DownloadClient != "qbittorrent" || updated.DownloadHash != "abc" {
		t.Fatalf("updated item = %+v", updated)
	}
}

func TestWantedReleaseDownloadWorksWithSeededDiscoverRelease(t *testing.T) {
	client := &recordingTorrentClient{}
	s, cleanup := newWantedDownloadAPIServer(t, client)
	defer cleanup()

	raw := "American Marxism by Mark R Levin [ENG / EPUB PDF]"
	create := httptest.NewRequest(http.MethodPost, "/api/v1/wanted", bytes.NewBufferString(`{
		"title":"`+raw+`",
		"source":"prowlarr",
		"origin_source":"prowlarr",
		"origin_release_title":"`+raw+`",
		"origin_indexer":"MyAnonamouse",
		"media_type":"ebook",
		"download_url":"magnet:?xt=urn:btih:abc",
		"download_protocol":"torrent",
		"format":"epub",
		"language":"en"
	}`))
	rr := httptest.NewRecorder()
	s.handleV1WantedCreate(rr, create)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		Item models.WantedBook `json:"item"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	releases, err := s.db.ListWantedReleases(created.Item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 {
		t.Fatalf("seeded releases = %+v", releases)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/releases/1/download", nil)
	req.SetPathValue("id", strconv.FormatInt(created.Item.ID, 10))
	req.SetPathValue("release_id", strconv.FormatInt(releases[0].ID, 10))
	rr = httptest.NewRecorder()
	s.handleV1WantedReleaseDownload(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("download status = %d body=%s", rr.Code, rr.Body.String())
	}
	if client.url != "magnet:?xt=urn:btih:abc" {
		t.Fatalf("torrent URL = %q", client.url)
	}
}

func TestWantedReleaseDownloadRejectsWrongWantedRelease(t *testing.T) {
	client := &recordingTorrentClient{}
	s, cleanup := newWantedDownloadAPIServer(t, client)
	defer cleanup()
	book, _ := createWantedReleaseForDownload(t, s, "found", models.WantedRelease{Title: "One", Protocol: "torrent", DownloadURL: "magnet:?xt=urn:btih:one"})
	other, err := s.db.CreateWantedBook(models.WantedBook{Title: "Project Hail Mary", Author: "Andy Weir", MediaType: "ebook", Monitored: true, Status: "found"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.ReplaceWantedReleases(other.ID, []models.WantedRelease{{Title: "Two", Protocol: "torrent", DownloadURL: "magnet:?xt=urn:btih:two"}}); err != nil {
		t.Fatal(err)
	}
	otherReleases, err := s.db.ListWantedReleases(other.ID)
	if err != nil {
		t.Fatal(err)
	}
	otherRelease := otherReleases[0]
	if book.ID == other.ID {
		t.Fatal("expected distinct wanted ids")
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/releases/2/download", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	req.SetPathValue("release_id", strconv.FormatInt(otherRelease.ID, 10))
	rr := httptest.NewRecorder()
	s.handleV1WantedReleaseDownload(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("wrong release status = %d body=%s", rr.Code, rr.Body.String())
	}
	if client.url != "" {
		t.Fatalf("unexpected torrent submission for wrong release: %q", client.url)
	}
}

func TestWantedReleaseDownloadRejectsUnsupportedOrMissingURL(t *testing.T) {
	client := &recordingTorrentClient{}
	s, cleanup := newWantedDownloadAPIServer(t, client)
	defer cleanup()
	book, release := createWantedReleaseForDownload(t, s, "found", models.WantedRelease{Title: "Usenet", Protocol: "usenet"})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/releases/1/download", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	req.SetPathValue("release_id", strconv.FormatInt(release.ID, 10))
	rr := httptest.NewRecorder()
	s.handleV1WantedReleaseDownload(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unsupported status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWantedReleaseDownloadFailureLeavesStatusUnchanged(t *testing.T) {
	client := &recordingTorrentClient{err: errors.New("qbit auth failed")}
	s, cleanup := newWantedDownloadAPIServer(t, client)
	defer cleanup()
	book, release := createWantedReleaseForDownload(t, s, "found", models.WantedRelease{Protocol: "torrent", DownloadURL: "magnet:?xt=urn:btih:abc"})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/releases/1/download", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	req.SetPathValue("release_id", strconv.FormatInt(release.ID, 10))
	rr := httptest.NewRecorder()
	s.handleV1WantedReleaseDownload(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("failure status = %d body=%s", rr.Code, rr.Body.String())
	}
	updated, err := s.db.GetWantedBook(book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != "found" || updated.SelectedReleaseID != 0 {
		t.Fatalf("failed submission changed handoff state: %+v", updated)
	}
	if updated.DownloadError == "" {
		t.Fatalf("download error not recorded: %+v", updated)
	}
}

func TestWantedReleaseDownloadDuplicateClickRejected(t *testing.T) {
	client := &recordingTorrentClient{}
	s, cleanup := newWantedDownloadAPIServer(t, client)
	defer cleanup()
	book, release := createWantedReleaseForDownload(t, s, "downloading", models.WantedRelease{Protocol: "torrent", DownloadURL: "magnet:?xt=urn:btih:abc"})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/wanted/1/releases/1/download", nil)
	req.SetPathValue("id", strconv.FormatInt(book.ID, 10))
	req.SetPathValue("release_id", strconv.FormatInt(release.ID, 10))
	rr := httptest.NewRecorder()
	s.handleV1WantedReleaseDownload(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body=%s", rr.Code, rr.Body.String())
	}
	if client.url != "" {
		t.Fatalf("duplicate click submitted torrent: %q", client.url)
	}
}
