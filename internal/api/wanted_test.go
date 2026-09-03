package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/scheduler"
	"github.com/JeremiahM37/librarr/internal/search"
	"github.com/JeremiahM37/librarr/internal/webhook"
)

func wantedTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	database, err := db.New(filepath.Join(dir, "librarr.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	cfg := &config.Config{
		SettingsFile:              filepath.Join(dir, "settings.json"),
		EbookDir:                  filepath.Join(dir, "ebooks"),
		SchedulerMinScore:         70,
		SchedulerItemDelaySeconds: 5,
		SchedulerIntervalHours:    24,
		AutoUpgradeEnabled:        true,
		AuthorMonitorEnabled:      true,
		AuthorMonitorAutoAdd:      true,
		AuthorCheckIntervalDays:   7,
	}
	health := search.NewHealthTracker(3, 300)
	searchMgr := search.NewManager(cfg, nil, health)
	s := &Server{cfg: cfg, db: database, searchMgr: searchMgr}
	s.scheduler = scheduler.NewScheduler(cfg, database, searchMgr, nil, webhook.NewSender())
	s.authorMonitor = scheduler.NewAuthorMonitor(cfg, database, webhook.NewSender())
	return s
}

func adminReq(method, target string, body interface{}) *http.Request {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, target, &buf)
	ctx := context.WithValue(req.Context(), ctxUserRole, "admin")
	ctx = context.WithValue(ctx, ctxUsername, "admin")
	return req.WithContext(ctx)
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("bad JSON %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestWishlistAPI_StateAndProfile(t *testing.T) {
	s := wantedTestServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/wishlist", s.handleGetWishlist)
	mux.HandleFunc("POST /api/wishlist", s.handleAddWishlist)
	mux.HandleFunc("PATCH /api/wishlist/{id}", s.handleUpdateWishlist)
	mux.HandleFunc("POST /api/wishlist/{id}/search", s.handleSearchWishlistItem)

	// Create with an unknown profile → 400.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/wishlist", map[string]interface{}{"title": "X", "quality_profile_id": 999}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown profile: %d %s", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/wishlist", map[string]interface{}{"title": "Dune", "author": "Frank Herbert"}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	id := int64(decode(t, rec)["id"].(float64))

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/wishlist", nil))
	body := decode(t, rec)
	items := body["items"].([]interface{})
	if len(items) != 1 {
		t.Fatalf("items: %v", items)
	}
	it := items[0].(map[string]interface{})
	if it["state"] != "missing" || it["profile_name"] != "Default Ebook" || it["monitored"] != true || it["cutoff_met"] != false {
		t.Fatalf("decorated item: %v", it)
	}
	if body["counts"].(map[string]interface{})["missing"] != float64(1) || body["upgrades_enabled"] != true {
		t.Fatalf("counts/flags: %v", body)
	}

	// Link a PDF: state becomes "upgrade" (cutoff epub unmet).
	path := filepath.Join(s.cfg.EbookDir, "dune.pdf")
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, []byte("x"), 0o644)
	itemID, _ := s.db.AddItem(&models.LibraryItem{Title: "Dune", FilePath: path, FileFormat: "pdf", MediaType: "ebook"})
	_, _ = s.db.SatisfyWishlistItem(id, itemID)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/wishlist", nil))
	it = decode(t, rec)["items"].([]interface{})[0].(map[string]interface{})
	if it["state"] != "upgrade" || it["current_format"] != "pdf" {
		t.Fatalf("after pdf: %v", it)
	}

	// Turn global upgrades off: same file now reads as satisfied.
	s.cfg.AutoUpgradeEnabled = false
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/wishlist", nil))
	if st := decode(t, rec)["items"].([]interface{})[0].(map[string]interface{})["state"]; st != "satisfied" {
		t.Fatalf("upgrades off should read satisfied, got %v", st)
	}
	s.cfg.AutoUpgradeEnabled = true

	// PATCH: unmonitor and switch to a custom profile whose cutoff is pdf.
	pid, _ := s.db.CreateQualityProfile(&db.QualityProfile{Name: "PDF is fine", FormatRanking: []string{"epub", "pdf"}, CutoffFormat: "pdf", UpgradeAllowed: true})
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PATCH", "/api/wishlist/"+itoa(id), map[string]interface{}{"monitored": false, "quality_profile_id": pid}))
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body)
	}
	it = decode(t, rec)["item"].(map[string]interface{})
	if it["state"] != "unmonitored" || it["profile_name"] != "PDF is fine" || it["cutoff_met"] != true {
		t.Fatalf("patched item: %v", it)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PATCH", "/api/wishlist/"+itoa(id), map[string]interface{}{"quality_profile_id": 4242}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("patch unknown profile: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PATCH", "/api/wishlist/9999", map[string]interface{}{"monitored": true}))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("patch missing: %d", rec.Code)
	}

	// Search-now (dry run) goes through the scheduler; with no sources the
	// honest answer is "no results", and the row records it.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/wishlist/"+itoa(id)+"/search", map[string]interface{}{"dry_run": true}))
	if rec.Code != http.StatusOK {
		t.Fatalf("search: %d %s", rec.Code, rec.Body)
	}
	out := decode(t, rec)
	outcome := out["outcome"].(map[string]interface{})
	// The item is satisfied under its custom profile (cutoff pdf), so it is skipped.
	if outcome["action"] != "skipped" || !strings.Contains(outcome["reason"].(string), "cutoff met") {
		t.Fatalf("outcome: %v", outcome)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/wishlist/9999/search", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("search missing: %d %s", rec.Code, rec.Body)
	}
}

func itoa(n int64) string { return json.Number(strings.TrimSpace(string(mustJSON(n)))).String() }

func mustJSON(v interface{}) []byte { b, _ := json.Marshal(v); return b }

func TestQualityProfileAPI_ValidationAndBuiltins(t *testing.T) {
	s := wantedTestServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/quality-profiles", s.handleGetQualityProfiles)
	mux.HandleFunc("GET /api/quality-profiles/formats", s.handleQualityFormats)
	mux.HandleFunc("GET /api/quality-profiles/default", s.handleGetDefaultQualityProfile)
	mux.HandleFunc("POST /api/quality-profiles", s.handleCreateQualityProfile)
	mux.HandleFunc("PUT /api/quality-profiles/{id}", s.handleUpdateQualityProfile)
	mux.HandleFunc("DELETE /api/quality-profiles/{id}", s.handleDeleteQualityProfile)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/quality-profiles", nil))
	var profiles []db.QualityProfile
	_ = json.Unmarshal(rec.Body.Bytes(), &profiles)
	if len(profiles) != 3 {
		t.Fatalf("expected 3 seeded profiles, got %d", len(profiles))
	}
	var ebookDefault db.QualityProfile
	for _, p := range profiles {
		if !p.Builtin {
			t.Fatalf("seeded profile not builtin: %+v", p)
		}
		if p.MediaType == "ebook" {
			ebookDefault = p
		}
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/quality-profiles/formats", nil))
	if !strings.Contains(rec.Body.String(), `"audiobook"`) || !strings.Contains(rec.Body.String(), `"m4b"`) {
		t.Fatalf("formats: %s", rec.Body)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/quality-profiles/default", nil))
	def := decode(t, rec)
	if def["auto_upgrade_enabled"] != true || def["defaults"].(map[string]interface{})["manga"] == nil {
		t.Fatalf("default: %v", def)
	}

	bad := []map[string]interface{}{
		{"name": "", "format_ranking": []string{"epub"}},
		{"name": "dup", "format_ranking": []string{"epub", "EPUB"}},
		{"name": "cutoff", "format_ranking": []string{"epub", "pdf"}, "cutoff_format": "mobi"},
		{"name": "type", "media_type": "video", "format_ranking": []string{"mkv"}},
		{"name": "sizes", "format_ranking": []string{"epub"}, "preferred_size_min": 10, "preferred_size_max": 5},
	}
	for _, b := range bad {
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, adminReq("POST", "/api/quality-profiles", b))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("payload %v: expected 400, got %d %s", b, rec.Code, rec.Body)
		}
	}

	// Empty ranking falls back to the media type default; builtin flag is ignored.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/quality-profiles", map[string]interface{}{"name": "Audio custom", "media_type": "audiobook", "builtin": true, "cutoff_format": "MP3"}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created db.QualityProfile
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if created.Builtin || created.MediaType != "audiobook" || created.FormatRanking[0] != "m4b" || created.CutoffFormat != "mp3" {
		t.Fatalf("created: %+v", created)
	}

	// Update with an invalid cutoff → 400; valid update sticks.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PUT", "/api/quality-profiles/"+itoa(created.ID), map[string]interface{}{"name": "Audio custom", "format_ranking": []string{"m4b"}, "cutoff_format": "flac"}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("update invalid cutoff: %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PUT", "/api/quality-profiles/"+itoa(created.ID), map[string]interface{}{"name": "Audio custom 2", "media_type": "audiobook", "format_ranking": []string{"FLAC", "m4b"}, "cutoff_format": "m4b", "upgrade_allowed": true}))
	if rec.Code != http.StatusOK {
		t.Fatalf("update: %d %s", rec.Code, rec.Body)
	}
	var updated db.QualityProfile
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if updated.Name != "Audio custom 2" || updated.FormatRanking[0] != "flac" || !updated.UpgradeAllowed {
		t.Fatalf("updated: %+v", updated)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PUT", "/api/quality-profiles/9999", map[string]interface{}{"name": "x", "format_ranking": []string{"epub"}}))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("update missing: %d", rec.Code)
	}

	// Editing a builtin cannot move it to another media type.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PUT", "/api/quality-profiles/"+itoa(ebookDefault.ID), map[string]interface{}{"name": "Ebooks", "media_type": "manga", "format_ranking": []string{"pdf", "epub"}, "cutoff_format": "epub", "upgrade_allowed": false}))
	if rec.Code != http.StatusOK {
		t.Fatalf("update builtin: %d %s", rec.Code, rec.Body)
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if !updated.Builtin || updated.MediaType != "ebook" || updated.FormatRanking[0] != "pdf" || updated.UpgradeAllowed {
		t.Fatalf("builtin after edit: %+v", updated)
	}

	// Builtins cannot be deleted; customs can.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("DELETE", "/api/quality-profiles/"+itoa(ebookDefault.ID), nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("delete builtin: %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("DELETE", "/api/quality-profiles/"+itoa(created.ID), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete custom: %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("DELETE", "/api/quality-profiles/"+itoa(created.ID), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("delete twice: %d", rec.Code)
	}
}

func TestSchedulerConfigAPI_PersistsAndApplies(t *testing.T) {
	s := wantedTestServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/scheduler/config", s.handleSchedulerConfig)
	mux.HandleFunc("GET /api/scheduler/status", s.handleSchedulerStatus)
	mux.HandleFunc("POST /api/scheduler/run", s.handleSchedulerRun)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PUT", "/api/scheduler/config", map[string]interface{}{"min_score": 101}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("min_score 101: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PUT", "/api/scheduler/config", map[string]interface{}{
		"enabled": true, "interval_hours": 6, "auto_download": true, "min_score": 0,
		"item_delay_seconds": 0, "auto_upgrade": false, "keep_old_files": true,
	}))
	if rec.Code != http.StatusOK {
		t.Fatalf("config: %d %s", rec.Code, rec.Body)
	}
	if !s.cfg.SchedulerEnabled || s.cfg.SchedulerIntervalHours != 6 || !s.cfg.SchedulerAutoDownload || s.cfg.SchedulerMinScore != 0 ||
		s.cfg.SchedulerItemDelaySeconds != 0 || s.cfg.AutoUpgradeEnabled || !s.cfg.UpgradeKeepOldFiles {
		t.Fatalf("config not applied: %+v", s.cfg)
	}
	raw, err := os.ReadFile(s.cfg.SettingsFile)
	if err != nil {
		t.Fatalf("settings not persisted: %v", err)
	}
	var saved map[string]interface{}
	_ = json.Unmarshal(raw, &saved)
	if saved["scheduler_interval_hours"] != float64(6) || saved["auto_upgrade_enabled"] != false || saved["scheduler_min_score"] != float64(0) {
		t.Fatalf("persisted: %v", saved)
	}

	// A fresh config reading that file picks the values up (the zero-valued
	// integers included).
	cfg := &config.Config{SettingsFile: s.cfg.SettingsFile, SchedulerMinScore: 70, SchedulerItemDelaySeconds: 5, AutoUpgradeEnabled: true}
	cfg.ReloadSettingsFile()
	if cfg.SchedulerIntervalHours != 6 || cfg.SchedulerMinScore != 0 || cfg.SchedulerItemDelaySeconds != 0 || cfg.AutoUpgradeEnabled || !cfg.UpgradeKeepOldFiles || !cfg.SchedulerEnabled {
		t.Fatalf("reloaded config: %+v", cfg)
	}

	// Status carries the new switches; a synchronous run returns stats.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/scheduler/status", nil))
	st := decode(t, rec)["status"].(map[string]interface{})
	if st["auto_upgrade"] != false || st["keep_old_files"] != true || st["item_delay_seconds"] != float64(0) {
		t.Fatalf("status: %v", st)
	}
	_, _ = s.db.AddWishlistItem("Nothing here", "", "ebook")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/scheduler/run?wait=1", nil))
	out := decode(t, rec)
	stats := out["stats"].(map[string]interface{})
	if stats["scanned"] != float64(1) || stats["searched"] != float64(1) {
		t.Fatalf("sync run stats: %v", stats)
	}
}

func TestDeleteBook_UnlinksWantedRow(t *testing.T) {
	s := wantedTestServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/library/book/{id}", s.handleDeleteBook)
	wid, _ := s.db.AddWishlistItem("Gone", "", "ebook")
	path := filepath.Join(s.cfg.EbookDir, "gone.epub")
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, []byte("x"), 0o644)
	itemID, _ := s.db.AddItem(&models.LibraryItem{Title: "Gone", FilePath: path, FileFormat: "epub", MediaType: "ebook"})
	_, _ = s.db.SatisfyWishlistItem(wid, itemID)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("DELETE", "/api/library/book/"+itoa(itemID), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	w, _ := s.db.GetWishlistItem(wid)
	if w.LibraryItemID != 0 {
		t.Fatalf("row should be unlinked: %+v", w)
	}
}

func TestAuthorsAPI_PatchAndCheck(t *testing.T) {
	s := wantedTestServer(t)
	ol := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"docs": []map[string]interface{}{
			{"key": "/works/OL1W", "title": "Debut", "first_publish_year": 2020, "author_name": []string{"New Author"}},
		}})
	}))
	defer ol.Close()
	s.authorMonitor.SetOpenLibraryURL(ol.URL)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/authors", s.handleListMonitoredAuthors)
	mux.HandleFunc("POST /api/authors/monitor", s.handleAddMonitoredAuthor)
	mux.HandleFunc("PATCH /api/authors/{id}", s.handleUpdateMonitoredAuthor)
	mux.HandleFunc("POST /api/authors/{id}/check", s.handleCheckMonitoredAuthor)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/authors/monitor", map[string]interface{}{"name": "New Author"}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("add: %d %s", rec.Code, rec.Body)
	}
	created := decode(t, rec)
	id := int64(created["id"].(float64))
	if created["auto_add"] != true {
		t.Fatalf("auto_add should default from config: %v", created)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PATCH", "/api/authors/"+itoa(id), map[string]interface{}{"auto_add": false, "check_interval_days": 0}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("interval 0 should be rejected: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("PATCH", "/api/authors/"+itoa(id), map[string]interface{}{"auto_add": false, "check_interval_days": 3}))
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body)
	}
	a := decode(t, rec)["author"].(map[string]interface{})
	if a["auto_add"] != false || a["check_interval_days"] != float64(3) {
		t.Fatalf("patched author: %v", a)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/authors/"+itoa(id)+"/check", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("check: %d %s", rec.Code, rec.Body)
	}
	res := decode(t, rec)["result"].(map[string]interface{})
	if res["baseline"] != true || res["seen"] != float64(1) {
		t.Fatalf("first check should be a baseline: %v", res)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("GET", "/api/authors", nil))
	list := decode(t, rec)
	if list["enabled"] != true || len(list["authors"].([]interface{})) != 1 {
		t.Fatalf("list: %v", list)
	}
	if got := list["authors"].([]interface{})[0].(map[string]interface{})["seen_works"]; got != float64(1) {
		t.Fatalf("seen_works: %v", got)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, adminReq("POST", "/api/authors/9999/check", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("check missing: %d", rec.Code)
	}
}

func TestSettingsAPI_ExposesAndAppliesWantedKeys(t *testing.T) {
	s := wantedTestServer(t)
	rec := httptest.NewRecorder()
	s.handleGetSettings(rec, adminReq("GET", "/api/settings", nil))
	got := decode(t, rec)
	for _, k := range []string{"scheduler_enabled", "scheduler_min_score", "auto_upgrade_enabled", "upgrade_keep_old_files", "author_monitor_enabled", "author_monitor_auto_add", "scheduler_item_delay_seconds"} {
		if _, ok := got[k]; !ok {
			t.Errorf("settings missing %s", k)
		}
	}
	rec = httptest.NewRecorder()
	s.handleSaveSettings(rec, adminReq("POST", "/api/settings", map[string]interface{}{"auto_upgrade_enabled": false, "scheduler_min_score": 55, "author_monitor_auto_add": false, "scheduler_interval_hours": 0}))
	if rec.Code != http.StatusOK {
		t.Fatalf("save: %d %s", rec.Code, rec.Body)
	}
	if s.cfg.AutoUpgradeEnabled || s.cfg.SchedulerMinScore != 55 || s.cfg.AuthorMonitorAutoAdd || s.cfg.SchedulerIntervalHours != 24 {
		t.Fatalf("settings not applied (interval 0 must be ignored): %+v", s.cfg)
	}
}
