package download

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jamie75/librarr/internal/config"
)

// mockTransmission is a configurable Transmission RPC test double. It enforces
// the 409 session-id handshake on the first request, then records and answers
// torrent-add / torrent-get / torrent-remove / session-get calls.
type mockTransmission struct {
	requireSession bool
	sessionID      string
	gotSession     bool

	lastMethod string
	lastArgs   map[string]interface{}

	// torrents returned by torrent-get (already shaped as RPC rows).
	torrents []map[string]interface{}
	// files returned for torrent-get fields=["files"].
	files []map[string]interface{}
}

func (m *mockTransmission) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// CSRF handshake: first request (or any without the header) gets 409.
		if m.requireSession && r.Header.Get("X-Transmission-Session-Id") != m.sessionID {
			w.Header().Set("X-Transmission-Session-Id", m.sessionID)
			w.WriteHeader(http.StatusConflict)
			return
		}
		m.gotSession = true

		body, _ := io.ReadAll(r.Body)
		var req transmissionRequest
		_ = json.Unmarshal(body, &req)
		m.lastMethod = req.Method
		m.lastArgs = req.Arguments

		resp := transmissionResponse{Result: "success", Arguments: map[string]interface{}{}}
		switch req.Method {
		case "session-get":
			resp.Arguments["version"] = "4.0.5"
		case "torrent-get":
			// If the caller asked for files, answer with the files payload.
			if fields, ok := req.Arguments["fields"].([]interface{}); ok && hasField(fields, "files") {
				resp.Arguments["torrents"] = []map[string]interface{}{
					{"files": m.files},
				}
			} else {
				resp.Arguments["torrents"] = m.torrents
			}
		case "torrent-add":
			resp.Arguments["torrent-added"] = map[string]interface{}{
				"hashString": "newhash", "id": 1, "name": "added",
			}
		case "torrent-remove":
			// no-op success
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func hasField(fields []interface{}, want string) bool {
	for _, f := range fields {
		if s, ok := f.(string); ok && s == want {
			return true
		}
	}
	return false
}

func newTestTransmission(serverURL string) *TransmissionClient {
	return NewTransmissionClient(&config.Config{
		TransmissionURL: serverURL,
		QBSavePath:      "/downloads",
		QBCategory:      "librarr",
	})
}

func TestTransmission_SessionHandshakeAndDiagnose(t *testing.T) {
	mock := &mockTransmission{requireSession: true, sessionID: "sess-xyz"}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	tc := newTestTransmission(srv.URL)
	res := tc.Diagnose()
	if res["success"] != true {
		t.Fatalf("Diagnose expected success, got %#v", res)
	}
	if res["version"] != "4.0.5" {
		t.Errorf("expected version 4.0.5, got %v", res["version"])
	}
	if tc.sessionID != "sess-xyz" {
		t.Errorf("expected session id to be cached, got %q", tc.sessionID)
	}
}

func TestTransmission_DiagnoseNotConfigured(t *testing.T) {
	tc := NewTransmissionClient(&config.Config{})
	res := tc.Diagnose()
	if res["success"] != false {
		t.Fatalf("expected failure when not configured, got %#v", res)
	}
}

func TestTransmission_AddTorrentSetsLabelAndDownloadDir(t *testing.T) {
	mock := &mockTransmission{requireSession: true, sessionID: "s"}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	tc := newTestTransmission(srv.URL)
	if err := tc.AddTorrent("magnet:?xt=urn:btih:abc", "Some Book", "/audiobooks-incoming", "audiobooks", ""); err != nil {
		t.Fatalf("AddTorrent error: %v", err)
	}
	if mock.lastMethod != "torrent-add" {
		t.Fatalf("expected torrent-add, got %s", mock.lastMethod)
	}
	if mock.lastArgs["download-dir"] != "/audiobooks-incoming" {
		t.Errorf("download-dir = %v, want /audiobooks-incoming", mock.lastArgs["download-dir"])
	}
	labels, ok := mock.lastArgs["labels"].([]interface{})
	if !ok || len(labels) != 1 || labels[0] != "audiobooks" {
		t.Errorf("labels = %v, want [audiobooks]", mock.lastArgs["labels"])
	}
	if mock.lastArgs["filename"] != "magnet:?xt=urn:btih:abc" {
		t.Errorf("filename = %v", mock.lastArgs["filename"])
	}
}

func TestTransmission_AddTorrentFallsBackToDefaults(t *testing.T) {
	mock := &mockTransmission{}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	tc := newTestTransmission(srv.URL) // QBSavePath=/downloads, QBCategory=librarr
	if err := tc.AddTorrent("http://x/t.torrent", "T", "", "", ""); err != nil {
		t.Fatalf("AddTorrent error: %v", err)
	}
	if mock.lastArgs["download-dir"] != "/downloads" {
		t.Errorf("download-dir default = %v, want /downloads", mock.lastArgs["download-dir"])
	}
	labels := mock.lastArgs["labels"].([]interface{})
	if labels[0] != "librarr" {
		t.Errorf("label default = %v, want librarr", labels[0])
	}
}

func TestTransmission_GetTorrentsFiltersByLabelAndMapsState(t *testing.T) {
	mock := &mockTransmission{
		torrents: []map[string]interface{}{
			{ // ours, seeding (status 6) => uploading => completed
				"name": "Mine", "hashString": "h1", "status": 6,
				"percentDone": 1.0, "totalSize": 1000, "rateDownload": 0,
				"downloadDir": "/downloads", "error": 0, "labels": []string{"librarr"},
			},
			{ // someone else's torrent — must be filtered out
				"name": "NotMine", "hashString": "h2", "status": 4,
				"percentDone": 0.5, "totalSize": 2000, "rateDownload": 500,
				"downloadDir": "/other", "error": 0, "labels": []string{"sonarr"},
			},
			{ // ours, downloading (status 4)
				"name": "Mine2", "hashString": "h3", "status": 4,
				"percentDone": 0.25, "totalSize": 4000, "rateDownload": 100,
				"downloadDir": "/downloads", "error": 0, "labels": []string{"librarr"},
			},
		},
	}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	tc := newTestTransmission(srv.URL)
	got, err := tc.GetTorrents("librarr")
	if err != nil {
		t.Fatalf("GetTorrents error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 librarr torrents, got %d: %#v", len(got), got)
	}

	byHash := map[string]TorrentInfo{}
	for _, ti := range got {
		byHash[ti.Hash] = ti
	}
	if _, ok := byHash["h2"]; ok {
		t.Error("non-librarr torrent leaked into results")
	}
	if MapTorrentStatus(byHash["h1"].State) != "completed" {
		t.Errorf("seeding torrent should map to completed, got %q -> %q", byHash["h1"].State, MapTorrentStatus(byHash["h1"].State))
	}
	if MapTorrentStatus(byHash["h3"].State) != "downloading" {
		t.Errorf("downloading torrent state = %q -> %q", byHash["h3"].State, MapTorrentStatus(byHash["h3"].State))
	}
	if byHash["h1"].ContentPath != "/downloads/Mine" {
		t.Errorf("ContentPath = %q, want /downloads/Mine", byHash["h1"].ContentPath)
	}
	if byHash["h3"].Progress != 0.25 {
		t.Errorf("Progress = %v, want 0.25", byHash["h3"].Progress)
	}
}

func TestTransmission_GetTorrentFiles(t *testing.T) {
	mock := &mockTransmission{
		files: []map[string]interface{}{
			{"name": "Book/chapter1.epub", "length": 100},
			{"name": "Book/chapter2.epub", "length": 200},
		},
	}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	tc := newTestTransmission(srv.URL)
	files, err := tc.GetTorrentFiles("h1")
	if err != nil {
		t.Fatalf("GetTorrentFiles error: %v", err)
	}
	if len(files) != 2 || files[0].Name != "Book/chapter1.epub" {
		t.Fatalf("unexpected files: %#v", files)
	}
}

func TestTransmission_DeleteTorrentSendsHashAndFlag(t *testing.T) {
	mock := &mockTransmission{}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	tc := newTestTransmission(srv.URL)
	if err := tc.DeleteTorrent("h1", true); err != nil {
		t.Fatalf("DeleteTorrent error: %v", err)
	}
	if mock.lastMethod != "torrent-remove" {
		t.Fatalf("expected torrent-remove, got %s", mock.lastMethod)
	}
	if mock.lastArgs["delete-local-data"] != true {
		t.Errorf("delete-local-data = %v, want true", mock.lastArgs["delete-local-data"])
	}
	ids, ok := mock.lastArgs["ids"].([]interface{})
	if !ok || len(ids) != 1 || ids[0] != "h1" {
		t.Errorf("ids = %v, want [h1]", mock.lastArgs["ids"])
	}
}

func TestMapTransmissionState(t *testing.T) {
	cases := []struct {
		status      int
		percentDone float64
		errCode     int
		want        string
	}{
		{0, 0.5, 0, "pausedDL"},
		{0, 1.0, 0, "pausedUP"},
		{2, 0.0, 0, "checkingDL"},
		{3, 0.0, 0, "queuedDL"},
		{4, 0.3, 0, "downloading"},
		{5, 1.0, 0, "queuedUP"},
		{6, 1.0, 0, "uploading"},
		{4, 0.3, 3, "error"},       // local error (code 3) is fatal
		{4, 0.3, 1, "downloading"}, // tracker warning is non-fatal — keep downloading
		{4, 0.3, 2, "downloading"}, // tracker error is non-fatal — keep downloading
		{6, 1.0, 2, "uploading"},   // seeding with a dead tracker is still seeding/complete
	}
	for _, c := range cases {
		if got := mapTransmissionState(c.status, c.percentDone, c.errCode); got != c.want {
			t.Errorf("mapTransmissionState(%d,%v,%d) = %q, want %q", c.status, c.percentDone, c.errCode, got, c.want)
		}
	}
}
