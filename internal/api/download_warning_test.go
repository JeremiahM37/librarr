package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/download"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/search"
)

type downloadWarningTorrentClient struct {
	err error
}

func (c downloadWarningTorrentClient) AddTorrent(string, string, string, string, string) error {
	return c.err
}

func (downloadWarningTorrentClient) GetTorrents(string) ([]download.TorrentInfo, error) {
	return nil, nil
}

func (downloadWarningTorrentClient) GetTorrentFiles(string) ([]download.TorrentFile, error) {
	return nil, nil
}

func (downloadWarningTorrentClient) DeleteTorrent(string, bool) error { return nil }

func (downloadWarningTorrentClient) Diagnose() map[string]interface{} { return nil }

func (downloadWarningTorrentClient) Name() string { return "test" }

type recordingTorrentClient struct {
	savePath string
	category string
	url      string
	title    string
	hash     string
	err      error
}

func (c *recordingTorrentClient) AddTorrent(url, title, savePath, category, hash string) error {
	c.url = url
	c.title = title
	c.savePath = savePath
	c.category = category
	c.hash = hash
	return c.err
}

func (*recordingTorrentClient) GetTorrents(string) ([]download.TorrentInfo, error) {
	return nil, nil
}

func (*recordingTorrentClient) GetTorrentFiles(string) ([]download.TorrentFile, error) {
	return nil, nil
}

func (*recordingTorrentClient) DeleteTorrent(string, bool) error { return nil }

func (*recordingTorrentClient) Diagnose() map[string]interface{} { return nil }

func (*recordingTorrentClient) Name() string { return "test" }

func newDownloadWarningTestServer(t *testing.T, client download.TorrentClient) *Server {
	t.Helper()
	database, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	cfg := &config.Config{QBUrl: "http://qbit.test"}
	manager := download.NewManager(cfg, database, client, nil, nil, nil, nil, search.NewHealthTracker(3, 300))
	return &Server{cfg: cfg, db: database, downloadMgr: manager}
}

func TestDownloadsV1RouteReturnsDownloadsEnvelope(t *testing.T) {
	database, err := db.New(filepath.Join(t.TempDir(), "library.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	cfg := &config.Config{}
	manager := download.NewManager(cfg, database, nil, nil, nil, nil, nil, search.NewHealthTracker(3, 300))
	server := &Server{cfg: cfg, db: database, downloadMgr: manager, mux: http.NewServeMux()}
	server.registerDownloadRoutes()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/downloads", nil)
	rr := httptest.NewRecorder()
	server.mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rr.Code, http.StatusOK, rr.Body.String())
	}

	var resp struct {
		Downloads []models.DownloadStatus `json:"downloads"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Downloads == nil {
		t.Fatal("downloads should be an empty array, got null")
	}
}

func TestHandleTorrentDownloadUsesConfiguredQBSavePaths(t *testing.T) {
	tests := []struct {
		name         string
		mediaType    string
		wantSavePath string
		wantCategory string
	}{
		{"ebook", "ebook", "/downloads/rclone-mnt/downloads", "librarr"},
		{"audiobook", "audiobook", "/downloads/rclone-mnt/audiobooks", "audio"},
		{"manga", "manga", "/downloads/rclone-mnt/manga", "manga"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &recordingTorrentClient{}
			server := newDownloadWarningTestServer(t, client)
			server.cfg.QBSavePath = "/downloads/rclone-mnt/downloads"
			server.cfg.QBCategory = "librarr"
			server.cfg.QBAudiobookSavePath = "/downloads/rclone-mnt/audiobooks"
			server.cfg.QBAudiobookCategory = "audio"
			server.cfg.QBMangaSavePath = "/downloads/rclone-mnt/manga"
			server.cfg.QBMangaCategory = "manga"
			server.cfg.IncomingDir = "/data/incoming"
			req := models.DownloadRequest{Title: "Test Book", Source: "torrent", DownloadURL: "magnet:?xt=urn:btih:abc", MediaType: tt.mediaType}
			r := httptest.NewRequest("POST", "/api/download", nil)
			rr := httptest.NewRecorder()

			server.handleTorrentDownload(rr, r, req)
			if client.savePath != tt.wantSavePath {
				t.Fatalf("savePath = %q, want %q", client.savePath, tt.wantSavePath)
			}
			if client.savePath == server.cfg.IncomingDir {
				t.Fatalf("savePath used local incoming dir: %q", client.savePath)
			}
			if client.category != tt.wantCategory {
				t.Fatalf("category = %q, want %q", client.category, tt.wantCategory)
			}
		})
	}
}

func TestHandleTorrentDownloadReturnsWarningAsAccepted(t *testing.T) {
	warning := &download.TorrentVerificationWarning{Err: errors.New("verification timeout")}
	server := newDownloadWarningTestServer(t, downloadWarningTorrentClient{err: warning})
	req := models.DownloadRequest{Title: "Test Book", Source: "torrent", DownloadURL: "magnet:?xt=urn:btih:abc"}
	r := httptest.NewRequest("POST", "/api/download", nil)
	rr := httptest.NewRecorder()

	server.handleTorrentDownload(rr, r, req)
	var response map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["success"] != true {
		t.Fatalf("success = %v, want true: %s", response["success"], rr.Body.String())
	}
	if response["warning"] == "" || response["error"] != "" {
		t.Fatalf("response = %s, want warning and empty error", rr.Body.String())
	}
}

func TestHandleTorrentDownloadKeepsQBitFailureAsFailure(t *testing.T) {
	server := newDownloadWarningTestServer(t, downloadWarningTorrentClient{err: errors.New("qBittorrent API failure")})
	req := models.DownloadRequest{Title: "Test Book", Source: "torrent", DownloadURL: "magnet:?xt=urn:btih:abc"}
	r := httptest.NewRequest("POST", "/api/download", nil)
	rr := httptest.NewRecorder()

	server.handleTorrentDownload(rr, r, req)
	var response map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["success"] != false || response["error"] != "qBittorrent API failure" {
		t.Fatalf("response = %s, want hard failure", rr.Body.String())
	}
}
