package api

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
	"github.com/JeremiahM37/librarr/internal/db"
	"github.com/JeremiahM37/librarr/internal/download"
	"github.com/JeremiahM37/librarr/internal/models"
	"github.com/JeremiahM37/librarr/internal/search"
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
