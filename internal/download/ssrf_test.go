package download

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahM37/librarr/internal/config"
)

// TestDirectDownloadRejectsLoopback verifies the default SSRF guard (no test
// override) blocks loopback/private targets at the downloadFile hop.
func TestDirectDownloadRejectsLoopback(t *testing.T) {
	cfg := &config.Config{IncomingDir: t.TempDir(), UserAgent: "test"}
	d := NewDirectDownloader(cfg, &http.Client{Timeout: time.Second})

	for _, u := range []string{
		"http://127.0.0.1/secret",
		"http://localhost:9696/api",
		"http://169.254.169.254/latest/meta-data/",
		"http://10.0.0.5/internal",
	} {
		if _, _, err := d.downloadFile(u, "x", nil); err == nil {
			t.Errorf("expected %s to be rejected by SSRF guard, got nil", u)
		} else if !strings.Contains(err.Error(), "restricted") {
			t.Errorf("expected restricted-address error for %s, got %v", u, err)
		}
	}
}

// TestStartDirectDownloadRejectsLoopback verifies the guard runs at the shared
// manager entry point before any job is created.
func TestStartDirectDownloadRejectsLoopback(t *testing.T) {
	cfg := &config.Config{IncomingDir: t.TempDir(), UserAgent: "test"}
	d := NewDirectDownloader(cfg, &http.Client{Timeout: time.Second})
	m := &Manager{cfg: cfg, direct: d}

	if _, err := m.StartDirectDownload("http://127.0.0.1/x", "t", "src", "", ""); err == nil {
		t.Fatal("expected loopback URL to be rejected at StartDirectDownload")
	}
}

// recordingTorrentClient captures whether a URL ever reached the client.
type recordingTorrentClient struct {
	added []string
}

func (r *recordingTorrentClient) AddTorrent(torrentURL, title, savePath, category, expectedInfoHash string) error {
	r.added = append(r.added, torrentURL)
	return nil
}
func (r *recordingTorrentClient) GetTorrents(category string) ([]TorrentInfo, error) { return nil, nil }
func (r *recordingTorrentClient) GetTorrentFiles(hash string) ([]TorrentFile, error) { return nil, nil }
func (r *recordingTorrentClient) DeleteTorrent(hash string, deleteFiles bool) error  { return nil }
func (r *recordingTorrentClient) Diagnose() map[string]interface{}                   { return nil }
func (r *recordingTorrentClient) Name() string                                       { return "recording" }

// TestStartTorrentDownloadRejectsInternalURLs covers the SSRF sink where the
// torrent client — not Librarr — performs the HTTP GET. Reported by Mahmoud
// Mostafa: /api/download/torrent passed download_url through unvalidated.
func TestStartTorrentDownloadRejectsInternalURLs(t *testing.T) {
	rec := &recordingTorrentClient{}
	m := &Manager{cfg: &config.Config{}, torrent: rec}

	for _, u := range []string{
		"http://169.254.169.254/latest/meta-data/iam/security-credentials/",
		"http://127.0.0.1:9696/api",
		"http://10.0.0.5/internal",
		"http://172.18.0.1:80/",
		"file:///etc/passwd",
	} {
		if err := m.StartTorrentDownload(u, "t", "", "", ""); err == nil {
			t.Errorf("expected %s to be rejected before reaching the torrent client", u)
		}
	}
	if len(rec.added) != 0 {
		t.Errorf("torrent client received blocked URLs: %v", rec.added)
	}
}

// TestStartTorrentDownloadAllowsLegitimateTargets verifies the guard does not
// break normal use: magnets, public trackers, and the operator-configured
// Prowlarr origin (usually a LAN address the outbound guard would reject).
func TestStartTorrentDownloadAllowsLegitimateTargets(t *testing.T) {
	rec := &recordingTorrentClient{}
	m := &Manager{cfg: &config.Config{ProwlarrURL: "http://192.168.1.225:9696"}, torrent: rec}

	for _, u := range []string{
		"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
		"https://example.org/torrents/book.torrent",
		"http://192.168.1.225:9696/1/download?apikey=x&link=y",
	} {
		if err := m.StartTorrentDownload(u, "t", "", "", ""); err != nil {
			t.Errorf("expected %s to be allowed, got %v", u, err)
		}
	}
	if len(rec.added) != 3 {
		t.Errorf("expected 3 URLs to reach the client, got %d: %v", len(rec.added), rec.added)
	}
}

// TestStartNZBDownloadRejectsInternalURLs covers the same sink on the SABnzbd
// addurl path.
func TestStartNZBDownloadRejectsInternalURLs(t *testing.T) {
	cfg := &config.Config{SABnzbdURL: "http://sab.invalid:8080", SABnzbdAPIKey: "k"}
	m := &Manager{cfg: cfg, sab: NewSABnzbdClient(cfg)}

	if _, err := m.StartNZBDownload("http://169.254.169.254/latest/meta-data/", "t", "ebook"); err == nil {
		t.Fatal("expected metadata URL to be rejected at StartNZBDownload")
	}
}
