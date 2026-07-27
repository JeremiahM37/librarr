package download

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jamie75/librarr/internal/config"
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
