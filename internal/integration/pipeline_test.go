//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jamie75/librarr/internal/api"
	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/download"
	"github.com/jamie75/librarr/internal/models"
	"github.com/jamie75/librarr/internal/organize"
	"github.com/jamie75/librarr/internal/search"
)

type pipelineMockSource struct{}

func (p *pipelineMockSource) Name() string         { return "test" }
func (p *pipelineMockSource) Label() string        { return "Test" }
func (p *pipelineMockSource) Enabled() bool        { return true }
func (p *pipelineMockSource) SearchTab() string    { return "main" }
func (p *pipelineMockSource) DownloadType() string { return "direct" }
func (p *pipelineMockSource) Search(_ context.Context, query string) ([]models.SearchResult, error) {
	return []models.SearchResult{{
		Source: "test",
		Title:  query,
		Format: "epub",
	}}, nil
}

// TestAPISearchEndpoint boots a minimal server and verifies authenticated search works end-to-end.
func TestAPISearchEndpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DBPath:       filepath.Join(dir, "api-pipeline.db"),
		SettingsFile: filepath.Join(dir, "settings.json"),
		APIKey:       "integration-test-key",
	}

	database, err := db.New(cfg.DBPath)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	defer database.Close()

	health := search.NewHealthTracker(3, 300)
	searchMgr := search.NewManager(cfg, []search.Searcher{&pipelineMockSource{}}, health)

	direct := download.NewDirectDownloader(cfg, http.DefaultClient)
	organizer := organize.NewOrganizer(cfg)
	downloadMgr := download.NewManager(cfg, database, nil, nil, direct, organizer, nil, health)

	srv := api.NewServer(cfg, database, searchMgr, downloadMgr, nil, nil, nil, organizer, nil)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/api/search?q=pipeline+integration", nil)
	req.Header.Set("X-Api-Key", cfg.APIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search status: %d", resp.StatusCode)
	}
}
