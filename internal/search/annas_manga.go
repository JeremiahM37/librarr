package search

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// AnnasManga searches Anna's Archive for CBZ/CBR manga/comics.
type AnnasManga struct {
	cfg    *config.Config
	client *http.Client
	annas  *AnnasArchive // reuses the existing Anna's search logic
}

func NewAnnasManga(cfg *config.Config, client *http.Client) *AnnasManga {
	return &AnnasManga{
		cfg:    cfg,
		client: client,
		annas:  NewAnnasArchive(cfg, client),
	}
}

func (a *AnnasManga) Name() string         { return "annas_manga" }
func (a *AnnasManga) Label() string        { return "Anna's Archive (Manga)" }
func (a *AnnasManga) Enabled() bool        { return a.cfg.AnnasArchiveDomain != "" }
func (a *AnnasManga) SearchTab() string    { return "manga" }
func (a *AnnasManga) DownloadType() string { return "direct" }

func (a *AnnasManga) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	var results []models.SearchResult
	seenMD5 := make(map[string]bool)

	for _, ext := range []string{"cbz", "cbr"} {
		res, err := a.annas.doSearch(ctx, query, ext, seenMD5)
		if err != nil {
			slog.Warn("anna's manga search failed", "ext", ext, "error", err)
			continue
		}
		// Mark results as manga type.
		for i := range res {
			res[i].Source = "annas_manga"
			res[i].MediaType = "manga"
			res[i].Format = ext
		}
		results = append(results, res...)
	}

	if len(results) > 20 {
		results = results[:20]
	}

	// Deduplicate by MD5.
	seen := make(map[string]bool)
	var deduped []models.SearchResult
	for _, r := range results {
		if r.MD5 != "" && seen[r.MD5] {
			continue
		}
		if r.MD5 != "" {
			seen[r.MD5] = true
		}
		deduped = append(deduped, r)
	}

	return deduped, nil
}

// SearchForTitle searches Anna's Archive for a specific title and returns results.
// Used by the alt-MD5 fallback logic.
func SearchAnnasForTitle(cfg *config.Config, client *http.Client, ctx context.Context, title string) ([]models.SearchResult, error) {
	a := NewAnnasArchive(cfg, client)
	seenMD5 := make(map[string]bool)
	results, err := a.doSearch(ctx, title, "epub", seenMD5)
	if err != nil {
		return nil, fmt.Errorf("anna's search for alt MD5: %w", err)
	}
	return results, nil
}
