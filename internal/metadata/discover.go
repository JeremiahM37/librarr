package metadata

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/JeremiahM37/librarr/internal/models"
)

// DiscoverService merges Google Books and Open Library search results for
// the browse/discover UI. Google Books results come first (better cover art
// and descriptions in practice); Open Library results fill in titles Google
// Books didn't return, deduplicated by normalized title so the same book
// doesn't appear twice just because both sources have it. Google Books
// requires no API key to function - if it's unreachable or rate-limited,
// results silently degrade to Open-Library-only rather than failing the
// whole request.
type DiscoverService struct {
	googleBooks *GoogleBooksClient
	openLibrary *Client
}

// NewDiscoverService creates a service wrapping both catalog clients.
func NewDiscoverService(googleBooks *GoogleBooksClient, openLibrary *Client) *DiscoverService {
	return &DiscoverService{googleBooks: googleBooks, openLibrary: openLibrary}
}

// Search queries both sources concurrently and returns a merged, deduped list.
func (s *DiscoverService) Search(ctx context.Context, query string, limit int) []models.DiscoverResult {
	var googleResults, olResults []models.DiscoverResult
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		r, err := s.googleBooks.Search(ctx, query, limit)
		if err != nil {
			slog.Warn("discover: google books search failed", "error", err)
			return
		}
		googleResults = r
	}()
	go func() {
		defer wg.Done()
		r, err := s.openLibrary.SearchMulti(ctx, query, limit)
		if err != nil {
			slog.Warn("discover: open library search failed", "error", err)
			return
		}
		olResults = r
	}()
	wg.Wait()

	seen := make(map[string]bool, len(googleResults))
	merged := make([]models.DiscoverResult, 0, len(googleResults)+len(olResults))
	for _, r := range googleResults {
		if r.Title == "" {
			continue
		}
		seen[normalizeTitle(r.Title)] = true
		merged = append(merged, r)
	}
	for _, r := range olResults {
		key := normalizeTitle(r.Title)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		merged = append(merged, r)
	}

	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// GetDetail fetches full detail for one result by source+id.
func (s *DiscoverService) GetDetail(ctx context.Context, source, id string) (*models.DiscoverResult, error) {
	if source == "open_library" {
		return s.openLibrary.GetWork(ctx, id)
	}
	return s.googleBooks.GetVolume(ctx, id)
}

func normalizeTitle(title string) string {
	return strings.ToLower(strings.TrimSpace(title))
}
