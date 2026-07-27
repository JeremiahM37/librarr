package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/models"
)

// WishlistCleaner periodically removes wishlist items that are already present
// in the configured library. Matching is intentionally conservative: media
// type must match, normalized titles must match exactly, author must agree when
// present on the wishlist item, and ambiguous matches are skipped.
type WishlistCleaner struct {
	cfg    *config.Config
	db     *db.DB
	client *http.Client
}

type cleanupCandidate struct {
	Title     string
	Author    string
	MediaType string
	Source    string
}

type CleanupResult struct {
	Scanned   int
	Deleted   int
	DryRun    int
	Matched   int
	Ambiguous int
	Errors    int
}

func NewWishlistCleaner(cfg *config.Config, database *db.DB) *WishlistCleaner {
	return &WishlistCleaner{
		cfg: cfg,
		db:  database,
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (c *WishlistCleaner) Start(ctx context.Context) {
	if !c.cfg.WishlistCleanupEnabled {
		slog.Info("wishlist cleanup disabled")
		return
	}

	interval := time.Duration(c.cfg.WishlistCleanupIntervalHours) * time.Hour
	if interval < time.Hour {
		interval = 12 * time.Hour
	}

	slog.Info("wishlist cleanup started", "interval", interval, "dry_run", c.cfg.WishlistCleanupDryRun)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("wishlist cleanup stopped")
			return
		case <-ticker.C:
			c.Run(ctx)
		}
	}
}

func (c *WishlistCleaner) Run(ctx context.Context) CleanupResult {
	result := CleanupResult{}

	wishlist, err := c.db.GetWishlist()
	if err != nil {
		slog.Error("wishlist cleanup: failed to load wishlist", "error", err)
		result.Errors++
		return result
	}
	result.Scanned = len(wishlist)
	if len(wishlist) == 0 {
		return result
	}

	candidates, err := c.libraryCandidates(ctx)
	if err != nil {
		slog.Error("wishlist cleanup: failed to load library candidates", "error", err)
		result.Errors++
		return result
	}

	for _, item := range wishlist {
		matches := conservativeWishlistMatches(item, candidates)
		switch len(matches) {
		case 0:
			continue
		case 1:
			result.Matched++
			if c.cfg.WishlistCleanupDryRun {
				result.DryRun++
				slog.Info("wishlist cleanup: dry-run match",
					"wishlist_id", item.ID,
					"title", item.Title,
					"author", item.Author,
					"library_source", matches[0].Source,
				)
				continue
			}
			if err := c.db.DeleteWishlistItem(item.ID); err != nil {
				result.Errors++
				slog.Error("wishlist cleanup: delete failed", "wishlist_id", item.ID, "error", err)
				continue
			}
			result.Deleted++
			slog.Info("wishlist cleanup: deleted matched item",
				"wishlist_id", item.ID,
				"title", item.Title,
				"author", item.Author,
				"library_source", matches[0].Source,
			)
		default:
			result.Ambiguous++
			slog.Info("wishlist cleanup: skipped ambiguous match", "wishlist_id", item.ID, "title", item.Title, "matches", len(matches))
		}
	}

	slog.Info("wishlist cleanup complete",
		"scanned", result.Scanned,
		"matched", result.Matched,
		"deleted", result.Deleted,
		"dry_run", result.DryRun,
		"ambiguous", result.Ambiguous,
		"errors", result.Errors,
	)
	return result
}

func (c *WishlistCleaner) libraryCandidates(ctx context.Context) ([]cleanupCandidate, error) {
	candidates := make([]cleanupCandidate, 0)
	for _, mediaType := range []string{"ebook", "audiobook", "manga"} {
		items, err := c.db.GetItems(mediaType, 100000, 0)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			candidates = append(candidates, cleanupCandidate{
				Title:     item.Title,
				Author:    item.Author,
				MediaType: item.MediaType,
				Source:    "local-db",
			})
		}
	}

	if c.cfg.HasAudiobookshelf() && c.cfg.ABSLibraryID != "" {
		items, err := c.absLibraryCandidates(ctx, c.cfg.ABSLibraryID, "audiobook")
		if err != nil {
			slog.Warn("wishlist cleanup: failed to load ABS audiobook library", "error", err)
		} else {
			candidates = append(candidates, items...)
		}
	}
	if c.cfg.HasAudiobookshelf() && c.cfg.ABSEbookLibraryID != "" {
		items, err := c.absLibraryCandidates(ctx, c.cfg.ABSEbookLibraryID, "ebook")
		if err != nil {
			slog.Warn("wishlist cleanup: failed to load ABS ebook library", "error", err)
		} else {
			candidates = append(candidates, items...)
		}
	}

	return candidates, nil
}

type absCleanupResponse struct {
	Results  []absCleanupItem `json:"results"`
	Total    int              `json:"total"`
	NumPages int              `json:"numPages"`
}

type absCleanupItem struct {
	Media struct {
		Metadata struct {
			Title      string `json:"title"`
			AuthorName string `json:"authorName"`
			Authors    []struct {
				Name string `json:"name"`
			} `json:"authors"`
		} `json:"metadata"`
	} `json:"media"`
}

func (c *WishlistCleaner) absLibraryCandidates(ctx context.Context, libraryID, mediaType string) ([]cleanupCandidate, error) {
	baseURL := strings.TrimRight(c.cfg.ABSURL, "/")
	if baseURL == "" || libraryID == "" {
		return nil, nil
	}

	const limit = 500
	page := 0
	totalPages := 1
	out := make([]cleanupCandidate, 0)

	for page < totalPages {
		params := url.Values{
			"page":  {strconv.Itoa(page)},
			"limit": {strconv.Itoa(limit)},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/libraries/%s/items?%s", baseURL, libraryID, params.Encode()), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.cfg.ABSToken)

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("ABS returned HTTP %d", resp.StatusCode)
		}

		var absResp absCleanupResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&absResp)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		for _, item := range absResp.Results {
			author := item.Media.Metadata.AuthorName
			if author == "" && len(item.Media.Metadata.Authors) > 0 {
				names := make([]string, 0, len(item.Media.Metadata.Authors))
				for _, a := range item.Media.Metadata.Authors {
					if strings.TrimSpace(a.Name) != "" {
						names = append(names, a.Name)
					}
				}
				author = strings.Join(names, ", ")
			}
			out = append(out, cleanupCandidate{
				Title:     item.Media.Metadata.Title,
				Author:    author,
				MediaType: mediaType,
				Source:    "audiobookshelf",
			})
		}

		if absResp.NumPages > 0 {
			totalPages = absResp.NumPages
		} else if absResp.Total > 0 {
			totalPages = int(math.Ceil(float64(absResp.Total) / float64(limit)))
		}
		if totalPages < 1 {
			totalPages = 1
		}
		page++
	}

	return out, nil
}

func conservativeWishlistMatches(item models.WishlistItem, candidates []cleanupCandidate) []cleanupCandidate {
	mediaType := strings.TrimSpace(item.MediaType)
	if mediaType == "" {
		mediaType = "ebook"
	}
	wishlistTitle := normalizeTitle(item.Title)
	if wishlistTitle == "" {
		return nil
	}
	wishlistAuthor := normalizePerson(item.Author)

	unique := make(map[string]cleanupCandidate)
	for _, candidate := range candidates {
		if candidate.MediaType != mediaType {
			continue
		}
		if normalizeTitle(candidate.Title) != wishlistTitle {
			continue
		}
		if wishlistAuthor != "" && !authorMatches(wishlistAuthor, candidate.Author) {
			continue
		}
		key := strings.Join([]string{
			normalizeTitle(candidate.Title),
			normalizePerson(candidate.Author),
			candidate.MediaType,
		}, "\x00")
		unique[key] = candidate
	}

	matches := make([]cleanupCandidate, 0, len(unique))
	for _, candidate := range unique {
		matches = append(matches, candidate)
	}
	return matches
}

func authorMatches(wishlistAuthor, libraryAuthor string) bool {
	if wishlistAuthor == "" {
		return true
	}
	if strings.TrimSpace(libraryAuthor) == "" {
		return false
	}
	if normalizePerson(libraryAuthor) == wishlistAuthor {
		return true
	}
	for _, part := range splitAuthorList(libraryAuthor) {
		if normalizePerson(part) == wishlistAuthor {
			return true
		}
	}
	return false
}

func splitAuthorList(author string) []string {
	replacer := strings.NewReplacer("&", ",", ";", ",", " and ", ",")
	return strings.Split(replacer.Replace(" "+strings.ToLower(author)+" "), ",")
}

func normalizeTitle(s string) string {
	normalized := normalizeWords(s)
	for _, suffix := range []string{" unabridged", " abridged"} {
		normalized = strings.TrimSuffix(normalized, suffix)
	}
	return strings.TrimSpace(normalized)
}

func normalizePerson(s string) string {
	return normalizeWords(s)
}

func normalizeWords(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastSpace := true
	for _, r := range s {
		switch {
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			b.WriteRune(r)
			lastSpace = false
		case unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r):
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}
