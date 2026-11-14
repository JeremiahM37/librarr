package links

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/JeremiahM37/librarr/internal/models"
)

const absPageSize = 500

type absItemsPage struct {
	Results []struct {
		ID      string `json:"id"`
		RelPath string `json:"relPath"`
	} `json:"results"`
	Total int `json:"total"`
}

// absEntries lists the configured Audiobookshelf libraries: ABS_LIBRARY_ID
// for audiobooks and ABS_EBOOK_LIBRARY_ID for ebooks. Audiobookshelf reports
// each item's path relative to its library folder.
func (s *Syncer) absEntries(ctx context.Context) ([]entry, error) {
	var entries []entry
	for _, lib := range []struct{ id, mediaType string }{
		{s.cfg.ABSLibraryID, "audiobook"},
		{s.cfg.ABSEbookLibraryID, "ebook"},
	} {
		if lib.id == "" {
			continue
		}
		for page, seen := 0, 0; ; page++ {
			var resp absItemsPage
			if err := s.absJSON(ctx, lib.id, page, &resp); err != nil {
				return nil, err
			}
			for _, item := range resp.Results {
				if item.RelPath == "" {
					continue
				}
				entries = append(entries, entry{
					mediaType: lib.mediaType,
					relPath:   path.Clean(item.RelPath),
					link: models.LibraryLink{
						Service:    ServiceABS,
						ExternalID: item.ID,
						URLPath:    "/item/" + url.PathEscape(item.ID),
						CoverPath:  "/api/items/" + url.PathEscape(item.ID) + "/cover",
					},
				})
			}
			seen += len(resp.Results)
			if len(resp.Results) == 0 || seen >= resp.Total {
				break
			}
		}
	}
	return entries, nil
}

// absJSON fetches one page of a library's items. Audiobookshelf pages from 0.
func (s *Syncer) absJSON(ctx context.Context, libraryID string, page int, out *absItemsPage) error {
	q := url.Values{
		"page":     {strconv.Itoa(page)},
		"limit":    {strconv.Itoa(absPageSize)},
		"minified": {"1"},
	}
	apiURL := fmt.Sprintf("%s/api/libraries/%s/items?%s", s.cfg.ABSURL, url.PathEscape(libraryID), q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.ABSToken)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("audiobookshelf library %s returned HTTP %d", libraryID, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
