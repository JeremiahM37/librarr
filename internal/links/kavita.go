package links

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// Kavita library types, from Kavita's LibraryType enum.
const (
	kavitaManga      = 0
	kavitaComic      = 1
	kavitaBook       = 2
	kavitaImage      = 3
	kavitaLightNovel = 4
	kavitaComicVine  = 5
)

// kavitaFilterLibraries is the "Libraries" field of Kavita's series filter.
const kavitaFilterLibraries = 19

const kavitaPageSize = 500

type kavitaLibrary struct {
	ID      int      `json:"id"`
	Type    int      `json:"type"`
	Folders []string `json:"folders"`
}

type kavitaSeries struct {
	ID               int    `json:"id"`
	LibraryID        int    `json:"libraryId"`
	FolderPath       string `json:"folderPath"`
	LowestFolderPath string `json:"lowestFolderPath"`
}

// kavitaMediaTypes reports which Librarr media types a Kavita library holds.
// A library named by KAVITA_EBOOK_LIBRARY_ID or KAVITA_MANGA_LIBRARY_ID holds
// that type, and is the only library of it; one named by both holds both.
// Otherwise the library type decides.
func kavitaMediaTypes(lib kavitaLibrary, ebookID, mangaID string) []string {
	id := strconv.Itoa(lib.ID)
	ebookID, mangaID = strings.TrimSpace(ebookID), strings.TrimSpace(mangaID)
	var types []string
	if id == ebookID {
		types = append(types, "ebook")
	}
	if id == mangaID {
		types = append(types, "manga")
	}
	if len(types) > 0 {
		return types
	}
	switch lib.Type {
	case kavitaBook, kavitaLightNovel:
		if ebookID == "" {
			return []string{"ebook"}
		}
	case kavitaManga, kavitaComic, kavitaImage, kavitaComicVine:
		if mangaID == "" {
			return []string{"manga"}
		}
	}
	return nil
}

func (s *Syncer) kavitaEntries(ctx context.Context) ([]entry, error) {
	authorize, err := KavitaAuthorizer(ctx, s.client, s.cfg.KavitaURL, s.cfg.KavitaAPIKey, s.cfg.KavitaUser, s.cfg.KavitaPass)
	if err != nil {
		return nil, fmt.Errorf("authenticate: %w", err)
	}
	var libraries []kavitaLibrary
	if err := s.kavitaJSON(ctx, authorize, http.MethodGet, "/api/Library/libraries", nil, &libraries, nil); err != nil {
		return nil, err
	}
	var entries []entry
	for _, lib := range libraries {
		mediaTypes := kavitaMediaTypes(lib, s.cfg.KavitaEbookLibraryID, s.cfg.KavitaMangaLibraryID)
		if len(mediaTypes) == 0 {
			continue
		}
		series, err := s.kavitaLibrarySeries(ctx, authorize, lib.ID)
		if err != nil {
			return nil, err
		}
		for _, sr := range series {
			dir := sr.LowestFolderPath
			if dir == "" {
				dir = sr.FolderPath
			}
			rel, ok := relativeTo(dir, lib.Folders)
			if !ok {
				continue
			}
			link := models.LibraryLink{
				Service:    ServiceKavita,
				ExternalID: strconv.Itoa(sr.ID),
				URLPath:    fmt.Sprintf("/library/%d/series/%d", lib.ID, sr.ID),
				CoverPath:  fmt.Sprintf("/api/image/series-cover?seriesId=%d", sr.ID),
			}
			for _, mediaType := range mediaTypes {
				entries = append(entries, entry{mediaType: mediaType, relPath: rel, link: link})
			}
		}
	}
	return entries, nil
}

// kavitaLibrarySeries lists every series in one library. Kavita pages from 1.
func (s *Syncer) kavitaLibrarySeries(ctx context.Context, authorize func(*http.Request), libraryID int) ([]kavitaSeries, error) {
	filter, _ := json.Marshal(map[string]interface{}{
		"statements": []map[string]interface{}{
			{"comparison": 0, "field": kavitaFilterLibraries, "value": strconv.Itoa(libraryID)},
		},
		"combination": 1,
		"limitTo":     0,
		"sortOptions": map[string]interface{}{"sortField": 1, "isAscending": true},
	})
	var all []kavitaSeries
	for page := 1; ; page++ {
		var series []kavitaSeries
		var pagination struct {
			TotalPages int `json:"totalPages"`
		}
		apiPath := fmt.Sprintf("/api/Series/all-v2?pageNumber=%d&pageSize=%d", page, kavitaPageSize)
		if err := s.kavitaJSON(ctx, authorize, http.MethodPost, apiPath, filter, &series, &pagination); err != nil {
			return nil, err
		}
		all = append(all, series...)
		if len(series) == 0 || page >= pagination.TotalPages {
			return all, nil
		}
	}
}

// kavitaJSON makes a Kavita API call and decodes its body into out and, when
// pagination is non-nil, its Pagination header into pagination.
func (s *Syncer) kavitaJSON(ctx context.Context, authorize func(*http.Request), method, apiPath string, body []byte, out, pagination interface{}) error {
	req, err := http.NewRequestWithContext(ctx, method, s.cfg.KavitaURL+apiPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	authorize(req)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("kavita %s returned HTTP %d", strings.SplitN(apiPath, "?", 2)[0], resp.StatusCode)
	}
	if pagination != nil {
		if err := json.Unmarshal([]byte(resp.Header.Get("Pagination")), pagination); err != nil {
			return fmt.Errorf("kavita pagination header: %w", err)
		}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// KavitaAuthorizer returns a function that adds credentials to a Kavita
// request. An auth key is sent as-is; otherwise it logs in with the username
// and password to get a JWT.
func KavitaAuthorizer(ctx context.Context, client *http.Client, baseURL, apiKey, user, pass string) (func(*http.Request), error) {
	if apiKey != "" {
		return func(req *http.Request) { req.Header.Set("x-api-key", apiKey) }, nil
	}
	payload, _ := json.Marshal(map[string]string{"username": user, "password": pass})
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/Account/login", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kavita login HTTP %d", resp.StatusCode)
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return func(req *http.Request) { req.Header.Set("Authorization", "Bearer "+result.Token) }, nil
}
