package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// MangaDex searches a MangaDex-compatible API for manga. API, uploads, and
// web URLs come from the runtime sources registry.
type MangaDex struct {
	cfg    *config.Config
	client *http.Client
}

func NewMangaDex(cfg *config.Config, client *http.Client) *MangaDex {
	return &MangaDex{cfg: cfg, client: client}
}

func (m *MangaDex) Name() string         { return "mangadex" }
func (m *MangaDex) Label() string        { return "MangaDex" }
func (m *MangaDex) Enabled() bool        { return m.cfg.MangaDexEnabled }
func (m *MangaDex) SearchTab() string    { return "manga" }
func (m *MangaDex) DownloadType() string { return "direct" }

type mangadexResponse struct {
	Data []mangadexManga `json:"data"`
}

type mangadexManga struct {
	ID            string                 `json:"id"`
	Attributes    mangadexAttrs          `json:"attributes"`
	Relationships []mangadexRelationship `json:"relationships"`
}

type mangadexAttrs struct {
	Title       map[string]string `json:"title"`
	LastChapter string            `json:"lastChapter"`
	Status      string            `json:"status"`
}

type mangadexRelationship struct {
	Type       string                 `json:"type"`
	ID         string                 `json:"id"`
	Attributes map[string]interface{} `json:"attributes"`
}

func (m *MangaDex) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", m.cfg.Sources.MangaDex.APIURL+"/manga", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("title", query)
	q.Set("limit", "20")
	q.Add("includes[]", "cover_art")
	q.Add("availableTranslatedLanguage[]", "en")
	q.Add("contentRating[]", "safe")
	q.Add("contentRating[]", "suggestive")
	req.URL.RawQuery = q.Encode()

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("MangaDex HTTP %d", resp.StatusCode)
	}

	var data mangadexResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var results []models.SearchResult
	for _, item := range data.Data {
		// Prefer English title.
		title := item.Attributes.Title["en"]
		if title == "" {
			for _, v := range item.Attributes.Title {
				title = v
				break
			}
		}
		if title == "" {
			title = "Unknown"
		}

		// Extract author.
		author := ""
		for _, rel := range item.Relationships {
			if rel.Type == "author" && rel.Attributes != nil {
				if name, ok := rel.Attributes["name"].(string); ok {
					author = name
				}
				break
			}
		}

		// Cover art.
		coverURL := ""
		for _, rel := range item.Relationships {
			if rel.Type == "cover_art" && rel.Attributes != nil {
				if fname, ok := rel.Attributes["fileName"].(string); ok && fname != "" {
					coverURL = fmt.Sprintf("%s/covers/%s/%s.256.jpg", m.cfg.Sources.MangaDex.UploadsURL, item.ID, fname)
				}
				break
			}
		}

		chapterCount := item.Attributes.LastChapter
		if chapterCount == "" {
			chapterCount = "?"
		}

		sizeHuman := fmt.Sprintf("%s chapters", chapterCount)
		if item.Attributes.Status != "" {
			sizeHuman += " (" + item.Attributes.Status + ")"
		}

		results = append(results, models.SearchResult{
			Source:    "mangadex",
			Title:     title,
			Author:    author,
			SourceID:  fmt.Sprintf("mangadex-%s", item.ID),
			CoverURL:  coverURL,
			SizeHuman: sizeHuman,
			MediaType: "manga",
			URL:       fmt.Sprintf("%s/title/%s", m.cfg.Sources.MangaDex.WebURL, item.ID),
		})
	}

	return results, nil
}
