package search

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/models"
)

// NyaaManga searches a Nyaa-style RSS source for manga torrents. RSS base URL
// comes from the runtime sources registry.
type NyaaManga struct {
	cfg    *config.Config
	client *http.Client
}

func NewNyaaManga(cfg *config.Config, client *http.Client) *NyaaManga {
	return &NyaaManga{cfg: cfg, client: client}
}

func (n *NyaaManga) Name() string         { return "nyaa_manga" }
func (n *NyaaManga) Label() string        { return "Nyaa" }
func (n *NyaaManga) Enabled() bool        { return true }
func (n *NyaaManga) SearchTab() string    { return "manga" }
func (n *NyaaManga) DownloadType() string { return "torrent" }

type nyaaRSSFeed struct {
	XMLName xml.Name       `xml:"rss"`
	Channel nyaaRSSChannel `xml:"channel"`
}

type nyaaRSSChannel struct {
	Items []nyaaRSSItem `xml:"item"`
}

type nyaaRSSItem struct {
	Title    string `xml:"title"`
	Link     string `xml:"link"`
	Seeders  string `xml:"seeders"`
	Size     string `xml:"size"`
	InfoHash string `xml:"infoHash"`
	Magnet   string `xml:"magnetUri"`
}

func (n *NyaaManga) Search(ctx context.Context, query string) ([]models.SearchResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", n.cfg.Sources.Nyaa.URL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("f", "0")
	q.Set("c", "3_1") // Literature / English-translated
	q.Set("q", query)
	q.Set("page", "rss")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", n.cfg.UserAgent)

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Nyaa HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var feed nyaaRSSFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("parse Nyaa RSS: %w", err)
	}

	var results []models.SearchResult
	for _, item := range feed.Channel.Items {
		if len(results) >= 20 {
			break
		}

		title := strings.TrimSpace(item.Title)
		if title == "" {
			continue
		}

		seeders, _ := strconv.Atoi(item.Seeders)

		downloadURL := item.Magnet
		if downloadURL == "" {
			downloadURL = item.Link
		}

		results = append(results, models.SearchResult{
			Source:      "nyaa_manga",
			Title:       title,
			DownloadURL: downloadURL,
			MagnetURL:   item.Magnet,
			InfoHash:    item.InfoHash,
			Seeders:     seeders,
			SizeHuman:   item.Size,
			Indexer:     "Nyaa",
			MediaType:   "manga",
			Format:      extractFormatFromTitle(title),
		})
	}

	return results, nil
}
