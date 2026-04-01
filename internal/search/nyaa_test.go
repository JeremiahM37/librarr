package search

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/JeremiahM37/librarr/internal/config"
)

func TestNyaaManga_Metadata(t *testing.T) {
	cfg := &config.Config{UserAgent: "test"}
	n := NewNyaaManga(cfg, http.DefaultClient)

	if n.Name() != "nyaa_manga" {
		t.Errorf("expected name nyaa_manga, got %s", n.Name())
	}
	if n.Label() != "Nyaa" {
		t.Errorf("expected label Nyaa, got %s", n.Label())
	}
	if !n.Enabled() {
		t.Error("expected always enabled")
	}
	if n.SearchTab() != "manga" {
		t.Errorf("expected tab manga, got %s", n.SearchTab())
	}
	if n.DownloadType() != "torrent" {
		t.Errorf("expected download type torrent, got %s", n.DownloadType())
	}
}

func TestNyaaManga_RSSParsing(t *testing.T) {
	rssXML := `<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:nyaa="https://nyaa.si/xmlns/nyaa">
  <channel>
    <item>
      <title>One Piece Vol 01-100 [English]</title>
      <link>https://nyaa.si/download/123456.torrent</link>
      <seeders>50</seeders>
      <size>5.2 GiB</size>
      <infoHash>abcdef1234567890abcdef1234567890abcdef12</infoHash>
      <magnetUri>magnet:?xt=urn:btih:abcdef1234567890</magnetUri>
    </item>
    <item>
      <title>Naruto Complete [English]</title>
      <link>https://nyaa.si/download/789012.torrent</link>
      <seeders>25</seeders>
      <size>3.1 GiB</size>
      <infoHash>fedcba0987654321fedcba0987654321fedcba09</infoHash>
    </item>
    <item>
      <title></title>
      <link></link>
    </item>
  </channel>
</rss>`

	var feed nyaaRSSFeed
	if err := xml.Unmarshal([]byte(rssXML), &feed); err != nil {
		t.Fatalf("failed to parse RSS: %v", err)
	}

	if len(feed.Channel.Items) != 3 {
		t.Fatalf("expected 3 items in RSS, got %d", len(feed.Channel.Items))
	}

	item0 := feed.Channel.Items[0]
	if item0.Title != "One Piece Vol 01-100 [English]" {
		t.Errorf("unexpected title: %s", item0.Title)
	}
	if item0.Seeders != "50" {
		t.Errorf("expected 50 seeders, got %s", item0.Seeders)
	}
	if item0.InfoHash != "abcdef1234567890abcdef1234567890abcdef12" {
		t.Errorf("unexpected info hash: %s", item0.InfoHash)
	}
	if item0.Magnet != "magnet:?xt=urn:btih:abcdef1234567890" {
		t.Errorf("unexpected magnet URI: %s", item0.Magnet)
	}

	// Empty title item
	item2 := feed.Channel.Items[2]
	if item2.Title != "" {
		t.Errorf("expected empty title, got %s", item2.Title)
	}
}
