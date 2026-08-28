package torznab

import (
	"crypto/md5"
	"fmt"
	"time"

	"github.com/JeremiahM37/librarr/internal/models"
)

// ResultToItem converts a search result to a Torznab RSS item.
func ResultToItem(r models.SearchResult, baseURL string) models.TorznabItem {
	// Determine the category based on media type and source.
	category := "7020" // default: ebook
	switch {
	case r.Source == "audiobook" || r.Source == "prowlarr_audiobooks":
		category = "3030"
	case r.Source == "prowlarr_manga" || r.MediaType == "manga":
		category = "7030"
	}

	// Generate a stable GUID from the result.
	guid := r.GUID
	if guid == "" {
		if r.MD5 != "" {
			guid = r.MD5
		} else if r.InfoHash != "" {
			guid = r.InfoHash
		} else if r.SourceID != "" {
			guid = r.SourceID
		} else {
			guid = fmt.Sprintf("%x", md5.Sum([]byte(r.Title+r.Source)))
		}
	}

	item := models.TorznabItem{
		Title:    r.Title,
		GUID:     guid,
		Size:     r.Size,
		Category: categoryName(category),
		PubDate:  time.Now().UTC().Format(time.RFC1123Z),
		Attrs: []models.TorznabAttr{
			{Name: "category", Value: category},
		},
	}

	if r.Size > 0 {
		item.Attrs = append(item.Attrs, models.TorznabAttr{Name: "size", Value: fmt.Sprintf("%d", r.Size)})
	}

	if r.Seeders > 0 {
		item.Attrs = append(item.Attrs, models.TorznabAttr{Name: "seeders", Value: fmt.Sprintf("%d", r.Seeders)})
	}
	if r.Leechers > 0 {
		item.Attrs = append(item.Attrs, models.TorznabAttr{Name: "peers", Value: fmt.Sprintf("%d", r.Leechers)})
	}

	// A usenet result reaches this feed on the books and manga tabs, where
	// Prowlarr labels it with a torrent source string. Announcing its .nzb as
	// application/x-bittorrent would hand a downstream *arr app an NZB to feed
	// to its torrent client, so the enclosure follows the protocol the driver
	// resolved rather than the source label. The magnet branches below are
	// torrents by construction and are left alone.
	enclosureType := "application/x-bittorrent"
	if r.DownloadProtocol == "nzb" {
		enclosureType = "application/x-nzb"
	}

	// Set the download link.
	if r.MagnetURL != "" {
		item.Link = r.MagnetURL
		item.Enclosure = &models.TorznabEnclosure{
			URL:    r.MagnetURL,
			Length: r.Size,
			Type:   "application/x-bittorrent",
		}
	} else if r.DownloadURL != "" {
		item.Link = r.DownloadURL
		item.Enclosure = &models.TorznabEnclosure{
			URL:    r.DownloadURL,
			Length: r.Size,
			Type:   enclosureType,
		}
	} else if r.InfoHash != "" {
		magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s&dn=%s", r.InfoHash, r.Title)
		item.Link = magnet
		item.Enclosure = &models.TorznabEnclosure{
			URL:    magnet,
			Length: r.Size,
			Type:   "application/x-bittorrent",
		}
	} else if r.MD5 != "" {
		// For direct downloads (Anna's Archive), provide a download link via the Librarr API.
		downloadLink := fmt.Sprintf("%s/api/download/nzb/%s", baseURL, r.MD5)
		item.Link = downloadLink
	} else if r.EpubURL != "" {
		item.Link = r.EpubURL
	}

	return item
}

func categoryName(id string) string {
	switch id {
	case "7000":
		return "Books"
	case "7020":
		return "Books/Ebook"
	case "7030":
		return "Books/Comics"
	case "7040":
		return "Books/Magazines"
	case "7050":
		return "Books/Technical"
	case "3030":
		return "Audio/Audiobook"
	default:
		return "Books"
	}
}
