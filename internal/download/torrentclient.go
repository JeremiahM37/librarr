package download

import "github.com/JeremiahM37/librarr/internal/config"

// TorrentClient is the common interface implemented by every torrent download
// backend (qBittorrent, Transmission). The rest of Librarr — the download
// Manager and the completion Watcher — talks to torrents exclusively through
// this interface so the active backend is a single configuration choice rather
// than a hard dependency.
//
// All implementations report torrent state using the qBittorrent state
// vocabulary so callers can keep using MapTorrentStatus uniformly. Listing and
// deletion are scoped to torrents Librarr itself added (qBittorrent categories,
// Transmission labels), so Librarr never touches a user's unrelated torrents.
type TorrentClient interface {
	// AddTorrent submits a torrent URL, torrent file URL, or magnet link.
	// savePath and category fall back to the client's configured defaults when
	// empty. expectedInfoHash is optional and used for post-add verification.
	AddTorrent(torrentURL, title, savePath, category, expectedInfoHash string) error
	// GetTorrents returns torrents Librarr added under the given category
	// (empty category returns all Librarr-scoped torrents).
	GetTorrents(category string) ([]TorrentInfo, error)
	// GetTorrentFiles lists the files contained in a torrent by hash.
	GetTorrentFiles(hash string) ([]TorrentFile, error)
	// DeleteTorrent removes a torrent by hash, optionally deleting its data.
	DeleteTorrent(hash string, deleteFiles bool) error
	// Diagnose reports connectivity/auth status for the settings "Test" button.
	Diagnose() map[string]interface{}
	// Name is the lowercase client identifier, e.g. "qbittorrent".
	Name() string
}

// Compile-time assertions that both backends satisfy the interface.
var (
	_ TorrentClient = (*QBittorrentClient)(nil)
	_ TorrentClient = (*TransmissionClient)(nil)
)

// SelectTorrentClient returns the active torrent backend per the configuration,
// or nil when no torrent client is configured. The selection rule lives in
// config.ActiveTorrentClient (explicit TORRENT_CLIENT override, else
// qBittorrent-preferred auto-detect).
func SelectTorrentClient(cfg *config.Config, qb *QBittorrentClient, tr *TransmissionClient) TorrentClient {
	switch cfg.ActiveTorrentClient() {
	case "qbittorrent":
		return qb
	case "transmission":
		return tr
	default:
		return nil
	}
}
