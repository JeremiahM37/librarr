# Librarr

Self-hosted book search and download manager. Searches multiple sources, downloads via direct HTTP or torrents, and auto-imports into your library.

## What It Does

Librarr searches for books across multiple sources simultaneously and downloads them through whatever method is available:

| Source | Type | Config Required |
|--------|------|-----------------|
| Anna's Archive | Direct EPUB download | None |
| Web Novel Sites (7 sites) | Scrape chapters to EPUB | None (lncrawl for scraping) |
| Prowlarr Indexers | Torrent search | Prowlarr |
| AudioBookBay | Audiobook torrents | None (qBittorrent for download) |

After downloading, books can be auto-imported into Calibre-Web and/or Audiobookshelf.

## Features

- **Multi-source search** — Anna's Archive, Prowlarr/torrent indexers, AudioBookBay, and 7 web novel sites searched in parallel
- **Smart download strategy** — For web novels: checks Anna's Archive for a pre-made EPUB first, falls back to chapter-by-chapter scraping only if needed
- **Link verification** — Validates that Anna's Archive results are actually downloadable before showing them
- **Auto-import** — Completed downloads automatically imported into Calibre-Web and trigger Audiobookshelf library scans
- **Audiobook support** — Search and download audiobooks via Prowlarr indexers and AudioBookBay
- **Library browsing** — Browse your ebook and audiobook libraries with cover art directly in the UI
- **All integrations optional** — Works with zero config (Anna's Archive + web novel search), add integrations as you need them
- **Dark UI** — Clean, responsive web interface

## Quick Start

```bash
# Clone the repo
git clone https://github.com/JeremiahM37/librarr.git
cd librarr

# Copy and edit the config
cp .env.example .env

# Run with Docker Compose
docker compose up -d
```

Open `http://localhost:5000` — Anna's Archive and web novel search work immediately with no configuration.

## Docker

### Build and run

```bash
docker build -t librarr .
docker run -d -p 5000:5000 --name librarr librarr
```

### Docker Compose (recommended)

See the included `docker-compose.yml` for a ready-to-use setup. Adjust the volume paths to match your media storage.

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` and uncomment what you need.

### Integrations

| Integration | What It Enables | Required Env Vars |
|-------------|-----------------|-------------------|
| **Prowlarr** | Torrent search via your indexers | `PROWLARR_URL`, `PROWLARR_API_KEY` |
| **qBittorrent** | Torrent downloads + audiobook downloads | `QB_URL`, `QB_USER`, `QB_PASS` |
| **Calibre-Web** | Auto-import ebooks via calibredb | `CALIBRE_CONTAINER` |
| **Audiobookshelf** | Library browsing, audiobook scan + metadata match | `ABS_URL`, `ABS_TOKEN`, `ABS_LIBRARY_ID` |
| **lightnovel-crawler** | Web novel chapter scraping to EPUB | `LNCRAWL_CONTAINER` |

### Minimal Setup (no integrations)

Just run the container — Anna's Archive search and web novel search work out of the box. Downloaded EPUBs are saved to `INCOMING_DIR` (`/data/media/books/ebooks/incoming` by default).

### Full Setup (all integrations)

```env
PROWLARR_URL=http://prowlarr:9696
PROWLARR_API_KEY=your-api-key
QB_URL=http://qbittorrent:8080
QB_USER=admin
QB_PASS=yourpassword
ABS_URL=http://audiobookshelf:80
ABS_TOKEN=your-abs-api-token
ABS_LIBRARY_ID=your-audiobook-library-id
ABS_EBOOK_LIBRARY_ID=your-ebook-library-id
CALIBRE_CONTAINER=calibre-web
LNCRAWL_CONTAINER=lncrawl
```

## How Search Works

When you search for a book, Librarr queries all configured sources in parallel:

1. **Anna's Archive** — Searches for EPUB files, verifies each result is actually downloadable by checking libgen mirrors, sorts by file size (largest = most complete)
2. **Prowlarr** — Searches your configured torrent indexers for ebook category results
3. **Web Novel Sites** — Searches FreeWebNovel, AllNovelFull, NovelFull, NovelBin, LightNovelPub, ReadNovelFull, and BoxNovel in parallel, deduplicates results

Results are filtered to remove junk (suspicious filenames, zero-seeder torrents, irrelevant titles) and sorted with direct downloads first.

## How Web Novel Download Works

When you download a web novel, Librarr uses a multi-strategy approach:

1. First checks Anna's Archive for a pre-made EPUB of that title (much faster)
2. If found, downloads directly and imports to Calibre
3. If not found, falls back to lightnovel-crawler to scrape all chapters from the source site
4. If one source site fails, automatically tries up to 3 alternative sites
5. Validates the resulting EPUB (checks for corruption, rejects suspiciously small files)
6. Imports to Calibre-Web and triggers Audiobookshelf library scan

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/config` | GET | Which integrations are enabled |
| `/api/search?q=...` | GET | Search all sources |
| `/api/search/audiobooks?q=...` | GET | Search audiobook sources |
| `/api/download/annas` | POST | Download from Anna's Archive |
| `/api/download/torrent` | POST | Send torrent to qBittorrent |
| `/api/download/novel` | POST | Download web novel |
| `/api/download/audiobook` | POST | Download audiobook torrent |
| `/api/downloads` | GET | List active downloads |
| `/api/library` | GET | Browse ebook library |
| `/api/library/audiobooks` | GET | Browse audiobook library |

## License

MIT
