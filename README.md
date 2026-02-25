# Librarr

Self-hosted book, audiobook, and webnovel search and download manager. Searches multiple sources, downloads via direct HTTP or torrents, and auto-imports into your library.

## Legal & Educational Use

Librarr is intended for personal archival, research, and educational use.

- **Free sources work out of the box** — Project Gutenberg, Open Library, Standard Ebooks, and Librivox provide thousands of legally free books with no configuration needed.
- **Prowlarr integration** — intended for use with content you have legal access to (e.g. your own rips, public trackers for public domain works, or content you have purchased).
- **Users are responsible** for complying with copyright law in their jurisdiction. Copyright law varies by country.
- **The developers do not condone copyright infringement.** Do not use Librarr to download books you do not have the right to access.

## What It Does

Librarr searches for books across multiple sources simultaneously and downloads them through whatever method is available:

| Source | Type | Config Required |
|--------|------|-----------------|
| Anna's Archive | Direct EPUB download | None |
| Project Gutenberg | Public domain EPUBs | None |
| Open Library | Public domain EPUBs via Internet Archive | None |
| Standard Ebooks | High-quality public domain EPUBs | None |
| Web Novel Sites (7 sites) | Scrape chapters to EPUB | None (lncrawl for scraping) |
| Prowlarr Indexers | Torrent search | Prowlarr |
| AudioBookBay | Audiobook torrents | None (qBittorrent for download) |
| Librivox | Public domain audiobooks (MP3) | None |

After downloading, books are organized into Author/Title folders and auto-imported into your library apps (Calibre-Web, Kavita, and/or Audiobookshelf).

## Features

- **Multi-source search** — Anna's Archive, Prowlarr/torrent indexers, AudioBookBay, and 7 web novel sites searched in parallel
- **Smart download strategy** — For web novels: checks Anna's Archive for a pre-made EPUB first, falls back to chapter-by-chapter scraping only if needed
- **Link verification** — Validates that Anna's Archive results are actually downloadable before showing them
- **Post-processing pipeline** — Organize files into Author/Title folders, import into multiple library apps, track everything
- **Multi-library import** — Calibre-Web (via calibredb), Kavita (API scan), Audiobookshelf (API scan) — enable any combination
- **Library tracking** — SQLite-backed history of every book processed, with duplicate detection across searches
- **Activity log** — Full event feed of downloads, imports, file moves, and errors
- **Audiobook support** — Search and download audiobooks via Prowlarr indexers and AudioBookBay
- **Library browsing** — Browse your ebook and audiobook libraries with cover art directly in the UI
- **All integrations optional** — Works with zero config (Anna's Archive + web novel search), add integrations as you need them
- **Plugin sources** — Add new search sources by dropping a Python file into `sources/` — no core code changes needed
- **Settings UI** — Configure all integrations from the web interface with connection testing
- **Persistent downloads** — Download state survives container restarts (SQLite-backed)
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

Configuration can be done two ways:

1. **Settings UI** — Click the Settings tab in the web interface to configure integrations, test connections, and save settings (persisted to `/data/librarr/settings.json`)
2. **Environment variables** — Set env vars in `.env` or `docker-compose.yml` (these override UI settings)

### Integrations

| Integration | What It Enables | Required Env Vars |
|-------------|-----------------|-------------------|
| **Prowlarr** | Torrent search via your indexers | `PROWLARR_URL`, `PROWLARR_API_KEY` |
| **qBittorrent** | Torrent downloads + audiobook downloads | `QB_URL`, `QB_USER`, `QB_PASS` |
| **Calibre-Web** | Auto-import ebooks via calibredb | `CALIBRE_CONTAINER` |
| **Audiobookshelf** | Library browsing, audiobook scan + metadata match | `ABS_URL`, `ABS_TOKEN`, `ABS_LIBRARY_ID` |
| **Kavita** | Ebook import via library scan | `KAVITA_URL`, `KAVITA_API_KEY` |
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
KAVITA_URL=http://kavita:5000
KAVITA_API_KEY=your-opds-api-key
LNCRAWL_CONTAINER=lncrawl
ENABLED_TARGETS=calibre,audiobookshelf,kavita
```

## How Search Works

When you search for a book, Librarr queries all configured sources in parallel:

1. **Anna's Archive** — Searches for EPUB files, verifies each result is actually downloadable by checking libgen mirrors, sorts by file size (largest = most complete)
2. **Project Gutenberg** — Searches 70,000+ public domain ebooks via the Gutendex API, returns results with direct EPUB download links
3. **Open Library** — Searches Internet Archive's public domain collection, downloads EPUBs from archive.org
4. **Prowlarr** — Searches your configured torrent indexers for ebook category results
5. **Web Novel Sites** — Searches FreeWebNovel, AllNovelFull, NovelFull, NovelBin, LightNovelPub, ReadNovelFull, and BoxNovel in parallel, deduplicates results

For audiobook searches:
1. **AudioBookBay** — Scrapes audiobook torrents (requires qBittorrent for download)
2. **Librivox** — Searches 18,000+ free public domain audiobooks, downloads MP3 chapter files directly
3. **Prowlarr** — Searches your torrent indexers for audiobook category results (if configured)

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
| `/api/settings` | GET/POST | Read/save integration settings |
| `/api/test/prowlarr` | POST | Test Prowlarr connection |
| `/api/test/qbittorrent` | POST | Test qBittorrent connection |
| `/api/test/audiobookshelf` | POST | Test Audiobookshelf connection |
| `/api/test/kavita` | POST | Test Kavita connection |
| `/api/sources` | GET | List all loaded sources with metadata |
| `/api/search?q=...` | GET | Search all sources |
| `/api/search/audiobooks?q=...` | GET | Search audiobook sources |
| `/api/download` | POST | Unified download (auto-dispatches by source) |
| `/api/download/annas` | POST | Download from Anna's Archive |
| `/api/download/torrent` | POST | Send torrent to qBittorrent |
| `/api/download/novel` | POST | Download web novel |
| `/api/download/audiobook` | POST | Download audiobook torrent |
| `/api/downloads` | GET | List active downloads |
| `/api/library` | GET | Browse ebook library |
| `/api/library/audiobooks` | GET | Browse audiobook library |
| `/api/library/tracked` | GET | Paginated list of tracked downloads |
| `/api/activity` | GET | Paginated activity log |
| `/api/check-duplicate` | GET | Check if a source_id is already in library |

## Adding Custom Sources

You can add new search sources by creating a Python file in the `sources/` directory. The app auto-discovers all `Source` subclasses on startup.

### Direct Download Source

For sources that provide direct file downloads:

```python
# sources/example.py
import requests
from .base import Source

class ExampleSource(Source):
    name = "example"           # Unique ID
    label = "Example Site"     # Badge label in UI
    color = "#2ecc71"          # Badge color (hex)
    download_type = "direct"   # Framework manages the download job

    def search(self, query):
        # Return list of result dicts. Each must have "title".
        # Include any fields your download() method needs.
        return [{"title": "My Book", "file_url": "https://..."}]

    def download(self, result, job):
        # Called in a background thread. Update job status as you go:
        job["detail"] = "Downloading..."
        # ... download the file ...
        job["status"] = "completed"
        job["detail"] = "Done!"
        return True
```

### Torrent Source

For sources that return torrent links (downloaded via qBittorrent):

```python
# sources/mytracker.py
from .base import Source

class MyTrackerSource(Source):
    name = "mytracker"
    label = "My Tracker"
    color = "#8e44ad"
    download_type = "torrent"  # Results sent to qBittorrent automatically
    config_fields = [          # Shows in Settings UI, stored in settings.json
        {"key": "api_key", "label": "API Key", "type": "text", "required": True},
    ]

    def search(self, query):
        api_key = self.get_config("api_key")
        # Return results with download_url, magnet_url, or info_hash
        return [{
            "title": "My Book",
            "seeders": 10,
            "size_human": "1.5 GB",
            "download_url": "https://...",
        }]
```

Sources with `config_fields` get a settings section automatically in the UI. Config values can also be set via environment variables: `SOURCE_MYTRACKER_API_KEY`.

See `sources/base.py` for the full API documentation.

## License

MIT
