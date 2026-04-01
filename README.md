# Librarr

Self-hosted book, audiobook, and manga search and download manager.

Librarr aggregates 13 search sources into a single interface, handles downloads via qBittorrent or SABnzbd, and automatically organizes files into your Calibre, Audiobookshelf, Kavita, or Komga libraries. It exposes a Torznab/Newznab API so it can act as an indexer in Prowlarr or Readarr, and an OPDS 1.2 feed for e-readers.

## Features

- **13 search sources** in one UI (see table below)
- **Torznab/Newznab API** -- add Librarr as an indexer in Prowlarr, Readarr, or any Torznab-compatible app
- **OPDS 1.2 feed** -- browse and download books from any e-reader or OPDS client
- **Dual download clients** -- qBittorrent (torrents) and SABnzbd (Usenet/NZB) with configurable priority
- **Post-download pipeline** -- organize files, import into Calibre/Audiobookshelf/Kavita/Komga, track in SQLite
- **Torrent completion watcher** -- background goroutine polls qBittorrent, auto-imports completed downloads
- **EPUB verification** -- checks title word overlap to detect wrong-book downloads
- **Multi-user auth** -- session login with bcrypt passwords, TOTP 2FA, admin/user roles
- **OIDC / SSO** -- OpenID Connect support for Authelia, Keycloak, Authentik, etc.
- **Series grouping** -- groups related books/volumes in the library view
- **Wishlist management** -- save searches and track wanted items
- **Per-source rate limiting** -- avoids bans with configurable circuit breakers
- **Prometheus metrics** at `/metrics`
- **CSV bulk import** -- import book lists from CSV files
- **Modern dark UI** -- Tailwind CSS, responsive, single-page app
- **Single static binary** -- ~17 MB, zero CGO, pure-Go SQLite (`modernc.org/sqlite`)
- **Docker-ready** -- minimal Alpine image, runs as non-root user

## Search Sources

| Source | Type | Content |
|--------|------|---------|
| Anna's Archive | Direct download | Ebooks (EPUB, PDF, MOBI) |
| Anna's Archive (manga) | Direct download | Manga volumes |
| Prowlarr (ebooks) | Torrent | Ebooks via configured indexers |
| Prowlarr (audiobooks) | Torrent | Audiobooks via configured indexers |
| Prowlarr (manga) | Torrent | Manga via configured indexers |
| AudioBookBay | Torrent | Audiobooks |
| Project Gutenberg | Direct download | Public domain ebooks |
| Open Library | Direct download | Borrowable ebooks |
| Standard Ebooks | Direct download | Free, high-quality ebooks |
| Librivox | Direct download | Free public domain audiobooks |
| MangaDex | Direct download | Manga chapters |
| Nyaa | Torrent | Manga, light novels |
| Web Novels (7 sites) | Scraping (lncrawl) | Web novels compiled to EPUB |

## Screenshots

<!-- Add screenshots of the search, library, and download views here -->

## Quick Start

### Docker (recommended)

```yaml
services:
  librarr:
    build: .
    ports:
      - "5050:5050"
    volumes:
      - ./data:/data
      - /path/to/ebooks:/books/ebooks
      - /path/to/audiobooks:/books/audiobooks
      - /path/to/manga:/books/manga
    environment:
      - AUTH_USERNAME=admin
      - AUTH_PASSWORD=changeme
      - API_KEY=your-api-key-here
      - QB_URL=http://qbittorrent:8080
      - QB_USER=admin
      - QB_PASS=changeme
      - PROWLARR_URL=http://prowlarr:9696
      - PROWLARR_API_KEY=your-prowlarr-api-key
    restart: unless-stopped
```

```bash
docker compose up -d
```

### Binary

```bash
# Build
go build -o librarr ./cmd/librarr/

# Configure
export AUTH_USERNAME=admin
export AUTH_PASSWORD=changeme
export QB_URL=http://localhost:8080
# ... set other env vars as needed

# Run
./librarr
```

Open `http://localhost:5050` in your browser.

## Configuration

All configuration is via environment variables. Every variable has a sensible default.

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `LIBRARR_PORT` | `5050` | HTTP listen port |
| `LIBRARR_DB_PATH` | `/data/librarr.db` | SQLite database path |
| `SETTINGS_FILE` | `/data/settings.json` | Persistent settings file |

### Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_USERNAME` | | Login username (enables session auth) |
| `AUTH_PASSWORD` | | Login password |
| `API_KEY` | | API key for programmatic access (`X-Api-Key` header or `?apikey=` param) |

### OIDC / SSO

| Variable | Default | Description |
|----------|---------|-------------|
| `OIDC_ENABLED` | `false` | Enable OpenID Connect login |
| `OIDC_PROVIDER_NAME` | `SSO` | Button label on login page |
| `OIDC_ISSUER` | | OIDC issuer URL |
| `OIDC_CLIENT_ID` | | OAuth2 client ID |
| `OIDC_CLIENT_SECRET` | | OAuth2 client secret |
| `OIDC_REDIRECT_URI` | | Callback URL (`https://librarr.example.com/auth/oidc/callback`) |
| `OIDC_AUTO_CREATE_USERS` | `true` | Auto-create users on first OIDC login |
| `OIDC_DEFAULT_ROLE` | `user` | Default role for OIDC-created users |

### qBittorrent

| Variable | Default | Description |
|----------|---------|-------------|
| `QB_URL` | | qBittorrent Web UI URL |
| `QB_USER` | `admin` | qBittorrent username |
| `QB_PASS` | | qBittorrent password |
| `QB_SAVE_PATH` | `/downloads` | Ebook download path (inside qBit container) |
| `QB_CATEGORY` | `librarr` | Torrent category for ebooks |
| `QB_AUDIOBOOK_SAVE_PATH` | `/audiobooks-incoming` | Audiobook download path |
| `QB_AUDIOBOOK_CATEGORY` | `audiobooks` | Torrent category for audiobooks |
| `QB_MANGA_SAVE_PATH` | `/manga-incoming` | Manga download path |
| `QB_MANGA_CATEGORY` | `manga` | Torrent category for manga |
| `QB_PRIORITY` | `1` | Download client priority (lower = preferred) |

### SABnzbd (Usenet)

| Variable | Default | Description |
|----------|---------|-------------|
| `SABNZBD_URL` | | SABnzbd URL |
| `SABNZBD_API_KEY` | | SABnzbd API key |
| `SABNZBD_CATEGORY` | `librarr` | NZB download category |
| `SAB_PRIORITY` | `2` | Download client priority |

### Prowlarr

| Variable | Default | Description |
|----------|---------|-------------|
| `PROWLARR_URL` | | Prowlarr URL |
| `PROWLARR_API_KEY` | | Prowlarr API key |

### Library Imports

| Variable | Default | Description |
|----------|---------|-------------|
| `CALIBRE_LIBRARY_PATH` | | Path to Calibre library (auto-import via `calibredb`) |
| `CALIBRE_URL` | | Calibre-Web URL |
| `KAVITA_URL` | | Kavita server URL |
| `KAVITA_USER` | | Kavita username |
| `KAVITA_PASS` | | Kavita password |
| `KAVITA_LIBRARY_PATH` | | Kavita ebook library path |
| `KAVITA_MANGA_LIBRARY_PATH` | | Kavita manga library path |
| `KAVITA_PUBLIC_URL` | | Kavita URL for external links |
| `ABS_URL` | | Audiobookshelf server URL |
| `ABS_TOKEN` | | Audiobookshelf API token |
| `ABS_LIBRARY_ID` | | Audiobookshelf audiobook library ID |
| `ABS_EBOOK_LIBRARY_ID` | | Audiobookshelf ebook library ID |
| `ABS_PUBLIC_URL` | | Audiobookshelf URL for external links |
| `KOMGA_URL` | | Komga server URL |
| `KOMGA_USER` | | Komga username |
| `KOMGA_PASS` | | Komga password |
| `KOMGA_LIBRARY_ID` | | Komga library ID |
| `KOMGA_LIBRARY_PATH` | | Komga library path |

### File Organization

| Variable | Default | Description |
|----------|---------|-------------|
| `FILE_ORG_ENABLED` | `true` | Auto-organize downloaded files |
| `EBOOK_DIR` | `/books/ebooks` | Organized ebook destination |
| `AUDIOBOOK_DIR` | `/books/audiobooks` | Organized audiobook destination |
| `MANGA_DIR` | `/books/manga` | Organized manga destination |
| `INCOMING_DIR` | `/data/incoming` | Incoming file staging directory |
| `MANGA_INCOMING_DIR` | `/data/manga-incoming` | Manga incoming staging directory |

### Search / Downloads

| Variable | Default | Description |
|----------|---------|-------------|
| `ANNAS_ARCHIVE_DOMAIN` | `annas-archive.gl` | Anna's Archive domain (changes periodically) |
| `MIN_TORRENT_SIZE_BYTES` | `10000` | Minimum torrent size filter (10 KB) |
| `MAX_TORRENT_SIZE_BYTES` | `2000000000` | Maximum torrent size filter (2 GB) |
| `MAX_RETRIES` | `2` | Download retry attempts |
| `RETRY_BACKOFF_SECONDS` | `60` | Seconds between retries |
| `CIRCUIT_BREAKER_THRESHOLD` | `3` | Failures before disabling a source |
| `CIRCUIT_BREAKER_TIMEOUT` | `300` | Seconds before re-enabling a tripped source |

### Feature Toggles

| Variable | Default | Description |
|----------|---------|-------------|
| `RATE_LIMIT_ENABLED` | `true` | Per-source rate limiting |
| `METRICS_ENABLED` | `true` | Prometheus metrics endpoint |
| `WEBNOVEL_ENABLED` | `true` | Web novel search (requires lncrawl container) |
| `MANGADEX_ENABLED` | `true` | MangaDex search |

### Torznab

| Variable | Default | Description |
|----------|---------|-------------|
| `TORZNAB_API_KEY` | | API key for the Torznab endpoint |

## API Endpoints

### Authentication

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/login` | Session login |
| POST | `/api/login/totp` | TOTP 2FA verification |
| POST | `/api/register` | Register new user |
| POST | `/api/logout` | End session |
| GET | `/api/auth/status` | Current auth state |

### User Management (admin only)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/users` | List all users |
| PATCH | `/api/users/{id}` | Update user role/status |
| DELETE | `/api/users/{id}` | Delete user |

### TOTP 2FA

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/totp/setup` | Generate TOTP secret + QR code |
| POST | `/api/totp/verify` | Verify and enable TOTP |
| POST | `/api/totp/disable` | Disable TOTP |
| GET | `/api/totp/status` | Check if TOTP is enabled |

### OIDC / SSO

| Method | Path | Description |
|--------|------|-------------|
| GET | `/auth/oidc/login` | Initiate OIDC login flow |
| GET | `/auth/oidc/callback` | OIDC callback handler |

### Search

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/search?q=` | Search ebooks across all sources |
| GET | `/api/search/audiobooks?q=` | Search audiobooks |
| GET | `/api/search/manga?q=` | Search manga |

### Downloads

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/download` | Download a direct-download result |
| POST | `/api/download/torrent` | Download a torrent result |
| POST | `/api/download/annas` | Download from Anna's Archive |
| POST | `/api/download/audiobook` | Download an audiobook |
| GET | `/api/downloads` | List active/completed downloads |
| DELETE | `/api/downloads/torrent/{hash}` | Remove a torrent download |
| DELETE | `/api/downloads/novel/{jobID}` | Remove a novel download job |
| POST | `/api/downloads/clear` | Clear finished downloads |
| POST | `/api/downloads/jobs/{id}/retry` | Retry a failed download |

### Library

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/library` | List ebooks in library |
| GET | `/api/library/audiobooks` | List audiobooks in library |
| GET | `/api/library/manga` | List manga in library |
| DELETE | `/api/library/book/{id}` | Remove ebook from library |
| DELETE | `/api/library/audiobook/{id}` | Remove audiobook from library |
| GET | `/api/stats` | Library statistics |
| GET | `/api/activity` | Recent activity log |

### Wishlist

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/wishlist` | List wishlist items |
| POST | `/api/wishlist` | Add item to wishlist |
| DELETE | `/api/wishlist/{id}` | Remove from wishlist |

### Configuration

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/sources` | List search sources and status |
| GET | `/api/config` | Current configuration summary |
| GET | `/api/settings` | Get persistent settings |
| POST | `/api/settings` | Update persistent settings |
| GET | `/api/check-duplicate` | Check if a book already exists |

### Connection Tests

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/test/prowlarr` | Test Prowlarr connection |
| POST | `/api/test/qbittorrent` | Test qBittorrent connection |
| POST | `/api/test/audiobookshelf` | Test Audiobookshelf connection |
| POST | `/api/test/kavita` | Test Kavita connection |
| POST | `/api/test/sabnzbd` | Test SABnzbd connection |

### Import

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/import/csv` | Bulk import books from CSV |

### System

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

## Torznab / Newznab API

Librarr exposes a standard Torznab API at `/torznab/api` that can be added as an indexer in Prowlarr, Readarr, or any Torznab-compatible application.

**Setup in Prowlarr / Readarr:**

1. Go to Settings > Indexers > Add
2. Select "Generic Torznab" (or "Generic Newznab")
3. Set the URL to `http://your-librarr-host:5050/torznab/api`
4. Set the API Key to your `TORZNAB_API_KEY` value
5. Test and save

**Capabilities:** `GET /torznab/api?t=caps` returns the supported search categories and capabilities.

## OPDS Feed

Librarr serves an OPDS 1.2 catalog at `/opds` for e-reader apps (KOReader, Moon+ Reader, Librera, etc.).

**Endpoints:**

| Path | Description |
|------|-------------|
| `/opds` | Catalog root |
| `/opds/books` | Browse all books |
| `/opds/search?q=` | Search the catalog |
| `/opds/download/{id}` | Download a book file |
| `/opds/opensearch.xml` | OpenSearch descriptor |

**Setup in your e-reader:**

1. Add a new OPDS catalog
2. Set the URL to `http://your-librarr-host:5050/opds`
3. If auth is enabled, enter your Librarr username and password

## Architecture

Single static binary, zero CGO dependencies, pure-Go SQLite via `modernc.org/sqlite`.

```
cmd/librarr/main.go            Entry point
internal/
  config/config.go              Env var configuration
  db/                           SQLite persistence + migrations
  models/                       Core types (books, downloads, wishlist)
  api/                          HTTP handlers, router, middleware
    auth.go                     Session auth + bcrypt
    totp.go                     TOTP 2FA (RFC 6238)
    oidc.go                     OpenID Connect / SSO
    search.go                   Search endpoint handlers
    download.go                 Download management
    library.go                  Library CRUD
    opds.go                     OPDS 1.2 feed
    metrics.go                  Prometheus metrics
    csv.go                      CSV bulk import
    ratelimit.go                Per-source rate limiting
    router.go                   Route registration
  search/                       Search source implementations
  download/                     Download manager (qBit + SABnzbd)
  organize/                     Post-download file organization + library import
  torznab/                      Torznab/Newznab API handler
web/
  index.html                    Single-page web UI (Tailwind CSS)
Dockerfile                      Multi-stage Alpine build
```

## License

MIT

## Disclaimer

This software is provided for **educational and personal use only**. Users are responsible for ensuring their use complies with all applicable laws and regulations in their jurisdiction. The developers do not condone or encourage copyright infringement or any illegal activity. This tool does not host, store, or distribute any copyrighted content.
