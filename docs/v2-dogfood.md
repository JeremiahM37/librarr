# Librarr 2.0 Dogfood Deployment

Warning: this is a development-only deployment for local dogfooding. It is
designed to run beside an existing Librarr 1.x instance without sharing state.

## Goals

The dogfood stack is isolated from production:

- separate database and settings storage
- separate organized library paths
- separate incoming/test-download paths
- separate container name
- separate host port
- explicit Librarr 2.0 feature flags

The repository keeps the stable production compose file unchanged. Dogfooding
uses [docker-compose.v2-dogfood.yml](../docker-compose.v2-dogfood.yml).

## Current port layout

The checked-in production compose file maps:

- `5050:5050`

The dogfood stack maps:

- `5051:5050`

## Feature flags

The dogfood deployment enables:

- `LIBRARR_LIBRARY_REPOSITORY_MODE=normalized`
- `LIBRARR_IMPORT_ENGINE=v2`

## Host paths

The checked-in dogfood compose file uses explicit host paths suitable for a
Portainer/homelab deployment:

- `/opt/docker/compose/librarr-v2/data` → `/data`
- `/opt/docker/compose/librarr-v2/library/ebooks` → `/books/ebooks`
- `/opt/docker/compose/librarr-v2/library/audiobooks` → `/books/audiobooks`
- `/opt/docker/compose/librarr-v2/library/manga` → `/books/manga`
- `/opt/docker/compose/librarr-v2/incoming` → `/data/incoming`
- `/opt/docker/compose/librarr-v2/manga-incoming` → `/data/manga-incoming`

This avoids sharing the production database, settings file, or organized
library paths.

`INCOMING_DIR` and `MANGA_INCOMING_DIR` are Librarr-local staging paths inside
the dogfood container. `QB_SAVE_PATH`, `QB_AUDIOBOOK_SAVE_PATH`, and
`QB_MANGA_SAVE_PATH` are remote paths as seen by qBittorrent and should not be
set to `/data/incoming` unless qBittorrent can actually write to that same
container path.

For local-only development, use equivalent paths under a disposable local
directory and update the compose file in your private working copy.

## Docker networking

The homelab deployment uses an external Docker network named `homelab-media` so
Librarr can reach optional services by container hostname.

Compose deployments should attach Librarr to:

```yaml
networks:
  - homelab-media

networks:
  homelab-media:
    external: true
```

Prefer Docker hostnames such as:

```text
http://prowlarr:9696
```

over hard-coded IP addresses. qBittorrent may remain remote over HTTPS.

## Fresh normalized startup behavior

Normalized startup now distinguishes two cases:

1. fresh normalized install
   - schema migrations complete
   - `library_items` contains zero rows
   - startup is allowed without fabricating a completed backfill

2. migrated legacy install
   - `library_items` contains one or more rows
   - normalized startup still requires the existing backfill and validation
     readiness checks

This keeps migration safeguards intact for real upgrades while allowing a clean
dogfood database to boot.

## Prerequisites

- Docker
- Docker Compose v2
- `curl`
- `python3`

## Build only

```bash
docker compose -f docker-compose.v2-dogfood.yml config -q
docker compose -f docker-compose.v2-dogfood.yml build
```

The built image tag is:

```text
librarr:v2-dogfood
```

It is local-only unless you explicitly retag and push it.

## Start

```bash
mkdir -p /opt/docker/compose/librarr-v2/data \
         /opt/docker/compose/librarr-v2/library/ebooks \
         /opt/docker/compose/librarr-v2/library/audiobooks \
         /opt/docker/compose/librarr-v2/library/manga \
         /opt/docker/compose/librarr-v2/incoming \
         /opt/docker/compose/librarr-v2/manga-incoming

docker compose -f docker-compose.v2-dogfood.yml up -d
```

Open:

```text
http://127.0.0.1:5051
```

## Stop

```bash
docker compose -f docker-compose.v2-dogfood.yml down --remove-orphans
```

## Reset / remove all dogfood state

```bash
docker compose -f docker-compose.v2-dogfood.yml down --remove-orphans
rm -rf /opt/docker/compose/librarr-v2/data \
       /opt/docker/compose/librarr-v2/library \
       /opt/docker/compose/librarr-v2/incoming \
       /opt/docker/compose/librarr-v2/manga-incoming
```

## Contributor-friendly smoke test

A reproducible smoke script is included:

- [scripts/v2-dogfood-smoke.sh](../scripts/v2-dogfood-smoke.sh)

It performs:

1. clean reset of dogfood state
2. compose validation
3. image build
4. isolated startup
5. health wait
6. first-admin registration
7. normalized/v2 mode verification
8. EPUB import
9. MOBI import for the same logical book
10. logical-book + formats verification
11. manual metadata override
12. container restart
13. post-restart persistence verification

Run it with:

```bash
./scripts/v2-dogfood-smoke.sh
```

## Manual smoke checklist

If you want to walk the flow manually in the UI:

1. Build and start the dogfood stack.
2. Open `http://127.0.0.1:5051`.
3. Register the first user; it becomes admin automatically.
4. Confirm `/api/config` reports:
   - `library_repository_mode=normalized`
   - `import_engine=v2`
5. Configure Library & Import folders.
6. Click Scan Library.
7. Review Ready, Duplicate, Already Imported, Manual Review, Unsupported, and
   Unreadable sections.
8. Use Import Selected or Import All Ready.
9. Verify the review refreshes and imported books move to Already Imported.
10. If Prowlarr or qBittorrent are configured, run Connection Diagnostics and
    verify staged results render in Settings.
11. Restart the container.
12. Verify onboarding remains complete and imported books remain present.

## Health check

The compose file uses:

```text
GET /api/health
```

against container port `5050`.

## Known limitations

- This is not a production deployment.
- The dogfood stack intentionally does not inherit your production `.env`.
- The smoke script uses placeholder `.epub` and `.mobi` files to exercise the
  import pipeline, not full real-world metadata extraction.
- qBittorrent, Prowlarr, and other external integrations are optional and are
  not configured by default in the dogfood stack.
- If a contributor wants to test against a copied real library, they should use
  a copied or read-only source, not the production write destination.
