# Librarr Architecture

## Purpose

This document defines the target architecture for the next major evolution of Librarr.

The design is intentionally book-centric, API-first, Docker-friendly, and compatible with familiar *arr workflows. It is a blueprint rather than a claim that every component already exists.

## Architectural Principles

1. **Books are logical entities; files are physical representations.**
2. **Acquisition is provider-agnostic.** Indexers and download clients are adapters behind stable interfaces.
3. **Imports are idempotent.** Reprocessing the same completed download must not create duplicate files or books.
4. **Matching is conservative.** Librarr must avoid merging unrelated works or editions.
5. **Metadata has provenance.** Embedded, provider, and user-edited values should be distinguishable.
6. **The API reflects the domain.** Clients should work with books, contributors, series, editions, and files.
7. **Migrations are additive first.** Existing data remains readable until the replacement path has been validated.
8. **Librarr remains independently useful.** Media Assistant integration is important but not required for operation.

## Current Implementation Status

Librarr 2.0 now has the normalized schema, repository switch,
`LibraryService`, backfill engine, import planner/executor, feature-flagged v2
import engine, first-run setup, empty-library onboarding, library scanner,
review UI, explicit scan-result import, manual-review resolution, metadata
editing, OPDS 1.2, Wanted monitoring, stored release inspection, and manual
Wanted-to-download-client handoff.

Legacy compatibility remains intentionally available. Unfinished features such
as Devices, Activity, send-to-device, richer metadata providers, OPDS v2, and
external-library synchronization are hidden or documented as planned rather than
advertised as current behavior.

## High-Level System

```text
Web UI / Media Assistant / OPDS / API clients
                     │
                     ▼
               HTTP API layer
                     │
      ┌──────────────┼──────────────┐
      ▼              ▼              ▼
 Library domain   Acquisition    Delivery
      │              │              │
      ▼              ▼              ▼
 Metadata       Indexer clients  Device targets
 Import         Download clients OPDS/downloads
 Matching       Activity/history
      │              │              │
      └──────────────┼──────────────┘
                     ▼
              SQLite repository
                     │
                     ▼
          Managed library filesystem
```

## Domain Model

### books

Represents a logical title or volume.

Suggested fields:

```text
id
work_key
preferred_edition_id
media_type
monitor_status
metadata_status
created_at
updated_at
```

A book should not directly own every descriptive field if edition support is introduced immediately. The preferred edition provides the display metadata for most API and UI responses.

### editions

Represents a publication or metadata identity of a book.

Suggested fields:

```text
id
book_id
title
subtitle
description
series_id
series_position
publisher
published_date
language
page_count
metadata_json
metadata_source
created_at
updated_at
```

Why editions matter:

- ISBNs generally identify editions, not abstract works.
- EPUB and PDF files may represent different publications.
- Covers, subtitles, publishers, and publication dates may differ.
- A future quality upgrade may replace one edition with another.

The first implementation may use one edition per book, but the schema should not make later edition support unnecessarily difficult.

### contributors

Represents people or organizations associated with a book.

```text
id
name
sort_name
normalized_name
created_at
updated_at
```

### edition_contributors

Joins editions to contributors.

```text
edition_id
contributor_id
role
position
```

Roles may include:

- author
- editor
- translator
- illustrator
- narrator
- contributor

### series

```text
id
name
normalized_name
description
metadata_json
created_at
updated_at
```

Series position belongs on the edition or a dedicated join because a title can participate in more than one sequence in future versions.

### files

Represents a physical file managed by Librarr.

```text
id
book_id
edition_id
file_path
original_path
file_format
file_size
content_hash
source_type
source_id
import_status
embedded_metadata_json
created_at
updated_at
```

Recommended file-level uniqueness strategy:

- unique canonical managed path
- indexed content hash
- optional uniqueness on content hash plus size
- no global uniqueness on format, because two editions can share a format

### identifiers

Stores external identifiers with explicit scope and provenance.

```text
id
book_id
edition_id
identifier_type
identifier_value
source
created_at
```

Examples:

- ISBN-10
- ISBN-13
- ASIN
- Open Library work ID
- Open Library edition ID
- Google Books volume ID
- provider-specific IDs

### covers

```text
id
edition_id
storage_path
source_url
source_name
content_hash
width
height
created_at
updated_at
```

Cover files should be cached locally. API responses should expose Librarr-owned URLs rather than requiring clients to depend directly on provider URLs.

### tags

Tags remain user-defined and attach to logical entities rather than files.

Suggested joins:

- book_tags
- contributor_tags, if author monitoring later needs it

### activity and history

Operational events should remain distinct from domain data.

Examples:

- search initiated
- release grabbed
- download added
- download completed
- import succeeded
- import rejected
- metadata refreshed
- file deleted
- device delivery attempted

History should retain enough external IDs to correlate with Prowlarr and download clients after configuration changes.

## Matching and Identity

### File identity

File deduplication remains technical and deterministic where possible.

Priority signals:

1. canonical managed path
2. content hash
3. file size plus stable source identifier
4. source download ID plus relative path

A new format of an existing book is not a duplicate file.

### Book identity

Book matching must be conservative and confidence-based.

Strong signals:

- matching trusted work identifier
- matching edition identifier with known work relationship
- matching ISBN with compatible metadata

Fallback signals:

- normalized title
- normalized primary author
- media type
- series and sequence
- publication metadata

Rules:

- Different media types do not merge automatically.
- Same title with different authors does not merge automatically.
- Missing author should require stronger supporting evidence.
- Similar titles alone are insufficient.
- Ambiguous candidates create separate books or a review item.
- User-confirmed merges and splits override automatic decisions.

The matching service should return both a decision and evidence, not only a book ID.

## Metadata Model

Metadata should be separated into three layers:

1. **Embedded metadata** from the imported file.
2. **Provider metadata** from external services.
3. **User overrides** that take precedence until explicitly cleared.

A resolved display value should be produced by policy rather than destructively overwriting every source value.

Example precedence:

```text
user override
  > trusted provider selection
  > embedded metadata
  > filename fallback
```

Each important field should retain provenance when practical.

## Import Pipeline

```text
Completed download, manual import, or library scan review
             │
             ▼
      Discover candidate files
             │
             ▼
      Validate supported formats
             │
             ▼
 Extract hashes and embedded metadata
             │
             ▼
       Identify edition/book
             │
             ▼
        Build ImportPlan
             │
      ┌──────┴─────────┐
      ▼                ▼
 ready to import   manual review/conflict
      │                │
      │          metadata editor
      │      (manual fields + live preview)
      │                │
      └───────┬────────┘
              ▼
      explicit user import
              │
              ▼
        organize/attach file
              │
              ▼
        Create file row
              │
              ▼
 metadata/cover enrichment
              │
              ▼
history, notifications, integrations
```

The metadata editor is part of the scan review flow rather than a separate
pipeline. It stores user edits on the scan candidate, sends them as explicit
manual metadata overrides when import starts, and never modifies the source
file. The first implementation is offline-only: no internet metadata provider
is called from the editor.

Local cover handling follows the same ownership rule. Librarr may extract
embedded artwork from files the user already owns, cache it under the data
directory, and attach a normalized `covers` row through `LibraryService`.
External cover downloads remain a separate metadata-provider milestone.

### Import guarantees

- Re-running an import does not duplicate the same physical file.
- A new format may attach to an existing book.
- Failed organization leaves enough state for retry and diagnosis.
- Database writes and filesystem moves are coordinated to avoid orphaning either side.
- The source download is not removed until import success is durable.
- Library scanning is discovery-only; it never imports until the user explicitly
  chooses Import Selected or Import All Ready.

## Download Client Architecture

Librarr should support download clients through a common interface modeled after *arr behavior.

Suggested capabilities:

```go
type DownloadClient interface {
    Kind() string
    TestConnection(ctx context.Context) error
    Add(ctx context.Context, request AddRequest) (DownloadReference, error)
    Get(ctx context.Context, id string) (DownloadStatus, error)
    List(ctx context.Context, filter DownloadFilter) ([]DownloadStatus, error)
    Remove(ctx context.Context, id string, deleteData bool) error
}
```

The exact Go interface should be designed around current application needs rather than copied mechanically from this example.

Initial adapters:

- qBittorrent
- Transmission
- rTorrent
- Deluge

Future adapters:

- SABnzbd
- NZBGet

The application should support multiple configured clients, not only one global client. A client record should include:

- name
- type
- enabled state
- priority
- protocol settings
- authentication
- category
- optional default save path
- tags or labels where supported

Credentials must not be returned unmasked by normal API endpoints.

## Indexer Architecture

Librarr should consume indexers through:

- Prowlarr
- Torznab
- Newznab

Indexer results should normalize into an internal release model containing:

- title
- protocol
- download URL or identifier
- indexer identity
- size
- seeders and peers where applicable
- age
- categories
- parsed author/title/series/format signals
- raw provider data for diagnostics

Release scoring should remain separate from transport-specific parsing.

## Quality and Release Profiles

A quality profile should rank formats and attributes for a monitored book.

Examples:

- EPUB preferred over MOBI
- allow PDF but do not automatically upgrade to it
- require unabridged audiobook
- prefer retail metadata
- reject scans below configured quality

A release profile should handle preferred and excluded terms.

Quality upgrades should add or replace files according to explicit policy, not silently remove a usable format merely because a preferred format arrived.

## API Design

### General conventions

- Version new domain APIs under `/api/v1`.
- Use stable book IDs and file IDs with explicit resource names.
- Return structured errors with machine-readable codes.
- Preserve pagination, filtering, sorting, and search conventions.
- Keep authentication consistent across UI and external clients.
- Document endpoints using OpenAPI when the API stabilizes.

### Example resources

```text
GET    /api/v1/books
POST   /api/v1/books
GET    /api/v1/books/{bookID}
PATCH  /api/v1/books/{bookID}
DELETE /api/v1/books/{bookID}

GET    /api/v1/books/{bookID}/files
GET    /api/v1/files/{fileID}
GET    /api/v1/files/{fileID}/download
DELETE /api/v1/files/{fileID}

GET    /api/v1/contributors
GET    /api/v1/series
GET    /api/v1/activity
GET    /api/v1/history

GET    /api/v1/download-clients
POST   /api/v1/download-clients
POST   /api/v1/download-clients/{id}/test
```

### Compatibility

Existing endpoints should remain available during migration.

A compatibility adapter may expose a preferred file at the top level while adding:

```json
{
  "id": 12,
  "book_id": 12,
  "title": "The Guardian's Path",
  "author": "Carla Jablonski",
  "file_format": "epub",
  "formats": ["epub", "mobi"],
  "files": [
    {"id": 31, "format": "epub"},
    {"id": 32, "format": "mobi"}
  ]
}
```

Compatibility fields should be documented as transitional rather than treated as the permanent domain model.

## OPDS

Librarr preserves OPDS 1.2 compatibility for reader apps while leaving OPDS 2.0
as a future delivery milestone.

OPDS reads from the selected normalized library service, not from legacy
`library_items` when normalized repository mode is active. A logical book is
represented as one catalog entry, with one acquisition link per available
compatible file. Downloads use stable normalized file IDs:

```text
/opds/download/{file_id}
```

The download handler resolves the file through `LibraryService`, verifies that
it belongs to a known book, confines the path to configured library roots, sets
safe content headers, and streams the file without loading it into memory.

OPDS must not invent duplicate catalog entries solely because multiple formats exist.

Authentication uses HTTP Basic Auth against existing Librarr users because many
OPDS clients cannot use browser sessions. Disabled users are rejected. API-key
access remains available for scripted clients. Generated absolute URLs honor
trusted reverse-proxy headers only when the request comes from a configured
trusted proxy.

Cover links reuse existing local cover records. OPDS does not extract or
download covers on its own.

## Device Delivery

Device delivery should be modeled as a destination plus a policy.

A destination may define:

- type
- display name
- endpoint or email
- supported formats
- preferred format order
- size limit
- conversion allowance
- authentication reference

Example destinations:

- direct browser download
- Kindle email delivery
- watched folder
- Kobo integration
- Apple Books export workflow

The delivery service receives a book ID, resolves the best eligible file, records the attempt, and reports success or failure.

## Storage Layout

The managed library should remain configurable through root folders.

A naming engine should operate from domain data and produce deterministic paths such as:

```text
{Author Sort}/{Title} ({Year})/{Title} - {Author}.{ext}
```

Requirements:

- sanitize unsafe path characters
- avoid collisions
- support dry-run previews
- preserve original source path in file metadata
- allow ebooks, audiobooks, and manga to use different roots and naming templates

## Security

- Passwords and API secrets are never logged.
- Download-client and provider credentials are encrypted at rest when a practical key-management approach is defined; until then, access is restricted and values are masked in APIs.
- API keys support rotation.
- File download endpoints validate ownership and path boundaries.
- Imported archives and metadata parsers are treated as untrusted input.
- Filesystem operations prevent traversal outside configured roots.

## Observability

Librarr should provide:

- structured application logs
- health checks for database, filesystem, indexers, and download clients
- activity queue visibility
- import rejection reasons
- migration status
- Prometheus metrics where useful

Diagnostics should be useful without requiring permanent verbose instrumentation
in normal operation. Prowlarr and qBittorrent now use the shared diagnostics
pipeline to report configuration, URL validation, DNS, TCP, TLS/HTTPS,
authentication, API validation, version, and latency, with actionable next steps
and without exposing secrets. Future services should reuse the same structured
result model.

## Migration from library_items

The first schema transition should be additive.

### Phase 1: create new tables

Create new domain tables without removing `library_items`.

### Phase 2: backfill

For every existing item:

1. Preserve the original row unchanged.
2. Compute conservative book and edition candidates.
3. Create or reuse contributor, series, book, and edition records.
4. Insert one file record.
5. Store migration linkage for validation and rollback.

### Phase 3: compatibility reads

Build repository methods that read the new model and emit the existing response shape where needed.

### Phase 4: dual validation

Compare old and new counts, paths, hashes, formats, and displayed metadata. Do not rely only on row totals.

### Phase 5: switch writes

New imports write to the normalized model. A temporary compatibility write path may remain if necessary.

### Phase 6: retire legacy storage

Only remove or convert `library_items` after:

- migration tests pass
- real imports have been validated
- rollback has been exercised
- all production queries use the new repositories

## Testing Strategy

Minimum architecture fixtures:

- same book in EPUB and MOBI
- same title by different authors
- same work in multiple editions
- EPUB plus PDF
- missing author
- conflicting embedded and provider metadata
- manga in CBZ and CBR
- duplicate content under different paths
- repeated completed-download event
- download client temporarily unavailable
- failed filesystem move followed by retry

Tests should cover migrations, repository behavior, matching decisions, API compatibility, and import idempotency.

## Open Decisions

These should be resolved through focused design tasks before implementation reaches them:

- whether works and editions are separate in the first migration or staged later
- how user-confirmed merges and splits are represented
- how audiobooks with many track files map to one edition/file set
- whether comics use the same edition model or a specialized volume concept
- which metadata providers are enabled by default
- how secrets are encrypted and backed up
- when format conversion becomes part of delivery

## Architectural Boundary with Media Assistant

Librarr owns:

- book-domain metadata
- monitoring and acquisition state
- file inventory
- indexer and download-client workflows
- OPDS and device delivery

Media Assistant owns:

- cross-media discovery
- conversational interaction
- household-level presentation
- recommendations and orchestration across services

Media Assistant should consume public Librarr APIs and must not query Librarr's database directly.
