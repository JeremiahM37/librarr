# Librarr 2.0 Backfill Design

This document designs the deterministic migration from legacy `library_items`
rows into the normalized Librarr 2.0 schema.

No data is migrated by this document. The production application continues to
read from and write to `library_items`.

## Legacy Data Inventory

`library_items` currently stores one row per imported physical item. For ebooks
this normally means one row per ebook file; for audiobooks it may mean one row
per audiobook directory.

| Field | Purpose | Source | Nullable? | Required? | Current usage | API usage | OPDS usage | UI usage | Importer usage |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `id` | Legacy row identity | SQLite autoincrement | No | Yes | Delete, tags, history references, OPDS download lookup, migration map key | Returned as `id`; delete endpoint accepts it | Used in OPDS entry ID and `/opds/download/{id}` | Used for delete buttons and item identity | Not supplied by importers |
| `title` | Display title for the imported item | Search result title, ebook metadata, filename fallback, audiobook metadata, manual import input | No, default `''` | Practically yes for display | Title search, listing, stats grouping indirectly, metadata backfill candidate detection | Returned as `title` | Entry title | Card/list title | Set by watcher, direct downloads, manual import, audiobook scanner |
| `author` | Display author/contributor text | Search result author, ebook metadata, filename fallback, audiobook artist, manual input | No, default `''` | No | Find-by-title author filtering, display, metadata backfill preservation | Returned as `author` | OPDS author element when non-empty | Card/list subtitle | Set by watcher, direct downloads, manual import, audiobook scanner |
| `file_path` | Managed local path or imported directory path | Organizer output, synchronized torrent path, manual import destination, scanner directory | No, default `''` | Yes for downloadable/imported items | Duplicate detection by canonical path, hashing, OPDS download, delete/download safety, metadata backfill | Returned as `file_path` | Used to serve download by legacy ID | Used to infer format when `format` is absent | Set by all import paths |
| `original_path` | Source path before organization or download source path | Manual import source, completed torrent path, direct download temporary/source path | No, default `''` | No | Metadata backfill torrent-derived-title detection; troubleshooting | Returned as `original_path` | Not used | Not directly used | Set by watcher/manual/direct paths when available |
| `file_size` | Size in bytes for the imported file or grouped directory | `os.Stat`, accumulated audiobook scan size, download metadata | No, default `0` | No | Stored for display/API; not part of current duplicate identity | Returned as `file_size` | Not used | Intended for size display; some UI paths currently read `size` from non-legacy sources | Set by importers when available |
| `file_format` | File/container format such as `epub`, `mobi`, `pdf`, `m4b`, `cbz` | File extension, manual import scan, metadata backfill | No, default `''` | Practically yes for file downloads | Duplicate detection scoped by format; OPDS MIME type; metadata backfill | Returned as `file_format` | Maps to MIME type and `<dc:format>` | UI infers display format from `format` or `file_path`; compatibility should preserve this | Set by importers when available |
| `media_type` | Library lane: ebook, audiobook, manga, comic | Search/download request, category, extension fallback, scanner | No, default `ebook` | Yes | Filtering, stats, duplicate detection scope, API tabs, OPDS counts | Returned as `media_type`; list filters use it | Filters OPDS ebook/audiobook views | Used for badges and tab separation | Set by all import paths; defaults to ebook |
| `source` | Acquisition/import source label | `torrent`, `annas`, `manual_import`, `scan`, provider name | No, default `''` | No | Troubleshooting and stats by source in DB tests; source provenance | Returned as `source` | Not used | Not directly used | Set by importers/download paths |
| `source_id` | Stable source/download identity | Torrent hash, scanner path key, provider ID, MD5/source ID | No, default `''` | No | Duplicate checks, delete by source ID, FindBookByIdentifier legacy adapter, scanner idempotency | Returned as `source_id` | Not used | Used indirectly when deleting external-source items | Set by importers when available |
| `metadata` | JSON metadata associated with the imported row | Ebook metadata, manual/direct/import metadata, backfill-enriched data | No, default `'{}'` | No | API serialization; metadata backfill may update title/author/format but does not rely on this field | Returned as parsed `metadata` when non-empty/non-`{}` | Not used | Available to clients; current UI does not heavily depend on it | Set only by paths that provide metadata; defaulted by DB |
| `content_hash` | SHA-256 content hash for file-level duplicate prevention | Importer or DB hash backfill from `file_path` | No, default `''` | No but important for idempotency | Duplicate detection across restarts; backfilled for existing files | Hidden from JSON | Not used | Not exposed | Computed by DB when possible |
| `added_at` | Import/listing timestamp as Unix seconds | SQLite default insert time | No | Yes | Ordering newest-first, stats recency indirectly | Returned as RFC3339 `added_at` | OPDS `<updated>` timestamp | Drives listing order through API | Not supplied by importers |

## Field Map

| Legacy field | Normalized destination | Notes |
| --- | --- | --- |
| `id` | `library_item_migration_map.library_item_id` | Durable backfill checkpoint and rollback/audit key. |
| `title` | `books.title`, `books.sort_title`, `editions.title` | Exact mapping depends on matching. A one-file/one-title legacy row initially creates or finds one logical book and one edition. |
| `author` | `contributors.name`, `contributors.sort_name`, `edition_contributors.role = 'author'` | Empty author creates no contributor. Multiple-author parsing is not part of the first deterministic backfill unless existing metadata clearly provides structure. |
| `file_path` | `files.file_path` | Preserve exact managed path; canonical path is used only for matching/idempotency checks. |
| `original_path` | `files.original_path` | Preserve for troubleshooting and provenance. |
| `file_size` | `files.file_size` | Preserve numeric value as-is. |
| `file_format` | `files.format` | Normalize case and trim a leading dot during lookup, but preserve semantic format. |
| `media_type` | `books.media_type`, `files.media_type` | Book-level media type is required for search/listing; file-level media type preserves current row filtering. |
| `source` | `files.source_type` | Preserve source label without interpreting it as a provider schema. |
| `source_id` | `files.source_id`; optionally `identifiers` when source type is a known stable provider | Always preserve on file. Only create identifiers for known stable types after explicit rules exist. |
| `metadata` | `files.embedded_metadata_json` | Additive schema column created to preserve the exact legacy JSON blob. |
| `content_hash` | `files.content_hash` | Preserve existing hash; compute only in a future backfill phase if the value is blank and file exists. |
| `added_at` | `files.imported_at`, and created/updated timestamps on inserted normalized records | Preserve import chronology without changing current listing behavior. |

## Identified Gaps

The existing normalized schema already had landing places for identity, title,
author relationships, media type, paths, size, format, source, source ID,
content hash, import time, and migration linkage.

The genuine gap was legacy `library_items.metadata`. Current API compatibility
can expose that parsed JSON. Without a normalized landing place, a future
cutover could lose cached or embedded metadata even if all files migrate
successfully.

## Minimal Schema Addition

Migration `2`, `librarr_2_file_metadata_json`, adds:

```sql
ALTER TABLE files
ADD COLUMN embedded_metadata_json TEXT NOT NULL DEFAULT '{}'
```

Rationale:

- The legacy row currently represents one physical item.
- The existing `metadata` field is attached to that row, not a normalized book.
- File-level storage preserves the data without forcing premature provider or
  user-override schema decisions.
- Default `'{}'` preserves compatibility for fresh databases and existing
  normalized rows.

No data is written by this migration.

## Backfill Architecture

The future backfill engine should run as an explicit task, not during ordinary
startup.

```text
legacy library_items row
  ↓
load or create migration-map checkpoint
  ↓
book lookup / create
  ↓
edition lookup / create
  ↓
contributor resolution
  ↓
series resolution
  ↓
identifier creation
  ↓
cover creation
  ↓
file attachment
  ↓
migration map update
  ↓
validation and report
```

### Stage 1: Legacy Row Load

Read legacy rows in deterministic order: ascending `id`.

Each row should be normalized into an in-memory candidate containing:

- trimmed title and author
- normalized title key
- normalized author key
- media type
- canonical file path when available
- content hash
- source/source ID
- parsed metadata JSON if valid

Invalid JSON in `metadata` should not block migration. Preserve the raw value
in the report and store `{}` only if the target column requires valid JSON.

### Stage 2: Migration Map Checkpoint

Check `library_item_migration_map` by `library_item_id`.

- `completed`: verify referenced normalized rows still exist, then skip.
- `pending` or `error`: resume from available normalized references.
- missing map row: start a new candidate.

The migration map is the durable idempotency source. In-memory maps are only
optimizations.

### Stage 3: Book Lookup

Preferred matching order:

1. Existing migration map with `book_id`.
2. Trusted identifier match, if a legacy source/source ID has an explicit rule.
3. Exact normalized title + primary author + media type.
4. Exact normalized title + media type when author is blank.
5. Create a new book.

Ambiguous matches should remain separate and be reported, not silently merged.

### Stage 4: Edition Lookup

Preferred matching order:

1. Existing migration map with `edition_id`.
2. Edition identifier match, if a trusted edition identifier exists.
3. Exact title + publisher/publication metadata when present.
4. Exact title under the selected book.
5. Create a new edition.

The first migration can create one edition per legacy book candidate unless
metadata gives stronger edition evidence.

### Stage 5: Contributor Resolution

If `author` is blank, skip contributor creation.

Otherwise:

1. Normalize the author string.
2. Look up contributor by case-insensitive name/sort name.
3. Create contributor if absent.
4. Attach to edition with role `author`.

The engine should not split multiple authors unless a future parser can do so
deterministically and safely.

### Stage 6: Series Resolution

Legacy `library_items` has no first-class series field. Series should only be
created if unambiguous structured metadata exists in `metadata`.

If no series metadata exists, skip series creation and record no warning.

### Stage 7: Identifier Creation

Always preserve `source` and `source_id` on `files`.

Create `identifiers` only for source/source ID combinations with explicit,
stable rules. Examples:

- `source = annas` with a known MD5/source ID can become a provider identifier.
- Open Library or Google Books IDs can be created when present in metadata.

Unknown `source_id` values remain file provenance, not book identity.

### Stage 8: Cover Creation

Legacy `library_items` has no dedicated cover fields. Create covers only when
metadata contains a trusted local cover path or safe provider URL that current
behavior already exposes.

Otherwise, skip cover creation and count it as informational, not an error.

### Stage 9: File Attachment

Attach one normalized `files` row per legacy row.

Use idempotency checks in this order:

1. Migration map `file_id`.
2. Exact canonical `file_path`.
3. Same `content_hash` + `media_type` + `format`.
4. Same `source_id` + `source_type` when both are non-empty and trusted.

The file row should preserve:

- `file_path`
- `original_path`
- `file_size`
- `format`
- `media_type`
- `source_type`
- `source_id`
- `content_hash`
- `embedded_metadata_json`
- `imported_at`

### Stage 10: Migration Map Update

Write or update `library_item_migration_map` only after the normalized book,
edition, and file references are known.

Statuses:

- `completed`: row migrated and validation passed.
- `skipped`: row intentionally not migrated, with reason.
- `error`: row failed with sanitized error.
- `ambiguous`: row requires manual review.

## Idempotency Strategy

The backfill must be safe to run repeatedly.

- Process rows by stable `library_items.id`.
- Use `library_item_migration_map` as the durable checkpoint.
- Wrap each legacy row in its own transaction.
- Never record `completed` until all referenced normalized rows exist.
- Use unique constraints and lookup-before-insert for files, identifiers, and
  covers.
- Resolve contributors by normalized name before insert.
- Resolve series by normalized title before insert.
- Treat duplicate file/content/hash matches as reuse, not failure, when they
  point to the same logical candidate.
- If a run is interrupted, restart from the map and revalidate references.

## Validation Plan

Validation should run after backfill and in dry-run mode.

Required checks:

- Legacy row count vs migration-map row count.
- Completed map rows have existing `book_id`, `edition_id`, and `file_id`.
- Every migrated file has an edition.
- Every migrated edition has a book.
- No file path is attached to more than one normalized file.
- No content hash creates duplicate files for the same media type and format
  unless explicitly reported as separate editions.
- Contributor links reference existing contributors and editions.
- Identifier rows obey single-owner rules.
- Primary covers obey single-owner primary constraints.
- `embedded_metadata_json` is valid JSON or `{}` with warning.
- Rows with missing local files are still migrated as records, but reported.
- Existing duplicate legacy rows are not deleted or modified.

## Migration Report

The backfill should produce a machine-readable and human-readable report.

Suggested fields:

- `started_at`
- `finished_at`
- `elapsed_ms`
- `dry_run`
- `legacy_rows_total`
- `rows_completed`
- `rows_skipped`
- `rows_ambiguous`
- `rows_failed`
- `books_created`
- `books_reused`
- `editions_created`
- `editions_reused`
- `files_created`
- `files_reused`
- `contributors_created`
- `contributors_reused`
- `identifiers_added`
- `covers_added`
- `missing_files`
- `invalid_metadata_json`
- `warnings`
- `errors`

Per-row details should include the legacy ID, selected normalized IDs, action,
reason, and sanitized error text.

## Dry-Run Behavior

Dry-run mode should perform every read, lookup, match, and validation step but
write nothing.

Implementation guidance:

- Use the same planner as real migration.
- Disable all INSERT/UPDATE/DELETE operations.
- Predict create/reuse decisions from current database state.
- Report would-create and would-reuse counts separately.
- Produce the same warning and error categories as real migration.
- Never write `schema_migrations`, normalized rows, or migration-map rows as
  part of dry-run.

## Rollback Strategy

The first public upgrade path should avoid destructive rollback.

Because this is an additive migration:

- `library_items` remains untouched.
- Normalized rows can be ignored by production until cutover.
- Failed backfill attempts can be rerun after deleting or correcting only
  normalized rows and migration-map rows created by the backfill.

Before production cutover, provide a documented rollback procedure:

1. Stop Librarr.
2. Back up the SQLite database.
3. Disable normalized repository wiring.
4. Restart using legacy `library_items`.
5. Preserve migration reports for diagnostics.

Do not automatically delete normalized data during rollback unless the user
explicitly requests cleanup.

## Future Public Upgrade Path

Recommended order:

1. Ship schema and documentation only.
2. Add a read-only dry-run command.
3. Add migration report generation.
4. Add backfill execution behind an explicit command or admin action.
5. Validate on copied databases.
6. Enable compatibility reads from normalized tables behind a feature flag.
7. Cut over new imports only after idempotency tests pass.
8. Keep legacy `library_items` available until at least one validated release
   after normalized reads are active.

## Remaining Unknowns

- Exact shape and provenance of all historical `metadata` JSON blobs.
- Whether any deployed databases contain non-standard legacy columns.
- Whether all `source_id` values are stable identifiers or mixed operational
  IDs.
- Whether audiobook directory rows need additional file-manifest storage before
  a perfect file-level migration.
- Whether tags and reading history should move in the same cutover or remain
  legacy-compatible for one release.
