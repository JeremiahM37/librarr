# Librarr Backfill Engine

The Librarr 2.0 backfill engine migrates legacy `library_items` rows into the
normalized book-centric schema.

It is implemented as reusable library code, not as a CLI command or API
handler. The backfill engine itself does not switch service, repository, API,
UI, OPDS, or import behavior; Repository Switch and Import Engine selection are
separate startup/runtime decisions.

## Architecture

```text
future CLI / UI wizard / upgrade task
  ↓
LibraryMigrationEngine
  ↓
LegacyStore + StateStore
  ↓
Normalized repository interfaces
  ↓
books / editions / contributors / files / identifiers / covers
```

The engine uses:

- `LegacyStore` to read legacy rows deterministically.
- `StateStore` to record runs, row state, and migration-map checkpoints.
- `Repository` to write normalized domain records.

The engine does not write normalized tables with direct SQL. Normalized writes
go through repository methods. SQL is isolated in the store implementation for
legacy reads, run tracking, migration-map updates, and validation snapshots.

## Execution Flow

Rows are processed by ascending `library_items.id`.

For each row:

1. Check `library_item_migration_map`.
2. Reuse a completed mapping when present.
3. Find or create a book by title/media type.
4. Find or create an edition under that book.
5. Attach the author as an edition contributor when available.
6. Reuse an existing file by path, source ID, or content hash.
7. Attach a new file when no match exists.
8. Preserve legacy metadata in `files.embedded_metadata_json`.
9. Preserve `source` and `source_id` on the file.
10. Add a book identifier for stable non-empty source/source ID pairs.
11. Save the migration map and backfill state.

Each real row migration runs through the normalized repository transaction
boundary. If the process stops before a mapping is recorded, rerunning the
engine reuses normalized records by path/source/hash and then records the map.

## State Tracking

The additive migration `librarr_2_backfill_state` creates:

- `backfill_runs`
- `backfill_state`

`backfill_runs` records:

- version
- start/completion timestamps
- status
- dry-run flag
- processed/migrated/skipped/error counts
- resume checkpoint
- structured report JSON

`backfill_state` records per-legacy-row progress:

- legacy item ID
- run ID
- status
- normalized book/edition/file IDs
- sanitized error text

`library_item_migration_map` remains the durable mapping used for idempotency
and future compatibility validation.

## Dry Run

`DryRun()` performs legacy reads, normalized lookups, planning, and validation
without writing:

- no normalized records
- no migration-map rows
- no backfill-run rows
- no backfill-state rows

Dry-run reports predicted creates/reuses and warnings such as invalid metadata
JSON.

## Validation

`Validate()` uses a database snapshot to check:

- every legacy item has a completed mapping
- mapped books exist
- mapped editions exist
- mapped files exist
- files are attached to editions
- duplicate normalized file paths are absent
- duplicate normalized identifiers are absent

Validation produces a structured result with errors, warnings, and snapshot
counts. Dry-run can report warnings without requiring completed mappings.

## Resume Behavior

`Resume(runID)` reruns the same migration path. Existing completed
`library_item_migration_map` rows are reused. Rows without completed mappings
are processed again and are expected to reuse any normalized records already
created before interruption.

This makes interruption safe:

- completed rows are skipped/reused
- partially written normalized rows are reused by lookup
- missing map rows are recreated
- failed rows can be retried

## Reporting

The engine returns a structured `Report` and a human-readable `Summary()`.

The report includes:

- legacy rows total
- rows processed/completed/skipped/failed
- books created/reused
- editions created/reused
- files created/reused
- contributors created/reused
- identifiers created
- covers created
- duplicates merged
- warnings
- errors
- elapsed time
- per-row results
- validation result

## Current Limits

This first working engine intentionally stays conservative.

- It does not parse free-form author strings into multiple contributors.
- It does not create series unless future structured metadata support is added.
- It does not create covers because legacy rows do not have dedicated cover
  fields.
- It treats unknown source/source ID pairs as file provenance and only adds a
  simple book identifier for non-empty pairs.
- It does not switch production reads.

## Future Upgrade Path

Recommended order:

1. Run dry-run on copied databases.
2. Review reports for missing files, invalid metadata, and ambiguous matches.
3. Run the real engine behind a CLI/admin action.
4. Validate normalized totals and relationships.
5. Select normalized repository mode only after validation passes.
6. Use the v2 import engine only after normalized write idempotency has live
   validation.
