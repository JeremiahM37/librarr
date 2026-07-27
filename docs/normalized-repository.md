# NormalizedRepository

`internal/library.NormalizedRepository` is the future Librarr 2.0 storage
engine for the normalized book-centric schema.

It is implemented and selectable at startup through repository mode. Legacy mode
remains the default rollback-safe path, while normalized mode is used for
Librarr 2.0 dogfooding and validated deployments.

## Responsibilities

The repository implements the library domain repository interfaces for:

- books
- editions
- files
- contributors and edition contributor roles
- series and book-series positions
- identifiers
- covers
- transaction boundaries

Callers should depend on domain methods such as `FindBook`, `AttachFile`,
`ListBookEditions`, or `PrimaryCover` instead of knowing SQL table names or
join details.

## Table Ownership

`NormalizedRepository` owns access to these normalized tables:

- `books`
- `editions`
- `contributors`
- `edition_contributors`
- `series`
- `book_series`
- `files`
- `identifiers`
- `covers`

It does not read or write `library_items`, and it does not populate
`library_item_migration_map`.

## Transaction Model

The repository implements `TransactionManager` with `WithinTransaction`.

Repository methods automatically use the transaction stored on the context
when called inside `WithinTransaction`; otherwise they execute directly against
the configured `*sql.DB`.

This lets future import and backfill code coordinate multi-table writes without
making every caller pass SQL transaction objects around.

## Expected Invariants

- A book is the logical entity.
- An edition belongs to exactly one book.
- A file belongs to exactly one edition.
- A book can have multiple editions.
- An edition can have multiple physical files and formats.
- Contributors are merge-safe by case-insensitive name/sort-name lookup.
- Contributor roles live on `edition_contributors`.
- Series membership lives on `book_series`.
- Identifiers are unique per owner, provider, and identifier value.
- File paths are unique when populated.
- Primary covers are unique per book or edition.

## Relationship Handling

Book reads hydrate direct book-level contributors, series, identifiers, and
covers where practical without requiring API callers to perform their own SQL
joins.

File reads join through `editions` to expose the owning `BookID`. This keeps
future delivery features, OPDS downloads, Kindle export, and send-to-device
flows able to reason about both the physical file and its logical book.

Series and contributor APIs use upserts or merge-safe lookups where the schema
supports it, while preserving conservative identity rules.

## Metadata Storage

The normalized schema includes `files.embedded_metadata_json` so the legacy
`library_items.metadata` JSON blob can be preserved during backfill. This is a
file-level landing place because each legacy row currently represents one
physical file.

Provider metadata and user override metadata still need explicit
provenance-aware storage before the `MetadataRepository` methods are used in
production.

## Repository Factory

`NewRepository(NormalizedRepositoryMode, *sql.DB)` returns a
`NormalizedRepository`.

`NewRepository(LegacyRepositoryMode, *sql.DB)` returns a
`LegacyLibraryRepository` backed by a small SQL legacy store. Startup wires
`LibraryService` to the selected repository implementation through
`LIBRARR_LIBRARY_REPOSITORY_MODE`.

## Future Migration Strategy

1. Keep legacy mode available for rollback.
2. Backfill normalized rows with conservative matching.
3. Validate counts and relationship integrity against legacy data.
4. Select `LIBRARR_LIBRARY_REPOSITORY_MODE=normalized` after validation.
5. Use `LIBRARR_IMPORT_ENGINE=v2` for normalized import dogfooding.
6. Make normalized mode the default only after repeated real-world validation.
