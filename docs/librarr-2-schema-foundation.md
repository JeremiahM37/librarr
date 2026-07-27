# Librarr 2.0 Schema Foundation

This note documents the additive database foundation for the Librarr 2.0
book-centric model. It does not describe a production cutover.

## Migration Versions

The database now records ordered schema migrations in:

```sql
schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at DATETIME NOT NULL
)
```

Version `1`, `librarr_2_schema_foundation`, creates the normalized Librarr 2.0
tables and indexes.

Version `2`, `librarr_2_file_metadata_json`, adds
`files.embedded_metadata_json` so legacy `library_items.metadata` can be
preserved during a future backfill.

The existing legacy schema creation remains in place. Existing production code
continues to read from and write to `library_items`.

## Tables Introduced

- `books`: logical works, with display title, sort title, media type,
  monitoring flag, and status.
- `editions`: publication-level records owned by books.
- `contributors`: authors, narrators, illustrators, translators, editors, and
  similar people or organizations.
- `edition_contributors`: ordered contributor roles for an edition.
- `series`: logical series records.
- `book_series`: book-to-series relationships with numeric and display
  positions.
- `files`: physical managed files or directories owned by editions, including
  a JSON landing place for embedded/legacy file metadata.
- `identifiers`: scoped provider identifiers owned by either a book or an
  edition.
- `covers`: locally cacheable cover records owned by either a book or an
  edition.
- `library_item_migration_map`: future linkage from legacy `library_items` rows
  to normalized records.

## Ownership And Constraints

- Deleting a book cascades to its editions, files, identifiers, covers, and
  book-series join rows.
- Deleting an edition cascades to its files and edition-contributor rows.
- Contributors are not deleted when an edition is deleted.
- Series records are not deleted when a book relationship is removed.
- Deleting a file does not delete its edition or book.
- `files.file_path` is unique when populated.
- `files.content_hash` and `files.source_id` are indexed but not globally
  unique.
- `identifiers` require exactly one owner: either `book_id` or `edition_id`.
- `covers` require exactly one owner: either `book_id` or `edition_id`.

## Legacy Path At This Step

At the schema-foundation milestone, the following intentionally remained
unchanged:

- `library_items` is still the active production data source.
- Imports still write `library_items`.
- Duplicate detection still uses the existing path/hash logic.
- REST API responses still use current compatibility models.
- OPDS, UI, tags, stats, downloads, and delete behavior are unchanged.
- `library_item_migration_map` is created but not populated.

Later milestones added the repository switch, backfill engine, normalized read
API, feature-flagged v2 import engine, scanner, review UI, and explicit
scan-result import. Keep this document as the schema-foundation reference, not
as the latest end-to-end product status.

## Not Included In This Step

This foundation does not backfill legacy rows, introduce book matching, cut over
repositories, change API routes, update the UI, change OPDS output, or migrate
tag ownership. Those steps remain later Librarr 2.0 milestones.
