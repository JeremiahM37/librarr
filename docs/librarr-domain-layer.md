# Librarr Domain Layer

`internal/library` is the Librarr 2.0 domain boundary. It defines how the rest
of the application should think about books, editions, contributors, series,
files, identifiers, covers, matching, import stages, and delivery without
requiring callers to know how those concepts are stored.

This package is now the service boundary for Repository Switch and the
feature-flagged import engine. Some compatibility paths still use
`library_items`, but new Librarr 2.0 work should flow through
`LibraryService`, repositories, and import planner/executor contracts rather
than directly through legacy storage.

## Responsibilities

The package owns domain terminology and contracts:

- `Book`, `Edition`, `Contributor`, `Series`, `BookFile`, `Identifier`, and
  `Cover`
- repository interfaces for logical operations
- matching contracts and evidence types
- file service contracts for managed file operations and downloads
- import pipeline stage contracts
- compatibility translation between domain objects and legacy library items

The package should not expose SQL joins, table names, route names, or browser
response details.

## Repository Pattern

Repository interfaces describe application intentions rather than CRUD tables.
For example, future code should ask for `FindBookByIdentifier`,
`GetBookFiles`, or `AttachFile`, not for a specific SQL query against `books`
or `files`.

The normalized repository now implements these contracts and can be selected at
startup. Legacy repository adapters remain for compatibility and rollback.

## Legacy Compatibility

`LegacyLibraryRepository` adapts the current `library_items` storage path into
the new interfaces. It represents each legacy row as one logical book with one
file, matching today's production behavior.

The adapter preserves legacy behavior where an operation maps cleanly to
`library_items`. Domain writes that imply normalized-only behavior should return
clear unsupported-operation errors instead of silently dropping data.

Compatibility translators convert:

```text
legacy LibraryItem -> Book + BookFile
Book + BookFile    -> legacy LibraryItem DTO
```

This gives future REST handlers a bridge while preserving current response
compatibility.

## Current Migration Path

The intended sequence is:

1. Keep legacy mode available for rollback.
2. Route production library APIs through `LibraryService`.
3. Use normalized repositories behind `LIBRARR_LIBRARY_REPOSITORY_MODE`.
4. Backfill normalized tables with conservative matching.
5. Use the v2 import engine behind `LIBRARR_IMPORT_ENGINE=v2`.
6. Retire legacy storage only after production validation.

## API Cutover

API handlers should depend on `internal/library` interfaces rather than
`internal/db` legacy methods. Compatibility DTOs allow existing endpoints to
keep their response shape while the backing repository changes from
`library_items` to normalized `books`, `editions`, and `files`.

New `/api/v1` endpoints expose logical books, explicit file resources,
metadata, scanner jobs, review results, and explicit import jobs directly.
