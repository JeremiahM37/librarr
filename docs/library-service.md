# LibraryService

`internal/library.LibraryService` is the application-facing boundary for
library operations during the Librarr 2.0 transition.

The service exists so HTTP handlers, OPDS, background work, and API
resources can depend on book/file domain operations instead of reaching
directly into the legacy `internal/db` package. It is now the production seam
for repository selection.

## Responsibilities

`LibraryService` owns the repository and service dependencies needed for
library operations:

- books
- files
- metadata
- series
- contributors
- identifiers
- covers
- matching
- managed file operations

The service delegates to the selected repository and preserves compatibility
behavior where needed. Normalized mode and the v2 import engine use this service
boundary for book, edition, contributor, metadata, and file writes.

## Current Dependency Flow

```text
HTTP / scanner / import engine / OPDS / background callers
  ↓
LibraryService
  ↓
selected repository
  ├─ LegacyLibraryRepository → library_items
  └─ NormalizedRepository    → books / editions / files / contributors / ...
```

Legacy mode remains the default rollback-safe source. Normalized mode is
selected explicitly with `LIBRARR_LIBRARY_REPOSITORY_MODE=normalized`.
Compatibility endpoints may still serialize legacy-shaped DTOs, but production
library APIs should obtain data through `LibraryService` or repository
interfaces.

## Repository Interaction

The service is constructed with explicit dependencies. It does not use globals.
Callers can inject test doubles, the legacy repository, or the normalized
repository.

The legacy repository implements the domain repository interfaces by mapping
each legacy row to one logical book and one file. Domain write operations that
would imply normalized storage remain read-only or unsupported unless they map
to an existing legacy compatibility operation.

## Error Boundary

`LibraryService` translates repository errors into domain errors such as:

- `ErrBookNotFound`
- `ErrInvalidIdentifier`
- `ErrRepositoryReadOnly`
- `ErrUnsupportedOperation`

This prevents future handlers from depending on SQL-specific error text.
Legacy compatibility routes still preserve their existing response shapes.

## Future NormalizedRepository Swap

Repository Switch has been implemented. Remaining work is operational cutover
and cleanup:

1. Keep legacy mode available for rollback.
2. Continue dogfooding normalized mode.
3. Keep imports behind `LIBRARR_IMPORT_ENGINE`.
4. Convert remaining compatibility-only paths in focused milestones.
5. Make normalized mode the default only after validation.

Because callers depend on `LibraryService`, the repository swap should be a
wiring change plus compatibility validation rather than a broad endpoint
rewrite.
