# Changelog

This changelog tracks notable Librarr changes as the project moves toward a
public Librarr 2.0 release. Librarr 2.0 is not yet stable.

## Unreleased — Librarr 2.0 development

### Added

- Normalized book-centric schema and repository selection.
- LibraryService domain boundary.
- Backfill and validation framework for migration from legacy `library_items`.
- Import planner, executor, and feature-flagged import engine.
- First-run administrator setup and empty-library onboarding.
- Book-first Home dashboard with recently added shelf, attention summary,
  activity counts, library totals, and role-aware quick actions.
- Library scanner with scan jobs, progress, review results, duplicate detection,
  manual review, and explicit import actions.
- Metadata editor for scan-review candidates and imported library books.
- Review-first Book Details metadata tools for extracting EPUB metadata from
  managed files, matching Open Library candidates, applying selected fields,
  storing identifiers/provenance, and attaching managed covers without
  modifying ebook files.
- Normalized Library cards group multiple formats under one logical book.
- Admin book details actions for removing catalog records or deleting managed
  files with explicit confirmation.
- Embedded EPUB cover extraction with local cover caching.
- Wanted Books with canonical book metadata, monitored Prowlarr searches, search
  history, stored release inspection, Discover-origin release seeding, manual
  selected-release qBittorrent handoff, and library reconciliation.
- Expanded local user management.
- Rich staged diagnostics for Prowlarr and qBittorrent.
- Remote qBittorrent torrent upload and remote-to-local path mapping fixes.
- OPDS 1.2 catalog with Basic Auth, covers, search, and file downloads.
- Repository security policy, issue templates, pull request template, and
  CODEOWNERS.
- Visible build identity in Settings/About and `/api/health`.

### Changed

- Module path moved to `github.com/jamie75/librarr`.
- Product positioning now presents Librarr 2.0 as an independently maintained
  continuation with its own roadmap while preserving original attribution.
- Docker release documentation now reserves `latest` for stable releases.
- Completed-download imports no longer insert library records when file
  organization is enabled but organization fails.
- Normalized format chips are deduplicated and sorted deliberately.
- Book Details now includes an explicit admin-only duplicate merge repair for
  historical logical-book splits.
- Settings now includes an admin-only nested ebook path repair with dry-run
  preview, collision/missing/unsafe statuses, safe moves, catalog path updates,
  and empty-directory cleanup.
- Wanted rows created from release results now preserve raw release titles as
  origin context instead of storing them as canonical book titles.
- Wanted rows that match imported normalized Library books are marked imported
  and move to Completed instead of remaining active.
- Downloads has a hidden details route for Home attention cards while primary
  navigation remains focused on Home, Library, Discover, Wanted, and Settings.
- Normalized startup now disables the legacy audiobook folder scanner so
  populated mounts do not create `library_items` rows before explicit scan and
  import.
- Audiobook path fallback parsing now treats the parent folder as author and
  the filename stem as title for single-file audiobook layouts.

### Known limitations

- Librarr 2.0 is not yet declared stable.
- External metadata provider lookup and provider-backed refresh are not yet
  implemented.
- Historical duplicate logical book rows and nested ebook paths are not repaired
  automatically; administrators must run the explicit repair tools.
- MOBI, AZW3, and PDF cover extraction remain incomplete.
- Reading progress sync, annotations, highlights, and bookmarks are not yet
  implemented.
- Media Assistant integration is planned but not implemented.
- A full import-repair workflow for resolving failed organization attempts is
  still planned.
- User account editor UX still has rough edges.
- Automatic Wanted grabbing, quality profiles, and durable torrent/import
  identity linkage are still planned.
