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
- Normalized Library cards group multiple formats under one logical book.
- Admin book details actions for removing catalog records or deleting managed
  files with explicit confirmation.
- Embedded EPUB cover extraction with local cover caching.
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

### Known limitations

- Librarr 2.0 is not yet declared stable.
- Some Library metadata editing paths still need polish.
- Historical duplicate logical book rows and nested ebook paths are not repaired
  automatically; administrators must run the explicit repair tools.
- MOBI, AZW3, and PDF cover extraction remain incomplete.
- Reading progress sync, annotations, highlights, and bookmarks are not yet
  implemented.
- Media Assistant integration is planned but not implemented.
- A full import-repair workflow for resolving failed organization attempts is
  still planned.
- User account editor UX still has rough edges.
