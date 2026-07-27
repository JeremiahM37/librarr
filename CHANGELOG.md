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
- Library scanner with scan jobs, progress, review results, duplicate detection,
  manual review, and explicit import actions.
- Metadata editor for scan-review candidates and imported library books.
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

### Known limitations

- Librarr 2.0 is not yet declared stable.
- Some Library metadata editing paths still need polish.
- MOBI, AZW3, and PDF cover extraction remain incomplete.
- Reading progress sync, annotations, highlights, and bookmarks are not yet
  implemented.
- Media Assistant integration is planned but not implemented.
- A full import-repair workflow for resolving failed organization attempts is
  still planned.
- User account editor UX still has rough edges.
