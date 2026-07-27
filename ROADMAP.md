# Librarr Roadmap

## Purpose

This roadmap turns the Librarr vision and architecture into staged, testable milestones.

The order matters. Librarr should establish a durable book-centric foundation before expanding into broad automation, delivery, and Media Assistant integration.

The roadmap is intentionally practical. Each milestone should leave the application usable, deployable, and testable.

## Current Baseline

The current codebase already provides a useful foundation, including:

- Docker deployment
- SQLite storage
- web UI and authentication
- Prowlarr/indexer integration
- torrent download workflows
- qBittorrent and Transmission support
- completed-download monitoring
- file organization
- EPUB embedded metadata extraction
- MOBI filename fallback handling
- content-hash duplicate prevention
- OPDS support
- REST endpoints
- first-run setup and empty-library onboarding
- recursive library scanner
- scan review with filters, search, duplicate details, and manual review
- explicit import from scan review results
- metadata editor with live import-location preview
- staged connection diagnostics for Prowlarr and qBittorrent

The immediate architectural limitation is that `library_items` represents both a logical book and a physical file.

## Milestone 2.0 — Book-Centric Foundation

### Objective

Replace the file-centric library core with a normalized domain model while preserving current import and deployment stability.

### 2.0.1 Architecture and migration fixtures

- [x] Define product vision.
- [x] Define target architecture.
- [x] Define staged roadmap.
- [x] Document the current `library_items` schema and every read/write path.
- [ ] Add fixtures for:
  - same book in EPUB and MOBI
  - same title by different authors
  - EPUB and PDF editions
  - missing author
  - manga CBZ and CBR
  - duplicate content under different paths
  - conflicting embedded metadata
- [x] Add migration test harness using copied/test databases.
- [x] Define rollback and validation procedure.

### 2.0.2 Normalized schema

- [x] Add domain tables additively:
  - `books`
  - `editions`
  - `contributors`
  - `edition_contributors`
  - `series`
  - `files`
  - `identifiers`
  - `covers`
- [x] Add indexes for normalized names, foreign keys, hashes, formats, and source IDs.
- [x] Preserve `library_items` during the transition.
- [x] Add explicit schema versioning and migration status logging.

### 2.0.3 Conservative matching and planning

- [x] Introduce planner/resolver components for books, editions, contributors, duplicates, and files.
- [x] Match trusted identifiers before text heuristics where available.
- [x] Use normalized title, primary author, media type, and file evidence as fallback evidence.
- [x] Return match confidence and evidence.
- [x] Keep ambiguous candidates separate through `manual_review`.
- [x] Resolve manual-review candidates with an inline metadata editor.
- [ ] Add user-confirmed merge and split design.

### 2.0.4 Backfill existing library

- [x] Backfill normalized records from `library_items`.
- [x] Preserve legacy rows and migration linkage.
- [x] Produce validation totals for books, editions, and files.
- [x] Report ambiguous records instead of silently merging them.
- [x] Verify that every old managed path maps to exactly one new file row.

### 2.0.5 Repository and import cutover

- [x] Add repositories for books, editions, contributors, series, and files.
- [x] Build the planner/executor import path behind a feature flag.
- [x] Update completed torrent imports, direct downloads, and manual import to support the normalized engine.
- [x] Preserve file-level path/hash duplicate prevention.
- [x] Make repeated completed-download events idempotent.
- [x] Coordinate filesystem and database failure recovery.
- [x] Add rollback-by-configuration through `LIBRARR_IMPORT_ENGINE=legacy`.
- [x] Add explicit scan-review import using the configured import engine.
- [ ] Make normalized import the default after more dogfooding.

### 2.0.6 Compatibility API

- [x] Add `/api/v1/books` and explicit file resources.
- [x] Add `/api/v1/library/summary` and local cover delivery for normalized reads.
- [ ] Keep existing library endpoints available during transition.
- [ ] Return one logical book with a `formats` list and `files` collection.
- [ ] Preserve a preferred/default file in compatibility responses.
- [ ] Add `total_books`, `total_files`, and format counts to stats.
- [ ] Document transitional fields.

### 2.0.7 Library UI and onboarding

- [x] Add a distinct Librarr 2.0 shell and visual direction.
- [x] Simplify navigation to Home, Library, Discover, and Settings.
- [x] Hide unfinished Devices, Activity, and send-to-device UI.
- [x] Add first-run administrator setup.
- [x] Add empty-library onboarding.
- [x] Add Library & Import folder configuration.
- [x] Add clear scan, review, duplicate, manual-review, and import-error states.
- [ ] Continue refining logical book cards as normalized data becomes the default.
- [ ] Defer complex editing until the normalized model is stable.

### 2.0.8 Connection diagnostics

- [x] Replace simple Prowlarr and qBittorrent connection tests with step-by-step diagnostics.
- [x] Report DNS, TCP, TLS/HTTPS, authentication, API version, and latency.
- [x] Return actionable next steps for timeouts, 401/403 errors, bad URLs, and unreachable hosts.
- [x] Keep secrets, headers, cookies, and sensitive URLs out of logs and responses.
- [x] Present diagnostics in Settings with clear success/failure rows.
- [ ] Extend diagnostics to Audiobookshelf, Kavita, Komga, SABnzbd, and Transmission.

### 2.0 exit criteria

- One logical book can own multiple physical formats.
- Existing library data migrates without lost files.
- Current qBittorrent import behavior remains functional.
- Repeated imports do not create duplicate files or books.
- Compatibility APIs remain usable.
- The Docker image builds and deploys on amd64 and arm64.
- Migration and repository tests pass in CI.

## Milestone 2.1 — Download Client Framework

### Objective

Provide *arr-style configurable download clients through a common interface.

### Core framework

- [ ] Define internal download client capabilities.
- [ ] Store multiple configured clients.
- [ ] Support enable/disable, priority, categories, and optional save paths.
- [ ] Add connection testing and health reporting.
- [ ] Mask credentials in API responses and logs.
- [ ] Separate torrent and Usenet capabilities where behavior differs.

### Client adapters

- [ ] Treat qBittorrent as the reference torrent implementation.
- [ ] Bring Transmission into the same adapter interface.
- [ ] Add rTorrent support.
- [ ] Add Deluge support.
- [ ] Validate completed-download handling against each client.
- [ ] Add client-specific diagnostics without leaking implementation details into the import pipeline.

### Settings UI

- [ ] Add a familiar **Settings → Download Clients** page.
- [ ] Support add, edit, delete, enable, disable, test, and priority.
- [ ] Display implementation-specific fields only when relevant.
- [ ] Add a completed-download handling section.

### 2.1 exit criteria

- qBittorrent, Transmission, rTorrent, and Deluge use one stable application-facing contract.
- The user can configure and test clients from the UI.
- Import code does not switch on client type outside the adapter layer.

## Milestone 2.2 — Metadata and Cover System

### Objective

Make metadata dependable, explainable, editable, and reusable across file formats.

### Metadata providers

- [ ] Define metadata provider interface.
- [ ] Evaluate initial providers such as Open Library and Google Books.
- [ ] Store provider identifiers and provenance.
- [ ] Score candidate matches.
- [ ] Avoid destructive refresh of user overrides.
- [ ] Add rate limiting, caching, and provider health reporting.

### Embedded metadata

- [ ] Preserve current EPUB extraction behavior.
- [ ] Preserve MOBI filename fallback as a last-resort source.
- [ ] Store embedded metadata at the file level.
- [ ] Resolve display metadata through explicit precedence rules.
- [ ] Add format-specific extractor tests.

### Covers

- [x] Extract embedded EPUB covers from local files.
- [ ] Download covers from selected metadata providers.
- [x] Cache extracted covers locally.
- [x] Store cover provenance and dimensions.
- [x] Serve local covers from Librarr-owned API URLs.
- [x] Provide placeholders when no cover is available.
- [ ] Add MOBI/AZW embedded cover extraction where practical.
- [ ] Add PDF first-page thumbnails when a safe renderer is available.
- [ ] Store cover checksums.

### Editing and refresh

- [ ] Add book/edition metadata edit APIs.
- [ ] Add refresh preview before applying provider changes.
- [ ] Add field-level user overrides.
- [ ] Record metadata history for troubleshooting.

### 2.2 exit criteria

- Metadata is stored once per logical edition, not duplicated per format.
- Users can understand where key fields came from.
- Covers persist locally and survive provider outages.

## Milestone 2.3 — *arr Media Management Experience

### Objective

Make Librarr operationally familiar to Sonarr and Radarr users.

### Media Management

- [ ] Root folder management.
- [ ] Naming templates with previews.
- [ ] Rename existing files safely.
- [ ] Copy, move, and hardlink policies.
- [ ] Rescan and missing-file detection.
- [ ] Manual import workflow.
- [x] Scan-review metadata editor for title, author, edition fields, identifiers, and preview.

### Profiles

- [ ] Quality profiles for ebook, audiobook, manga, and comic formats.
- [ ] Preferred and cutoff format behavior.
- [ ] Release profiles with preferred and excluded terms.
- [ ] Upgrade policy that does not unnecessarily remove useful formats.

### Operations

- [ ] Activity queue.
- [ ] History.
- [ ] Blocklist.
- [ ] Health checks.
- [ ] System status.
- [ ] Backup and restore validation.
- [ ] Log level controls and support bundle.

### 2.3 exit criteria

- A user familiar with another *arr can configure core workflows without learning unique terminology.
- File organization, upgrades, and failures are visible and recoverable.

## Milestone 2.4 — Monitoring and Automated Acquisition

### Objective

Move from one-off search/download behavior to reliable library automation.

- [ ] Monitor individual books.
- [ ] Monitor editions.
- [ ] Monitor contributors/authors.
- [ ] Monitor series and identify gaps.
- [ ] Schedule RSS or periodic indexer checks.
- [ ] Apply quality and release profiles automatically.
- [ ] Record grab decisions and rejection reasons.
- [ ] Prevent repeated grabs of blocked or failed releases.
- [ ] Add manual and automatic search parity.

### 2.4 exit criteria

- Librarr can automatically acquire a monitored title according to configured rules.
- Every automatic decision is visible in history and explainable.

## Milestone 2.5 — OPDS and Delivery

### Objective

Make acquired books easy to consume.

### Downloads and OPDS

- [x] Download a specific file by file ID.
- [ ] Download a book using a requested or preferred format.
- [x] Expose one OPDS entry per logical book.
- [x] Include multiple acquisition links for available formats.
- [x] Preserve OPDS 1.2 compatibility where needed.
- [ ] Add OPDS 2.0 support.

### Device destinations

- [ ] Define device/destination model.
- [ ] Add preferred formats and size limits.
- [ ] Add direct download destination.
- [ ] Add watched-folder delivery.
- [ ] Add Kindle email delivery.
- [ ] Record delivery history and failures.
- [ ] Evaluate Kobo and Apple Books workflows.

### Optional conversion integration

- [ ] Evaluate Calibre integration for explicit conversion requests.
- [ ] Keep conversion optional and outside the core import requirement.
- [ ] Never perform silent lossy conversion.

### 2.5 exit criteria

- A user can choose a book and acquire or send an appropriate available format.
- Clients do not need to understand Librarr's internal storage paths.

## Milestone 2.6 — Media Assistant Integration

### Objective

Expose Librarr as the authoritative book service for Media Assistant.

### API capabilities

- [ ] Search books, contributors, and series.
- [ ] Determine ownership and available formats.
- [ ] Add a wanted or monitored title.
- [ ] Trigger search or acquisition.
- [ ] Retrieve recent additions and activity.
- [ ] Trigger device delivery.
- [ ] Expose stable event/webhook payloads.

### Integration principles

- [ ] Media Assistant uses the public API only.
- [ ] Librarr remains fully usable without Media Assistant.
- [ ] Cross-media recommendations and household presentation remain Media Assistant responsibilities.
- [ ] Book metadata, files, acquisition, OPDS, and delivery remain Librarr responsibilities.

### 2.6 exit criteria

- Media Assistant can discover, request, inspect, and deliver books through documented Librarr APIs.
- No direct database coupling exists between the projects.

## Milestone 3.0 — Mature Personal Library Platform

Potential scope after the 2.x foundation is proven:

- multiple users and per-user reading state
- reading progress integrations
- richer audiobook chapter and track modeling
- edition comparison and manual merge/split UI
- advanced duplicate review
- recommendation signals
- event subscriptions
- mobile-friendly or native clients
- broader provider ecosystem
- localization and accessibility improvements

These items should not delay the book-centric foundation.

## Engineering Workstreams

The following apply across all milestones.

### Testing

- migration tests
- repository tests
- import idempotency tests
- client adapter contract tests
- API compatibility tests
- filesystem failure tests
- end-to-end Docker smoke tests

### Documentation

- architecture decision records for major choices
- OpenAPI documentation
- migration and rollback instructions
- Docker deployment examples
- supported-client capability matrix
- troubleshooting guides

### CI/CD

- Go formatting and tests
- static analysis
- amd64 and arm64 image builds
- migration smoke tests
- release notes and schema version reporting

### Security

- secret masking
- path traversal protection
- authentication and API key review
- dependency updates
- parser hardening for untrusted files
- backup/restore validation

## Near-Term Recommended Task Order

The next implementation sequence should be:

1. Extend rich diagnostics to Audiobookshelf, Kavita, Komga, SABnzbd, and Transmission.
2. Continue dogfooding the scanner/review/import path with real libraries.
3. Add focused fixes for manual-review edge cases found during dogfooding.
4. Refine logical book cards and details using the normalized read API.
5. Begin metadata provider and cover improvements.
6. Begin the download-client adapter framework after diagnostics are reliable.

## Import Repair Follow-up

Librarr now avoids inserting library records when file organization is enabled
but organization fails. A future import-repair workflow should add:

- explicit repair state for failed organization/import attempts
- retry after permissions or folder mappings are fixed
- UI filters for repair-needed items
- clear activity/history entries for failed and retried imports
- duplicate protection across repair retries

## Definition of Done for Major Changes

A major change is not complete until:

- code is formatted
- tests pass
- migrations have upgrade and rollback guidance
- Docker builds succeed
- no secrets are exposed in logs or APIs
- user-visible behavior is documented
- the change is deployed to a test instance
- representative real-world imports are verified
