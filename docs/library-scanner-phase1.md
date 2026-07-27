# Library Scanner, Review, and Explicit Import

This document describes the current Librarr 2.0 existing-library workflow.

The scanner is deliberately read-only. It discovers and classifies files, then
builds a review payload. Import happens only when the user explicitly chooses
`Import Selected` or `Import All Ready`.

## Architecture

The scan API is intentionally thin:

```text
API
  -> scanner.Manager
  -> filesystem walk of configured library roots
  -> ImportPlanner metadata and duplicate decisions
  -> in-memory review payload
  -> explicit import job
  -> ImportEngine
  -> LibraryService
  -> selected repository
```

The scanner reads the current Library & Import folder configuration when a scan
starts. Saved `settings.json` values override startup config defaults so a user
can save folders and scan without restarting the container.

## Endpoints

`POST /api/v1/library/scan`

Starts a scan job and returns `202 Accepted` with a job id. Only one scan can run
at a time; a second start request receives `409 Conflict` with the active job id.

`GET /api/v1/library/scan/{job_id}`

Returns job status and progress.

`GET /api/v1/library/scan/{job_id}/results`

Returns the completed review payload. If the job is not complete, the endpoint
returns `409 Conflict`.

`POST /api/v1/library/scan/{job_id}/resolve`

Resolves a review candidate in the completed scan payload. Supported actions
are `use_suggested` and `edit_metadata`. `use_suggested` applies only to
manual-review candidates; `edit_metadata` may update manual-review or ready
candidates before import. This updates the in-memory review result so the
candidate can be imported; it does not write library data.

`POST /api/v1/library/import`

Starts an explicit import job from scan results. Only `new` / ready candidates
are eligible. Duplicates, already-imported files, unsupported files, unreadable
files, and unresolved manual-review items are never imported.

`GET /api/v1/library/import/{job_id}`

Returns import job status and progress.

`GET /api/v1/library/import/{job_id}/results`

Returns the completion summary and per-candidate results.

The endpoints require an authenticated administrator session or API key because
they expose configured application file paths.

## Supported Formats

Ebook root:

- `.epub`
- `.pdf`
- `.mobi`
- `.azw3`

Audiobook root:

- `.m4b`
- `.mp3`

Manga root:

- `.cbz`
- `.cbr`
- `.pdf`

Extension matching is case-insensitive. PDF is classified by the configured root:
ebook PDFs under the ebook root, manga PDFs under the manga root.

## Candidate Schema

Each result candidate includes:

- stable candidate id for the scan result
- media type
- format
- absolute application/container path
- relative path within the configured root
- filename
- file size
- modified time
- title and author
- series and volume placeholders
- metadata source and confidence
- classification and reason
- destination preview path when the planner can determine one
- duplicate details for duplicate candidates
- manual-review details for ambiguous candidates
- error details for unreadable files

Metadata uses existing embedded extraction where available and falls back to the
existing filename parser when embedded metadata is missing or unsupported.

## Classification Rules

`new`

The planner found no existing file conflict. The file is ready to import.

`manual_review`

The planner found ambiguity or a conflict that needs a user decision. The review
payload includes a human-readable reason, metadata source, confidence, and
suggested destination when available.

`already_imported`

The exact file path already exists in the selected library repository. This means
the scanner found the same application path that Librarr already tracks.

`duplicate`

The file appears to duplicate an existing library file without being the exact
same tracked path. Duplicate reasons include identical content hash, same
destination, and same format already planned or imported.

`unsupported`

The file extension is not supported for the configured media root.

`unreadable`

The scanner could not stat or read the file. The scan continues.

## Review UI

The review screen supports:

- progress display
- summary cards
- client-side filters
- search by title, author, or filename
- collapsible result sections
- destination preview
- metadata source and confidence display
- duplicate details
- manual-review controls

Current sections:

- Ready to Import
- Manual Review
- Duplicates
- Already Imported
- Unsupported
- Unreadable

Representative development results were roughly 29 files discovered, 14
imported, 13 duplicates, and 2 manual-review items. These numbers are examples
only.

## Explicit Import

Import is always user-triggered.

Supported review actions:

- Select All
- Import Selected
- Import All Ready
- Skip Selected
- Clear Selection
- Retry Failed

Import jobs provide progress, current title, elapsed time, completion summary,
partial-failure reporting, duplicate exclusion, and concurrent-import
protection. When an import finishes, Librarr refreshes the review payload and
library summary so imported rows move into Already Imported without another
scan.

Skipping is session-only and does not persist to the database.

## Manual Review

Manual-review items currently support:

- Use Suggested
- Edit Metadata
- Skip
- destination preview
- metadata source and confidence

The metadata editor is the primary manual-resolution interface for scan review.
It edits only the in-memory scan candidate and supports title, subtitle, author,
series, series number, publisher, publication year, ISBN, language, description,
tags, library selection, destination folder preview, and filename preview.

Saving an edit marks the candidate ready to import and sends those values as
explicit manual metadata overrides when the import job starts. Save & Import
resolves the candidate first, then imports that one candidate through the
existing import engine. The editor does not modify source files, download cover
art, or call internet metadata providers.

## Job Lifecycle

Jobs are in memory only. Completed jobs/results are retained using a small
bounded retention policy. This is enough for the Step 2 review flow without
adding Redis or another external dependency.

Statuses:

- `pending`
- `scanning`
- `processing_metadata`
- `classifying`
- `completed`
- `failed`
- `cancelled`

Progress includes the current media type, current path/phase, directories
scanned, files discovered, files processed, candidates ready, warnings, and
timestamps.

## Missing Folders

A missing or inaccessible configured folder becomes a folder-level warning. The
scanner continues with the remaining configured roots.

## Current Limitations

- No file organization or moves.
- No external metadata providers.
- No internet metadata lookup.
- No cover downloading. Embedded EPUB artwork may be extracted from local
  user-owned files and cached for review/import display.
- No full metadata editor.
- Duplicate detection intentionally reuses the current planner/service path; a
  later performance pass can add repository-level batch lookups if large real
  libraries need more optimization.
