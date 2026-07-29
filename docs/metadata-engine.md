# Librarr 2.0 Metadata Engine

Librarr 2.0 stores descriptive metadata at the logical book layer, with
edition-specific fields tracked separately when they belong to a specific
publication.

## Current flow

```text
API
  ↓
LibraryService
  ↓
MetadataEngine
  ↓
MetadataRepository
  ↓
books / editions / metadata_values / metadata_evidence
```

## Effective metadata

The metadata engine currently exposes one effective metadata document per book:

- book-scoped fields
  - `title`
  - `description`
  - `genres`
  - `language`
- edition-scoped fields for the book's effective edition
  - `edition_title`
  - `subtitle`
  - `publisher`
  - `publication_date`

Contributors and identifiers are also returned with source and confidence
annotations based on the normalized records already attached to the book.

## Provenance model

Two additive tables track metadata state:

- `metadata_values`
  - one current selected value per `(scope_type, scope_id, field)`
- `metadata_evidence`
  - all known competing values for that field
  - `selected=1` marks the currently effective evidence

Each value tracks:

- source
- confidence
- updated_at
- manual_override

## Override behavior

Manual edits use:

- `source = manual`
- `confidence = exact`
- `manual_override = true`

Automatic/provider refreshes:

- always record new evidence
- update the effective value only when the current field is not manually
  overridden
- preserve manual overrides without discarding competing provider evidence

## API

Normalized mode adds:

- `GET /api/v1/books/{id}/metadata`
- `PATCH /api/v1/books/{id}/metadata`
- `POST /api/v1/books/{id}/metadata/extract`
- `POST /api/v1/books/{id}/metadata/matches`
- `POST /api/v1/books/{id}/metadata/apply`
- `GET /api/v1/books/{id}/provenance`

`PATCH` accepts a partial `fields` object and only writes changed fields.

## Review-first enrichment tools

Book Details includes two metadata tools:

- **Extract from File** reads the backend-managed file already attached to the
  book. EPUB is currently supported. Librarr reads the OPF package metadata,
  Dublin Core fields, common Calibre series metadata, identifiers, subjects,
  and EPUB 2/3 cover declarations. MOBI, AZW3, and PDF extraction remain
  unsupported unless existing lightweight metadata is already available.
- **Match Online** searches Open Library. ISBN matches rank highest, followed
  by exact title/author matches and conservative fuzzy title/author matches.

Both endpoints return proposals only. Applying a proposal requires
`POST /api/v1/books/{id}/metadata/apply` with a server-issued proposal token and
an explicit list of selected fields. This prevents the browser from selecting
arbitrary local file paths or server-side cover URLs.

Applied proposals can update:

- book metadata fields: title, description, language, genres
- edition metadata fields: edition title, subtitle, publisher, publication date
- primary author contributor
- series link and position
- ISBN/Open Library identifiers
- primary managed cover, when no valid local cover already exists

Ebook files are never modified. Cover images are copied into Librarr's managed
cover cache before the catalog points at them.

## Effective-field synchronization

When the selected metadata value changes, Librarr also updates the existing
display columns used by current read paths:

- `books.title`
- `books.sort_title`
- `books.description`
- `books.language`
- `editions.title`
- `editions.subtitle`
- `editions.publisher`
- `editions.publication_date`

This keeps the current normalized list/detail APIs consistent while provenance
remains additive.

## Current limitations

- Google Books or other provider adapters
- internet metadata lookup outside Book Details review flow
- background refresh scheduling
- MOBI/AZW/PDF embedded cover extraction

Those remain follow-on metadata-engine milestones.
