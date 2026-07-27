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
- `GET /api/v1/books/{id}/provenance`

`PATCH` accepts a partial `fields` object and only writes changed fields.

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

This milestone does not yet introduce:

- external provider adapters
- contributor edit overrides
- identifier edit overrides
- background refresh scheduling
- cover-provider integration

Those remain follow-on metadata-engine milestones.
