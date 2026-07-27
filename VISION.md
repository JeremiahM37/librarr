# Librarr Vision

## Mission

Librarr is an open-source, self-hosted personal library server for books, audiobooks, comics, and manga.

Its mission is to provide a modern, reliable replacement for Readarr that fits naturally into the familiar *arr ecosystem while improving the areas where book management differs from movie and television management.

Librarr manages books. Files are representations of books.

That principle guides the database model, API, user interface, import pipeline, OPDS catalog, download handling, metadata management, and device delivery workflows.

## Product Direction

Librarr began as a fork of the original Librarr project. Librarr 2.0 is
independently maintained, with its own architecture, product goals, release
process, and roadmap while preserving original licensing and attribution.

The project should feel familiar to Sonarr, Radarr, and other *arr users:

- Indexers are configured through Prowlarr, Torznab, or Newznab.
- Download clients are configured, tested, and monitored through a common interface.
- Completed Download Handling imports and organizes acquired files.
- Root folders, naming, quality profiles, monitoring, history, activity, and system status use familiar terminology.
- Docker is the primary deployment method.

At the same time, Librarr should be book-centric rather than forcing books into a file-centric or movie-centric model.

## The Core Domain

A logical book can have multiple physical files:

```text
Book
  ├── EPUB
  ├── MOBI
  ├── AZW3
  ├── PDF
  └── other supported formats
```

The book owns descriptive metadata such as:

- title and subtitle
- contributors
- description
- series and sequence
- identifiers
- publication information
- cover art
- tags and monitoring state

Each file owns technical information such as:

- path
- format
- size
- hash
- embedded metadata
- import source
- download-client identifiers

This separation allows Librarr to answer both:

- "Do I have this book?"
- "Which formats do I have for it?"

## Primary Goals

### 1. Be a real Readarr replacement

Librarr should provide the essential workflows expected from a mature *arr application:

- search and interactive search
- monitoring and automatic acquisition
- Prowlarr, Torznab, and Newznab integration
- quality and release profiles
- multiple torrent and Usenet download clients
- completed-download handling
- root folders and naming rules
- activity, history, blocklist, logs, health, backup, and restore
- modern authentication and API access

### 2. Model books correctly

The data model must distinguish logical books from physical files and support:

- multiple formats per book
- multiple contributors and contributor roles
- editions and identifiers
- series and sequence information
- conservative matching that avoids incorrect merges
- technical metadata retained at the file level

### 3. Make metadata dependable

Metadata should combine local embedded metadata with configurable external providers.

Librarr should:

- preserve provenance
- score and reconcile provider results
- permit user corrections
- avoid silently replacing trusted local edits
- support refresh without duplicating work across formats
- cache cover art locally

### 4. Be API-first

Every important capability should be available through a documented API before or alongside the web UI.

This supports:

- Media Assistant
- future mobile applications
- scripts and automations
- OPDS clients
- third-party integrations

The API should expose logical books and their available files rather than requiring clients to reconstruct relationships from file rows.

### 5. Support delivery, not only acquisition

A useful library server should help the user consume the result.

Librarr should support:

- direct download by format
- OPDS acquisition links
- send-to-device workflows
- Kindle delivery
- configurable preferred formats per destination
- optional future conversion integrations

### 6. Integrate cleanly with Media Assistant

Media Assistant is the broader user-facing discovery and orchestration layer. Librarr is the authoritative service for the book domain.

Media Assistant should be able to ask Librarr questions such as:

- Is this book already owned?
- Which formats are available?
- Is it monitored or wanted?
- What is new in the library?
- Can this title be acquired?
- Can this book be sent to a configured device?

Librarr remains independently useful and must not require Media Assistant to operate.

## Experience Principles

### Familiar to *arr users

Settings and workflows should use recognizable categories:

- Media Management
- Profiles
- Indexers
- Download Clients
- Importing
- Metadata
- Connect
- General
- UI
- Status

The project should borrow successful interaction patterns without copying another application's implementation or visual design pixel-for-pixel.

### Conservative with user data

Librarr must prefer a false negative over an incorrect merge.

Ambiguous books, editions, contributors, or files should remain separate until better metadata or user confirmation is available.

### Practical and self-hosted

The project should remain approachable for a homelab operator:

- simple Docker deployment
- SQLite as the default database
- low idle resource use
- clear logs and health checks
- migrations that preserve user data
- no required hosted service
- sensible defaults
- polished onboarding
- reviewable incremental improvements

Librarr should be treated like a hobby self-hosted application in the spirit of
Jellyfin, Audiobookshelf, Kavita, and Calibre-Web. Prefer working software,
clear workflows, and responsive UI over enterprise-style abstraction.

### Incremental and testable

Large architectural changes should be delivered in staged migrations with compatibility layers and focused tests. Stable import and download behavior must not be casually broken while new models are introduced.

## Scope

### In scope

- ebooks
- audiobooks
- comics and manga
- metadata and cover management
- acquisition and completed-download handling
- library organization
- monitoring and automation
- OPDS
- device delivery
- REST API
- Media Assistant integration

### Initial non-goals

- DRM removal
- ebook editing
- replacing a full ebook editor
- mandatory cloud accounts
- social-network features
- AI-generated metadata replacing authoritative sources
- built-in format conversion in the first major redesign

Conversion may later be supported through an explicit integration such as Calibre, but the core service should not depend on it.

## Success Criteria

Librarr succeeds when a user can:

1. Add or monitor a book, author, or series.
2. Search configured indexers through familiar *arr workflows.
3. Send the selected release to a supported download client.
4. Reliably import the completed download.
5. See one logical book with all available formats.
6. Correct or refresh metadata without editing every file separately.
7. Browse and acquire available formats through the UI, API, or OPDS.
8. Send the best available format to a configured device.
9. Integrate the library with Media Assistant without exposing internal file-centric implementation details.

## Guiding Statement

> Librarr manages books. Files are just one representation of a book.
