# Wanted Books

Wanted Books is Librarr's local monitored-title workflow. It is designed to
feel familiar to *arr users while remaining book-centric and conservative.

Current scope:

- add/remove Wanted books
- monitor/unmonitor Wanted books
- search one Wanted book or all monitored Wanted books
- store search history and latest matched releases
- inspect stored releases before action
- manually hand one selected torrent/magnet release to the configured torrent client
- reconcile Wanted rows against the normalized Library after import

Out of scope for the current milestone:

- automatic grabbing
- quality profiles
- release profiles
- RSS monitoring
- blocklist/history automation
- automatic upgrades
- device delivery

## Data model

Wanted records store canonical book intent, not raw release titles.

Important fields include:

- title
- author
- ISBN / ASIN
- language
- media type
- preferred format
- monitored
- status
- origin release context when the row came from Discover

When a user adds a Prowlarr result from Discover, Librarr preserves raw release
context separately:

- raw release title
- indexer
- source id / guid
- preferred format
- protocol and download metadata

This prevents torrent names such as `Title by Author [ENG EPUB]` from becoming
the canonical book title.

## Statuses

Current statuses:

- `wanted`
- `searching`
- `found`
- `missing`
- `downloading`
- `downloaded`
- `imported`
- `ignored`

The UI groups them as:

- Active: `wanted`, `searching`, `found`, `missing`, `downloading`
- Ignored: `ignored`
- Completed: `downloaded`, `imported`

No status should make a Wanted card disappear. Search history renders below the
persistent Wanted list.

## Search behavior

Wanted monitor search is intentionally stricter than Discover search.

Wanted search should prefer:

1. exact ISBN/ASIN match when available
2. normalized title plus normalized author
3. high-confidence title-only fallback only when safe

Discover search remains free-form. Author searches such as `Tom Bower` should
return relevant releases without forcing every result to match one canonical
Wanted title.

## Release storage and inspection

Search results are stored so users can reopen **View Releases** without running
another search. Stored releases retain enough metadata for inspection:

- title
- guid/source id
- indexer
- protocol
- publish date / age
- size
- seeders/leechers/grabs when available
- language
- format
- categories
- score
- search timestamp

Download URLs are treated as internal server-side data and are not exposed to
the browser unless a future API explicitly requires it.

## Manual handoff

Manual Wanted downloads use the existing torrent-client handoff path. The
browser submits only:

- wanted id
- release id

Librarr retrieves the stored release server-side, then uses the same Prowlarr
and qBittorrent safety behavior used by Discover downloads:

- HTTP/HTTPS Prowlarr torrent URLs are fetched by Librarr first.
- Prowlarr API authentication stays server-side.
- `.torrent` bytes are uploaded to qBittorrent using multipart file upload.
- Magnet links use qBittorrent URL submission.
- qBittorrent save path and category come from Librarr settings.

After qBittorrent accepts the handoff, the Wanted row moves to `downloading` and
records the selected release and download reference when available.

## Library reconciliation

Wanted rows are reconciled with the normalized Library when the Wanted list is
loaded. If a non-final Wanted row now matches an imported library book by:

- normalized title
- normalized author
- media type

then Librarr marks it `imported` and moves it to Completed.

This is intentionally conservative. Same-author different-title rows are not
marked imported.

Future work should replace or augment this with durable torrent/import identity
linkage so completion can be traced directly from selected release to imported
file.

## Dogfood checklist

1. Add a Prowlarr result from Discover to Wanted.
2. Verify the Wanted row title and author are clean canonical metadata.
3. Open **View Releases** and confirm the origin release is available without a
   new search.
4. Manually hand off a release to qBittorrent.
5. Confirm the card moves to `downloading` and records the selected release.
6. Let the torrent import through the normal completed-download pipeline.
7. Reload Wanted and verify the matching row moves to `imported` / Completed.
8. Verify unrelated books by the same author remain active.
