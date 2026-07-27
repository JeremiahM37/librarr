# Source Registry

Librarr uses a runtime source registry to keep search-source endpoint
definitions outside the binary. The registry contains endpoint and mirror data
for built-in search drivers such as Anna's Archive, LibGen mirrors,
AudioBookBay, Gutenberg, Open Library, Librivox, Standard Ebooks, MangaDex,
Nyaa, The Pirate Bay, Z-Library defaults, and web-novel sites.

It is a runtime dependency for search behavior, not a build-time dependency.
The application still starts if the registry is unavailable, but searches that
depend on registry-provided endpoints may return no results until a registry or
cache is available.

## Load order

At startup Librarr resolves the registry in this order:

1. `LIBRARR_SOURCES_PATH` — local JSON file, highest priority.
2. `LIBRARR_SOURCES_URL` — user-configured HTTP(S) JSON URL.
3. built-in default URL.
4. on-disk cache from the last successful HTTP fetch.

Successful HTTP loads are cached next to the SQLite database as
`sources-cache.json`. This allows later restarts to keep working when the
network or remote registry is unavailable.

## Current default

The current built-in default remains:

```text
https://raw.githubusercontent.com/JeremiahM37/librarr-sources/main/sources.json
```

This is an explicit temporary operational dependency on the upstream companion
source registry. It remains in place because no Jamie-owned replacement registry
has been confirmed in this repository. Do not replace it with a nonexistent URL.

Audit notes from the public GitHub repository:

- repository: `JeremiahM37/librarr-sources`
- visibility: public
- fork status: not a fork
- default branch: `main`
- contents: source endpoint and mirror definitions in `sources.json`
- license reported by GitHub and the repository `LICENSE`: MIT
- last observed push: 2026-05-13
- repository size: small JSON/documentation repository

The repository README describes an embedded-default fallback, but this Librarr
codebase does not currently embed a complete registry. Its actual fallback after
remote failures is the on-disk cache, then an empty registry.

Recommended follow-up before fork detachment:

1. Confirm the license and maintenance plan for the source registry.
2. Create or confirm a Jamie-owned `librarr-sources` repository.
3. Mirror the current registry and validate the live canary test.
4. Update the built-in default URL only after the replacement is reachable.

## Resilience and limits

- HTTP requests use an explicit timeout.
- Responses are capped at 1 MiB.
- Non-2xx responses, malformed JSON, and oversized responses are rejected.
- Cache writes are best-effort and do not block startup.
- If every source fails, Librarr returns an empty registry and logs an
  actionable error rather than aborting startup.

## Security note

`LIBRARR_SOURCES_URL` is administrator-controlled configuration. Treat it as a
trusted deployment setting. If it ever becomes user-editable from the web UI, it
must go through the stricter outbound URL validation path.
