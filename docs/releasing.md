# Releasing Librarr

This document defines the expected release process for independently maintained
Librarr builds. It is preparation documentation only; it does not declare
Librarr 2.0 stable.

## Release channels

- `v2-dogfood` — current development and household validation builds.
- `v2.0.0-beta.1`, `v2.0.0-beta.2`, ... — public beta builds.
- `v2.0.0` — first stable Librarr 2.0 release after release criteria are met.
- `latest` — stable releases only.

The GitHub release workflow publishes container images under:

```text
ghcr.io/jamie75/librarr:<tag>
```

The workflow only publishes `ghcr.io/jamie75/librarr:latest` for stable semver
tags matching `vMAJOR.MINOR.PATCH`.

## Pre-release checklist

Before tagging a beta or stable release:

1. Merge the release candidate branch into `main`.
2. Verify the Go module path and import paths are final.
3. Verify Docker image names point to `ghcr.io/jamie75/librarr`.
4. Verify Settings/About and `/api/health` show the expected version, channel,
   commit, and build time.
5. Run the full validation suite:

   ```bash
   go test ./...
   go test -race ./...
   go vet ./...
   node --check web/static/js/app.js
   node --test web/static/js/app.test.js
   python3 -m json.tool internal/api/openapi.json >/dev/null
   git diff --check
   ```

6. Build and run the Docker image locally.
7. Validate first-run setup on a fresh database.
8. Validate upgrade behavior on an existing legacy database.
9. Validate scanner, review, import, metadata editing, covers, qBittorrent, and
   OPDS.
10. Update [CHANGELOG.md](../CHANGELOG.md).
11. Prepare GitHub release notes.
12. Confirm attribution files are still present.

## GHCR visibility

The repository workflow can publish package tags, but GHCR package visibility
and anonymous pull access are GitHub package settings. Verify them manually in
GitHub before telling users they can pull images without authentication.

## Release notes template

```markdown
## Librarr <version>

### Highlights

- ...

### Upgrade notes

- ...

### Docker

Image:

`ghcr.io/jamie75/librarr:<version>`

### Known limitations

- ...

### Attribution

Librarr began as a fork of the original Librarr project. Librarr 2.0 is
independently maintained. Original licensing and attribution are preserved.
```

## Librarr 1.x migration notes

Existing `library_items` data remains preserved during the Librarr 2.0
transition. The normalized schema, backfill engine, and repository switch are
designed to make migration deterministic and repeatable.

Do not remove or manually edit legacy rows as part of a release unless a
specific migration and rollback plan exists.
