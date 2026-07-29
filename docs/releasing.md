# Releasing Librarr

This document defines the expected release process for independently maintained
Librarr builds. It is preparation documentation only; it does not declare
Librarr 2.0 stable.

## Release channels

- `latest` — current production image from `main`.
- `v2` — stable Librarr 2 line.
- `main` — latest image built from the `main` branch.
- `v2-dogfood` — mutable feature-branch dogfood and household validation builds.
- `2.0.0`, `2.0`, ... — immutable/minor version tags created from
  stable release tags such as `v2.0.0`.

The GitHub release workflow publishes container images under:

```text
ghcr.io/jamie75/librarr:<tag>
```

## Container tag matrix

| Source ref | Published tags |
|---|---|
| `main` branch | `latest`, `v2`, `main`, `sha-<short-sha>` |
| `feature/**` branches | `v2-dogfood`, `dogfood-<short-sha>` |
| Pull requests | Build/test only; no GHCR publish |
| Stable tag `v2.0.0` on `main` | `2.0.0`, `2.0`, `v2`, `latest` |
| Stable tag `v2.0.0` not on `main` | `2.0.0`, `2.0`, `v2`; no `latest` |

The workflow strips the leading `v` from stable semantic version image tags, so
Git tag `v2.0.0` publishes container tag `2.0.0`. `v2-dogfood` is intentionally
mutable and may change between feature builds. Use `latest` for normal
production installs, `v2` to pin the stable Librarr 2 line, and `v2-dogfood`
only for active validation.

All image pushes use `GITHUB_TOKEN` with package-write permission and publish
only to `ghcr.io/jamie75/librarr`. Published images are built for
`linux/amd64` and `linux/arm64`.

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

Images:

- `ghcr.io/jamie75/librarr:<version>`
- `ghcr.io/jamie75/librarr:v2`
- `ghcr.io/jamie75/librarr:latest`

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
