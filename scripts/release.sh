#!/usr/bin/env bash
# Cut a release: bump internal/version/VERSION, commit, tag, and push.
# Pushing the tag triggers .github/workflows/release.yml.
#
# Usage: scripts/release.sh 1.6.0
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

version="${1:-}"
version="${version#v}"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "usage: $0 X.Y.Z"
tag="v$version"

cd "$(git rev-parse --show-toplevel)"
version_file=internal/version/VERSION

[[ "$(git branch --show-current)" == main ]] || die "not on main"
[[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"

git fetch --quiet --tags origin main
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] \
  || die "main is not in sync with origin/main"
git rev-parse -q --verify "refs/tags/$tag" >/dev/null && die "tag $tag already exists"

current="$(tr -d '[:space:]' < "$version_file")"
[[ "$current" != "$version" ]] || die "VERSION is already $version"

echo "Releasing $tag (current: $current)"
read -r -p "Continue? [y/N] " reply
[[ "$reply" =~ ^[Yy]$ ]] || die "aborted"

echo "$version" > "$version_file"
git commit --quiet -m "Bump version to $version" -- "$version_file"
git tag -a "$tag" -m "$tag"
git push --atomic origin main "$tag"

echo "Pushed $tag; the Release workflow will take it from here."
