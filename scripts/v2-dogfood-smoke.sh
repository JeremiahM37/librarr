#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.v2-dogfood.yml"
DOGFOOD_DIR="$ROOT_DIR/.dogfood"
DATA_DIR="$DOGFOOD_DIR/librarr-v2-data"
LIBRARY_DIR="$DOGFOOD_DIR/librarr-v2-library"
INCOMING_DIR="$DOGFOOD_DIR/librarr-v2-incoming"
MANGA_INCOMING_DIR="$DOGFOOD_DIR/librarr-v2-manga-incoming"
COOKIE_JAR="$DOGFOOD_DIR/.cookies.txt"
BASE_URL="http://127.0.0.1:5051"
ADMIN_USER="dogfood-admin"
ADMIN_PASS="dogfood-pass123"

log() {
  printf '[v2-dogfood] %s\n' "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

cleanup_state() {
  log "stopping prior dogfood stack if present"
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR" "$LIBRARY_DIR" "$INCOMING_DIR" "$MANGA_INCOMING_DIR" "$COOKIE_JAR"
}

ensure_dirs() {
  mkdir -p \
    "$DATA_DIR" \
    "$LIBRARY_DIR/ebooks" \
    "$LIBRARY_DIR/audiobooks" \
    "$LIBRARY_DIR/manga" \
    "$INCOMING_DIR/manual-import" \
    "$MANGA_INCOMING_DIR"
}

seed_input_files() {
  cat >"$INCOMING_DIR/manual-import/Librarr Tester - The Dogfood Book.epub" <<'EOF'
This is a dogfood EPUB placeholder for Librarr v2 smoke testing.
EOF
  cat >"$INCOMING_DIR/manual-import/Librarr Tester - The Dogfood Book.mobi" <<'EOF'
This is a dogfood MOBI placeholder for Librarr v2 smoke testing with different bytes.
EOF
}

wait_for_health() {
  for _ in $(seq 1 60); do
    if curl -fsS "$BASE_URL/api/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "dogfood container did not become healthy in time" >&2
  return 1
}

json_eval() {
  python3 -c "$1"
}

register_admin() {
  log "registering first admin user"
  curl -fsS \
    -c "$COOKIE_JAR" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
    "$BASE_URL/api/register" >/dev/null
}

verify_modes() {
  log "verifying normalized repository mode and v2 import engine"
  local cfg_json
  cfg_json="$(curl -fsS -b "$COOKIE_JAR" "$BASE_URL/api/config")"
  JSON_INPUT="$cfg_json" json_eval '
import json, os, sys
data = json.loads(os.environ["JSON_INPUT"])
assert data["library_repository_mode"] == "normalized", data
assert data["import_engine"] == "v2", data
'
}

login_admin() {
  log "logging back in as admin"
  curl -fsS \
    -c "$COOKIE_JAR" \
    -b "$COOKIE_JAR" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
    "$BASE_URL/api/login" >/dev/null
}

import_two_formats() {
  log "importing EPUB and MOBI through the v2 manual import path"
  local payload
  payload='{"files":[
    {"path":"/data/incoming/manual-import/Librarr Tester - The Dogfood Book.epub","title":"The Dogfood Book","author":"Librarr Tester","media_type":"ebook","copy":true},
    {"path":"/data/incoming/manual-import/Librarr Tester - The Dogfood Book.mobi","title":"The Dogfood Book","author":"Librarr Tester","media_type":"ebook","copy":true}
  ]}'
  curl -fsS \
    -b "$COOKIE_JAR" \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$BASE_URL/api/import/files" >/dev/null
}

verify_book_and_formats() {
  log "verifying one logical book with two formats"
  local books_json book_id
  books_json="$(curl -fsS -b "$COOKIE_JAR" "$BASE_URL/api/v1/books?media_type=ebook")"
  book_id="$(JSON_INPUT="$books_json" json_eval '
import json, os
data = json.loads(os.environ["JSON_INPUT"])
items = data["items"]
assert len(items) == 1, items
item = items[0]
assert item["title"] == "The Dogfood Book", item
assert sorted(item["formats"]) == ["epub", "mobi"], item
print(item["id"])
')"
  local files_json
  files_json="$(curl -fsS -b "$COOKIE_JAR" "$BASE_URL/api/v1/books/$book_id/files")"
  JSON_INPUT="$files_json" json_eval '
import json, os
data = json.loads(os.environ["JSON_INPUT"])
items = data["items"]
assert len(items) == 2, items
assert sorted([item["format"] for item in items]) == ["epub", "mobi"], items
'
  printf '%s' "$book_id" > "$DOGFOOD_DIR/.last-book-id"
}

apply_metadata_override() {
  log "applying manual metadata override"
  local book_id
  book_id="$(cat "$DOGFOOD_DIR/.last-book-id")"
  curl -fsS \
    -b "$COOKIE_JAR" \
    -H 'Content-Type: application/json' \
    -X PATCH \
    -d '{"fields":{"publisher":"Dogfood Press"}}' \
    "$BASE_URL/api/v1/books/$book_id/metadata" >/dev/null
}

restart_and_verify_persistence() {
  log "restarting dogfood container and verifying metadata persistence"
  docker compose -f "$COMPOSE_FILE" restart librarr-v2 >/dev/null
  wait_for_health
  login_admin
  local book_id meta_json
  book_id="$(cat "$DOGFOOD_DIR/.last-book-id")"
  meta_json="$(curl -fsS -b "$COOKIE_JAR" "$BASE_URL/api/v1/books/$book_id/metadata")"
  JSON_INPUT="$meta_json" json_eval '
import json, os
data = json.loads(os.environ["JSON_INPUT"])
publisher = data["fields"]["publisher"]
assert publisher["value"] == "Dogfood Press", publisher
assert publisher["manual_override"] is True, publisher
'
}

main() {
  require_cmd docker
  require_cmd curl
  require_cmd python3

  cleanup_state
  ensure_dirs
  seed_input_files

  log "validating compose file"
  docker compose -f "$COMPOSE_FILE" config -q

  log "building dogfood image"
  docker compose -f "$COMPOSE_FILE" build

  log "starting dogfood stack"
  docker compose -f "$COMPOSE_FILE" up -d

  log "waiting for health endpoint"
  wait_for_health

  register_admin
  verify_modes
  import_two_formats
  verify_book_and_formats
  apply_metadata_override
  restart_and_verify_persistence

  log "smoke test passed"
  log "UI: $BASE_URL"
}

main "$@"
