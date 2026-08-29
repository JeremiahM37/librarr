#!/bin/sh
# Starts as root so it can fix up ownership on the mounted directories, then
# drops to PUID:PGID before ever executing the librarr binary. This is what
# actually fixes the "permission denied" crash loop for every deployment
# target (a fresh Docker named volume, a bare host directory on a real Linux
# NAS, a bind mount), not just the Docker-Desktop-on-Windows case a
# workaround in docker-compose.yml alone would cover.
#
# Non-recursive on purpose: write permission on a directory is a property of
# the directory itself, not of everything already inside it, and a
# recursive chown over a large existing library would be needlessly slow on
# every container start. New files the app creates are owned by PUID:PGID
# automatically once the process itself runs as that user.
set -e

PUID=${PUID:-1000}
PGID=${PGID:-1000}

for dir in /data /books/ebooks /books/audiobooks /books/manga; do
  if [ -d "$dir" ]; then
    chown "$PUID:$PGID" "$dir" 2>/dev/null || true
  fi
done

exec su-exec "$PUID:$PGID" /usr/local/bin/librarr "$@"
