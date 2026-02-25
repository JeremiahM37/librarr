"""Library tracking database — records downloaded books and activity log."""
import json
import os
import sqlite3
import threading
import logging

from db_migrations import apply_migrations

logger = logging.getLogger("librarr")


class LibraryDB:
    """SQLite-backed library tracking with activity log.

    Uses the same DB file as DownloadStore (separate tables).
    Thread-safe via locking.
    """

    def __init__(self, db_path):
        self._db_path = db_path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            apply_migrations(conn)

    # --- Library Items ---

    def add_item(self, title, author="", file_path="", original_path="",
                 file_size=0, file_format="", media_type="ebook",
                 source="", source_id="", metadata=None):
        """Record a successfully processed book. Returns the new item ID."""
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """INSERT INTO library_items
                       (title, author, file_path, original_path, file_size,
                        file_format, media_type, source, source_id, metadata)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (title, author, file_path, original_path, file_size,
                     file_format, media_type, source, source_id,
                     json.dumps(metadata or {})),
                )
                return cur.lastrowid

    def has_source_id(self, source_id):
        """Check if we've already downloaded something with this source_id."""
        if not source_id:
            return False
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM library_items WHERE source_id = ?", (source_id,)
            ).fetchone()
            return row is not None

    def find_by_title(self, title):
        """Case-insensitive title lookup for duplicate detection."""
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM library_items WHERE title = ? COLLATE NOCASE",
                (title,),
            ).fetchall()

    def get_items(self, media_type=None, limit=50, offset=0):
        """Paginated list of library items, newest first."""
        query = "SELECT * FROM library_items"
        params = []
        if media_type:
            query += " WHERE media_type = ?"
            params.append(media_type)
        query += " ORDER BY added_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(query, params).fetchall()]

    def count_items(self, media_type=None):
        """Count library items, optionally filtered by media type."""
        query = "SELECT COUNT(*) FROM library_items"
        params = []
        if media_type:
            query += " WHERE media_type = ?"
            params.append(media_type)
        with self._connect() as conn:
            return conn.execute(query, params).fetchone()[0]

    # --- Activity Log ---

    def log_event(self, event_type, title="", detail="",
                  library_item_id=None, job_id=""):
        """Append an event to the activity log."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """INSERT INTO activity_log
                       (event_type, title, detail, library_item_id, job_id)
                       VALUES (?, ?, ?, ?, ?)""",
                    (event_type, title, detail, library_item_id, job_id),
                )

    def get_activity(self, limit=50, offset=0):
        """Recent activity, newest first."""
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM activity_log ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()]

    def count_activity(self):
        """Total number of activity events."""
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM activity_log").fetchone()[0]

    def cleanup_activity(self, days: int = 90) -> int:
        """Delete activity log entries older than `days` days."""
        import time
        cutoff = time.time() - days * 86400
        with self._connect() as conn:
            deleted = conn.execute(
                "DELETE FROM activity_log WHERE timestamp < ?",
                (cutoff,)
            ).rowcount
        if deleted:
            logger.info(f"Pruned {deleted} old activity log entries (>{days}d)")
        return deleted
