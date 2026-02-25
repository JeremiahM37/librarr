"""SQLite schema migrations for Librarr.

Lightweight internal migration registry so future schema changes are applied
deterministically without requiring Alembic.
"""
from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger("librarr")


MIGRATIONS = [
    ("0001_download_jobs_table", "Create persistent download job table", "download_jobs_table"),
    ("0002_library_tables", "Create library items/activity tables + indexes", "library_tables"),
    ("0003_download_jobs_timestamps", "Ensure download_jobs timestamps exist", "download_jobs_timestamps"),
]


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply any pending migrations to the provided SQLite connection."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at REAL DEFAULT (strftime('%s','now'))
        )
        """
    )
    applied = 0
    for name, description, handler in MIGRATIONS:
        exists = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name = ?",
            (name,),
        ).fetchone()
        if exists:
            continue
        _HANDLERS[handler](conn)
        conn.execute(
            "INSERT INTO schema_migrations (name, description) VALUES (?, ?)",
            (name, description),
        )
        applied += 1
        logger.info("Applied DB migration %s", name)
    return applied


def get_migration_status(conn: sqlite3.Connection):
    """Return applied migration names and counts for diagnostics/tests."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at REAL DEFAULT (strftime('%s','now'))
        )
        """
    )
    rows = conn.execute(
        "SELECT name, description, applied_at FROM schema_migrations ORDER BY applied_at, name"
    ).fetchall()
    return [{"name": r[0], "description": r[1], "applied_at": r[2]} for r in rows]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c[1] == column for c in cols)


def _ensure_index(conn: sqlite3.Connection, ddl: str):
    conn.execute(ddl)


def _migrate_download_jobs_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS download_jobs (
            job_id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            created_at REAL DEFAULT (strftime('%s', 'now')),
            updated_at REAL DEFAULT (strftime('%s', 'now'))
        )
        """
    )


def _migrate_library_tables(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS library_items (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT NOT NULL,
            author      TEXT DEFAULT '',
            file_path   TEXT DEFAULT '',
            original_path TEXT DEFAULT '',
            file_size   INTEGER DEFAULT 0,
            file_format TEXT DEFAULT '',
            media_type  TEXT DEFAULT 'ebook',
            source      TEXT DEFAULT '',
            source_id   TEXT DEFAULT '',
            added_at    REAL DEFAULT (strftime('%s','now')),
            metadata    TEXT DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS activity_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   REAL DEFAULT (strftime('%s','now')),
            event_type  TEXT NOT NULL,
            title       TEXT DEFAULT '',
            detail      TEXT DEFAULT '',
            library_item_id INTEGER DEFAULT NULL,
            job_id      TEXT DEFAULT ''
        );
        """
    )
    _ensure_index(conn, "CREATE INDEX IF NOT EXISTS idx_library_source_id ON library_items(source_id)")
    _ensure_index(conn, "CREATE INDEX IF NOT EXISTS idx_library_title ON library_items(title)")
    _ensure_index(conn, "CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON activity_log(timestamp)")


def _migrate_download_jobs_timestamps(conn: sqlite3.Connection):
    if not _table_exists(conn, "download_jobs"):
        _migrate_download_jobs_table(conn)
        return
    if not _column_exists(conn, "download_jobs", "created_at"):
        conn.execute("ALTER TABLE download_jobs ADD COLUMN created_at REAL DEFAULT (strftime('%s','now'))")
    if not _column_exists(conn, "download_jobs", "updated_at"):
        conn.execute("ALTER TABLE download_jobs ADD COLUMN updated_at REAL DEFAULT (strftime('%s','now'))")


_HANDLERS = {
    "download_jobs_table": _migrate_download_jobs_table,
    "library_tables": _migrate_library_tables,
    "download_jobs_timestamps": _migrate_download_jobs_timestamps,
}
