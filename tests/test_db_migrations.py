import sqlite3


def test_apply_migrations_creates_expected_tables(tmp_path):
    from db_migrations import apply_migrations, get_migration_status

    db = tmp_path / "fresh.db"
    conn = sqlite3.connect(str(db))
    try:
        applied = apply_migrations(conn)
        assert applied >= 1
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "download_jobs" in tables
        assert "library_items" in tables
        assert "activity_log" in tables
        assert "schema_migrations" in tables
        status = get_migration_status(conn)
        assert len(status) >= 3
    finally:
        conn.close()


def test_apply_migrations_upgrades_legacy_download_jobs_table(tmp_path):
    from db_migrations import apply_migrations

    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("CREATE TABLE download_jobs (job_id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        conn.commit()
        apply_migrations(conn)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(download_jobs)").fetchall()]
        assert "created_at" in cols
        assert "updated_at" in cols
    finally:
        conn.close()
