"""
Tests for Librarr Flask application.
Runs without any external services (qBittorrent, Prowlarr, etc.).
"""
import io
import hashlib
import json
import os
import sys
import tempfile

import pytest

# ── Environment setup (must happen before app import) ─────────────────────────
_tmp = tempfile.mkdtemp()
os.environ.setdefault("LIBRARR_DB_PATH", os.path.join(_tmp, "test.db"))
os.environ.setdefault("AI_MONITOR_ENABLED", "false")
os.environ.setdefault("SECRET_KEY", "test-secret-key")
os.environ.setdefault("AUTH_USERNAME", "")
os.environ.setdefault("AUTH_PASSWORD", "")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def client():
    import app as librarr_app
    import opds
    import sources

    librarr_app.app.config["TESTING"] = True
    sources.load_sources()
    opds.init_app(librarr_app.app, librarr_app.library)
    with librarr_app.app.test_client() as c:
        yield c


# ── Core health & config ───────────────────────────────────────────────────────

def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.get_json()
    assert data["status"] == "ok"


def test_metrics_endpoint(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    body = r.data.decode()
    assert "librarr_jobs_by_status" in body
    assert "librarr_library_items_total" in body


def test_schema_endpoint(client):
    r = client.get("/api/schema")
    assert r.status_code == 200
    data = r.get_json()
    assert "migrations" in data
    assert data["count"] >= 1


def test_config(client):
    r = client.get("/api/config")
    assert r.status_code == 200
    data = r.get_json()
    assert isinstance(data, dict)


def test_validate_config_endpoint(client):
    r = client.get("/api/validate/config")
    assert r.status_code == 200
    data = r.get_json()
    assert "paths" in data
    assert "services" in data
    assert "success" in data


def test_sources_endpoint(client):
    r = client.get("/api/sources")
    assert r.status_code == 200
    data = r.get_json()
    assert isinstance(data, (list, dict))
    if isinstance(data, dict) and data:
        first = next(iter(data.values()))
        assert "health" in first


# ── Downloads & library ────────────────────────────────────────────────────────

def test_downloads_list(client):
    r = client.get("/api/downloads")
    assert r.status_code == 200
    data = r.get_json()
    assert "torrents" in data or "downloads" in data or isinstance(data, (list, dict))


def test_library_empty(client):
    r = client.get("/api/library")
    assert r.status_code == 200


def test_activity_empty(client):
    r = client.get("/api/activity")
    assert r.status_code == 200


# ── AI Monitor ────────────────────────────────────────────────────────────────

def test_monitor_status(client):
    r = client.get("/api/monitor/status")
    assert r.status_code == 200
    data = r.get_json()
    assert "enabled" in data
    assert data["enabled"] is False  # AI_MONITOR_ENABLED=false


def test_monitor_dismiss_nonexistent(client):
    r = client.post("/api/monitor/actions/nonexistent/dismiss")
    assert r.status_code == 200


# ── OPDS catalog ──────────────────────────────────────────────────────────────

def test_opds_root(client):
    r = client.get("/opds/")
    assert r.status_code == 200
    assert b"<feed" in r.data
    assert b"Librarr" in r.data
    assert b"opds-catalog" in r.data


def test_opds_root_no_slash(client):
    r = client.get("/opds")
    assert r.status_code in (200, 301, 308)


def test_opds_library(client):
    r = client.get("/opds/library")
    assert r.status_code == 200
    assert b"<feed" in r.data


def test_opds_library_ebook_filter(client):
    r = client.get("/opds/library?type=ebook")
    assert r.status_code == 200
    assert b"<feed" in r.data


def test_opds_search_no_query(client):
    r = client.get("/opds/search")
    assert r.status_code == 200
    assert b"<feed" in r.data


def test_opds_search_with_query(client):
    r = client.get("/opds/search?q=dune")
    assert r.status_code == 200
    assert b"<feed" in r.data


def test_opds_opensearch(client):
    r = client.get("/opds/opensearch.xml")
    assert r.status_code == 200
    assert b"OpenSearchDescription" in r.data
    assert b"Librarr" in r.data


def test_opds_download_nonexistent(client):
    r = client.get("/opds/download/nonexistent-id")
    assert r.status_code in (404, 503)


# ── Goodreads / StoryGraph CSV import ─────────────────────────────────────────

GOODREADS_CSV = (
    "Book Id,Title,Author,Author l-f,Additional Authors,ISBN,ISBN13,"
    "My Rating,Average Rating,Publisher,Binding,Number of Pages,Year Published,"
    "Original Publication Year,Date Read,Date Added,Bookshelves,Bookshelves with positions,"
    "Exclusive Shelf,My Review,Spoiler,Private Notes,Read Count,Owned Copies\n"
    '1,"Dune","Frank Herbert","Herbert, Frank","",="0441013597",="9780441013593",'
    '0,4.26,"Ace","Mass Market Paperback",604,1990,1965,,2023/01/01,,'
    '"to-read","to-read (#1)","to-read","","","",0,0\n'
)

STORYGRAPH_CSV = (
    "Title,Authors,ISBN/UID,Format,Read Status,Star Rating,Review,Tags,Last Date Read,Dates Read\n"
    '"Dune","Frank Herbert",,"Paperback","to-read","","","",,""\n'
)


def test_csv_import_goodreads(client):
    data = {
        "csv_file": (io.BytesIO(GOODREADS_CSV.encode()), "goodreads.csv"),
        "shelf": "to-read",
        "media_type": "ebook",
    }
    r = client.post(
        "/api/import/csv",
        data=data,
        content_type="multipart/form-data",
    )
    assert r.status_code == 200
    d = r.get_json()
    assert "queued" in d
    assert d.get("format") == "goodreads"


def test_csv_import_storygraph(client):
    data = {
        "csv_file": (io.BytesIO(STORYGRAPH_CSV.encode()), "storygraph.csv"),
        "shelf": "to-read",
        "media_type": "ebook",
    }
    r = client.post(
        "/api/import/csv",
        data=data,
        content_type="multipart/form-data",
    )
    assert r.status_code == 200
    d = r.get_json()
    assert "queued" in d
    assert d.get("format") == "storygraph"


def test_csv_import_no_file(client):
    r = client.post("/api/import/csv")
    assert r.status_code == 400


# ── Settings ──────────────────────────────────────────────────────────────────

def test_settings_get(client):
    r = client.get("/api/settings")
    assert r.status_code == 200
    assert isinstance(r.get_json(), dict)


def test_settings_masks_secrets_and_preserves_masked_placeholders(client):
    import config

    r = client.post("/api/settings", json={
        "prowlarr_api_key": "prow-secret",
        "qb_pass": "qb-secret",
        "abs_token": "abs-secret",
        "kavita_api_key": "kav-secret",
        "api_key": "api-secret",
        "qb_user": "admin",
    })
    assert r.status_code == 200

    r = client.get("/api/settings")
    data = r.get_json()
    for key in ("prowlarr_api_key", "qb_pass", "abs_token", "kavita_api_key", "api_key"):
        assert data[key] == config.MASKED_SECRET

    # Sending masked placeholders back should not overwrite stored values.
    r = client.post("/api/settings", json={
        "prowlarr_api_key": config.MASKED_SECRET,
        "qb_pass": config.MASKED_SECRET,
        "abs_token": config.MASKED_SECRET,
        "kavita_api_key": config.MASKED_SECRET,
        "api_key": config.MASKED_SECRET,
        "qb_user": "admin2",
    })
    assert r.status_code == 200
    assert config.PROWLARR_API_KEY == "prow-secret"
    assert config.QB_PASS == "qb-secret"
    assert config.ABS_TOKEN == "abs-secret"
    assert config.KAVITA_API_KEY == "kav-secret"
    assert config.API_KEY == "api-secret"
    assert config.QB_USER == "admin2"


def test_password_hash_verify_supports_modern_and_legacy():
    import config

    hashed = config.hash_password("swordfish")
    assert hashed != "swordfish"
    assert config.verify_password("swordfish", hashed) is True
    assert config.verify_password("wrong", hashed) is False

    legacy = "sha256:" + hashlib.sha256(b"swordfish").hexdigest()
    assert config.verify_password("swordfish", legacy) is True
    assert config.verify_password("wrong", legacy) is False


def test_downloadstore_marks_searching_as_interrupted_on_restart(tmp_path):
    import app as librarr_app

    db_path = tmp_path / "restart-state.db"
    store1 = librarr_app.DownloadStore(str(db_path))
    store1["job-searching"] = {
        "title": "Dune",
        "status": "searching",
        "error": None,
        "detail": "Searching sources...",
    }

    store2 = librarr_app.DownloadStore(str(db_path))
    restored = store2["job-searching"]
    assert restored["status"] == "error"
    assert restored["error"] == "Interrupted by restart"


def test_downloadstore_rejects_invalid_terminal_transition(tmp_path):
    import app as librarr_app

    db_path = tmp_path / "state-machine.db"
    store = librarr_app.DownloadStore(str(db_path))
    store["job1"] = librarr_app._base_job_fields("Book", "test")
    store["job1"]["status"] = "completed"
    store["job1"]["status"] = "downloading"  # invalid from terminal
    assert store["job1"]["status"] == "completed"


def test_retry_scheduler_metadata_progresses_to_dead_letter(tmp_path):
    import app as librarr_app

    db_path = tmp_path / "retry.db"
    store = librarr_app.DownloadStore(str(db_path))
    old_store = librarr_app.download_jobs
    try:
        librarr_app.download_jobs = store
        store["job1"] = librarr_app._base_job_fields("Book", "test", max_retries=1)
        librarr_app._schedule_or_dead_letter("job1", "first fail", retry_kind="source", retry_payload={})
        assert store["job1"]["status"] == "retry_wait"
        assert store["job1"]["retry_count"] == 1
        librarr_app._schedule_or_dead_letter("job1", "second fail", retry_kind="source", retry_payload={})
        assert store["job1"]["status"] == "dead_letter"
        assert store["job1"]["retry_count"] == 2
    finally:
        librarr_app.download_jobs = old_store


def test_source_health_circuit_breaker_opens_and_recovers():
    import app as librarr_app

    tracker = librarr_app.SourceHealthTracker(threshold=2, open_seconds=1)
    assert tracker.can_search("annas") is True
    tracker.record_failure("annas", "timeout", kind="search")
    assert tracker.can_search("annas") is True
    snap = tracker.record_failure("annas", "timeout", kind="search")
    assert snap["search_fail_streak"] == 2
    assert tracker.can_search("annas") is False
    tracker.record_success("annas", kind="search")
    assert tracker.can_search("annas") is True


def test_qb_test_connection_classifies_timeout(monkeypatch):
    import app as librarr_app

    class FakeSession:
        def post(self, *args, **kwargs):
            raise librarr_app.requests.Timeout("boom")

    monkeypatch.setattr(librarr_app.requests, "Session", lambda: FakeSession())
    result = librarr_app._test_qbittorrent_connection("http://qb:8080", "jam", "1301")
    assert result["success"] is False
    assert result["error_class"] == "timeout"


def test_api_download_dry_run_and_duplicate_precheck(client, monkeypatch):
    import app as librarr_app

    class FakeSource:
        name = "fake"
        label = "Fake"
        download_type = "direct"
        search_tab = "main"
        def enabled(self): return True

    monkeypatch.setattr(librarr_app.sources, "get_source", lambda name: FakeSource() if name == "fake" else None)

    # Dry-run should return preflight info without queuing a job
    r = client.post("/api/download", json={
        "source": "fake",
        "title": "Dune",
        "source_id": "fake-1",
        "dry_run": True,
    })
    assert r.status_code == 200
    d = r.get_json()
    assert d["dry_run"] is True
    assert d["duplicate_check"]["duplicate"] is False

    # Add duplicate to tracked library and verify queue is blocked
    librarr_app.library.add_item(title="Dune", source="fake", source_id="fake-1")
    r = client.post("/api/download", json={
        "source": "fake",
        "title": "Dune",
        "source_id": "fake-1",
    })
    assert r.status_code == 409
    d = r.get_json()
    assert d["duplicate_check"]["duplicate"] is True
