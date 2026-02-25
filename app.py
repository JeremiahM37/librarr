"""
Librarr — Self-hosted book search and download manager.

Searches Anna's Archive, Prowlarr indexers, AudioBookBay, and web novel sites.
Downloads via direct HTTP, qBittorrent, or lightnovel-crawler.
Auto-imports into Calibre-Web and Audiobookshelf.
"""
import glob
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

import requests
from flask import Flask, Response, jsonify, redirect, request, send_file, session, url_for

import config
import pipeline
import monitor
import opds
import sources
import telemetry
from library_db import LibraryDB
from db_migrations import apply_migrations, get_migration_status

app = Flask(__name__)
app.secret_key = config.SECRET_KEY
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("librarr")

JOB_MAX_RETRIES = max(0, int(os.getenv("LIBRARR_JOB_MAX_RETRIES", "2")))
JOB_RETRY_BACKOFF_SEC = max(1, int(os.getenv("LIBRARR_JOB_RETRY_BACKOFF_SEC", "60")))
SOURCE_CIRCUIT_FAILURE_THRESHOLD = max(1, int(os.getenv("LIBRARR_SOURCE_CIRCUIT_FAILURE_THRESHOLD", "3")))
SOURCE_CIRCUIT_OPEN_SEC = max(5, int(os.getenv("LIBRARR_SOURCE_CIRCUIT_OPEN_SEC", "300")))
JOB_STATE_TRANSITIONS = {
    None: {"queued", "searching", "downloading", "importing", "retry_wait", "completed", "error", "dead_letter"},
    "queued": {"searching", "downloading", "importing", "completed", "retry_wait", "error", "dead_letter"},
    "searching": {"queued", "downloading", "completed", "retry_wait", "error", "dead_letter"},
    "downloading": {"importing", "completed", "retry_wait", "error", "dead_letter"},
    "importing": {"completed", "retry_wait", "error", "dead_letter"},
    "retry_wait": {"queued", "error", "dead_letter"},
    "error": {"queued", "retry_wait", "dead_letter"},
    "dead_letter": {"queued"},
    "completed": set(),
}


def _job_transition_allowed(old_status, new_status):
    return new_status in JOB_STATE_TRANSITIONS.get(old_status, set())


def _record_job_status_transition(job_id, old_status, new_status, job_data):
    telemetry.metrics.inc(
        "librarr_job_transitions_total",
        from_status=old_status or "none",
        to_status=new_status,
        source=job_data.get("source", "unknown"),
    )
    if new_status in ("completed", "error", "dead_letter"):
        telemetry.metrics.inc(
            "librarr_job_terminal_total",
            status=new_status,
            source=job_data.get("source", "unknown"),
        )
    if new_status == "retry_wait":
        telemetry.metrics.inc(
            "librarr_job_retry_scheduled_total",
            source=job_data.get("source", "unknown"),
        )
    if new_status in ("completed", "error", "dead_letter", "retry_wait"):
        telemetry.emit_event(
            f"job_{new_status}",
            {
                "job_id": job_id,
                "title": job_data.get("title"),
                "source": job_data.get("source"),
                "status": new_status,
                "retry_count": job_data.get("retry_count", 0),
                "max_retries": job_data.get("max_retries", JOB_MAX_RETRIES),
                "error": job_data.get("error"),
                "detail": job_data.get("detail"),
            },
        )


# =============================================================================
# Authentication
# =============================================================================
PUBLIC_PATHS = {"/login", "/api/health", "/metrics"}
PUBLIC_PREFIXES = ("/static/",)


@app.before_request
def require_auth():
    """Protect all routes when auth is configured."""
    if not config.has_auth():
        return None

    path = request.path

    # Public paths
    if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
        return None

    # Check API key (header or query param)
    api_key = request.headers.get("X-Api-Key") or request.args.get("apikey")
    if api_key and api_key == config.API_KEY:
        return None

    # Check session
    if session.get("authenticated"):
        return None

    # Not authenticated
    if request.path.startswith("/api/"):
        return jsonify({"error": "Unauthorized"}), 401
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if not config.has_auth():
        return redirect("/")

    if request.method == "GET":
        return send_file("templates/login.html")

    # POST — authenticate
    data = request.form if request.form else (request.json or {})
    username = data.get("username", "")
    password = data.get("password", "")

    if username == config.AUTH_USERNAME and config.verify_password(password, config.AUTH_PASSWORD):
        session["authenticated"] = True
        if request.is_json:
            return jsonify({"success": True})
        return redirect("/")

    error = "Invalid username or password"
    if request.is_json:
        return jsonify({"success": False, "error": error}), 401
    # Re-serve login page with error via query param
    return redirect(url_for("login", error="1"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# =============================================================================
# Persistent Download Job Store (SQLite-backed)
# =============================================================================
class DownloadStore:
    """Dict-like download job tracker backed by SQLite for persistence.

    Jobs survive container restarts. Access pattern is the same as a dict:
        download_jobs[job_id] = {...}
        download_jobs[job_id]["status"] = "downloading"
        for job_id, job in download_jobs.items(): ...
    """

    def __init__(self, db_path):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._cache = {}
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()
        self._load_all()

    def _connect(self):
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            apply_migrations(conn)

    def _load_all(self):
        with self._connect() as conn:
            rows = conn.execute("SELECT job_id, data FROM download_jobs").fetchall()
        stale = 0
        for job_id, data_str in rows:
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue
            # Mark jobs that were in-progress when the app stopped as failed
            if data.get("status") in ("queued", "searching", "downloading", "importing"):
                data["status"] = "error"
                data["error"] = "Interrupted by restart"
                data["last_error_at"] = time.time()
                stale += 1
            data.setdefault("retry_count", 0)
            data.setdefault("max_retries", JOB_MAX_RETRIES)
            data.setdefault("next_retry_at", None)
            data.setdefault("failure_history", [])
            data.setdefault("status_history", [])
            self._cache[job_id] = _PersistentJob(self, job_id, data)
        if self._cache:
            msg = f"Restored {len(self._cache)} download jobs from database"
            if stale:
                msg += f" ({stale} marked as failed due to restart)"
            logger.info(msg)

    def _persist(self, job_id, data):
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO download_jobs (job_id, data, updated_at)
                       VALUES (?, ?, strftime('%s', 'now'))""",
                    (job_id, json.dumps(data)),
                )

    def _delete(self, job_id):
        with self._lock:
            self._cache.pop(job_id, None)
            with self._connect() as conn:
                conn.execute("DELETE FROM download_jobs WHERE job_id = ?", (job_id,))

    def __setitem__(self, job_id, value):
        data = dict(value)
        data.setdefault("retry_count", 0)
        data.setdefault("max_retries", JOB_MAX_RETRIES)
        data.setdefault("next_retry_at", None)
        data.setdefault("failure_history", [])
        data.setdefault("status_history", [])
        job = _PersistentJob(self, job_id, data)
        self._cache[job_id] = job
        self._persist(job_id, job._data)
        if data.get("status"):
            _record_job_status_transition(job_id, None, data["status"], data)

    def __getitem__(self, job_id):
        return self._cache[job_id]

    def __contains__(self, job_id):
        return job_id in self._cache

    def __delitem__(self, job_id):
        self._delete(job_id)

    def items(self):
        return list(self._cache.items())

    def get(self, job_id, default=None):
        return self._cache.get(job_id, default)

    def transition(self, job_id, status, **updates):
        job = self._cache[job_id]
        job["status"] = status
        for key, value in updates.items():
            job[key] = value
        return job


class _PersistentJob:
    """Wraps a job dict so that mutations auto-persist to SQLite."""

    def __init__(self, store, job_id, data):
        self._store = store
        self._job_id = job_id
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if key == "status":
            old_status = self._data.get("status")
            new_status = value
            if old_status != new_status and not _job_transition_allowed(old_status, new_status):
                telemetry.metrics.inc(
                    "librarr_job_invalid_transitions_total",
                    from_status=old_status or "none",
                    to_status=new_status,
                    source=self._data.get("source", "unknown"),
                )
                logger.warning(
                    "Rejected invalid job status transition %s -> %s for %s",
                    old_status, new_status, self._job_id,
                )
                return
            if old_status != new_status:
                history = list(self._data.get("status_history") or [])
                history.append({"from": old_status, "to": new_status, "ts": time.time()})
                self._data["status_history"] = history[-25:]
        self._data[key] = value
        if key == "error" and value:
            self._data["last_error_at"] = time.time()
        self._store._persist(self._job_id, self._data)
        if key == "status":
            old_status = self._data.get("status_history", [])[-1]["from"] if self._data.get("status_history") else None
            if old_status != value:
                _record_job_status_transition(self._job_id, old_status, value, self._data)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __repr__(self):
        return repr(self._data)


_DB_PATH = os.getenv("LIBRARR_DB_PATH", "/data/librarr/downloads.db")
download_jobs = DownloadStore(_DB_PATH)
library = LibraryDB(_DB_PATH)
_monitor = None  # initialized in __main__
_retry_loop_started = False
_retry_thread_lock = threading.Lock()


def _base_job_fields(title, source, **extra):
    data = {
        "status": "queued",
        "title": title,
        "source": source,
        "error": None,
        "detail": None,
        "retry_count": 0,
        "max_retries": JOB_MAX_RETRIES,
        "next_retry_at": None,
        "failure_history": [],
        "status_history": [],
    }
    data.update(extra)
    return data


def _schedule_or_dead_letter(job_id, error_message, *, retry_kind=None, retry_payload=None):
    job = download_jobs[job_id]
    job["error"] = error_message
    failures = list(job.get("failure_history") or [])
    failures.append({"ts": time.time(), "error": error_message})
    job["failure_history"] = failures[-10:]
    retry_count = int(job.get("retry_count", 0)) + 1
    job["retry_count"] = retry_count
    max_retries = int(job.get("max_retries", JOB_MAX_RETRIES))
    if retry_kind:
        job["retry_kind"] = retry_kind
    if retry_payload is not None:
        job["retry_payload"] = retry_payload
    if retry_count <= max_retries:
        delay = JOB_RETRY_BACKOFF_SEC * retry_count
        job["next_retry_at"] = time.time() + delay
        job["detail"] = f"Retry {retry_count}/{max_retries} scheduled in {delay}s"
        job["status"] = "retry_wait"
    else:
        job["next_retry_at"] = None
        job["detail"] = f"Moved to dead-letter after {retry_count - 1} retries"
        job["status"] = "dead_letter"


def _reset_job_for_retry(job_id):
    job = download_jobs[job_id]
    job["error"] = None
    job["detail"] = f"Retrying (attempt {int(job.get('retry_count', 0)) + 1})..."
    job["next_retry_at"] = None
    job["status"] = "queued"


def _start_job_thread(target, args):
    t = threading.Thread(target=target, args=args, daemon=True)
    t.start()
    return t


def _run_source_download_worker(job_id, source_name, data):
    source = sources.get_source(source_name)
    if not source:
        _schedule_or_dead_letter(job_id, f"Unknown source: {source_name}", retry_kind="source", retry_payload={"source_name": source_name, "data": data})
        return
    download_jobs[job_id]["status"] = "downloading"
    try:
        success = source.download(data, download_jobs[job_id])
        _record_source_download_result(source_name, bool(success), download_jobs[job_id].get("error", ""))
        if not success and download_jobs[job_id]["status"] != "completed":
            msg = download_jobs[job_id].get("error") or "Download failed"
            _schedule_or_dead_letter(job_id, msg, retry_kind="source", retry_payload={"source_name": source_name, "data": data})
    except Exception as e:
        logger.error(f"Download error ({source_name}): {e}")
        _record_source_download_result(source_name, False, str(e))
        _schedule_or_dead_letter(job_id, str(e), retry_kind="source", retry_payload={"source_name": source_name, "data": data})


def _dispatch_retry(job_id):
    job = download_jobs.get(job_id)
    if not job:
        return False
    retry_kind = job.get("retry_kind")
    payload = job.get("retry_payload") or {}
    _reset_job_for_retry(job_id)
    if retry_kind == "novel":
        _start_job_thread(download_novel_worker, (job_id, payload.get("url", job.get("url", "")), payload.get("title", job.get("title", "Unknown"))))
        return True
    if retry_kind == "annas":
        _start_job_thread(download_annas_worker, (job_id, payload.get("md5", ""), payload.get("title", job.get("title", "Unknown"))))
        return True
    if retry_kind == "source":
        _start_job_thread(_run_source_download_worker, (job_id, payload.get("source_name", job.get("source", "")), payload.get("data", payload)))
        return True
    logger.warning("No retry handler for job %s (kind=%s)", job_id, retry_kind)
    download_jobs[job_id]["error"] = f"No retry handler for kind={retry_kind}"
    download_jobs[job_id]["status"] = "dead_letter"
    return False


def _retry_scheduler_loop():
    while True:
        now = time.time()
        for job_id, job in list(download_jobs.items()):
            if job.get("status") != "retry_wait":
                continue
            next_retry_at = job.get("next_retry_at") or 0
            if next_retry_at and next_retry_at <= now:
                try:
                    _dispatch_retry(job_id)
                except Exception as e:
                    logger.error("Retry dispatch failed for %s: %s", job_id, e)
        time.sleep(2)


def _ensure_retry_scheduler():
    global _retry_loop_started
    with _retry_thread_lock:
        if _retry_loop_started:
            return
        _retry_loop_started = True
        threading.Thread(target=_retry_scheduler_loop, daemon=True).start()


class SourceHealthTracker:
    """Tracks source reliability and temporarily opens a circuit on repeated failures."""

    def __init__(self, threshold=SOURCE_CIRCUIT_FAILURE_THRESHOLD, open_seconds=SOURCE_CIRCUIT_OPEN_SEC):
        self.threshold = max(1, int(threshold))
        self.open_seconds = max(1, int(open_seconds))
        self._lock = threading.Lock()
        self._data = {}

    def _row(self, name):
        row = self._data.get(name)
        if row is None:
            row = {
                "search_ok": 0,
                "search_fail": 0,
                "download_ok": 0,
                "download_fail": 0,
                "search_fail_streak": 0,
                "download_fail_streak": 0,
                "circuit_open_until": 0.0,
                "last_error": "",
                "last_error_kind": "",
                "last_error_at": 0.0,
                "last_success_at": 0.0,
                "score": 100.0,
            }
            self._data[name] = row
        return row

    def _recompute_score(self, row):
        total = row["search_ok"] + row["search_fail"] + row["download_ok"] + row["download_fail"]
        if total <= 0:
            row["score"] = 100.0
            return
        ok = row["search_ok"] + row["download_ok"]
        fail = row["search_fail"] + row["download_fail"]
        streak_penalty = 5 * max(row["search_fail_streak"], row["download_fail_streak"])
        row["score"] = max(0.0, round((ok / total) * 100 - (fail / total) * 10 - streak_penalty, 1))

    def can_search(self, name):
        with self._lock:
            row = self._row(name)
            return time.time() >= float(row.get("circuit_open_until", 0) or 0)

    def record_success(self, name, kind="search"):
        with self._lock:
            row = self._row(name)
            key_ok = f"{kind}_ok"
            key_streak = f"{kind}_fail_streak"
            if key_ok in row:
                row[key_ok] += 1
            if key_streak in row:
                row[key_streak] = 0
            row["last_success_at"] = time.time()
            was_open = time.time() < float(row.get("circuit_open_until", 0) or 0)
            row["circuit_open_until"] = 0.0
            self._recompute_score(row)
        if was_open:
            telemetry.emit_event("source_recovered", {"source": name, "kind": kind})

    def record_failure(self, name, error, kind="search"):
        opened = False
        with self._lock:
            row = self._row(name)
            key_fail = f"{kind}_fail"
            key_streak = f"{kind}_fail_streak"
            if key_fail in row:
                row[key_fail] += 1
            if key_streak in row:
                row[key_streak] += 1
            row["last_error"] = str(error)[:400]
            row["last_error_kind"] = kind
            row["last_error_at"] = time.time()
            if kind == "search" and row["search_fail_streak"] >= self.threshold:
                row["circuit_open_until"] = time.time() + self.open_seconds
                opened = True
            self._recompute_score(row)
            snapshot = dict(row)
        if opened:
            telemetry.emit_event("source_degraded", {
                "source": name,
                "kind": kind,
                "search_fail_streak": snapshot.get("search_fail_streak", 0),
                "circuit_open_until": snapshot.get("circuit_open_until", 0),
                "last_error": snapshot.get("last_error", ""),
            })
        return snapshot

    def snapshot(self):
        now = time.time()
        with self._lock:
            out = {}
            for name, row in self._data.items():
                info = dict(row)
                info["circuit_open"] = now < float(info.get("circuit_open_until", 0) or 0)
                info["circuit_retry_in_sec"] = max(0, int((info.get("circuit_open_until", 0) or 0) - now))
                out[name] = info
            return out


_source_health = SourceHealthTracker()


def _source_health_metadata():
    meta = sources.get_source_metadata()
    health = _source_health.snapshot()
    for name, source_meta in meta.items():
        h = health.get(name, {})
        source_meta["health"] = {
            "score": h.get("score", 100.0),
            "circuit_open": h.get("circuit_open", False),
            "circuit_retry_in_sec": h.get("circuit_retry_in_sec", 0),
            "search_fail_streak": h.get("search_fail_streak", 0),
            "last_error": h.get("last_error", ""),
        }
    return meta


def _search_source_safe(source, query):
    if not _source_health.can_search(source.name):
        telemetry.metrics.inc("librarr_source_search_total", source=source.name, result="circuit_open")
        return []
    start = time.time()
    try:
        results = source.search(query) or []
        _source_health.record_success(source.name, kind="search")
        telemetry.metrics.inc("librarr_source_search_total", source=source.name, result="ok")
        telemetry.metrics.inc(
            "librarr_source_search_results_total",
            source=source.name,
            result_count=str(min(len(results), 1000)),
        )
        return results
    except Exception as e:
        _source_health.record_failure(source.name, e, kind="search")
        telemetry.metrics.inc("librarr_source_search_total", source=source.name, result="error")
        logger.error(f"Search error ({source.name}): {e}")
        return []
    finally:
        _elapsed = int((time.time() - start) * 1000)
        telemetry.metrics.inc("librarr_source_search_calls_total", source=source.name)


def _record_source_download_result(source_name, ok, error=""):
    if ok:
        _source_health.record_success(source_name, kind="download")
        telemetry.metrics.inc("librarr_source_download_total", source=source_name, result="ok")
        return
    _source_health.record_failure(source_name, error or "download failed", kind="download")
    telemetry.metrics.inc("librarr_source_download_total", source=source_name, result="error")


def _truthy(v):
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("1", "true", "yes", "on")


def _extract_download_source_id(data):
    if not isinstance(data, dict):
        return ""
    return (
        data.get("source_id")
        or data.get("md5")
        or data.get("info_hash")
        or data.get("hash")
        or ""
    )


def _duplicate_summary(title="", source_id=""):
    summary = {"duplicate": False, "by_source_id": False, "by_title": False, "matches": []}
    if source_id and library.has_source_id(source_id):
        summary["duplicate"] = True
        summary["by_source_id"] = True
        summary["matches"].append({"kind": "source_id", "value": source_id})
    if title:
        rows = library.find_by_title(title)
        if rows:
            summary["duplicate"] = True
            summary["by_title"] = True
            summary["matches"].append({"kind": "title", "count": len(rows)})
    return summary


def _parse_requested_targets(data):
    raw = (data or {}).get("target_names")
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = [t.strip() for t in raw.split(",") if t.strip()]
    if not isinstance(raw, list):
        return None
    return [str(t).strip() for t in raw if str(t).strip()]


def _download_preflight_response(data, source_name="", source_type=""):
    title = (data or {}).get("title", "Unknown")
    source_id = _extract_download_source_id(data or {})
    duplicate = _duplicate_summary(title=title, source_id=source_id)
    target_names = _parse_requested_targets(data or {})
    return {
        "success": True,
        "dry_run": True,
        "title": title,
        "source": source_name or (data or {}).get("source", ""),
        "source_type": source_type,
        "source_id": source_id,
        "duplicate_check": duplicate,
        "target_names": target_names,
        "resolved_target_names": sorted(pipeline._resolve_target_names((data or {}).get("media_type", "ebook"), source_name or (data or {}).get("source", ""), target_names)),
        "qb": qb.diagnose() if source_type == "torrent" else None,
    }


def _validate_config():
    """Warn about misconfigured paths at startup."""
    paths_to_check = []
    if config.INCOMING_DIR:
        paths_to_check.append(("INCOMING_DIR", config.INCOMING_DIR))
    if config.EBOOK_ORGANIZED_DIR and config.FILE_ORG_ENABLED:
        paths_to_check.append(("EBOOK_ORGANIZED_DIR", config.EBOOK_ORGANIZED_DIR))
    if config.AUDIOBOOK_DIR:
        paths_to_check.append(("AUDIOBOOK_DIR", config.AUDIOBOOK_DIR))
    for name, path in paths_to_check:
        if not os.path.exists(path):
            logger.warning(f"Config path {name}={path!r} does not exist — creating it")
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                logger.error(f"Cannot create {name}={path!r}: {e}")


# =============================================================================
# qBittorrent Client
# =============================================================================
class QBittorrentClient:
    def __init__(self):
        self.session = requests.Session()
        self.authenticated = False
        self._ban_until = 0
        self.last_error = None

    def _set_last_error(self, kind, message, **extra):
        self.last_error = {"kind": kind, "message": message, "ts": time.time(), **extra}

    def _clear_last_error(self):
        self.last_error = None

    def _classify_exception(self, exc):
        if isinstance(exc, requests.Timeout):
            return "timeout", "Timed out connecting to qBittorrent"
        if isinstance(exc, requests.ConnectionError):
            return "unreachable", "Connection refused/unreachable — is qBittorrent running?"
        return "request_error", str(exc)

    def login(self):
        if not config.has_qbittorrent():
            self._set_last_error("not_configured", "qBittorrent not configured")
            return False
        try:
            resp = self.session.post(
                f"{config.QB_URL}/api/v2/auth/login",
                data={"username": config.QB_USER, "password": config.QB_PASS},
                timeout=10,
            )
            if "banned" in resp.text.lower():
                logger.error("qBittorrent: IP banned, backing off for 60s")
                self._ban_until = time.time() + 60
                self.authenticated = False
                self._set_last_error("ip_banned", "IP banned by qBittorrent", cooldown_sec=60)
                return False
            self.authenticated = resp.text == "Ok."
            if not self.authenticated:
                self._ban_until = time.time() + 30
                logger.error(f"qBittorrent login failed: {resp.text!r}")
                self._set_last_error("auth_failed", "Login failed — check username/password", response=resp.text[:120])
            else:
                self._clear_last_error()
            return self.authenticated
        except Exception as e:
            logger.error(f"qBittorrent login failed: {e}")
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return False

    def _ensure_auth(self):
        if not self.authenticated:
            if hasattr(self, '_ban_until') and time.time() < self._ban_until:
                logger.warning("qBittorrent: skipping login attempt, still in cooldown")
                self._set_last_error("cooldown", "Skipping login attempt during cooldown", retry_in_sec=int(self._ban_until - time.time()))
                return
            self.login()

    def add_torrent(self, url, title="", save_path=None, category=None):
        self._ensure_auth()
        data = {
            "urls": url,
            "savepath": save_path or config.QB_SAVE_PATH,
            "category": category or config.QB_CATEGORY,
        }
        try:
            resp = self.session.post(
                f"{config.QB_URL}/api/v2/torrents/add",
                data=data,
                timeout=15,
            )
            if resp.status_code == 403:
                self.login()
                resp = self.session.post(
                    f"{config.QB_URL}/api/v2/torrents/add",
                    data=data,
                    timeout=15,
                )
            ok = resp.text == "Ok."
            if ok:
                self._clear_last_error()
            elif resp.status_code == 403:
                self._set_last_error("auth_failed", "qBittorrent rejected add_torrent request (403)")
            else:
                self._set_last_error(f"http_{resp.status_code}", f"qBittorrent add_torrent returned HTTP {resp.status_code}")
            return ok
        except Exception as e:
            logger.error(f"qBittorrent add torrent failed: {e}")
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return False

    def get_torrents(self, category=None):
        self._ensure_auth()
        try:
            params = {}
            if category:
                params["category"] = category
            resp = self.session.get(
                f"{config.QB_URL}/api/v2/torrents/info",
                params=params,
                timeout=10,
            )
            if resp.status_code == 403:
                self.login()
                resp = self.session.get(
                    f"{config.QB_URL}/api/v2/torrents/info",
                    params=params,
                    timeout=10,
                )
            if resp.status_code == 200:
                self._clear_last_error()
                return resp.json()
            if resp.status_code == 403:
                self._set_last_error("auth_failed", "qBittorrent rejected torrents/info (403)")
            else:
                self._set_last_error(f"http_{resp.status_code}", f"qBittorrent torrents/info returned HTTP {resp.status_code}")
            return []
        except Exception as e:
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return []

    def delete_torrent(self, torrent_hash, delete_files=True):
        self._ensure_auth()
        try:
            resp = self.session.post(
                f"{config.QB_URL}/api/v2/torrents/delete",
                data={
                    "hashes": torrent_hash,
                    "deleteFiles": str(delete_files).lower(),
                },
                timeout=10,
            )
            if resp.status_code == 403:
                self.login()
                resp = self.session.post(
                    f"{config.QB_URL}/api/v2/torrents/delete",
                    data={
                        "hashes": torrent_hash,
                        "deleteFiles": str(delete_files).lower(),
                    },
                    timeout=10,
                )
            ok = resp.status_code == 200
            if ok:
                self._clear_last_error()
            elif resp.status_code == 403:
                self._set_last_error("auth_failed", "qBittorrent rejected delete_torrent request (403)")
            else:
                self._set_last_error(f"http_{resp.status_code}", f"qBittorrent delete_torrent returned HTTP {resp.status_code}")
            return ok
        except Exception as e:
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return False

    def diagnose(self):
        if not config.has_qbittorrent():
            return {"success": False, "error_class": "not_configured", "error": "qBittorrent not configured"}
        if self._ban_until and time.time() < self._ban_until:
            return {
                "success": False,
                "error_class": "cooldown",
                "error": "qBittorrent login cooldown active",
                "retry_in_sec": int(self._ban_until - time.time()),
            }
        try:
            self._ensure_auth()
            if not self.authenticated:
                err = self.last_error or {}
                return {"success": False, "error_class": err.get("kind", "auth_failed"), "error": err.get("message", "Login failed")}
            resp = self.session.get(f"{config.QB_URL}/api/v2/app/version", timeout=5)
            if resp.status_code == 403:
                self.authenticated = False
                self._set_last_error("auth_failed", "qBittorrent rejected app/version (403)")
                return {"success": False, "error_class": "auth_failed", "error": "Session expired or invalid credentials"}
            if resp.status_code != 200:
                self._set_last_error(f"http_{resp.status_code}", f"HTTP {resp.status_code}")
                return {"success": False, "error_class": f"http_{resp.status_code}", "error": f"HTTP {resp.status_code}"}
            self._clear_last_error()
            return {"success": True, "version": resp.text.strip() or "unknown"}
        except Exception as e:
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return {"success": False, "error_class": kind, "error": msg}


qb = QBittorrentClient()


# =============================================================================
# Prowlarr Search
# =============================================================================
def search_prowlarr(query):
    if not config.has_prowlarr():
        return []
    results = []
    try:
        resp = requests.get(
            f"{config.PROWLARR_URL}/api/v1/search",
            params={
                "query": query,
                "categories": [7000, 7020],
                "type": "search",
                "limit": 50,
            },
            headers={"X-Api-Key": config.PROWLARR_API_KEY},
            timeout=30,
        )
        for item in resp.json():
            size = item.get("size", 0)
            results.append({
                "source": "torrent",
                "title": item.get("title", ""),
                "size": size,
                "size_human": _human_size(size),
                "seeders": item.get("seeders", 0),
                "leechers": item.get("leechers", 0),
                "indexer": item.get("indexer", ""),
                "download_url": item.get("downloadUrl", ""),
                "magnet_url": item.get("magnetUrl", ""),
                "info_hash": item.get("infoHash", ""),
                "guid": item.get("guid", ""),
            })
    except Exception as e:
        logger.error(f"Prowlarr search failed: {e}")
    return results


def search_prowlarr_audiobooks(query):
    if not config.has_prowlarr():
        return []
    results = []
    seen_hashes = set()
    searches = [
        {"query": query, "categories": [3030], "type": "search", "limit": 50},
        {"query": f"{query} audiobook", "type": "search", "limit": 30},
    ]
    for params in searches:
        try:
            resp = requests.get(
                f"{config.PROWLARR_URL}/api/v1/search",
                params=params,
                headers={"X-Api-Key": config.PROWLARR_API_KEY},
                timeout=30,
            )
            for item in resp.json():
                ih = item.get("infoHash", "")
                if ih and ih in seen_hashes:
                    continue
                if ih:
                    seen_hashes.add(ih)
                size = item.get("size", 0)
                results.append({
                    "source": "audiobook",
                    "title": item.get("title", ""),
                    "size": size,
                    "size_human": _human_size(size),
                    "seeders": item.get("seeders", 0),
                    "leechers": item.get("leechers", 0),
                    "indexer": item.get("indexer", ""),
                    "download_url": item.get("downloadUrl", ""),
                    "magnet_url": item.get("magnetUrl", ""),
                    "info_hash": ih,
                    "guid": item.get("guid", ""),
                })
        except Exception as e:
            logger.error(f"Prowlarr audiobook search failed: {e}")
    return results


# =============================================================================
# AudioBookBay Search
# =============================================================================
# Primary and fallback domains for AudioBookBay
ABB_DOMAINS = [
    "https://audiobookbay.lu",
    "https://audiobookbay.is",
    "https://audiobookbay.li",
]
ABB_URL = ABB_DOMAINS[0]
ABB_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.dler.org:6969/announce",
    "http://tracker.files.fm:6969/announce",
]



def _get_abb_response(path, params=None, **kwargs):
    """Try ABB domains in order, return first successful response."""
    for domain in ABB_DOMAINS:
        try:
            resp = requests.get(
                f"{domain}{path}",
                params=params,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=15,
                **kwargs
            )
            if resp.status_code == 200:
                return resp, domain
        except Exception:
            continue
    return None, None


def search_audiobookbay(query):
    results = []
    try:
        resp, _active_domain = _get_abb_response("/", params={"s": query, "tt": "1"})
        if resp is None:
            return results

        content = resp.text[resp.text.find('<div id="content">'):]

        entries = re.findall(
            r'<h2[^>]*><a href="(/abss/[^"]+)"[^>]*>(.*?)</a></h2>'
            r'.*?<div class="postInfo">(.*?)</div>',
            content, re.DOTALL,
        )

        for url, title_raw, info_raw in entries:
            title = re.sub(r'<[^>]+>', '', title_raw).strip()
            if not title:
                continue
            lang_m = re.search(r'Language:\s*(\w+)', info_raw)
            lang = lang_m.group(1) if lang_m else ""
            if lang and lang.lower() not in ("english", ""):
                continue
            results.append({
                "source": "audiobook",
                "title": title,
                "size": 0,
                "size_human": "?",
                "seeders": 0,
                "leechers": 0,
                "indexer": "AudioBookBay",
                "download_url": "",
                "magnet_url": "",
                "info_hash": "",
                "abb_url": url,
            })
    except Exception as e:
        logger.error(f"AudioBookBay search failed: {e}")
    return results


def _resolve_abb_magnet(abb_path):
    try:
        resp, _domain = _get_abb_response(abb_path)
        if resp is None:
            return None
        hash_m = re.search(r'Info Hash:.*?<td[^>]*>\s*([0-9a-fA-F]{40})', resp.text, re.DOTALL)
        if not hash_m:
            return None
        info_hash = hash_m.group(1)
        trackers = re.findall(r'<td>((?:udp|http)://[^<]+)</td>', resp.text)
        if not trackers:
            trackers = ABB_TRACKERS
        tr_params = "&".join(f"tr={requests.utils.quote(t)}" for t in trackers)
        title_m = re.search(r'<h1[^>]*>(.*?)</h1>', resp.text)
        dn = requests.utils.quote(re.sub(r'<[^>]+>', '', title_m.group(1)).strip()) if title_m else ""
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={dn}&{tr_params}"
        return magnet
    except Exception as e:
        logger.error(f"ABB resolve failed: {e}")
        return None


def _human_size(size_bytes):
    if not size_bytes:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


# =============================================================================
# Web Novel Search
# =============================================================================
class FreeWebNovelParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.results = []
        self._in_title = False
        self._current = None
        self._capture_text = False
        self._text_target = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        cls = attrs_dict.get("class", "")
        if tag == "div" and "li-row" in cls:
            self._current = {"source": "webnovel", "site": "FreeWebNovel"}
        if self._current and tag == "a" and "tit" in cls:
            self._current["url"] = attrs_dict.get("href", "")
            if self._current["url"] and not self._current["url"].startswith("http"):
                self._current["url"] = "https://freewebnovel.com" + self._current["url"]
            self._in_title = True
        if self._current and tag == "span":
            if "s1" in cls:
                self._capture_text, self._text_target = True, "genre"
            elif "s2" in cls:
                self._capture_text, self._text_target = True, "status"
            elif "s3" in cls:
                self._capture_text, self._text_target = True, "update"
            elif "s4" in cls:
                self._capture_text, self._text_target = True, "chapters"

    def handle_data(self, data):
        if self._in_title and self._current:
            self._current["title"] = data.strip()
            self._in_title = False
        if self._capture_text and self._current and self._text_target:
            self._current[self._text_target] = data.strip()
            self._capture_text = False
            self._text_target = None

    def handle_endtag(self, tag):
        if tag == "div" and self._current and "title" in self._current:
            self.results.append(self._current)
            self._current = None


def search_freewebnovel(query):
    results = []
    try:
        resp = requests.get(
            "https://freewebnovel.com/search/",
            params={"searchkey": query},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        parser = FreeWebNovelParser()
        parser.feed(resp.text)
        results = parser.results
    except Exception as e:
        logger.error(f"FreeWebNovel search failed: {e}")
    return results


def search_allnovelfull(query):
    results = []
    try:
        resp = requests.get(
            "https://allnovelfull.net/search",
            params={"keyword": query},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        pattern = r'<h3[^>]*class="[^"]*truyen-title[^"]*"[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, resp.text)
        for url, title in matches:
            if not url.startswith("http"):
                url = "https://allnovelfull.net" + url
            results.append({
                "source": "webnovel",
                "site": "AllNovelFull",
                "title": title.strip(),
                "url": url,
            })
    except Exception as e:
        logger.error(f"AllNovelFull search failed: {e}")
    return results


def search_boxnovel(query):
    results = []
    try:
        resp = requests.get(
            "https://boxnovel.com/",
            params={"s": query, "post_type": "wp-manga"},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        pattern = r'<div class="post-title">\s*<h3[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, resp.text)
        for url, title in matches:
            results.append({
                "source": "webnovel",
                "site": "BoxNovel",
                "title": title.strip(),
                "url": url,
            })
    except Exception as e:
        logger.error(f"BoxNovel search failed: {e}")
    return results


def search_novelbin(query):
    results = []
    try:
        resp = requests.get(
            "https://novelbin.me/search",
            params={"keyword": query},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        pattern = r'<h3[^>]*class="[^"]*novel-title[^"]*"[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, resp.text)
        if not matches:
            matches = re.findall(
                r'<a\s+href="(https?://novelbin\.me/novel-book/[^"]+)"[^>]*title="([^"]+)"',
                resp.text,
            )
        seen = set()
        for url, title in matches:
            if not url.startswith("http"):
                url = "https://novelbin.me" + url
            if "/cchapter-" in url or "/chapter-" in url:
                continue
            if url in seen:
                continue
            seen.add(url)
            results.append({
                "source": "webnovel",
                "site": "NovelBin",
                "title": title.strip(),
                "url": url,
            })
    except Exception as e:
        logger.error(f"NovelBin search failed: {e}")
    return results


def search_novelfull(query):
    results = []
    try:
        resp = requests.get(
            "https://novelfull.com/ajax/search-novel",
            params={"keyword": query},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return results
        matches = re.findall(
            r'<a\s+href="([^"]+)"[^>]*class="list-group-item"[^>]*title="([^"]+)"',
            resp.text,
        )
        for url, title in matches:
            if "see more" in title.lower() or "search?" in url:
                continue
            if not url.startswith("http"):
                url = "https://novelfull.com" + url
            results.append({
                "source": "webnovel",
                "site": "NovelFull",
                "title": title.strip(),
                "url": url,
            })
    except Exception as e:
        logger.error(f"NovelFull search failed: {e}")
    return results


def search_lightnovelpub(query):
    results = []
    try:
        resp = requests.get(
            "https://www.lightnovelpub.com/lnwsearchlive",
            params={"inputContent": query},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=15,
        )
        if resp.status_code == 403:
            return results
        try:
            data = resp.json()
            for item in data.get("resultlist", []):
                url = item.get("novelNameHref", "")
                title = item.get("novelName", "")
                if not url or not title:
                    continue
                if not url.startswith("http"):
                    url = "https://www.lightnovelpub.com" + url
                results.append({
                    "source": "webnovel",
                    "site": "LightNovelPub",
                    "title": title.strip(),
                    "url": url,
                })
        except ValueError:
            pattern = r'<a\s+href="(/novel/[^"]+)"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, resp.text)
            for url, title in matches:
                results.append({
                    "source": "webnovel",
                    "site": "LightNovelPub",
                    "title": title.strip(),
                    "url": "https://www.lightnovelpub.com" + url,
                })
    except Exception as e:
        logger.error(f"LightNovelPub search failed: {e}")
    return results


def search_readnovelfull(query):
    results = []
    try:
        resp = requests.get(
            "https://readnovelfull.com/ajax/search-novel",
            params={"keyword": query},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return results
        matches = re.findall(
            r'<a\s+href="([^"]+)"[^>]*class="list-group-item"[^>]*title="([^"]+)"',
            resp.text,
        )
        for url, title in matches:
            if "see more" in title.lower() or "search?" in url:
                continue
            if not url.startswith("http"):
                url = "https://readnovelfull.com" + url
            results.append({
                "source": "webnovel",
                "site": "ReadNovelFull",
                "title": title.strip(),
                "url": url,
            })
    except Exception as e:
        logger.error(f"ReadNovelFull search failed: {e}")
    return results


def search_webnovels(query):
    all_results = []
    searchers = [
        search_allnovelfull,
        search_readnovelfull,
        search_novelfull,
        search_freewebnovel,
        search_novelbin,
        search_lightnovelpub,
        search_boxnovel,
    ]
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(fn, query): fn.__name__ for fn in searchers}
        for future in as_completed(futures, timeout=20):
            try:
                all_results.extend(future.result())
            except Exception as e:
                logger.error(f"Web novel search error ({futures[future]}): {e}")
    # Deduplicate by title, prefer sites in priority order
    site_priority = [
        "AllNovelFull", "ReadNovelFull", "NovelFull",
        "FreeWebNovel", "NovelBin", "LightNovelPub", "BoxNovel",
    ]
    grouped = {}
    for r in all_results:
        key = re.sub(r'[^a-z0-9]', '', r["title"].lower())
        if key not in grouped:
            grouped[key] = r
            grouped[key]["alt_urls"] = []
        else:
            existing_pri = next((i for i, s in enumerate(site_priority) if s in grouped[key].get("site", "")), 99)
            new_pri = next((i for i, s in enumerate(site_priority) if s == r.get("site", "")), 99)
            if new_pri < existing_pri:
                grouped[key]["alt_urls"].append(grouped[key].get("url", ""))
                grouped[key]["url"] = r.get("url", "")
                old_site = grouped[key].get("site", "")
                grouped[key]["site"] = r.get("site", "") + ", " + old_site
            else:
                grouped[key]["alt_urls"].append(r.get("url", ""))
                sites = grouped[key].get("site", "")
                new_site = r.get("site", "")
                if new_site and new_site not in sites:
                    grouped[key]["site"] = sites + ", " + new_site
    # Filter results that don't match the query
    stopwords = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "is", "it", "by"}
    q_words = set(re.findall(r'\w+', query.lower())) - stopwords
    filtered = []
    for r in grouped.values():
        t_words = set(re.findall(r'\w+', r["title"].lower())) - stopwords
        if q_words and t_words and len(q_words & t_words) >= 1:
            filtered.append(r)
    return filtered


def _read_audio_metadata(path: str):
    """Try to extract author/title from ID3/vorbis tags in audio files."""
    try:
        from mutagen import File as MutagenFile
    except ImportError:
        return None, None

    # Audio formats that support metadata tags (based on Mutagen support)
    extensions = (
        "*.mp3",  # ID3 tags (v1, v2.2, v2.3, v2.4)
        "*.m4b",  # MP4 metadata (iTunes-style)
        "*.m4a",  # MP4 metadata (iTunes-style)
        "*.flac",  # Vorbis comments
        "*.ogg",  # Vorbis comments
        "*.opus",  # Vorbis comments
        "*.oga",  # Ogg audio with Vorbis comments
        "*.wma",  # WMA Content Description / Extended Content Description
        "*.aiff",  # AIFF with ID3 tags
        "*.ape",  # Monkey's Audio with APEv2 tags
        "*.wv",  # WavPack with APEv2 tags
        "*.asf",  # ASF format with metadata
        "*.tta",  # True Audio with ID3/APEv2
    )

    if os.path.isfile(path):
        import fnmatch
        if any(fnmatch.fnmatch(os.path.basename(path), ext) for ext in extensions):
            try:
                audio = MutagenFile(path, easy=True)
                if audio:
                    title = (audio.get("album") or audio.get("title") or [None])[0]
                    author = (audio.get("artist") or audio.get("albumartist") or [None])[0]
                    if title or author:
                        return author, title
            except Exception:
                pass
        return None, None

    for ext in extensions:
        matches = glob.glob(os.path.join(path, "**", ext), recursive=True)
        if not matches:
            continue
        try:
            audio = MutagenFile(matches[0], easy=True)
            if not audio:
                continue
            title = (audio.get("album") or audio.get("title") or [None])[0]
            author = (audio.get("artist") or audio.get("albumartist") or [None])[0]
            if title or author:
                return author, title
        except Exception:
            continue
    return None, None
# =============================================================================
# Anna's Archive Search & Download
# =============================================================================
def _check_libgen_available(md5):
    try:
        resp = requests.get(
            f"https://libgen.li/ads.php?md5={md5}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=10,
        )
        if resp.status_code != 200:
            return False
        get_match = re.search(r'href="(get\.php\?md5=[^"]+)"', resp.text)
        if not get_match:
            return False
        dl_url = f"https://libgen.li/{get_match.group(1)}"
        dl_resp = requests.get(
            dl_url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=10,
            stream=True,
            allow_redirects=True,
        )
        ct = dl_resp.headers.get("Content-Type", "")
        dl_resp.close()
        if dl_resp.status_code >= 400:
            return False
        if "text/html" in ct:
            text = dl_resp.text if hasattr(dl_resp, '_content') else ""
            if "Error" in text or "not found" in text.lower():
                return False
        return True
    except Exception:
        return False


def search_annas_archive(query):
    results = []
    try:
        resp = requests.get(
            "https://annas-archive.li/search",
            params={"q": query, "ext": "epub"},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        if resp.status_code != 200:
            return results

        html = resp.text
        blocks = re.findall(
            r'<div class="flex\s+pt-3 pb-3 border-b.*?">(.*?)(?=<div class="flex\s+pt-3 pb-3 border-b|<footer)',
            html, re.DOTALL,
        )
        candidates = []
        for block in blocks[:20]:
            md5 = re.search(r'/md5/([a-f0-9]+)', block)
            if not md5:
                continue
            title_m = re.search(r'font-semibold text-lg[^>]*>(.*?)</a>', block, re.DOTALL)
            author_m = re.search(r'user-edit[^>]*></span>\s*(.*?)</a>', block, re.DOTALL)
            size = ""
            size_m = re.search(r'(\d+[\.\d]*\s*[KMG]i?B)', block)
            if size_m:
                size = size_m.group(1)

            title = re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else ""
            author = re.sub(r'<[^>]+>', '', author_m.group(1)).strip() if author_m else ""

            if not title:
                continue

            candidates.append({
                "source": "annas",
                "title": title,
                "author": author,
                "size_human": size,
                "md5": md5.group(1),
                "url": f"https://annas-archive.li/md5/{md5.group(1)}",
            })

        # Sort by size descending (complete compilations first)
        def _parse_size_bytes(s):
            if not s:
                return 0
            m = re.match(r'([\d.]+)\s*(GB|MB|KB|B)', s.strip(), re.IGNORECASE)
            if not m:
                return 0
            val = float(m.group(1))
            unit = m.group(2).upper()
            return val * {"GB": 1e9, "MB": 1e6, "KB": 1e3, "B": 1}.get(unit, 1)

        candidates.sort(key=lambda c: _parse_size_bytes(c.get("size_human", "")), reverse=True)

        # Verify top candidates are actually downloadable (parallel)
        if candidates:
            to_check = candidates[:20]
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(_check_libgen_available, c["md5"]): c for c in to_check}
                for future in as_completed(futures, timeout=45):
                    try:
                        if future.result():
                            results.append(futures[future])
                    except Exception:
                        pass
            results.sort(key=lambda c: _parse_size_bytes(c.get("size_human", "")), reverse=True)
            if not results:
                logger.info(f"Anna's Archive: all {len(to_check)} candidates for '{query}' are dead on libgen")
    except Exception as e:
        logger.error(f"Anna's Archive search failed: {e}")
    return results



# =============================================================================
# Background Novel Download
# =============================================================================
def _clean_incoming():
    incoming = config.INCOMING_DIR
    for d in ["epub", "json"]:
        path = os.path.join(incoming, d)
        if os.path.isdir(path):
            subprocess.run(["rm", "-rf", path], timeout=10)
    for f in ["meta.json", "cover.jpg"]:
        path = os.path.join(incoming, f)
        if os.path.isfile(path):
            os.remove(path)
    for zf in glob.glob(os.path.join(incoming, "*.zip")):
        os.remove(zf)
    for cf in glob.glob(os.path.join(incoming, "cover.*")):
        os.remove(cf)


def download_novel_worker(job_id, url, title):
    download_jobs[job_id]["status"] = "downloading"

    # Step 1: Check Anna's Archive for a pre-made EPUB
    download_jobs[job_id]["detail"] = "Checking Anna's Archive for pre-made EPUB..."
    logger.info(f"[{title}] Checking Anna's Archive first...")
    try:
        annas_results = search_annas_archive(title)
        if annas_results:
            def _parse_size_mb(s):
                if not s:
                    return 0
                s = s.strip().upper()
                m = re.match(r'([\d.]+)\s*(GB|MB|KB|B)', s)
                if not m:
                    return 0
                val = float(m.group(1))
                unit = m.group(2)
                return val * {"GB": 1024, "MB": 1, "KB": 1/1024, "B": 1/(1024*1024)}.get(unit, 0)

            def _title_match(query, candidate_title):
                q = query.lower().strip()
                c = candidate_title.lower().strip()
                if q in c or c in q:
                    return True
                stopwords = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "is", "it", "by"}
                q_words = set(re.findall(r'\w+', q)) - stopwords
                c_words = set(re.findall(r'\w+', c)) - stopwords
                if not q_words:
                    return False
                overlap = len(q_words & c_words) / len(q_words)
                return overlap >= 0.8

            matched = [r for r in annas_results if _title_match(title, r["title"])]
            if matched:
                matched.sort(key=lambda r: _parse_size_mb(r.get("size_human", "")), reverse=True)
                for i, candidate in enumerate(matched[:3]):
                    logger.info(f"[{title}] Trying Anna's Archive #{i+1}: {candidate['title']} ({candidate.get('size_human', '?')})")
                    download_jobs[job_id]["detail"] = f"Found EPUB on Anna's Archive ({candidate.get('size_human', '?')})! Downloading..."
                    if _download_from_annas(job_id, candidate["md5"], title):
                        return
                    logger.info(f"[{title}] Anna's Archive candidate #{i+1} failed, trying next...")
            else:
                logger.info(f"[{title}] Anna's Archive had results but none matched title")
        else:
            logger.info(f"[{title}] Not found on Anna's Archive")
    except Exception as e:
        logger.error(f"[{title}] Anna's Archive check failed: {e}")

    # Step 2: Fall back to lncrawl web scraping
    if not config.has_lncrawl():
        _schedule_or_dead_letter(
            job_id,
            "No pre-made EPUB found and lightnovel-crawler not configured",
            retry_kind="novel",
            retry_payload={"url": url, "title": title},
        )
        return

    source_urls = [url]
    try:
        download_jobs[job_id]["detail"] = "Finding best source for scraping..."
        alt_results = search_webnovels(title)
        for r in alt_results:
            if r.get("url") and r["url"] != url and r["url"] not in source_urls:
                source_urls.append(r["url"])
    except Exception:
        pass

    download_jobs[job_id]["detail"] = "No pre-made EPUB found. Scraping chapters..."
    download_jobs[job_id]["status"] = "downloading"

    for src_idx, src_url in enumerate(source_urls[:4]):
        try:
            _clean_incoming()
            subprocess.run(
                ["docker", "exec", "-u", "root", config.LNCRAWL_CONTAINER,
                 "chmod", "-R", "777", "/output"],
                capture_output=True, timeout=10,
            )

            site_name = src_url.split("//")[-1].split("/")[0]
            download_jobs[job_id]["detail"] = f"Scraping from {site_name} ({src_idx+1}/{min(len(source_urls),4)})..."
            logger.info(f"[{title}] Starting lncrawl from {src_url}")

            result = subprocess.run(
                [
                    "docker", "exec", config.LNCRAWL_CONTAINER,
                    "python3", "-m", "lncrawl",
                    "-s", src_url,
                    "--all",
                    "--noin", "--suppress",
                    "-o", "/output",
                    "--format", "epub",
                ],
                capture_output=True, text=True, timeout=7200,
            )

            epubs = glob.glob(os.path.join(config.INCOMING_DIR, "**", "*.epub"), recursive=True)

            if not epubs:
                logger.warning(f"[{title}] lncrawl produced no EPUB from {site_name}")
                continue

            # Validate EPUBs
            valid_epubs = []
            for ep in epubs:
                try:
                    import zipfile
                    with zipfile.ZipFile(ep, 'r') as zf:
                        zf.testzip()
                    valid_epubs.append(ep)
                except Exception:
                    logger.warning(f"[{title}] Corrupt EPUB: {os.path.basename(ep)}")

            if not valid_epubs:
                logger.warning(f"[{title}] All {len(epubs)} EPUBs from {site_name} are corrupt")
                _clean_incoming()
                continue

            valid_epubs.sort(key=lambda p: os.path.getsize(p), reverse=True)
            best_epub = valid_epubs[0]
            epub_size = os.path.getsize(best_epub)
            logger.info(f"[{title}] lncrawl produced {len(valid_epubs)} valid EPUBs from {site_name}, largest: {_human_size(epub_size)}")

            if epub_size < 500_000:
                logger.warning(f"[{title}] EPUB too small ({_human_size(epub_size)}) from {site_name}, trying next source")
                _clean_incoming()
                continue

            download_jobs[job_id]["status"] = "importing"
            download_jobs[job_id]["detail"] = f"Processing ({_human_size(epub_size)})..."

            pipeline.run_pipeline(
                best_epub, title=title, media_type="ebook",
                source="webnovel", source_id=url,
                job_id=job_id, library_db=library,
                target_names=download_jobs[job_id].get("target_names"),
            )
            download_jobs[job_id]["status"] = "completed"
            download_jobs[job_id]["detail"] = f"Done (scraped from {site_name}, {_human_size(epub_size)})"
            _clean_incoming()
            return

        except subprocess.TimeoutExpired:
            logger.warning(f"[{title}] lncrawl timed out on {src_url}")
            _clean_incoming()
            continue
        except Exception as e:
            if "Post-import verification failed" in str(e):
                _schedule_or_dead_letter(
                    job_id,
                    str(e),
                    retry_kind="novel",
                    retry_payload={"url": url, "title": title},
                )
                _clean_incoming()
                return
            logger.warning(f"[{title}] lncrawl error on {src_url}: {e}")
            _clean_incoming()
            continue

    _schedule_or_dead_letter(
        job_id,
        f"All {min(len(source_urls),4)} sources failed",
        retry_kind="novel",
        retry_payload={"url": url, "title": title},
    )


# =============================================================================
# Background Anna's Archive Download
# =============================================================================
def _try_download_url(url, job_id, connect_timeout=15, read_timeout=120):
    try:
        dl_resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=(connect_timeout, read_timeout),
            stream=True,
            allow_redirects=True,
        )
        content_type = dl_resp.headers.get("Content-Type", "")
        if dl_resp.status_code >= 400:
            logger.warning(f"[Anna's] HTTP {dl_resp.status_code} from {url[:60]}")
            return None, None

        if dl_resp.status_code == 200 and "text/html" in content_type:
            page_html = dl_resp.text
            if "File not found" in page_html or "Error</h1>" in page_html:
                logger.warning(f"[Anna's] Error page from {url[:60]}")
                return None, None
            get_link = re.search(r'href="(https?://[^"]+)"[^>]*>GET</a>', page_html)
            if not get_link:
                get_link = re.search(r'<a\s+href="(https?://[^"]+)"[^>]*>\s*GET\s*</a>', page_html, re.IGNORECASE)
            if not get_link:
                get_link = re.search(r'href="(https?://[^"]*\.(epub|pdf|mobi)[^"]*)"', page_html)
            if get_link:
                logger.info(f"[Anna's] Following: {get_link.group(1)}")
                dl_resp = requests.get(
                    get_link.group(1),
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=(connect_timeout, read_timeout),
                    stream=True,
                    allow_redirects=True,
                )
            else:
                return None, None

        if dl_resp.status_code != 200:
            return None, None

        title = download_jobs[job_id]["title"]
        safe_title = re.sub(r'[^\w\s-]', '', title)[:80].strip()
        filename = f"{safe_title}.epub"
        filepath = os.path.join(config.INCOMING_DIR, filename)

        total_size = int(dl_resp.headers.get("Content-Length", 0))
        downloaded = 0
        with open(filepath, "wb") as f:
            for chunk in dl_resp.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = int(downloaded * 100 / total_size)
                    download_jobs[job_id]["detail"] = f"Downloading... {pct}% ({_human_size(downloaded)} / {_human_size(total_size)})"
                else:
                    download_jobs[job_id]["detail"] = f"Downloading... {_human_size(downloaded)}"

        file_size = os.path.getsize(filepath)
        if file_size < 1000:
            os.remove(filepath)
            return None, None

        return filepath, file_size
    except Exception as e:
        logger.error(f"[Anna's] Download attempt failed for {url}: {e}")
        return None, None


def _download_from_annas(job_id, md5, title):
    try:
        download_jobs[job_id]["detail"] = "Fetching download link from Anna's Archive..."

        ads_resp = requests.get(
            f"https://libgen.li/ads.php?md5={md5}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )

        download_url = None
        if ads_resp.status_code == 200:
            get_match = re.search(r'href="(get\.php\?md5=[^"]+)"', ads_resp.text)
            if get_match:
                download_url = f"https://libgen.li/{get_match.group(1)}"
                logger.info(f"[Anna's] Found libgen GET link for {title}")

        if not download_url:
            download_jobs[job_id]["detail"] = "Trying alternative mirrors..."
            resp = requests.get(
                f"https://annas-archive.li/md5/{md5}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            if resp.status_code == 200:
                for m in re.finditer(r'href="(https?://libgen\.li/file\.php\?id=\d+)"', resp.text):
                    download_url = m.group(1)
                    break

        if not download_url:
            return False

        download_jobs[job_id]["detail"] = "Downloading EPUB..."
        logger.info(f"[Anna's] Downloading {title} from {download_url}")

        filepath, file_size = _try_download_url(download_url, job_id, connect_timeout=15, read_timeout=300)

        if not filepath:
            return False

        logger.info(f"[Anna's] Downloaded {title}: {_human_size(file_size)}")

        download_jobs[job_id]["status"] = "importing"
        download_jobs[job_id]["detail"] = "Processing..."

        pipeline.run_pipeline(
            filepath, title=title, media_type="ebook",
            source="annas", source_id=md5,
            job_id=job_id, library_db=library,
            target_names=download_jobs[job_id].get("target_names"),
        )
        download_jobs[job_id]["status"] = "completed"
        download_jobs[job_id]["detail"] = f"Done ({_human_size(file_size)})"
        return True

    except Exception as e:
        logger.error(f"[Anna's] Download error for {title}: {e}")
        try:
            download_jobs[job_id]["error"] = str(e)
        except Exception:
            pass
        return False


def download_annas_worker(job_id, md5, title):
    download_jobs[job_id]["status"] = "downloading"
    if not _download_from_annas(job_id, md5, title):
        if download_jobs[job_id]["status"] != "completed":
            _schedule_or_dead_letter(
                job_id,
                download_jobs[job_id].get("error") or "Download failed",
                retry_kind="annas",
                retry_payload={"md5": md5, "title": title},
            )


# =============================================================================
# Background Auto-Import for Completed Torrents
# =============================================================================
import_event = threading.Event()
imported_hashes = set()
_imported_hashes_lock = threading.Lock()


def import_completed_torrents():
    if not config.has_qbittorrent():
        return
    try:
        torrents = qb.get_torrents(category=config.QB_CATEGORY)
        for t in torrents:
            if t.get("progress", 0) < 1.0:
                continue
            with _imported_hashes_lock:
                if t["hash"] in imported_hashes:
                    continue
                imported_hashes.add(t["hash"])
            save_path = t.get("content_path", t.get("save_path", ""))
            # qBit reports paths as seen inside qBit's container.
            # If content_path exists on our filesystem, use it.
            # Otherwise, try replacing qBit's save path with our INCOMING_DIR.
            if not os.path.exists(save_path):
                qb_save = config.QB_SAVE_PATH.rstrip("/")
                local_incoming = config.INCOMING_DIR.rstrip("/")
                if qb_save and save_path.startswith(qb_save):
                    save_path = local_incoming + save_path[len(qb_save):]
                elif save_path.startswith("/books-incoming"):
                    save_path = save_path.replace("/books-incoming", config.INCOMING_DIR, 1)
            book_files = []
            for ext in ("*.epub", "*.mobi", "*.pdf", "*.azw3"):
                if os.path.isdir(save_path):
                    book_files.extend(
                        glob.glob(os.path.join(save_path, "**", ext), recursive=True)
                    )
                elif save_path.lower().endswith(ext[1:]):
                    book_files.append(save_path)
            for bf in book_files:
                pipeline.run_pipeline(
                    bf, title=t.get("name", ""),
                    media_type="ebook", source="torrent",
                    source_id=t["hash"], library_db=library,
                )
            qb.delete_torrent(t["hash"], delete_files=True)
            logger.info(f"Removed completed torrent: {t.get('name', t['hash'])}")
    except Exception as e:
        logger.error(f"Auto-import error: {e}")

    # --- Audiobook torrents ---
    try:
        torrents = qb.get_torrents(category=config.QB_AUDIOBOOK_CATEGORY)
        for t in torrents:
            if t.get("progress", 0) >= 1.0 and t["hash"] not in imported_hashes:
                save_path = t.get("content_path", t.get("save_path", ""))
                qb_ab_path = config.QB_AUDIOBOOK_SAVE_PATH.rstrip("/")
                if qb_ab_path and save_path.startswith(qb_ab_path):
                    save_path = config.AUDIOBOOK_DIR + save_path[len(qb_ab_path):]

                already_organised = (
                    config.FILE_ORG_ENABLED
                    and config.AUDIOBOOK_ORGANIZED_DIR
                    and os.path.abspath(save_path).startswith(
                        os.path.abspath(config.AUDIOBOOK_ORGANIZED_DIR)
                    )
                )

                if not already_organised and config.FILE_ORG_ENABLED:
                    author, resolved_title = "", t.get("name", "")

                    if os.path.isfile(save_path):
                        resolved_title = os.path.splitext(os.path.basename(save_path))[0]

                    if not author and os.path.exists(save_path):
                        id3_author, id3_title = _read_audio_metadata(save_path)
                        if id3_author or id3_title:
                            author = id3_author or author
                            resolved_title = id3_title or resolved_title

                    if not author and " - " in resolved_title:
                        parts = resolved_title.split(" - ", 1)
                        author, resolved_title = parts[0].strip(), parts[1].strip()

                    if not author:
                        author = "Unknown"

                    pipeline.run_pipeline(
                        save_path, title=resolved_title, author=author,
                        media_type="audiobook", source="torrent",
                        source_id=t["hash"], library_db=library,
                    )
                elif already_organised:
                    logger.info(f"Audiobook already in organised directory, skipping pipeline: {save_path}")

                if config.has_audiobookshelf() and config.ABS_LIBRARY_ID:
                    known_ids = set()
                    try:
                        resp = requests.get(
                            f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/items",
                            params={"limit": 500},
                            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                            timeout=15,
                        )
                        known_ids = {i["id"] for i in resp.json().get("results", [])}
                    except Exception:
                        pass
                    try:
                        requests.post(
                            f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/scan",
                            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                            timeout=10,
                        )
                        logger.info("Audiobookshelf library scan triggered")
                    except Exception as e:
                        logger.error(f"Audiobookshelf scan failed: {e}")
                    time.sleep(20)
                    _abs_match_new_items(known_ids)

                qb.delete_torrent(t["hash"], delete_files=True)
                logger.info(f"Removed completed audiobook torrent: {t.get('name', t['hash'])}")
                imported_hashes.add(t["hash"])
    except Exception as e:
        logger.error(f"Audiobook auto-import error: {e}")

    # --- Scan AUDIOBOOK_DIR for non-torrent drops ---
    if config.AUDIOBOOK_DIR and os.path.isdir(config.AUDIOBOOK_DIR):
        try:
            # Collect paths currently managed by qBittorrent so we don't touch them
            active_paths = set()
            if config.has_qbittorrent():
                for cat in (config.QB_CATEGORY, config.QB_AUDIOBOOK_CATEGORY):
                    try:
                        for t in qb.get_torrents(category=cat):
                            cp = t.get("content_path", t.get("save_path", ""))
                            qb_ab_path = config.QB_AUDIOBOOK_SAVE_PATH.rstrip("/")
                            if qb_ab_path and cp.startswith(qb_ab_path):
                                cp = config.AUDIOBOOK_DIR + cp[len(qb_ab_path):]
                            active_paths.add(os.path.abspath(cp))
                    except Exception:
                        pass

            for entry in os.listdir(config.AUDIOBOOK_DIR):
                entry_path = os.path.join(config.AUDIOBOOK_DIR, entry)
                abs_entry = os.path.abspath(entry_path)

                if abs_entry in active_paths or abs_entry in imported_hashes:
                    continue

                # Skip if already in the organised directory
                if (config.FILE_ORG_ENABLED and config.AUDIOBOOK_ORGANIZED_DIR
                        and abs_entry.startswith(os.path.abspath(config.AUDIOBOOK_ORGANIZED_DIR))):
                    continue

                # Must contain audio files
                audio_exts = (".mp3", ".m4b", ".m4a", ".flac", ".ogg", ".opus")
                has_audio = False
                if os.path.isdir(entry_path):
                    for root, _dirs, files in os.walk(entry_path):
                        if any(f.lower().endswith(audio_exts) for f in files):
                            has_audio = True
                            break
                elif any(entry.lower().endswith(ext) for ext in audio_exts):
                    has_audio = True

                if not has_audio:
                    continue

                author, resolved_title = "", entry
                if os.path.isfile(entry_path):
                    resolved_title = os.path.splitext(entry)[0]

                id3_author, id3_title = _read_audio_metadata(entry_path)
                if id3_author or id3_title:
                    author = id3_author or author
                    resolved_title = id3_title or resolved_title

                if not author and " - " in resolved_title:
                    parts = resolved_title.split(" - ", 1)
                    author, resolved_title = parts[0].strip(), parts[1].strip()

                if not author:
                    author = "Unknown"

                logger.info(f"Folder-scan importing audiobook: {entry}")
                pipeline.run_pipeline(
                    entry_path, title=resolved_title, author=author,
                    media_type="audiobook", source="folder-scan",
                    source_id=abs_entry, library_db=library,
                )
                imported_hashes.add(abs_entry)

                if config.has_audiobookshelf() and config.ABS_LIBRARY_ID:
                    try:
                        requests.post(
                            f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/scan",
                            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                            timeout=10,
                        )
                    except Exception:
                        pass
                    time.sleep(20)
                    _abs_match_new_items(set())
        except Exception as e:
            logger.error(f"Audiobook folder-scan error: {e}")


def auto_import_loop():
    while True:
        import_event.wait(timeout=10)
        import_event.clear()
        import_completed_torrents()


def watch_torrent(title):
    logger.info(f"Watching torrent: {title}")
    while True:
        try:
            torrents = qb.get_torrents(category=config.QB_CATEGORY)
            for t in torrents:
                if t.get("name") == title or t.get("progress", 0) >= 1.0:
                    if t.get("progress", 0) >= 1.0:
                        import_event.set()
                        return
            time.sleep(5)
        except Exception:
            time.sleep(5)


def _abs_match_new_items(known_ids):
    if not config.has_audiobookshelf() or not config.ABS_LIBRARY_ID:
        return
    try:
        resp = requests.get(
            f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/items",
            params={"limit": 100},
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=15,
        )
        for item in resp.json().get("results", []):
            item_id = item["id"]
            if item_id in known_ids:
                continue
            title = item.get("media", {}).get("metadata", {}).get("title", "")
            author = item.get("media", {}).get("metadata", {}).get("authorName", "")
            try:
                requests.post(
                    f"{config.ABS_URL}/api/items/{item_id}/match",
                    headers={
                        "Authorization": f"Bearer {config.ABS_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json={"provider": "audible"},
                    timeout=15,
                )
                logger.info(f"ABS auto-matched: {title} by {author}")
            except Exception as e:
                logger.error(f"ABS match failed for {title}: {e}")
    except Exception as e:
        logger.error(f"ABS match scan failed: {e}")


def watch_audiobook_torrent(title):
    logger.info(f"Watching audiobook torrent: {title}")
    while True:
        try:
            torrents = qb.get_torrents(category=config.QB_AUDIOBOOK_CATEGORY)
            for t in torrents:
                if t.get("name") == title and t.get("progress", 0) >= 1.0:
                    logger.info(f"Audiobook torrent completed: {title}")
                    save_path = t.get("content_path", t.get("save_path", ""))
                    qb_ab_path = config.QB_AUDIOBOOK_SAVE_PATH.rstrip("/")
                    if qb_ab_path and save_path.startswith(qb_ab_path):
                        save_path = config.AUDIOBOOK_DIR + save_path[len(qb_ab_path):]

                    already_organised = (
                        config.FILE_ORG_ENABLED
                        and config.AUDIOBOOK_ORGANIZED_DIR
                        and os.path.abspath(save_path).startswith(
                            os.path.abspath(config.AUDIOBOOK_ORGANIZED_DIR)
                        )
                    )

                    if not already_organised and config.FILE_ORG_ENABLED:
                        author, resolved_title = "", t.get("name", "")

                        if os.path.isfile(save_path):
                            resolved_title = os.path.splitext(os.path.basename(save_path))[0]

                        if not author and os.path.exists(save_path):
                            id3_author, id3_title = _read_audio_metadata(save_path)
                            if id3_author or id3_title:
                                author = id3_author or author
                                resolved_title = id3_title or resolved_title
                                logger.info(f"Metadata from ID3 tags: {author} - {resolved_title}")

                        if not author and " - " in resolved_title:
                            parts = resolved_title.split(" - ", 1)
                            author, resolved_title = parts[0].strip(), parts[1].strip()
                            logger.info(f"Metadata from torrent name parse: {author} - {resolved_title}")

                        if not author:
                            author = "Unknown"

                        pipeline.run_pipeline(
                            save_path, title=resolved_title, author=author,
                            media_type="audiobook", source="torrent",
                            source_id=t["hash"], library_db=library,
                        )
                    elif already_organised:
                        logger.info(f"Audiobook already in organised directory, skipping pipeline: {save_path}")
                    # if FILE_ORG_ENABLED is False, skip pipeline silently — just do ABS scan below

                    # ABS scan + match
                    if config.has_audiobookshelf() and config.ABS_LIBRARY_ID:
                        known_ids = set()
                        try:
                            resp = requests.get(
                                f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/items",
                                params={"limit": 500},
                                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                                timeout=15,
                            )
                            known_ids = {i["id"] for i in resp.json().get("results", [])}
                        except Exception:
                            pass
                        try:
                            requests.post(
                                f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/scan",
                                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                                timeout=10,
                            )
                            logger.info("Audiobookshelf library scan triggered")
                        except Exception as e:
                            logger.error(f"Audiobookshelf scan failed: {e}")
                        time.sleep(20)
                        _abs_match_new_items(known_ids)
                    return
            time.sleep(5)
        except Exception:
            time.sleep(5)
# =============================================================================
# Result Filtering
# =============================================================================
_SUSPICIOUS_KEYWORDS = re.compile(
    r'\.(exe|msi|bat|scr|com|vbs|js|ps1|cmd)\b|password|keygen|crack|warez|DevCourseWeb',
    re.IGNORECASE,
)
_STOPWORDS = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "is", "it", "by"}


def _title_relevant(query, title):
    q_words = set(re.findall(r'\w+', query.lower())) - _STOPWORDS
    t_words = set(re.findall(r'\w+', title.lower())) - _STOPWORDS
    if not q_words or not t_words:
        return False
    return len(q_words & t_words) >= 1


def filter_results(results, query):
    filtered = []
    seen_titles = {}

    for r in results:
        title = r.get("title", "")
        source = r.get("source", "")

        if _SUSPICIOUS_KEYWORDS.search(title):
            continue

        if source == "torrent":
            if r.get("seeders", 0) < 1:
                continue
            size = r.get("size", 0)
            if size and (size < 10_000 or size > 500_000_000):
                continue
            if not _title_relevant(query, title):
                continue
            norm = re.sub(r'[^a-z0-9]', '', title.lower())[:60]
            if norm in seen_titles:
                existing = seen_titles[norm]
                if r.get("seeders", 0) > existing.get("seeders", 0):
                    filtered.remove(existing)
                    seen_titles[norm] = r
                    filtered.append(r)
                continue
            seen_titles[norm] = r

        elif source == "audiobook":
            if r.get("seeders", 0) < 1 and not r.get("abb_url"):
                continue
            if not _title_relevant(query, title):
                continue

        filtered.append(r)

    return filtered


# =============================================================================
# API Routes
# =============================================================================
@app.route("/")
def index():
    return app.send_static_file("index.html") if os.path.exists(
        os.path.join(app.static_folder, "index.html")
    ) else send_file("templates/index.html")


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok", "version": "1.0.0"})


@app.route("/api/schema")
def api_schema_status():
    with sqlite3.connect(_DB_PATH, timeout=10) as conn:
        migrations = get_migration_status(conn)
    return jsonify({"migrations": migrations, "count": len(migrations)})


@app.route("/metrics")
def metrics_endpoint():
    status_counts = {}
    for _job_id, job in list(download_jobs.items()):
        status = job.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    lines = [
        "# HELP librarr_jobs_by_status Number of Librarr jobs by current status.",
        "# TYPE librarr_jobs_by_status gauge",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(f'librarr_jobs_by_status{{status="{status}"}} {count}')
    lines.extend([
        "# HELP librarr_library_items_total Number of tracked library items.",
        "# TYPE librarr_library_items_total gauge",
        f"librarr_library_items_total {library.count_items()}",
        "# HELP librarr_activity_events_total Number of activity log events.",
        "# TYPE librarr_activity_events_total gauge",
        f"librarr_activity_events_total {library.count_activity()}",
    ])
    lines.extend([
        "# HELP librarr_source_health_score Source health score (0-100).",
        "# TYPE librarr_source_health_score gauge",
        "# HELP librarr_source_circuit_open Whether source search circuit is open (1=open).",
        "# TYPE librarr_source_circuit_open gauge",
    ])
    for name, health in sorted(_source_health.snapshot().items()):
        score = float(health.get("score", 100.0))
        is_open = 1 if health.get("circuit_open") else 0
        lines.append(f'librarr_source_health_score{{source="{name}"}} {score}')
        lines.append(f'librarr_source_circuit_open{{source="{name}"}} {is_open}')
    return Response(telemetry.metrics.render(lines), mimetype="text/plain; version=0.0.4")


@app.route("/api/config")
def api_config():
    return jsonify({
        "prowlarr": config.has_prowlarr(),
        "qbittorrent": config.has_qbittorrent(),
        "calibre": config.has_calibre(),
        "audiobookshelf": config.has_audiobookshelf(),
        "lncrawl": config.has_lncrawl(),
        "audiobooks": config.has_audiobooks(),
        "kavita": config.has_kavita(),
        "file_org_enabled": config.FILE_ORG_ENABLED,
        "enabled_targets": list(config.get_enabled_target_names()),
        "auth_enabled": config.has_auth(),
    })


@app.route("/api/search")
def api_search():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"results": [], "error": "No query provided"})

    all_results = []
    start = time.time()
    enabled = sources.get_enabled_sources(tab="main")

    with ThreadPoolExecutor(max_workers=max(len(enabled), 1)) as executor:
        futures = {executor.submit(_search_source_safe, s, query): s for s in enabled}
        for future in as_completed(futures, timeout=35):
            source = futures[future]
            try:
                results = future.result()
                for r in results:
                    r.setdefault("source", source.name)
                all_results.extend(results)
            except Exception as e:
                logger.error(f"Search wrapper error ({source.name}): {e}")

    all_results = filter_results(all_results, query)
    elapsed = int((time.time() - start) * 1000)
    return jsonify({
        "results": all_results,
        "search_time_ms": elapsed,
        "sources": _source_health_metadata(),
    })


@app.route("/api/download/torrent", methods=["POST"])
def api_download_torrent():
    if not config.has_qbittorrent():
        return jsonify({"success": False, "error": "qBittorrent not configured. Set QB_URL, QB_USER, QB_PASS."}), 400

    data = request.json
    if _truthy((data or {}).get("dry_run")):
        return jsonify(_download_preflight_response(data or {}, source_name=(data or {}).get("source", "torrent"), source_type="torrent"))
    guid = data.get("guid", "")
    url = data.get("download_url") or (guid if guid.startswith("magnet:") else "") or data.get("magnet_url", "")
    if not url and data.get("info_hash"):
        url = f"magnet:?xt=urn:btih:{data['info_hash']}"
    title = data.get("title", "Unknown")

    if not url:
        return jsonify({"success": False, "error": "No download URL"}), 400

    dup = _duplicate_summary(title=title, source_id=_extract_download_source_id(data))
    if dup["duplicate"] and not _truthy(data.get("force")):
        return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

    success = qb.add_torrent(url, title)
    if success:
        threading.Thread(target=watch_torrent, args=(title,), daemon=True).start()
    return jsonify({"success": success, "title": title})


@app.route("/api/search/audiobooks")
def api_search_audiobooks():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"results": [], "error": "No query provided"})
    start = time.time()
    enabled = sources.get_enabled_sources(tab="audiobook")

    with ThreadPoolExecutor(max_workers=max(len(enabled), 1)) as executor:
        futures = {executor.submit(_search_source_safe, s, query): s for s in enabled}
        results = []
        try:
            for future in as_completed(futures, timeout=60):
                source = futures[future]
                try:
                    batch = future.result()
                    for r in batch:
                        r.setdefault("source", source.name)
                    results.extend(batch)
                except Exception as e:
                    logger.error(f"Audiobook search wrapper error ({source.name}): {e}")
        except TimeoutError:
            logger.warning("Audiobook search timed out — returning partial results")
            for future, source in futures.items():
                if future.done():
                    try:
                        batch = future.result()
                        for r in batch:
                            r.setdefault("source", source.name)
                        results.extend(batch)
                    except Exception:
                        pass

    results = filter_results(results, query)
    elapsed = int((time.time() - start) * 1000)
    return jsonify({
        "results": results,
        "search_time_ms": elapsed,
        "sources": _source_health_metadata(),
    })

@app.route("/api/download/audiobook", methods=["POST"])
def api_download_audiobook():
    if not config.has_qbittorrent():
        return jsonify({"success": False, "error": "qBittorrent not configured. Set QB_URL, QB_USER, QB_PASS."}), 400

    data = request.json
    if _truthy((data or {}).get("dry_run")):
        payload = dict(data or {})
        payload.setdefault("media_type", "audiobook")
        return jsonify(_download_preflight_response(payload, source_name=(data or {}).get("source", "audiobook"), source_type="torrent"))
    guid = data.get("guid", "")
    url = data.get("download_url") or (guid if guid.startswith("magnet:") else "") or data.get("magnet_url", "")
    if not url and data.get("info_hash"):
        url = f"magnet:?xt=urn:btih:{data['info_hash']}"

    abb_url = data.get("abb_url", "")
    if not url and abb_url:
        magnet = _resolve_abb_magnet(abb_url)
        if magnet:
            url = magnet
        else:
            return jsonify({"success": False, "error": "Failed to resolve AudioBookBay link"}), 400

    title = data.get("title", "Unknown")

    if not url:
        return jsonify({"success": False, "error": "No download URL"}), 400

    dup = _duplicate_summary(title=title, source_id=_extract_download_source_id(data))
    if dup["duplicate"] and not _truthy(data.get("force")):
        return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

    success = qb.add_torrent(
        url, title,
        save_path=config.QB_AUDIOBOOK_SAVE_PATH,
        category=config.QB_AUDIOBOOK_CATEGORY,
    )
    return jsonify({"success": success, "title": title})


@app.route("/api/download/novel", methods=["POST"])
def api_download_novel():
    data = request.json
    if _truthy((data or {}).get("dry_run")):
        payload = dict(data or {})
        payload.setdefault("media_type", "ebook")
        payload.setdefault("source", "webnovel")
        return jsonify(_download_preflight_response(payload, source_name="webnovel", source_type="direct"))
    url = data.get("url", "")
    title = data.get("title", "Unknown")

    if not url:
        return jsonify({"success": False, "error": "No URL"}), 400

    dup = _duplicate_summary(title=title, source_id=data.get("url", ""))
    if dup["duplicate"] and not _truthy(data.get("force")):
        return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

    _ensure_retry_scheduler()
    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = _base_job_fields(
        title,
        "webnovel",
        url=url,
        target_names=_parse_requested_targets(data),
        retry_kind="novel",
        retry_payload={"url": url, "title": title},
    )

    _start_job_thread(download_novel_worker, (job_id, url, title))

    return jsonify({"success": True, "job_id": job_id, "title": title})


@app.route("/api/download/annas", methods=["POST"])
def api_download_annas():
    data = request.json
    if _truthy((data or {}).get("dry_run")):
        payload = dict(data or {})
        payload.setdefault("media_type", "ebook")
        payload.setdefault("source", "annas")
        return jsonify(_download_preflight_response(payload, source_name="annas", source_type="direct"))
    md5 = data.get("md5", "")
    title = data.get("title", "Unknown")

    if not md5:
        return jsonify({"success": False, "error": "No MD5 hash"}), 400

    dup = _duplicate_summary(title=title, source_id=md5)
    if dup["duplicate"] and not _truthy(data.get("force")):
        return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

    _ensure_retry_scheduler()
    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = _base_job_fields(
        title,
        "annas",
        url=f"https://annas-archive.li/md5/{md5}",
        target_names=_parse_requested_targets(data),
        retry_kind="annas",
        retry_payload={"md5": md5, "title": title},
    )

    _start_job_thread(download_annas_worker, (job_id, md5, title))

    return jsonify({"success": True, "job_id": job_id, "title": title})


@app.route("/api/downloads")
def api_downloads():
    downloads = []

    # Torrent downloads
    if config.has_qbittorrent():
        for cat, source_label in [(config.QB_CATEGORY, "torrent"), (config.QB_AUDIOBOOK_CATEGORY, "audiobook")]:
            try:
                torrents = qb.get_torrents(category=cat)
                for t in torrents:
                    state = t.get("state", "")
                    status_map = {
                        "downloading": "downloading", "stalledDL": "downloading",
                        "metaDL": "downloading", "forcedDL": "downloading",
                        "pausedDL": "paused", "queuedDL": "queued",
                        "uploading": "completed", "stalledUP": "completed",
                        "pausedUP": "completed", "queuedUP": "completed",
                        "checkingDL": "checking", "checkingUP": "checking",
                    }
                    downloads.append({
                        "source": source_label,
                        "title": t.get("name", ""),
                        "progress": round(t.get("progress", 0) * 100, 1),
                        "status": status_map.get(state, state),
                        "size": _human_size(t.get("total_size", 0)),
                        "speed": _human_size(t.get("dlspeed", 0)) + "/s",
                        "hash": t.get("hash", ""),
                    })
            except Exception:
                pass

    # Web novel + Anna's Archive downloads
    for job_id, job in list(download_jobs.items()):
        downloads.append({
            "source": job.get("source", "webnovel"),
            "title": job["title"],
            "status": job["status"],
            "job_id": job_id,
            "error": job.get("error"),
            "detail": job.get("detail"),
            "retry_count": job.get("retry_count", 0),
            "max_retries": job.get("max_retries", JOB_MAX_RETRIES),
            "next_retry_at": job.get("next_retry_at"),
        })

    return jsonify({"downloads": downloads})


@app.route("/api/downloads/torrent/<torrent_hash>", methods=["DELETE"])
def api_delete_torrent(torrent_hash):
    success = qb.delete_torrent(torrent_hash, delete_files=True)
    return jsonify({"success": success})


@app.route("/api/downloads/novel/<job_id>", methods=["DELETE"])
def api_delete_novel(job_id):
    if job_id in download_jobs:
        del download_jobs[job_id]
        return jsonify({"success": True})
    return jsonify({"success": False, "error": "Job not found"}), 404


@app.route("/api/downloads/jobs/<job_id>/retry", methods=["POST"])
def api_retry_job(job_id):
    if job_id not in download_jobs:
        return jsonify({"success": False, "error": "Job not found"}), 404
    job = download_jobs[job_id]
    if job.get("status") not in ("error", "dead_letter", "retry_wait"):
        return jsonify({"success": False, "error": f"Job status {job.get('status')} not retryable"}), 400
    _ensure_retry_scheduler()
    if not _dispatch_retry(job_id):
        return jsonify({"success": False, "error": "No retry handler for job"}), 400
    return jsonify({"success": True, "job_id": job_id, "status": download_jobs[job_id].get("status")})


@app.route("/api/downloads/clear", methods=["POST"])
def api_clear_finished():
    to_remove = [jid for jid, j in download_jobs.items()
                 if j["status"] in ("completed", "error", "dead_letter")]
    for jid in to_remove:
        del download_jobs[jid]
    removed_torrents = 0
    if config.has_qbittorrent():
        for cat in (config.QB_CATEGORY, config.QB_AUDIOBOOK_CATEGORY):
            try:
                torrents = qb.get_torrents(category=cat)
                for t in torrents:
                    state = t.get("state", "")
                    if state in ("uploading", "stalledUP", "pausedUP", "queuedUP", "error", "missingFiles"):
                        qb.delete_torrent(t["hash"], delete_files=False)
                        removed_torrents += 1
            except Exception:
                pass
    return jsonify({"success": True, "cleared_novels": len(to_remove), "cleared_torrents": removed_torrents})


@app.route("/api/library")
def api_library():
    if not config.has_audiobookshelf() or not config.ABS_EBOOK_LIBRARY_ID:
        return jsonify({"books": [], "total": 0, "page": 1, "pages": 1})
    page = int(request.args.get("page", 1))
    per_page = 24
    search = request.args.get("q", "").strip()
    try:
        params = {"limit": per_page, "page": page - 1, "sort": "addedAt", "desc": 1}
        if search:
            params["filter"] = f"search={search}"
        resp = requests.get(
            f"{config.ABS_URL}/api/libraries/{config.ABS_EBOOK_LIBRARY_ID}/items",
            params=params,
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        data = resp.json()
        total = data.get("total", 0)
        books = []
        for item in data.get("results", []):
            media = item.get("media", {})
            meta = media.get("metadata", {})
            books.append({
                "id": item["id"],
                "title": meta.get("title", "Unknown"),
                "authors": meta.get("authorName", "Unknown"),
                "has_cover": bool(media.get("coverPath")),
                "cover_url": f"/api/cover/{item['id']}",
                "added": item.get("addedAt", ""),
            })
        return jsonify({
            "books": books,
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        })
    except Exception as e:
        logger.error(f"Library error: {e}")
        return jsonify({"books": [], "total": 0, "page": 1, "pages": 1})


@app.route("/api/cover/<item_id>")
def api_cover(item_id):
    if not config.has_audiobookshelf():
        return "", 404
    try:
        resp = requests.get(
            f"{config.ABS_URL}/api/items/{item_id}/cover",
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        if resp.status_code == 200:
            from io import BytesIO
            return send_file(
                BytesIO(resp.content),
                mimetype=resp.headers.get("Content-Type", "image/jpeg"),
            )
    except Exception:
        pass
    return "", 404


@app.route("/api/library/book/<item_id>", methods=["DELETE"])
def api_delete_book(item_id):
    if not config.has_audiobookshelf():
        return jsonify({"success": False, "error": "Audiobookshelf not configured"}), 500
    try:
        resp = requests.delete(
            f"{config.ABS_URL}/api/items/{item_id}?hard=1",
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        success = resp.status_code == 200
        if success:
            logger.info(f"Deleted ebook {item_id} from Audiobookshelf")
        return jsonify({"success": success})
    except Exception as e:
        logger.error(f"Delete ebook error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/library/audiobook/<item_id>", methods=["DELETE"])
def api_delete_audiobook(item_id):
    if not config.has_audiobookshelf():
        return jsonify({"success": False, "error": "Audiobookshelf not configured"}), 500
    try:
        resp = requests.delete(
            f"{config.ABS_URL}/api/items/{item_id}?hard=1",
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        success = resp.status_code == 200
        if success:
            logger.info(f"Deleted audiobook {item_id} from Audiobookshelf")
        return jsonify({"success": success})
    except Exception as e:
        logger.error(f"Delete audiobook error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/external-urls")
def api_external_urls():
    return jsonify({
        "abs_url": config.ABS_PUBLIC_URL,
    })


@app.route("/api/library/audiobooks")
def api_library_audiobooks():
    if not config.has_audiobookshelf() or not config.ABS_LIBRARY_ID:
        return jsonify({"audiobooks": [], "total": 0})
    page = int(request.args.get("page", 1))
    per_page = 24
    search = request.args.get("q", "").strip()
    try:
        params = {"limit": per_page, "page": page - 1, "sort": "addedAt", "desc": 1}
        if search:
            params["filter"] = f"search={search}"
        resp = requests.get(
            f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/items",
            params=params,
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        data = resp.json()
        total = data.get("total", 0)
        audiobooks = []
        for item in data.get("results", []):
            media = item.get("media", {})
            meta = media.get("metadata", {})
            duration = media.get("duration", 0)
            hours = int(duration // 3600)
            mins = int((duration % 3600) // 60)
            audiobooks.append({
                "id": item["id"],
                "title": meta.get("title", "Unknown"),
                "authors": meta.get("authorName", "Unknown"),
                "narrator": meta.get("narratorName", ""),
                "duration": f"{hours}h {mins}m" if duration else "",
                "num_chapters": media.get("numChapters", 0),
                "cover_url": f"/api/audiobook/cover/{item['id']}",
                "has_cover": bool(media.get("coverPath")),
            })
        return jsonify({
            "audiobooks": audiobooks,
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        })
    except Exception as e:
        logger.error(f"Audiobook library error: {e}")
        return jsonify({"audiobooks": [], "total": 0, "page": 1, "pages": 1})


@app.route("/api/audiobook/cover/<item_id>")
def api_audiobook_cover(item_id):
    if not config.has_audiobookshelf():
        return "", 404
    try:
        resp = requests.get(
            f"{config.ABS_URL}/api/items/{item_id}/cover",
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        if resp.status_code == 200:
            from io import BytesIO
            return send_file(
                BytesIO(resp.content),
                mimetype=resp.headers.get("Content-Type", "image/jpeg"),
            )
    except Exception:
        pass
    return "", 404


# =============================================================================
# Plugin Source API
# =============================================================================
@app.route("/api/sources")
def api_sources():
    """Return metadata for all loaded sources (for UI rendering and settings)."""
    return jsonify(_source_health_metadata())


@app.route("/api/download", methods=["POST"])
def api_download():
    """Unified download endpoint — dispatches to the right source/method."""
    data = request.json
    source_name = data.get("source", "")
    source = sources.get_source(source_name)

    if not source:
        return jsonify({"success": False, "error": f"Unknown source: {source_name}"}), 400

    if not source.enabled():
        return jsonify({"success": False, "error": f"Source '{source.label}' is not configured"}), 400

    title = data.get("title", "Unknown")
    requested_targets = _parse_requested_targets(data)
    source_id = _extract_download_source_id(data)
    media_type_guess = "audiobook" if getattr(source, "search_tab", "main") == "audiobook" else "ebook"

    if _truthy(data.get("dry_run")):
        dry_payload = dict(data)
        dry_payload.setdefault("media_type", media_type_guess)
        return jsonify(_download_preflight_response(dry_payload, source_name=source_name, source_type=source.download_type))

    # Torrent sources: send to qBittorrent
    if source.download_type == "torrent":
        if not config.has_qbittorrent():
            return jsonify({"success": False, "error": "qBittorrent not configured"}), 400

        guid = data.get("guid", "")
        url = data.get("download_url") or (guid if guid.startswith("magnet:") else "") or data.get("magnet_url", "")
        if not url and data.get("info_hash"):
            url = f"magnet:?xt=urn:btih:{data['info_hash']}"
        if not url and data.get("abb_url"):
            magnet = _resolve_abb_magnet(data["abb_url"])
            if magnet:
                url = magnet
            else:
                return jsonify({"success": False, "error": "Failed to resolve download link"}), 400
        if not url:
            return jsonify({"success": False, "error": "No download URL"}), 400

        dup = _duplicate_summary(title=title, source_id=source_id)
        if dup["duplicate"] and not _truthy(data.get("force")):
            return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

        if source.search_tab == "audiobook":
            save_path = config.QB_AUDIOBOOK_SAVE_PATH
            category = config.QB_AUDIOBOOK_CATEGORY
        else:
            save_path = config.QB_SAVE_PATH
            category = config.QB_CATEGORY

        success = qb.add_torrent(url, title, save_path=save_path, category=category)
        return jsonify({"success": success, "title": title})

    # Direct/custom sources: create a job and run in background thread
    dup = _duplicate_summary(title=title, source_id=source_id)
    if dup["duplicate"] and not _truthy(data.get("force")):
        return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

    _ensure_retry_scheduler()
    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = _base_job_fields(
        title,
        source_name,
        target_names=requested_targets,
        retry_kind="source",
        retry_payload={"source_name": source_name, "data": data},
    )
    _start_job_thread(_run_source_download_worker, (job_id, source_name, data))
    return jsonify({"success": True, "job_id": job_id, "title": title})


# =============================================================================
# Activity & Library Tracking API
# =============================================================================
@app.route("/api/activity")
def api_activity():
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    events = library.get_activity(limit=limit, offset=offset)
    total = library.count_activity()
    return jsonify({"events": events, "total": total})


@app.route("/api/library/tracked")
def api_library_tracked():
    media_type = request.args.get("type", None)
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    items = library.get_items(media_type=media_type, limit=limit, offset=offset)
    total = library.count_items(media_type=media_type)
    return jsonify({"items": items, "total": total})


@app.route("/api/check-duplicate")
def api_check_duplicate():
    source_id = request.args.get("source_id", "")
    if not source_id:
        return jsonify({"duplicate": False})
    return jsonify({"duplicate": library.has_source_id(source_id)})


# =============================================================================
# Settings API
# =============================================================================
@app.route("/api/settings")
def api_get_settings():
    return jsonify(config.get_all_settings())


@app.route("/api/settings", methods=["POST"])
def api_save_settings():
    data = request.json
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    try:
        data = dict(data)
        if "target_routing_rules" in data:
            raw_rules = (data.get("target_routing_rules") or "").strip() or "{}"
            try:
                parsed = json.loads(raw_rules)
                if not isinstance(parsed, dict):
                    return jsonify({"success": False, "error": "target_routing_rules must be a JSON object"}), 400
                data["target_routing_rules"] = json.dumps(parsed)
            except Exception as e:
                return jsonify({"success": False, "error": f"Invalid target_routing_rules JSON: {e}"}), 400
        masked = getattr(config, "MASKED_SECRET", "••••••••")
        for key in ("prowlarr_api_key", "qb_pass", "abs_token", "kavita_api_key", "api_key"):
            if data.get(key) == masked:
                del data[key]
        # Hash auth password if it's being changed (not the masked placeholder)
        if "auth_password" in data:
            pw = data["auth_password"]
            if pw and pw != masked:
                data["auth_password"] = config.hash_password(pw)
            else:
                # Don't overwrite existing hash with placeholder
                del data["auth_password"]
        config.save_settings(data)
        # Update secret key if it changed
        app.secret_key = config.SECRET_KEY
        # Re-authenticate qBittorrent if credentials changed
        qb.authenticated = False
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


def _path_check(name, path, create=False):
    check = {"name": name, "path": path or "", "exists": False, "writable": False, "ok": False}
    if not path:
        check["error"] = "not configured"
        return check
    if os.path.exists(path):
        check["exists"] = True
        check["writable"] = os.access(path, os.W_OK)
        check["ok"] = check["writable"]
        if not check["ok"]:
            check["error"] = "path not writable"
        return check
    if create:
        try:
            os.makedirs(path, exist_ok=True)
            check["exists"] = True
            check["writable"] = os.access(path, os.W_OK)
            check["ok"] = check["writable"]
            if not check["ok"]:
                check["error"] = "created but not writable"
            return check
        except Exception as e:
            check["error"] = str(e)
            return check
    check["error"] = "path does not exist"
    return check


def _test_prowlarr_connection(url, api_key):
    if not url or not api_key:
        return {"success": False, "error": "URL and API key required", "error_class": "missing_config"}
    try:
        resp = requests.get(f"{url.rstrip('/')}/api/v1/indexer", headers={"X-Api-Key": api_key}, timeout=10)
        if resp.status_code == 200:
            indexers = resp.json()
            return {"success": True, "message": f"Connected ({len(indexers)} indexers)", "indexer_count": len(indexers)}
        if resp.status_code == 401:
            return {"success": False, "error": "Invalid API key", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {resp.status_code}", "error_class": f"http_{resp.status_code}"}
    except requests.Timeout:
        return {"success": False, "error": "Timed out connecting to Prowlarr", "error_class": "timeout"}
    except requests.ConnectionError:
        return {"success": False, "error": "Connection refused — is Prowlarr running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def _test_qbittorrent_connection(url, user, password):
    if not url:
        return {"success": False, "error": "URL required", "error_class": "missing_config"}
    try:
        session = requests.Session()
        resp = session.post(f"{url.rstrip('/')}/api/v2/auth/login", data={"username": user, "password": password}, timeout=10)
        if "banned" in resp.text.lower():
            return {"success": False, "error": "IP banned by qBittorrent", "error_class": "ip_banned"}
        if resp.text != "Ok.":
            return {"success": False, "error": "Login failed — check username/password", "error_class": "auth_failed"}
        ver_resp = session.get(f"{url.rstrip('/')}/api/v2/app/version", timeout=5)
        if ver_resp.status_code == 200:
            return {"success": True, "message": f"Connected (v{ver_resp.text})", "version": ver_resp.text}
        if ver_resp.status_code == 403:
            return {"success": False, "error": "Session rejected by qBittorrent", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {ver_resp.status_code}", "error_class": f"http_{ver_resp.status_code}"}
    except requests.Timeout:
        return {"success": False, "error": "Timed out connecting to qBittorrent", "error_class": "timeout"}
    except requests.ConnectionError:
        return {"success": False, "error": "Connection refused — is qBittorrent running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def _test_audiobookshelf_connection(url, token):
    if not url or not token:
        return {"success": False, "error": "URL and API token required", "error_class": "missing_config"}
    try:
        resp = requests.get(f"{url.rstrip('/')}/api/libraries", headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if resp.status_code == 200:
            libraries = resp.json().get("libraries", [])
            return {
                "success": True,
                "message": f"Connected ({len(libraries)} libraries)",
                "libraries": [{"id": l["id"], "name": l["name"]} for l in libraries],
            }
        if resp.status_code == 401:
            return {"success": False, "error": "Invalid API token", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {resp.status_code}", "error_class": f"http_{resp.status_code}"}
    except requests.Timeout:
        return {"success": False, "error": "Timed out connecting to Audiobookshelf", "error_class": "timeout"}
    except requests.ConnectionError:
        return {"success": False, "error": "Connection refused — is Audiobookshelf running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def _test_kavita_connection(url, api_key):
    if not url or not api_key:
        return {"success": False, "error": "URL and API key required", "error_class": "missing_config"}
    try:
        resp = requests.post(
            f"{url.rstrip('/')}/api/Plugin/authenticate",
            params={"apiKey": api_key, "pluginName": "Librarr"},
            timeout=10,
        )
        if resp.status_code == 200:
            token = resp.json().get("token", "")
            if not token:
                return {"success": False, "error": "Auth returned no token", "error_class": "protocol_error"}
            lib_resp = requests.get(f"{url.rstrip('/')}/api/Library", headers={"Authorization": f"Bearer {token}"}, timeout=10)
            libraries = [{"id": l["id"], "name": l["name"]} for l in lib_resp.json()] if lib_resp.status_code == 200 else []
            return {"success": True, "message": f"Connected ({len(libraries)} libraries)", "libraries": libraries}
        if resp.status_code == 401:
            return {"success": False, "error": "Invalid API key", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {resp.status_code}", "error_class": f"http_{resp.status_code}"}
    except requests.Timeout:
        return {"success": False, "error": "Timed out connecting to Kavita", "error_class": "timeout"}
    except requests.ConnectionError:
        return {"success": False, "error": "Connection refused — is Kavita running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def _runtime_config_validation(run_network_tests=False):
    rules_raw = config.TARGET_ROUTING_RULES or "{}"
    routing_rule_error = None
    try:
        parsed_rules = json.loads(rules_raw)
        if not isinstance(parsed_rules, dict):
            routing_rule_error = "target_routing_rules must be a JSON object"
    except Exception as e:
        parsed_rules = {}
        routing_rule_error = f"Invalid target_routing_rules JSON: {e}"
    checks = {
        "paths": [],
        "services": {},
        "qb": qb.diagnose() if run_network_tests else None,
        "routing_rules": {"valid": routing_rule_error is None, "error": routing_rule_error, "rules": parsed_rules if routing_rule_error is None else None},
    }
    checks["paths"].append(_path_check("incoming_dir", config.INCOMING_DIR, create=True))
    if config.FILE_ORG_ENABLED:
        checks["paths"].append(_path_check("ebook_organized_dir", config.EBOOK_ORGANIZED_DIR, create=True))
        checks["paths"].append(_path_check("audiobook_organized_dir", config.AUDIOBOOK_ORGANIZED_DIR, create=True))
    else:
        checks["paths"].append({"name": "file_org", "ok": True, "info": "disabled"})
    if config.KAVITA_LIBRARY_PATH:
        checks["paths"].append(_path_check("kavita_library_path", config.KAVITA_LIBRARY_PATH, create=True))

    checks["services"]["prowlarr"] = (
        _test_prowlarr_connection(config.PROWLARR_URL, config.PROWLARR_API_KEY) if config.has_prowlarr()
        else {"success": None, "info": "not configured"}
    ) if run_network_tests else {"success": None, "info": "skipped"}
    checks["services"]["audiobookshelf"] = (
        _test_audiobookshelf_connection(config.ABS_URL, config.ABS_TOKEN) if config.has_audiobookshelf()
        else {"success": None, "info": "not configured"}
    ) if run_network_tests else {"success": None, "info": "skipped"}
    checks["services"]["kavita"] = (
        _test_kavita_connection(config.KAVITA_URL, config.KAVITA_API_KEY) if config.has_kavita()
        else {"success": None, "info": "not configured"}
    ) if run_network_tests else {"success": None, "info": "skipped"}

    path_errors = [p for p in checks["paths"] if p.get("ok") is False]
    svc_failures = [v for v in checks["services"].values() if v.get("success") is False]
    qb_fail = checks["qb"] and checks["qb"].get("success") is False
    checks["success"] = not path_errors and not svc_failures and not qb_fail and checks["routing_rules"]["valid"]
    return checks


@app.route("/api/validate/config")
def api_validate_config():
    include_network = request.args.get("network", "0").lower() in ("1", "true", "yes")
    return jsonify(_runtime_config_validation(run_network_tests=include_network))


@app.route("/api/test/all", methods=["POST"])
def api_test_all():
    return jsonify(_runtime_config_validation(run_network_tests=True))


@app.route("/api/test/prowlarr", methods=["POST"])
def api_test_prowlarr():
    data = request.json
    url = data.get("url", "").rstrip("/")
    api_key = data.get("api_key", "")
    return jsonify(_test_prowlarr_connection(url, api_key))


@app.route("/api/test/qbittorrent", methods=["POST"])
def api_test_qbittorrent():
    data = request.json
    url = data.get("url", "").rstrip("/")
    user = data.get("user", "admin")
    password = data.get("pass", "")
    return jsonify(_test_qbittorrent_connection(url, user, password))


@app.route("/api/test/audiobookshelf", methods=["POST"])
def api_test_audiobookshelf():
    data = request.json
    url = data.get("url", "").rstrip("/")
    token = data.get("token", "")
    return jsonify(_test_audiobookshelf_connection(url, token))


@app.route("/api/test/kavita", methods=["POST"])
def api_test_kavita():
    data = request.json
    url = data.get("url", "").rstrip("/")
    api_key = data.get("api_key", "")
    return jsonify(_test_kavita_connection(url, api_key))





# =============================================================================
# Goodreads / StoryGraph CSV Import
# =============================================================================
# Users export their "to-read" shelf from Goodreads (My Books → Export) or
# StoryGraph (Settings → Export Data). Librarr parses the CSV, extracts
# book titles + authors, and queues a search+download for each.

@app.route("/api/import/csv", methods=["POST"])
def api_import_csv():
    """Parse a Goodreads or StoryGraph CSV export and queue downloads.

    Accepts multipart/form-data with file field "csv_file".
    Optional fields:
      - shelf: filter to this shelf name (default: "to-read")
      - media_type: "ebook" or "audiobook" (default: "ebook")
      - limit: max items to queue (default: 50, max: 200)
    """
    import csv, io
    f = request.files.get("csv_file")
    if not f:
        return jsonify({"error": "csv_file is required"}), 400

    shelf_filter = request.form.get("shelf", "to-read").lower()
    media_type = request.form.get("media_type", "ebook")
    limit = min(int(request.form.get("limit", 50)), 200)

    try:
        text = f.read().decode("utf-8-sig", errors="replace")
    except Exception as e:
        return jsonify({"error": f"Could not read file: {e}"}), 400

    reader = csv.DictReader(io.StringIO(text))
    headers = [h.lower().strip() for h in (reader.fieldnames or [])]

    # Detect format: Goodreads vs StoryGraph
    # Goodreads: "Title", "Author", "Exclusive Shelf"
    # StoryGraph: "Title", "Authors", "Read Status"
    is_goodreads = "exclusive shelf" in headers
    is_storygraph = "read status" in headers

    queued = []
    skipped = 0

    for row in reader:
        if len(queued) >= limit:
            break

        # Normalize field names
        def _get(*keys):
            for k in keys:
                for h, v in row.items():
                    if h.lower().strip() == k:
                        return (v or "").strip()
            return ""

        title = _get("title")
        author = _get("author", "authors")
        if not title:
            skipped += 1
            continue

        # Shelf / status filter
        if is_goodreads:
            shelf = _get("exclusive shelf").lower()
            if shelf_filter and shelf != shelf_filter:
                skipped += 1
                continue
        elif is_storygraph:
            read_status = _get("read status").lower()
            # StoryGraph statuses: "to-read", "currently-reading", "read"
            sg_map = {"to-read": "to-read", "currently-reading": "reading", "read": "read"}
            mapped = sg_map.get(read_status, read_status)
            if shelf_filter and mapped != shelf_filter and read_status != shelf_filter:
                skipped += 1
                continue

        # Skip if already in library
        search_title = f"{title} {author}".strip()
        if library.find_by_title(title):
            skipped += 1
            continue

        # Queue a search job
        job_id = str(uuid.uuid4())[:8]
        download_jobs[job_id] = _base_job_fields(
            title,
            "csv_import",
            type="search_import",
            author=author,
            query=search_title,
            media_type=media_type,
        )
        queued.append({"job_id": job_id, "title": title, "author": author})

    # Kick off a background worker to process the queued search-import jobs
    if queued:
        threading.Thread(target=_process_csv_import_jobs, daemon=True).start()

    return jsonify({
        "queued": len(queued),
        "skipped": skipped,
        "items": queued,
        "format": "goodreads" if is_goodreads else "storygraph" if is_storygraph else "unknown",
    })


def _process_csv_import_jobs():
    """Background worker: find and download queued search-import jobs."""
    import time
    sources.load_sources()
    for job_id, job in list(download_jobs.items()):
        if job.get("type") != "search_import" or job.get("status") != "queued":
            continue
        query = job.get("query", "")
        media_type = job.get("media_type", "ebook")
        try:
            download_jobs[job_id]["status"] = "searching"
            results = []
            for src in sources.get_enabled_sources("main"):
                try:
                    results.extend(_search_source_safe(src, query))
                except Exception:
                    pass
            if results:
                best = results[0]
                url = best.get("download_url") or best.get("magnet")
                if url:
                    download_jobs[job_id]["url"] = url
                    download_jobs[job_id]["status"] = "queued"
                    download_jobs[job_id]["type"] = "torrent" if (url or "").startswith("magnet:") else "direct"
                    logger.info(f"CSV import: queued '{query}'")
                else:
                    download_jobs[job_id]["status"] = "error"
                    download_jobs[job_id]["error"] = "No downloadable result found"
            else:
                download_jobs[job_id]["status"] = "error"
                download_jobs[job_id]["error"] = "Not found in any source"
        except Exception as e:
            download_jobs[job_id]["status"] = "error"
            download_jobs[job_id]["error"] = str(e)
        time.sleep(1)


# =============================================================================
# AI Monitor helpers
# =============================================================================

def _rotate_abb_domain():
    """Rotate ABB_DOMAINS list so the next domain becomes primary."""
    global ABB_DOMAINS, ABB_URL
    if len(ABB_DOMAINS) > 1:
        ABB_DOMAINS = ABB_DOMAINS[1:] + [ABB_DOMAINS[0]]
        ABB_URL = ABB_DOMAINS[0]
    return ABB_URL


def _do_abs_scan():
    """Trigger Audiobookshelf library rescan for all configured libraries."""
    if not config.has_audiobookshelf():
        return
    for lib_id in filter(None, [config.ABS_LIBRARY_ID, config.ABS_EBOOK_LIBRARY_ID]):
        try:
            requests.post(
                f"{config.ABS_URL}/api/libraries/{lib_id}/scan",
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
        except Exception as e:
            logger.error(f"ABS scan failed ({lib_id}): {e}")


# =============================================================================
# AI Monitor API
# =============================================================================

@app.route("/api/monitor/status")
def api_monitor_status():
    if _monitor is None:
        return jsonify({"enabled": False, "error": "Monitor not initialized"})
    return jsonify(_monitor.get_status())


@app.route("/api/monitor/analyze", methods=["POST"])
def api_monitor_analyze():
    if _monitor is None:
        return jsonify({"success": False, "error": "Monitor not initialized"})
    _monitor.trigger_manual()
    # Give the cycle a moment to complete if running synchronously (disabled mode)
    import time as _time; _time.sleep(0.1)
    return jsonify({"success": True, **_monitor.get_status()})


@app.route("/api/monitor/actions/<action_id>/approve", methods=["POST"])
def api_monitor_approve(action_id):
    if _monitor is None:
        return jsonify({"success": False, "error": "Monitor not initialized"})
    success, message = _monitor.execute_approved(action_id)
    return jsonify({"success": success, "message": message})


@app.route("/api/monitor/actions/<action_id>/dismiss", methods=["POST"])
def api_monitor_dismiss(action_id):
    if _monitor is None:
        return jsonify({"success": False, "error": "Monitor not initialized"})
    dismissed = _monitor.action_queue.dismiss(action_id)
    return jsonify({"success": dismissed})


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
    # Validate config paths and prune old activity log entries
    _validate_config()
    library.cleanup_activity(days=90)
    # Load source plugins
    sources.load_sources()
    enabled_sources = sources.get_enabled_sources("main") + sources.get_enabled_sources("audiobook")
    source_names = ", ".join(s.label for s in enabled_sources) or "none"
    logger.info(f"Librarr starting — {len(enabled_sources)} sources enabled: {source_names}")

    # Log enabled integrations
    integrations = []
    if config.has_qbittorrent():
        integrations.append("qBittorrent")
    if config.has_calibre():
        integrations.append("Calibre-Web")
    if config.has_audiobookshelf():
        integrations.append("Audiobookshelf")
    if config.has_lncrawl():
        integrations.append("lightnovel-crawler")
    if config.has_kavita():
        integrations.append("Kavita")

    if integrations:
        logger.info(f"Integrations: {', '.join(integrations)}")

    # Start auto-import background thread if qBittorrent is configured
    if config.has_qbittorrent():
        import_thread = threading.Thread(target=auto_import_loop, daemon=True)
        import_thread.start()

    _ensure_retry_scheduler()

    # Register OPDS catalog server
    opds.init_app(app, library)

    # Initialize and start AI monitor
    _monitor = monitor.LibrarrMonitor(
        get_jobs=lambda: list(download_jobs.items()),
        qb_reauth=lambda: qb.login(),
        get_abb_domains=lambda: list(ABB_DOMAINS),
        rotate_abb_domain=_rotate_abb_domain,
        trigger_abs_scan=_do_abs_scan,
        docker_socket=config.DOCKER_SOCKET,
    )
    _monitor.start()

    app.run(host="0.0.0.0", port=5000, debug=False)
