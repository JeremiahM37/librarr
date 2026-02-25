from __future__ import annotations

import json
import os
import sqlite3
import threading
import time


class DownloadStore:
    """Dict-like download job tracker backed by SQLite for persistence."""

    def __init__(
        self,
        db_path,
        *,
        apply_migrations,
        logger,
        telemetry,
        job_max_retries,
        transition_allowed,
        record_transition,
    ):
        self._db_path = db_path
        self._apply_migrations = apply_migrations
        self._logger = logger
        self._telemetry = telemetry
        self._job_max_retries = job_max_retries
        self._transition_allowed = transition_allowed
        self._record_transition = record_transition
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
            self._apply_migrations(conn)

    def _load_all(self):
        with self._connect() as conn:
            rows = conn.execute("SELECT job_id, data FROM download_jobs").fetchall()
        stale = 0
        for job_id, data_str in rows:
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue
            if data.get("status") in ("queued", "searching", "downloading", "importing"):
                data["status"] = "error"
                data["error"] = "Interrupted by restart"
                data["last_error_at"] = time.time()
                stale += 1
            data.setdefault("retry_count", 0)
            data.setdefault("max_retries", self._job_max_retries)
            data.setdefault("next_retry_at", None)
            data.setdefault("failure_history", [])
            data.setdefault("status_history", [])
            self._cache[job_id] = _PersistentJob(self, job_id, data)
        if self._cache:
            msg = f"Restored {len(self._cache)} download jobs from database"
            if stale:
                msg += f" ({stale} marked as failed due to restart)"
            self._logger.info(msg)

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
        data.setdefault("max_retries", self._job_max_retries)
        data.setdefault("next_retry_at", None)
        data.setdefault("failure_history", [])
        data.setdefault("status_history", [])
        job = _PersistentJob(self, job_id, data)
        self._cache[job_id] = job
        self._persist(job_id, job._data)
        if data.get("status"):
            self._record_transition(job_id, None, data["status"], data)

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
            if old_status != new_status and not self._store._transition_allowed(old_status, new_status):
                self._store._telemetry.metrics.inc(
                    "librarr_job_invalid_transitions_total",
                    from_status=old_status or "none",
                    to_status=new_status,
                    source=self._data.get("source", "unknown"),
                )
                self._store._logger.warning(
                    "Rejected invalid job status transition %s -> %s for %s",
                    old_status,
                    new_status,
                    self._job_id,
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
                self._store._record_transition(self._job_id, old_status, value, self._data)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __repr__(self):
        return repr(self._data)
