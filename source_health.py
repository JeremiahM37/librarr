"""Source health tracking and circuit breaker state."""
from __future__ import annotations

import threading
import time


class SourceHealthTracker:
    """Tracks source reliability and temporarily opens a circuit on repeated failures."""

    def __init__(self, telemetry_module, threshold=3, open_seconds=300):
        self.telemetry = telemetry_module
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
            self.telemetry.emit_event("source_recovered", {"source": name, "kind": kind})

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
            self.telemetry.emit_event("source_degraded", {
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
