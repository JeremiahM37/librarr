from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque

from flask import jsonify, request


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except Exception:
        return default


class InMemoryRateLimiter:
    def __init__(self, *, window_sec: int, rules: dict[str, int]):
        self.window_sec = max(1, int(window_sec))
        self.rules = {k: max(1, int(v)) for k, v in rules.items()}
        self._lock = threading.Lock()
        self._buckets: dict[tuple[str, str], deque[float]] = defaultdict(deque)

    def _prune(self, bucket: deque[float], now: float) -> None:
        cutoff = now - self.window_sec
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

    def _rule_for_path(self, path: str) -> str:
        if path == "/login":
            return "login"
        if path.startswith("/api/search"):
            return "search"
        if path.startswith("/api/download"):
            return "download"
        if path.startswith("/api/"):
            return "api"
        return "default"

    def check(self, *, identity: str, path: str) -> dict:
        now = time.time()
        rule = self._rule_for_path(path)
        limit = self.rules.get(rule, self.rules.get("default", 600))
        key = (rule, identity)
        with self._lock:
            bucket = self._buckets[key]
            self._prune(bucket, now)
            allowed = len(bucket) < limit
            retry_after = 0
            if allowed:
                bucket.append(now)
            elif bucket:
                retry_after = max(1, int(self.window_sec - (now - bucket[0])))
            remaining = max(0, limit - len(bucket))
        return {
            "allowed": allowed,
            "rule": rule,
            "limit": limit,
            "remaining": remaining,
            "retry_after": retry_after,
            "window_sec": self.window_sec,
        }


def register_rate_limiter(app):
    enabled = os.getenv("LIBRARR_RATE_LIMIT_ENABLED", "true").lower() in ("1", "true", "yes")
    limiter = None
    if enabled:
        limiter = InMemoryRateLimiter(
            window_sec=_env_int("LIBRARR_RATE_LIMIT_WINDOW_SEC", 60),
            rules={
                "default": _env_int("LIBRARR_RATE_LIMIT_DEFAULT", 600),
                "api": _env_int("LIBRARR_RATE_LIMIT_API", 300),
                "search": _env_int("LIBRARR_RATE_LIMIT_SEARCH", 120),
                "download": _env_int("LIBRARR_RATE_LIMIT_DOWNLOAD", 60),
                "login": _env_int("LIBRARR_RATE_LIMIT_LOGIN", 20),
            },
        )

        @app.before_request
        def _enforce_rate_limit():
            if app.config.get("TESTING"):
                return None
            # Do not rate limit internal/static health checks and assets.
            if request.path in ("/api/health", "/readyz", "/metrics") or request.path.startswith("/static/"):
                return None
            identity = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
            result = limiter.check(identity=identity, path=request.path)
            if result["allowed"]:
                return None
            headers = {"Retry-After": str(result["retry_after"])}
            payload = {
                "error": "Rate limit exceeded",
                "rule": result["rule"],
                "limit": result["limit"],
                "retry_after": result["retry_after"],
                "window_sec": result["window_sec"],
            }
            if request.path.startswith("/api/"):
                return jsonify(payload), 429, headers
            return ("Rate limit exceeded", 429, headers)

    return limiter
