"""Lightweight runtime telemetry for Librarr (webhooks + Prometheus counters)."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import socket
import threading
import time
from collections import defaultdict
from typing import Dict, Iterable, Tuple

import requests

logger = logging.getLogger("librarr")


class Metrics:
    """In-memory counter registry with Prometheus text rendering."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = defaultdict(float)

    def inc(self, name: str, amount: float = 1.0, **labels):
        key = (name, tuple(sorted((k, str(v)) for k, v in labels.items())))
        with self._lock:
            self._counters[key] += amount

    def snapshot(self):
        with self._lock:
            return dict(self._counters)

    def render(self, dynamic_lines: Iterable[str] | None = None) -> str:
        dynamic_lines = list(dynamic_lines or [])
        help_map = {
            "librarr_job_transitions_total": "Count of job status transitions.",
            "librarr_job_terminal_total": "Count of job terminal outcomes.",
            "librarr_job_retry_scheduled_total": "Count of scheduled job retries.",
            "librarr_job_invalid_transitions_total": "Count of rejected invalid job status transitions.",
            "librarr_import_verifications_total": "Count of target import verifications.",
            "librarr_webhooks_total": "Count of webhook delivery attempts/results.",
            "librarr_webhook_events_total": "Count of webhook events emitted.",
        }
        type_map = {name: "counter" for name in help_map}
        lines = []
        seen_names = set()
        for (name, _), _value in sorted(self.snapshot().items()):
            if name not in seen_names:
                lines.append(f"# HELP {name} {help_map.get(name, name)}")
                lines.append(f"# TYPE {name} {type_map.get(name, 'counter')}")
                seen_names.add(name)
        for (name, labels), value in sorted(self.snapshot().items()):
            if labels:
                label_str = ",".join(f'{k}="{_escape(v)}"' for k, v in labels)
                lines.append(f"{name}{{{label_str}}} {value}")
            else:
                lines.append(f"{name} {value}")
        lines.extend(dynamic_lines)
        return "\n".join(lines) + "\n"


def _escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


metrics = Metrics()


def _webhook_urls():
    raw = os.getenv("LIBRARR_WEBHOOK_URLS", "").strip()
    if not raw:
        return []
    # support comma-separated and newline-separated values
    urls = []
    for part in raw.replace("\n", ",").split(","):
        url = part.strip()
        if url:
            urls.append(url)
    return urls


def emit_event(event_type: str, payload=None):
    """Emit a webhook event asynchronously (best effort)."""
    payload = dict(payload or {})
    payload.setdefault("ts", time.time())
    payload.setdefault("host", socket.gethostname())
    payload["event"] = event_type
    metrics.inc("librarr_webhook_events_total", event=event_type)

    urls = _webhook_urls()
    if not urls:
        metrics.inc("librarr_webhooks_total", result="skipped", event=event_type)
        return

    t = threading.Thread(target=_post_event, args=(event_type, payload, urls), daemon=True)
    t.start()


def _post_event(event_type: str, payload: dict, urls):
    timeout = float(os.getenv("LIBRARR_WEBHOOK_TIMEOUT_SEC", "5"))
    secret = os.getenv("LIBRARR_WEBHOOK_SECRET", "")
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest() if secret else ""
    headers = {"Content-Type": "application/json", "User-Agent": "Librarr/telemetry"}
    if sig:
        headers["X-Librarr-Signature"] = "sha256=" + sig
    for url in urls:
        try:
            resp = requests.post(url, data=body, headers=headers, timeout=timeout)
            code_bucket = f"{resp.status_code//100}xx"
            metrics.inc("librarr_webhooks_total", result="sent", event=event_type, code=code_bucket)
            if resp.status_code >= 400:
                logger.warning("Webhook %s returned HTTP %s", url, resp.status_code)
        except Exception as exc:  # pragma: no cover - network failure path
            metrics.inc("librarr_webhooks_total", result="error", event=event_type)
            logger.warning("Webhook %s failed: %s", url, exc)
