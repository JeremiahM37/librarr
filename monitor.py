"""
monitor.py — AI-powered self-healing monitor for Librarr.

Captures WARNING/ERROR log events, periodically calls an AI backend to
diagnose issues, and either auto-executes safe remediations or queues
riskier ones for user approval via the /api/monitor/* endpoints.

Supported AI backends:
  - ollama / openai-compatible: any server implementing OpenAI chat completions
  - anthropic: Anthropic Claude API (native)

Integration in app.py:
    import monitor
    _monitor = monitor.LibrarrMonitor(
        get_jobs=lambda: list(download_jobs.items()),
        qb_reauth=lambda: qb.login(),
        get_abb_domains=lambda: list(ABB_DOMAINS),
        rotate_abb_domain=_rotate_abb_domain,
        trigger_abs_scan=_do_abs_scan,
        docker_socket=config.DOCKER_SOCKET,
    )
    _monitor.start()
"""

import collections
import json
import logging
import os
import socket as _socket
import threading
import time
import uuid

import requests

import config

logger = logging.getLogger("librarr.monitor")


# ── Error capture ──────────────────────────────────────────────────────────────

class ErrorCapture(logging.Handler):
    """Logging handler that appends WARNING+ records to a rolling deque."""

    def __init__(self, maxlen: int = 200):
        super().__init__(level=logging.WARNING)
        self.setFormatter(logging.Formatter("%(message)s"))
        self._entries = collections.deque(maxlen=maxlen)

    def emit(self, record):
        try:
            self._entries.append({
                "ts": record.created,
                "ts_str": time.strftime("%H:%M:%S", time.localtime(record.created)),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            })
        except Exception:
            pass

    def recent(self, n: int = 50):
        entries = list(self._entries)
        return entries[-n:]

    def clear(self):
        self._entries.clear()


# ── Action queue ───────────────────────────────────────────────────────────────

class ActionQueue:
    """Thread-safe queue for pending user-approval actions and history."""

    def __init__(self):
        self._lock = threading.Lock()
        self._pending = {}
        self._history = collections.deque(maxlen=20)

    def add(self, action, params, description, risk="safe"):
        action_id = str(uuid.uuid4())[:8]
        entry = {
            "id": action_id,
            "action": action,
            "params": params,
            "description": description,
            "risk": risk,
            "created_at": time.time(),
        }
        with self._lock:
            self._pending[action_id] = entry
        return action_id

    def get(self, action_id):
        with self._lock:
            return self._pending.get(action_id)

    def complete(self, action_id, result):
        with self._lock:
            entry = self._pending.pop(action_id, None)
            if entry:
                entry["result"] = result
                entry["done_at"] = time.time()
                self._history.appendleft(entry)

    def dismiss(self, action_id):
        with self._lock:
            entry = self._pending.pop(action_id, None)
            if entry:
                entry["result"] = "dismissed"
                entry["done_at"] = time.time()
                self._history.appendleft(entry)
        return entry is not None

    def list_pending(self):
        with self._lock:
            return list(self._pending.values())

    def list_history(self):
        return list(self._history)


# ── Allowed actions ────────────────────────────────────────────────────────────

ALLOWED_ACTIONS = {
    "retry_torrent":       ("safe",     "Re-queue a failed torrent download"),
    "retry_novel":         ("safe",     "Re-queue a failed novel download"),
    "clear_interrupted":   ("safe",     "Mark interrupted/stuck jobs as cleared"),
    "reauth_qbittorrent":  ("safe",     "Force qBittorrent session re-authentication"),
    "switch_abb_domain":   ("safe",     "Switch to next AudioBookBay fallback domain"),
    "trigger_abs_scan":    ("safe",     "Trigger Audiobookshelf library rescan"),
    "restart_qbittorrent": ("approval", "Restart qBittorrent Docker container"),
}

_ACTIONS_DOC = "\n".join(
    f"- {name}: {label} [risk: {risk}]"
    for name, (risk, label) in ALLOWED_ACTIONS.items()
)

SYSTEM_PROMPT = """You are an AI monitor for Librarr, a self-hosted book/audiobook/web-novel download manager.

Librarr integrates with: qBittorrent (torrent client), Audiobookshelf (audiobook library), \
Calibre-Web (ebook library), AudioBookBay (ABB — audiobook torrent site with domain fallbacks), \
Prowlarr (torrent indexer), and lightnovel-crawler (web novel scraper).

Analyze the provided system state (recent WARNING/ERROR log entries and job summary) and respond \
ONLY with valid JSON in this exact schema — no markdown, no extra text:
{
  "diagnosis": "plain-text summary of what is wrong, or 'All systems nominal' if nothing is wrong",
  "actions": [
    {"action": "action_name", "params": {}, "description": "brief human-readable explanation"}
  ]
}

Allowed actions:
""" + _ACTIONS_DOC + """

Action params:
- retry_torrent / retry_novel: {"job_id": "<id from failed_jobs list>"}
- all others: {}

Rules:
- Only suggest actions that address observed problems.
- Only use job_ids from the provided failed_jobs list — never invent them.
- restart_qbittorrent requires user approval — only suggest for severe failures.
- If everything is fine, return an empty actions array."""


# ── AI Monitor ─────────────────────────────────────────────────────────────────

class LibrarrMonitor:
    """AI-powered self-healing monitor for Librarr."""

    def __init__(
        self,
        *,
        get_jobs,
        qb_reauth,
        get_abb_domains,
        rotate_abb_domain,
        trigger_abs_scan,
        docker_socket="",
    ):
        self._get_jobs = get_jobs
        self._qb_reauth = qb_reauth
        self._get_abb_domains = get_abb_domains
        self._rotate_abb_domain = rotate_abb_domain
        self._trigger_abs_scan = trigger_abs_scan
        self._docker_socket = docker_socket

        self.error_capture = ErrorCapture()
        self.action_queue = ActionQueue()

        self._diagnosis = "Monitor has not run yet."
        self._last_checked = None
        self._running = False
        self._thread = None
        self._manual_event = threading.Event()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def start(self):
        if not config.AI_MONITOR_ENABLED:
            logger.info("AI monitor disabled (set AI_MONITOR_ENABLED=true to enable)")
            return

        logging.getLogger().addHandler(self.error_capture)

        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="ai-monitor"
        )
        self._thread.start()
        logger.info(
            f"AI monitor started — provider={config.AI_PROVIDER}, "
            f"model={config.AI_MODEL}, interval={config.AI_MONITOR_INTERVAL}s"
        )

    def stop(self):
        self._running = False
        self._manual_event.set()

    def _loop(self):
        # Wait one interval before first auto-run
        self._manual_event.wait(timeout=config.AI_MONITOR_INTERVAL)
        self._manual_event.clear()
        while self._running:
            try:
                self.run_cycle()
            except Exception as e:
                logger.error(f"AI monitor cycle error: {e}")
            self._manual_event.wait(timeout=config.AI_MONITOR_INTERVAL)
            self._manual_event.clear()

    def trigger_manual(self):
        """Fire an immediate cycle (for POST /api/monitor/analyze)."""
        if self._running:
            self._manual_event.set()
        else:
            try:
                self.run_cycle()
            except Exception as e:
                logger.error(f"AI monitor manual cycle error: {e}")

    # ── Core cycle ─────────────────────────────────────────────────────────────

    def run_cycle(self):
        context = self._collect_context()
        response = self._call_ai(context)
        if response:
            self._process_response(response)
        self._last_checked = time.time()

    def _collect_context(self):
        jobs = self._get_jobs()
        failed_jobs = [
            {
                "job_id": jid,
                "title": job.get("title", "unknown"),
                "type": job.get("type", "torrent"),
                "status": job.get("status"),
                "error": job.get("error", ""),
            }
            for jid, job in jobs
            if job.get("status") in ("error", "interrupted")
        ]

        return {
            "recent_errors": self.error_capture.recent(50),
            "failed_jobs": failed_jobs[:20],
            "total_jobs": len(jobs),
            "abb_domains": self._get_abb_domains(),
            "integrations": {
                "prowlarr": config.has_prowlarr(),
                "qbittorrent": config.has_qbittorrent(),
                "audiobookshelf": config.has_audiobookshelf(),
                "calibre": config.has_calibre(),
                "kavita": config.has_kavita(),
            },
        }

    # ── AI backend calls ───────────────────────────────────────────────────────

    def _call_ai(self, context):
        user_msg = f"System state:\n{json.dumps(context, indent=2, default=str)}"
        try:
            if config.AI_PROVIDER == "anthropic":
                return self._call_anthropic(user_msg)
            return self._call_openai_compat(user_msg)
        except Exception as e:
            logger.error(f"AI monitor API call failed: {e}")
            return None

    def _call_openai_compat(self, user_msg):
        headers = {"Content-Type": "application/json"}
        if config.AI_API_KEY:
            headers["Authorization"] = f"Bearer {config.AI_API_KEY}"

        payload = {
            "model": config.AI_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            "temperature": 0.1,
        }
        if config.AI_PROVIDER == "openai":
            payload["response_format"] = {"type": "json_object"}

        resp = requests.post(
            f"{config.AI_API_URL}/chat/completions",
            headers=headers,
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return self._parse_json(content)

    def _call_anthropic(self, user_msg):
        headers = {
            "Content-Type": "application/json",
            "x-api-key": config.AI_API_KEY,
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": config.AI_MODEL,
            "max_tokens": 1024,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_msg}],
        }
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]
        return self._parse_json(content)

    def _parse_json(self, content):
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            end = len(lines) - 1 if lines[-1].strip() == "```" else len(lines)
            text = "\n".join(lines[1:end])
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(
                f"AI monitor: could not parse JSON response ({e}): {content[:200]!r}"
            )
            return None

    # ── Response processing ────────────────────────────────────────────────────

    def _process_response(self, response):
        self._diagnosis = response.get("diagnosis", "No diagnosis provided.")

        for suggestion in response.get("actions", []):
            action = suggestion.get("action")
            params = suggestion.get("params") or {}
            description = suggestion.get("description") or action

            if action not in ALLOWED_ACTIONS:
                logger.warning(f"AI monitor suggested unknown action: {action!r} — ignored")
                continue

            risk, _ = ALLOWED_ACTIONS[action]

            if risk == "safe" and config.AI_AUTO_FIX:
                result = self._execute_action(action, params)
                entry_id = self.action_queue.add(action, params, description, risk)
                self.action_queue.complete(entry_id, result)
                logger.info(f"AI monitor auto-applied '{action}': {result}")
            else:
                self.action_queue.add(action, params, description, risk)
                logger.info(f"AI monitor queued '{action}' for user approval")

    # ── Action execution ───────────────────────────────────────────────────────

    def execute_approved(self, action_id):
        entry = self.action_queue.get(action_id)
        if not entry:
            return False, "Action not found or already processed"
        result = self._execute_action(entry["action"], entry["params"])
        self.action_queue.complete(action_id, result)
        return True, result

    def _execute_action(self, action, params):
        try:
            dispatch = {
                "retry_torrent":       lambda: self._act_retry_job(params.get("job_id")),
                "retry_novel":         lambda: self._act_retry_job(params.get("job_id")),
                "clear_interrupted":   self._act_clear_interrupted,
                "reauth_qbittorrent":  self._act_reauth_qbittorrent,
                "switch_abb_domain":   self._act_switch_abb_domain,
                "trigger_abs_scan":    self._act_trigger_abs_scan,
                "restart_qbittorrent": self._act_restart_qbittorrent,
            }
            fn = dispatch.get(action)
            if fn is None:
                return f"Unknown action: {action}"
            return fn()
        except Exception as e:
            logger.error(f"AI monitor action '{action}' raised: {e}")
            return f"Error: {e}"

    def _act_retry_job(self, job_id):
        if not job_id:
            return "No job_id provided"
        jobs = dict(self._get_jobs())
        if job_id not in jobs:
            return f"Job {job_id!r} not found"
        job = jobs[job_id]
        if job.get("status") not in ("error", "interrupted"):
            return f"Job {job_id!r} not in a failed state (status={job.get('status')!r})"
        job["status"] = "queued"
        job["error"] = None
        return f"Job {job_id!r} re-queued"

    def _act_clear_interrupted(self):
        jobs = self._get_jobs()
        cleared = 0
        for _, job in jobs:
            if job.get("status") == "interrupted" or (
                job.get("status") == "error"
                and "Interrupted by restart" in (job.get("error") or "")
            ):
                job["status"] = "cleared"
                cleared += 1
        return f"Cleared {cleared} interrupted job(s)"

    def _act_reauth_qbittorrent(self):
        success = self._qb_reauth()
        return "qBittorrent re-authenticated" if success else "qBittorrent re-auth failed"

    def _act_switch_abb_domain(self):
        new_domain = self._rotate_abb_domain()
        return f"Switched ABB domain to {new_domain}"

    def _act_trigger_abs_scan(self):
        self._trigger_abs_scan()
        return "Audiobookshelf library rescan triggered"

    def _act_restart_qbittorrent(self):
        if not self._docker_socket or not os.path.exists(self._docker_socket):
            return "Docker socket not available"
        container = "qbittorrent"
        try:
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(self._docker_socket)
            req = (
                f"POST /containers/{container}/restart HTTP/1.0\r\n"
                f"Host: localhost\r\n"
                f"Content-Length: 0\r\n\r\n"
            )
            sock.sendall(req.encode())
            resp_raw = sock.recv(256).decode(errors="replace")
            sock.close()
            if "204" in resp_raw or "200" in resp_raw:
                return f"Container '{container}' restart requested"
            return f"Unexpected Docker response: {resp_raw[:100]}"
        except Exception as e:
            return f"Docker socket error: {e}"

    # ── Status ─────────────────────────────────────────────────────────────────

    def get_status(self):
        return {
            "enabled": config.AI_MONITOR_ENABLED,
            "provider": config.AI_PROVIDER,
            "model": config.AI_MODEL,
            "auto_fix": config.AI_AUTO_FIX,
            "interval": config.AI_MONITOR_INTERVAL,
            "last_checked": self._last_checked,
            "diagnosis": self._diagnosis,
            "recent_errors": self.error_capture.recent(50),
            "pending_actions": self.action_queue.list_pending(),
            "action_history": self.action_queue.list_history(),
        }
