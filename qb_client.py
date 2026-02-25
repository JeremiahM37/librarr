"""qBittorrent client and connectivity diagnostics."""
from __future__ import annotations

import logging
import time

import requests

import config

logger = logging.getLogger("librarr")


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
                logger.error("qBittorrent login failed: %r", resp.text)
                self._set_last_error("auth_failed", "Login failed — check username/password", response=resp.text[:120])
            else:
                self._clear_last_error()
            return self.authenticated
        except Exception as e:
            logger.error("qBittorrent login failed: %s", e)
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return False

    def _ensure_auth(self):
        if not self.authenticated:
            if self._ban_until and time.time() < self._ban_until:
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
            resp = self.session.post(f"{config.QB_URL}/api/v2/torrents/add", data=data, timeout=15)
            if resp.status_code == 403:
                self.login()
                resp = self.session.post(f"{config.QB_URL}/api/v2/torrents/add", data=data, timeout=15)
            ok = resp.text == "Ok."
            if ok:
                self._clear_last_error()
            elif resp.status_code == 403:
                self._set_last_error("auth_failed", "qBittorrent rejected add_torrent request (403)")
            else:
                self._set_last_error(f"http_{resp.status_code}", f"qBittorrent add_torrent returned HTTP {resp.status_code}")
            return ok
        except Exception as e:
            logger.error("qBittorrent add torrent failed: %s", e)
            kind, msg = self._classify_exception(e)
            self._set_last_error(kind, msg)
            return False

    def get_torrents(self, category=None):
        self._ensure_auth()
        try:
            params = {"category": category} if category else {}
            resp = self.session.get(f"{config.QB_URL}/api/v2/torrents/info", params=params, timeout=10)
            if resp.status_code == 403:
                self.login()
                resp = self.session.get(f"{config.QB_URL}/api/v2/torrents/info", params=params, timeout=10)
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
                data={"hashes": torrent_hash, "deleteFiles": str(delete_files).lower()},
                timeout=10,
            )
            if resp.status_code == 403:
                self.login()
                resp = self.session.post(
                    f"{config.QB_URL}/api/v2/torrents/delete",
                    data={"hashes": torrent_hash, "deleteFiles": str(delete_files).lower()},
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


def test_qbittorrent_connection(url, user, password, requests_module=requests):
    if not url:
        return {"success": False, "error": "URL required", "error_class": "missing_config"}
    try:
        session = requests_module.Session()
        resp = session.post(
            f"{url.rstrip('/')}/api/v2/auth/login",
            data={"username": user, "password": password},
            timeout=10,
        )
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
    except requests_module.Timeout:
        return {"success": False, "error": "Timed out connecting to qBittorrent", "error_class": "timeout"}
    except requests_module.ConnectionError:
        return {"success": False, "error": "Connection refused — is qBittorrent running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}
