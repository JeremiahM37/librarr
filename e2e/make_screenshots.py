#!/usr/bin/env python3
"""Regenerate the README screenshots for the Wanted tab and the Wanted List &
Quality settings card.

Boots the real librarr binary against a local gutendex-style stub (no external
network), seeds a wanted list whose items sit in every state the UI shows, and
captures the two PNGs at the same 2x scale as the other screenshots.

    LIBRARR_E2E_BIN=./librarr python3 e2e/make_screenshots.py

Without LIBRARR_E2E_BIN the binary is built with `go build`.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conftest import _minimal_epub, _minimal_pdf  # noqa: E402

from playwright.sync_api import sync_playwright  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "docs" / "screenshots"

# Public-domain titles the stub "Project Gutenberg" serves. Frankenstein is
# delivered as a PDF so the wanted list has an "upgrade wanted" row.
BOOKS = [
    {"id": 1342, "title": "Pride and Prejudice", "author": "Austen, Jane", "year": 1813},
    {"id": 84, "title": "Frankenstein; Or, The Modern Prometheus", "author": "Shelley, Mary Wollstonecraft", "year": 1818, "pdf": True},
    {"id": 345, "title": "Dracula", "author": "Stoker, Bram", "year": 1897},
    {"id": 2701, "title": "Moby Dick; Or, The Whale", "author": "Melville, Herman", "year": 1851},
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Stub(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        base = f"http://127.0.0.1:{self.server.server_address[1]}"
        path, _, query = self.path.partition("?")
        if path == "/books":
            q = ""
            for part in query.split("&"):
                if part.startswith("search="):
                    q = urllib.request.unquote(part[7:].replace("+", " ")).lower()
            words = [w for w in q.split() if len(w) > 2]
            results = []
            for b in BOOKS:
                hay = (b["title"] + " " + b["author"]).lower()
                if words and not any(w in hay for w in words):
                    continue
                results.append({
                    "id": b["id"], "title": b["title"], "authors": [{"name": b["author"]}],
                    "languages": ["en"], "download_count": 5000 + b["id"],
                    "formats": {"application/epub+zip": f"{base}/files/{b['id']}.epub"},
                })
            self._send(200, json.dumps({"count": len(results), "results": results}).encode(), "application/json")
        elif path.startswith("/files/"):
            bid = int(path.split("/")[-1].split(".")[0])
            b = next(x for x in BOOKS if x["id"] == bid)
            if b.get("pdf"):
                self._send(200, _minimal_pdf(b["title"]), "application/pdf")
            else:
                self._send(200, _minimal_epub(b["title"]), "application/epub+zip")
        elif path == "/search.json":
            self._send(200, json.dumps({"docs": [
                {"key": "/works/OL1W", "title": "Pride and Prejudice", "first_publish_year": 1813, "author_name": ["Jane Austen"]},
                {"key": "/works/OL2W", "title": "Emma", "first_publish_year": 1815, "author_name": ["Jane Austen"]},
                {"key": "/works/OL3W", "title": "Persuasion", "first_publish_year": 1817, "author_name": ["Jane Austen"]},
            ]}).encode(), "application/json")
        else:
            self._send(404, b"", "text/plain")

    def log_message(self, *a):
        pass


def api(base, path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read() or b"{}")


def wait_idle(base):
    """Wait until no download is running and no wanted row is mid-grab."""
    for _ in range(90):
        jobs = api(base, "/api/downloads").get("downloads") or []
        busy = [j for j in jobs if j.get("status") in ("queued", "downloading", "importing", "retry_wait", "searching")]
        rows = api(base, "/api/wishlist").get("items") or []
        if not busy and not [r for r in rows if r.get("state") == "downloading"]:
            return
        time.sleep(1)


def main():
    binary = os.environ.get("LIBRARR_E2E_BIN")
    tmp = Path(tempfile.mkdtemp(prefix="librarr-shots-"))
    if not binary:
        binary = str(tmp / "librarr")
        subprocess.run(["go", "build", "-o", binary, "./cmd/librarr"], cwd=REPO, check=True)

    stub_port = free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", stub_port), Stub)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    stub = f"http://127.0.0.1:{stub_port}"
    dead = "http://127.0.0.1:1"
    registry = {
        "version": 1, "annas": {"domain": ""}, "audiobookbay": {"url": dead}, "thepiratebay": {"url": dead},
        "gutenberg": {"url": f"{stub}/books"}, "openlibrary": {"search_url": dead, "covers_url": dead},
        "librivox": {"url": dead}, "standardebooks": {"url": dead}, "mangadex": {"url": dead}, "nyaa": {"url": dead},
        "webnovels": [], "libgen_mirrors": [], "zlibrary_default": "",
    }
    (tmp / "sources.json").write_text(json.dumps(registry))
    for d in ("books", "incoming", "audiobooks"):
        (tmp / d).mkdir()
    port = free_port()
    env = {
        **os.environ,
        "LIBRARR_PORT": str(port), "LIBRARR_DB_PATH": str(tmp / "librarr.db"),
        "SETTINGS_FILE": str(tmp / "settings.json"), "LIBRARR_SOURCES_PATH": str(tmp / "sources.json"),
        "EBOOK_DIR": str(tmp / "books"), "AUDIOBOOK_DIR": str(tmp / "audiobooks"), "INCOMING_DIR": str(tmp / "incoming"),
        "LIBRARR_INSECURE_ALLOW_PRIVATE_URLS": "1",
        "SCHEDULER_ENABLED": "1", "SCHEDULER_AUTO_DOWNLOAD": "1", "AUTO_UPGRADE_ENABLED": "1",
        "SCHEDULER_ITEM_DELAY_SECONDS": "0", "AUTHOR_MONITOR_ENABLED": "1", "OPENLIBRARY_URL": stub,
    }
    log = open(tmp / "librarr.log", "w")
    proc = subprocess.Popen([binary], env=env, stdout=log, stderr=log)
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(60):
            try:
                urllib.request.urlopen(base + "/api/health", timeout=1)
                break
            except Exception:
                time.sleep(0.5)

        # Seed the wanted list: one row per state.
        api(base, "/api/wishlist", "POST", {"title": "Pride and Prejudice", "author": "Jane Austen"})
        api(base, "/api/wishlist", "POST", {"title": "Frankenstein; Or, The Modern Prometheus", "author": "Mary Wollstonecraft Shelley"})
        api(base, "/api/wishlist", "POST", {"title": "The Count of Monte Cristo", "author": "Alexandre Dumas"})
        api(base, "/api/wishlist", "POST", {"title": "Dracula", "author": "Bram Stoker", "media_type": "audiobook"})
        mid = api(base, "/api/wishlist", "POST", {"title": "Moby Dick", "author": "Herman Melville", "monitored": False})["id"]
        api(base, "/api/scheduler/run?wait=1", "POST")
        wait_idle(base)
        api(base, "/api/scheduler/run?wait=1", "POST")  # second pass records "not an upgrade"/"no results" lines
        wait_idle(base)
        aid = api(base, "/api/authors/monitor", "POST", {"name": "Jane Austen", "check_interval_days": 7})["id"]
        api(base, f"/api/authors/{aid}/check", "POST")

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
            page.goto(base, wait_until="networkidle")
            page.click('[data-action="switchTab"][data-arg="wishlist"]')
            page.wait_for_selector('[data-wanted-id]')
            page.wait_for_timeout(600)
            page.screenshot(path=str(OUT / "wishlist.png"))

            page.click('[data-action="switchTab"][data-arg="settings"]')
            page.wait_for_selector('[data-qp]')
            page.wait_for_timeout(800)
            # The sticky top bar would be composited over a tall element
            # capture; hide it for this shot.
            page.add_style_tag(content=".sticky { visibility: hidden !important; }")
            card = page.locator("#wanted-settings")
            card.scroll_into_view_if_needed()
            card.screenshot(path=str(OUT / "settings.png"))
            browser.close()
        print("wrote", OUT / "wishlist.png", "and", OUT / "settings.png")
        print("wanted rows:", [(i["title"], i["state"]) for i in api(base, "/api/wishlist")["items"]])
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
        httpd.shutdown()


if __name__ == "__main__":
    main()
