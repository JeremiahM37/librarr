"""Hermetic end-to-end test harness for the librarr web UI.

Boots the real librarr binary against a local stub "Project Gutenberg"
(gutendex-compatible) server, so the full user journey — search, sort,
download, import, wishlist — runs with ZERO external network access:

    [chromium] -> [librarr binary] -> [stub gutendex on 127.0.0.1]

Every other source in the injected registry points at a dead local port and
fails instantly, keeping runs fast and deterministic. The stub serves a
minimal-but-valid EPUB so the download pipeline completes for real.

Requires: the librarr binary (built automatically, or set LIBRARR_E2E_BIN),
pytest-playwright with chromium installed.
"""
import io
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Titles are chosen so relevance / size orderings differ — the tests assert
# that each rendered card maps back to the correct result object under every
# sort mode.
BOOKS = [
    {"id": 101, "title": "Test Adventure", "author": "Alice Author"},
    {"id": 102, "title": "The Grand Test Adventure Compendium", "author": "Bob Writer"},
    {"id": 103, "title": "Adventure of the Test Case", "author": "Carol Coder"},
]

# A deliberately broken cover on one book exercises the delegated
# capture-phase error handler (gradient placeholder fallback).
BROKEN_COVER_ID = 103


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _minimal_epub(title: str) -> bytes:
    """A tiny but structurally valid EPUB (zip: mimetype first + OPF + XHTML)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(
            zipfile.ZipInfo("mimetype"), "application/epub+zip",
            compress_type=zipfile.ZIP_STORED,
        )
        z.writestr("META-INF/container.xml", (
            '<?xml version="1.0"?>\n'
            '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
            '<rootfiles><rootfile full-path="OEBPS/content.opf" '
            'media-type="application/oebps-package+xml"/></rootfiles></container>'
        ))
        z.writestr("OEBPS/content.opf", (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="uid">'
            '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
            f'<dc:identifier id="uid">e2e-{hash(title) & 0xffff}</dc:identifier>'
            f'<dc:title>{title}</dc:title><dc:language>en</dc:language>'
            '</metadata>'
            '<manifest><item id="text" href="text.xhtml" media-type="application/xhtml+xml"/></manifest>'
            '<spine><itemref idref="text"/></spine></package>'
        ))
        z.writestr("OEBPS/text.xhtml", (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>e2e</title></head>'
            '<body><p>Generated fixture for librarr end-to-end tests.</p></body></html>'
        ))
    return buf.getvalue()


# 1x1 PNG — a valid cover image for the books that should NOT hit the
# placeholder fallback path (browsers sniff content, not extensions).
import base64
TINY_IMG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


class _StubHandler(BaseHTTPRequestHandler):
    """gutendex-compatible search endpoint + epub/cover file hosting."""

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 (http.server API)
        base = f"http://127.0.0.1:{self.server.server_address[1]}"
        path = self.path.split("?")[0]

        if path == "/books":
            # Covers must satisfy the UI's CSP (img-src 'self' data: https:):
            # good covers ride along as data: URIs; the "broken" one is an
            # https URL to a dead port (load error -> placeholder fallback).
            import base64 as _b64
            data_uri = "data:image/png;base64," + _b64.b64encode(TINY_IMG).decode()
            results = []
            for b in BOOKS:
                cover = ("https://127.0.0.1:1/broken.jpg"
                         if b["id"] == BROKEN_COVER_ID
                         else data_uri)
                results.append({
                    "id": b["id"],
                    "title": b["title"],
                    "authors": [{"name": b["author"]}],
                    "languages": ["en"],
                    "formats": {
                        "application/epub+zip": f"{base}/files/{b['id']}.epub",
                        "image/jpeg": cover,
                    },
                })
            self._send(200, json.dumps({"count": len(results), "results": results}).encode(),
                       "application/json")
        elif path.startswith("/files/"):
            book_id = int(path.split("/")[-1].split(".")[0])
            title = next(b["title"] for b in BOOKS if b["id"] == book_id)
            self._send(200, _minimal_epub(title), "application/epub+zip")
        elif path.startswith("/covers/broken-"):
            self._send(404, b"no such cover", "text/plain")
        elif path.startswith("/covers/"):
            self._send(200, TINY_IMG, "image/png")
        else:
            self._send(404, b"not found", "text/plain")

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture(scope="session")
def stub_server():
    port = _free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), _StubHandler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{port}"
    httpd.shutdown()


class _KavitaHandler(BaseHTTPRequestHandler):
    """A stand-in Kavita: hands out a JWT on login and records scan calls.

    Mirrors the real API's contract that /api/Library/scan without a libraryId
    is a 400, so a scan that Kavita would have rejected can't look like a pass.
    """

    def do_POST(self):  # noqa: N802 (http.server API)
        path = self.path.split("?")[0]
        if path == "/api/Account/login":
            body = json.dumps({"token": "e2e-jwt"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.server.scan_calls.append(self.path)
        rejected = path == "/api/Library/scan" and "libraryId=" not in self.path
        self.send_response(400 if rejected else 200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture(scope="session")
def kavita_stub():
    """Records every scan librarr asks Kavita to run (issue #98)."""
    port = _free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), _KavitaHandler)
    httpd.scan_calls = []
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield {"url": f"http://127.0.0.1:{port}", "scan_calls": httpd.scan_calls}
    httpd.shutdown()


@pytest.fixture(scope="session")
def librarr_binary(tmp_path_factory) -> Path:
    env_bin = os.environ.get("LIBRARR_E2E_BIN")
    if env_bin:
        return Path(env_bin).resolve()
    out = tmp_path_factory.mktemp("bin") / "librarr"
    subprocess.run(
        ["go", "build", "-o", str(out), "./cmd/librarr"],
        cwd=REPO_ROOT, check=True,
    )
    return out


@pytest.fixture(scope="session")
def app(stub_server, kavita_stub, librarr_binary, tmp_path_factory):
    """Boot librarr with an injected registry: gutenberg -> stub, everything
    else -> a dead port that refuses connections instantly."""
    data = tmp_path_factory.mktemp("data")
    dead = "http://127.0.0.1:1"  # nothing listens; instant connection refused
    registry = {
        "version": 1,
        "annas": {"domain": ""},
        "audiobookbay": {"url": dead},
        "thepiratebay": {"url": dead},
        "gutenberg": {"url": f"{stub_server}/books"},
        "openlibrary": {"search_url": dead, "covers_url": dead},
        "librivox": {"url": dead},
        "standardebooks": {"url": dead},
        "mangadex": {"url": dead},
        "nyaa": {"url": dead},
        "webnovels": [],
        "libgen_mirrors": [],
        "zlibrary_default": "",
    }
    reg_path = data / "sources.json"
    reg_path.write_text(json.dumps(registry))

    port = _free_port()
    books = data / "books"
    incoming = data / "incoming"
    for d in (books, incoming, data / "audiobooks"):
        d.mkdir()

    env = {
        **os.environ,
        "LIBRARR_PORT": str(port),
        "LIBRARR_DB_PATH": str(data / "librarr.db"),
        "SETTINGS_FILE": str(data / "settings.json"),
        "LIBRARR_SOURCES_PATH": str(reg_path),
        "EBOOK_DIR": str(books),
        "AUDIOBOOK_DIR": str(data / "audiobooks"),
        "INCOMING_DIR": str(incoming),
        # Kavita integration on, no library ID — the reporter's configuration
        # in issue #98, where EBOOK_DIR already sits inside a Kavita folder.
        "KAVITA_URL": kavita_stub["url"],
        "KAVITA_USER": "e2e",
        "KAVITA_PASS": "e2e",
        # The stub serves downloads from 127.0.0.1, which the SSRF guard
        # rightly blocks by default. This opt-out exists for LAN mirrors and
        # for exactly this kind of hermetic test.
        "LIBRARR_INSECURE_ALLOW_PRIVATE_URLS": "1",
    }
    log = open(data / "librarr.log", "w")
    proc = subprocess.Popen([str(librarr_binary)], env=env, stdout=log, stderr=log)

    base = f"http://127.0.0.1:{port}"
    for _ in range(60):
        try:
            urllib.request.urlopen(f"{base}/api/health", timeout=1)
            break
        except Exception:
            if proc.poll() is not None:
                log.close()
                raise RuntimeError(
                    "librarr exited during startup:\n" + (data / "librarr.log").read_text())
            time.sleep(0.5)
    else:
        proc.kill()
        raise RuntimeError("librarr did not become healthy within 30s")

    yield {
        "base": base,
        "data": data,
        "books_dir": books,
        "kavita_scans": kavita_stub["scan_calls"],
    }

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()


@pytest.fixture()
def ui(app, page):
    """A page on the librarr UI that records every JS error. Tests assert the
    journey stays error-free (the strongest 'the frontend works' invariant)."""
    errors: list[str] = []
    page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
    page.on(
        "console",
        lambda m: errors.append(f"console: {m.text}")
        if m.type == "error" and "Failed to load resource" not in m.text
        else None,
    )
    page.goto(app["base"], wait_until="networkidle")
    yield {"page": page, "errors": errors, **app}
    assert errors == [], f"JS errors during journey: {errors}"
