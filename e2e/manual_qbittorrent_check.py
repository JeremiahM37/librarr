#!/usr/bin/env python3
"""Manual end-to-end check of librarr's import modes against a REAL qBittorrent.

The automated suites cover this logic with a fake torrent client. This script
proves the part a fake cannot: that qBittorrent itself still considers an
imported torrent seedable, and that deleting a torrent's files does not take
the library copy with it.

For each scenario it writes a real payload, has qBittorrent hash it into a real
torrent, seeds it, runs the real librarr binary against the real client, then
force-rechecks the torrent through the API — the operation that exposes a
"ghost" torrent whose data was moved away.

    # disposable qBittorrent in Docker, started and removed by the script
    LIBRARR_BIN=./librarr python3 e2e/manual_qbittorrent_check.py --spawn

    # or point it at a qBittorrent you already run
    LIBRARR_BIN=./librarr QBT_URL=http://127.0.0.1:8080 QBT_PASSWORD=secret \
      TEST_ROOT=/data/qbt-check QBT_DOWNLOADS_MOUNT=/downloads \
      python3 e2e/manual_qbittorrent_check.py

TEST_ROOT/downloads must be the host side of the client's QBT_DOWNLOADS_MOUNT,
and TEST_ROOT must sit on one filesystem so hardlinks are possible. Not run by
CI: it needs a torrent client. The filename deliberately avoids pytest's
test_*.py / *_test.py collection patterns.
"""
import argparse
import io
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import uuid
import zipfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--spawn", action="store_true",
                    help="start (and remove) a disposable qBittorrent container")
parser.add_argument("--keep", action="store_true",
                    help="with --spawn, leave the container running afterwards")
args = parser.parse_args()

BIN = os.path.abspath(os.environ.get("LIBRARR_BIN", "./librarr"))
if not os.path.exists(BIN):
    sys.exit(f"LIBRARR_BIN not found: {BIN}")

ROOT = os.path.abspath(os.environ.get("TEST_ROOT", "./qbt-check"))
DOWNLOADS = f"{ROOT}/downloads"
MOUNT = os.environ.get("QBT_DOWNLOADS_MOUNT", "/downloads")
QB = os.environ.get("QBT_URL", "http://127.0.0.1:18080")
PW = os.environ.get("QBT_PASSWORD", "")
CATEGORY = "librarr-import-check"
CONTAINER = "librarr-qbt-import-check"
IMAGE = os.environ.get("QBT_IMAGE", "lscr.io/linuxserver/qbittorrent:latest")

_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())


def spawn_qbittorrent():
    """Run a throwaway qBittorrent and return the session's temp password."""
    global QB
    QB = "http://127.0.0.1:18080"
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    subprocess.run([
        "docker", "run", "-d", "--name", CONTAINER,
        "-e", f"PUID={os.getuid()}", "-e", f"PGID={os.getgid()}",
        "-e", "WEBUI_PORT=18080", "-p", "127.0.0.1:18080:18080",
        "-v", f"{ROOT}/qbconfig:/config", "-v", f"{DOWNLOADS}:{MOUNT}",
        IMAGE,
    ], check=True, capture_output=True)

    deadline = time.time() + 120
    while time.time() < deadline:
        logs = subprocess.run(["docker", "logs", CONTAINER],
                              capture_output=True, text=True)
        for line in (logs.stdout + logs.stderr).splitlines():
            if "temporary password is provided" in line:
                return line.rsplit(":", 1)[1].strip()
        time.sleep(2)
    sys.exit("qBittorrent container never printed a session password")


def stop_qbittorrent():
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)




def qb(path, data=None, raw=False, files=None):
    url = f"{QB}/api/v2/{path}"
    if files is not None:
        boundary = uuid.uuid4().hex
        body = b""
        for k, v in (data or {}).items():
            body += (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n").encode()
        for k, (fname, content) in files.items():
            body += (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"; filename=\"{fname}\"\r\n"
                     f"Content-Type: application/x-bittorrent\r\n\r\n").encode() + content + b"\r\n"
        body += f"--{boundary}--\r\n".encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    elif data is not None:
        req = urllib.request.Request(url, data=urllib.parse.urlencode(data).encode())
    else:
        req = urllib.request.Request(url)
    with _opener.open(req, timeout=30) as r:
        payload = r.read()
    if raw:
        return payload
    text = payload.decode(errors="replace").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def login():
    # qBittorrent 4.x answers "Ok."; 5.x answers 204 with an empty body and
    # only the session cookie. Probe an authenticated endpoint to be sure.
    qb("auth/login", {"username": "admin", "password": PW})
    try:
        version = qb("app/version")
    except Exception as err:  # noqa: BLE001 - surfaced verbatim below
        sys.exit(f"qBittorrent login failed: {err}")
    print(f"authenticated against qBittorrent {version} "
          f"(WebAPI {qb('app/webapiVersion')})", flush=True)


def minimal_epub(title):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(zipfile.ZipInfo("mimetype"), "application/epub+zip",
                   compress_type=zipfile.ZIP_STORED)
        z.writestr("META-INF/container.xml",
                   '<?xml version="1.0"?>\n<container version="1.0" '
                   'xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
                   '<rootfiles><rootfile full-path="OEBPS/content.opf" '
                   'media-type="application/oebps-package+xml"/></rootfiles></container>')
        z.writestr("OEBPS/content.opf",
                   '<?xml version="1.0" encoding="UTF-8"?>\n<package '
                   'xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="uid">'
                   '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
                   '<dc:identifier id="uid">real-qbt</dc:identifier>'
                   f'<dc:title>{title}</dc:title><dc:creator>Real Tester</dc:creator>'
                   '<dc:language>en</dc:language></metadata>'
                   '<manifest><item id="text" href="text.xhtml" '
                   'media-type="application/xhtml+xml"/></manifest>'
                   '<spine><itemref idref="text"/></spine></package>')
        # Padding so the torrent has real content to hash.
        z.writestr("OEBPS/text.xhtml",
                   '<?xml version="1.0" encoding="UTF-8"?>\n<html '
                   'xmlns="http://www.w3.org/1999/xhtml"><head><title>t</title></head>'
                   '<body><p>' + ("real qbittorrent payload " * 2000) + '</p></body></html>')
    return buf.getvalue()


def seed_torrent(name, title):
    """Create a real payload + torrent and get qBittorrent seeding it at 100%."""
    folder = f"{DOWNLOADS}/{name}"
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/book.epub", "wb") as f:
        f.write(minimal_epub(title))

    task = qb("torrentcreator/addTask", {
        "sourcePath": f"{MOUNT}/{name}",
        "format": "v1",
        "private": "false",
        "startSeeding": "false",
        "trackers": "http://127.0.0.1:6969/announce",
    })
    task_id = task["taskID"]
    for _ in range(60):
        status = qb(f"torrentcreator/status?taskID={task_id}")
        state = status[0]["status"] if status else "?"
        if state == "Finished":
            break
        if state == "Failed":
            sys.exit(f"torrent creation failed: {status}")
        time.sleep(1)
    else:
        sys.exit("torrent creation timed out")

    torrent = qb(f"torrentcreator/torrentFile?taskID={task_id}", raw=True)
    qb("torrents/add", data={"savepath": MOUNT, "category": CATEGORY,
                             "autoTMM": "false", "skip_checking": "false"},
       files={"torrents": (f"{name}.torrent", torrent)})

    # Wait for qBittorrent to hash the existing data and report a complete seed.
    for _ in range(60):
        rows = qb(f"torrents/info?category={CATEGORY}")
        for t in rows:
            if t["name"] == name and t["progress"] == 1.0 and "checking" not in t["state"]:
                return t["hash"]
        time.sleep(1)
    sys.exit(f"torrent never reached 100%: {qb(f'torrents/info?category={CATEGORY}')}")


def torrent_by_hash(h):
    for t in qb("torrents/info"):
        if t["hash"] == h:
            return t
    return None


def force_recheck(h):
    """Force a hash recheck and return the torrent's state afterwards. This is
    the exact operation that exposes a 'ghost' torrent whose data was moved."""
    qb("torrents/recheck", {"hashes": h})
    time.sleep(2)
    for _ in range(60):
        t = torrent_by_hash(h)
        if t and "checking" not in t["state"] and t["state"] != "moving":
            time.sleep(2)  # let the final state settle
            return torrent_by_hash(h)
        time.sleep(1)
    return torrent_by_hash(h)


def run_librarr(scenario, import_mode, remove_after):
    data = f"{ROOT}/data/{scenario}"
    books = f"{ROOT}/books/{scenario}"
    os.makedirs(data, exist_ok=True)
    os.makedirs(books, exist_ok=True)
    registry = f"{data}/sources.json"
    with open(registry, "w") as f:
        json.dump({"version": 1, "annas": {"domain": ""}, "webnovels": [],
                   "libgen_mirrors": [], "zlibrary_default": ""}, f)

    env = {
        **os.environ,
        "LIBRARR_PORT": "15051",
        "LIBRARR_DB_PATH": f"{data}/librarr.db",
        "SETTINGS_FILE": f"{data}/settings.json",
        "LIBRARR_SOURCES_PATH": registry,
        "QB_URL": QB,
        "QB_USER": "admin",
        "QB_PASS": PW,
        "QB_CATEGORY": CATEGORY,
        "QB_SAVE_PATH": MOUNT,
        "INCOMING_DIR": DOWNLOADS,
        "EBOOK_DIR": books,
        "AUDIOBOOK_DIR": f"{books}/audio",
        "MANGA_DIR": f"{books}/manga",
        "FILE_ORG_ENABLED": "true",
        "REMOVE_TORRENT_AFTER_IMPORT": "true" if remove_after else "false",
    }
    # import_mode None means "not configured at all" — the automatic path.
    if import_mode is not None:
        env["IMPORT_MODE"] = import_mode
    log = open(f"{data}/librarr.log", "w")
    proc = subprocess.Popen([BIN], env=env, stdout=log, stderr=log)
    return proc, log, books


def wait_for_import(books, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for dirpath, _, names in os.walk(books):
            for n in names:
                if n.endswith(".epub"):
                    time.sleep(2)  # let the post-import steps finish
                    return os.path.join(dirpath, n)
        time.sleep(2)
    return None


results = []


def check(scenario, ok, detail):
    results.append((scenario, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {detail}", flush=True)


def scenario_hardlink_keeps_seeding():
    name = "Real Hardlink Book"
    print("\n=== A: IMPORT_MODE=hardlink, REMOVE_TORRENT_AFTER_IMPORT=false ===", flush=True)
    h = seed_torrent(name, "Real Hardlink Book")
    payload = f"{DOWNLOADS}/{name}/book.epub"
    src_inode = os.stat(payload).st_ino

    proc, log, books = run_librarr("hardlink", "hardlink", False)
    try:
        imported = wait_for_import(books)
        check("A", imported is not None, f"librarr imported the torrent ({imported})")
        if not imported:
            return
        check("A", os.path.exists(payload), "download payload still on disk after import")
        check("A", os.stat(imported).st_ino == src_inode,
              f"library file shares the payload's inode ({src_inode})")
        check("A", os.stat(imported).st_nlink == 2, "inode has 2 links (download + library)")
    finally:
        proc.terminate(); proc.wait(timeout=10); log.close()

    t = torrent_by_hash(h)
    check("A", t is not None, "torrent still registered in qBittorrent")
    t = force_recheck(h)
    check("A", t["progress"] == 1.0,
          f"after force-recheck: progress={t['progress']:.2f} state={t['state']}")
    check("A", t["state"] not in ("missingFiles", "error"),
          f"qBittorrent state is seedable: {t['state']}")
    qb("torrents/delete", {"hashes": h, "deleteFiles": "true"})


def scenario_move_reproduces_the_bug():
    name = "Real Move Book"
    print("\n=== B: IMPORT_MODE=move (old behavior), REMOVE_TORRENT_AFTER_IMPORT=false ===", flush=True)
    h = seed_torrent(name, "Real Move Book")
    payload = f"{DOWNLOADS}/{name}/book.epub"

    proc, log, books = run_librarr("move", "move", False)
    try:
        imported = wait_for_import(books)
        check("B", imported is not None, f"librarr imported the torrent ({imported})")
        check("B", not os.path.exists(payload), "move mode consumed the download payload")
    finally:
        proc.terminate(); proc.wait(timeout=10); log.close()

    t = torrent_by_hash(h)
    check("B", t is not None and t["progress"] == 1.0,
          f"torrent still *claims* 100% before recheck (ghost): progress={t['progress']:.2f} state={t['state']}")
    t = force_recheck(h)
    check("B", t["progress"] < 1.0 or t["state"] in ("missingFiles", "error"),
          f"force-recheck exposes the reported bug: progress={t['progress']:.2f} state={t['state']}")
    qb("torrents/delete", {"hashes": h, "deleteFiles": "true"})


def scenario_hardlink_with_removal():
    name = "Real Removal Book"
    print("\n=== C: IMPORT_MODE=hardlink, REMOVE_TORRENT_AFTER_IMPORT=true ===", flush=True)
    h = seed_torrent(name, "Real Removal Book")
    payload = f"{DOWNLOADS}/{name}/book.epub"
    original = open(payload, "rb").read()

    proc, log, books = run_librarr("removal", "hardlink", True)
    try:
        imported = wait_for_import(books)
        check("C", imported is not None, f"librarr imported the torrent ({imported})")
        if imported:
            time.sleep(5)  # deletion happens right after the import
    finally:
        proc.terminate(); proc.wait(timeout=10); log.close()

    check("C", torrent_by_hash(h) is None, "torrent removed from qBittorrent")
    check("C", not os.path.exists(payload),
          "qBittorrent deleted the download payload (no orphan left behind)")
    if imported:
        check("C", os.path.exists(imported), "library file survived the client-side delete")
        check("C", open(imported, "rb").read() == original,
              "library file's bytes are intact after the payload link was removed")
        check("C", os.stat(imported).st_nlink == 1, "library file is now the only link to the data")


def scenario_copy_keeps_seeding():
    name = "Real Copy Book"
    print("\n=== D: IMPORT_MODE=copy, REMOVE_TORRENT_AFTER_IMPORT=false ===", flush=True)
    h = seed_torrent(name, "Real Copy Book")
    payload = f"{DOWNLOADS}/{name}/book.epub"
    src_inode = os.stat(payload).st_ino

    proc, log, books = run_librarr("copy", "copy", False)
    try:
        imported = wait_for_import(books)
        check("D", imported is not None, f"librarr imported the torrent ({imported})")
        if not imported:
            return
        check("D", os.path.exists(payload), "download payload still on disk after import")
        check("D", os.stat(imported).st_ino != src_inode, "library file is an independent copy")
    finally:
        proc.terminate(); proc.wait(timeout=10); log.close()

    t = force_recheck(h)
    check("D", t["progress"] == 1.0 and t["state"] not in ("missingFiles", "error"),
          f"after force-recheck: progress={t['progress']:.2f} state={t['state']}")
    qb("torrents/delete", {"hashes": h, "deleteFiles": "true"})


def scenario_single_knob():
    """The one-config-change path: REMOVE_TORRENT_AFTER_IMPORT=false and
    nothing else. IMPORT_MODE is not set at all."""
    name = "Real Single Knob Book"
    print("\n=== E: only REMOVE_TORRENT_AFTER_IMPORT=false (IMPORT_MODE unset) ===", flush=True)
    h = seed_torrent(name, "Real Single Knob Book")
    payload = f"{DOWNLOADS}/{name}/book.epub"
    src_inode = os.stat(payload).st_ino

    proc, log, books = run_librarr("singleknob", None, False)
    try:
        imported = wait_for_import(books)
        check("E", imported is not None, f"librarr imported the torrent ({imported})")
        if not imported:
            return
        policy = [l for l in open(f"{ROOT}/data/singleknob/librarr.log") if "import policy" in l]
        check("E", any("mode=hardlink" in l and "automatic=true" in l for l in policy),
              f"startup log resolved the mode: {policy[0].strip() if policy else 'MISSING'}")
        check("E", os.path.exists(payload), "download payload still on disk after import")
        check("E", os.stat(imported).st_ino == src_inode,
              "library file shares the payload inode with IMPORT_MODE unset")
    finally:
        proc.terminate(); proc.wait(timeout=10); log.close()

    check("E", torrent_by_hash(h) is not None, "torrent still registered in qBittorrent")
    t = force_recheck(h)
    check("E", t["progress"] == 1.0 and t["state"] not in ("missingFiles", "error"),
          f"still seedable after force-recheck: progress={t['progress']:.2f} state={t['state']}")
    qb("torrents/delete", {"hashes": h, "deleteFiles": "true"})


os.makedirs(DOWNLOADS, exist_ok=True)
os.makedirs(f"{ROOT}/qbconfig", exist_ok=True)
if args.spawn:
    PW = spawn_qbittorrent()
    print(f"spawned {CONTAINER} (session password {PW})", flush=True)
elif not PW:
    sys.exit("set QBT_PASSWORD, or pass --spawn to start a disposable client")

login()
for stale in qb(f"torrents/info?category={CATEGORY}"):
    qb("torrents/delete", {"hashes": stale["hash"], "deleteFiles": "true"})
shutil.rmtree(f"{ROOT}/books", ignore_errors=True)
shutil.rmtree(f"{ROOT}/data", ignore_errors=True)

scenario_single_knob()
scenario_hardlink_keeps_seeding()
scenario_move_reproduces_the_bug()
scenario_hardlink_with_removal()
scenario_copy_keeps_seeding()

if args.spawn and not args.keep:
    stop_qbittorrent()

failed = [r for r in results if not r[1]]
print("\n" + "=" * 60)
print(f"{len(results) - len(failed)}/{len(results)} checks passed")
if failed:
    for s, _, d in failed:
        print(f"  FAILED [{s}] {d}")
    sys.exit(1)
print("ALL REAL-QBITTORRENT CHECKS PASSED")
