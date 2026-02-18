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
from flask import Flask, jsonify, request, send_file

import config
import sources

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("librarr")


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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS download_jobs (
                    job_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)

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
            if data.get("status") in ("queued", "downloading", "importing"):
                data["status"] = "error"
                data["error"] = "Interrupted by restart"
                stale += 1
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
        job = _PersistentJob(self, job_id, dict(value))
        self._cache[job_id] = job
        self._persist(job_id, job._data)

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


class _PersistentJob:
    """Wraps a job dict so that mutations auto-persist to SQLite."""

    def __init__(self, store, job_id, data):
        self._store = store
        self._job_id = job_id
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        self._store._persist(self._job_id, self._data)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __repr__(self):
        return repr(self._data)


_DB_PATH = os.getenv("LIBRARR_DB_PATH", "/data/librarr/downloads.db")
download_jobs = DownloadStore(_DB_PATH)


# =============================================================================
# qBittorrent Client
# =============================================================================
class QBittorrentClient:
    def __init__(self):
        self.session = requests.Session()
        self.authenticated = False

    def login(self):
        if not config.has_qbittorrent():
            return False
        try:
            resp = self.session.post(
                f"{config.QB_URL}/api/v2/auth/login",
                data={"username": config.QB_USER, "password": config.QB_PASS},
                timeout=10,
            )
            self.authenticated = resp.text == "Ok."
            return self.authenticated
        except Exception as e:
            logger.error(f"qBittorrent login failed: {e}")
            return False

    def _ensure_auth(self):
        if not self.authenticated:
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
            return resp.text == "Ok."
        except Exception as e:
            logger.error(f"qBittorrent add torrent failed: {e}")
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
            return resp.json()
        except Exception:
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
            return resp.status_code == 200
        except Exception:
            return False


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
ABB_URL = "https://audiobookbay.lu"
ABB_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.dler.org:6969/announce",
    "http://tracker.files.fm:6969/announce",
]


def search_audiobookbay(query):
    results = []
    try:
        resp = requests.get(
            f"{ABB_URL}/",
            params={"s": query, "tt": "1"},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        if resp.status_code != 200:
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
        resp = requests.get(
            f"{ABB_URL}{abb_path}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
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
                for future in as_completed(futures, timeout=25):
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
# Calibre Import
# =============================================================================
def import_to_calibre(host_path, title="", author=""):
    if not config.has_calibre():
        logger.info(f"Calibre not configured, skipping import for: {title or host_path}")
        return False

    container_path = host_path
    if host_path.startswith("/books-incoming"):
        container_path = host_path.replace("/books-incoming", "/books/incoming", 1)
    else:
        container_path = host_path.replace(config.CALIBRE_LIBRARY, config.CALIBRE_LIBRARY_CONTAINER)
    try:
        result = subprocess.run(
            [
                "docker", "exec", config.CALIBRE_CONTAINER,
                "calibredb", "add", container_path,
                "--library-path", config.CALIBRE_LIBRARY_CONTAINER,
            ],
            capture_output=True, text=True, timeout=120,
        )
        match = re.search(r"Added book ids: (\d+)", result.stdout)
        if match:
            book_id = match.group(1)
            if author or title:
                meta_cmd = [
                    "docker", "exec", config.CALIBRE_CONTAINER,
                    "calibredb", "set_metadata", book_id,
                    "--library-path", config.CALIBRE_LIBRARY_CONTAINER,
                ]
                if author:
                    meta_cmd.extend(["--field", f"authors:{author}"])
                if title:
                    meta_cmd.extend(["--field", f"title:{title}"])
                meta_cmd.extend(["--field", "tags:Web Novel"])
                subprocess.run(meta_cmd, capture_output=True, timeout=30)
            logger.info(f"Imported to Calibre: {title or host_path} (ID: {book_id})")
            _scan_abs_ebook_library()
            return True
        logger.error(f"Calibre import failed: {result.stderr}")
        return False
    except Exception as e:
        logger.error(f"Calibre import error: {e}")
        return False


def _scan_abs_ebook_library():
    if not config.has_audiobookshelf() or not config.ABS_EBOOK_LIBRARY_ID:
        return
    try:
        requests.post(
            f"{config.ABS_URL}/api/libraries/{config.ABS_EBOOK_LIBRARY_ID}/scan",
            headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
            timeout=10,
        )
        logger.info("Audiobookshelf ebook library scan triggered")
    except Exception as e:
        logger.error(f"ABS ebook library scan failed: {e}")


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
        download_jobs[job_id]["status"] = "error"
        download_jobs[job_id]["error"] = "No pre-made EPUB found and lightnovel-crawler not configured"
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
            download_jobs[job_id]["detail"] = f"Importing to Calibre ({_human_size(epub_size)})..."

            if import_to_calibre(best_epub, title=title):
                download_jobs[job_id]["status"] = "completed"
                download_jobs[job_id]["detail"] = f"Done (scraped from {site_name}, {_human_size(epub_size)})"
                _clean_incoming()
                return
            else:
                logger.warning(f"[{title}] Calibre import failed for {site_name}")
                _clean_incoming()
                continue

        except subprocess.TimeoutExpired:
            logger.warning(f"[{title}] lncrawl timed out on {src_url}")
            _clean_incoming()
            continue
        except Exception as e:
            logger.warning(f"[{title}] lncrawl error on {src_url}: {e}")
            _clean_incoming()
            continue

    download_jobs[job_id]["status"] = "error"
    download_jobs[job_id]["error"] = f"All {min(len(source_urls),4)} sources failed"


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
        download_jobs[job_id]["detail"] = "Importing to Calibre..."

        if import_to_calibre(filepath, title=title):
            download_jobs[job_id]["status"] = "completed"
            download_jobs[job_id]["detail"] = f"Done ({_human_size(file_size)})"
            if os.path.exists(filepath):
                os.remove(filepath)
            return True
        else:
            # Even if calibre import fails, file was downloaded successfully
            download_jobs[job_id]["status"] = "completed"
            download_jobs[job_id]["detail"] = f"Downloaded ({_human_size(file_size)}) — Calibre import skipped"
            return True

    except Exception as e:
        logger.error(f"[Anna's] Download error for {title}: {e}")
        return False


def download_annas_worker(job_id, md5, title):
    download_jobs[job_id]["status"] = "downloading"
    if not _download_from_annas(job_id, md5, title):
        if download_jobs[job_id]["status"] != "completed":
            download_jobs[job_id]["status"] = "error"
            download_jobs[job_id]["error"] = download_jobs[job_id].get("error") or "Download failed"


# =============================================================================
# Background Auto-Import for Completed Torrents
# =============================================================================
import_event = threading.Event()
imported_hashes = set()


def import_completed_torrents():
    if not config.has_qbittorrent():
        return
    try:
        torrents = qb.get_torrents(category=config.QB_CATEGORY)
        for t in torrents:
            if t.get("progress", 0) >= 1.0 and t["hash"] not in imported_hashes:
                save_path = t.get("content_path", t.get("save_path", ""))
                if save_path.startswith("/books-incoming"):
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
                    import_to_calibre(bf)
                qb.delete_torrent(t["hash"], delete_files=True)
                logger.info(f"Removed completed torrent: {t.get('name', t['hash'])}")
                imported_hashes.add(t["hash"])
    except Exception as e:
        logger.error(f"Auto-import error: {e}")


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


@app.route("/api/config")
def api_config():
    return jsonify({
        "prowlarr": config.has_prowlarr(),
        "qbittorrent": config.has_qbittorrent(),
        "calibre": config.has_calibre(),
        "audiobookshelf": config.has_audiobookshelf(),
        "lncrawl": config.has_lncrawl(),
        "audiobooks": config.has_audiobooks(),
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
        futures = {executor.submit(s.search, query): s for s in enabled}
        for future in as_completed(futures, timeout=35):
            source = futures[future]
            try:
                results = future.result()
                for r in results:
                    r.setdefault("source", source.name)
                all_results.extend(results)
            except Exception as e:
                logger.error(f"Search error ({source.name}): {e}")

    all_results = filter_results(all_results, query)
    elapsed = int((time.time() - start) * 1000)
    return jsonify({
        "results": all_results,
        "search_time_ms": elapsed,
        "sources": sources.get_source_metadata(),
    })


@app.route("/api/download/torrent", methods=["POST"])
def api_download_torrent():
    if not config.has_qbittorrent():
        return jsonify({"success": False, "error": "qBittorrent not configured. Set QB_URL, QB_USER, QB_PASS."}), 400

    data = request.json
    url = data.get("download_url") or data.get("magnet_url", "")
    if not url and data.get("info_hash"):
        url = f"magnet:?xt=urn:btih:{data['info_hash']}"
    title = data.get("title", "Unknown")

    if not url:
        return jsonify({"success": False, "error": "No download URL"}), 400

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
        futures = {executor.submit(s.search, query): s for s in enabled}
        results = []
        for future in as_completed(futures, timeout=35):
            source = futures[future]
            try:
                batch = future.result()
                for r in batch:
                    r.setdefault("source", source.name)
                results.extend(batch)
            except Exception as e:
                logger.error(f"Audiobook search error ({source.name}): {e}")

    results = filter_results(results, query)
    elapsed = int((time.time() - start) * 1000)
    return jsonify({
        "results": results,
        "search_time_ms": elapsed,
        "sources": sources.get_source_metadata(),
    })


@app.route("/api/download/audiobook", methods=["POST"])
def api_download_audiobook():
    if not config.has_qbittorrent():
        return jsonify({"success": False, "error": "qBittorrent not configured. Set QB_URL, QB_USER, QB_PASS."}), 400

    data = request.json
    url = data.get("download_url") or data.get("magnet_url", "")
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

    success = qb.add_torrent(
        url, title,
        save_path=config.QB_AUDIOBOOK_SAVE_PATH,
        category=config.QB_AUDIOBOOK_CATEGORY,
    )
    if success:
        threading.Thread(target=watch_audiobook_torrent, args=(title,), daemon=True).start()
    return jsonify({"success": success, "title": title})


@app.route("/api/download/novel", methods=["POST"])
def api_download_novel():
    data = request.json
    url = data.get("url", "")
    title = data.get("title", "Unknown")

    if not url:
        return jsonify({"success": False, "error": "No URL"}), 400

    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = {
        "status": "queued",
        "title": title,
        "url": url,
        "source": "webnovel",
        "error": None,
        "detail": None,
    }

    thread = threading.Thread(
        target=download_novel_worker,
        args=(job_id, url, title),
        daemon=True,
    )
    thread.start()

    return jsonify({"success": True, "job_id": job_id, "title": title})


@app.route("/api/download/annas", methods=["POST"])
def api_download_annas():
    data = request.json
    md5 = data.get("md5", "")
    title = data.get("title", "Unknown")

    if not md5:
        return jsonify({"success": False, "error": "No MD5 hash"}), 400

    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = {
        "status": "queued",
        "title": title,
        "url": f"https://annas-archive.li/md5/{md5}",
        "source": "annas",
        "error": None,
        "detail": None,
    }

    thread = threading.Thread(
        target=download_annas_worker,
        args=(job_id, md5, title),
        daemon=True,
    )
    thread.start()

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


@app.route("/api/downloads/clear", methods=["POST"])
def api_clear_finished():
    to_remove = [jid for jid, j in download_jobs.items()
                 if j["status"] in ("completed", "error")]
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
    return jsonify(sources.get_source_metadata())


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

    # Torrent sources: send to qBittorrent
    if source.download_type == "torrent":
        if not config.has_qbittorrent():
            return jsonify({"success": False, "error": "qBittorrent not configured"}), 400

        url = data.get("download_url") or data.get("magnet_url", "")
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

        if source.search_tab == "audiobook":
            save_path = config.QB_AUDIOBOOK_SAVE_PATH
            category = config.QB_AUDIOBOOK_CATEGORY
            watch_fn = watch_audiobook_torrent
        else:
            save_path = config.QB_SAVE_PATH
            category = config.QB_CATEGORY
            watch_fn = watch_torrent

        success = qb.add_torrent(url, title, save_path=save_path, category=category)
        if success:
            threading.Thread(target=watch_fn, args=(title,), daemon=True).start()
        return jsonify({"success": success, "title": title})

    # Direct/custom sources: create a job and run in background thread
    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = {
        "status": "queued",
        "title": title,
        "source": source_name,
        "error": None,
        "detail": None,
    }

    def _run_download():
        download_jobs[job_id]["status"] = "downloading"
        try:
            success = source.download(data, download_jobs[job_id])
            if not success and download_jobs[job_id]["status"] != "completed":
                download_jobs[job_id]["status"] = "error"
                download_jobs[job_id]["error"] = download_jobs[job_id].get("error") or "Download failed"
        except Exception as e:
            logger.error(f"Download error ({source_name}): {e}")
            download_jobs[job_id]["status"] = "error"
            download_jobs[job_id]["error"] = str(e)

    threading.Thread(target=_run_download, daemon=True).start()
    return jsonify({"success": True, "job_id": job_id, "title": title})


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
        config.save_settings(data)
        # Re-authenticate qBittorrent if credentials changed
        qb.authenticated = False
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/test/prowlarr", methods=["POST"])
def api_test_prowlarr():
    data = request.json
    url = data.get("url", "").rstrip("/")
    api_key = data.get("api_key", "")
    if not url or not api_key:
        return jsonify({"success": False, "error": "URL and API key required"})
    try:
        resp = requests.get(
            f"{url}/api/v1/indexer",
            headers={"X-Api-Key": api_key},
            timeout=10,
        )
        if resp.status_code == 200:
            indexers = resp.json()
            count = len(indexers)
            return jsonify({"success": True, "message": f"Connected ({count} indexer{'s' if count != 1 else ''})"})
        elif resp.status_code == 401:
            return jsonify({"success": False, "error": "Invalid API key"})
        else:
            return jsonify({"success": False, "error": f"HTTP {resp.status_code}"})
    except requests.ConnectionError:
        return jsonify({"success": False, "error": "Connection refused — is Prowlarr running?"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/test/qbittorrent", methods=["POST"])
def api_test_qbittorrent():
    data = request.json
    url = data.get("url", "").rstrip("/")
    user = data.get("user", "admin")
    password = data.get("pass", "")
    if not url:
        return jsonify({"success": False, "error": "URL required"})
    try:
        session = requests.Session()
        resp = session.post(
            f"{url}/api/v2/auth/login",
            data={"username": user, "password": password},
            timeout=10,
        )
        if resp.text == "Ok.":
            # Get version
            ver_resp = session.get(f"{url}/api/v2/app/version", timeout=5)
            version = ver_resp.text if ver_resp.status_code == 200 else "unknown"
            return jsonify({"success": True, "message": f"Connected (v{version})"})
        else:
            return jsonify({"success": False, "error": "Login failed — check username/password"})
    except requests.ConnectionError:
        return jsonify({"success": False, "error": "Connection refused — is qBittorrent running?"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/test/audiobookshelf", methods=["POST"])
def api_test_audiobookshelf():
    data = request.json
    url = data.get("url", "").rstrip("/")
    token = data.get("token", "")
    if not url or not token:
        return jsonify({"success": False, "error": "URL and API token required"})
    try:
        resp = requests.get(
            f"{url}/api/libraries",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code == 200:
            libraries = resp.json().get("libraries", [])
            lib_info = ", ".join(f"{l['name']} ({l['id'][:8]}...)" for l in libraries[:5])
            return jsonify({
                "success": True,
                "message": f"Connected ({len(libraries)} libraries)",
                "libraries": [{"id": l["id"], "name": l["name"]} for l in libraries],
            })
        elif resp.status_code == 401:
            return jsonify({"success": False, "error": "Invalid API token"})
        else:
            return jsonify({"success": False, "error": f"HTTP {resp.status_code}"})
    except requests.ConnectionError:
        return jsonify({"success": False, "error": "Connection refused — is Audiobookshelf running?"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
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
    if integrations:
        logger.info(f"Integrations: {', '.join(integrations)}")

    # Start auto-import background thread if qBittorrent is configured
    if config.has_qbittorrent():
        import_thread = threading.Thread(target=auto_import_loop, daemon=True)
        import_thread.start()

    app.run(host="0.0.0.0", port=5000, debug=False)
