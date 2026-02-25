from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed


class ProviderSearchService:
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    def __init__(self, *, config, logger, requests_module, human_size):
        self.config = config
        self.logger = logger
        self.requests = requests_module
        self.human_size = human_size
        self.abb_domains = [
            "https://audiobookbay.lu",
            "https://audiobookbay.is",
            "https://audiobookbay.li",
        ]
        self.abb_trackers = [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://open.stealth.si:80/announce",
            "udp://exodus.desync.com:6969/announce",
            "udp://tracker.torrent.eu.org:451/announce",
            "udp://tracker.tiny-vps.com:6969/announce",
            "udp://tracker.dler.org:6969/announce",
            "http://tracker.files.fm:6969/announce",
        ]
        self.abb_url = self.abb_domains[0]

    def rotate_abb_domain(self):
        if len(self.abb_domains) > 1:
            first = self.abb_domains.pop(0)
            self.abb_domains.append(first)
        self.abb_url = self.abb_domains[0] if self.abb_domains else None
        return self.abb_url

    def search_prowlarr(self, query):
        if not self.config.has_prowlarr():
            return []
        results = []
        try:
            resp = self.requests.get(
                f"{self.config.PROWLARR_URL}/api/v1/search",
                params={
                    "query": query,
                    "categories": [7000, 7020],
                    "type": "search",
                    "limit": 50,
                },
                headers={"X-Api-Key": self.config.PROWLARR_API_KEY},
                timeout=30,
            )
            for item in resp.json():
                size = item.get("size", 0)
                results.append({
                    "source": "torrent",
                    "title": item.get("title", ""),
                    "size": size,
                    "size_human": self.human_size(size),
                    "seeders": item.get("seeders", 0),
                    "leechers": item.get("leechers", 0),
                    "indexer": item.get("indexer", ""),
                    "download_url": item.get("downloadUrl", ""),
                    "magnet_url": item.get("magnetUrl", ""),
                    "info_hash": item.get("infoHash", ""),
                    "guid": item.get("guid", ""),
                })
        except Exception as e:
            self.logger.error("Prowlarr search failed: %s", e)
        return results

    def search_prowlarr_audiobooks(self, query):
        if not self.config.has_prowlarr():
            return []
        results = []
        seen_hashes = set()
        searches = [
            {"query": query, "categories": [3030], "type": "search", "limit": 50},
            {"query": f"{query} audiobook", "type": "search", "limit": 30},
        ]
        for params in searches:
            try:
                resp = self.requests.get(
                    f"{self.config.PROWLARR_URL}/api/v1/search",
                    params=params,
                    headers={"X-Api-Key": self.config.PROWLARR_API_KEY},
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
                        "size_human": self.human_size(size),
                        "seeders": item.get("seeders", 0),
                        "leechers": item.get("leechers", 0),
                        "indexer": item.get("indexer", ""),
                        "download_url": item.get("downloadUrl", ""),
                        "magnet_url": item.get("magnetUrl", ""),
                        "info_hash": ih,
                        "guid": item.get("guid", ""),
                    })
            except Exception as e:
                self.logger.error("Prowlarr audiobook search failed: %s", e)
        return results

    def get_abb_response(self, path, params=None, **kwargs):
        for domain in self.abb_domains:
            try:
                resp = self.requests.get(
                    f"{domain}{path}",
                    params=params,
                    headers={"User-Agent": self.USER_AGENT},
                    timeout=15,
                    **kwargs,
                )
                if resp.status_code == 200:
                    return resp, domain
            except Exception:
                continue
        return None, None

    def search_audiobookbay(self, query):
        results = []
        try:
            resp, _active_domain = self.get_abb_response("/", params={"s": query, "tt": "1"})
            if resp is None:
                return results
            content = resp.text[resp.text.find('<div id="content">'):]
            entries = re.findall(
                r'<h2[^>]*><a href="(/abss/[^"]+)"[^>]*>(.*?)</a></h2>'
                r'.*?<div class="postInfo">(.*?)</div>',
                content, re.DOTALL,
            )
            for url, title_raw, info_raw in entries:
                title = re.sub(r"<[^>]+>", "", title_raw).strip()
                if not title:
                    continue
                lang_m = re.search(r"Language:\s*(\w+)", info_raw)
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
            self.logger.error("AudioBookBay search failed: %s", e)
        return results

    def resolve_abb_magnet(self, abb_path):
        try:
            resp, _domain = self.get_abb_response(abb_path)
            if resp is None:
                return None
            hash_m = re.search(r"Info Hash:.*?<td[^>]*>\s*([0-9a-fA-F]{40})", resp.text, re.DOTALL)
            if not hash_m:
                return None
            info_hash = hash_m.group(1)
            trackers = re.findall(r"<td>((?:udp|http)://[^<]+)</td>", resp.text)
            if not trackers:
                trackers = self.abb_trackers
            tr_params = "&".join(f"tr={self.requests.utils.quote(t)}" for t in trackers)
            title_m = re.search(r"<h1[^>]*>(.*?)</h1>", resp.text)
            dn = self.requests.utils.quote(re.sub(r"<[^>]+>", "", title_m.group(1)).strip()) if title_m else ""
            return f"magnet:?xt=urn:btih:{info_hash}&dn={dn}&{tr_params}"
        except Exception as e:
            self.logger.error("ABB resolve failed: %s", e)
            return None

    def check_libgen_available(self, md5):
        try:
            resp = self.requests.get(
                f"https://libgen.li/ads.php?md5={md5}",
                headers={"User-Agent": self.USER_AGENT},
                timeout=10,
            )
            if resp.status_code != 200:
                return False
            get_match = re.search(r'href="(get\.php\?md5=[^"]+)"', resp.text)
            if not get_match:
                return False
            dl_url = f"https://libgen.li/{get_match.group(1)}"
            dl_resp = self.requests.get(
                dl_url,
                headers={"User-Agent": self.USER_AGENT},
                timeout=10,
                stream=True,
                allow_redirects=True,
            )
            ct = dl_resp.headers.get("Content-Type", "")
            dl_resp.close()
            if dl_resp.status_code >= 400:
                return False
            if "text/html" in ct:
                text = dl_resp.text if hasattr(dl_resp, "_content") else ""
                if "Error" in text or "not found" in text.lower():
                    return False
            return True
        except Exception:
            return False

    def search_annas_archive(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://annas-archive.li/search",
                params={"q": query, "ext": "epub"},
                headers={"User-Agent": self.USER_AGENT},
                timeout=15,
            )
            if resp.status_code != 200:
                return results

            html = resp.text
            blocks = re.findall(
                r'<div class="flex\s+pt-3 pb-3 border-b.*?">(.*?)(?=<div class="flex\s+pt-3 pb-3 border-b|<footer)',
                html,
                re.DOTALL,
            )
            candidates = []
            for block in blocks[:20]:
                md5 = re.search(r"/md5/([a-f0-9]+)", block)
                if not md5:
                    continue
                title_m = re.search(r"font-semibold text-lg[^>]*>(.*?)</a>", block, re.DOTALL)
                author_m = re.search(r"user-edit[^>]*></span>\s*(.*?)</a>", block, re.DOTALL)
                size = ""
                size_m = re.search(r"(\d+[\.\d]*\s*[KMG]i?B)", block)
                if size_m:
                    size = size_m.group(1)

                title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
                author = re.sub(r"<[^>]+>", "", author_m.group(1)).strip() if author_m else ""
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

            def _parse_size_bytes(s):
                if not s:
                    return 0
                m = re.match(r"([\d.]+)\s*(GB|MB|KB|B)", s.strip(), re.IGNORECASE)
                if not m:
                    return 0
                val = float(m.group(1))
                unit = m.group(2).upper()
                return val * {"GB": 1e9, "MB": 1e6, "KB": 1e3, "B": 1}.get(unit, 1)

            candidates.sort(key=lambda c: _parse_size_bytes(c.get("size_human", "")), reverse=True)

            if candidates:
                to_check = candidates[:20]
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = {executor.submit(self.check_libgen_available, c["md5"]): c for c in to_check}
                    for future in as_completed(futures, timeout=45):
                        try:
                            if future.result():
                                results.append(futures[future])
                        except Exception:
                            pass
                results.sort(key=lambda c: _parse_size_bytes(c.get("size_human", "")), reverse=True)
                if not results:
                    self.logger.info("Anna's Archive: all %s candidates for '%s' are dead on libgen", len(to_check), query)
        except Exception as e:
            self.logger.error("Anna's Archive search failed: %s", e)
        return results
