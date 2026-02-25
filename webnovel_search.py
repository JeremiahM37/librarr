from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser


class FreeWebNovelParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.results = []
        self._current = None
        self._in_title = False
        self._capture_text = False
        self._text_target = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "div" and "class" in attrs and "li-row" in attrs["class"]:
            self._current = {"source": "webnovel", "site": "FreeWebNovel"}
        if not self._current:
            return
        if tag == "a" and attrs.get("class", "").startswith("tit"):
            self._current["url"] = attrs.get("href", "")
            self._in_title = True
        if tag == "span" and "class" in attrs:
            cls = attrs["class"]
            if "s1" in cls:
                self._capture_text = True
                self._text_target = "author"
            elif "s2" in cls:
                self._capture_text = True
                self._text_target = "genre"

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


class WebNovelSearchService:
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    def __init__(self, *, requests_module, logger):
        self.requests = requests_module
        self.logger = logger

    def search_freewebnovel(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://freewebnovel.com/search/",
                params={"searchkey": query},
                headers={"User-Agent": self.USER_AGENT},
                timeout=15,
            )
            parser = FreeWebNovelParser()
            parser.feed(resp.text)
            results = parser.results
        except Exception as e:
            self.logger.error("FreeWebNovel search failed: %s", e)
        return results

    def search_allnovelfull(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://allnovelfull.net/search",
                params={"keyword": query},
                headers={"User-Agent": self.USER_AGENT},
                timeout=15,
            )
            pattern = r'<h3[^>]*class="[^"]*truyen-title[^"]*"[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, resp.text)
            for url, title in matches:
                if not url.startswith("http"):
                    url = "https://allnovelfull.net" + url
                results.append({"source": "webnovel", "site": "AllNovelFull", "title": title.strip(), "url": url})
        except Exception as e:
            self.logger.error("AllNovelFull search failed: %s", e)
        return results

    def search_boxnovel(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://boxnovel.com/",
                params={"s": query, "post_type": "wp-manga"},
                headers={"User-Agent": self.USER_AGENT},
                timeout=15,
            )
            pattern = r'<div class="post-title">\s*<h3[^>]*>\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, resp.text)
            for url, title in matches:
                results.append({"source": "webnovel", "site": "BoxNovel", "title": title.strip(), "url": url})
        except Exception as e:
            self.logger.error("BoxNovel search failed: %s", e)
        return results

    def search_novelbin(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://novelbin.me/search",
                params={"keyword": query},
                headers={"User-Agent": self.USER_AGENT},
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
                results.append({"source": "webnovel", "site": "NovelBin", "title": title.strip(), "url": url})
        except Exception as e:
            self.logger.error("NovelBin search failed: %s", e)
        return results

    def search_novelfull(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://novelfull.com/ajax/search-novel",
                params={"keyword": query},
                headers={"User-Agent": self.USER_AGENT, "X-Requested-With": "XMLHttpRequest"},
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
                results.append({"source": "webnovel", "site": "NovelFull", "title": title.strip(), "url": url})
        except Exception as e:
            self.logger.error("NovelFull search failed: %s", e)
        return results

    def search_lightnovelpub(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://www.lightnovelpub.com/lnwsearchlive",
                params={"inputContent": query},
                headers={"User-Agent": self.USER_AGENT, "X-Requested-With": "XMLHttpRequest"},
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
                    results.append({"source": "webnovel", "site": "LightNovelPub", "title": title.strip(), "url": url})
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
            self.logger.error("LightNovelPub search failed: %s", e)
        return results

    def search_readnovelfull(self, query):
        results = []
        try:
            resp = self.requests.get(
                "https://readnovelfull.com/ajax/search-novel",
                params={"keyword": query},
                headers={"User-Agent": self.USER_AGENT, "X-Requested-With": "XMLHttpRequest"},
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
                results.append({"source": "webnovel", "site": "ReadNovelFull", "title": title.strip(), "url": url})
        except Exception as e:
            self.logger.error("ReadNovelFull search failed: %s", e)
        return results

    def search_webnovels(self, query):
        all_results = []
        searchers = [
            self.search_allnovelfull,
            self.search_readnovelfull,
            self.search_novelfull,
            self.search_freewebnovel,
            self.search_novelbin,
            self.search_lightnovelpub,
            self.search_boxnovel,
        ]
        with ThreadPoolExecutor(max_workers=7) as executor:
            futures = {executor.submit(fn, query): fn.__name__ for fn in searchers}
            for future in as_completed(futures, timeout=20):
                try:
                    all_results.extend(future.result())
                except Exception as e:
                    self.logger.error("Web novel search error (%s): %s", futures[future], e)

        site_priority = [
            "AllNovelFull", "ReadNovelFull", "NovelFull",
            "FreeWebNovel", "NovelBin", "LightNovelPub", "BoxNovel",
        ]
        grouped = {}
        for r in all_results:
            key = re.sub(r"[^a-z0-9]", "", r["title"].lower())
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

        stopwords = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "is", "it", "by"}
        q_words = set(re.findall(r"\w+", query.lower())) - stopwords
        filtered = []
        for r in grouped.values():
            t_words = set(re.findall(r"\w+", r["title"].lower())) - stopwords
            if q_words and t_words and len(q_words & t_words) >= 1:
                filtered.append(r)
        return filtered
