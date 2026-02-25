from __future__ import annotations

import glob
import os
import re
import subprocess


class NovelAnnasWorkers:
    def __init__(
        self,
        *,
        config,
        logger,
        requests_module,
        pipeline_module,
        library,
        download_jobs,
        schedule_or_dead_letter,
        search_annas_archive,
        search_webnovels,
        human_size,
    ):
        self.config = config
        self.logger = logger
        self.requests = requests_module
        self.pipeline = pipeline_module
        self.library = library
        self.download_jobs = download_jobs
        self.schedule_or_dead_letter = schedule_or_dead_letter
        self.search_annas_archive = search_annas_archive
        self.search_webnovels = search_webnovels
        self.human_size = human_size

    def clean_incoming(self):
        incoming = self.config.INCOMING_DIR
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

    def download_novel_worker(self, job_id, url, title):
        self.download_jobs[job_id]["status"] = "downloading"

        self.download_jobs[job_id]["detail"] = "Checking Anna's Archive for pre-made EPUB..."
        self.logger.info("[%s] Checking Anna's Archive first...", title)
        try:
            annas_results = self.search_annas_archive(title)
            if annas_results:
                def _parse_size_mb(s):
                    if not s:
                        return 0
                    s = s.strip().upper()
                    m = re.match(r"([\d.]+)\s*(GB|MB|KB|B)", s)
                    if not m:
                        return 0
                    val = float(m.group(1))
                    unit = m.group(2)
                    return val * {"GB": 1024, "MB": 1, "KB": 1 / 1024, "B": 1 / (1024 * 1024)}.get(unit, 0)

                def _title_match(query, candidate_title):
                    q = query.lower().strip()
                    c = candidate_title.lower().strip()
                    if q in c or c in q:
                        return True
                    stopwords = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "is", "it", "by"}
                    q_words = set(re.findall(r"\w+", q)) - stopwords
                    c_words = set(re.findall(r"\w+", c)) - stopwords
                    if not q_words:
                        return False
                    overlap = len(q_words & c_words) / len(q_words)
                    return overlap >= 0.8

                matched = [r for r in annas_results if _title_match(title, r["title"])]
                if matched:
                    matched.sort(key=lambda r: _parse_size_mb(r.get("size_human", "")), reverse=True)
                    for i, candidate in enumerate(matched[:3]):
                        self.logger.info(
                            "[%s] Trying Anna's Archive #%s: %s (%s)",
                            title,
                            i + 1,
                            candidate["title"],
                            candidate.get("size_human", "?"),
                        )
                        self.download_jobs[job_id]["detail"] = (
                            f"Found EPUB on Anna's Archive ({candidate.get('size_human', '?')})! Downloading..."
                        )
                        if self.download_from_annas(job_id, candidate["md5"], title):
                            return
                        self.logger.info("[%s] Anna's Archive candidate #%s failed, trying next...", title, i + 1)
                else:
                    self.logger.info("[%s] Anna's Archive had results but none matched title", title)
            else:
                self.logger.info("[%s] Not found on Anna's Archive", title)
        except Exception as e:
            self.logger.error("[%s] Anna's Archive check failed: %s", title, e)

        if not self.config.has_lncrawl():
            self.schedule_or_dead_letter(
                job_id,
                "No pre-made EPUB found and lightnovel-crawler not configured",
                retry_kind="novel",
                retry_payload={"url": url, "title": title},
            )
            return

        source_urls = [url]
        try:
            self.download_jobs[job_id]["detail"] = "Finding best source for scraping..."
            alt_results = self.search_webnovels(title)
            for r in alt_results:
                if r.get("url") and r["url"] != url and r["url"] not in source_urls:
                    source_urls.append(r["url"])
        except Exception:
            pass

        self.download_jobs[job_id]["detail"] = "No pre-made EPUB found. Scraping chapters..."
        self.download_jobs[job_id]["status"] = "downloading"

        for src_idx, src_url in enumerate(source_urls[:4]):
            try:
                self.clean_incoming()
                subprocess.run(
                    ["docker", "exec", "-u", "root", self.config.LNCRAWL_CONTAINER, "chmod", "-R", "777", "/output"],
                    capture_output=True,
                    timeout=10,
                )

                site_name = src_url.split("//")[-1].split("/")[0]
                self.download_jobs[job_id]["detail"] = f"Scraping from {site_name} ({src_idx+1}/{min(len(source_urls),4)})..."
                self.logger.info("[%s] Starting lncrawl from %s", title, src_url)

                subprocess.run(
                    [
                        "docker", "exec", self.config.LNCRAWL_CONTAINER,
                        "python3", "-m", "lncrawl",
                        "-s", src_url,
                        "--all",
                        "--noin", "--suppress",
                        "-o", "/output",
                        "--format", "epub",
                    ],
                    capture_output=True, text=True, timeout=7200,
                )

                epubs = glob.glob(os.path.join(self.config.INCOMING_DIR, "**", "*.epub"), recursive=True)
                if not epubs:
                    self.logger.warning("[%s] lncrawl produced no EPUB from %s", title, site_name)
                    continue

                valid_epubs = []
                for ep in epubs:
                    try:
                        import zipfile
                        with zipfile.ZipFile(ep, "r") as zf:
                            zf.testzip()
                        valid_epubs.append(ep)
                    except Exception:
                        self.logger.warning("[%s] Corrupt EPUB: %s", title, os.path.basename(ep))

                if not valid_epubs:
                    self.logger.warning("[%s] All %s EPUBs from %s are corrupt", title, len(epubs), site_name)
                    self.clean_incoming()
                    continue

                valid_epubs.sort(key=lambda p: os.path.getsize(p), reverse=True)
                best_epub = valid_epubs[0]
                epub_size = os.path.getsize(best_epub)
                self.logger.info(
                    "[%s] lncrawl produced %s valid EPUBs from %s, largest: %s",
                    title,
                    len(valid_epubs),
                    site_name,
                    self.human_size(epub_size),
                )

                if epub_size < 500_000:
                    self.logger.warning(
                        "[%s] EPUB too small (%s) from %s, trying next source",
                        title,
                        self.human_size(epub_size),
                        site_name,
                    )
                    self.clean_incoming()
                    continue

                self.download_jobs[job_id]["status"] = "importing"
                self.download_jobs[job_id]["detail"] = f"Processing ({self.human_size(epub_size)})..."

                self.pipeline.run_pipeline(
                    best_epub,
                    title=title,
                    media_type="ebook",
                    source="webnovel",
                    source_id=url,
                    job_id=job_id,
                    library_db=self.library,
                    target_names=self.download_jobs[job_id].get("target_names"),
                )
                self.download_jobs[job_id]["status"] = "completed"
                self.download_jobs[job_id]["detail"] = f"Done (scraped from {site_name}, {self.human_size(epub_size)})"
                self.clean_incoming()
                return

            except subprocess.TimeoutExpired:
                self.logger.warning("[%s] lncrawl timed out on %s", title, src_url)
                self.clean_incoming()
                continue
            except Exception as e:
                if "Post-import verification failed" in str(e):
                    self.schedule_or_dead_letter(
                        job_id,
                        str(e),
                        retry_kind="novel",
                        retry_payload={"url": url, "title": title},
                    )
                    self.clean_incoming()
                    return
                self.logger.warning("[%s] lncrawl error on %s: %s", title, src_url, e)
                self.clean_incoming()
                continue

        self.schedule_or_dead_letter(
            job_id,
            f"All {min(len(source_urls),4)} sources failed",
            retry_kind="novel",
            retry_payload={"url": url, "title": title},
        )

    def try_download_url(self, url, job_id, connect_timeout=15, read_timeout=120):
        try:
            dl_resp = self.requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=(connect_timeout, read_timeout),
                stream=True,
                allow_redirects=True,
            )
            content_type = dl_resp.headers.get("Content-Type", "")
            if dl_resp.status_code >= 400:
                self.logger.warning("[Anna's] HTTP %s from %s", dl_resp.status_code, url[:60])
                return None, None

            if dl_resp.status_code == 200 and "text/html" in content_type:
                page_html = dl_resp.text
                if "File not found" in page_html or "Error</h1>" in page_html:
                    self.logger.warning("[Anna's] Error page from %s", url[:60])
                    return None, None
                get_link = re.search(r'href=\"(https?://[^\"]+)\"[^>]*>GET</a>', page_html)
                if not get_link:
                    get_link = re.search(r'<a\\s+href=\"(https?://[^\"]+)\"[^>]*>\\s*GET\\s*</a>', page_html, re.IGNORECASE)
                if not get_link:
                    get_link = re.search(r'href=\"(https?://[^\"]*\\.(epub|pdf|mobi)[^\"]*)\"', page_html)
                if get_link:
                    self.logger.info("[Anna's] Following: %s", get_link.group(1))
                    dl_resp = self.requests.get(
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

            title = self.download_jobs[job_id]["title"]
            safe_title = re.sub(r"[^\w\s-]", "", title)[:80].strip()
            filepath = os.path.join(self.config.INCOMING_DIR, f"{safe_title}.epub")

            total_size = int(dl_resp.headers.get("Content-Length", 0))
            downloaded = 0
            with open(filepath, "wb") as f:
                for chunk in dl_resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        pct = int(downloaded * 100 / total_size)
                        self.download_jobs[job_id]["detail"] = (
                            f"Downloading... {pct}% ({self.human_size(downloaded)} / {self.human_size(total_size)})"
                        )
                    else:
                        self.download_jobs[job_id]["detail"] = f"Downloading... {self.human_size(downloaded)}"

            file_size = os.path.getsize(filepath)
            if file_size < 1000:
                os.remove(filepath)
                return None, None

            return filepath, file_size
        except Exception as e:
            self.logger.error("[Anna's] Download attempt failed for %s: %s", url, e)
            return None, None

    def download_from_annas(self, job_id, md5, title):
        try:
            self.download_jobs[job_id]["detail"] = "Fetching download link from Anna's Archive..."
            ads_resp = self.requests.get(
                f"https://libgen.li/ads.php?md5={md5}",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=15,
            )

            download_url = None
            if ads_resp.status_code == 200:
                get_match = re.search(r'href=\"(get\\.php\\?md5=[^\"]+)\"', ads_resp.text)
                if get_match:
                    download_url = f"https://libgen.li/{get_match.group(1)}"
                    self.logger.info("[Anna's] Found libgen GET link for %s", title)

            if not download_url:
                self.download_jobs[job_id]["detail"] = "Trying alternative mirrors..."
                resp = self.requests.get(
                    f"https://annas-archive.li/md5/{md5}",
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=15,
                )
                if resp.status_code == 200:
                    for m in re.finditer(r'href=\"(https?://libgen\\.li/file\\.php\\?id=\\d+)\"', resp.text):
                        download_url = m.group(1)
                        break

            if not download_url:
                return False

            self.download_jobs[job_id]["detail"] = "Downloading EPUB..."
            self.logger.info("[Anna's] Downloading %s from %s", title, download_url)
            filepath, file_size = self.try_download_url(download_url, job_id, connect_timeout=15, read_timeout=300)
            if not filepath:
                return False

            self.logger.info("[Anna's] Downloaded %s: %s", title, self.human_size(file_size))
            self.download_jobs[job_id]["status"] = "importing"
            self.download_jobs[job_id]["detail"] = "Processing..."

            self.pipeline.run_pipeline(
                filepath,
                title=title,
                media_type="ebook",
                source="annas",
                source_id=md5,
                job_id=job_id,
                library_db=self.library,
                target_names=self.download_jobs[job_id].get("target_names"),
            )
            self.download_jobs[job_id]["status"] = "completed"
            self.download_jobs[job_id]["detail"] = f"Done ({self.human_size(file_size)})"
            return True
        except Exception as e:
            self.logger.error("[Anna's] Download error for %s: %s", title, e)
            try:
                self.download_jobs[job_id]["error"] = str(e)
            except Exception:
                pass
            return False

    def download_annas_worker(self, job_id, md5, title):
        self.download_jobs[job_id]["status"] = "downloading"
        if not self.download_from_annas(job_id, md5, title):
            if self.download_jobs[job_id]["status"] != "completed":
                self.schedule_or_dead_letter(
                    job_id,
                    self.download_jobs[job_id].get("error") or "Download failed",
                    retry_kind="annas",
                    retry_payload={"md5": md5, "title": title},
                )
