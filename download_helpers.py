from __future__ import annotations

import re
import time


_SUSPICIOUS_KEYWORDS = re.compile(
    r"\.(exe|msi|bat|scr|com|vbs|js|ps1|cmd)\b|password|keygen|crack|warez|DevCourseWeb",
    re.IGNORECASE,
)
_STOPWORDS = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "is", "it", "by"}


class DownloadHelpers:
    def __init__(self, *, config, qb, logger, sources, source_health, telemetry, library, pipeline_module):
        self.config = config
        self.qb = qb
        self.logger = logger
        self.sources = sources
        self.source_health = source_health
        self.telemetry = telemetry
        self.library = library
        self.pipeline = pipeline_module

    def source_health_metadata(self):
        meta = self.sources.get_source_metadata()
        health = self.source_health.snapshot()
        for name, source_meta in meta.items():
            h = health.get(name, {})
            source_meta["health"] = {
                "score": h.get("score", 100.0),
                "circuit_open": h.get("circuit_open", False),
                "circuit_retry_in_sec": h.get("circuit_retry_in_sec", 0),
                "search_fail_streak": h.get("search_fail_streak", 0),
                "last_error": h.get("last_error", ""),
            }
        return meta

    def search_source_safe(self, source, query):
        if not self.source_health.can_search(source.name):
            self.telemetry.metrics.inc("librarr_source_search_total", source=source.name, result="circuit_open")
            return []
        start = time.time()
        try:
            results = source.search(query) or []
            self.source_health.record_success(source.name, kind="search")
            self.telemetry.metrics.inc("librarr_source_search_total", source=source.name, result="ok")
            self.telemetry.metrics.inc(
                "librarr_source_search_results_total",
                source=source.name,
                result_count=str(min(len(results), 1000)),
            )
            return results
        except Exception as e:
            self.source_health.record_failure(source.name, e, kind="search")
            self.telemetry.metrics.inc("librarr_source_search_total", source=source.name, result="error")
            self.logger.error("Search error (%s): %s", source.name, e)
            return []
        finally:
            _ = int((time.time() - start) * 1000)
            self.telemetry.metrics.inc("librarr_source_search_calls_total", source=source.name)

    def record_source_download_result(self, source_name, ok, error=""):
        if ok:
            self.source_health.record_success(source_name, kind="download")
            self.telemetry.metrics.inc("librarr_source_download_total", source=source_name, result="ok")
            return
        self.source_health.record_failure(source_name, error or "download failed", kind="download")
        self.telemetry.metrics.inc("librarr_source_download_total", source=source_name, result="error")

    @staticmethod
    def truthy(v):
        if isinstance(v, bool):
            return v
        return str(v).lower() in ("1", "true", "yes", "on")

    @staticmethod
    def extract_download_source_id(data):
        if not isinstance(data, dict):
            return ""
        return data.get("source_id") or data.get("md5") or data.get("info_hash") or data.get("hash") or ""

    def duplicate_summary(self, title="", source_id=""):
        summary = {"duplicate": False, "by_source_id": False, "by_title": False, "matches": []}
        if source_id and self.library.has_source_id(source_id):
            summary["duplicate"] = True
            summary["by_source_id"] = True
            summary["matches"].append({"kind": "source_id", "value": source_id})
        if title:
            rows = self.library.find_by_title(title)
            if rows:
                summary["duplicate"] = True
                summary["by_title"] = True
                summary["matches"].append({"kind": "title", "count": len(rows)})
        return summary

    @staticmethod
    def parse_requested_targets(data):
        raw = (data or {}).get("target_names")
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = [t.strip() for t in raw.split(",") if t.strip()]
        if not isinstance(raw, list):
            return None
        return [str(t).strip() for t in raw if str(t).strip()]

    def download_preflight_response(self, data, source_name="", source_type=""):
        title = (data or {}).get("title", "Unknown")
        source_id = self.extract_download_source_id(data or {})
        duplicate = self.duplicate_summary(title=title, source_id=source_id)
        target_names = self.parse_requested_targets(data or {})
        return {
            "success": True,
            "dry_run": True,
            "title": title,
            "source": source_name or (data or {}).get("source", ""),
            "source_type": source_type,
            "source_id": source_id,
            "duplicate_check": duplicate,
            "target_names": target_names,
            "resolved_target_names": sorted(
                self.pipeline._resolve_target_names(
                    (data or {}).get("media_type", "ebook"),
                    source_name or (data or {}).get("source", ""),
                    target_names,
                )
            ),
            "qb": self.qb.diagnose() if source_type == "torrent" else None,
        }

    @staticmethod
    def title_relevant(query, title):
        q_words = set(re.findall(r"\w+", query.lower())) - _STOPWORDS
        t_words = set(re.findall(r"\w+", title.lower())) - _STOPWORDS
        if not q_words or not t_words:
            return False
        return len(q_words & t_words) >= 1

    def filter_results(self, results, query):
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
                if not self.title_relevant(query, title):
                    continue
                norm = re.sub(r"[^a-z0-9]", "", title.lower())[:60]
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
                if not self.title_relevant(query, title):
                    continue

            filtered.append(r)

        return filtered
