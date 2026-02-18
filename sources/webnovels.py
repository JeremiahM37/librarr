"""Web novel sites — searches 7 novel sites in parallel (always available)."""
from .base import Source


class WebNovelSource(Source):
    name = "webnovel"
    label = "Web Novel"
    color = "#00b894"
    download_type = "custom"

    def enabled(self):
        return True  # Always available, no config needed

    def search(self, query):
        from app import search_webnovels
        return search_webnovels(query)

    def download(self, result, job):
        from app import download_novel_worker
        url = result.get("url", "")
        title = result.get("title", "Unknown")
        if not url:
            job["error"] = "No URL"
            return False
        download_novel_worker(job._job_id, url, title)
        return job["status"] == "completed"
