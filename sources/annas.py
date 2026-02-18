"""Anna's Archive — direct EPUB download source (always available)."""
from .base import Source


class AnnasArchiveSource(Source):
    name = "annas"
    label = "Anna's Archive"
    color = "#6c5ce7"
    download_type = "direct"

    def enabled(self):
        return True  # Always available, no config needed

    def search(self, query):
        from app import search_annas_archive
        return search_annas_archive(query)

    def download(self, result, job):
        from app import _download_from_annas
        md5 = result.get("md5", "")
        title = result.get("title", "Unknown")
        if not md5:
            job["error"] = "No MD5 hash"
            return False
        return _download_from_annas(job._job_id, md5, title)
