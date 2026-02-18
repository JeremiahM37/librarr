"""Project Gutenberg — public domain ebook downloads via Gutendex API."""
import logging
import os
import re

import requests

from .base import Source

logger = logging.getLogger("librarr")

GUTENDEX_URL = "https://gutendex.com/books"


class GutenbergSource(Source):
    name = "gutenberg"
    label = "Gutenberg"
    color = "#e84393"
    download_type = "direct"

    def enabled(self):
        return True

    def search(self, query):
        results = []
        try:
            resp = requests.get(
                GUTENDEX_URL,
                params={
                    "search": query,
                    "languages": "en",
                    "mime_type": "application/epub+zip",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                return results

            data = resp.json()
            for book in data.get("results", [])[:10]:
                epub_url = book.get("formats", {}).get("application/epub+zip", "")
                if not epub_url:
                    continue
                authors = book.get("authors", [])
                author = authors[0]["name"] if authors else ""
                # Gutenberg uses "Last, First" — flip it
                if ", " in author:
                    parts = author.split(", ", 1)
                    author = f"{parts[1]} {parts[0]}"

                cover_url = book.get("formats", {}).get("image/jpeg", "")

                results.append({
                    "title": book.get("title", ""),
                    "author": author,
                    "source_id": f"gutenberg-{book['id']}",
                    "gutenberg_id": book["id"],
                    "epub_url": epub_url,
                    "cover_url": cover_url,
                    "size_human": "Public Domain",
                    "download_count": book.get("download_count", 0),
                })
        except Exception as e:
            logger.error(f"Gutenberg search failed: {e}")
        return results

    def download(self, result, job):
        import config
        import pipeline

        title = result.get("title", "Unknown")
        author = result.get("author", "")
        epub_url = result.get("epub_url", "")
        source_id = result.get("source_id", "")

        if not epub_url:
            job["error"] = "No EPUB URL"
            return False

        job["detail"] = "Downloading from Project Gutenberg..."

        try:
            resp = requests.get(
                epub_url,
                headers={"User-Agent": "Librarr/1.0 (book download manager)"},
                timeout=(15, 120),
                stream=True,
                allow_redirects=True,
            )
            if resp.status_code != 200:
                job["error"] = f"HTTP {resp.status_code}"
                return False

            safe_title = re.sub(r'[^\w\s-]', '', title)[:80].strip() or "book"
            filepath = os.path.join(config.INCOMING_DIR, f"{safe_title}.epub")
            os.makedirs(config.INCOMING_DIR, exist_ok=True)

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)

            file_size = os.path.getsize(filepath)
            job["detail"] = "Processing..."
            job["status"] = "importing"

            from app import library
            pipeline.run_pipeline(
                filepath, title=title, author=author,
                media_type="ebook", source="gutenberg",
                source_id=source_id,
                job_id=job._job_id, library_db=library,
            )

            size_mb = file_size / (1024 * 1024)
            job["status"] = "completed"
            job["detail"] = f"Done ({size_mb:.1f} MB)"
            return True

        except Exception as e:
            logger.error(f"Gutenberg download failed: {e}")
            job["error"] = str(e)
            return False
