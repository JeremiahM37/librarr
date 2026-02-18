"""Open Library — public domain ebook downloads via Internet Archive."""
import logging
import os
import re

import requests

from .base import Source

logger = logging.getLogger("librarr")

SEARCH_URL = "https://openlibrary.org/search.json"
IA_DOWNLOAD_URL = "https://archive.org/download"
IA_METADATA_URL = "https://archive.org/metadata"
HEADERS = {"User-Agent": "Librarr/1.0 (book download manager; github.com/JeremiahM37/librarr)"}


class OpenLibrarySource(Source):
    name = "openlibrary"
    label = "Open Library"
    color = "#e67e22"
    download_type = "direct"

    def enabled(self):
        return True

    def search(self, query):
        results = []
        try:
            resp = requests.get(
                SEARCH_URL,
                params={
                    "q": query,
                    "fields": "key,title,author_name,ebook_access,ia,first_publish_year,cover_i",
                    "limit": 15,
                },
                headers=HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                return results

            data = resp.json()
            for doc in data.get("docs", []):
                # Only include public domain books we can actually download
                if doc.get("ebook_access") != "public":
                    continue

                ia_ids = doc.get("ia", [])
                if not ia_ids:
                    continue

                authors = doc.get("author_name", [])
                author = authors[0] if authors else ""
                year = doc.get("first_publish_year", "")
                cover_id = doc.get("cover_i")
                cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""

                results.append({
                    "title": doc.get("title", ""),
                    "author": author,
                    "source_id": f"ol-{doc['key']}",
                    "ia_ids": ia_ids[:5],  # Keep top 5 IA identifiers
                    "cover_url": cover_url,
                    "size_human": f"Public Domain{f' ({year})' if year else ''}",
                })
        except Exception as e:
            logger.error(f"Open Library search failed: {e}")
        return results

    def _find_epub_url(self, ia_id):
        """Check an IA item for an EPUB file, return download URL or None."""
        try:
            resp = requests.get(
                f"{IA_METADATA_URL}/{ia_id}/files",
                headers=HEADERS,
                timeout=10,
            )
            if resp.status_code != 200:
                return None
            files = resp.json().get("result", [])
            for f in files:
                name = f.get("name", "")
                if name.lower().endswith(".epub"):
                    return f"{IA_DOWNLOAD_URL}/{ia_id}/{name}"
        except Exception:
            pass
        # Try the conventional URL pattern as fallback
        return f"{IA_DOWNLOAD_URL}/{ia_id}/{ia_id}.epub"

    def download(self, result, job):
        import config
        import pipeline

        title = result.get("title", "Unknown")
        author = result.get("author", "")
        ia_ids = result.get("ia_ids", [])
        source_id = result.get("source_id", "")

        if not ia_ids:
            job["error"] = "No Internet Archive identifier"
            return False

        job["detail"] = "Finding EPUB on Internet Archive..."

        # Try each IA identifier until we get a working EPUB
        for ia_id in ia_ids:
            epub_url = self._find_epub_url(ia_id)
            if not epub_url:
                continue

            job["detail"] = f"Downloading from Internet Archive..."
            try:
                resp = requests.get(
                    epub_url,
                    headers=HEADERS,
                    timeout=(15, 180),
                    stream=True,
                    allow_redirects=True,
                )
                if resp.status_code != 200:
                    logger.warning(f"[OpenLibrary] HTTP {resp.status_code} for {ia_id}")
                    continue

                content_type = resp.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    # Got an HTML error page, not an EPUB
                    logger.warning(f"[OpenLibrary] HTML response for {ia_id}, skipping")
                    continue

                safe_title = re.sub(r'[^\w\s-]', '', title)[:80].strip() or "book"
                filepath = os.path.join(config.INCOMING_DIR, f"{safe_title}.epub")
                os.makedirs(config.INCOMING_DIR, exist_ok=True)

                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)

                file_size = os.path.getsize(filepath)
                if file_size < 1000:
                    # Too small — probably an error page saved as file
                    logger.warning(f"[OpenLibrary] File too small ({file_size}B) for {ia_id}")
                    os.remove(filepath)
                    continue

                job["detail"] = "Processing..."
                job["status"] = "importing"

                from app import library
                pipeline.run_pipeline(
                    filepath, title=title, author=author,
                    media_type="ebook", source="openlibrary",
                    source_id=source_id,
                    job_id=job._job_id, library_db=library,
                )

                size_mb = file_size / (1024 * 1024)
                job["status"] = "completed"
                job["detail"] = f"Done ({size_mb:.1f} MB)"
                return True

            except Exception as e:
                logger.warning(f"[OpenLibrary] Download failed for {ia_id}: {e}")
                continue

        job["error"] = "No downloadable EPUB found across available editions"
        return False
