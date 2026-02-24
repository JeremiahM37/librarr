"""Standard Ebooks source — high-quality public domain ebooks.

Standard Ebooks (https://standardebooks.org) is a volunteer-run project that
produces carefully formatted, free public domain ebooks. No API key required.
"""
import re

import requests

from .base import Source


class StandardEbooksSource(Source):
    name = "standardebooks"
    label = "Standard Ebooks"
    color = "#dc2626"
    download_type = "direct"
    search_tab = "main"

    def enabled(self):
        return True

    def search(self, query):
        results = []
        try:
            resp = requests.get(
                "https://standardebooks.org/opds/all",
                headers={"User-Agent": "Librarr/1.0 (self-hosted book manager)"},
                timeout=20,
            )
            if resp.status_code != 200:
                return results

            # Parse Atom feed
            content = resp.text
            # Each entry is <entry>...</entry>
            entries = re.findall(r"<entry>(.*?)</entry>", content, re.DOTALL)

            q_words = set(re.findall(r"\w+", query.lower()))
            q_words -= {"the", "a", "an", "of", "in", "by", "and", "or"}

            for entry in entries:
                title_m = re.search(r"<title[^>]*>(.*?)</title>", entry, re.DOTALL)
                author_m = re.search(r"<author[^>]*>.*?<name>(.*?)</name>", entry, re.DOTALL)
                id_m = re.search(r"<id>(.*?)</id>", entry)
                cover_m = re.search(r'rel="http://opds-spec\.org/image"[^>]*href="([^"]+)"', entry)

                if not title_m or not id_m:
                    continue

                title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()
                author = re.sub(r"<[^>]+>", "", author_m.group(1)).strip() if author_m else ""
                book_id = id_m.group(1).strip()
                cover_url = cover_m.group(1) if cover_m else ""

                # Relevance: check if query words appear in title or author
                combined = (title + " " + author).lower()
                combined_words = set(re.findall(r"\w+", combined))
                if not q_words or not (q_words & combined_words):
                    continue

                # Derive the EPUB URL from the book's URL identifier
                # Standard Ebooks IDs look like: https://standardebooks.org/ebooks/author/title
                se_url = book_id if book_id.startswith("http") else f"https://standardebooks.org{book_id}"
                # The EPUB URL pattern: /ebooks/author/title/downloads/author_title.epub
                path = se_url.replace("https://standardebooks.org/ebooks/", "")
                epub_url = f"https://standardebooks.org/ebooks/{path}/downloads/{path.replace('/', '_')}.epub"

                results.append({
                    "title": title,
                    "author": author,
                    "size_human": "~1 MB",
                    "cover_url": cover_url,
                    "source_id": f"standardebooks-{path}",
                    "file_url": epub_url,
                    "file_ext": "epub",
                })
        except Exception as e:
            import logging
            logging.getLogger("librarr.sources.standardebooks").error(f"Standard Ebooks search failed: {e}")
        return results[:15]

    def download(self, result, job):
        import os
        import config

        url = result.get("file_url", "")
        title = result.get("title", "unknown")
        if not url:
            job["status"] = "error"
            job["error"] = "No download URL"
            return False

        job["detail"] = "Downloading from Standard Ebooks..."
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "Librarr/1.0"},
                timeout=60,
                stream=True,
            )
            if resp.status_code != 200:
                job["status"] = "error"
                job["error"] = f"HTTP {resp.status_code} from Standard Ebooks"
                return False

            os.makedirs(config.INCOMING_DIR, exist_ok=True)
            safe_title = re.sub(r'[^\w\s-]', '', title)[:80].strip()
            filepath = os.path.join(config.INCOMING_DIR, f"{safe_title}.epub")

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)

            size = os.path.getsize(filepath)
            if size < 10_000:
                os.remove(filepath)
                job["status"] = "error"
                job["error"] = "Downloaded file too small — likely an error page"
                return False

            # Run through the import pipeline
            import pipeline
            from library_db import LibraryDB
            from app import library
            pipeline.run_pipeline(
                filepath,
                title=title,
                author=result.get("author", ""),
                media_type="ebook",
                source="standardebooks",
                source_id=result.get("source_id", ""),
                library_db=library,
            )

            job["status"] = "completed"
            job["detail"] = f"Downloaded and imported ({size // 1024} KB)"
            return True

        except Exception as e:
            job["status"] = "error"
            job["error"] = str(e)
            return False
