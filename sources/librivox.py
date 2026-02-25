"""Librivox — free public domain audiobook downloads."""
import logging
import os
import re

import requests

from .base import Source

logger = logging.getLogger("librarr")

API_URL = "https://librivox.org/api/feed/audiobooks"
HEADERS = {"User-Agent": "Librarr/1.0 (book download manager)"}


class LibrivoxSource(Source):
    name = "librivox"
    label = "Librivox"
    color = "#00b894"
    download_type = "direct"
    search_tab = "audiobook"

    def enabled(self):
        return True

    def search(self, query):
        results = []
        # Librivox has no general search — try title first, then author
        for field in ("title", "author"):
            try:
                resp = requests.get(
                    API_URL,
                    params={
                        field: query,
                        "format": "json",
                        "extended": "1",
                        "coverart": "1",
                        "limit": "15",
                    },
                    headers=HEADERS,
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue

                data = resp.json()
                books = data.get("books", [])
                if not books:
                    continue

                for book in books:
                    authors = book.get("authors", [])
                    author_parts = []
                    for a in authors:
                        first = a.get("first_name", "")
                        last = a.get("last_name", "")
                        author_parts.append(f"{first} {last}".strip())
                    author = ", ".join(author_parts)

                    total_time = book.get("totaltime", "")
                    num_sections = book.get("num_sections", "")
                    zip_url = book.get("url_zip_file", "")

                    if not zip_url:
                        continue

                    cover = book.get("coverart_thumbnail", "") or book.get("coverart_jpg", "")

                    results.append({
                        "title": book.get("title", ""),
                        "author": author,
                        "source_id": f"librivox-{book.get('id', '')}",
                        "librivox_id": book.get("id", ""),
                        "zip_url": zip_url,
                        "librivox_url": book.get("url_librivox", ""),
                        "cover_url": cover,
                        "size_human": total_time or "Public Domain",
                        "chapters": num_sections,
                    })

                # If title search got results, don't also search by author
                if results:
                    break

            except Exception as e:
                logger.error(f"Librivox search ({field}) failed: {e}")

        return results

    def download(self, result, job):
        import config
        import pipeline

        title = result.get("title", "Unknown")
        author = result.get("author", "")
        zip_url = result.get("zip_url", "")
        source_id = result.get("source_id", "")

        if not zip_url:
            job["error"] = "No download URL"
            return False

        job["detail"] = "Downloading audiobook from Librivox..."

        try:
            resp = requests.get(
                zip_url,
                headers=HEADERS,
                timeout=(15, 600),  # Audiobook zips can be large
                stream=True,
                allow_redirects=True,
            )
            if resp.status_code != 200:
                job["error"] = f"HTTP {resp.status_code}"
                return False

            safe_author = re.sub(r'[^\w\s-]', '', author or "Unknown")[:40].strip()
            safe_title = re.sub(r'[^\w\s-]', '', title)[:80].strip() or "audiobook"

            # Save to audiobook directory in Author/Title structure
            dest_dir = os.path.join(config.AUDIOBOOK_DIR, safe_author, safe_title)
            os.makedirs(dest_dir, exist_ok=True)
            zip_path = os.path.join(dest_dir, f"{safe_title}.zip")

            total_size = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        pct = int(downloaded / total_size * 100)
                        size_mb = downloaded / (1024 * 1024)
                        job["detail"] = f"Downloading... {size_mb:.0f} MB ({pct}%)"

            file_size = os.path.getsize(zip_path)

            # Extract the zip
            job["detail"] = "Extracting MP3 files..."
            import zipfile
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(dest_dir)
                os.remove(zip_path)  # Remove zip after extraction
            except zipfile.BadZipFile:
                logger.warning(f"[Librivox] Bad zip file, keeping as-is: {zip_path}")

            job["detail"] = "Processing..."
            job["status"] = "importing"

            from app import library
            pipeline.run_pipeline(
                dest_dir, title=title, author=author,
                media_type="audiobook", source="librivox",
                source_id=source_id,
                job_id=job._job_id, library_db=library,
                target_names=job.get("target_names"),
            )

            size_mb = file_size / (1024 * 1024)
            job["status"] = "completed"
            job["detail"] = f"Done ({size_mb:.0f} MB)"
            return True

        except Exception as e:
            logger.error(f"Librivox download failed: {e}")
            job["error"] = str(e)
            return False
