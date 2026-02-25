from __future__ import annotations

import glob
import os
import threading
import time


class TorrentImportWorkers:
    def __init__(
        self,
        *,
        config,
        logger,
        qb,
        pipeline_module,
        library,
        requests_module,
        read_audio_metadata,
    ):
        self.config = config
        self.logger = logger
        self.qb = qb
        self.pipeline = pipeline_module
        self.library = library
        self.requests = requests_module
        self.read_audio_metadata = read_audio_metadata
        self.import_event = threading.Event()
        self.imported_hashes = set()
        self._imported_hashes_lock = threading.Lock()

    def import_completed_torrents(self):
        if not self.config.has_qbittorrent():
            return
        try:
            torrents = self.qb.get_torrents(category=self.config.QB_CATEGORY)
            for t in torrents:
                if t.get("progress", 0) < 1.0:
                    continue
                with self._imported_hashes_lock:
                    if t["hash"] in self.imported_hashes:
                        continue
                    self.imported_hashes.add(t["hash"])
                save_path = t.get("content_path", t.get("save_path", ""))
                if not os.path.exists(save_path):
                    qb_save = self.config.QB_SAVE_PATH.rstrip("/")
                    local_incoming = self.config.INCOMING_DIR.rstrip("/")
                    if qb_save and save_path.startswith(qb_save):
                        save_path = local_incoming + save_path[len(qb_save):]
                    elif save_path.startswith("/books-incoming"):
                        save_path = save_path.replace("/books-incoming", self.config.INCOMING_DIR, 1)
                book_files = []
                for ext in ("*.epub", "*.mobi", "*.pdf", "*.azw3"):
                    if os.path.isdir(save_path):
                        book_files.extend(glob.glob(os.path.join(save_path, "**", ext), recursive=True))
                    elif save_path.lower().endswith(ext[1:]):
                        book_files.append(save_path)
                for bf in book_files:
                    self.pipeline.run_pipeline(
                        bf,
                        title=t.get("name", ""),
                        media_type="ebook",
                        source="torrent",
                        source_id=t["hash"],
                        library_db=self.library,
                    )
                self.qb.delete_torrent(t["hash"], delete_files=True)
                self.logger.info("Removed completed torrent: %s", t.get("name", t["hash"]))
        except Exception as e:
            self.logger.error("Auto-import error: %s", e)

        try:
            torrents = self.qb.get_torrents(category=self.config.QB_AUDIOBOOK_CATEGORY)
            for t in torrents:
                if t.get("progress", 0) >= 1.0 and t["hash"] not in self.imported_hashes:
                    save_path = t.get("content_path", t.get("save_path", ""))
                    qb_ab_path = self.config.QB_AUDIOBOOK_SAVE_PATH.rstrip("/")
                    if qb_ab_path and save_path.startswith(qb_ab_path):
                        save_path = self.config.AUDIOBOOK_DIR + save_path[len(qb_ab_path):]

                    already_organised = (
                        self.config.FILE_ORG_ENABLED
                        and self.config.AUDIOBOOK_ORGANIZED_DIR
                        and os.path.abspath(save_path).startswith(os.path.abspath(self.config.AUDIOBOOK_ORGANIZED_DIR))
                    )

                    if not already_organised and self.config.FILE_ORG_ENABLED:
                        author, resolved_title = "", t.get("name", "")
                        if os.path.isfile(save_path):
                            resolved_title = os.path.splitext(os.path.basename(save_path))[0]
                        if not author and os.path.exists(save_path):
                            id3_author, id3_title = self.read_audio_metadata(save_path)
                            if id3_author or id3_title:
                                author = id3_author or author
                                resolved_title = id3_title or resolved_title
                        if not author and " - " in resolved_title:
                            parts = resolved_title.split(" - ", 1)
                            author, resolved_title = parts[0].strip(), parts[1].strip()
                        if not author:
                            author = "Unknown"
                        self.pipeline.run_pipeline(
                            save_path,
                            title=resolved_title,
                            author=author,
                            media_type="audiobook",
                            source="torrent",
                            source_id=t["hash"],
                            library_db=self.library,
                        )
                    elif already_organised:
                        self.logger.info("Audiobook already in organised directory, skipping pipeline: %s", save_path)

                    if self.config.has_audiobookshelf() and self.config.ABS_LIBRARY_ID:
                        known_ids = set()
                        try:
                            resp = self.requests.get(
                                f"{self.config.ABS_URL}/api/libraries/{self.config.ABS_LIBRARY_ID}/items",
                                params={"limit": 500},
                                headers={"Authorization": f"Bearer {self.config.ABS_TOKEN}"},
                                timeout=15,
                            )
                            known_ids = {i["id"] for i in resp.json().get("results", [])}
                        except Exception:
                            pass
                        try:
                            self.requests.post(
                                f"{self.config.ABS_URL}/api/libraries/{self.config.ABS_LIBRARY_ID}/scan",
                                headers={"Authorization": f"Bearer {self.config.ABS_TOKEN}"},
                                timeout=10,
                            )
                            self.logger.info("Audiobookshelf library scan triggered")
                        except Exception as e:
                            self.logger.error("Audiobookshelf scan failed: %s", e)
                        time.sleep(20)
                        self.abs_match_new_items(known_ids)

                    self.qb.delete_torrent(t["hash"], delete_files=True)
                    self.logger.info("Removed completed audiobook torrent: %s", t.get("name", t["hash"]))
                    self.imported_hashes.add(t["hash"])
        except Exception as e:
            self.logger.error("Audiobook auto-import error: %s", e)

        if self.config.AUDIOBOOK_DIR and os.path.isdir(self.config.AUDIOBOOK_DIR):
            try:
                active_paths = set()
                if self.config.has_qbittorrent():
                    for cat in (self.config.QB_CATEGORY, self.config.QB_AUDIOBOOK_CATEGORY):
                        try:
                            for t in self.qb.get_torrents(category=cat):
                                cp = t.get("content_path", t.get("save_path", ""))
                                qb_ab_path = self.config.QB_AUDIOBOOK_SAVE_PATH.rstrip("/")
                                if qb_ab_path and cp.startswith(qb_ab_path):
                                    cp = self.config.AUDIOBOOK_DIR + cp[len(qb_ab_path):]
                                active_paths.add(os.path.abspath(cp))
                        except Exception:
                            pass

                for entry in os.listdir(self.config.AUDIOBOOK_DIR):
                    entry_path = os.path.join(self.config.AUDIOBOOK_DIR, entry)
                    abs_entry = os.path.abspath(entry_path)
                    if abs_entry in active_paths or abs_entry in self.imported_hashes:
                        continue
                    if (
                        self.config.FILE_ORG_ENABLED
                        and self.config.AUDIOBOOK_ORGANIZED_DIR
                        and abs_entry.startswith(os.path.abspath(self.config.AUDIOBOOK_ORGANIZED_DIR))
                    ):
                        continue

                    audio_exts = (".mp3", ".m4b", ".m4a", ".flac", ".ogg", ".opus")
                    has_audio = False
                    if os.path.isdir(entry_path):
                        for root, _dirs, files in os.walk(entry_path):
                            if any(f.lower().endswith(audio_exts) for f in files):
                                has_audio = True
                                break
                    elif any(entry.lower().endswith(ext) for ext in audio_exts):
                        has_audio = True
                    if not has_audio:
                        continue

                    author, resolved_title = "", entry
                    if os.path.isfile(entry_path):
                        resolved_title = os.path.splitext(entry)[0]
                    id3_author, id3_title = self.read_audio_metadata(entry_path)
                    if id3_author or id3_title:
                        author = id3_author or author
                        resolved_title = id3_title or resolved_title
                    if not author and " - " in resolved_title:
                        parts = resolved_title.split(" - ", 1)
                        author, resolved_title = parts[0].strip(), parts[1].strip()
                    if not author:
                        author = "Unknown"

                    self.logger.info("Folder-scan importing audiobook: %s", entry)
                    self.pipeline.run_pipeline(
                        entry_path,
                        title=resolved_title,
                        author=author,
                        media_type="audiobook",
                        source="folder-scan",
                        source_id=abs_entry,
                        library_db=self.library,
                    )
                    self.imported_hashes.add(abs_entry)

                    if self.config.has_audiobookshelf() and self.config.ABS_LIBRARY_ID:
                        try:
                            self.requests.post(
                                f"{self.config.ABS_URL}/api/libraries/{self.config.ABS_LIBRARY_ID}/scan",
                                headers={"Authorization": f"Bearer {self.config.ABS_TOKEN}"},
                                timeout=10,
                            )
                        except Exception:
                            pass
                        time.sleep(20)
                        self.abs_match_new_items(set())
            except Exception as e:
                self.logger.error("Audiobook folder-scan error: %s", e)

    def auto_import_loop(self):
        while True:
            self.import_event.wait(timeout=10)
            self.import_event.clear()
            self.import_completed_torrents()

    def watch_torrent(self, title):
        self.logger.info("Watching torrent: %s", title)
        while True:
            try:
                torrents = self.qb.get_torrents(category=self.config.QB_CATEGORY)
                for t in torrents:
                    if t.get("name") == title or t.get("progress", 0) >= 1.0:
                        if t.get("progress", 0) >= 1.0:
                            self.import_event.set()
                            return
                time.sleep(5)
            except Exception:
                time.sleep(5)

    def abs_match_new_items(self, known_ids):
        if not self.config.has_audiobookshelf() or not self.config.ABS_LIBRARY_ID:
            return
        try:
            resp = self.requests.get(
                f"{self.config.ABS_URL}/api/libraries/{self.config.ABS_LIBRARY_ID}/items",
                params={"limit": 100},
                headers={"Authorization": f"Bearer {self.config.ABS_TOKEN}"},
                timeout=15,
            )
            for item in resp.json().get("results", []):
                item_id = item["id"]
                if item_id in known_ids:
                    continue
                title = item.get("media", {}).get("metadata", {}).get("title", "")
                author = item.get("media", {}).get("metadata", {}).get("authorName", "")
                try:
                    self.requests.post(
                        f"{self.config.ABS_URL}/api/items/{item_id}/match",
                        headers={
                            "Authorization": f"Bearer {self.config.ABS_TOKEN}",
                            "Content-Type": "application/json",
                        },
                        json={"provider": "audible"},
                        timeout=15,
                    )
                    self.logger.info("ABS auto-matched: %s by %s", title, author)
                except Exception as e:
                    self.logger.error("ABS match failed for %s: %s", title, e)
        except Exception as e:
            self.logger.error("ABS match scan failed: %s", e)

    def watch_audiobook_torrent(self, title):
        self.logger.info("Watching audiobook torrent: %s", title)
        while True:
            try:
                torrents = self.qb.get_torrents(category=self.config.QB_AUDIOBOOK_CATEGORY)
                for t in torrents:
                    if t.get("name") == title and t.get("progress", 0) >= 1.0:
                        self.logger.info("Audiobook torrent completed: %s", title)
                        save_path = t.get("content_path", t.get("save_path", ""))
                        qb_ab_path = self.config.QB_AUDIOBOOK_SAVE_PATH.rstrip("/")
                        if qb_ab_path and save_path.startswith(qb_ab_path):
                            save_path = self.config.AUDIOBOOK_DIR + save_path[len(qb_ab_path):]

                        already_organised = (
                            self.config.FILE_ORG_ENABLED
                            and self.config.AUDIOBOOK_ORGANIZED_DIR
                            and os.path.abspath(save_path).startswith(os.path.abspath(self.config.AUDIOBOOK_ORGANIZED_DIR))
                        )

                        if not already_organised and self.config.FILE_ORG_ENABLED:
                            author, resolved_title = "", t.get("name", "")
                            if os.path.isfile(save_path):
                                resolved_title = os.path.splitext(os.path.basename(save_path))[0]
                            if not author and os.path.exists(save_path):
                                id3_author, id3_title = self.read_audio_metadata(save_path)
                                if id3_author or id3_title:
                                    author = id3_author or author
                                    resolved_title = id3_title or resolved_title
                                    self.logger.info("Metadata from ID3 tags: %s - %s", author, resolved_title)
                            if not author and " - " in resolved_title:
                                parts = resolved_title.split(" - ", 1)
                                author, resolved_title = parts[0].strip(), parts[1].strip()
                                self.logger.info("Metadata from torrent name parse: %s - %s", author, resolved_title)
                            if not author:
                                author = "Unknown"
                            self.pipeline.run_pipeline(
                                save_path,
                                title=resolved_title,
                                author=author,
                                media_type="audiobook",
                                source="torrent",
                                source_id=t["hash"],
                                library_db=self.library,
                            )
                        elif already_organised:
                            self.logger.info("Audiobook already in organised directory, skipping pipeline: %s", save_path)

                        if self.config.has_audiobookshelf() and self.config.ABS_LIBRARY_ID:
                            known_ids = set()
                            try:
                                resp = self.requests.get(
                                    f"{self.config.ABS_URL}/api/libraries/{self.config.ABS_LIBRARY_ID}/items",
                                    params={"limit": 500},
                                    headers={"Authorization": f"Bearer {self.config.ABS_TOKEN}"},
                                    timeout=15,
                                )
                                known_ids = {i["id"] for i in resp.json().get("results", [])}
                            except Exception:
                                pass
                            try:
                                self.requests.post(
                                    f"{self.config.ABS_URL}/api/libraries/{self.config.ABS_LIBRARY_ID}/scan",
                                    headers={"Authorization": f"Bearer {self.config.ABS_TOKEN}"},
                                    timeout=10,
                                )
                                self.logger.info("Audiobookshelf library scan triggered")
                            except Exception as e:
                                self.logger.error("Audiobookshelf scan failed: %s", e)
                            time.sleep(20)
                            self.abs_match_new_items(known_ids)
                        return
                time.sleep(5)
            except Exception:
                time.sleep(5)
