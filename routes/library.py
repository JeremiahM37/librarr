from __future__ import annotations

import csv
import io
import threading
import uuid
from io import BytesIO

import requests
from flask import Blueprint, jsonify, request, send_file


def create_blueprint(ctx):
    bp = Blueprint("library_routes", __name__)
    config = ctx["config"]
    logger = ctx["logger"]
    library = ctx["library"]

    @bp.route("/api/library")
    def api_library():
        if not config.has_audiobookshelf() or not config.ABS_EBOOK_LIBRARY_ID:
            return jsonify({"books": [], "total": 0, "page": 1, "pages": 1})
        page = int(request.args.get("page", 1))
        per_page = 24
        search = request.args.get("q", "").strip()
        try:
            params = {"limit": per_page, "page": page - 1, "sort": "addedAt", "desc": 1}
            if search:
                params["filter"] = f"search={search}"
            resp = requests.get(
                f"{config.ABS_URL}/api/libraries/{config.ABS_EBOOK_LIBRARY_ID}/items",
                params=params,
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
            data = resp.json()
            total = data.get("total", 0)
            books = []
            for item in data.get("results", []):
                media = item.get("media", {})
                meta = media.get("metadata", {})
                books.append({
                    "id": item["id"],
                    "title": meta.get("title", "Unknown"),
                    "authors": meta.get("authorName", "Unknown"),
                    "has_cover": bool(media.get("coverPath")),
                    "cover_url": f"/api/cover/{item['id']}",
                    "added": item.get("addedAt", ""),
                })
            return jsonify({
                "books": books,
                "total": total,
                "page": page,
                "pages": max(1, (total + per_page - 1) // per_page),
            })
        except Exception as e:
            logger.error("Library error: %s", e)
            return jsonify({"books": [], "total": 0, "page": 1, "pages": 1})

    @bp.route("/api/cover/<item_id>")
    def api_cover(item_id):
        if not config.has_audiobookshelf():
            return "", 404
        try:
            resp = requests.get(
                f"{config.ABS_URL}/api/items/{item_id}/cover",
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
            if resp.status_code == 200:
                return send_file(
                    BytesIO(resp.content),
                    mimetype=resp.headers.get("Content-Type", "image/jpeg"),
                )
        except Exception:
            pass
        return "", 404

    @bp.route("/api/library/book/<item_id>", methods=["DELETE"])
    def api_delete_book(item_id):
        if not config.has_audiobookshelf():
            return jsonify({"success": False, "error": "Audiobookshelf not configured"}), 500
        try:
            resp = requests.delete(
                f"{config.ABS_URL}/api/items/{item_id}?hard=1",
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
            success = resp.status_code == 200
            if success:
                logger.info("Deleted ebook %s from Audiobookshelf", item_id)
            return jsonify({"success": success})
        except Exception as e:
            logger.error("Delete ebook error: %s", e)
            return jsonify({"success": False, "error": str(e)}), 500

    @bp.route("/api/library/audiobook/<item_id>", methods=["DELETE"])
    def api_delete_audiobook(item_id):
        if not config.has_audiobookshelf():
            return jsonify({"success": False, "error": "Audiobookshelf not configured"}), 500
        try:
            resp = requests.delete(
                f"{config.ABS_URL}/api/items/{item_id}?hard=1",
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
            success = resp.status_code == 200
            if success:
                logger.info("Deleted audiobook %s from Audiobookshelf", item_id)
            return jsonify({"success": success})
        except Exception as e:
            logger.error("Delete audiobook error: %s", e)
            return jsonify({"success": False, "error": str(e)}), 500

    @bp.route("/api/external-urls")
    def api_external_urls():
        return jsonify({"abs_url": config.ABS_PUBLIC_URL})

    @bp.route("/api/library/audiobooks")
    def api_library_audiobooks():
        if not config.has_audiobookshelf() or not config.ABS_LIBRARY_ID:
            return jsonify({"audiobooks": [], "total": 0})
        page = int(request.args.get("page", 1))
        per_page = 24
        search = request.args.get("q", "").strip()
        try:
            params = {"limit": per_page, "page": page - 1, "sort": "addedAt", "desc": 1}
            if search:
                params["filter"] = f"search={search}"
            resp = requests.get(
                f"{config.ABS_URL}/api/libraries/{config.ABS_LIBRARY_ID}/items",
                params=params,
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
            data = resp.json()
            total = data.get("total", 0)
            audiobooks = []
            for item in data.get("results", []):
                media = item.get("media", {})
                meta = media.get("metadata", {})
                duration = media.get("duration", 0)
                hours = int(duration // 3600)
                mins = int((duration % 3600) // 60)
                audiobooks.append({
                    "id": item["id"],
                    "title": meta.get("title", "Unknown"),
                    "authors": meta.get("authorName", "Unknown"),
                    "narrator": meta.get("narratorName", ""),
                    "duration": f"{hours}h {mins}m" if duration else "",
                    "num_chapters": media.get("numChapters", 0),
                    "cover_url": f"/api/audiobook/cover/{item['id']}",
                    "has_cover": bool(media.get("coverPath")),
                })
            return jsonify({
                "audiobooks": audiobooks,
                "total": total,
                "page": page,
                "pages": max(1, (total + per_page - 1) // per_page),
            })
        except Exception as e:
            logger.error("Audiobook library error: %s", e)
            return jsonify({"audiobooks": [], "total": 0, "page": 1, "pages": 1})

    @bp.route("/api/audiobook/cover/<item_id>")
    def api_audiobook_cover(item_id):
        if not config.has_audiobookshelf():
            return "", 404
        try:
            resp = requests.get(
                f"{config.ABS_URL}/api/items/{item_id}/cover",
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
            if resp.status_code == 200:
                return send_file(
                    BytesIO(resp.content),
                    mimetype=resp.headers.get("Content-Type", "image/jpeg"),
                )
        except Exception:
            pass
        return "", 404

    @bp.route("/api/activity")
    def api_activity():
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)
        events = library.get_activity(limit=limit, offset=offset)
        total = library.count_activity()
        return jsonify({"events": events, "total": total})

    @bp.route("/api/library/tracked")
    def api_library_tracked():
        media_type = request.args.get("type", None)
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)
        items = library.get_items(media_type=media_type, limit=limit, offset=offset)
        total = library.count_items(media_type=media_type)
        return jsonify({"items": items, "total": total})

    @bp.route("/api/import/csv", methods=["POST"])
    def api_import_csv():
        """Parse a Goodreads or StoryGraph CSV export and queue downloads."""
        f = request.files.get("csv_file")
        if not f:
            return jsonify({"error": "csv_file is required"}), 400

        shelf_filter = request.form.get("shelf", "to-read").lower()
        media_type = request.form.get("media_type", "ebook")
        limit = min(int(request.form.get("limit", 50)), 200)

        try:
            text = f.read().decode("utf-8-sig", errors="replace")
        except Exception as e:
            return jsonify({"error": f"Could not read file: {e}"}), 400

        reader = csv.DictReader(io.StringIO(text))
        headers = [h.lower().strip() for h in (reader.fieldnames or [])]
        is_goodreads = "exclusive shelf" in headers
        is_storygraph = "read status" in headers

        queued = []
        skipped = 0

        for row in reader:
            if len(queued) >= limit:
                break

            def _get(*keys):
                for k in keys:
                    for h, v in row.items():
                        if h.lower().strip() == k:
                            return (v or "").strip()
                return ""

            title = _get("title")
            author = _get("author", "authors")
            if not title:
                skipped += 1
                continue

            if is_goodreads:
                shelf = _get("exclusive shelf").lower()
                if shelf_filter and shelf != shelf_filter:
                    skipped += 1
                    continue
            elif is_storygraph:
                read_status = _get("read status").lower()
                sg_map = {"to-read": "to-read", "currently-reading": "reading", "read": "read"}
                mapped = sg_map.get(read_status, read_status)
                if shelf_filter and mapped != shelf_filter and read_status != shelf_filter:
                    skipped += 1
                    continue

            search_title = f"{title} {author}".strip()
            if library.find_by_title(title):
                skipped += 1
                continue

            job_id = str(uuid.uuid4())[:8]
            ctx["download_jobs"][job_id] = ctx["base_job_fields"](
                title,
                "csv_import",
                type="search_import",
                author=author,
                query=search_title,
                media_type=media_type,
            )
            queued.append({"job_id": job_id, "title": title, "author": author})

        if queued:
            threading.Thread(target=ctx["process_csv_import_jobs"], daemon=True).start()

        return jsonify({
            "queued": len(queued),
            "skipped": skipped,
            "items": queued,
            "format": "goodreads" if is_goodreads else "storygraph" if is_storygraph else "unknown",
        })

    return bp
