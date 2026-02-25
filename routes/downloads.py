from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, jsonify, request


def create_blueprint(ctx):
    bp = Blueprint("download_routes", __name__)
    config = ctx["config"]
    qb = ctx["qb"]
    logger = ctx["logger"]

    @bp.route("/api/search")
    def api_search():
        query = request.args.get("q", "").strip()
        if not query:
            return jsonify({"results": [], "error": "No query provided"})

        all_results = []
        start = time.time()
        enabled = ctx["sources"].get_enabled_sources(tab="main")

        with ThreadPoolExecutor(max_workers=max(len(enabled), 1)) as executor:
            futures = {executor.submit(ctx["search_source_safe"], s, query): s for s in enabled}
            for future in as_completed(futures, timeout=35):
                source = futures[future]
                try:
                    results = future.result()
                    for r in results:
                        r.setdefault("source", source.name)
                    all_results.extend(results)
                except Exception as e:
                    logger.error("Search wrapper error (%s): %s", source.name, e)

        all_results = ctx["filter_results"](all_results, query)
        elapsed = int((time.time() - start) * 1000)
        return jsonify({
            "results": all_results,
            "search_time_ms": elapsed,
            "sources": ctx["source_health_metadata"](),
        })

    @bp.route("/api/search/audiobooks")
    def api_search_audiobooks():
        query = request.args.get("q", "").strip()
        if not query:
            return jsonify({"results": [], "error": "No query provided"})
        start = time.time()
        enabled = ctx["sources"].get_enabled_sources(tab="audiobook")

        with ThreadPoolExecutor(max_workers=max(len(enabled), 1)) as executor:
            futures = {executor.submit(ctx["search_source_safe"], s, query): s for s in enabled}
            results = []
            try:
                for future in as_completed(futures, timeout=60):
                    source = futures[future]
                    try:
                        batch = future.result()
                        for r in batch:
                            r.setdefault("source", source.name)
                        results.extend(batch)
                    except Exception as e:
                        logger.error("Audiobook search wrapper error (%s): %s", source.name, e)
            except TimeoutError:
                logger.warning("Audiobook search timed out — returning partial results")
                for future, source in futures.items():
                    if future.done():
                        try:
                            batch = future.result()
                            for r in batch:
                                r.setdefault("source", source.name)
                            results.extend(batch)
                        except Exception:
                            pass

        results = ctx["filter_results"](results, query)
        elapsed = int((time.time() - start) * 1000)
        return jsonify({
            "results": results,
            "search_time_ms": elapsed,
            "sources": ctx["source_health_metadata"](),
        })

    @bp.route("/api/download/torrent", methods=["POST"])
    def api_download_torrent():
        if not config.has_qbittorrent():
            return jsonify({"success": False, "error": "qBittorrent not configured. Set QB_URL, QB_USER, QB_PASS."}), 400

        data = request.json or {}
        if ctx["truthy"](data.get("dry_run")):
            return jsonify(ctx["download_preflight_response"](data, source_name=data.get("source", "torrent"), source_type="torrent"))
        guid = data.get("guid", "")
        url = data.get("download_url") or (guid if guid.startswith("magnet:") else "") or data.get("magnet_url", "")
        if not url and data.get("info_hash"):
            url = f"magnet:?xt=urn:btih:{data['info_hash']}"
        title = data.get("title", "Unknown")
        if not url:
            return jsonify({"success": False, "error": "No download URL"}), 400
        dup = ctx["duplicate_summary"](title=title, source_id=ctx["extract_download_source_id"](data))
        if dup["duplicate"] and not ctx["truthy"](data.get("force")):
            return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409
        success = qb.add_torrent(url, title)
        if success:
            threading.Thread(target=ctx["watch_torrent"], args=(title,), daemon=True).start()
        return jsonify({"success": success, "title": title})

    @bp.route("/api/download/audiobook", methods=["POST"])
    def api_download_audiobook():
        if not config.has_qbittorrent():
            return jsonify({"success": False, "error": "qBittorrent not configured. Set QB_URL, QB_USER, QB_PASS."}), 400

        data = request.json or {}
        if ctx["truthy"](data.get("dry_run")):
            payload = dict(data)
            payload.setdefault("media_type", "audiobook")
            return jsonify(ctx["download_preflight_response"](payload, source_name=data.get("source", "audiobook"), source_type="torrent"))

        guid = data.get("guid", "")
        url = data.get("download_url") or (guid if guid.startswith("magnet:") else "") or data.get("magnet_url", "")
        if not url and data.get("info_hash"):
            url = f"magnet:?xt=urn:btih:{data['info_hash']}"
        abb_url = data.get("abb_url", "")
        if not url and abb_url:
            magnet = ctx["resolve_abb_magnet"](abb_url)
            if magnet:
                url = magnet
            else:
                return jsonify({"success": False, "error": "Failed to resolve AudioBookBay link"}), 400
        title = data.get("title", "Unknown")
        if not url:
            return jsonify({"success": False, "error": "No download URL"}), 400
        dup = ctx["duplicate_summary"](title=title, source_id=ctx["extract_download_source_id"](data))
        if dup["duplicate"] and not ctx["truthy"](data.get("force")):
            return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409
        success = qb.add_torrent(url, title, save_path=config.QB_AUDIOBOOK_SAVE_PATH, category=config.QB_AUDIOBOOK_CATEGORY)
        return jsonify({"success": success, "title": title})

    @bp.route("/api/download/novel", methods=["POST"])
    def api_download_novel():
        data = request.json or {}
        if ctx["truthy"](data.get("dry_run")):
            payload = dict(data)
            payload.setdefault("media_type", "ebook")
            payload.setdefault("source", "webnovel")
            return jsonify(ctx["download_preflight_response"](payload, source_name="webnovel", source_type="direct"))
        url = data.get("url", "")
        title = data.get("title", "Unknown")
        if not url:
            return jsonify({"success": False, "error": "No URL"}), 400
        dup = ctx["duplicate_summary"](title=title, source_id=data.get("url", ""))
        if dup["duplicate"] and not ctx["truthy"](data.get("force")):
            return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409
        ctx["ensure_retry_scheduler"]()
        job_id = str(uuid.uuid4())[:8]
        ctx["download_jobs"][job_id] = ctx["base_job_fields"](
            title,
            "webnovel",
            url=url,
            target_names=ctx["parse_requested_targets"](data),
            retry_kind="novel",
            retry_payload={"url": url, "title": title},
        )
        ctx["start_job_thread"](ctx["download_novel_worker"], (job_id, url, title))
        return jsonify({"success": True, "job_id": job_id, "title": title})

    @bp.route("/api/download/annas", methods=["POST"])
    def api_download_annas():
        data = request.json or {}
        if ctx["truthy"](data.get("dry_run")):
            payload = dict(data)
            payload.setdefault("media_type", "ebook")
            payload.setdefault("source", "annas")
            return jsonify(ctx["download_preflight_response"](payload, source_name="annas", source_type="direct"))
        md5 = data.get("md5", "")
        title = data.get("title", "Unknown")
        if not md5:
            return jsonify({"success": False, "error": "No MD5 hash"}), 400
        dup = ctx["duplicate_summary"](title=title, source_id=md5)
        if dup["duplicate"] and not ctx["truthy"](data.get("force")):
            return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409
        ctx["ensure_retry_scheduler"]()
        job_id = str(uuid.uuid4())[:8]
        ctx["download_jobs"][job_id] = ctx["base_job_fields"](
            title,
            "annas",
            url=f"https://annas-archive.li/md5/{md5}",
            target_names=ctx["parse_requested_targets"](data),
            retry_kind="annas",
            retry_payload={"md5": md5, "title": title},
        )
        ctx["start_job_thread"](ctx["download_annas_worker"], (job_id, md5, title))
        return jsonify({"success": True, "job_id": job_id, "title": title})

    @bp.route("/api/downloads")
    def api_downloads():
        downloads = []
        if config.has_qbittorrent():
            for cat, source_label in [(config.QB_CATEGORY, "torrent"), (config.QB_AUDIOBOOK_CATEGORY, "audiobook")]:
                try:
                    torrents = qb.get_torrents(category=cat)
                    for t in torrents:
                        state = t.get("state", "")
                        status_map = {
                            "downloading": "downloading", "stalledDL": "downloading",
                            "metaDL": "downloading", "forcedDL": "downloading",
                            "pausedDL": "paused", "queuedDL": "queued",
                            "uploading": "completed", "stalledUP": "completed",
                            "pausedUP": "completed", "queuedUP": "completed",
                            "checkingDL": "checking", "checkingUP": "checking",
                        }
                        downloads.append({
                            "source": source_label,
                            "title": t.get("name", ""),
                            "progress": round(t.get("progress", 0) * 100, 1),
                            "status": status_map.get(state, state),
                            "size": ctx["human_size"](t.get("total_size", 0)),
                            "speed": ctx["human_size"](t.get("dlspeed", 0)) + "/s",
                            "hash": t.get("hash", ""),
                        })
                except Exception:
                    pass
        for job_id, job in list(ctx["download_jobs"].items()):
            downloads.append({
                "source": job.get("source", "webnovel"),
                "title": job["title"],
                "status": job["status"],
                "job_id": job_id,
                "error": job.get("error"),
                "detail": job.get("detail"),
                "retry_count": job.get("retry_count", 0),
                "max_retries": job.get("max_retries", ctx["job_max_retries"]),
                "next_retry_at": job.get("next_retry_at"),
            })
        return jsonify({"downloads": downloads})

    @bp.route("/api/downloads/torrent/<torrent_hash>", methods=["DELETE"])
    def api_delete_torrent(torrent_hash):
        return jsonify({"success": qb.delete_torrent(torrent_hash, delete_files=True)})

    @bp.route("/api/downloads/novel/<job_id>", methods=["DELETE"])
    def api_delete_novel(job_id):
        if job_id in ctx["download_jobs"]:
            del ctx["download_jobs"][job_id]
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "Job not found"}), 404

    @bp.route("/api/downloads/jobs/<job_id>/retry", methods=["POST"])
    def api_retry_job(job_id):
        if job_id not in ctx["download_jobs"]:
            return jsonify({"success": False, "error": "Job not found"}), 404
        job = ctx["download_jobs"][job_id]
        if job.get("status") not in ("error", "dead_letter", "retry_wait"):
            return jsonify({"success": False, "error": f"Job status {job.get('status')} not retryable"}), 400
        ctx["ensure_retry_scheduler"]()
        if not ctx["dispatch_retry"](job_id):
            return jsonify({"success": False, "error": "No retry handler for job"}), 400
        return jsonify({"success": True, "job_id": job_id, "status": ctx["download_jobs"][job_id].get("status")})

    @bp.route("/api/downloads/clear", methods=["POST"])
    def api_clear_finished():
        to_remove = [jid for jid, j in ctx["download_jobs"].items() if j["status"] in ("completed", "error", "dead_letter")]
        for jid in to_remove:
            del ctx["download_jobs"][jid]
        removed_torrents = 0
        if config.has_qbittorrent():
            for cat in (config.QB_CATEGORY, config.QB_AUDIOBOOK_CATEGORY):
                try:
                    torrents = qb.get_torrents(category=cat)
                    for t in torrents:
                        state = t.get("state", "")
                        if state in ("uploading", "stalledUP", "pausedUP", "queuedUP", "error", "missingFiles"):
                            qb.delete_torrent(t["hash"], delete_files=False)
                            removed_torrents += 1
                except Exception:
                    pass
        return jsonify({"success": True, "cleared_novels": len(to_remove), "cleared_torrents": removed_torrents})

    @bp.route("/api/sources")
    def api_sources():
        return jsonify(ctx["source_health_metadata"]())

    @bp.route("/api/download", methods=["POST"])
    def api_download():
        data = request.json or {}
        source_name = data.get("source", "")
        source = ctx["sources"].get_source(source_name)
        if not source:
            return jsonify({"success": False, "error": f"Unknown source: {source_name}"}), 400
        if not source.enabled():
            return jsonify({"success": False, "error": f"Source '{source.label}' is not configured"}), 400

        title = data.get("title", "Unknown")
        requested_targets = ctx["parse_requested_targets"](data)
        source_id = ctx["extract_download_source_id"](data)
        media_type_guess = "audiobook" if getattr(source, "search_tab", "main") == "audiobook" else "ebook"

        if ctx["truthy"](data.get("dry_run")):
            dry_payload = dict(data)
            dry_payload.setdefault("media_type", media_type_guess)
            return jsonify(ctx["download_preflight_response"](dry_payload, source_name=source_name, source_type=source.download_type))

        if source.download_type == "torrent":
            if not config.has_qbittorrent():
                return jsonify({"success": False, "error": "qBittorrent not configured"}), 400
            guid = data.get("guid", "")
            url = data.get("download_url") or (guid if guid.startswith("magnet:") else "") or data.get("magnet_url", "")
            if not url and data.get("info_hash"):
                url = f"magnet:?xt=urn:btih:{data['info_hash']}"
            if not url and data.get("abb_url"):
                magnet = ctx["resolve_abb_magnet"](data["abb_url"])
                if magnet:
                    url = magnet
                else:
                    return jsonify({"success": False, "error": "Failed to resolve download link"}), 400
            if not url:
                return jsonify({"success": False, "error": "No download URL"}), 400
            dup = ctx["duplicate_summary"](title=title, source_id=source_id)
            if dup["duplicate"] and not ctx["truthy"](data.get("force")):
                return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409
            if source.search_tab == "audiobook":
                save_path = config.QB_AUDIOBOOK_SAVE_PATH
                category = config.QB_AUDIOBOOK_CATEGORY
            else:
                save_path = config.QB_SAVE_PATH
                category = config.QB_CATEGORY
            success = qb.add_torrent(url, title, save_path=save_path, category=category)
            return jsonify({"success": success, "title": title})

        dup = ctx["duplicate_summary"](title=title, source_id=source_id)
        if dup["duplicate"] and not ctx["truthy"](data.get("force")):
            return jsonify({"success": False, "error": "Duplicate detected", "duplicate_check": dup}), 409

        ctx["ensure_retry_scheduler"]()
        job_id = str(uuid.uuid4())[:8]
        ctx["download_jobs"][job_id] = ctx["base_job_fields"](
            title,
            source_name,
            target_names=requested_targets,
            retry_kind="source",
            retry_payload={"source_name": source_name, "data": data},
        )
        ctx["start_job_thread"](ctx["run_source_download_worker"], (job_id, source_name, data))
        return jsonify({"success": True, "job_id": job_id, "title": title})

    @bp.route("/api/check-duplicate")
    def api_check_duplicate():
        source_id = request.args.get("source_id", "")
        if not source_id:
            return jsonify({"duplicate": False})
        return jsonify({"duplicate": ctx["library"].has_source_id(source_id)})

    return bp
