from __future__ import annotations

import time


def process_csv_import_jobs(*, download_jobs, sources, search_source_safe, logger):
    """Background worker: find and download queued search-import jobs."""
    sources.load_sources()
    for job_id, job in list(download_jobs.items()):
        if job.get("type") != "search_import" or job.get("status") != "queued":
            continue
        query = job.get("query", "")
        try:
            download_jobs[job_id]["status"] = "searching"
            results = []
            for src in sources.get_enabled_sources("main"):
                try:
                    results.extend(search_source_safe(src, query))
                except Exception:
                    pass
            if results:
                best = results[0]
                url = best.get("download_url") or best.get("magnet")
                if url:
                    download_jobs[job_id]["url"] = url
                    download_jobs[job_id]["status"] = "queued"
                    download_jobs[job_id]["type"] = "torrent" if (url or "").startswith("magnet:") else "direct"
                    logger.info("CSV import: queued '%s'", query)
                else:
                    download_jobs[job_id]["status"] = "error"
                    download_jobs[job_id]["error"] = "No downloadable result found"
            else:
                download_jobs[job_id]["status"] = "error"
                download_jobs[job_id]["error"] = "Not found in any source"
        except Exception as e:
            download_jobs[job_id]["status"] = "error"
            download_jobs[job_id]["error"] = str(e)
        time.sleep(1)
