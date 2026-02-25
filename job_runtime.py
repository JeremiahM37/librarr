from __future__ import annotations

import threading
import time


class JobRuntime:
    def __init__(
        self,
        *,
        download_jobs,
        logger,
        sources,
        job_max_retries,
        retry_backoff_sec,
        record_source_download_result,
        get_novel_worker,
        get_annas_worker,
    ):
        self.download_jobs = download_jobs
        self.logger = logger
        self.sources = sources
        self.job_max_retries = job_max_retries
        self.retry_backoff_sec = retry_backoff_sec
        self.record_source_download_result = record_source_download_result
        self.get_novel_worker = get_novel_worker
        self.get_annas_worker = get_annas_worker
        self._retry_loop_started = False
        self._retry_thread_lock = threading.Lock()

    def base_job_fields(self, title, source, **extra):
        data = {
            "status": "queued",
            "title": title,
            "source": source,
            "error": None,
            "detail": None,
            "retry_count": 0,
            "max_retries": self.job_max_retries,
            "next_retry_at": None,
            "failure_history": [],
            "status_history": [],
        }
        data.update(extra)
        return data

    def schedule_or_dead_letter(self, job_id, error_message, *, retry_kind=None, retry_payload=None):
        job = self.download_jobs[job_id]
        job["error"] = error_message
        failures = list(job.get("failure_history") or [])
        failures.append({"ts": time.time(), "error": error_message})
        job["failure_history"] = failures[-10:]
        retry_count = int(job.get("retry_count", 0)) + 1
        job["retry_count"] = retry_count
        max_retries = int(job.get("max_retries", self.job_max_retries))
        if retry_kind:
            job["retry_kind"] = retry_kind
        if retry_payload is not None:
            job["retry_payload"] = retry_payload
        if retry_count <= max_retries:
            delay = self.retry_backoff_sec * retry_count
            job["next_retry_at"] = time.time() + delay
            job["detail"] = f"Retry {retry_count}/{max_retries} scheduled in {delay}s"
            job["status"] = "retry_wait"
        else:
            job["next_retry_at"] = None
            job["detail"] = f"Moved to dead-letter after {retry_count - 1} retries"
            job["status"] = "dead_letter"

    def reset_job_for_retry(self, job_id):
        job = self.download_jobs[job_id]
        job["error"] = None
        job["detail"] = f"Retrying (attempt {int(job.get('retry_count', 0)) + 1})..."
        job["next_retry_at"] = None
        job["status"] = "queued"

    @staticmethod
    def start_job_thread(target, args):
        t = threading.Thread(target=target, args=args, daemon=True)
        t.start()
        return t

    def run_source_download_worker(self, job_id, source_name, data):
        source = self.sources.get_source(source_name)
        if not source:
            self.schedule_or_dead_letter(
                job_id,
                f"Unknown source: {source_name}",
                retry_kind="source",
                retry_payload={"source_name": source_name, "data": data},
            )
            return
        self.download_jobs[job_id]["status"] = "downloading"
        try:
            success = source.download(data, self.download_jobs[job_id])
            self.record_source_download_result(source_name, bool(success), self.download_jobs[job_id].get("error", ""))
            if not success and self.download_jobs[job_id]["status"] != "completed":
                msg = self.download_jobs[job_id].get("error") or "Download failed"
                self.schedule_or_dead_letter(
                    job_id,
                    msg,
                    retry_kind="source",
                    retry_payload={"source_name": source_name, "data": data},
                )
        except Exception as e:
            self.logger.error("Download error (%s): %s", source_name, e)
            self.record_source_download_result(source_name, False, str(e))
            self.schedule_or_dead_letter(
                job_id,
                str(e),
                retry_kind="source",
                retry_payload={"source_name": source_name, "data": data},
            )

    def dispatch_retry(self, job_id):
        job = self.download_jobs.get(job_id)
        if not job:
            return False
        retry_kind = job.get("retry_kind")
        payload = job.get("retry_payload") or {}
        self.reset_job_for_retry(job_id)
        if retry_kind == "novel":
            self.start_job_thread(
                self.get_novel_worker(),
                (job_id, payload.get("url", job.get("url", "")), payload.get("title", job.get("title", "Unknown"))),
            )
            return True
        if retry_kind == "annas":
            self.start_job_thread(
                self.get_annas_worker(),
                (job_id, payload.get("md5", ""), payload.get("title", job.get("title", "Unknown"))),
            )
            return True
        if retry_kind == "source":
            self.start_job_thread(
                self.run_source_download_worker,
                (job_id, payload.get("source_name", job.get("source", "")), payload.get("data", payload)),
            )
            return True
        self.logger.warning("No retry handler for job %s (kind=%s)", job_id, retry_kind)
        self.download_jobs[job_id]["error"] = f"No retry handler for kind={retry_kind}"
        self.download_jobs[job_id]["status"] = "dead_letter"
        return False

    def _retry_scheduler_loop(self):
        while True:
            now = time.time()
            for job_id, job in list(self.download_jobs.items()):
                if job.get("status") != "retry_wait":
                    continue
                next_retry_at = job.get("next_retry_at") or 0
                if next_retry_at and next_retry_at <= now:
                    try:
                        self.dispatch_retry(job_id)
                    except Exception as e:
                        self.logger.error("Retry dispatch failed for %s: %s", job_id, e)
            time.sleep(2)

    def ensure_retry_scheduler(self):
        with self._retry_thread_lock:
            if self._retry_loop_started:
                return
            self._retry_loop_started = True
            threading.Thread(target=self._retry_scheduler_loop, daemon=True).start()
