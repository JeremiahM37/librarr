from __future__ import annotations


class JobRuntimeBridge:
    """Keeps JobRuntime synced with the current module-level download store."""

    def __init__(self, runtime, get_download_jobs):
        self.runtime = runtime
        self.get_download_jobs = get_download_jobs

    def _sync(self):
        self.runtime.download_jobs = self.get_download_jobs()
        return self.runtime

    def base_job_fields(self, title, source, **extra):
        return self.runtime.base_job_fields(title, source, **extra)

    def schedule_or_dead_letter(self, job_id, error_message, *, retry_kind=None, retry_payload=None):
        return self._sync().schedule_or_dead_letter(
            job_id,
            error_message,
            retry_kind=retry_kind,
            retry_payload=retry_payload,
        )

    def reset_job_for_retry(self, job_id):
        return self._sync().reset_job_for_retry(job_id)

    def start_job_thread(self, target, args):
        return self.runtime.start_job_thread(target, args)

    def run_source_download_worker(self, job_id, source_name, data):
        return self._sync().run_source_download_worker(job_id, source_name, data)

    def dispatch_retry(self, job_id):
        return self._sync().dispatch_retry(job_id)

    def retry_scheduler_loop(self):
        return self._sync()._retry_scheduler_loop()

    def ensure_retry_scheduler(self):
        return self._sync().ensure_retry_scheduler()
