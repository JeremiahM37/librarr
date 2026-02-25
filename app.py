"""Compatibility shim for legacy imports.

Exports the runtime objects from `app_factory` so existing plugins/tests that import
`app` continue to work while the app wiring lives in `app_factory`.
"""
import app_factory as _app_factory

globals().update({k: v for k, v in vars(_app_factory).items() if not k.startswith('__')})

def _sync_factory_state():
    if "download_jobs" in globals():
        _app_factory.download_jobs = globals()["download_jobs"]
    return _app_factory

def _schedule_or_dead_letter(*args, **kwargs):
    return _sync_factory_state()._schedule_or_dead_letter(*args, **kwargs)

def _reset_job_for_retry(*args, **kwargs):
    return _sync_factory_state()._reset_job_for_retry(*args, **kwargs)

def _run_source_download_worker(*args, **kwargs):
    return _sync_factory_state()._run_source_download_worker(*args, **kwargs)

def _dispatch_retry(*args, **kwargs):
    return _sync_factory_state()._dispatch_retry(*args, **kwargs)

def _retry_scheduler_loop(*args, **kwargs):
    return _sync_factory_state()._retry_scheduler_loop(*args, **kwargs)

def _ensure_retry_scheduler(*args, **kwargs):
    return _sync_factory_state()._ensure_retry_scheduler(*args, **kwargs)

if __name__ == "__main__":
    _app_factory.run_main()
