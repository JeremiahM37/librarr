from __future__ import annotations


def job_transition_allowed(old_status, new_status, state_transitions):
    return new_status in state_transitions.get(old_status, set())


def record_job_status_transition(job_id, old_status, new_status, job_data, *, telemetry, job_max_retries):
    telemetry.metrics.inc(
        "librarr_job_transitions_total",
        from_status=old_status or "none",
        to_status=new_status,
        source=job_data.get("source", "unknown"),
    )
    if new_status in ("completed", "error", "dead_letter"):
        telemetry.metrics.inc(
            "librarr_job_terminal_total",
            status=new_status,
            source=job_data.get("source", "unknown"),
        )
    if new_status == "retry_wait":
        telemetry.metrics.inc(
            "librarr_job_retry_scheduled_total",
            source=job_data.get("source", "unknown"),
        )
    if new_status in ("completed", "error", "dead_letter", "retry_wait"):
        telemetry.emit_event(
            f"job_{new_status}",
            {
                "job_id": job_id,
                "title": job_data.get("title"),
                "source": job_data.get("source"),
                "status": new_status,
                "retry_count": job_data.get("retry_count", 0),
                "max_retries": job_data.get("max_retries", job_max_retries),
                "error": job_data.get("error"),
                "detail": job_data.get("detail"),
            },
        )
