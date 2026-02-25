from __future__ import annotations

import sqlite3

from flask import Blueprint, Response, jsonify, request


def create_blueprint(ctx):
    bp = Blueprint("system_routes", __name__)

    @bp.route("/api/health")
    def api_health():
        return jsonify({"status": "ok", "version": "1.0.0"})

    @bp.route("/readyz")
    def readyz():
        deep = request.args.get("deep", "0").lower() in ("1", "true", "yes")
        strict = request.args.get("strict", "0").lower() in ("1", "true", "yes")

        db_ok = True
        db_error = None
        try:
            with sqlite3.connect(ctx["db_path"], timeout=5) as conn:
                conn.execute("SELECT 1")
        except Exception as e:
            db_ok = False
            db_error = str(e)

        source_count = len(ctx["sources"].get_sources()) if "sources" in ctx else 0
        runtime_diag = ctx["runtime_config_validation"](run_network_tests=deep)

        local_failures = []
        if not db_ok:
            local_failures.append({"component": "database", "error": db_error})
        if source_count == 0:
            local_failures.append({"component": "sources", "error": "no sources loaded"})
        if runtime_diag.get("routing_rules", {}).get("valid") is False:
            local_failures.append({"component": "routing_rules", "error": runtime_diag["routing_rules"].get("error")})
        for path_check in runtime_diag.get("paths", []):
            if path_check.get("ok") is False:
                local_failures.append({
                    "component": "path",
                    "name": path_check.get("name"),
                    "error": path_check.get("error", "path check failed"),
                })

        service_failures = []
        if deep:
            qb_diag = runtime_diag.get("qb")
            if qb_diag and qb_diag.get("success") is False:
                service_failures.append({
                    "component": "qbittorrent",
                    "error": qb_diag.get("error"),
                    "error_class": qb_diag.get("error_class"),
                })
            for name, check in (runtime_diag.get("services") or {}).items():
                if check.get("success") is False:
                    service_failures.append({
                        "component": name,
                        "error": check.get("error"),
                        "error_class": check.get("error_class"),
                    })

        failures = list(local_failures)
        if strict:
            failures.extend(service_failures)

        return jsonify({
            "status": "ready" if not failures else "not_ready",
            "strict": strict,
            "deep": deep,
            "checks": {
                "database": {"ok": db_ok, "error": db_error},
                "sources": {"ok": source_count > 0, "count": source_count},
                "runtime": runtime_diag,
            },
            "failures": failures,
            "warnings": [] if strict else service_failures,
        }), (200 if not failures else 503)

    @bp.route("/api/schema")
    def api_schema_status():
        with sqlite3.connect(ctx["db_path"], timeout=10) as conn:
            migrations = ctx["get_migration_status"](conn)
        return jsonify({"migrations": migrations, "count": len(migrations)})

    @bp.route("/metrics")
    def metrics_endpoint():
        status_counts = {}
        for _job_id, job in list(ctx["download_jobs"].items()):
            status = job.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        lines = [
            "# HELP librarr_jobs_by_status Number of Librarr jobs by current status.",
            "# TYPE librarr_jobs_by_status gauge",
        ]
        for status, count in sorted(status_counts.items()):
            lines.append(f'librarr_jobs_by_status{{status="{status}"}} {count}')
        lines.extend([
            "# HELP librarr_library_items_total Number of tracked library items.",
            "# TYPE librarr_library_items_total gauge",
            f"librarr_library_items_total {ctx['library'].count_items()}",
            "# HELP librarr_activity_events_total Number of activity log events.",
            "# TYPE librarr_activity_events_total gauge",
            f"librarr_activity_events_total {ctx['library'].count_activity()}",
            "# HELP librarr_source_health_score Source health score (0-100).",
            "# TYPE librarr_source_health_score gauge",
            "# HELP librarr_source_circuit_open Whether source search circuit is open (1=open).",
            "# TYPE librarr_source_circuit_open gauge",
        ])
        for name, health in sorted(ctx["source_health"].snapshot().items()):
            score = float(health.get("score", 100.0))
            is_open = 1 if health.get("circuit_open") else 0
            lines.append(f'librarr_source_health_score{{source="{name}"}} {score}')
            lines.append(f'librarr_source_circuit_open{{source="{name}"}} {is_open}')
        return Response(
            ctx["telemetry"].metrics.render(lines),
            mimetype="text/plain; version=0.0.4",
        )

    @bp.route("/api/config")
    def api_config():
        config = ctx["config"]
        return jsonify({
            "prowlarr": config.has_prowlarr(),
            "qbittorrent": config.has_qbittorrent(),
            "calibre": config.has_calibre(),
            "audiobookshelf": config.has_audiobookshelf(),
            "lncrawl": config.has_lncrawl(),
            "audiobooks": config.has_audiobooks(),
            "kavita": config.has_kavita(),
            "file_org_enabled": config.FILE_ORG_ENABLED,
            "enabled_targets": list(config.get_enabled_target_names()),
            "auth_enabled": config.has_auth(),
        })

    return bp
