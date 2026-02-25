from __future__ import annotations

import sqlite3

from flask import Blueprint, Response, jsonify


def create_blueprint(ctx):
    bp = Blueprint("system_routes", __name__)

    @bp.route("/api/health")
    def api_health():
        return jsonify({"status": "ok", "version": "1.0.0"})

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
