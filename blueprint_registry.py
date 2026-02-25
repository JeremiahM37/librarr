from __future__ import annotations

from routes.downloads import create_blueprint as create_downloads_blueprint
from routes.library import create_blueprint as create_library_blueprint
from routes.monitor import create_blueprint as create_monitor_blueprint
from routes.settings import create_blueprint as create_settings_blueprint
from routes.system import create_blueprint as create_system_blueprint
from routes.web import create_blueprint as create_web_blueprint


def register_blueprints(app, deps):
    app.register_blueprint(create_web_blueprint({"config": deps["config"]}))
    app.register_blueprint(create_system_blueprint({
        "config": deps["config"],
        "db_path": deps["db_path"],
        "get_migration_status": deps["get_migration_status"],
        "download_jobs": deps["download_jobs"],
        "library": deps["library"],
        "source_health": deps["source_health"],
        "telemetry": deps["telemetry"],
        "sources": deps["sources"],
        "runtime_config_validation": deps["runtime_config_validation"],
    }))
    app.register_blueprint(create_settings_blueprint({
        "config": deps["config"],
        "db_path": deps["db_path"],
        "flask_app": app,
        "qb": deps["qb"],
        "logger": deps["logger"],
        "runtime_config_validation": deps["runtime_config_validation"],
        "test_prowlarr_connection": deps["test_prowlarr_connection"],
        "test_qbittorrent_connection": deps["test_qbittorrent_connection"],
        "test_audiobookshelf_connection": deps["test_audiobookshelf_connection"],
        "test_kavita_connection": deps["test_kavita_connection"],
    }))
    app.register_blueprint(create_downloads_blueprint({
        "config": deps["config"],
        "qb": deps["qb"],
        "logger": deps["logger"],
        "sources": deps["sources"],
        "filter_results": deps["filter_results"],
        "search_source_safe": deps["search_source_safe"],
        "source_health_metadata": deps["source_health_metadata"],
        "truthy": deps["truthy"],
        "download_preflight_response": deps["download_preflight_response"],
        "duplicate_summary": deps["duplicate_summary"],
        "extract_download_source_id": deps["extract_download_source_id"],
        "resolve_abb_magnet": deps["resolve_abb_magnet"],
        "watch_torrent": deps["watch_torrent"],
        "ensure_retry_scheduler": deps["ensure_retry_scheduler"],
        "download_jobs": deps["download_jobs"],
        "base_job_fields": deps["base_job_fields"],
        "parse_requested_targets": deps["parse_requested_targets"],
        "start_job_thread": deps["start_job_thread"],
        "download_novel_worker": deps["download_novel_worker"],
        "download_annas_worker": deps["download_annas_worker"],
        "human_size": deps["human_size"],
        "job_max_retries": deps["job_max_retries"],
        "dispatch_retry": deps["dispatch_retry"],
        "run_source_download_worker": deps["run_source_download_worker"],
        "library": deps["library"],
    }))
    app.register_blueprint(create_library_blueprint({
        "config": deps["config"],
        "logger": deps["logger"],
        "library": deps["library"],
        "download_jobs": deps["download_jobs"],
        "base_job_fields": deps["base_job_fields"],
        "process_csv_import_jobs": deps["process_csv_import_jobs"],
    }))
    app.register_blueprint(create_monitor_blueprint({"get_monitor": deps["get_monitor"]}))
