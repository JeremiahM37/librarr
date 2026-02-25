"""
Librarr — Self-hosted book search and download manager.

Searches Anna's Archive, Prowlarr indexers, AudioBookBay, and web novel sites.
Downloads via direct HTTP, qBittorrent, or lightnovel-crawler.
Auto-imports into Calibre-Web and Audiobookshelf.
"""
import glob
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from html.parser import HTMLParser

import requests
from flask import Flask, Response, jsonify, redirect, request, send_file, session, url_for

from app_dependency_builders import build_blueprint_registrar_context, build_startup_kwargs
import config
import csv_import_jobs
import diagnostics
import monitor_helpers
import pipeline
import monitor
import opds
import sources
import telemetry
from app_callbacks import make_abb_rotate_callback, make_blueprint_registrar
from auth_guard import register_auth_guard
import blueprint_registry
from download_helpers import DownloadHelpers
from job_store import DownloadStore as _DownloadStoreBase
from job_runtime import JobRuntime
from job_events import job_transition_allowed, record_job_status_transition
from media_utils import human_size as _human_size, read_audio_metadata as _read_audio_metadata, validate_config_paths
from novel_annas_workers import NovelAnnasWorkers
from provider_search import ProviderSearchService
from qb_client import QBittorrentClient, test_qbittorrent_connection
from runtime_bridge import JobRuntimeBridge
from source_health import SourceHealthTracker
from startup_runner import initialize_runtime_services
from torrent_import_workers import TorrentImportWorkers
from library_db import LibraryDB
from db_migrations import apply_migrations, get_migration_status
from webnovel_search import FreeWebNovelParser as _FreeWebNovelParser, WebNovelSearchService

app = Flask(__name__)
app.secret_key = config.SECRET_KEY
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("librarr")
register_auth_guard(app, config)

JOB_MAX_RETRIES = max(0, int(os.getenv("LIBRARR_JOB_MAX_RETRIES", "2")))
JOB_RETRY_BACKOFF_SEC = max(1, int(os.getenv("LIBRARR_JOB_RETRY_BACKOFF_SEC", "60")))
SOURCE_CIRCUIT_FAILURE_THRESHOLD = max(1, int(os.getenv("LIBRARR_SOURCE_CIRCUIT_FAILURE_THRESHOLD", "3")))
SOURCE_CIRCUIT_OPEN_SEC = max(5, int(os.getenv("LIBRARR_SOURCE_CIRCUIT_OPEN_SEC", "300")))
JOB_STATE_TRANSITIONS = {
    None: {"queued", "searching", "downloading", "importing", "retry_wait", "completed", "error", "dead_letter"},
    "queued": {"searching", "downloading", "importing", "completed", "retry_wait", "error", "dead_letter"},
    "searching": {"queued", "downloading", "completed", "retry_wait", "error", "dead_letter"},
    "downloading": {"importing", "completed", "retry_wait", "error", "dead_letter"},
    "importing": {"completed", "retry_wait", "error", "dead_letter"},
    "retry_wait": {"queued", "error", "dead_letter"},
    "error": {"queued", "retry_wait", "dead_letter"},
    "dead_letter": {"queued"},
    "completed": set(),
}


_job_transition_allowed = partial(job_transition_allowed, state_transitions=JOB_STATE_TRANSITIONS)
_record_job_status_transition = partial(
    record_job_status_transition,
    telemetry=telemetry,
    job_max_retries=JOB_MAX_RETRIES,
)


# =============================================================================
# Persistent Download Job Store (SQLite-backed)
# =============================================================================
class DownloadStore(_DownloadStoreBase):
    def __init__(self, db_path):
        super().__init__(
            db_path,
            apply_migrations=apply_migrations,
            logger=logger,
            telemetry=telemetry,
            job_max_retries=JOB_MAX_RETRIES,
            transition_allowed=_job_transition_allowed,
            record_transition=_record_job_status_transition,
        )


_DB_PATH = os.getenv("LIBRARR_DB_PATH", "/data/librarr/downloads.db")
download_jobs = DownloadStore(_DB_PATH)
library = LibraryDB(_DB_PATH)
_monitor = None  # initialized in __main__
_job_runtime = None
_job_runtime_bridge = None


_source_health = SourceHealthTracker(
    telemetry,
    threshold=SOURCE_CIRCUIT_FAILURE_THRESHOLD,
    open_seconds=SOURCE_CIRCUIT_OPEN_SEC,
)


_validate_config = partial(validate_config_paths, config, logger)


# =============================================================================
# qBittorrent Client
# =============================================================================
qb = QBittorrentClient()

_download_helpers = DownloadHelpers(
    config=config,
    qb=qb,
    logger=logger,
    sources=sources,
    source_health=_source_health,
    telemetry=telemetry,
    library=library,
    pipeline_module=pipeline,
)
_source_health_metadata = _download_helpers.source_health_metadata
_search_source_safe = _download_helpers.search_source_safe
_record_source_download_result = _download_helpers.record_source_download_result
_truthy = _download_helpers.truthy
_extract_download_source_id = _download_helpers.extract_download_source_id
_duplicate_summary = _download_helpers.duplicate_summary
_parse_requested_targets = _download_helpers.parse_requested_targets
_download_preflight_response = _download_helpers.download_preflight_response

_job_runtime = JobRuntime(
    download_jobs=download_jobs,
    logger=logger,
    sources=sources,
    job_max_retries=JOB_MAX_RETRIES,
    retry_backoff_sec=JOB_RETRY_BACKOFF_SEC,
    record_source_download_result=_record_source_download_result,
    get_novel_worker=lambda: download_novel_worker,
    get_annas_worker=lambda: download_annas_worker,
)
_job_runtime_bridge = JobRuntimeBridge(_job_runtime, get_download_jobs=lambda: download_jobs)
_base_job_fields = _job_runtime_bridge.base_job_fields
_schedule_or_dead_letter = _job_runtime_bridge.schedule_or_dead_letter
_reset_job_for_retry = _job_runtime_bridge.reset_job_for_retry
_start_job_thread = _job_runtime_bridge.start_job_thread
_run_source_download_worker = _job_runtime_bridge.run_source_download_worker
_dispatch_retry = _job_runtime_bridge.dispatch_retry
_retry_scheduler_loop = _job_runtime_bridge.retry_scheduler_loop
_ensure_retry_scheduler = _job_runtime_bridge.ensure_retry_scheduler

_webnovel_search = WebNovelSearchService(requests_module=requests, logger=logger)
FreeWebNovelParser = _FreeWebNovelParser
search_freewebnovel = _webnovel_search.search_freewebnovel
search_allnovelfull = _webnovel_search.search_allnovelfull
search_boxnovel = _webnovel_search.search_boxnovel
search_novelbin = _webnovel_search.search_novelbin
search_novelfull = _webnovel_search.search_novelfull
search_lightnovelpub = _webnovel_search.search_lightnovelpub
search_readnovelfull = _webnovel_search.search_readnovelfull
search_webnovels = _webnovel_search.search_webnovels


# Prowlarr / AudioBookBay provider search helpers are provided by provider_search.py


_provider_search = ProviderSearchService(
    config=config,
    logger=logger,
    requests_module=requests,
    human_size=_human_size,
)
ABB_DOMAINS = _provider_search.abb_domains
ABB_URL = _provider_search.abb_url
ABB_TRACKERS = _provider_search.abb_trackers
search_prowlarr = _provider_search.search_prowlarr
search_prowlarr_audiobooks = _provider_search.search_prowlarr_audiobooks
_get_abb_response = _provider_search.get_abb_response
search_audiobookbay = _provider_search.search_audiobookbay
_resolve_abb_magnet = _provider_search.resolve_abb_magnet
_check_libgen_available = _provider_search.check_libgen_available
search_annas_archive = _provider_search.search_annas_archive


# =============================================================================
# Web Novel Search
# =============================================================================
# Web novel search parsers/searchers are provided by webnovel_search.py


_torrent_import_workers = TorrentImportWorkers(
    config=config,
    logger=logger,
    qb=qb,
    pipeline_module=pipeline,
    library=library,
    requests_module=requests,
    read_audio_metadata=_read_audio_metadata,
)
import_event = _torrent_import_workers.import_event
imported_hashes = _torrent_import_workers.imported_hashes


_novel_annas_workers = NovelAnnasWorkers(
    config=config,
    logger=logger,
    requests_module=requests,
    pipeline_module=pipeline,
    library=library,
    download_jobs=download_jobs,
    schedule_or_dead_letter=_schedule_or_dead_letter,
    search_annas_archive=search_annas_archive,
    search_webnovels=search_webnovels,
    human_size=_human_size,
)

_clean_incoming = _novel_annas_workers.clean_incoming
download_novel_worker = _novel_annas_workers.download_novel_worker
_try_download_url = _novel_annas_workers.try_download_url
_download_from_annas = _novel_annas_workers.download_from_annas
download_annas_worker = _novel_annas_workers.download_annas_worker



# =============================================================================
# Background Novel Download
# =============================================================================
# Novel/Anna's background workers are provided by novel_annas_workers.py


# =============================================================================
# Background Auto-Import for Completed Torrents
# =============================================================================
import_completed_torrents = _torrent_import_workers.import_completed_torrents
auto_import_loop = _torrent_import_workers.auto_import_loop
watch_torrent = _torrent_import_workers.watch_torrent
_abs_match_new_items = _torrent_import_workers.abs_match_new_items
watch_audiobook_torrent = _torrent_import_workers.watch_audiobook_torrent

# =============================================================================
# Result Filtering
# =============================================================================
_title_relevant = _download_helpers.title_relevant
filter_results = _download_helpers.filter_results


# =============================================================================
# API Routes
# =============================================================================
# web/login/logout routes are registered via routes.web blueprint


# health/config/metrics/schema routes are registered via routes.system blueprint


# search/download/source routes are registered via routes.downloads blueprint


# library, cover, activity, and CSV import routes are registered via routes.library blueprint


# plugin source and unified download routes are registered via routes.downloads blueprint


# duplicate check route is registered via routes.downloads blueprint


# =============================================================================
# Settings API
# =============================================================================
# settings and test routes are registered via routes.settings blueprint


_test_prowlarr_connection = partial(diagnostics.test_prowlarr_connection, requests_module=requests)
_test_qbittorrent_connection = partial(test_qbittorrent_connection, requests_module=requests)
_test_audiobookshelf_connection = partial(diagnostics.test_audiobookshelf_connection, requests_module=requests)
_test_kavita_connection = partial(diagnostics.test_kavita_connection, requests_module=requests)
_runtime_config_validation = partial(diagnostics.runtime_config_validation, config, qb, requests_module=requests)


# diagnostics test routes are registered via routes.settings blueprint

# CSV import route is registered via routes.library blueprint


_process_csv_import_jobs = partial(
    csv_import_jobs.process_csv_import_jobs,
    download_jobs=download_jobs,
    sources=sources,
    search_source_safe=_search_source_safe,
    logger=logger,
)
_register_blueprints = make_blueprint_registrar(
    app,
    build_blueprint_registrar_context(
        blueprint_registry=blueprint_registry,
        config=config,
        db_path=_DB_PATH,
        get_migration_status=get_migration_status,
        download_jobs=download_jobs,
        library=library,
        source_health=_source_health,
        telemetry=telemetry,
        qb=qb,
        logger=logger,
        runtime_config_validation=_runtime_config_validation,
        test_prowlarr_connection=_test_prowlarr_connection,
        test_qbittorrent_connection=_test_qbittorrent_connection,
        test_audiobookshelf_connection=_test_audiobookshelf_connection,
        test_kavita_connection=_test_kavita_connection,
        sources=sources,
        filter_results=filter_results,
        search_source_safe=_search_source_safe,
        source_health_metadata=_source_health_metadata,
        truthy=_truthy,
        download_preflight_response=_download_preflight_response,
        duplicate_summary=_duplicate_summary,
        extract_download_source_id=_extract_download_source_id,
        resolve_abb_magnet=_resolve_abb_magnet,
        watch_torrent=watch_torrent,
        ensure_retry_scheduler=_ensure_retry_scheduler,
        base_job_fields=_base_job_fields,
        parse_requested_targets=_parse_requested_targets,
        start_job_thread=_start_job_thread,
        download_novel_worker=download_novel_worker,
        download_annas_worker=download_annas_worker,
        human_size=_human_size,
        job_max_retries=JOB_MAX_RETRIES,
        dispatch_retry=_dispatch_retry,
        run_source_download_worker=_run_source_download_worker,
        process_csv_import_jobs=lambda: _process_csv_import_jobs(),
        get_monitor=lambda: _monitor,
    ),
)


_abb_state = {"ABB_URL": ABB_URL}
_rotate_abb_domain = make_abb_rotate_callback(_provider_search, _abb_state)


_do_abs_scan = partial(monitor_helpers.trigger_abs_scan, config=config, logger=logger, requests_module=requests)


# AI monitor routes are registered via routes.monitor blueprint


_register_blueprints()


def run_main():
    _monitor = initialize_runtime_services(**build_startup_kwargs(
        app=app,
        config=config,
        logger=logger,
        library=library,
        sources=sources,
        qb=qb,
        opds_module=opds,
        monitor_module=monitor,
        ensure_retry_scheduler=_ensure_retry_scheduler,
        auto_import_loop=auto_import_loop,
        validate_config=_validate_config,
        get_jobs=lambda: list(download_jobs.items()),
        get_abb_domains=lambda: list(ABB_DOMAINS),
        rotate_abb_domain=_rotate_abb_domain,
        trigger_abs_scan=_do_abs_scan,
    ))
    app.run(host="0.0.0.0", port=5000, debug=False)