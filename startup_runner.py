from __future__ import annotations

import threading


def initialize_runtime_services(
    *,
    app,
    config,
    logger,
    library,
    sources,
    qb,
    opds_module,
    monitor_module,
    ensure_retry_scheduler,
    auto_import_loop,
    validate_config,
    get_jobs,
    get_abb_domains,
    rotate_abb_domain,
    trigger_abs_scan,
):
    validate_config()
    library.cleanup_activity(days=90)

    sources.load_sources()
    enabled_sources = sources.get_enabled_sources("main") + sources.get_enabled_sources("audiobook")
    source_names = ", ".join(s.label for s in enabled_sources) or "none"
    logger.info("Librarr starting — %s sources enabled: %s", len(enabled_sources), source_names)

    integrations = []
    if config.has_qbittorrent():
        integrations.append("qBittorrent")
    if config.has_calibre():
        integrations.append("Calibre-Web")
    if config.has_audiobookshelf():
        integrations.append("Audiobookshelf")
    if config.has_lncrawl():
        integrations.append("lightnovel-crawler")
    if config.has_kavita():
        integrations.append("Kavita")
    if integrations:
        logger.info("Integrations: %s", ", ".join(integrations))

    if config.has_qbittorrent():
        threading.Thread(target=auto_import_loop, daemon=True).start()

    ensure_retry_scheduler()
    opds_module.init_app(app, library)

    runtime_monitor = monitor_module.LibrarrMonitor(
        get_jobs=get_jobs,
        qb_reauth=lambda: qb.login(),
        get_abb_domains=get_abb_domains,
        rotate_abb_domain=rotate_abb_domain,
        trigger_abs_scan=trigger_abs_scan,
        docker_socket=config.DOCKER_SOCKET,
    )
    runtime_monitor.start()
    return runtime_monitor
