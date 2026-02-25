from __future__ import annotations


def rotate_list_primary(values):
    if len(values) > 1:
        values = values[1:] + [values[0]]
    return values, (values[0] if values else None)


def trigger_abs_scan(*, config, logger, requests_module):
    """Trigger Audiobookshelf library rescan for all configured libraries."""
    if not config.has_audiobookshelf():
        return
    for lib_id in filter(None, [config.ABS_LIBRARY_ID, config.ABS_EBOOK_LIBRARY_ID]):
        try:
            requests_module.post(
                f"{config.ABS_URL}/api/libraries/{lib_id}/scan",
                headers={"Authorization": f"Bearer {config.ABS_TOKEN}"},
                timeout=10,
            )
        except Exception as e:
            logger.error("ABS scan failed (%s): %s", lib_id, e)
