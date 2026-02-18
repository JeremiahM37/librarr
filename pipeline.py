"""Post-processing pipeline — organize, import, track."""
import logging
import os
import re
import shutil

import config
import targets

logger = logging.getLogger("librarr")


def sanitize_filename(name, max_len=80):
    """Make a string safe for use as a filename."""
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.strip(".")
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "Unknown"


def organize_file(file_path, title, author, media_type="ebook"):
    """Move a downloaded file into Author/Title/ structure.

    Returns the new file path, or the original if organization is disabled
    or fails.
    """
    if not config.FILE_ORG_ENABLED:
        return file_path

    if not os.path.isfile(file_path):
        logger.warning(f"organize_file: file not found: {file_path}")
        return file_path

    safe_author = sanitize_filename(author or "Unknown")
    safe_title = sanitize_filename(title or "Unknown")
    ext = os.path.splitext(file_path)[1].lower() or ".epub"

    if media_type == "audiobook":
        base_dir = config.AUDIOBOOK_ORGANIZED_DIR
    else:
        base_dir = config.EBOOK_ORGANIZED_DIR

    dest_dir = os.path.join(base_dir, safe_author, safe_title)
    dest_path = os.path.join(dest_dir, f"{safe_title}{ext}")

    if os.path.abspath(file_path) == os.path.abspath(dest_path):
        return file_path

    try:
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(file_path, dest_path)
        logger.info(f"Organized: {dest_path}")
    except Exception as e:
        logger.error(f"organize_file failed: {e}")
        return file_path

    # Copy to Kavita library path if configured (separate volume)
    if config.KAVITA_LIBRARY_PATH and media_type == "ebook":
        try:
            kavita_dir = os.path.join(
                config.KAVITA_LIBRARY_PATH, safe_author, safe_title
            )
            kavita_path = os.path.join(kavita_dir, f"{safe_title}{ext}")
            os.makedirs(kavita_dir, exist_ok=True)
            shutil.copy2(dest_path, kavita_path)
            logger.info(f"Copied to Kavita library: {kavita_path}")
        except Exception as e:
            logger.warning(f"Kavita copy failed: {e}")

    return dest_path


def run_pipeline(file_path, title="", author="", media_type="ebook",
                 source="", source_id="", job_id="", library_db=None):
    """Full post-processing: organize → import → track.

    Args:
        file_path: Path to the downloaded file.
        title: Book title.
        author: Author name.
        media_type: 'ebook' or 'audiobook'.
        source: Source name (e.g. 'annas', 'prowlarr').
        source_id: Unique identifier for duplicate detection (md5, hash, url).
        job_id: Download job ID for activity log cross-reference.
        library_db: LibraryDB instance (optional — skips tracking if None).

    Returns:
        dict with pipeline results, or None if skipped (duplicate).
    """
    result = {"organized": False, "imports": {}, "tracked": False}

    # Duplicate check
    if library_db and source_id and library_db.has_source_id(source_id):
        logger.info(f"Duplicate skipped: {title} (source_id={source_id})")
        library_db.log_event("skip", title=title,
                             detail=f"Duplicate (source: {source})",
                             job_id=job_id)
        return None

    # Organize
    original_path = file_path
    file_size = 0
    try:
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
        elif os.path.isdir(file_path):
            # Sum all files in directory (e.g. audiobook folders)
            for dirpath, _, filenames in os.walk(file_path):
                for f in filenames:
                    file_size += os.path.getsize(os.path.join(dirpath, f))
    except OSError:
        pass

    organized_path = organize_file(file_path, title, author, media_type)
    result["organized"] = organized_path != original_path

    if library_db and result["organized"]:
        library_db.log_event("organize", title=title,
                             detail=f"→ {organized_path}", job_id=job_id)

    # Import to enabled targets
    enabled = targets.get_enabled_targets()
    for target in enabled:
        try:
            import_result = target.import_book(
                organized_path, title=title, author=author, media_type=media_type
            )
            if import_result:
                result["imports"][target.name] = import_result
                if library_db:
                    library_db.log_event(
                        "import", title=title,
                        detail=f"Imported to {target.label}",
                        job_id=job_id,
                    )
        except Exception as e:
            logger.error(f"Target {target.name} failed: {e}")
            if library_db:
                library_db.log_event(
                    "error", title=title,
                    detail=f"{target.label} import failed: {e}",
                    job_id=job_id,
                )

    # Track in library
    file_format = os.path.splitext(organized_path)[1].lstrip(".").lower()
    item_id = None
    if library_db:
        item_id = library_db.add_item(
            title=title, author=author,
            file_path=organized_path, original_path=original_path,
            file_size=file_size, file_format=file_format,
            media_type=media_type, source=source, source_id=source_id,
            metadata=result["imports"],
        )
        library_db.log_event(
            "download", title=title,
            detail=f"Added to library ({source})",
            library_item_id=item_id, job_id=job_id,
        )
        result["tracked"] = True

    return result
