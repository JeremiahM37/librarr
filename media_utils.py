from __future__ import annotations

import glob
import os


def human_size(size_bytes):
    if not size_bytes:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def validate_config_paths(config, logger):
    paths_to_check = []
    if config.INCOMING_DIR:
        paths_to_check.append(("INCOMING_DIR", config.INCOMING_DIR))
    if config.EBOOK_ORGANIZED_DIR and config.FILE_ORG_ENABLED:
        paths_to_check.append(("EBOOK_ORGANIZED_DIR", config.EBOOK_ORGANIZED_DIR))
    if config.AUDIOBOOK_DIR:
        paths_to_check.append(("AUDIOBOOK_DIR", config.AUDIOBOOK_DIR))
    for name, path in paths_to_check:
        if not os.path.exists(path):
            logger.warning("Config path %s=%r does not exist — creating it", name, path)
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                logger.error("Cannot create %s=%r: %s", name, path, e)


def read_audio_metadata(path: str):
    """Try to extract author/title from ID3/vorbis tags in audio files."""
    try:
        from mutagen import File as MutagenFile
    except ImportError:
        return None, None

    extensions = (
        "*.mp3",
        "*.m4b",
        "*.m4a",
        "*.flac",
        "*.ogg",
        "*.opus",
        "*.oga",
        "*.wma",
        "*.aiff",
        "*.ape",
        "*.wv",
        "*.asf",
        "*.tta",
    )
    if os.path.isfile(path):
        import fnmatch
        if any(fnmatch.fnmatch(os.path.basename(path), ext) for ext in extensions):
            try:
                audio = MutagenFile(path, easy=True)
                if audio:
                    title = (audio.get("album") or audio.get("title") or [None])[0]
                    author = (audio.get("artist") or audio.get("albumartist") or [None])[0]
                    if title or author:
                        return author, title
            except Exception:
                pass
        return None, None

    for ext in extensions:
        matches = glob.glob(os.path.join(path, "**", ext), recursive=True)
        if not matches:
            continue
        try:
            audio = MutagenFile(matches[0], easy=True)
            if not audio:
                continue
            title = (audio.get("album") or audio.get("title") or [None])[0]
            author = (audio.get("artist") or audio.get("albumartist") or [None])[0]
            if title or author:
                return author, title
        except Exception:
            continue
    return None, None
